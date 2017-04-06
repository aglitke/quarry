#!/usr/bin/python
#
#  Copyright 2017 Red Hat, Inc. and/or its affiliates.
#
# Licensed to you under the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.  See the files README and
# LICENSE_GPL_v2 which accompany this distribution.
#
# Copyright 2013 OpenStack Foundation

import copy
import json
import logging
import math
import os
import rados
import rbd
import six
from six.moves import urllib
import subprocess
import tempfile

from ansible.module_utils import quarry_common


CONFIG_DEFAULTS = {
    # The name of the ceph cluster
    'rbd_cluster_name': 'ceph',

    # The RADOS pool where rbd volumes are stored
    'rbd_pool': 'rbd',

    # The RADOS client name for accessing rbd volumes. Only set when using
    # cephx authentication
    'rbd_user': None,

    # Path to the ceph configuration file (default determined by libraries)
    'rbd_ceph_conf': '',

    # Flatten volumes created from snapshots to remove dependency from volume
    # to snapshot
    'rbd_flatten_volume_from_snapshot': False,

    # The libvirt uuid of the secret for the rbd_user volumes
    'rbd_secret_uuid': None,

    # Maximum number of nested volume clones that are taken before a flatten
    # occurs. Set to 0 to disable cloning.
    'rbd_max_clone_depth': 5,

    # Volumes will be chunked into objects of this size (in megabytes).
    'rbd_store_chunk_size': 4,

    # Timeout value (in seconds) used when connecting to ceph cluster. If
    # value < 0, no timeout is set and default librados value is used.
    'rados_connect_timeout': -1,

    # Number of retries if connection to ceph cluster failed.
    'rados_connection_retries': 3,

    # Interval value (in seconds) between connection retries to ceph cluster.
    'rados_connection_interval': 5,

    # Timeout value (in seconds) used when connecting to ceph cluster to do a
    # demotion/promotion of volumes. If value < 0, no timeout is set and
    # default librados value is used.
    'replication_connect_timeout': 5,

    # Template string to be used to generate volume names
    'volume_name_template': 'volume-%s',
}


EXTRA_SPECS_REPL_ENABLED = "replication_enabled"


class VolumeIsBusy(Exception):
    def __init__(self, msg, volume_name=None):
        self.msg = msg
        self.volume_name = volume_name


class Driver(quarry_common.Driver):
    VERSION = '1.2.0'

    SYSCONFDIR = '/etc/ceph/'

    def __init__(self, config, **kwargs):
        self.configuration = copy.copy(CONFIG_DEFAULTS)
        self.configuration.update(config)

        # Allow override for testing
        self.rados = kwargs.get('rados', rados)
        self.rbd = kwargs.get('rbd', rbd)

        # All string args used with librbd must be None or utf-8 otherwise
        # librbd will break.
        for attr in ['rbd_cluster_name', 'rbd_user',
                     'rbd_ceph_conf', 'rbd_pool']:
            val = self.configuration.get(attr)
            if val is not None:
                self.configuration[attr] = quarry_common.convert_str(val)

        self._backend_name = (self.configuration.get('volume_backend_name') or
                              self.__class__.__name__)
        self._active_backend_id = None  # AGL: add init param?
        self._active_config = {}
        self._is_replication_enabled = False
        self._replication_targets = []
        self._target_names = []

    #
    # New Quarry methods
    #
    def get_volume(self, volume):
        try:
            with RBDVolumeProxy(self, volume.name, read_only=True) as proxy:
                size = proxy.size() / quarry_common.GB
                return quarry_common.Volume(volume.id, size=size)
        except rbd.ImageNotFound:
            return None

    def get_snapshot(self, snapshot):
        with RADOSClient(self) as client:
            volumes = self.rbd.RBD().list(client.ioctx)
            for volume in volumes:
                try:
                    self.rbd.Image(client.ioctx, volume, snapshot.name)
                    return quarry_common.Snapshot(snapshot.id,
                                                  volume_name=volume)
                except rbd.ImageNotFound:
                    pass
            return None
    #
    # New Quarry methods
    #

    def _get_target_config(self, target_id):
        """Get a replication target from known replication targets."""
        for target in self._replication_targets:
            if target['name'] == target_id:
                return target
        if not target_id or target_id == 'default':
            return {
                'name': self.configuration['rbd_cluster_name'],
                'conf': self.configuration['rbd_ceph_conf'],
                'user': self.configuration['rbd_user']
            }
        raise quarry_common.InvalidReplicationTarget(
            'RBD: Unknown failover target host %s.' % target_id)

    def do_setup(self, context):
        """Performs initialization steps that could raise exceptions."""
        self._do_setup_replication()
        self._active_config = self._get_target_config(self._active_backend_id)

    def _do_setup_replication(self):
        replication_devices = self.configuration.get('replication_device')
        if replication_devices:
            self._parse_replication_configs(replication_devices)
            self._is_replication_enabled = True
            self._target_names.append('default')

    def _parse_replication_configs(self, replication_devices):
        for replication_device in replication_devices:
            if 'backend_id' not in replication_device:
                msg = 'Missing backend_id in replication_device configuration.'
                raise quarry_common.InvalidConfigurationValue(msg)

            name = replication_device['backend_id']
            conf = replication_device.get('conf',
                                          self.SYSCONFDIR + name + '.conf')
            user = replication_device.get(
                'user', self.configuration['rbd_user'] or 'cinder')
            # Pool has to be the same in all clusters
            replication_target = {'name': name,
                                  'conf': quarry_common.convert_str(conf),
                                  'user': quarry_common.convert_str(user)}
            logging.info('Adding replication target: %s.', name)
            self._replication_targets.append(replication_target)
            self._target_names.append(name)

    def _get_config_tuple(self, remote=None):
        if not remote:
            remote = self._active_config
        return (remote.get('name'), remote.get('conf'), remote.get('user'))

    def check_for_setup_error(self):
        """Returns an error if prerequisites aren't met."""
        if rados is None:
            msg = 'rados and rbd python libraries not found'
            raise quarry_common.VolumeBackendAPIException(msg)

        for attr in ['rbd_cluster_name', 'rbd_pool']:
            val = self.configuration.get(attr)
            if not val:
                msg = "Configuration option %s is required" % attr
                raise quarry_common.InvalidConfigurationValue(msg)
        # NOTE: Checking connection to ceph
        # RADOSClient __init__ method invokes _connect_to_rados
        # so no need to check for self.rados.Error here.
        with RADOSClient(self):
            pass

    def RBDProxy(self):
        # AGL: I'm pretty sure we'll not need a threadpool for our impl...
        return self.rbd.RBD()

    def _ceph_args(self):
        args = []

        name, conf, user = self._get_config_tuple()

        if user:
            args.extend(['--id', user])
        if name:
            args.extend(['--cluster', name])
        if conf:
            args.extend(['--conf', conf])

        return args

    # TODO: Honor passed-in config for interval and retries
    @quarry_common.retry(quarry_common.VolumeBackendAPIException,
                         CONFIG_DEFAULTS['rados_connection_interval'],
                         CONFIG_DEFAULTS['rados_connection_retries'])
    def _connect_to_rados(self, pool=None, remote=None, timeout=None):

        name, conf, user = self._get_config_tuple(remote)

        if pool is not None:
            pool = quarry_common.convert_str(pool)
        else:
            pool = self.configuration['rbd_pool']

        if timeout is None:
            timeout = self.configuration['rados_connect_timeout']

        logging.debug("connecting to %(name)s (timeout=%(timeout)s).",
                      {'name': name, 'timeout': timeout})
        logging.debug("conf = %s, user = %s" % (conf, user))

        client = self.rados.Rados(rados_id=user,
                                  clustername=name,
                                  conffile=conf)

        try:
            if timeout >= 0:
                timeout = six.text_type(timeout)
                client.conf_set('rados_osd_op_timeout', timeout)
                client.conf_set('rados_mon_op_timeout', timeout)
                client.conf_set('client_mount_timeout', timeout)

            client.connect()
            ioctx = client.open_ioctx(pool)
            return client, ioctx
        except self.rados.Error:
            msg = "Error connecting to ceph cluster."
            logging.exception(msg)
            client.shutdown()
            raise quarry_common.VolumeBackendAPIException(msg)

    def _disconnect_from_rados(self, client, ioctx):
        # closing an ioctx cannot raise an exception
        ioctx.close()
        client.shutdown()

    def _get_backup_snaps(self, rbd_image):
        """Get list of any backup snapshots that exist on this volume.

        There should only ever be one but accept all since they need to be
        deleted before the volume can be.
        """
        # NOTE(dosaboy): we do the import here otherwise we get import conflict
        # issues between the rbd driver and the ceph backup driver. These
        # issues only seem to occur when NOT using them together and are
        # triggered when the ceph backup driver imports the rbd volume driver.

        # TODO: Implement this
        # from cinder.backup.drivers import ceph
        # return ceph.CephBackupDriver.get_backup_snaps(rbd_image)
        return []

    def _get_mon_addrs(self):
        args = ['ceph', 'mon', 'dump', '--format=json']
        args.extend(self._ceph_args())
        #out, _ = self._execute(*args)
        out = subprocess.check_output(args)
        lines = out.split('\n')
        if lines[0].startswith('dumped monmap epoch'):
            lines = lines[1:]
        monmap = json.loads('\n'.join(lines))
        addrs = [mon['addr'] for mon in monmap['mons']]
        hosts = []
        ports = []
        for addr in addrs:
            host_port = addr[:addr.rindex('/')]
            host, port = host_port.rsplit(':', 1)
            hosts.append(host.strip('[]'))
            ports.append(port)
        return hosts, ports

    def _get_usage_info(self):
        with RADOSClient(self) as client:
            for t in self.RBDProxy().list(client.ioctx):
                if t.startswith('volume'):
                    # Only check for "volume" to allow some flexibility with
                    # non-default volume_name_template settings.  Template
                    # must start with "volume".
                    with RBDVolumeProxy(self, t, read_only=True) as v:
                        self._total_usage += v.size()

    def _update_volume_stats(self):
        stats = {
            'vendor_name': 'Open Source',
            'driver_version': self.VERSION,
            'storage_protocol': 'ceph',
            'total_capacity_gb': 'unknown',
            'free_capacity_gb': 'unknown',
            'provisioned_capacity_gb': 0,
            'reserved_percentage': (
                self.configuration.get('reserved_percentage')),
            'multiattach': False,
            'thin_provisioning_support': True,
            'max_over_subscription_ratio': (
                self.configuration.get('max_over_subscription_ratio'))

        }
        backend_name = self.configuration.get('volume_backend_name')
        stats['volume_backend_name'] = backend_name or 'RBD'

        stats['replication_enabled'] = self._is_replication_enabled
        if self._is_replication_enabled:
            stats['replication_targets'] = self._target_names

        try:
            with RADOSClient(self) as client:
                ret, outbuf, _outs = client.cluster.mon_command(
                    '{"prefix":"df", "format":"json"}', '')
                if ret != 0:
                    logging.warning('Unable to get rados pool stats.')
                else:
                    outbuf = json.loads(outbuf)
                    pool_stats = [pool for pool in outbuf['pools'] if
                                  pool['name'] ==
                                  self.configuration['rbd_pool']][0]['stats']
                    stats['free_capacity_gb'] = round((float(
                        pool_stats['max_avail']) / quarry_common.GB), 2)
                    used_capacity_gb = float(
                        pool_stats['bytes_used']) / quarry_common.GB
                    stats['total_capacity_gb'] = round(
                        (stats['free_capacity_gb'] + used_capacity_gb), 2)

            self._total_usage = 0
            self._get_usage_info()
            total_usage_gb = math.ceil(float(self._total_usage) /
                                       quarry_common.GB)
            stats['provisioned_capacity_gb'] = total_usage_gb
        except self.rados.Error:
            # just log and return unknown capacities
            logging.exception('error refreshing volume stats')
        self._stats = stats

    def get_volume_stats(self, refresh=False):
        """Return the current state of the volume service.

        If 'refresh' is True, run the update first.
        """
        if refresh:
            self._update_volume_stats()
        return self._stats

    def _get_clone_depth(self, client, volume_name, depth=0):
        """Returns the number of ancestral clones of the given volume."""
        parent_volume = self.rbd.Image(client.ioctx, volume_name)
        try:
            _pool, parent, _snap = self._get_clone_info(parent_volume,
                                                        volume_name)
        finally:
            parent_volume.close()

        if not parent:
            return depth

        # If clone depth was reached, flatten should have occurred so if it has
        # been exceeded then something has gone wrong.
        if depth > self.configuration['rbd_max_clone_depth']:
            raise Exception("clone depth exceeds limit of %s" %
                            self.configuration['rbd_max_clone_depth'])

        return self._get_clone_depth(client, parent, depth + 1)

    def create_cloned_volume(self, volume, src_vref):
        """Create a cloned volume from another volume.

        Since we are cloning from a volume and not a snapshot, we must first
        create a snapshot of the source volume.

        The user has the option to limit how long a volume's clone chain can be
        by setting rbd_max_clone_depth. If a clone is made of another clone
        and that clone has rbd_max_clone_depth clones behind it, the source
        volume will be flattened.
        """
        src_name = quarry_common.convert_str(src_vref.name)
        dest_name = quarry_common.convert_str(volume.name)
        flatten_parent = False

        # Do full copy if requested
        if self.configuration['rbd_max_clone_depth'] <= 0:
            with RBDVolumeProxy(self, src_name, read_only=True) as vol:
                vol.copy(vol.ioctx, dest_name)

            return

        # Otherwise do COW clone.
        with RADOSClient(self) as client:
            depth = self._get_clone_depth(client, src_name)
            # If source volume is a clone and rbd_max_clone_depth reached,
            # flatten the source before cloning. Zero rbd_max_clone_depth means
            # infinite is allowed.
            if depth == self.configuration['rbd_max_clone_depth']:
                logging.debug("maximum clone depth (%d) has been reached - "
                              "flattening source volume",
                              self.configuration['rbd_max_clone_depth'])
                flatten_parent = True

            src_volume = self.rbd.Image(client.ioctx, src_name)
            try:
                # First flatten source volume if required.
                if flatten_parent:
                    _pool, parent, snap = self._get_clone_info(src_volume,
                                                               src_name)
                    # Flatten source volume
                    logging.debug("flattening source volume %s", src_name)
                    src_volume.flatten()
                    # Delete parent clone snap
                    parent_volume = self.rbd.Image(client.ioctx, parent)
                    try:
                        parent_volume.unprotect_snap(snap)
                        parent_volume.remove_snap(snap)
                    finally:
                        parent_volume.close()

                # Create new snapshot of source volume
                clone_snap = "%s.clone_snap" % dest_name
                logging.debug("creating snapshot='%s'", clone_snap)
                src_volume.create_snap(clone_snap)
                src_volume.protect_snap(clone_snap)
            except Exception:
                # Only close if exception since we still need it.
                src_volume.close()
                raise

            # Now clone source volume snapshot
            try:
                logging.debug("cloning '%(src_vol)s@%(src_snap)s' to "
                              "'%(dest)s'",
                              {'src_vol': src_name, 'src_snap': clone_snap,
                               'dest': dest_name})
                self.RBDProxy().clone(client.ioctx, src_name, clone_snap,
                                      client.ioctx, dest_name,
                                      features=client.features)
            except Exception:
                src_volume.unprotect_snap(clone_snap)
                src_volume.remove_snap(clone_snap)
                src_volume.close()
                raise

            try:
                volume_update = self._enable_replication_if_needed(volume)
            except Exception:
                self.RBDProxy().remove(client.ioctx, dest_name)
                err_msg = ('Failed to enable image replication for volume %s' %
                           volume.id)
                raise quarry_common.ReplicationError(err_msg)
            finally:
                src_volume.close()

        if volume.size != src_vref.size:
            logging.debug("resize volume '%(dst_vol)s' from %(src_size)d to "
                          "%(dst_size)d",
                          {'dst_vol': volume.name, 'src_size': src_vref.size,
                           'dst_size': volume.size})
            self._resize(volume)

        logging.debug("clone created successfully")
        return volume_update

    def _enable_replication(self, volume):
        """Enable replication for a volume.

        Returns required volume update.
        """
        vol_name = quarry_common.convert_str(volume.name)
        with RBDVolumeProxy(self, vol_name) as image:
            had_journaling = image.features() & self.rbd.RBD_FEATURE_JOURNALING
            if not had_journaling:
                image.update_features(self.rbd.RBD_FEATURE_JOURNALING, True)
            image.mirror_image_enable()

        driver_data = self._dumps({'had_journaling': bool(had_journaling)})
        return {'replication_status': quarry_common.ReplicationStatus.ENABLED,
                'replication_driver_data': driver_data}

    def _is_replicated_type(self, volume_type):
        # We do a safe attribute get because volume_type could be None
        specs = getattr(volume_type, 'extra_specs', {})
        return specs.get(EXTRA_SPECS_REPL_ENABLED) == "<is> True"

    def _enable_replication_if_needed(self, volume):
        if self._is_replicated_type(volume.volume_type):
            return self._enable_replication(volume)
        if self._is_replication_enabled:
            return {'replication_status':
                        quarry_common.ReplicationStatus.DISABLED}
        return None

    def create_volume(self, volume):
        """Creates a logical volume."""

        if volume.encryption_key_id:
            message = "Encryption is not yet supported."
            raise quarry_common.VolumeDriverException(message)

        size = int(volume.size) * quarry_common.GB

        logging.debug("creating volume '%s'", volume.name)

        chunk_size = (self.configuration['rbd_store_chunk_size'] *
                      quarry_common.MB)
        order = int(math.log(chunk_size, 2))
        vol_name = quarry_common.convert_str(volume.name)

        with RADOSClient(self) as client:
            self.RBDProxy().create(client.ioctx,
                                   vol_name,
                                   size,
                                   order,
                                   old_format=False,
                                   features=client.features)

            try:
                volume_update = self._enable_replication_if_needed(volume)
            except Exception:
                logging.exception("Failed to enable replication")
                self.RBDProxy().remove(client.ioctx, vol_name)
                err_msg = ('Failed to enable image replication for volume %s' %
                           volume.id)
                raise quarry_common.ReplicationError(err_msg)
        return volume_update

    def _flatten(self, pool, volume_name):
        logging.debug('flattening %(pool)s/%(img)s',
                      dict(pool=pool, img=volume_name))
        with RBDVolumeProxy(self, volume_name, pool) as vol:
            vol.flatten()

    def _clone(self, volume, src_pool, src_image, src_snap):
        logging.debug('cloning %(pool)s/%(img)s@%(snap)s to %(dst)s',
                      dict(pool=src_pool, img=src_image, snap=src_snap,
                           dst=volume.name))

        chunk_size = (self.configuration['rbd_store_chunk_size'] *
                      quarry_common.MB)
        order = int(math.log(chunk_size, 2))
        vol_name = quarry_common.convert_str(volume.name)

        with RADOSClient(self, src_pool) as src_client:
            with RADOSClient(self) as dest_client:
                self.RBDProxy().clone(src_client.ioctx,
                                      quarry_common.convert_str(src_image),
                                      quarry_common.convert_str(src_snap),
                                      dest_client.ioctx,
                                      vol_name,
                                      features=src_client.features,
                                      order=order)

            try:
                volume_update = self._enable_replication_if_needed(volume)
            except Exception:
                self.RBDProxy().remove(dest_client.ioctx, vol_name)
                err_msg = ('Failed to enable image replication for volume %s' %
                           volume.id)
                raise quarry_common.ReplicationError(err_msg)
            return volume_update or {}

    def _resize(self, volume, **kwargs):
        size = kwargs.get('size', None)
        if not size:
            size = int(volume.size) * quarry_common.GB

        with RBDVolumeProxy(self, volume.name) as vol:
            vol.resize(size)

    def create_volume_from_snapshot(self, volume, snapshot):
        """Creates a volume from a snapshot."""
        volume_update = self._clone(volume, self.configuration['rbd_pool'],
                                    snapshot.volume_name, snapshot.name)
        if self.configuration['rbd_flatten_volume_from_snapshot']:
            self._flatten(self.configuration['rbd_pool'], volume.name)
        if int(volume.size):
            self._resize(volume)
        return volume_update

    def _delete_backup_snaps(self, rbd_image):
        backup_snaps = self._get_backup_snaps(rbd_image)
        if backup_snaps:
            for snap in backup_snaps:
                rbd_image.remove_snap(snap['name'])
        else:
            logging.debug("volume has no backup snaps")

    def _get_clone_info(self, volume, volume_name, snap=None):
        """If volume is a clone, return its parent info.

        Returns a tuple of (pool, parent, snap). A snapshot may optionally be
        provided for the case where a cloned volume has been flattened but it's
        snapshot still depends on the parent.
        """
        try:
            if snap:
                volume.set_snap(snap)
            pool, parent, parent_snap = tuple(volume.parent_info())
            if snap:
                volume.set_snap(None)
            # Strip the tag off the end of the volume name since it will not be
            # in the snap name.
            if volume_name.endswith('.deleted'):
                volume_name = volume_name[:-len('.deleted')]
            # Now check the snap name matches.
            if parent_snap == "%s.clone_snap" % volume_name:
                return pool, parent, parent_snap
        except self.rbd.ImageNotFound:
            logging.debug("Volume %s is not a clone.", volume_name)
            volume.set_snap(None)

        return (None, None, None)

    def _get_children_info(self, volume, snap):
        """List children for the given snapshot of a volume(image).

        Returns a list of (pool, image).
        """

        children_list = []

        if snap:
            volume.set_snap(snap)
            children_list = volume.list_children()
            volume.set_snap(None)

        return children_list

    def _delete_clone_parent_refs(self, client, parent_name, parent_snap):
        """Walk back up the clone chain and delete references.

        Deletes references i.e. deleted parent volumes and snapshots.
        """
        parent_rbd = self.rbd.Image(client.ioctx, parent_name)
        parent_has_snaps = False
        try:
            # Check for grandparent
            _pool, g_parent, g_parent_snap = self._get_clone_info(parent_rbd,
                                                                  parent_name,
                                                                  parent_snap)

            logging.debug("deleting parent snapshot %s", parent_snap)
            parent_rbd.unprotect_snap(parent_snap)
            parent_rbd.remove_snap(parent_snap)

            parent_has_snaps = bool(list(parent_rbd.list_snaps()))
        finally:
            parent_rbd.close()

        # If parent has been deleted in Cinder, delete the silent reference and
        # keep walking up the chain if it is itself a clone.
        if (not parent_has_snaps) and parent_name.endswith('.deleted'):
            logging.debug("deleting parent %s", parent_name)
            self.RBDProxy().remove(client.ioctx, parent_name)

            # Now move up to grandparent if there is one
            if g_parent:
                self._delete_clone_parent_refs(client, g_parent, g_parent_snap)

    def delete_volume(self, volume):
        """Deletes a logical volume."""
        # NOTE(dosaboy): this was broken by commit cbe1d5f. Ensure names are
        #                utf-8 otherwise librbd will barf.
        volume_name = quarry_common.convert_str(volume.name)
        with RADOSClient(self) as client:
            try:
                rbd_image = self.rbd.Image(client.ioctx, volume_name)
            except self.rbd.ImageNotFound:
                logging.info("volume %s no longer exists in backend",
                             volume_name)
                return

            clone_snap = None
            parent = None

            # Ensure any backup snapshots are deleted
            self._delete_backup_snaps(rbd_image)

            # If the volume has non-clone snapshots this delete is expected to
            # raise VolumeIsBusy so do so straight away.
            try:
                snaps = rbd_image.list_snaps()
                for snap in snaps:
                    if snap['name'].endswith('.clone_snap'):
                        logging.debug("volume has clone snapshot(s)")
                        # We grab one of these and use it when fetching parent
                        # info in case the volume has been flattened.
                        clone_snap = snap['name']
                        break

                    raise quarry_common.VolumeIsBusy(volume_name)

                # Determine if this volume is itself a clone
                _pool, parent, parent_snap = self._get_clone_info(rbd_image,
                                                                  volume_name,
                                                                  clone_snap)
            finally:
                rbd_image.close()

            @quarry_common.retry(
                self.rbd.ImageBusy,
                self.configuration['rados_connection_interval'],
                self.configuration['rados_connection_retries'])
            def _try_remove_volume(client, volume_name):
                self.RBDProxy().remove(client.ioctx, volume_name)

            if clone_snap is None:
                logging.debug("deleting rbd volume %s", volume_name)
                try:
                    _try_remove_volume(client, volume_name)
                except self.rbd.ImageBusy:
                    msg = ("ImageBusy error raised while deleting rbd "
                           "volume %s. This may have been caused by a "
                           "connection from a client that has crashed and, "
                           "if so, may be resolved by retrying the delete "
                           "after 30 seconds has elapsed." % volume_name)
                    logging.warning(msg)
                    # Now raise this so that volume stays available so that we
                    # delete can be retried.
                    raise quarry_common.VolumeIsBusy(msg)
                except self.rbd.ImageNotFound:
                    logging.info("RBD volume %s not found, allowing delete "
                                 "operation to proceed.", volume_name)
                    return

                # If it is a clone, walk back up the parent chain deleting
                # references.
                if parent:
                    logging.debug("volume is a clone so cleaning references")
                    self._delete_clone_parent_refs(client, parent, parent_snap)
            else:
                # If the volume has copy-on-write clones we will not be able to
                # delete it. Instead we will keep it as a silent volume which
                # will be deleted when it's snapshot and clones are deleted.
                new_name = "%s.deleted" % (volume_name)
                self.RBDProxy().rename(client.ioctx, volume_name, new_name)

    def create_snapshot(self, snapshot):
        """Creates an rbd snapshot."""
        with RBDVolumeProxy(self, snapshot.volume_name) as volume:
            snap = quarry_common.convert_str(snapshot.name)
            volume.create_snap(snap)
            volume.protect_snap(snap)

    def delete_snapshot(self, snapshot):
        """Deletes an rbd snapshot."""
        # NOTE(dosaboy): this was broken by commit cbe1d5f. Ensure names are
        #                utf-8 otherwise librbd will barf.
        volume_name = quarry_common.convert_str(snapshot.volume_name)
        snap_name = quarry_common.convert_str(snapshot.name)

        with RBDVolumeProxy(self, volume_name) as volume:
            try:
                volume.unprotect_snap(snap_name)
            except self.rbd.InvalidArgument:
                logging.info(
                    "InvalidArgument: Unable to unprotect snapshot %s.",
                    snap_name)
            except self.rbd.ImageNotFound:
                logging.info(
                    "ImageNotFound: Unable to unprotect snapshot %s.",
                    snap_name)
            except self.rbd.ImageBusy:
                children_list = self._get_children_info(volume, snap_name)

                if children_list:
                    for (pool, image) in children_list:
                        logging.info('Image %(pool)s/%(image)s is dependent '
                                     'on the snapshot %(snap)s.',
                                     {'pool': pool,
                                      'image': image,
                                      'snap': snap_name})

                raise quarry_common.SnapshotIsBusy(snap_name)
            try:
                logging.info("Removing snapshot %s of volume %s",
                             snap_name, volume_name)
                volume.remove_snap(snap_name)
            except self.rbd.ImageNotFound:
                logging.info("Snapshot %s does not exist in backend.",
                             snap_name)

    def _disable_replication(self, volume):
        """Disable replication on the given volume."""
        vol_name = quarry_common.convert_str(volume.name)
        with RBDVolumeProxy(self, vol_name) as image:
            image.mirror_image_disable(False)
            driver_data = json.loads(volume.replication_driver_data)
            # If we didn't have journaling enabled when we enabled replication
            # we must remove journaling since it we added it for the
            # replication
            if not driver_data['had_journaling']:
                image.update_features(self.rbd.RBD_FEATURE_JOURNALING, False)
        return {'replication_status': quarry_common.ReplicationStatus.DISABLED,
                'replication_driver_data': None}

    def retype(self, context, volume, new_type, diff, host):
        """Retype from one volume type to another on the same backend."""
        old_vol_replicated = self._is_replicated_type(volume.volume_type)
        new_vol_replicated = self._is_replicated_type(new_type)

        if old_vol_replicated and not new_vol_replicated:
            try:
                return True, self._disable_replication(volume)
            except Exception:
                err_msg = ('Failed to disable image replication for volume '
                           '%s' % volume.id)

                raise quarry_common.ReplicationError(err_msg)
        elif not old_vol_replicated and new_vol_replicated:
            try:
                return True, self._enable_replication(volume)
            except Exception:
                err_msg = ('Failed to enable image replication for volume '
                           '%s' % volume.id)
                raise quarry_common.ReplicationError(err_msg)

        if not new_vol_replicated and self._is_replication_enabled:
            update = {'replication_status':
                          quarry_common.ReplicationStatus.DISABLED}
        else:
            update = None
        return True, update

    def _dumps(self, obj):
        return json.dumps(obj, separators=(',', ':'))

    def _exec_on_volume(self, volume_name, remote, operation, *args, **kwargs):
        @quarry_common.retry(rbd.ImageBusy,
                             self.configuration['rados_connection_interval'],
                             self.configuration['rados_connection_retries'])
        def _do_exec():
            timeout = self.configuration['replication_connect_timeout']
            with RBDVolumeProxy(self, volume_name,
                                self.configuration['rbd_pool'],
                                remote=remote, timeout=timeout) as rbd_image:
                return getattr(rbd_image, operation)(*args, **kwargs)
        return _do_exec()

    def _failover_volume(self, volume, remote, is_demoted, replication_status):
        """Process failover for a volume.

        There are 3 different cases that will return different update values
        for the volume:

        - Volume has replication enabled and failover succeeded: Set
          replication status to failed-over.
        - Volume has replication enabled and failover fails: Set status to
          error, replication status to failover-error, and store previous
          status in previous_status field.
        - Volume replication is disabled: Set status to error, and store
          status in previous_status field.
        """
        # Failover is allowed when volume has it enabled or it has already
        # failed over, because we may want to do a second failover.
        if self._is_replicated_type(volume.volume_type):
            vol_name = quarry_common.convert_str(volume.name)
            try:
                self._exec_on_volume(vol_name, remote,
                                     'mirror_image_promote', not is_demoted)

                return {'volume_id': volume.id,
                        'updates': {'replication_status': replication_status}}
            except Exception as e:
                replication_status = \
                    quarry_common.ReplicationStatus.FAILOVER_ERROR
                logging.error('Failed to failover volume %(volume)s with '
                              'error: %(error)s.',
                              {'volume': volume.name, 'error': e})
        else:
            replication_status = quarry_common.ReplicationStatus.NOT_CAPABLE
            logging.debug('Skipping failover for non replicated volume '
                          '%(volume)s with status: %(status)s',
                          {'volume': volume.name, 'status': volume.status})

        # Failover did not happen
        error_result = {
            'volume_id': volume.id,
            'updates': {
                'status': 'error',
                'previous_status': volume.status,
                'replication_status': replication_status
            }
        }

        return error_result

    def _demote_volumes(self, volumes, until_failure=True):
        """Try to demote volumes on the current primary cluster."""
        result = []
        try_demoting = True
        for volume in volumes:
            demoted = False
            if try_demoting and self._is_replicated_type(volume.volume_type):
                vol_name = quarry_common.convert_str(volume.name)
                try:
                    self._exec_on_volume(vol_name, self._active_config,
                                         'mirror_image_demote')
                    demoted = True
                except Exception as e:
                    logging.debug('Failed to demote %(volume)s with error: '
                                  '%(error)s.',
                                  {'volume': volume.name, 'error': e})
                    try_demoting = not until_failure
            result.append(demoted)
        return result

    def _get_failover_target_config(self, secondary_id=None):
        if not secondary_id:
            # In auto mode exclude failback and active
            candidates = set(self._target_names).difference(
                ('default', self._active_backend_id))
            if not candidates:
                raise quarry_common.InvalidReplicationTarget(
                    'RBD: No available failover target host.')
            secondary_id = candidates.pop()
        return secondary_id, self._get_target_config(secondary_id)

    def failover_host(self, context, volumes, secondary_id=None):
        """Failover to replication target."""
        logging.info('RBD driver failover started.')
        if not self._is_replication_enabled:
            raise quarry_common.UnableToFailOver(
                'RBD: Replication is not enabled.')

        if secondary_id == 'default':
            replication_status = quarry_common.ReplicationStatus.ENABLED
        else:
            replication_status = quarry_common.ReplicationStatus.FAILED_OVER

        secondary_id, remote = self._get_failover_target_config(secondary_id)

        # Try to demote the volumes first
        demotion_results = self._demote_volumes(volumes)
        # Do the failover taking into consideration if they have been demoted
        updates = [self._failover_volume(volume, remote, is_demoted,
                                         replication_status)
                   for volume, is_demoted in zip(volumes, demotion_results)]
        self._active_backend_id = secondary_id
        self._active_config = remote
        logging.info('RBD driver failover completed.')
        return secondary_id, updates

    def ensure_export(self, context, volume):
        """Synchronously recreates an export for a logical volume."""
        pass

    def create_export(self, context, volume, connector):
        """Exports the volume."""
        pass

    def remove_export(self, context, volume):
        """Removes an export for a logical volume."""
        pass

    def initialize_connection(self, volume, connector):
        hosts, ports = self._get_mon_addrs()
        data = {
            'driver_volume_type': 'rbd',
            'data': {
                'name': '%s/%s' % (self.configuration['rbd_pool'],
                                   volume.name),
                'hosts': hosts,
                'ports': ports,
                'cluster_name': self.configuration['rbd_cluster_name'],
                'auth_enabled': (self.configuration['rbd_user'] is not None),
                'auth_username': self.configuration['rbd_user'],
                'secret_type': 'ceph',
                'secret_uuid': self.configuration['rbd_secret_uuid'],
                'volume_id': volume.id,
            }
        }
        logging.debug('connection data: %s', data)
        return data

    def terminate_connection(self, volume, connector, **kwargs):
        pass

    def _parse_location(self, location):
        prefix = 'rbd://'
        if not location.startswith(prefix):
            msg = 'Location %s not stored in rbd' % location
            raise quarry_common.ImageUnacceptable(msg)
        pieces = [urllib.parse.unquote(loc)
                  for loc in location[len(prefix):].split('/')]
        if any(map(lambda p: p == '', pieces)):
            msg = 'Location %s contains blank components' % location
            raise quarry_common.ImageUnacceptable(msg)
        if len(pieces) != 4:
            msg = 'Location %s is not an rbd snapshot' % location
            raise quarry_common.ImageUnacceptable(msg)
        return pieces

    def _get_fsid(self):
        with RADOSClient(self) as client:
            return client.cluster.get_fsid()

    def _is_cloneable(self, image_location, image_meta):
        try:
            fsid, pool, image, snapshot = self._parse_location(image_location)
        except quarry_common.ImageUnacceptable as e:
            logging.debug('not cloneable: %s.', e)
            return False

        if self._get_fsid() != fsid:
            logging.debug('%s is in a different ceph cluster.', image_location)
            return False

        if image_meta['disk_format'] != 'raw':
            logging.debug("rbd image clone requires image format to be "
                          "'raw' but image %(image)s is '%(format)s'",
                          {"image": image_location,
                           "format": image_meta['disk_format']})
            return False

        # check that we can read the image
        try:
            with RBDVolumeProxy(self, image,
                                pool=pool,
                                snapshot=snapshot,
                                read_only=True):
                return True
        except self.rbd.Error as e:
            logging.debug('Unable to open image %(loc)s: %(err)s.',
                          dict(loc=image_location, err=e))
            return False

    def clone_image(self, context, volume,
                    image_location, image_meta,
                    image_service):
        if image_location:
            # Note: image_location[0] is glance image direct_url.
            # image_location[1] contains the list of all locations (including
            # direct_url) or None if show_multiple_locations is False in
            # glance configuration.
            if image_location[1]:
                url_locations = [location['url'] for
                                 location in image_location[1]]
            else:
                url_locations = [image_location[0]]

            # iterate all locations to look for a cloneable one.
            for url_location in url_locations:
                if url_location and self._is_cloneable(
                    url_location, image_meta):
                    _prefix, pool, image, snapshot = \
                        self._parse_location(url_location)
                    volume_update = self._clone(volume, pool, image, snapshot)
                    volume_update['provider_location'] = None
                    self._resize(volume)
                    return volume_update, True
        return ({}, False)

    def _image_conversion_dir(self):
        tmpdir = (self.configuration['image_conversion_dir'] or
                  tempfile.gettempdir())

        # ensure temporary directory exists
        if not os.path.exists(tmpdir):
            os.makedirs(tmpdir)

        return tmpdir

    def copy_image_to_volume(self, context, volume, image_service, image_id):

        tmp_dir = self._image_conversion_dir()

        with tempfile.NamedTemporaryFile(dir=tmp_dir) as tmp:
            image_utils.fetch_to_raw(context, image_service, image_id,
                                     tmp.name,
                                     self.configuration['volume_dd_blocksize'],
                                     size=volume.size)

            self.delete_volume(volume)

            chunk_size = (self.configuration['rbd_store_chunk_size'] *
                          quarry_common.MB)
            order = int(math.log(chunk_size, 2))
            # keep using the command line import instead of librbd since it
            # detects zeroes to preserve sparseness in the image
            args = ['rbd', 'import',
                    '--pool', self.configuration['rbd_pool'],
                    '--order', order,
                    tmp.name, volume.name,
                    '--new-format']
            args.extend(self._ceph_args())
            self._try_execute(*args)
        self._resize(volume)
        # We may need to re-enable replication because we have deleted the
        # original image and created a new one using the command line import.
        try:
            self._enable_replication_if_needed(volume)
        except Exception:
            err_msg = ('Failed to enable image replication for volume '
                       '%s' % volume.id)
            raise quarry_common.ReplicationError(err_msg)

    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        tmp_dir = self._image_conversion_dir()
        tmp_file = os.path.join(tmp_dir,
                                volume.name + '-' + image_meta['id'])
        with fileutils.remove_path_on_error(tmp_file):
            args = ['rbd', 'export',
                    '--pool', self.configuration['rbd_pool'],
                    volume.name, tmp_file]
            args.extend(self._ceph_args())
            self._try_execute(*args)
            image_utils.upload_volume(context, image_service,
                                      image_meta, tmp_file)
        os.unlink(tmp_file)

    def backup_volume(self, context, backup, backup_service):
        """Create a new backup from an existing volume."""
        volume = self.db.volume_get(context, backup.volume_id)

        with RBDVolumeProxy(self, volume.name,
                            self.configuration['rbd_pool']) as rbd_image:
            rbd_meta = linuxrbd.RBDImageMetadata(
                rbd_image, self.configuration['rbd_pool'],
                self.configuration['rbd_user'],
                self.configuration['rbd_ceph_conf'])
            rbd_fd = linuxrbd.RBDVolumeIOWrapper(rbd_meta)
            backup_service.backup(backup, rbd_fd)

        logging.debug("volume backup complete.")

    def restore_backup(self, context, backup, volume, backup_service):
        """Restore an existing backup to a new or existing volume."""
        with RBDVolumeProxy(self, volume.name,
                            self.configuration['rbd_pool']) as rbd_image:
            rbd_meta = linuxrbd.RBDImageMetadata(
                rbd_image, self.configuration['rbd_pool'],
                self.configuration['rbd_user'],
                self.configuration['rbd_ceph_conf'])
            rbd_fd = linuxrbd.RBDVolumeIOWrapper(rbd_meta)
            backup_service.restore(backup, volume.id, rbd_fd)

        logging.debug("volume restore complete.")

    def extend_volume(self, volume, new_size):
        """Extend an existing volume."""
        old_size = volume.size

        try:
            size = int(new_size) * quarry_common.GB
            self._resize(volume, size=size)
        except Exception:
            msg = ('Failed to Extend Volume %(volname)s' %
                   {'volname': volume.name})
            logging.error(msg)
            raise quarry_common.VolumeBackendAPIException(msg)

        logging.debug("Extend volume from %(old_size)s GB to %(new_size)s GB.",
                      {'old_size': old_size, 'new_size': new_size})

    def manage_existing(self, volume, existing_ref):
        """Manages an existing image.

        Renames the image name to match the expected name for the volume.
        Error checking done by manage_existing_get_size is not repeated.

        :param volume:
            volume ref info to be set
        :param existing_ref:
            existing_ref is a dictionary of the form:
            {'source-name': <name of rbd image>}
        """
        # Raise an exception if we didn't find a suitable rbd image.
        with RADOSClient(self) as client:
            rbd_name = existing_ref['source-name']
            self.RBDProxy().rename(client.ioctx,
                                   quarry_common.convert_str(rbd_name),
                                   quarry_common.convert_str(volume.name))

    def manage_existing_get_size(self, volume, existing_ref):
        """Return size of an existing image for manage_existing.

        :param volume:
            volume ref info to be set
        :param existing_ref:
            existing_ref is a dictionary of the form:
            {'source-name': <name of rbd image>}
        """

        # Check that the reference is valid
        if 'source-name' not in existing_ref:
            msg = ('Reference %s must contain source-name element.' %
                   existing_ref)
            raise quarry_common.ManageExistingInvalidReference(msg)

        rbd_name = quarry_common.convert_str(existing_ref['source-name'])

        with RADOSClient(self) as client:
            # Raise an exception if we didn't find a suitable rbd image.
            try:
                rbd_image = self.rbd.Image(client.ioctx, rbd_name)
            except self.rbd.ImageNotFound:
                msg = ("Specified rbd image does not exist. existing_ref=%s",
                       existing_ref)
                raise quarry_common.ManageExistingInvalidReference(msg)

            image_size = rbd_image.size()
            rbd_image.close()

            # RBD image size is returned in bytes.  Attempt to parse
            # size as a float and round up to the next integer.
            try:
                convert_size = int(math.ceil(float(image_size) /
                                             quarry_common.GB))
                return convert_size
            except ValueError:
                msg = ("Failed to manage existing volume %s, because "
                       "reported size %s was not a floating-point number."
                       % (rbd_name, image_size))
                raise quarry_common.VolumeBackendAPIException(msg)

    def unmanage(self, volume):
        pass

    def update_migrated_volume(self, ctxt, volume, new_volume,
                               original_volume_status):
        """Return model update from RBD for migrated volume.

        This method should rename the back-end volume name(id) on the
        destination host back to its original name(id) on the source host.

        :param ctxt: The context used to run the method update_migrated_volume
        :param volume: The original volume that was migrated to this backend
        :param new_volume: The migration volume object that was created on
                           this backend as part of the migration process
        :param original_volume_status: The status of the original volume
        :returns: model_update to update DB with any needed changes
        """
        name_id = None
        provider_location = None

        existing_name = (self.configuration['volume_name_template'] %
                         new_volume.id)
        wanted_name = self.configuration['volume_name_template'] % volume.id
        with RADOSClient(self) as client:
            try:
                self.RBDProxy().rename(client.ioctx,
                                       quarry_common.convert_str(existing_name),
                                       quarry_common.convert_str(wanted_name))
            except self.rbd.ImageNotFound:
                logging.error('Unable to rename the logical volume '
                              'for volume %s.', volume.id)
                # If the rename fails, _name_id should be set to the new
                # volume id and provider_location should be set to the
                # one from the new volume as well.
                name_id = new_volume._name_id or new_volume.id
                provider_location = new_volume['provider_location']
        return {'_name_id': name_id, 'provider_location': provider_location}

    def migrate_volume(self, context, volume, host):
        return (False, None)



class RBDVolumeProxy(object):
    """Context manager for dealing with an existing rbd volume.

    This handles connecting to rados and opening an ioctx automatically, and
    otherwise acts like a librbd Image object.

    The underlying librados client and ioctx can be accessed as the attributes
    'client' and 'ioctx'.
    """
    def __init__(self, driver, name, pool=None, snapshot=None,
                 read_only=False, remote=None, timeout=None):
        client, ioctx = driver._connect_to_rados(pool, remote, timeout)
        if snapshot is not None:
            snapshot = quarry_common.convert_str(snapshot)

        try:
            self.volume = driver.rbd.Image(ioctx,
                                           quarry_common.convert_str(name),
                                           snapshot=snapshot,
                                           read_only=read_only)
        except driver.rbd.Error:
            logging.exception("error opening rbd image %s", name)
            driver._disconnect_from_rados(client, ioctx)
            raise
        self.driver = driver
        self.client = client
        self.ioctx = ioctx

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        try:
            self.volume.close()
        finally:
            self.driver._disconnect_from_rados(self.client, self.ioctx)

    def __getattr__(self, attrib):
        return getattr(self.volume, attrib)


class RADOSClient(object):
    """Context manager to simplify error handling for connecting to ceph."""
    def __init__(self, driver, pool=None):
        self.driver = driver
        self.cluster, self.ioctx = driver._connect_to_rados(pool)

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.driver._disconnect_from_rados(self.cluster, self.ioctx)

    @property
    def features(self):
        features = self.cluster.conf_get('rbd_default_features')
        if ((features is None) or (int(features) == 0)):
            features = self.driver.rbd.RBD_FEATURE_LAYERING
        return int(features)
