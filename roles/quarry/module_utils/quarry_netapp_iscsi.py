# Copyright 2015 NetApp, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""
Volume driver for NetApp Data ONTAP (C-mode) iSCSI storage.
"""
import os
import re
import logging
from logging.handlers import RotatingFileHandler

from ansible.module_utils import quarry_netapp_zapi as zapi
from ansible.module_utils import quarry_netapp_zapi_errors as zapi_errors


#rotating_file_handler = RotatingFileHandler(
#    '/var/log/netapp.log', maxBytes=50*1024*1024, backupCount=5)
#rotating_file_handler.setFormatter(
#    logging.Formatter('%(levelname)s [%(name)s:%(lineno)s] %(message)s'))

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
#logger.setLevel(logging.DEBUG)
#logger.addHandler(rotating_file_handler)  # 50 MB


class CmodeISCSIDriver(object):
    def __init__(self, hostname, username, password, vserver,
                 api_minor_version, api_major_version, port, server_type,
                 transport_type):
        logger.debug('Init %s', self.__class__.__name__)
        self.server = zapi.NaServer(hostname)
        self.server.set_username(username)
        self.server.set_password(password)
        self.server.set_vserver(vserver)
        self.server.set_api_version(api_minor_version, api_major_version)
        self.server.set_port(port)
        self.server.set_server_type(server_type)
        self.server.set_transport_type(transport_type)

    def create_volume(self, pool_name, volume_name, volume_size):
        """
        Create a new volume.

        :param pool_name: Name of the pool (NetApp Volume)
        :param volume_name: Name of the volume (NetApp LUN)
        :param volume_size: Volume size (in bytes)
        """
        path = '/vol/%s/%s' % (pool_name, volume_name)
        logger.debug('Creating volume %s of size %s', path, volume_size)

        lun_create = zapi.NaElement.create_node_with_children(
            'lun-create-by-size', **{'path': path,
                                     'size': str(volume_size),
                                     'ostype': 'linux'})

        try:
            self.server.invoke_successfully(lun_create, enable_tunneling=True)
        except zapi.NaApiError:
            logger.exception('Error provisioning volume %s of size %s',
                             path, volume_size)
            raise

    def delete_volume(self, pool_name, volume_name):
        """
        Delete existing volume.

        :param pool_name: Name of the pool (NetApp Volume)
        :param volume_name: Name of the volume (NetApp LUN)
        """
        path = '/vol/%s/%s' % (pool_name, volume_name)
        logger.debug('Deleting volume %s', path)

        lun_delete = zapi.NaElement.create_node_with_children(
            'lun-destroy', **{'path': path, 'force': 'true'})

        try:
            self.server.invoke_successfully(lun_delete, enable_tunneling=True)
        except zapi.NaApiError:
            logger.exception('Error deleting volume %s', path)
            raise

    def volumes(self, vserver, pool_name):
        """
        Returns a list of existing volumes (NetApp LUNs).

        :param vserver: Name of vserver
        :param pool_name: Name of the pool (NetApp Volume)

        :return: The existing volumes
        :rtype: list
        """
        logger.debug('Retrieving list of volumes on vserver %s in volume %s',
                     vserver, pool_name)

        luns = []
        tag = None
        while True:
            lun_info = zapi.NaElement('lun-get-iter')
            if tag:
                lun_info.add_new_child('tag', tag, True)

            query_details = zapi.NaElement('lun-info')
            query_details.add_new_child('vserver', vserver)
            query_details.add_new_child('volume', pool_name)

            query = zapi.NaElement('query')
            query.add_child_elem(query_details)

            lun_info.add_child_elem(query)

            result = self.server.invoke_successfully(lun_info, True)
            if (result.get_child_by_name('num-records')
                    and int(result.get_child_content('num-records')) >= 1):
                attr_list = result.get_child_by_name('attributes-list')
                luns.extend(attr_list.get_children())

            tag = result.get_child_content('next-tag')

            if tag is None:
                break

        logger.debug('Found the following LUNs: %s', luns)

        volumes = []

        # The LUNs have been extracted. Get the name, size, attached_to,
        # and lun_id (if attached) for every LUN.
        for lun in luns:
            path = lun.get_child_content('path')
            _rest, _splitter, name = path.rpartition('/')
            size = lun.get_child_content('size')

            # Find out if the lun is attached
            attached_to = None
            lun_id = None
            if lun.get_child_content('mapped') == 'true':
                lun_map_list = zapi.NaElement.create_node_with_children(
                    'lun-map-list-info', **{'path': path})

                result = self.server.invoke_successfully(
                    lun_map_list, enable_tunneling=True)

                igroups = result.get_child_by_name('initiator-groups')
                if igroups:
                    for igroup_info in igroups.get_children():
                        igroup = igroup_info.get_child_content(
                            'initiator-group-name')
                        attached_to = igroup
                        lun_id = igroup_info.get_child_content('lun-id')

            volume = {
                'blockdevice_id': name,
                'size': size,
                'attached_to': attached_to,
                'lun_id': lun_id
            }
            volumes.append(volume)
        return volumes

    def attach_volume(self, pool_name, volume_name, igroup_name):
        """
        Maps the volume (NetApp LUN) to the initiator group.

        :param pool_name: Name of the pool (NetApp Volume)
        :param volume_name: Name of the volume (NetApp LUN)
        :param igroup_name: Name of the IGroup
        """
        path = '/vol/%s/%s' % (pool_name, volume_name)
        logger.debug('Attaching volume %s', path)

        self._add_igroup(igroup_name)

        # TODO: Extract the initiator ID and add it to the IGroup
        # TODO: This must be done manually right now

        lun_map = zapi.NaElement.create_node_with_children(
            'lun-map', **{'path': path,
                          'initiator-group': igroup_name})

        try:
            self.server.invoke_successfully(lun_map, True)
        except zapi.NaApiError:
            logger.exception('Error attaching volume %s', path)
            raise

    def _add_igroup(self, igroup_name):
        """
        Add the IGroup to the controller.

        Does not raise an error if IGroup already exists.

        :param igroup_name: Name of the IGroup
        """
        logger.debug('Adding igroup %s', igroup_name)
        igroup_create = zapi.NaElement.create_node_with_children(
            'igroup-create', **{'initiator-group-name': igroup_name,
                                'ostype': 'linux',
                                'initiator-group-type': 'iscsi'})

        try:
            self.server.invoke_successfully(igroup_create, True)
        except zapi.NaApiError as e:
            if str(e.code) != zapi_errors.EVDISK_ERROR_INITGROUP_EXISTS:
                logger.exception('Error creating IGroup %s', igroup_name)
                raise
            logger.debug('IGroup %s already added', igroup_name)

        self._add_initiator(igroup_name=igroup_name)

    def _add_initiator(self, igroup_name):
        """
        Add the initiator to the igroup.

        Does not raise an error if initiator already exists.

        :param igroup_name: Name of the IGroup to add the initiator to
        """
        # TODO: Add cache to prevent unnecessary ZAPI calls
        initiator = ""
        INITIATOR_FILE = "/etc/iscsi/initiatorname.iscsi"
        # TODO: os.popen is depracated, use subprocess instead
        iscsin = os.popen('cat %s' % INITIATOR_FILE).read()
        match = re.search('InitiatorName=.*', iscsin)
        if len(match.group(0)) > 13:
            initiator = match.group(0)[14:]

        igroup_add = zapi.NaElement.create_node_with_children(
            'igroup-add', **{'initiator-group-name': igroup_name,
                             'initiator': str(initiator)})
        try:
            self.server.invoke_successfully(igroup_add, True)
        except zapi.NaApiError as e:
            if str(e.code) != zapi_errors.EVDISK_ERROR_INITGROUP_HAS_NODE:
                logger.exception('Error adding initiator to IGroup %s',
                                 igroup_name)
                raise
            logger.debug('Initiator %s already exists', igroup_name)

    def detach_volume(self, pool_name, volume_name, igroup_name):
        """
        Un-maps the LUN from the initiator.

        Does not raise an error if the LUN is already unmapped.

        :param pool_name: Name of the pool (NetApp Volume)
        :param volume_name: Name of the volume (NetApp LUN)
        :param igroup_name: Name of the IGroup
        """
        path = '/vol/%s/%s' % (pool_name, volume_name)
        logger.debug('Detaching volume %s', path)

        lun_unmap = zapi.NaElement.create_node_with_children(
            'lun-unmap', **{'path': path, 'initiator-group': igroup_name})
        try:
            output = self.server.invoke_successfully(lun_unmap, True)
            status = output.get_attr('status')
            if status and status != 'passed':
                logger.warn('Possible error detaching volume: %s', status)

        except zapi.NaApiError as e:
            if e.code not in [zapi_errors.EINVALIDINPUTERROR,
                              zapi_errors.EVDISK_ERROR_NO_SUCH_LUNMAP]:
                logger.exception('Error detaching volume %s', path)
                raise
            logger.debug('The LUN %s is already unmapped', path)

    def _get_lun_id(self, blockdevice_id, vserver, pool_name):
        """
        Get the lun id.

        :param blockdevice_id: Name of the volume (NetApp LUN)
        :param vserver: Name of vserver
        :param pool_name: Name of the pool (NetApp Volume)

        :return: The LUN ID
        """
        logger.debug('Getting the LUN ID for volume %s vserver %s pool '
                     'name %s', blockdevice_id, vserver, pool_name)
        volumes = self.volumes(vserver=vserver, pool_name=pool_name)
        for volume in volumes:
            if volume['blockdevice_id'] == blockdevice_id:
                return volume['lun_id']
