#!/usr/bin/python
#
#  Copyright 2017 Red Hat, Inc. and/or its affiliates.
#
# Licensed to you under the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.  See the files README and
# LICENSE_GPL_v2 which accompany this distribution.
#

from contextlib import contextmanager
import fcntl
import logging
import os
import subprocess

from ansible.module_utils import quarry_common


MOUNT_DIR = '/run/quarry_nfs'


class Driver(quarry_common.Driver):
    VERSION = '0.0.1'

    def __init__(self, config):
        self.host = config['nfs_host']
        self.export = config['nfs_export']
        self.mountpoint = os.path.join(MOUNT_DIR,
                                       self.export.replace('/', '_'))
        self.refcount = self.mountpoint + '.count'

    def do_setup(self, context):
        if not os.path.exists(self.mountpoint):
            os.makedirs(self.mountpoint)
        with open(self.refcount, 'a'):
            pass

    def get_volume(self, volume):
        vol_file = os.path.join(self.mountpoint, volume.name)
        with self.mounted():
            if os.path.exists(vol_file):
                size = os.stat(vol_file).st_size / quarry_common.GB
                return quarry_common.Volume(volume.id, size)
            else:
                return None

    def create_volume(self, volume):
        vol_file = os.path.join(self.mountpoint, volume.name)
        with self.mounted():
            fd = os.open(vol_file, os.O_CREAT | os.O_RDWR)
            try:
                os.ftruncate(fd, volume.size * quarry_common.GB)
            finally:
                os.close(fd)

    def delete_volume(self, volume):
        vol_file = os.path.join(self.mountpoint, volume.name)
        with self.mounted():
            os.unlink(vol_file)

    def get_snapshot(self, snapshot):
        raise quarry_common.OperationNotSupported()

    def create_snapshot(self, snapshot):
        raise quarry_common.OperationNotSupported()

    def delete_snapshot(self, snapshot):
        raise quarry_common.OperationNotSupported()

    def initialize_connection(self, volume, connector):
        self._mount()
        data = {
            'driver_volume_type': 'nfs',
            'data': {
                'name': volume.name,
                'volume_id': volume.id,
                'volume_path': os.path.join(self.mountpoint, volume.name)
            }
        }
        return data

    def terminate_connection(self, volume, connector):
        self._unmount()

    def _is_mounted(self):
        logging.debug("looking for %s:%s %s", self.host, self.export, self.mountpoint)
        with open('/proc/mounts') as f:
            for line in f.read().splitlines():
                dev, mountpoint, unused = line.split(None, 2)
                logging.debug(line)
                logging.debug("Got: %s %s", dev, mountpoint)
                if dev != '%s:%s' % (self.host, self.export):
                    continue
                if mountpoint != self.mountpoint:
                    continue
                logging.debug("Found mount")
                return True
            logging.debug("Mount not found")
            return False

    def _lock(self):
        with open(self.refcount) as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                count = int(f.read())
            except ValueError:
                count = 0
        logging.debug("Locking mount %s with starting refcount %i",
                      self.mountpoint, count)
        return count

    def _unlock(self, count):
        logging.debug("Unlocking mount %s with new refcount %i",
                      self.mountpoint, count)
        with open(self.refcount, 'w') as f:
            f.write(str(count))
            fcntl.flock(f, fcntl.LOCK_UN)

    def _mount(self):
        count = self._lock()
        if not self._is_mounted():
            cmd = 'mount -tnfs %s:%s %s' % (
                  self.host, self.export, self.mountpoint)
            logging.debug("Mounting: %s", cmd)
            out = subprocess.check_output(cmd, shell=True)
            logging.debug(out)
        self._unlock(count + 1)

    def _unmount(self):
        count = self._lock()
        if count <= 1 and self._is_mounted():
            cmd = 'umount %s' % self.mountpoint
            out = subprocess.check_output(cmd, shell=True)
            logging.debug(out)
        self._unlock(count - 1)

    @contextmanager
    def mounted(self):
        self._mount()
        try:
            yield
        finally:
            self._unmount()