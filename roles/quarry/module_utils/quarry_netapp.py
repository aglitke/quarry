import copy
import math

from ansible.module_utils import quarry_common
from ansible.module_utils import quarry_netapp_iscsi


CONFIG_DEFAULTS = {
    'api_minor_version': 1,
    'api_major_version': 21,
    'port': 80,
    'server_type': 'FILER',
    'transport_type': 'HTTP',
}


class Driver(quarry_common.Driver):
    def __init__(self, config, **kwargs):
        self.configuration = copy.copy(CONFIG_DEFAULTS)
        self.configuration.update(config)
        self.vserver = self.configuration['vserver']
        self.pool = self.configuration['pool']
        self._driver = quarry_netapp_iscsi.CmodeISCSIDriver(
            self.configuration['hostname'],
            self.configuration['username'],
            self.configuration['password'],
            self.configuration['vserver'],
            self.configuration['api_minor_version'],
            self.configuration['api_major_version'],
            self.configuration['port'],
            self.configuration['server_type'],
            self.configuration['transport_type'])

    def do_setup(self, context):
        pass

    def get_volume(self, volume):
        volumes = self._driver.volumes(self.vserver, self.pool)
        for v in volumes:
            if v['blockdevice_id'] == volume.name:
                size = int(math.ceil(float(v['size']) / quarry_common.GB))
                return quarry_common.Volume(volume.id, size=size)
        return None

    def create_volume(self, volume):
        size = volume.size * quarry_common.GB
        self._driver.create_volume(self.pool, volume.name, size)

    def delete_volume(self, volume):
        self._driver.delete_volume(self.pool, volume.name)
