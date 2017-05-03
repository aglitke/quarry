import copy

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
        pass

    def create_volume(self, volume):
        pass

    def delete_volume(self, volume):
        pass
