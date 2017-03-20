# This file is part of Ansible
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.

from ansible.module_utils import quarry_common


class Driver(object):
    VERSION = '0.0.1'

    def __init__(self, config):
        pass

    def do_setup(self, context):
        raise quarry_common.OperationNotSupported()

    def get_volume(self, volume):
        raise quarry_common.OperationNotSupported()

    def create_volume(self, volume):
        raise quarry_common.OperationNotSupported()

    def delete_volume(self, volume):
        raise quarry_common.OperationNotSupported()

    def get_snapshot(self, snapshot):
        raise quarry_common.OperationNotSupported()

    def create_snapshot(self, snapshot):
        raise quarry_common.OperationNotSupported()

    def delete_snapshot(self, snapshot):
        raise quarry_common.OperationNotSupported()

    def initialize_connection(self, volume, connector):
        raise quarry_common.OperationNotSupported()

    def terminate_connection(self, volume, connector):
        raise quarry_common.OperationNotSupported()
