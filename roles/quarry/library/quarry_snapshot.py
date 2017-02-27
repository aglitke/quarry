#!/usr/bin/python
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

DOCUMENTATION = '''
---
module: quarry_snapshot
author: "Adam Litke (@aglitke)"
version_added: "2.0"
short_description: Quarry Volume Provisioning
options:
  backend:
    description:
      - The backend storage driver to use for the requested operation
    required: true
  config:
    description:
      - A dictionary of backend-specific configuration parameters.  See the
        backend documentation for more information.
    required: false
'''

EXAMPLES = '''

'''

import logging

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils import quarry_rbd

BACKENDS = {'rbd': quarry_rbd.Driver}


class Snapshot(object):

    def __init__(self, id, volume_id=None):
        self.id = id
        self.name = "snapshot-%s" % self.id
        self.volume_id = volume_id


def main():
    mod = AnsibleModule(
        argument_spec=dict(
            backend=dict(required=True, choices=BACKENDS.keys()),
            config=dict(required=False, type='dict', default={}),
            state=dict(required=False, choices=['present', 'absent'],
                       default='present'),
            id=dict(required=True, type='str'),
            volume_id=dict(required=False, type='str')),
        supports_check_mode=False)
    result = dict(changed=False)

    config = mod.params['config']
    file = config.get('log', '/dev/null')
    logging.basicConfig(filename=file, level=logging.DEBUG)

    snapshot = Snapshot(mod.params['id'], mod.params['volume_id'])
    backend_type = BACKENDS[mod.params['backend']]
    driver = backend_type(config)
    driver.do_setup(None)

    present = driver.detect_snapshot(snapshot)
    result['present'] = present
    logging.debug("Snapshot %s %s present", snapshot.id,
                  'is' if present else 'is not')

    state = mod.params['state']
    if not present and state == 'present':
        driver.create_snapshot(snapshot)
        result.update(dict(changed=True, id=snapshot.id,
                           volume_id=snapshot.volume_id))
    elif present and state == 'absent':
        driver.delete_volume(snapshot)
        result.update(dict(changed=True, id=None))

    mod.exit_json(**result)


if __name__ == '__main__':
    main()