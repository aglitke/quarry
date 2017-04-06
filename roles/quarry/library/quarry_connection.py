#!/usr/bin/python
#
#  Copyright 2017 Red Hat, Inc. and/or its affiliates.
#
# Licensed to you under the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.  See the files README and
# LICENSE_GPL_v2 which accompany this distribution.
#


DOCUMENTATION = '''
---
module: quarry_connection
author: "Adam Litke (@aglitke)"
version_added: "2.0"
short_description: Quarry volume connection management
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
from ansible.module_utils import quarry_common
from ansible.module_utils.quarry_backends import backends


def _get_connector(mod):
    connector = dict()
    for param in ('initiator',):
        if param in mod.params:
            connector[param] = mod.params[param]
    return connector

def main():
    mod = AnsibleModule(
        argument_spec=dict(
            backend=dict(required=True, choices=backends.keys()),
            config=dict(required=False, type='dict', default={}),
            state=dict(required=False, choices=['present', 'absent'],
                       default='present'),
            volume_id=dict(required=True, type='str'),
            initiator=dict(required=False, type='str')),
        supports_check_mode=False)

    config = mod.params['config']
    file = config.get('log', '/dev/null')
    logging.basicConfig(filename=file, level=logging.DEBUG)

    volume = quarry_common.Volume(mod.params['volume_id'])
    result = dict(changed=True, volume_id=mod.params['volume_id'])

    backend_type = backends[mod.params['backend']]
    driver = backend_type(config)
    driver.do_setup(None)

    connector = _get_connector(mod)
    if mod.params['state'] == 'present':
        ret = driver.initialize_connection(volume, connector)
        result['connection_info'] = ret
    elif mod.params['state'] == 'absent':
        driver.terminate_connection(volume, connector)
    mod.exit_json(**result)


if __name__ == '__main__':
    main()
