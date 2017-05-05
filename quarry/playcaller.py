#
#  Copyright 2017 Red Hat, Inc. and/or its affiliates.
#
# Licensed to you under the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.  See the files README and
# LICENSE_GPL_v2 which accompany this distribution.
#

from Cheetah.Template import Template
from contextlib import contextmanager
import copy
import json
import logging
import os
import subprocess

import config
import utils


class PlayCaller(object):
    log = logging.getLogger('PlayCaller')

    def __init__(self, volume_type, operation, params):
        self.volume_type = volume_type
        self.operation = operation
        self.params = params

    def run(self):
        with self._playbook() as playbook:
            cmd = ['ansible-playbook', playbook]
            env = copy.copy(os.environ)
            env['QUARRY_VOLUME_TYPE'] = self.volume_type
            env['ANSIBLE_ROLES_PATH'] = config.roles_path
            env['ANSIBLE_STDOUT_CALLBACK'] = 'json'
            self.log.debug("Running ansible: %s", cmd)
            p = subprocess.Popen(cmd, shell=False, env=env,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            out, err = p.communicate()
            rc = p.returncode
            if rc != 0:
                raise utils.AnsibleError(rc, out, err)

            # This makes some assumptions:
            # 1. The ansible command is running only one play
            # 2. We always return the result of the last task
            result = json.loads(out)['plays'][0]['tasks'][-1]['hosts']
            hosts = result.keys()
            if len(hosts) != 1:
                raise RuntimeError("Expecting exactly one host in report, got "
                                   "%s" % hosts)
            return result[hosts[0]]

    def _template_name(self):
        return '%s.t' % self.operation

    @contextmanager
    def _playbook(self):
        template_file = os.path.join(config.template_path,
                                     self._template_name())
        with open(template_file) as f:
            template = f.read()
            data = str(Template(template, searchList=[self.params]))
            self.log.debug("Playbook content:\n%s", data)
            with utils.temp_file() as path:
                with open(path, 'w') as f:
                    f.write(data)
                yield path
