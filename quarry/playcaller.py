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


def factory(backend, operation, params):
    if backend in ('nfs', 'rbd', 'xtremio'):
        return PlayCaller(backend, operation, params)
    raise utils.ConfigurationError('Unsupported backend %s' % backend)


class PlayCaller(object):
    log = logging.getLogger('PlayCaller')

    def __init__(self, backend, operation, params):
        self.backend = backend
        self.operation = operation
        self.params = params

    def run(self):
        try:
            host = self.params['config']['ansible_host']
        except KeyError:
            raise utils.ConfigurationError("Missing parameter 'ansible_host'")

        with self._playbook() as playbook:
            cmd = ['ansible-playbook', '-i', '%s,' % host, playbook]
            env = copy.copy(os.environ)
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
            return json.loads(out)['plays'][0]['tasks'][-1]['hosts'][host]

    def _template_name(self):
        return '%s.t' % self.operation

    def _template_params(self):
        ret = dict(backend=self.backend,
                   config_str=self._build_config_str())
        ret.update(self.params)
        return ret

    @contextmanager
    def _playbook(self):
        template_file = os.path.join(config.template_path,
                                     self._template_name())
        with open(template_file) as f:
            template = f.read()
        data = str(Template(template,
                            searchList=[self._template_params()]))
        self.log.debug("Playbook content:\n%s", data)

        with utils.temp_file() as path:
            with open(path, 'w') as f:
                f.write(data)
            yield path

    def _build_config_str(self):
        cfg_str = ""
        for kv_pair in self.params['config'].items():
            cfg_str += "      %s: %s\n" % kv_pair
        return cfg_str
