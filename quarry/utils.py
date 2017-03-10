from Cheetah.Template import Template
import cherrypy
from contextlib import contextmanager
import copy
import json
import os
import re
import subprocess
import tempfile

import config


KB = 1024
MB = KB * 1024
GB = MB * 1024


class AnsibleError(Exception):
    def __init__(self, rc, out, err):
        super(AnsibleError, self).__init__("Ansible failed (rc:%i)\n"
                                           "stdout\n------\n%s\n\n"
                                           "stderr\n------\n%s\n" %
                                           (rc, out, err))


@contextmanager
def temp_file():
    fd, src = tempfile.mkstemp()
    os.close(fd)
    try:
        yield src
    finally:
        os.unlink(src)


@contextmanager
def make_playbook(name, params):
    template_file = os.path.join(config.template_path, '%s.t' % name)
    with open(template_file) as f:
        template = f.read()
    data = str(Template(template, searchList=[params]))
    print data

    with temp_file() as path:
        with open(path, 'w') as f:
            f.write(data)
        yield path


def validate_volume_type(volume_type):
    types = cherrypy.request.app.config['volume_types'].keys()
    if volume_type not in types:
        raise cherrypy.HTTPError(400, "Unrecognized volume type: %s" %
                                 volume_type)


def get_base_template_params(volume_type):
    validate_volume_type(volume_type)
    params = {}
    params.update(cherrypy.request.app.config['ansible'])
    params['backend'] = \
        cherrypy.request.app.config['volume_types'][volume_type]

    # Convert backend section into a yaml dict
    config_str = ""
    for k,v in cherrypy.request.app.config[volume_type].items():
        config_str += "      %s: %s\n" % (k,v)
    params['config'] = config_str

    return params


def run_playbook(host, playbook):
    cmd = ['ansible-playbook', '-i', '%s,' % host, playbook]
    env = copy.copy(os.environ)
    env['ANSIBLE_ROLES_PATH'] = config.roles_path
    env['ANSIBLE_STDOUT_CALLBACK'] = 'json'
    print (cmd, env['ANSIBLE_ROLES_PATH'])
    p = subprocess.Popen(cmd, shell=False, env=env,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    rc = p.returncode
    if rc != 0:
        raise AnsibleError(rc, out, err)
    print out
    return json.loads(out)['plays'][0]['tasks'][-1]['hosts'][host]


def ansible_operation(op, params):
    with make_playbook(op, params) as playbook:
        host = cherrypy.request.app.config['ansible']['ansible_host']
        try:
            return run_playbook(host, playbook)
        except AnsibleError as e:
            raise cherrypy.HTTPError(500, str(e))
