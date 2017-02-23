from Cheetah.Template import Template
import cherrypy
from contextlib import contextmanager
import copy
import os
import subprocess
import tempfile

import config


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


def get_base_template_params():
    params = {}
    params.update(cherrypy.request.app.config['ansible'])

    # Convert backend section into a yaml dict
    config_str = ""
    for k,v in cherrypy.request.app.config['backend'].items():
        config_str += "      %s: %s\n" % (k,v)
    params['config'] = config_str

    return params


def run_playbook(host, playbook):
    cmd = ['ansible-playbook', '-i', '%s,' % host, playbook]
    env = copy.copy(os.environ)
    env['ANSIBLE_ROLES_PATH'] = config.roles_path
    print cmd,  env['ANSIBLE_ROLES_PATH']
    p = subprocess.Popen(cmd, shell=False, env=env,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    rc = p.returncode
    if rc != 0:
        raise AnsibleError(rc, out, err)


def ansible_operation(op, params):
    with make_playbook(op, params) as playbook:
        host = cherrypy.request.app.config['ansible']['ansible_host']
        try:
            run_playbook(host, playbook)
        except AnsibleError as e:
            raise cherrypy.HTTPError(500, str(e))
