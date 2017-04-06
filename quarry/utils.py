#
#  Copyright 2017 Red Hat, Inc. and/or its affiliates.
#
# Licensed to you under the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.  See the files README and
# LICENSE_GPL_v2 which accompany this distribution.
#

from contextlib import contextmanager
import os
import tempfile


KB = 1024
MB = KB * 1024
GB = MB * 1024


class QuarryError(Exception):
    pass


class AnsibleError(QuarryError):
    def __init__(self, rc, out, err):
        super(AnsibleError, self).__init__("Ansible failed (rc:%i)\n"
                                           "stdout\n------\n%s\n\n"
                                           "stderr\n------\n%s\n" %
                                           (rc, out, err))


class ConfigurationError(QuarryError):
    pass


@contextmanager
def temp_file():
    fd, src = tempfile.mkstemp()
    os.close(fd)
    try:
        yield src
    finally:
        os.unlink(src)
