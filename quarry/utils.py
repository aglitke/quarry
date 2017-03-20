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
