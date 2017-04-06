#
#  Copyright 2017 Red Hat, Inc. and/or its affiliates.
#
# Licensed to you under the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.  See the files README and
# LICENSE_GPL_v2 which accompany this distribution.
#
# Copyright 2013 OpenStack Foundation

import functools
import logging
import random
import retrying
import six


KB = 1024
MB = KB * 1024
GB = MB * 1024


class Driver(object):
    VERSION = '0.0.1'

    def __init__(self, config):
        pass

    def do_setup(self, context):
        pass

    def get_volume(self, volume):
        raise OperationNotSupported()

    def create_volume(self, volume):
        raise OperationNotSupported()

    def delete_volume(self, volume):
        raise OperationNotSupported()

    def get_snapshot(self, snapshot):
        raise OperationNotSupported()

    def create_snapshot(self, snapshot):
        raise OperationNotSupported()

    def delete_snapshot(self, snapshot):
        raise OperationNotSupported()

    def initialize_connection(self, volume, connector):
        raise OperationNotSupported()

    def terminate_connection(self, volume, connector):
        raise OperationNotSupported()


class DictIface(object):
    def __getitem__(self, item):
        try:
            return getattr(self, item)
        except AttributeError:
            raise KeyError

    def get(self, item, default=None):
        try:
            self.__getitem__(item)
        except KeyError:
            return default


class Volume(DictIface):
    def __init__(self, id, size=None):
        self.id = id
        self.name = 'volume-%s' % id
        self.size = size
        self.volume_type = None  # TODO: Implement
        self.encryption_key_id = None


class Snapshot(DictIface):

    def __init__(self, id, volume_id=None, volume_name=None):
        self.id = id
        self.name = "snapshot-%s" % self.id
        if volume_id is not None:
            self.volume_id = volume_id
            self.volume_name = 'volume-%s' % volume_id
        elif volume_name is not None:
            self.volume_name = volume_name
            self.volume_id = volume_name.split('volume-')[1]


class OperationNotSupported(Exception):
    pass


class ImageUnacceptable(Exception):
    pass


class InvalidConfigurationValue(Exception):
    pass


class InvalidInput(Exception):
    pass


class InvalidReplicationTarget(Exception):
    pass


class ManageExistingInvalidReference(Exception):
    pass


class ReplicationError(Exception):
    pass


class SnapshotIsBusy(Exception):
    pass


class UnableToFailOver(Exception):
    pass


class VolumeBackendAPIException(Exception):
    pass


class VolumeDriverException(Exception):
    pass


class VolumeIsBusy(Exception):
    pass


class VolumeNotFound(Exception):
    pass


class ReplicationStatus(object):
    ERROR = 'error'
    ENABLED = 'enabled'
    DISABLED = 'disabled'
    NOT_CAPABLE = 'not-capable'
    FAILING_OVER = 'failing-over'
    FAILOVER_ERROR = 'failover-error'
    FAILED_OVER = 'failed-over'

    ALL = (ERROR, ENABLED, DISABLED, NOT_CAPABLE, FAILOVER_ERROR, FAILING_OVER,
           FAILED_OVER)


class ConsistencyGroupStatus(object):
    ERROR = 'error'
    AVAILABLE = 'available'
    CREATING = 'creating'
    DELETING = 'deleting'
    DELETED = 'deleted'
    UPDATING = 'updating'
    ERROR_DELETING = 'error_deleting'

    ALL = (ERROR, AVAILABLE, CREATING, DELETING, DELETED,
           UPDATING, ERROR_DELETING)


def convert_str(text):
    """Convert to native string.

    Convert bytes and Unicode strings to native strings:

    * convert to bytes on Python 2:
      encode Unicode using encodeutils.safe_encode()
    * convert to Unicode on Python 3: decode bytes from UTF-8
    """
    if six.PY2:
        return to_utf8(text)
    else:
        if isinstance(text, bytes):
            return text.decode('utf-8')
        else:
            return text


def to_utf8(text):
    if isinstance(text, bytes):
        return text
    elif isinstance(text, six.text_type):
        return text.encode('utf-8')
    else:
        raise TypeError("bytes or Unicode expected, got %s"
                        % type(text).__name__)


def retry(exceptions, interval=1, retries=3, backoff_rate=2,
          wait_random=False):

    def _retry_on_exception(e):
        return isinstance(e, exceptions)

    def _backoff_sleep(previous_attempt_number, delay_since_first_attempt_ms):
        exp = backoff_rate ** previous_attempt_number
        wait_for = interval * exp

        if wait_random:
            random.seed()
            wait_val = random.randrange(interval * 1000.0, wait_for * 1000.0)
        else:
            wait_val = wait_for * 1000.0

        logging.debug("Sleeping for %s seconds", (wait_val / 1000.0))

        return wait_val

    def _print_stop(previous_attempt_number, delay_since_first_attempt_ms):
        delay_since_first_attempt = delay_since_first_attempt_ms / 1000.0
        logging.debug("Failed attempt %s", previous_attempt_number)
        logging.debug("Have been at this for %s seconds",
                  delay_since_first_attempt)
        return previous_attempt_number == retries

    if retries < 1:
        raise ValueError('Retries must be greater than or '
                         'equal to 1 (received: %s). ' % retries)

    def _decorator(f):

        @functools.wraps(f)
        def _wrapper(*args, **kwargs):
            r = retrying.Retrying(retry_on_exception=_retry_on_exception,
                                  wait_func=_backoff_sleep,
                                  stop_func=_print_stop)
            return r.call(f, *args, **kwargs)

        return _wrapper

    return _decorator
