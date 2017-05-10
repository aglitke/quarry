#
#  Copyright 2017 Red Hat, Inc. and/or its affiliates.
#
# Licensed to you under the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.  See the files README and
# LICENSE_GPL_v2 which accompany this distribution.
#

from ansible.module_utils import \
    quarry_netapp, \
    quarry_nfs, \
    quarry_rbd, \
    quarry_xtremio

backends = dict(
    nfs=quarry_nfs.Driver,
    rbd=quarry_rbd.Driver,
    xtremio=quarry_xtremio.ISCSIDriver,
    netapp=quarry_netapp.Driver,
)
