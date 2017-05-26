# quarry: Provision storage with Ansible

Infrastructure management applications such as virtualization managers
and container orchestrators need to interface with storage services in
order to acquire, access, and manage volumes.  This is a challenging
task due to the diversity of deployed storage technologies.  Further,
there is an increasing desire to leverage the unique features of
storage arrays to maximize performance of data intensive operations
such as cloning and snapshotting.  It is impractical for every
management application to implement interfaces to all storage types
for which support is intended.

Quarry uses Ansible to provide unified provisioning.  Site-specific
configuration is combined with a standard set of playbook templates to
perform standard tasks such as creating and deleting volumes, creating
and deleting snapshots, and connecting a host to a volume.  Due to its
large and expanding community, Ansible is a great place to collaborate
on storage backend plugins.  In addition, quarry provides a mock cinder
API frontend to ease integration with applications that can already
use cinder.

## Quickstart

To begin, determine the storage backend(s) you would like to drive with
quarry.  We currently support ceph, NetApp Data ONTap, and EMC XtremIO.
Gather the configuration details for your installation and create a
configuration file for each type in /etc/quarry/config.  The files
should be named <volume_type>.yml where <volume_type> is an identifier
to use when referring to volumes from this source.  See the example
configuration files provided in the doc/ directory of this repository.

Next, configure the quarry server.  Copy doc/quarry.conf.sample to
/etc/quarry/quarry.conf and edit as appropriate.  Specifically, edit
volume_types so that is is a list of all configured backends you
created in the previous step.  Use the same <volume_type> values in
this list (which may or not be the same as the backend types).

Start the server

    python quarry/server.py -c /etc/quarry/quarry.conf

You can then use the mock cinder API at http://localhost:8776/v2/

## About cinder emulation

Currently the only way to create playbooks and run them with quarry is
by using the mock cinder API.  Quarry implements just enough of the API
to allow for provisioning of volumes and snapshots, and connecting
volumes to hosts.  Thus the following endpoints are available:

**GET /v2** - This is the API root and contains no information.

**GET /v2/:tenant_id/types** - List the different volume types that are
enabled in the global configuration file.

**GET /v2/:tenant_id/limits** - Get mock quota information.  These
values are not enforced.

**POST /v2/:tenant_id/volumes** - Create a volume.

**GET /v2/:tenant_id/volumes/:volume_id** - Get basic volume
information.

**DELETE /v2/:tenant_id/volumes/:volume_id** - Delete a volume.

**POST /v2/:tenant_id/volumes/:volume_id/action** - Perform an action
on a volume:
 - os-initialize_connection - Attach a volume to a host
 - os-terminate_connection - Detach a volume from a host

**POST /v2/:tenant_id/snapshots** - Create a snapshot.

**GET /v2/:tenant_id/snapshots/:snapshot_id** - Get basic snapshot
information.

**DELETE /v2/:tenant_id/snapshots/:snapshot_id** - Delete a snapshot.

For more information about how to use the cinder API (such as the
expected format of requests and responses), please consult the cinder
documentation.

## Supported backends

Currently we support the following storage backends:
 - ceph
 - NetApp Data ONTap
 - EMC ExtremIO
