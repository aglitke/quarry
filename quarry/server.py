#
#  Copyright 2017 Red Hat, Inc. and/or its affiliates.
#
# Licensed to you under the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.  See the files README and
# LICENSE_GPL_v2 which accompany this distribution.
#

import argparse
import cherrypy
import json
import logging
import uuid
from collections import namedtuple

import playcaller
import utils


DiscoveredResource = namedtuple('DiscoveredVolume', 'type,info')


def volume_types():
    return cherrypy.request.app.config['global']['volume_types']


class V2Controller(object):

    def index(self):
        return json.dumps({})


class VolumeTypesController(object):

    def index(self, api_ver, tenant_id):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        result = dict(volume_types=[])
        for i, vol_type in enumerate(volume_types(), start=1):
            entry = dict(
                id="00000000-0000-0000-0000-%012d" % i,
                name=vol_type,
                extra_specs=dict(volume_backend_name=vol_type)
            )
            result['volume_types'].append(entry)
        return json.dumps(result)


class LimitsController(object):

    def index(self, api_ver, tenant_id):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        return json.dumps({
            "limits": {
                "rate": [],
                "absolute": {
                    "totalSnapshotsUsed": 0,
                    "maxTotalBackups": 10,
                    "maxTotalVolumeGigabytes": 1000,
                    "maxTotalSnapshots": 10,
                    "maxTotalBackupGigabytes": 1000,
                    "totalBackupGigabytesUsed": 0,
                    "maxTotalVolumes": 10,
                    "totalVolumesUsed": 0,
                    "totalBackupsUsed": 0,
                    "totalGigabytesUsed": 0
                }
            }
        })


class VolumeController(object):

    @cherrypy.tools.json_in()
    def collection(self, api_ver, tenant_id):
        if cherrypy.request.method.upper() == 'POST':
            return self._create_volume()
        else:
            raise cherrypy.HTTPError(400, "Method not supported")

    @cherrypy.tools.json_in()
    def resource(self, api_ver, tenant_id, volume_id):
        if cherrypy.request.method.upper() == 'DELETE':
            return self._delete_volume(volume_id)
        elif cherrypy.request.method.upper() == 'GET':
            return self._get_volume(volume_id)
        else:
            raise cherrypy.HTTPError(400, "Method not supported")

    @cherrypy.tools.json_in()
    def action(self, api_ver, tenant_id, volume_id):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        if cherrypy.request.method.upper() != 'POST':
            raise cherrypy.HTTPError(400, "POST method expected")
        req = cherrypy.request.json
        if 'os-initialize_connection' in req:
            connector = req['os-initialize_connection']['connector']
            initiator = connector.get('initiator')
            return self._initialize_connection(volume_id, initiator)
        elif 'os-terminate_connection' in req:
            connector = req['os-terminate_connection']['connector']
            initiator = connector.get('initiator')
            return self._terminate_connection(volume_id, initiator)
        raise cherrypy.HTTPError(400, "Action Not implemented")

    def _get_volume(self, volume_id):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        res = find_volume(volume_id)
        return json.dumps(dict(volume=dict(
            status="available",
            attachments=[],
            links=[],
            availability_zone="nova",
            bootable=True,
            description="",
            name="volume-%s" % volume_id,
            volume_type=res.type,
            id=volume_id,
            size=res.info['size'],
            metadata={},
        )))

    def _create_volume(self):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        volume_id = str(uuid.uuid4())
        volume_type = cherrypy.request.json['volume']['volume_type']
        volume_size = cherrypy.request.json['volume']['size']
        source_volid = cherrypy.request.json['volume'].get('source_volid')
        snapshot_id = cherrypy.request.json['volume'].get('snapshot_id')

        if volume_type not in volume_types():
            raise cherrypy.HTTPError(400, "Unsupported volume type")

        params = dict(
            volume_id=volume_id,
            volume_size=volume_size,
            source_volid=source_volid,
            snapshot_id=snapshot_id,
        )

        if source_volid and snapshot_id:
            raise cherrypy.HTTPError(401, "Only one of source_volid and "
                                     "snapshot_id is allowed")

        playcaller.PlayCaller(volume_type, 'create_volume', params).run()
        cherrypy.response.status = 202  # Accepted
        return json.dumps(dict(volume=dict(
            status="creating",
            id=volume_id,
            size=params['volume_size'],
            volume_type=volume_type,
            source_volid=source_volid,
            snapshot_id=snapshot_id,
        )))

    def _delete_volume(self, volume_id):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        res = find_volume(volume_id)
        params = dict(volume_id=volume_id)
        playcaller.PlayCaller(res.type, 'delete_volume', params).run()
        cherrypy.response.status = 202  # Accepted

    def _initialize_connection(self, volume_id, initiator):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        res = find_volume(volume_id)
        params = dict(
            volume_id=volume_id,
            initiator=initiator
        )
        ret = playcaller.PlayCaller(
            res.type, 'initialize_connection', params).run()
        return json.dumps(dict(connection_info=ret['connection_info']))

    def _terminate_connection(self, volume_id, initiator):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        res = find_volume(volume_id)
        params = dict(
            volume_id=volume_id,
            initiator=initiator
        )
        playcaller.PlayCaller(res.type, 'terminate_connection', params).run()


class SnapshotController(object):

    @cherrypy.tools.json_in()
    def collection(self, api_ver, tenant_id):
        if cherrypy.request.method.upper() == 'POST':
            return self._create_snapshot()
        else:
            raise cherrypy.HTTPError(400, "Method not supported")

    @cherrypy.tools.json_in()
    def resource(self, api_ver, tenant_id, snapshot_id):
        if cherrypy.request.method.upper() == 'DELETE':
            return self._delete_snapshot(snapshot_id)
        elif cherrypy.request.method.upper() == 'GET':
            return self._get_snapshot(snapshot_id)
        else:
            raise cherrypy.HTTPError(400, "Method not supported")

    def _get_snapshot(self, snapshot_id):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        res = find_snapshot(snapshot_id)
        return json.dumps(dict(snapshot=dict(
            status="available",
            id=snapshot_id,
            volume_id=res.info['volume_id'],
        )))

    def _create_snapshot(self):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        snapshot_id = str(uuid.uuid4())
        volume_id = cherrypy.request.json['snapshot']['volume_id']
        res = find_volume(volume_id)
        params = dict(
            volume_id=volume_id,
            snapshot_id=snapshot_id
        )

        playcaller.PlayCaller(res.type, 'create_snapshot', params).run()
        cherrypy.response.status = 202  # Accepted
        return json.dumps(dict(snapshot=dict(
            status="creating",
            id=snapshot_id,
            volume_id=volume_id,
        )))

    def _delete_snapshot(self, snapshot_id):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        res = find_snapshot(snapshot_id)
        params = dict(snapshot_id=snapshot_id)
        playcaller.PlayCaller(res.type, 'delete_snapshot', params).run()
        cherrypy.response.status = 202  # Accepted


def find_volume(volume_id):
    for volume_type in volume_types():
        params = dict(
            volume_id=volume_id,
            volume_size=0,
        )
        cherrypy.log("Searching backend %s for volume %s" % (volume_type,
                                                             volume_id))
        ret = playcaller.PlayCaller(volume_type, 'get_volume', params).run()
        if ret['state'] == 'present':
            return DiscoveredResource(volume_type, ret)
    else:
        raise cherrypy.HTTPError(404, "Volume not found")


def find_snapshot(snapshot_id):
    for volume_type in volume_types():
        params = dict(
            snapshot_id=snapshot_id,
            volume_id=None,  # Will be looked up
        )
        cherrypy.log("Searching backend %s for snapshot %s" % (volume_type,
                                                               snapshot_id))
        ret = playcaller.PlayCaller(volume_type, 'get_snapshot', params).run()
        if ret["state"] == 'present':
            return DiscoveredResource(volume_type, ret)
    else:
        raise cherrypy.HTTPError(404, "Snapshot not found")


dispatcher = None


def setup_routes():
    d = cherrypy.dispatch.RoutesDispatcher()
    d.connect('api', '/v2/', controller=V2Controller(), action='index')
    d.connect('volume_types', '/:api_ver/:tenant_id/types',
              controller=VolumeTypesController(), action='index')
    d.connect('limits', '/:api_ver/:tenant_id/limits',
              controller=LimitsController(), action='index')
    d.connect('volume_collection', '/:api_ver/:tenant_id/volumes',
              controller=VolumeController(), action='collection')
    d.connect('volume_resource', '/:api_ver/:tenant_id/volumes/:volume_id',
              controller=VolumeController(), action='resource')
    d.connect('volume_action', '/:api_ver/:tenant_id/volumes/:volume_id/action',
              controller=VolumeController(), action='action')
    d.connect('snap_collection', '/:api_ver/:tenant_id/snapshots',
              controller=SnapshotController(), action='collection')
    d.connect('snap_resource', '/:api_ver/:tenant_id/snapshots/:snapshot_id',
              controller=SnapshotController(), action='resource')
    dispatcher = d
    return dispatcher


dispatcher_conf = {
    '/': {
        'request.dispatch': setup_routes(),
    }
}


def start():
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help="Location of the config file",
                        required=True)
    args = parser.parse_args()
    cherrypy.config.update(args.config)
    app = cherrypy.tree.mount(None, config=args.config)
    app.config.update(dispatcher_conf)
    cherrypy.quickstart(app)


if __name__ == '__main__':
    start()
