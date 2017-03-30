import argparse
import cherrypy
import json
import logging
import uuid

import playcaller
import utils


class QuarryController(object):
    def __init__(self):
        cherrypy.response.headers['Content-Type'] = 'application/json'


class V2Controller(QuarryController):

    def index(self):
        return json.dumps({})


class VolumeTypesController(QuarryController):

    def index(self, api_ver, tenant_id):
        types = cherrypy.request.app.config['volume_types'].keys()
        result = dict(volume_types=[])
        for i, vol_type in enumerate(types, start=1):
            entry = dict(
                id="00000000-0000-0000-0000-%012d" % i,
                name=vol_type,
                extra_specs=dict(volume_backend_name=vol_type)
            )
            result['volume_types'].append(entry)
        return json.dumps(result)


class LimitsController(QuarryController):

    def index(self, api_ver, tenant_id):
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


class VolumeController(QuarryController):

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
        backend, params = get_backend_params(get_volume_type(volume_id))
        params.update(dict(
            volume_id=volume_id,
            volume_size=0,
        ))

        ret = playcaller.factory(backend, 'get_volume', params).run()
        state = ret["state"]
        if state == 'present':
            return json.dumps(dict(volume=dict(
                status="available",
                attachments=[],
                links=[],
                availability_zone="nova",
                bootable=True,
                description="",
                name="volume-%s" % volume_id,
                volume_type=cherrypy.request.app.config['volume_types'].keys()[0],
                id=volume_id,
                size=0,
                metadata={},
            )))
        elif state == 'absent':
            raise cherrypy.HTTPError(404, "Volume not found")
        else:
            raise cherrypy.HTTPError(500, "Invalid state: %s" % state)

    def _create_volume(self):
        volume_id = str(uuid.uuid4())
        volume_type = cherrypy.request.json['volume']['volume_type']
        volume_size = cherrypy.request.json['volume']['size']
        source_volid = cherrypy.request.json['volume'].get('source_volid')
        snapshot_id = cherrypy.request.json['volume'].get('snapshot_id')
        backend, params = get_backend_params(volume_type)
        params.update(dict(
            volume_id=volume_id,
            volume_size=volume_size,
            source_volid=source_volid,
            snapshot_id=snapshot_id,
        ))

        if source_volid and snapshot_id:
            raise cherrypy.HTTPError(401, "Only one of source_volid and "
                                     "snapshot_id is allowed")

        playcaller.factory(backend, 'create_volume', params).run()
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
        backend, params = get_backend_params(get_volume_type(volume_id))
        params['volume_id'] = volume_id
        playcaller.factory(backend, 'delete_volume', params).run()
        cherrypy.response.status = 202  # Accepted

    def _initialize_connection(self, volume_id, initiator):
        backend, params = get_backend_params(get_volume_type(volume_id))
        params.update(dict(
            volume_id=volume_id,
            initiator=initiator
        ))
        ret = playcaller.factory(backend, 'initialize_connection', params).run()
        return json.dumps(dict(connection_info=ret['connection_info']))

    def _terminate_connection(self, volume_id, initiator):
        backend, params = get_backend_params(get_volume_type(volume_id))
        params.update(dict(
            volume_id=volume_id,
            initiator=initiator
        ))
        playcaller.factory(backend, 'terminate_connection', params).run()


class SnapshotController(QuarryController):

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
        # XXX: How can we look up the backend type?
        backend, params = get_backend_params(get_volume_type(None))
        params.update(dict(
            snapshot_id=snapshot_id,
            volume_id=None,  # Will be looked up
        ))

        ret = playcaller.factory(backend, 'get_snapshot', params).run()
        state = ret["state"]
        if state == 'present':
            return json.dumps(dict(snapshot=dict(
                status="available",
                id=ret['id'],
                volume_id=ret['volume_id'],
            )))
        elif state == 'absent':
            raise cherrypy.HTTPError(404, "Snapshot not found")
        else:
            raise cherrypy.HTTPError(500, "Invalid state: %s" % state)

    def _create_snapshot(self):
        snapshot_id = str(uuid.uuid4())
        volume_id = cherrypy.request.json['snapshot']['volume_id']
        backend, params = get_backend_params(get_volume_type(volume_id))
        params.update(dict(
            volume_id=volume_id,
            snapshot_id=snapshot_id
        ))

        playcaller.factory(backend, 'create_snapshot', params).run()
        cherrypy.response.status = 202  # Accepted
        return json.dumps(dict(snapshot=dict(
            status="creating",
            id=snapshot_id,
            volume_id=volume_id,
        )))

    def _delete_snapshot(self, snapshot_id):
        # XXX: How can we look up the backend type?
        backend, params = get_backend_params(get_volume_type(None))
        params['snapshot_id'] = snapshot_id
        playcaller.factory(backend, 'create_snapshot', params).run()
        cherrypy.response.status = 202  # Accepted


def get_volume_type(volume_id):
    types = cherrypy.request.app.config['volume_types'].keys()
    if len(types) != 1:
        raise cherrypy.HTTPError(500, "Exactly one backend may be configured")
    return types[0]


def get_backend(volume_type):
    try:
        return cherrypy.request.app.config['volume_types'][volume_type]
    except KeyError:
        raise cherrypy.HTTPError(400, "Unrecognized volume type: %s" %
                                 volume_type)


def get_backend_params(volume_type):
    backend = get_backend(volume_type)
    return backend, dict(config=cherrypy.request.app.config[volume_type])


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
