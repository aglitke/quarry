import argparse
import cherrypy
import json
import uuid

import utils


class V2Controller(object):

    def index(self):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        return json.dumps({})
        # return json.dumps({"versions": [{"status": "SUPPORTED", "updated": "2014-06-28T12:20:21Z", "links": [{"href": "http://docs.openstack.org/", "type": "text/html", "rel": "describedby"}, {"href": "http://192.168.2.16:8776/v2/", "rel": "self"}], "min_version": "", "version": "", "media-types": [{"base": "application/json", "type": "application/vnd.openstack.volume+json;version=1"}], "id": "v2.0"}]})


class VolumeTypesController(object):

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
        cherrypy.response.headers['Content-type'] = 'application/json'
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

    def _get_volume(self, volume_id):
        # XXX: This is a hack to support ovirt-engine.
        cherrypy.response.headers['Content-Type'] = 'application/json'
        params = dict(
            volume_id=volume_id,
            volume_size=0,
            check_mode='yes',
        )
        params.update(utils.get_base_template_params(None))
        ret = utils.ansible_operation('create_volume', params)
        # XXX: We need a better way to do this...
        state = utils.search_playbook_output(ret, "state")
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
            print "Invalid state: %s" % state
            raise cherrypy.HTTPError(500)

    def _create_volume(self):
        params = dict(
            volume_id=str(uuid.uuid4()),
            volume_size=int(cherrypy.request.json['volume']['size']),
        )
        volume_type = cherrypy.request.json['volume'].get('volume_type')
        params.update(utils.get_base_template_params(volume_type))

        utils.ansible_operation('create_volume', params)
        cherrypy.response.status = 202  # Accepted
        cherrypy.response.headers['Content-Type'] = 'application/json'
        return json.dumps(dict(volume=dict(
            status="creating",
            id=params['volume_id'],
            size=params['volume_size'],
            volume_type=volume_type,
        )))

    def _delete_volume(self, volume_id):
        # XXX: Until we can pass volume_type here we can only support one type
        params = utils.get_base_template_params(None)
        params['volume_id'] = volume_id
        utils.ansible_operation('delete_volume', params)
        cherrypy.response.status = 202  # Accepted


class SnapshotController(object):

    @cherrypy.tools.json_in()
    def create(self, api_ver, tenant_id):
        if cherrypy.request.method.upper() != 'POST':
            raise cherrypy.HTTPError(400, "Query snapshots not supported")

        params = utils.get_base_template_params()
        snapshot_id = str(uuid.uuid4())
        volume_id = cherrypy.request.json['snapshot']['volume_id']
        params['snapshot_id'] = snapshot_id
        params['volume_id'] = volume_id
        utils.ansible_operation('create_snapshot', params)
        cherrypy.response.status = 202  # Accepted
        return json.dumps(dict(volume=dict(
            status="creating",
            id=snapshot_id,
            size=volume_id,
        )))

    def delete(self, api_ver, tenant_id, snapshot_id):
        if cherrypy.request.method.upper() != 'DELETE':
            raise cherrypy.HTTPError(400, "Query snapshot not supported")

        params = utils.get_base_template_params()
        params['volume_id'] = snapshot_id
        utils.ansible_operation('delete_snapshot', params)
        cherrypy.response.status = 202  # Accepted


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
    d.connect('create_snapshot', '/:api_ver/:tenant_id/snapshots',
              controller=SnapshotController(), action='create')
    d.connect('delete_snapshot', '/:api_ver/:tenant_id/snapshots/:snapshot_id',
              controller=SnapshotController(), action='delete')
    dispatcher = d
    return dispatcher


dispatcher_conf = {
    '/': {
        'request.dispatch': setup_routes(),
    }
}


def start():
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

