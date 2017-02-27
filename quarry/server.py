import argparse
import cherrypy
import json
import uuid

import utils


class V2Controller(object):

    def index(self):
        return json.dumps({})


class VolumeController(object):

    @cherrypy.tools.json_in()
    def create(self, api_ver, tenant_id):
        if cherrypy.request.method.upper() != 'POST':
            raise cherrypy.HTTPError(400, "Query volumes not supported")

        params = utils.get_base_template_params()
        volume_id = str(uuid.uuid4())
        volume_size = int(cherrypy.request.json['volume']['size'])
        params['volume_id'] = volume_id
        params['volume_size'] = volume_size
        utils.ansible_operation('create_volume', params)
        cherrypy.response.status = 202  # Accepted
        return json.dumps(dict(volume=dict(
            status="creating",
            id=volume_id,
            size=volume_size,
        )))

    def delete(self, api_ver, tenant_id, volume_id):
        if cherrypy.request.method.upper() != 'DELETE':
            raise cherrypy.HTTPError(400, "Query volume not supported")

        params = utils.get_base_template_params()
        params['volume_id'] = volume_id
        utils.ansible_operation('delete_volume', params)
        cherrypy.response.status = 202  # Accepted

    @cherrypy.tools.json_in()
    def action(self, api_ver, tenant_id, volume_id):
        if cherrypy.request.method.upper() != 'POST':
            raise cherrypy.HTTPError(400)
        return "Hi %s %s %s" % (api_ver, tenant_id, volume_id)


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
    d.connect('create_volume', '/:api_ver/:tenant_id/volumes',
              controller=VolumeController(), action='create')
    d.connect('delete_volume', '/:api_ver/:tenant_id/volumes/:volume_id',
              controller=VolumeController(), action='delete')
    d.connect('volume_action', '/:api_ver/:tenant_id/volumes/:volume_id/action',
              controller=VolumeController(), action='action')
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
    # start(os.path.join(os.path.dirname(__file__), 'quarry.conf'))
