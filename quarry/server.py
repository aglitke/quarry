import argparse
import cherrypy
import json
import uuid

import utils


class VolumeController(object):

    @cherrypy.tools.json_in()
    def create(self, api_ver, tenant_id):
        if cherrypy.request.method.upper() != 'POST':
            raise cherrypy.HTTPError(404, "Query volumes not supported")

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
            raise cherrypy.HTTPError(404, "Query volume not supported")

        params = utils.get_base_template_params()
        params['volume_id'] = volume_id
        utils.ansible_operation('delete_volume', params)
        cherrypy.response.status = 202  # Accepted

    @cherrypy.tools.json_in()
    def action(self, api_ver, tenant_id, volume_id):
        if cherrypy.request.method.upper() != 'POST':
            raise cherrypy.HTTPError(404)
        return "Hi %s %s %s" % (api_ver, tenant_id, volume_id)


dispatcher = None


def setup_routes():
    d = cherrypy.dispatch.RoutesDispatcher()
    d.connect('volumes', '/:api_ver/:tenant_id/volumes',
              controller=VolumeController(), action='create')
    d.connect('volumes', '/:api_ver/:tenant_id/volumes/:volume_id',
              controller=VolumeController(), action='delete')
    d.connect('volumes', '/:api_ver/:tenant_id/volumes/:volume_id/action',
              controller=VolumeController(), action='action')
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
