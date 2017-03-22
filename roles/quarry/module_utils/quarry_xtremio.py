import json
import logging
import random
import requests
import requests.auth
import string

from ansible.module_utils import quarry_common


XTREMIO_OID_NAME = 1
XTREMIO_OID_INDEX = 2


class Driver(quarry_common.Driver):
    VERSION = '0.0.1'

    def __init__(self, config):
        self.cluster_name = config.get('xtremio_cluster_name')
        self.san_ip = config['san_ip']
        self.san_login = config['san_login']  # TODO: Secure password config
        self.san_pass = config['san_password']
        if config.get('driver_ssl_cert_verify'):
            self.verify = config.get('driver_ssl_cert_path', '/dev/null')
        else:
            self.verify = False
        self.api_base = "https://%s/api/json/v2" % self.san_ip
        self.auth = requests.auth.HTTPBasicAuth(self.san_login, self.san_pass)

    def do_setup(self, context):
        pass

    def get_volume(self, volume):
        uri = "%s/types/volumes" % self.api_base
        ret = self._get(uri, params=dict(name=volume.name))
        size = int(ret['vol-size']) / quarry_common.GB
        return quarry_common.Volume(volume.id, size=size)

    def create_volume(self, volume):
        uri = "%s/types/volumes" % self.api_base
        data = {'vol-name': volume.name, 'vol-size': "%sG" % volume.size}
        ret = self._post(uri, data)
        uri = ret['links'][0]['href']
        return self._get(uri)

    def delete_volume(self, volume):
        uri = "%s/types/volumes" % self.api_base
        self._delete(uri, params=dict(name=volume.name))

    def get_snapshot(self, snapshot):
        pass

    def create_snapshot(self, snapshot):
        pass

    def delete_snapshot(self, snapshot):
        pass

    def initialize_connection(self, volume, connector):
        cluster = self._get("%s/types/clusters/1" % self.api_base)
        login_chap = (cluster.get('chap-authentication-mode', 'disabled') !=
                      'disabled')
        discovery_chap = (cluster.get('chap-discovery-mode', 'disabled') !=
                          'disabled')
        initiator_name = self._get_initiator_names(connector)[0]
        initiator = self._get_initiator(initiator_name)

        if initiator:
            login_passwd = initiator['chap-authentication-initiator-password']
            discovery_passwd = initiator['chap-discovery-initiator-password']
            ig = self._get_ig(initiator['ig-id'][XTREMIO_OID_NAME])
        else:
            ig = self._get_ig(self._get_ig_name(connector))
            if not ig:
                ig = self._create_ig(self._get_ig_name(connector))
            (login_passwd,
             discovery_passwd) = self._create_initiator(connector,
                                                        login_chap,
                                                        discovery_chap)

        # if CHAP was enabled after the initiator was created
        if login_chap and not login_passwd:
            logging.info('initiator has no password while using chap, '
                         'adding it')
            data = {}
            (login_passwd,
             d_passwd) = self._add_auth(data, login_chap, discovery_chap and
                                        not discovery_passwd)
            discovery_passwd = (discovery_passwd if discovery_passwd
                                else d_passwd)
            uri = "%s/types/initiators/%d" % (
                self.api_base, initiator['index'])
            self._put(uri, data=data)

        # lun mappping
        lunmap = self.create_lun_map(volume, ig['ig-id'][XTREMIO_OID_NAME])

        properties = self._get_iscsi_properties(lunmap)

        if login_chap:
            properties['auth_method'] = 'CHAP'
            properties['auth_username'] = 'chap_user'
            properties['auth_password'] = login_passwd
        if discovery_chap:
            properties['discovery_auth_method'] = 'CHAP'
            properties['discovery_auth_username'] = 'chap_user'
            properties['discovery_auth_password'] = discovery_passwd
        logging.debug('init conn params:\n%s', properties)
        return {
            'driver_volume_type': 'iscsi',
            'data': properties
        }

    def terminate_connection(self, volume, connector):
        pass

    def _get_initiator_names(self, connector):
        return [connector['initiator']]

    def _get_ig_name(self, connector):
        return connector['initiator']

    def _get_initiator(self, port_address):
        params = dict(filter='port-address:eq:' + port_address, full=1)
        resp = self._get("%s/types/intiators" % self.api_base, params=params)
        initiators = resp['initiators']
        if len(initiators) == 1:
            return initiators[0]
        else:
            raise quarry_common.VolumeDriverException(
                "initiator not found for port address %s" % port_address)

    def _get_ig(self, name):
        uri = "%s/types/initiator-groups" % self.api_base
        resp = self._get(uri, params=dict(name=name))
        return resp['content']

    def _create_ig(self, name):
        # create an initiator group to hold the initiator
        uri = "%s/types/initiator-groups" % self.api_base
        data = {'ig-name': name}
        resp = self._post(uri, data=data)
        return resp['content']

    def _create_initiator(self, connector, login_chap, discovery_chap):
        initiator = self._get_initiator_names(connector)[0]
        # create an initiator
        data = {'initiator-name': initiator,
                'ig-id': initiator,
                'port-address': initiator}
        l, d = self._add_auth(data, login_chap, discovery_chap)
        uri = "%s/types/initiators" % self.api_base
        self._post(uri, data)
        return l, d

    def _add_auth(self, data, login_chap, discovery_chap):
        login_passwd, discovery_passwd = None, None
        if login_chap:
            data['initiator-authentication-user-name'] = 'chap_user'
            login_passwd = self._get_password()
            data['initiator-authentication-password'] = login_passwd
        if discovery_chap:
            data['initiator-discovery-user-name'] = 'chap_user'
            discovery_passwd = self._get_password()
            data['initiator-discovery-password'] = discovery_passwd
        return login_passwd, discovery_passwd

    def _get_password(self):
        return ''.join(random.Random.choice
                       (string.ascii_uppercase + string.digits)
                       for _ in range(12))

    def create_lun_map(self, volume, ig, lun_num=None):
        try:
            data = {'ig-id': ig, 'vol-id': volume['id']}
            if lun_num:
                data['lun'] = lun_num
            uri = "%s/types/lun-maps" % self.api_base
            res = self._post(uri, data=data)

            lunmap = self._obj_from_result(res)
            logging.info('Created lun-map:\n%s', lunmap)
        except exception.XtremIOAlreadyMappedError:
            logging.info('Volume already mapped, retrieving %(ig)s, %(vol)s',
                     {'ig': ig, 'vol': volume['id']})
            lunmap = self.client.find_lunmap(ig, volume['id'])
        return lunmap

    def _obj_from_result(self, res):
        typ, idx = res['links'][0]['href'].split('/')[-2:]
        uri = "%s/types/%s/%d" % (self.api_base, typ, idx)
        return self._get(uri)['content']

    def _get(self, uri, params=None):
        params = self._get_params(params)
        resp = requests.get(uri, auth=self.auth, verify=self.verify,
                            params=params)
        resp.raise_for_status()
        return resp.json()

    def _post(self, uri, data):
        data = json.dumps(data)
        params = self._get_params(None)
        resp = requests.post(uri, auth=self.auth, verify=self.verify,
                             params=params, data=data)
        resp.raise_for_status()
        return resp.json()

    def _put(self, uri, data):
        data = json.dumps(data)
        params = self._get_params(None)
        resp = requests.post(uri, auth=self.auth, verify=self.verify,
                             params=params, data=data)
        resp.raise_for_status()
        return resp.json()

    def _delete(self, uri, params=None):
        params = self._get_params(params)
        resp = requests.delete(uri, auth=self.auth, verify=self.verify,
                               params=params)
        resp.raise_for_status()

    def _get_params(self, params):
        if params is None:
            params = dict()
        if self.cluster_name:
            params['cluster-name'] = self.cluster_name
        return params
