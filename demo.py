#!/usr/bin/env python

import json
import requests
import time

KEYSTONE_URL = "http://192.168.2.16:35357/v3/auth/tokens"
KEYSTONE_USER = "admin"
KEYSTONE_PASS = "letmein!"
CINDER_URL = "http://192.168.2.16:8776/v2/422e341dd2884e88a466a5c68ca54aa5"
QUARRY_URL = "http://localhost:8776/v2/tenant_id"


def keystone_auth():
    headers = {'Content-Type': 'application/json'}
    payload = json.dumps({
        "auth": {
            "identity": {
                "methods": ["password"],
                "password": {
                    "user": {
                        "domain": {
                            "name": "Default"
                        },
                        "name": KEYSTONE_USER,
                        "password": KEYSTONE_PASS
                    }
                }
            },
            "scope": {
                "project": {
                    "domain": {
                        "name": "Default"
                    },
                    "name": "admin"
                }
            }
        }
    })
    r = requests.post(KEYSTONE_URL, data=payload, headers=headers)
    if r.status_code != 201:
        raise Exception("unable to authenticate: %s" % r.text)
    print("Auth-token: %s" % r.headers['X-Subject-Token'])
    return r.headers['X-Subject-Token']


def test_volume_create_delete(url, headers):
    create_url = "%s/volumes" % url
    payload = json.dumps(dict(volume=dict(
        name='foo', size=1, volume_type='ceph'
    )))
    print "Creating volume..."
    r = requests.post(create_url, headers=headers, data=payload)
    if r.status_code != 202:
        raise Exception("Volume create failed: %s" % r.text)

    volume_id = r.json()['volume']['id']
    raw_input("Volume %s created. (Press enter)" % volume_id)

    print "Deleting volume..."
    delete_url = "%s/volumes/%s" % (url, volume_id)
    r = requests.delete(delete_url, headers=headers)
    if r.status_code != 202:
        raise Exception("Volume delete failed: %s" % r.text)
    raw_input("done. (Press enter)")


def main():
    auth_token = keystone_auth()
    headers = {'Content-Type': 'application/json',
               'X-Auth-Token': auth_token}
    for url in (QUARRY_URL,): #CINDER_URL, QUARRY_URL:
        print "Testing endpoint: %s" % url
        test_volume_create_delete(url, headers)


if __name__ == '__main__':
    main()
