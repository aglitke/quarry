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


def test_volume_create_delete(url, headers, vol_type):
    create_url = "%s/volumes" % url
    payload = json.dumps(dict(volume=dict(
        name='foo', size=1, volume_type=vol_type
    )))
    print "Creating volume..."
    r = requests.post(create_url, headers=headers, data=payload)
    if r.status_code != 202:
        raise Exception("Volume create failed: %s" % r.text)

    volume_id = r.json()['volume']['id']
    get_url = "%s/volumes/%s" % (url, volume_id)
    r = requests.get(get_url, headers=headers)
    if r.status_code == 200:
        raw_input("Volume %s created. (Press enter)" %
                  r.json()['volume']['id'])

    print "Connecting volume"
    action_url = "%s/volumes/%s/action" % (url, volume_id)
    payload = json.dumps({'os-initialize_connection': dict(
        connector=dict(
            initiator="iqn.1994-05.com.redhat:adam-test"
        )
    )})
    r = requests.post(action_url, headers=headers, data=payload)
    print r.text
    raw_input("done. (Press enter)")

    print "Disconnecting volume"
    payload = json.dumps({'os-terminate_connection': dict(
        connector=dict(
            initiator="iqn.1994-05.com.redhat:adam-test"
        )
    )})
    r = requests.post(action_url, headers=headers, data=payload)
    print r.text
    raw_input("done. (Press enter)")

    print "Deleting volume..."
    delete_url = "%s/volumes/%s" % (url, volume_id)
    r = requests.delete(delete_url, headers=headers)
    if r.status_code != 202:
        raise Exception("Volume delete failed: %s" % r.text)
    raw_input("done. (Press enter)")


def test_volume_snapshot(url, headers):
    create_volume_url = "%s/volumes" % url
    payload = json.dumps(dict(volume=dict(
        name='foo', size=1, volume_type='ceph'
    )))
    print "Creating volume..."
    r = requests.post(create_volume_url, headers=headers, data=payload)
    if r.status_code != 202:
        raise Exception("Volume create failed: %s" % r.text)
    volume_id = r.json()['volume']['id']
    raw_input("done. (Press enter)")

    create_snap_url = "%s/snapshots" % url
    payload = json.dumps(dict(snapshot=dict(
        name='snap-test',
        description='',
        volume_id=volume_id
    )))

    print "Creating snapshot"
    r = requests.post(create_snap_url, headers=headers, data=payload)
    if r.status_code != 202:
        raise Exception("Snapshot create failed: %s" % r.text)
    snap_id = r.json()['snapshot']['id']
    print r.text
    raw_input("done. (Press enter)")

    delete_snap_url = "%s/snapshots/%s" % (url, snap_id)
    print "Deleting snapshot"
    r = requests.delete(delete_snap_url, headers=headers)
    if r.status_code != 202:
        raise Exception("Snapshot delete failed: %s" % r.text)
    raw_input("done. (Press enter)")

    delete_volume_url = "%s/volumes/%s" % (url, volume_id)
    r = requests.delete(delete_volume_url, headers=headers)
    if r.status_code != 202:
        raise Exception("Volume delete failed: %s" % r.text)


def main():
    auth_token = keystone_auth()
    headers = {'Content-Type': 'application/json',
               'X-Auth-Token': auth_token}
    #for url in (QUARRY_URL,): #CINDER_URL, QUARRY_URL:
    #for url in (CINDER_URL,):
    for url in (QUARRY_URL,):
        print "Testing endpoint: %s" % url
        test_volume_create_delete(url, headers, 'xtremio')
        #test_volume_snapshot(url, headers)


if __name__ == '__main__':
    main()
