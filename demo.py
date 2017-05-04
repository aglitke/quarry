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
    volume_id = _create_volume(url, headers, vol_type)
    _get_volume(url, headers, volume_id)

    print("Connecting volume")
    action_url = "%s/volumes/%s/action" % (url, volume_id)
    payload = json.dumps({'os-initialize_connection': dict(
        connector=dict(
            initiator="iqn.1994-05.com.redhat:adam-test"
        )
    )})
    r = requests.post(action_url, headers=headers, data=payload)
    print(r.text)
    raw_input("done. (Press enter)")

    print("Disconnecting volume")
    payload = json.dumps({'os-terminate_connection': dict(
        connector=dict(
            initiator="iqn.1994-05.com.redhat:adam-test"
        )
    )})
    r = requests.post(action_url, headers=headers, data=payload)
    print(r.text)
    raw_input("done. (Press enter)")

    _delete_volume(url, headers, volume_id)
    raw_input("done. (Press enter)")


def test_volume_snapshot(url, headers, vol_type):
    volume_id = _create_volume(url, headers, vol_type)
    raw_input("done. (Press enter)")

    snap_id = _create_snapshot(url, headers, volume_id)
    raw_input("done. (Press enter)")

    _delete_snapshot(url, headers, snap_id)
    raw_input("done. (Press enter)")

    _delete_volume(url, headers, volume_id)


def test_multi_backend_search(url, headers, vol_types):
    vols = []
    for vol_type in vol_types:
        volume_id = _create_volume(url, headers, vol_type)
        vols.append(volume_id)

    for volume_id in vols:
        info = _get_volume(url, headers, volume_id)
        print("Volume %s has type %s" % (volume_id,
                                         info['volume']['volume_type']))
        snapshot_id = _create_snapshot(url, headers, volume_id)
        info = _get_snapshot(url, headers, snapshot_id)
        print("Snapshot %s belongs to volume %s" % (
            snapshot_id, info['snapshot']['volume_id']))
        _delete_snapshot(url, headers, snapshot_id)
        _delete_volume(url, headers, volume_id)


def _create_volume(url, headers, vol_type):
    create_volume_url = "%s/volumes" % url
    payload = json.dumps(dict(volume=dict(
        name='foo', size=1, volume_type=vol_type
    )))
    print("Creating volume (type=%s) ..." % vol_type)
    r = requests.post(create_volume_url, headers=headers, data=payload)
    if r.status_code != 202:
        raise Exception("Volume create failed: %s" % r.text)
    volume_id = r.json()['volume']['id']
    print("Volume %s created" % volume_id)
    return volume_id


def _get_volume(url, headers, volume_id):
    get_url = "%s/volumes/%s" % (url, volume_id)
    print("Getting volume %s" % volume_id)
    r = requests.get(get_url, headers=headers)
    if r.status_code == 200:
        return r.json()
    else:
        raise Exception("Cannot get Volume: %s", r.text)


def _delete_volume(url, headers, volume_id):
    delete_url = "%s/volumes/%s" % (url, volume_id)
    print("Deleting volume %s" % volume_id)
    r = requests.delete(delete_url, headers=headers)
    if r.status_code != 202:
        raise Exception("Volume delete failed: %s" % r.text)
    print("Volume %s deleted" % volume_id)


def _create_snapshot(url, headers, volume_id):
    create_snap_url = "%s/snapshots" % url
    payload = json.dumps(dict(snapshot=dict(
        name='snap-test',
        description='',
        volume_id=volume_id
    )))

    print("Creating snapshot of volume %s" % volume_id)
    r = requests.post(create_snap_url, headers=headers, data=payload)
    if r.status_code != 202:
        raise Exception("Snapshot create failed: %s" % r.text)
    snap_id = r.json()['snapshot']['id']
    print("Snapshot %s created of volume %s" % (snap_id, volume_id))
    return snap_id


def _get_snapshot(url, headers, snapshot_id):
    get_url = "%s/snapshots/%s" % (url, snapshot_id)
    print("Getting snapshot %s" % snapshot_id)
    r = requests.get(get_url, headers=headers)
    if r.status_code == 200:
        return r.json()
    else:
        raise Exception("Cannot get Snapshot: %s", r.text)


def _delete_snapshot(url, headers, snapshot_id):
    delete_snap_url = "%s/snapshots/%s" % (url, snapshot_id)
    print("Deleting snapshot %s" % snapshot_id)
    r = requests.delete(delete_snap_url, headers=headers)
    if r.status_code != 202:
        raise Exception("Snapshot delete failed: %s" % r.text)
    print("Snapshot %s deleted" % snapshot_id)


def main():
    auth_token = keystone_auth()
    headers = {'Content-Type': 'application/json',
               'X-Auth-Token': auth_token}
    #for url in (QUARRY_URL,): #CINDER_URL, QUARRY_URL:
    #for url in (CINDER_URL,):
    for url in (QUARRY_URL,):
        print("Testing endpoint: %s" % url)
        #test_volume_create_delete(url, headers, 'xtremio')
        #test_volume_snapshot(url, headers, 'xtremio')
        test_multi_backend_search(url, headers, ('ceph', 'xtremio'))


if __name__ == '__main__':
    main()
