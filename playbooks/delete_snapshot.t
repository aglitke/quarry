- hosts: $ansible_host
  remote_user: $ansible_user

  roles:
  - quarry

  vars:
    config:
$config

  tasks:
  - name: Delete a snapshot
    quarry_snapshot:
      backend: $backend
      config: "{{config}}"
      state: absent
      id: $snapshot_id
