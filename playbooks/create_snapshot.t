- hosts: $ansible_host
  remote_user: $ansible_user

  roles:
  - quarry

  vars:
    config:
$config

  tasks:
  - name: Create a snapshot
    quarry_snapshot:
      backend: $backend
      config: "{{config}}"
      state: present
      id: $snapshot_id
      volume_id: $volume_id
    check_mode: $getVar('check_mode', 'no')
