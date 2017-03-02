- hosts: $ansible_host
  remote_user: $ansible_user

  roles:
  - quarry

  vars:
    config:
$config

  tasks:
  - name: Create a volume
    quarry_volume:
      backend: $backend
      config: "{{config}}"
      state: present
      id: $volume_id
      size: $volume_size
    check_mode: $getVar('check_mode', 'no')
