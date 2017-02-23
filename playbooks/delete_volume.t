- hosts: $ansible_host
  remote_user: $ansible_user

  roles:
  - quarry

  vars:
    config:
$config

  tasks:
  - name: Delete a volume
    quarry_volume:
      backend: $backend
      config: "{{config}}"
      state: absent
      id: $volume_id
