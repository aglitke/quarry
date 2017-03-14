- hosts: $config['ansible_host']
  remote_user: $config['ansible_user']

  roles:
  - quarry

  vars:
    config:
$config_str

  tasks:
  - name: Initialize a volume connection
    quarry_connection:
      backend: $backend
      config: "{{config}}"
      state: present
      volume_id: $volume_id
      initiator: $getVar('initiator', 'null')
