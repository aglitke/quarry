- hosts: $config['ansible_host']
  remote_user: $config['ansible_user']

  roles:
  - quarry

  vars:
    config:
$config_str

  tasks:
  - name: Terminate a volume connection
    quarry_connection:
      backend: $backend
      config: "{{config}}"
      state: absent
      volume_id: $volume_id
      initiator: $getVar('initiator', 'null')
