- hosts: $config['ansible_host']
  remote_user: $config['ansible_user']

  roles:
  - quarry

  vars:
    config:
$config_str

  tasks:
  - name: Delete a volume
    quarry_volume:
      backend: $backend
      config: "{{config}}"
      state: absent
      id: $volume_id
