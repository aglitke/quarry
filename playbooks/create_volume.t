- hosts: $config['ansible_host']
  remote_user: $config['ansible_user']

  roles:
  - quarry

  vars:
    config:
$config_str

  tasks:
  - name: Create a volume
    quarry_volume:
      backend: $backend
      config: "{{config}}"
      state: present
      id: $volume_id
      size: $volume_size
