- hosts: $config['ansible_host']
  remote_user: $config['ansible_user']

  roles:
  - quarry

  vars:
    config:
$config_str

  tasks:
  - name: Create a snapshot
    quarry_snapshot:
      backend: $backend
      config: "{{config}}"
      state: present
      id: $snapshot_id
      volume_id: $volume_id
    check_mode: yes
