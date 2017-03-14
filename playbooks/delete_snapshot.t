- hosts: $config['ansible_host']
  remote_user: $config['ansible_user']

  roles:
  - quarry

  vars:
    config:
$config_str

  tasks:
  - name: Delete a snapshot
    quarry_snapshot:
      backend: $backend
      config: "{{config}}"
      state: absent
      id: $snapshot_id
