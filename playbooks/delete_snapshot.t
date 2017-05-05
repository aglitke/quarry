- hosts: "{{ ansible_host }}"
  remote_user: "{{ ansible_user }}"

  roles:
  - quarry

  vars_files:
  - "{{ backend_vars_file }}"

  tasks:
  - name: Delete a snapshot
    quarry_snapshot:
      backend: "{{ backend }}"
      config: "{{ backend_config }}"
      state: absent
      id: $snapshot_id
