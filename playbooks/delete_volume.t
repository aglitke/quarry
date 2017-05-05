- hosts: "{{ ansible_host }}"
  remote_user: "{{ ansible_user }}"

  roles:
  - quarry

  vars_files:
  - "{{ backend_vars_file }}"

  tasks:
  - name: Delete a volume
    quarry_volume:
      backend: "{{ backend }}"
      config: "{{ backend_config }}"
      state: absent
      id: $volume_id
