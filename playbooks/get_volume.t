- hosts: "{{ ansible_host }}"
  remote_user: "{{ ansible_user }}"

  roles:
  - quarry

  vars_files:
  - "{{ backend_vars_file }}"

  tasks:
  - name: Get a volume
    quarry_volume:
      backend: "{{ backend }}"
      config: "{{backend_config}}"
      state: present
      id: $volume_id
      size: $volume_size
    check_mode: yes
