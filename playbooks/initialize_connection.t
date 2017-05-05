- hosts: "{{ ansible_host }}"
  remote_user: "{{ ansible_user }}"

  roles:
  - quarry

  vars_files:
  - "{{ backend_vars_file }}"

  tasks:
  - name: Initialize a volume connection
    quarry_connection:
      backend: "{{ backend }}"
      config: "{{backend_config}}"
      state: present
      volume_id: $volume_id
      initiator: $getVar('initiator', 'null')
