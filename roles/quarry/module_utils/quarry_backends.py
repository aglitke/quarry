from ansible.module_utils import quarry_rbd, quarry_nfs

backends = {'rbd': quarry_rbd.Driver,
            'nfs': quarry_nfs.Driver}
