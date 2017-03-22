from ansible.module_utils import quarry_rbd, quarry_nfs, quarry_xtremio

backends = dict(
    nfs=quarry_nfs.Driver,
    rbd=quarry_rbd.Driver,
    xtremio=quarry_xtremio.Driver,
)
