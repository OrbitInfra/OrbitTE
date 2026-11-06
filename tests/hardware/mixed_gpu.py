"""Two-host mixed GPU profiles for the installed peer supervisor."""
from pathlib import Path

PROTOCOL = 'orbite-installed-mixed-gpu'
LEGS = {'tcp-local', 'tcp-pin', 'rdma-pin'}


def validate(profile):
    seen = set()
    for case in profile.get('cases', []):
        leg = case.get('staged_leg')
        if (leg not in LEGS or case.get('transport') != 'rdma'
                or any(case.get(role + '_kind') != 'cuda:0' for role in ('source', 'requester'))
                or set(case) != {'source', 'requester', 'source_kind', 'requester_kind',
                                 'transport', 'staged_leg'}):
            raise ValueError('mixed GPU case requires two CUDA peers, Direct RDMA and a staged_leg')
        identities = []
        for role in ('source', 'requester'):
            node = profile['nodes'][case[role]]
            identity = tuple(node.get(key) for key in ('hostname', 'boot_id', 'gpu_uuid'))
            if (not all(identity) or not node['gpu_uuid'].startswith('GPU-')
                    or node.get('gpu_lock') == node.get('nic_lock')
                    or not all(Path(node[k]).is_absolute() for k in ('gpu_lock', 'nic_lock'))):
                raise ValueError('mixed GPU nodes require pinned host, GPU and RNIC identities and locks')
            identities.append(identity)
        if any(a == b for a, b in zip(*identities)):
            raise ValueError('mixed GPU source and requester must be on distinct hosts and GPUs')
        seen.add(leg)
    if seen != LEGS:
        raise ValueError('mixed GPU profile must cover tcp-local, tcp-pin and rdma-pin')


def command(profile, case, node, role, root, ready=None):
    if role == 'requester' and (ready is None or len(ready) != 2):
        raise ValueError('expected the ready source endpoint and epoch')
    args = [role, profile['nodes'][node]['bind'], profile['rnic'], str(profile['gid_index']),
            case['staged_leg'], *(ready or [])]
    return args, {'ORBITE_HARDWARE_MAPS': root + '/maps.txt'}


def verify(case, evidence):
    if set(evidence) != {'source', 'requester'}:
        raise ValueError('mixed GPU evidence requires both peers')
    leg = case['staged_leg']
    native = 27 if leg == 'rdma-pin' else 14
    for role, record in evidence.items():
        # The consumer checks all guards, task aliases and physical drain before emitting this line.
        prefix = f'ORBITE_MIXED_OK {role} {leg} '
        lines = [line for line in record['stdout.log'].splitlines() if line.startswith(prefix)]
        if len(lines) != 1:
            raise ValueError(f'missing or duplicate {role} mixed GPU receipt')
        fields = {key: int(n) for key, n in (item.split('=') for item in lines[0][len(prefix):].split())}
        pins = 2 if role == 'source' and leg != 'tcp-local' else 0
        if ((fields['staged_bytes'], fields['pins']) != (26, pins)
                or role == 'source' and fields['private_pds'] != 4
                or role == 'requester' and (fields['rdma_read'], fields['rdma_write']) != (native, native)):
            raise ValueError(f'incorrect {role} direct/staged route counters')
