"""Validate the installed host consumer's profile and independent result ledger."""
import json

PROTOCOL = 'orbitte-installed-host-v4'


def validate(case):
    if (case.get('mode') not in ('installed-host', 'installed-restart', 'installed-interruption', 'installed-lost-completion', 'installed-cleanup-failure') or case.get('transport') != 'rdma'
            or case.get('source_kind') != 'cpu' or case.get('requester_kind') != 'cpu'
            or case.get('source') == case.get('requester')
            or case.get('operation') not in ('read', 'write')
            or type(case.get('rdma_events')) is not bool):
        raise ValueError('installed host requires two peers and explicit CPU RDMA/profile choices')
    for name, low, high in [('parents', 1, 32), ('payload', 128, 32 << 20),
                            ('block', 64 << 10, 4 << 20), ('num_lanes', 1, 3), ('mask', 1, 3)]:
        if type(case.get(name)) is not int or not low <= case[name] <= high:
            raise ValueError('invalid installed host ' + name)
    if case['mode'] in ('installed-restart', 'installed-interruption', 'installed-lost-completion', 'installed-cleanup-failure'):
        if (case.get('fault_role') not in ('source', 'requester')
                or type(case.get('generations')) is not int or case['generations'] not in (2, 3)):
            raise ValueError('restart requires an explicit target and two or three generations')
    elif 'fault_role' in case or 'generations' in case:
        raise ValueError('restart controls require the installed-restart mode')
    if case['mode'] in ('installed-interruption', 'installed-lost-completion', 'installed-cleanup-failure') and case['payload'] > case['block']:
        raise ValueError('interruption profiles require one slice per parent')
    if case['mode'] == 'installed-cleanup-failure' and (case['fault_role'] != 'requester'
            or case['generations'] != 2 or case['parents'] != 1 or case['num_lanes'] != 1
            or case['mask'] not in (1, 2)):
        raise ValueError('cleanup failure requires one requester QP and two generations')
    if case['parents'] * (2 * case['payload'] + 3 * 64) > 256 << 20:
        raise ValueError('installed host backing exceeds 256 MiB')


def command(profile, case, node, role, root, readiness=None):
    environment = {
        'ORBITTE_HOST_MODE': case['mode'],
        'ORBITTE_SHARED_ROLE': role,
        'ORBITTE_SHARED_BIND': profile['nodes'][node]['bind'],
        'ORBITTE_SHARED_RAILS': ','.join(profile['rails']),
        'ORBITTE_SHARED_GID': str(profile['gid_index']),
        'ORBITTE_SHARED_MAPS': root + '/maps.txt',
        'ORBITTE_RDMA_EVENTS': str(int(case['rdma_events'])),
        'ORBITTE_RDMA_LANES': str(case['num_lanes']),
        'ORBITTE_RAIL_OPERATION': case['operation'],
        'ORBITTE_RAIL_MASK': str(case['mask']),
        'ORBITTE_HOST_PARENTS': str(case['parents']),
        'ORBITTE_HOST_PAYLOAD': str(case['payload']),
        'ORBITTE_HOST_BLOCK': str(case['block']),
        'CUDA_VISIBLE_DEVICES': '',
    }
    if case['mode'] in ('installed-restart', 'installed-interruption', 'installed-lost-completion', 'installed-cleanup-failure'):
        environment.update(ORBITTE_RESTART_GENERATIONS=str(case['generations']),
                           ORBITTE_RESTART_KILL_ROLE=case['fault_role'])
    if readiness is not None:
        if len(readiness) != 1:
            raise ValueError('invalid installed host readiness')
        environment['ORBITTE_SHARED_PEER'] = readiness[0]
    return [], environment


def verify(profile, case, evidence):
    validate(case)
    if case['mode'] in ('installed-restart', 'installed-interruption', 'installed-lost-completion', 'installed-cleanup-failure'):
        from installed_restart import verify as verify_restart
        return verify_restart(profile, case, evidence)
    for role in ('source', 'requester'):
        record = evidence[role]
        supervisor = json.loads(record['result.json'])
        if (supervisor.get('exit_code') != 0 or not supervisor.get('child_reaped')
                or not supervisor.get('resource_locks_released')):
            raise ValueError('installed peer did not exit and release its resource locks')
        maps = record.get('maps.txt') or ''
        if 'libibverbs' not in maps or 'libmlx5' not in maps or 'libcuda' in maps:
            raise ValueError('installed peer native provider evidence is missing or unexpected')

        def records(prefix):
            return [json.loads(line[len(prefix):]) for line in record['stdout.log'].splitlines()
                    if line.startswith(prefix)]

        nics = records('ORBITTE_INSTALLED_NIC ')
        if (len(nics) != 2 or {n['name'] for n in nics} != set(profile['rails'])
                or len({n['pci'] for n in nics}) != 2):
            raise ValueError('installed peer did not publish the two locked RNICs')
        rounds = records('ORBITTE_INSTALLED_ROUND ')
        if len(rounds) != 3 or [(r['phase'], r['round']) for r in rounds] != [(0, i) for i in range(3)]:
            raise ValueError('installed peer omitted or duplicated a round')
        for index, row in enumerate(rounds):
            bound = case['mask'].bit_count() * case['num_lanes']
            if (not 1 <= row['opened_qps'] <= bound or row['opened_mrs'] != 4
                    or row['closed_qps'] != 0):
                raise ValueError('installed QP/MR bound was violated')
            if row['completed_grants'] <= (rounds[index - 1]['completed_grants'] if index else 0):
                raise ValueError('installed peer made no native grant progress')
            if index and row['reused_qps'] <= rounds[index - 1]['reused_qps']:
                raise ValueError('installed peer did not reuse native QPs')
            expected = (index + 1) * case['parents'] * case['payload']
            for operation in ('read', 'write'):
                value = expected if role == 'requester' and case['operation'] == operation else 0
                if row[operation + '_bytes'] != value:
                    raise ValueError('installed native byte ledger disagrees with requested bytes')
        results = records('ORBITTE_INSTALLED_RESULT ')
        if results != [dict(protocol=PROTOCOL, status='passed', role=role,
                            operation=case['operation'], parents=case['parents'],
                            payload=case['payload'], rounds=3,
                            full_bytes_and_guards=True, locally_drained=True)]:
            raise ValueError('installed peer has no exact complete result')
