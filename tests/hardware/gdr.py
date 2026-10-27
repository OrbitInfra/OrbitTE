"""Validate passive provider witnesses against public GPU owners and full bytes."""
import json


def require(condition, message):
    if not condition:
        raise ValueError(message)


def records(evidence, prefix):
    return [json.loads(line[len(prefix):]) for line in evidence['stdout.log'].splitlines()
            if line.startswith(prefix)]


def verify(profile, case, evidence):
    native, memory, contexts = {}, {}, {}
    rails = set(profile.get("rails", [profile["rnic"]]))
    for role in ('source', 'requester'):
        proof = evidence[role]
        result = json.loads(proof['result.json'])
        require(result['status'] == 'passed' and result['child_reaped']
                and result['resource_locks_released'] and result.get('fault_library_sha256'),
                role + ': process/observer proof missing')
        allocations = records(proof, 'ORBITTE_MEMORY ')
        require(len(allocations) == 1, role + ': ambiguous allocation')
        memory[role] = allocations[0]
        require(memory[role]['cuda'] == case[role + '_kind'].startswith('cuda:'),
                role + ': allocation kind mismatch')
        native[role] = [json.loads(line) for line in proof['cq-fault-0.jsonl'].splitlines()]
        require(native[role] and all(row['pid'] == result['native_pid'] for row in native[role]),
                role + ': provider records belong to another process')
        contexts[role] = {row['context']: row['device'] for row in native[role] if row['kind'] == 'context'}
        require(set(contexts[role].values()) == rails, role + ': native contexts differ from selected rails')
        closed = {row['context'] for row in native[role] if row['kind'] == 'close_context' and row['code'] == 0}
        require(closed == set(contexts[role]), role + ': native context close proof missing')
        mrs = [row for row in native[role] if row['kind'] == 'gdr_mr']
        require(mrs and all(row['mr'] for row in mrs), role + ': native MR registration failed')
        if memory[role]['cuda']:
            gpu = [row for row in mrs if row['cuda_status'] == 0 and row['memory_type'] == 2]
            require(gpu and all(row['access'] & 16 == 0 for row in gpu),
                    role + ': missing GPU registration or unsupported MW permission')
            local = {row['context'] for row in gpu if row['address'] == memory[role]['base']
                     and row['length'] == memory[role]['length'] and row['access'] == 1}
            require(local == set(contexts[role]), role + ': persistent local GPU MR missing on a context')
        qp = records(proof, 'ORBITTE_GPU_MR_STATS ')
        require(len(qp) == 1 and qp[0]['active_private_pds'] == 0
                and qp[0]['opened_private_pds'] == qp[0]['closed_private_pds'],
                role + ': private PD drain failed')

    requester = native['requester']
    posts = [row for row in requester if row['kind'] == 'gdr_post' and row['opcode'] in (0, 4)]
    length = case.get('slice_bytes', case['payload'] // 4 - 23)
    require(posts and all(row['code'] == 0 for row in posts), 'missing successful native payload WR')
    for opcode in (0, 4):
        require({row['context'] for row in posts if row['opcode'] == opcode} == set(contexts['requester']),
                'payload did not use every selected rail')
        require(sum(row['length'] for row in posts if row['opcode'] == opcode) == 3 * length,
                'native payload byte sum differs from requested ranges')
    for post in posts:
        if memory['requester']['cuda']:
            matches = [mr for mr in requester if mr['kind'] == 'gdr_mr'
                       and mr['cuda_status'] == 0 and mr['memory_type'] == 2
                       and mr['context'] == post['context']
                       and mr['pd'] == post['pd'] and mr['lkey'] == post['lkey']
                       and mr['address'] <= post['local']
                       and post['local'] + post['length'] <= mr['address'] + mr['length']]
            require(matches, 'payload SGE is not inside an actual GPU MR on its QP PD')
        if memory['source']['cuda']:
            permission = 2 if post['opcode'] == 0 else 4
            matches = [mr for mr in native['source'] if mr['kind'] == 'gdr_mr'
                       and mr['cuda_status'] == 0 and mr['memory_type'] == 2
                       and contexts['source'][mr['context']] == contexts['requester'][post['context']]
                       and mr['access'] == (1 | permission) and mr['rkey'] == post['rkey']
                       and mr['address'] == post['remote'] and mr['length'] == post['length']]
            require(matches, 'remote WR does not name an exact GPU MR with the requested permission')
            require(any(qp['kind'] == 'gdr_qp' and qp['context'] == mr['context'] and qp['pd'] == mr['pd'] and qp['qpn']
                        for mr in matches for qp in native['source']), 'GPU MR and source QP do not share private PD')
    sends = [row for row in requester if row['kind'] == 'gdr_post' and row['opcode'] in (2, 9)]
    expected = 2 if memory['source']['cuda'] else 9
    require(sends and all(row['opcode'] == expected and row['code'] == 0 for row in sends),
            'wrong completion SEND authority profile')
    if memory['source']['cuda']:
        require(not any(row['kind'] == 'gdr_post' and row['opcode'] in (7, 8)
                        for row in native['source']), 'GPU source posted a window operation')
