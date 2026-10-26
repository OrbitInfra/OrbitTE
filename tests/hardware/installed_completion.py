"""Join accepted native WRs, lost successful CQEs and proven QP destruction."""
import json

from installed_restart import records


def verify(case, evidence):
    requester = evidence['requester']
    source = evidence['source']
    target = case['fault_role'] == 'requester'
    processes = json.loads(requester['result.json'])['phases']
    fault_hash = json.loads(requester['result.json']).get('fault_library_sha256', '')
    if len(fault_hash) != 64 or any(c not in '0123456789abcdef' for c in fault_hash):
        raise ValueError('missing immutable native fault library')
    if (json.loads(source['result.json']).get('fault_library_sha256')
            or any(source.get('cq-fault-' + str(g) + '.jsonl') for g in range(3))
            or any('liborbitte_cq_fault.so' in (value or '') for name, value in source.items() if name.startswith('maps'))):
        raise ValueError('native completion fault escaped requester scope')
    logs = []
    for generation, process in enumerate(processes):
        maps = requester['maps-' + str(generation) + '.txt']
        if 'liborbitte_cq_fault.so' not in maps:
            raise ValueError('native child did not map the pinned fault library')
        rows = [json.loads(line) for line in (requester.get('cq-fault-' + str(generation) + '.jsonl') or '').splitlines()]
        if not rows or len(rows) > 4096:
            raise ValueError('missing or unbounded native receipts')
        active = set()
        for index, row in enumerate(rows):
            if row['seq'] != index + 1 or row['pid'] != process['native_pid']:
                raise ValueError('native receipt sequence or process identity differs')
            kind = row['kind']
            if kind == 'context':
                if row['context'] in active or 'libmlx5' not in row['provider'] or row['provider'] not in maps:
                    raise ValueError('native callback lacks its actual mapped provider')
                active.add(row['context'])
            elif kind == 'close_context':
                if row['code'] != 0 or row['context'] not in active:
                    raise ValueError('native context did not close successfully')
                active.remove(row['context'])
            elif kind == 'arm':
                if len(active) != 2:
                    raise ValueError('fault did not arm on two live native contexts')
            elif kind in ('post', 'lost', 'destroy_qp'):
                if row['context'] not in active or (kind == 'destroy_qp' and row['code'] != 0):
                    raise ValueError('native operation escaped its context or failed cleanup')
            elif kind != 'disarm':
                raise ValueError('unknown native receipt')
        if process['exit_code'] == 0 and active:
            raise ValueError('orderly child retained a native context')
        logs.append(rows)
    if any(requester.get('cq-fault-' + str(g) + '.jsonl') for g in range(len(processes), 3)):
        raise ValueError('unexpected extra native process log')
    pending = records(requester, 'ORBITTE_INSTALLED_CQE_PENDING ')
    drains = records(requester, 'ORBITTE_INSTALLED_CQE_DRAIN ')
    phases = list(range(case['generations'] - 1))
    if ([r['phase'] for r in pending] != phases
            or [r['phase'] for r in drains] != ([] if target else phases)):
        raise ValueError('missing native pending or drain receipts')
    local = records(requester, 'ORBITTE_INSTALLED_PHASE ')
    remote = records(source, 'ORBITTE_INSTALLED_PHASE ')
    operation = 1 if case['operation'] == 'read' else 2
    count, payload = case['parents'], case['payload']
    bank, destination = 2 * payload + 192, payload + 128
    for phase, receipt in enumerate(pending):
        rows = logs[phase if target else 0]
        arms = [r for r in rows if r['kind'] == 'arm' and r['phase'] == phase]
        if (len(arms) != 1 or arms[0]['operation'] != operation or arms[0]['expected'] != count
                or receipt['pid'] != local[phase]['pid'] or receipt['posts'] != count
                or receipt['lost'] != count or receipt['destroyed_qps'] != 0):
            raise ValueError('native pending counts or fault generation disagree')
        start, end = arms[0]['seq'], receipt['sequence']
        window = [r for r in rows if start < r['seq'] <= end]
        posts = [r for r in window if r['kind'] == 'post']
        lost = [r for r in window if r['kind'] == 'lost']
        identity = lambda r: (r['context'], r['cq'], r['qpn'], r['wr_id'])
        if (len(window) != 2 * count or len(posts) != count or len(lost) != count
                or any(r['phase'] != phase or r['operation'] != operation for r in window)
                or any(r['code'] != 0 or r['length'] != payload for r in posts)
                or any(r['status'] != 0 for r in lost)
                or len({identity(r) for r in posts}) != count
                or {identity(r) for r in posts} != {identity(r) for r in lost}):
            raise ValueError('accepted native WRs do not match exact lost successful CQEs')
        addresses = {(local[phase]['address'] + p * bank + (destination if operation == 1 else 64),
                      remote[phase]['address'] + p * bank + (64 if operation == 1 else destination))
                     for p in range(count)}
        if {(r['local'], r['remote']) for r in posts} != addresses:
            raise ValueError('native payload WRs do not cover the declared parent buffers')
        if target:
            if any(r['kind'] == 'disarm' and r['phase'] == phase for r in rows):
                raise ValueError('killed child disarmed before supervised death')
        else:
            drain = drains[phase]
            after = [r for r in rows if end < r['seq'] <= drain['sequence']]
            retired = [r for r in after if r['kind'] == 'destroy_qp']
            disarmed = [r for r in after if r['kind'] == 'disarm']
            qps = lambda values: {(r['context'], r['cq'], r['qpn']) for r in values}
            if (drain['pid'] != receipt['pid'] or drain['posts'] != count or drain['lost'] != count
                    or drain['write_effect_unknown'] is not (operation == 2)
                    or drain['destroyed_qps'] != len(retired) or not retired
                    or not qps(lost).issubset(qps(retired))
                    or len(disarmed) != 1 or disarmed[0]['seq'] != drain['sequence']
                    or any(r['phase'] != phase or r['kind'] not in ('destroy_qp', 'disarm') for r in after)):
                raise ValueError('lost payload QPs were not destroyed before public drain')
    if sum(r['kind'] == 'arm' for rows in logs for r in rows) != len(phases):
        raise ValueError('unexpected native fault phase')
    if sum(r['kind'] in ('post', 'lost') for rows in logs for r in rows) != 2 * count * len(phases):
        raise ValueError('payload receipts escaped the declared fault windows')
