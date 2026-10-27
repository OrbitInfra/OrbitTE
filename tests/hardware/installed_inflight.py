"""Join one interrupted native WR to post-drain partial bytes and live recovery."""
import json

from installed import PROTOCOL
from installed_restart import records


def one(record, suffix):
    rows = records(record, 'ORBITTE_INSTALLED_DMA_' + suffix + ' ')
    if len(rows) != 1:
        raise ValueError('missing or duplicate DMA ' + suffix)
    return rows[0]


def verify(profile, case, evidence):
    owners = {}
    drains = {}
    for role in ('source', 'requester'):
        record = evidence[role]
        supervisor = json.loads(record['result.json'])
        pid = supervisor.get('native_pid')
        if (type(pid) is not int or pid <= 0 or supervisor.get('exit_code') != 0
                or not supervisor.get('child_reaped') or not supervisor.get('resource_locks_released')
                or supervisor.get('phases') or supervisor.get('timeout')):
            raise ValueError('in-flight peer did not drain and reap its original child')
        maps = record.get('maps.txt') or ''
        if 'libibverbs' not in maps or 'libmlx5' not in maps or 'libcuda' in maps:
            raise ValueError('in-flight peer lacks its host RDMA providers')
        if role == 'requester':
            digest = supervisor.get('fault_library_sha256', '')
            if (len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest)
                    or 'liborbitte_cq_fault.so' not in maps):
                raise ValueError('requester lacks the pinned native observer')
        elif (supervisor.get('fault_library_sha256') or 'liborbitte_cq_fault.so' in maps
              or any(record.get('cq-fault-' + str(g) + '.jsonl') for g in range(3))):
            raise ValueError('in-flight fault escaped its requester process')
        nics = records(record, 'ORBITTE_INSTALLED_NIC ')
        if (len(nics) != 2 or {r['name'] for r in nics} != set(profile['rails'])
                or len({r['pci'] for r in nics}) != 2):
            raise ValueError('in-flight peer lacks both locked RNIC contexts')
        owner, drain, payload = (one(record, name) for name in ('OWNER', 'DRAIN', 'BYTES'))
        if (any(r['pid'] != pid for r in (owner, drain, payload))
                or type(owner['address']) is not int or owner['address'] <= 0):
            raise ValueError('public DMA proof belongs to another process or backing')
        owners[role], drains[role] = owner, drain
        if (any(drain[key] != 0 for key in ('sq_debt', 'rq_debt', 'grants', 'windows',
                                           'batches', 'direct_bytes', 'inflight_bytes'))
                or drain['persistent_mrs'] != 4 or drain['closed_qps'] != 1
                or drain['completion_errors'] not in ((1,) if role == 'requester' else (0, 1))
                or drain['progress_failures'] != drain['progress_recoveries']
                or drain['locally_drained'] is not True):
            raise ValueError('public DMA owners did not drain with persistent MRs retained')
        destination = (role == 'source') != (case['operation'] == 'read')
        counts = [payload[k] for k in ('copied', 'unchanged', 'ambiguous')]
        if (payload['destination'] is not destination or payload['full_bytes_and_guards'] is not True
                or payload['memory_writable'] is not True
                or any(type(n) is not int or n < 0 for n in counts)
                or (destination and (sum(counts) != case['payload'] or min(counts[:2]) <= 0))
                or (not destination and any(counts))):
            raise ValueError('partial DMA bytes were not demonstrated after native drain')
        rounds = records(record, 'ORBITTE_INSTALLED_ROUND ')
        if [(r['phase'], r['round']) for r in rounds] != [(p, r) for p in range(2) for r in range(3)]:
            raise ValueError('in-flight peer lacks healthy rounds before and after recovery')
        for index, row in enumerate(rounds):
            if (row['opened_mrs'] != 4 or row['opened_qps'] != 1 + row['phase']
                    or row['closed_qps'] != row['phase']
                    or (index and row['completed_grants'] <= rounds[index - 1]['completed_grants'])
                    or (row['round'] and row['reused_qps'] <= rounds[index - 1]['reused_qps'])):
                raise ValueError('recovery lost MR ownership or bounded QP reuse')
            for operation in ('read', 'write'):
                expected = ((index + 1) * case['payload']
                            if role == 'requester' and case['operation'] == operation else 0)
                if row[operation + '_bytes'] != expected:
                    raise ValueError('failed WR was counted as successful payload or recovery bytes differ')
        if records(record, 'ORBITTE_INSTALLED_RESULT ') != [dict(
                protocol=PROTOCOL, status='passed', role=role, operation=case['operation'],
                parents=1, payload=case['payload'], rounds=3,
                full_bytes_and_guards=True, locally_drained=True)]:
            raise ValueError('missing final closed installed result')
        # The byte observer must follow drain and precede every recovery write.
        tagged = [line.split(' ', 1) for line in record['stdout.log'].splitlines()
                  if line.startswith('ORBITTE_INSTALLED_')]
        positions = {tag: i for i, (tag, _) in enumerate(tagged) if tag.startswith('ORBITTE_INSTALLED_DMA_')}
        recovery = next(i for i, (tag, raw) in enumerate(tagged)
                        if tag == 'ORBITTE_INSTALLED_ROUND' and json.loads(raw)['phase'] == 1)
        if not positions['ORBITTE_INSTALLED_DMA_DRAIN'] < positions['ORBITTE_INSTALLED_DMA_BYTES'] < recovery:
            raise ValueError('byte proof was taken outside the drained fault interval')

    requester = evidence['requester']
    proof = one(requester, 'NATIVE')
    if (proof['pid'] != owners['requester']['pid'] or proof['posts'] != 1
            or proof['modifications'] != 1 or proof['failed'] != 1 or proof['completed'] != 0
            or proof['destroyed'] != 1 or proof['write_effect_unknown'] is not (case['operation'] == 'write')):
        raise ValueError('native DMA proof does not describe one failed, destroyed payload QP')
    rows = [json.loads(line) for line in (requester.get('cq-fault-0.jsonl') or '').splitlines()]
    if not rows or len(rows) > 4096 or any(requester.get('cq-fault-' + str(g) + '.jsonl') for g in (1, 2)):
        raise ValueError('missing or extra native DMA receipt stream')
    active = set()
    for seq, row in enumerate(rows, 1):
        if row['seq'] != seq or row['pid'] != proof['pid'] or row['phase'] != 0:
            raise ValueError('native DMA sequence or process identity changed')
        kind = row['kind']
        if kind == 'context':
            if row['context'] in active or 'libmlx5' not in row['provider'] or row['provider'] not in requester['maps.txt']:
                raise ValueError('native DMA lacks its actual provider')
            active.add(row['context'])
        elif kind == 'close_context':
            if row['code'] != 0 or row['context'] not in active:
                raise ValueError('native DMA context failed close')
            active.remove(row['context'])
        elif kind in ('dma_post', 'dma_error', 'dma_completion', 'destroy_qp'):
            if row['context'] not in active or (kind == 'destroy_qp' and row['code'] != 0):
                raise ValueError('native DMA escaped its context or failed teardown')
        elif kind == 'dma_arm':
            if len(active) != 2 or row['operation'] != (1 if case['operation'] == 'read' else 2):
                raise ValueError('native DMA armed the wrong contexts or operation')
        elif kind != 'dma_disarm':
            raise ValueError('unexpected native DMA receipt')
    if active:
        raise ValueError('native DMA retained a context after close')
    armed = [r for r in rows if r['kind'] == 'dma_arm']
    disarmed = [r for r in rows if r['kind'] == 'dma_disarm']
    if len(armed) != 1 or len(disarmed) != 1 or disarmed[0]['seq'] != proof['sequence']:
        raise ValueError('native DMA arm/disarm fence missing')
    window = [r for r in rows if armed[0]['seq'] < r['seq'] < disarmed[0]['seq']]
    if [r['kind'] for r in window] != ['dma_post', 'dma_error', 'dma_completion', 'destroy_qp']:
        raise ValueError('native DMA requires accepted WR, QP ERR, failed WC and successful destroy in order')
    post, error, completion, destroyed = window
    identity = lambda r: (r['context'], r['cq'], r['qpn'])
    if (len({identity(r) for r in window}) != 1
            or any(r['qpn'] != proof['qpn'] for r in window)
            or any(r['wr_id'] != proof['wr_id'] for r in (post, error, completion))
            or post['code'] != 0 or post['length'] != case['payload']
            or post['operation'] != (1 if case['operation'] == 'read' else 2)
            or error['code'] != 0 or error['state'] != 6 or completion['status'] == 0):
        raise ValueError('native DMA receipts do not identify one actually interrupted WR')
    destination = case['payload'] + 128
    if (post['local'] != owners['requester']['address'] + (destination if case['operation'] == 'read' else 64)
            or post['remote'] != owners['source']['address'] + (64 if case['operation'] == 'read' else destination)):
        raise ValueError('native DMA addresses disagree with verified public backing')
    if any(r['kind'].startswith('dma_') and not armed[0]['seq'] <= r['seq'] <= disarmed[0]['seq'] for r in rows):
        raise ValueError('native DMA escaped its declared fault window')
