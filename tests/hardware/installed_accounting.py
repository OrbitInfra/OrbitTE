"""Reconcile physical RNIC, payload range, native CQE and public byte ledgers."""
import json

from installed_restart import records


def verify(profile, case, evidence):
    requester, source = evidence['requester'], evidence['source']
    process = json.loads(requester['result.json'])
    fault_hash = process.get('fault_library_sha256', '')
    maps = requester.get('maps.txt') or ''
    if (len(fault_hash) != 64 or any(c not in '0123456789abcdef' for c in fault_hash)
            or 'liborbitte_cq_fault.so' not in maps
            or json.loads(source['result.json']).get('fault_library_sha256')
            or 'liborbitte_cq_fault.so' in (source.get('maps.txt') or '')
            or any(source.get('cq-fault-' + str(g) + '.jsonl') for g in range(3))
            or any(requester.get('cq-fault-' + str(g) + '.jsonl') for g in (1, 2))):
        raise ValueError('rail observer escaped its pinned requester process')
    bases = {}
    for role, record in evidence.items():
        memory = records(record, 'ORBITTE_INSTALLED_RAIL_MEMORY ')
        if (len(memory) != 1 or memory[0]['pid'] != json.loads(record['result.json'])['native_pid']
                or memory[0]['address'] <= 0):
            raise ValueError('rail memory identity is missing')
        bases[role] = memory[0]['address']
    rows = [json.loads(line) for line in (requester.get('cq-fault-0.jsonl') or '').splitlines()]
    receipts = records(requester, 'ORBITTE_INSTALLED_RAIL_ROUND ')
    if not rows or len(rows) > 4096 or [r['round'] for r in receipts] != [0, 1, 2]:
        raise ValueError('missing bounded native rail rounds')
    contexts, qps, destroyed = {}, set(), set()
    round_index, active = 0, False
    posts, pending, ranges = {}, {}, set()
    peaks, rail_peaks, competing, overlap, byte_count = 0, [0, 0], set(), False, 0
    operation = 1 if case['operation'] == 'read' else 2
    payload, block = case['payload'], case['block']
    bank, destination = 2 * payload + 192, payload + 128
    expected_ranges = {(parent, offset * block, block if offset < 3 else block + 1)
                       for parent in range(16) for offset in range(4)}
    for index, row in enumerate(rows):
        if row['seq'] != index + 1 or row['pid'] != process['native_pid']:
            raise ValueError('native rail sequence or PID mismatch')
        kind = row['kind']
        if kind == 'context':
            device = row.get('device')
            if (active or round_index or row['context'] not in (0, 1) or row['context'] in contexts
                    or device not in profile['rails'] or device in contexts.values()
                    or 'libmlx5' not in row['provider'] or row['provider'] not in maps):
                raise ValueError('native context is not a pinned physical RNIC/provider')
            contexts[row['context']] = device
        elif kind == 'rail_arm':
            if (active or round_index >= 3 or row['phase'] != round_index
                    or len(contexts) != 2 or row['operation'] != operation or row['expected'] != 64):
                raise ValueError('native rail arm mismatch')
            active = True
            posts, pending, ranges = {}, {}, set()
            peaks, rail_peaks, competing, overlap, byte_count = 0, [0, 0], set(), False, 0
        elif kind in ('rail_post', 'rail_complete'):
            context = row['context']
            if not active or row['phase'] != round_index or context not in contexts or row['operation'] != operation:
                raise ValueError('payload escaped its native context/round')
            qp = (context, row['cq'], row['qpn'])
            identity = (*qp, row['wr_id'])
            if not all(v > 0 for v in qp[1:]) or row['wr_id'] <= 0:
                raise ValueError('missing native WR identity')
            if kind == 'rail_post':
                relative = row['local'] - bases['requester'] - (destination if operation == 1 else 64)
                parent, offset = divmod(relative, bank)
                span = (parent, offset, row['length'])
                remote = bases['source'] + parent * bank + (64 if operation == 1 else destination) + offset
                if row['code'] != 0 or identity in posts or span not in expected_ranges or span in ranges or row['remote'] != remote:
                    raise ValueError('native slices duplicate, omit or escape a parent payload')
                posts[identity] = (parent, row['length'])
                pending[identity] = posts[identity]
                ranges.add(span)
                qps.add(qp)
                if sum(key[:3] == qp for key in pending) > 8 or sum(key[0] == context for key in qps) > 1:
                    raise ValueError('native SQ/QP bound exceeded')
                peaks = max(peaks, len(pending))
                for rail in (0, 1):
                    owners = [value[0] for key, value in pending.items() if key[0] == rail]
                    rail_peaks[rail] = max(rail_peaks[rail], len(owners))
                    if len(set(owners)) >= 2:
                        competing.add(rail)
                overlap |= {key[0] for key in pending} == {0, 1}
            else:
                if (identity not in pending or row['status'] != 0
                        or (operation == 1 and row['length'] != pending[identity][1])):
                    raise ValueError('native CQE lacks its exact pending successful WR')
                byte_count += pending.pop(identity)[1]
        elif kind == 'rail_done':
            receipt = receipts[round_index] if round_index < 3 else {}
            if (not active or row['phase'] != round_index or pending or ranges != expected_ranges
                    or len(posts) != 64 or row['posts'] != 64 or row['completed'] != 64
                    or byte_count != 16 * payload or row['bytes'] != byte_count
                    or competing != {0, 1} or not overlap or receipt != dict(
                        round=round_index, posts=64, completed=64, bytes=byte_count, peak=peaks,
                        rail_peak=rail_peaks, sequence=row['seq'], drained=True)):
                raise ValueError('native rail totals, concurrent owners or public drain disagree')
            active = False
            round_index += 1
        elif kind == 'destroy_qp':
            qp = (row['context'], row['cq'], row['qpn'])
            if active or round_index != 3 or row['code'] != 0 or qp not in qps or qp in destroyed:
                raise ValueError('payload QP cleanup occurred before accounting drained')
            destroyed.add(qp)
        elif kind == 'close_context':
            if (active or round_index != 3 or row['context'] not in contexts or row['code'] != 0
                    or any(qp not in destroyed for qp in qps if qp[0] == row['context'])):
                raise ValueError('native context closed before its payload QPs')
            del contexts[row['context']]
        else:
            raise ValueError('unexpected native fault or accounting receipt')
    if active or round_index != 3 or contexts or destroyed != qps or len(qps) != 2:
        raise ValueError('native accounting or resource cleanup is incomplete')
