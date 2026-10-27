"""Reject incomplete DMA interruption evidence, including completed/no-op copies."""
import copy
import json
import unittest

import installed


def fixture(operation):
    case = dict(mode='installed-inflight-dma', transport='rdma', source_kind='cpu',
                requester_kind='cpu', source='a', requester='b', operation=operation,
                rdma_events=False, parents=1, payload=65536, block=65536, num_lanes=1, mask=1)
    profile = {'rails': ['mlx5_1', 'mlx5_2']}
    evidence = {}
    for role, pid, address in [('source', 100, 1000), ('requester', 200, 2000)]:
        destination = (role == 'source') != (operation == 'read')
        rows = [('ORBITTE_INSTALLED_NIC', dict(name=n, pci=str(i))) for i, n in enumerate(profile['rails'])]
        rows.append(('ORBITTE_INSTALLED_DMA_OWNER', dict(pid=pid, address=address)))
        for phase in range(2):
            if phase:
                if role == 'requester':
                    rows.append(('ORBITTE_INSTALLED_DMA_NATIVE', dict(pid=pid, qpn=7, wr_id=9,
                                 sequence=8, posts=1, modifications=1, failed=1, completed=0,
                                 destroyed=1, write_effect_unknown=operation == 'write')))
                rows.append(('ORBITTE_INSTALLED_DMA_DRAIN', dict(pid=pid, sq_debt=0, rq_debt=0,
                             grants=0, windows=0, batches=0, direct_bytes=0, inflight_bytes=0,
                             persistent_mrs=4, closed_qps=1, completion_errors=1,
                             progress_failures=1, progress_recoveries=1, locally_drained=True)))
                rows.append(('ORBITTE_INSTALLED_DMA_BYTES', dict(pid=pid, destination=destination,
                             copied=32768 if destination else 0, unchanged=32768 if destination else 0,
                             ambiguous=0, full_bytes_and_guards=True, memory_writable=True)))
            for r in range(3):
                n = phase * 3 + r + 1
                rows.append(('ORBITTE_INSTALLED_ROUND', dict(phase=phase, round=r, opened_qps=phase+1,
                             closed_qps=phase, reused_qps=n, completed_grants=n, opened_mrs=4,
                             read_bytes=n*65536 if role == 'requester' and operation == 'read' else 0,
                             write_bytes=n*65536 if role == 'requester' and operation == 'write' else 0)))
        rows.append(('ORBITTE_INSTALLED_RESULT', dict(protocol=installed.PROTOCOL, status='passed',
                     role=role, operation=operation, parents=1, payload=65536, rounds=3,
                     full_bytes_and_guards=True, locally_drained=True)))
        supervisor = dict(native_pid=pid, exit_code=0, child_reaped=True, resource_locks_released=True)
        maps = '/lib/libibverbs.so.1\n/lib/libmlx5.so.1\n'
        if role == 'requester':
            maps += '/tmp/liborbitte_cq_fault.so\n'
            supervisor['fault_library_sha256'] = 'a' * 64
        evidence[role] = {'stdout.log': '\n'.join(tag+' '+json.dumps(row) for tag, row in rows),
                          'maps.txt': maps, 'result.json': json.dumps(supervisor)}
    identity = dict(context=0, cq=10, qpn=7)
    rows = [dict(kind='context', context=i, provider='/lib/libmlx5.so.1') for i in range(2)]
    rows += [dict(kind='dma_arm', operation=1 if operation == 'read' else 2),
             dict(kind='dma_post', **identity, wr_id=9, code=0, length=65536,
                  operation=1 if operation == 'read' else 2,
                  local=2000+(65664 if operation == 'read' else 64),
                  remote=1000+(64 if operation == 'read' else 65664)),
             dict(kind='dma_error', **identity, wr_id=9, code=0, state=6),
             dict(kind='dma_completion', **identity, wr_id=9, status=5, vendor_err=0),
             dict(kind='destroy_qp', **identity, code=0), dict(kind='dma_disarm')]
    rows += [dict(kind='close_context', context=i, code=0) for i in range(2)]
    evidence['requester']['cq-fault-0.jsonl'] = '\n'.join(
        json.dumps(dict(seq=i, pid=200, phase=0, **r)) for i, r in enumerate(rows, 1))
    return profile, case, evidence


def change_public(record, suffix, key, value):
    prefix = 'ORBITTE_INSTALLED_' + suffix + ' '
    lines = record['stdout.log'].splitlines()
    for index, line in enumerate(lines):
        if line.startswith(prefix):
            row = json.loads(line[len(prefix):]); row[key] = value
            lines[index] = prefix + json.dumps(row)
            break
    record['stdout.log'] = '\n'.join(lines)


class InflightEvidence(unittest.TestCase):
    def test_read_write_require_partial_bytes_and_original_process_recovery(self):
        for operation in ('read', 'write'):
            profile, case, evidence = fixture(operation)
            installed.verify(profile, case, evidence)
            destination = 'requester' if operation == 'read' else 'source'
            controls = [(destination, 'DMA_BYTES', 'copied', 0),
                        (destination, 'DMA_BYTES', 'unchanged', 0),
                        (destination, 'DMA_BYTES', 'ambiguous', 1),
                        (destination, 'DMA_BYTES', 'memory_writable', False),
                        ('requester', 'DMA_NATIVE', 'sequence', 7),
                        ('requester', 'DMA_NATIVE', 'completed', 1),
                        ('requester', 'DMA_DRAIN', 'sq_debt', 1),
                        ('requester', 'DMA_DRAIN', 'persistent_mrs', 0),
                        ('source', 'DMA_DRAIN', 'rq_debt', 1),
                        ('source', 'DMA_OWNER', 'address', 999),
                        ('requester', 'DMA_DRAIN', 'locally_drained', False),
                        ('source', 'ROUND', 'opened_mrs', 8)]
            for role, suffix, key, value in controls:
                broken = copy.deepcopy(evidence)
                change_public(broken[role], suffix, key, value)
                with self.subTest(operation=operation, suffix=suffix, key=key), self.assertRaises(ValueError):
                    installed.verify(profile, case, broken)
            for kind, key, value in [('dma_post', 'code', 5), ('dma_post', 'length', 1),
                                     ('dma_error', 'qpn', 8), ('dma_error', 'code', 5),
                                     ('dma_completion', 'wr_id', 10), ('dma_completion', 'status', 0),
                                     ('destroy_qp', 'code', 5), ('dma_disarm', 'pid', 201)]:
                broken = copy.deepcopy(evidence)
                rows = [json.loads(line) for line in broken['requester']['cq-fault-0.jsonl'].splitlines()]
                next(r for r in rows if r['kind'] == kind)[key] = value
                broken['requester']['cq-fault-0.jsonl'] = '\n'.join(json.dumps(r) for r in rows)
                with self.subTest(operation=operation, kind=kind, key=key), self.assertRaises(ValueError):
                    installed.verify(profile, case, broken)

    def test_profile_cannot_add_parents_or_restart(self):
        _, case, _ = fixture('read')
        for update in [dict(parents=2), dict(num_lanes=2), dict(mask=3),
                       dict(payload=131072), dict(generations=2), dict(fault_role='requester')]:
            with self.subTest(update=update), self.assertRaises(ValueError):
                installed.validate({**case, **update})


if __name__ == '__main__':
    unittest.main()
