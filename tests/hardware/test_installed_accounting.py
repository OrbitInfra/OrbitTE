"""Reject corrupted native accounting and false concurrency claims."""
import copy
import json
import unittest

import installed
from supervise import fault_scope


class InstalledRailAccounting(unittest.TestCase):
    def setUp(self):
        self.profile = dict(rails=['mlx5_3', 'mlx5_4'])
        self.case = dict(mode='installed-rail-accounting', transport='rdma', source_kind='cpu',
                         requester_kind='cpu', source='a', requester='b', operation='read',
                         rdma_events=False, parents=16, payload=(4 << 20) + 1,
                         block=1 << 20, num_lanes=1, mask=3)

    def evidence(self):
        case, native, summaries = self.case, [], []
        payload, block = case['payload'], case['block']
        bank, destination = 2 * payload + 192, payload + 128
        operation = 1 if case['operation'] == 'read' else 2
        def append(kind, phase=0, **fields):
            native.append(dict(kind=kind, phase=phase, seq=len(native) + 1, pid=20, **fields))
        for rail, name in enumerate(self.profile['rails']):
            append('context', context=rail, device=name, provider='/lib/libmlx5.so.1')
        for phase in range(3):
            append('rail_arm', phase, operation=operation, expected=64)
            for group in range(2):
                for pair in range(2):
                    batch = []
                    for rail in (0, 1):
                        for parent in range(group * 8, (group + 1) * 8):
                            offset = (pair * 2 + rail) * block
                            length = block + int(pair == 1 and rail == 1)
                            identity = dict(context=rail, cq=10 + rail, qpn=7 + rail,
                                            wr_id=1 + phase * 64 + parent * 4 + pair * 2 + rail,
                                            operation=operation)
                            append('rail_post', phase, **identity, code=0, length=length,
                                   local=2000 + parent * bank + (destination if operation == 1 else 64) + offset,
                                   remote=1000 + parent * bank + (64 if operation == 1 else destination) + offset)
                            batch.append((identity, length))
                    for identity, length in batch:
                        append('rail_complete', phase, **identity, status=0,
                               length=length if operation == 1 else 0)
            append('rail_done', phase, posts=64, completed=64, bytes=16 * payload)
            summaries.append(dict(round=phase, posts=64, completed=64, bytes=16 * payload,
                                  peak=16, rail_peak=[8, 8], sequence=len(native), drained=True))
        for rail in (0, 1):
            append('destroy_qp', 2, context=rail, cq=10 + rail, qpn=7 + rail, code=0)
            append('close_context', 2, context=rail, code=0)
        evidence = {}
        for role, pid, base in [('source', 10, 1000), ('requester', 20, 2000)]:
            lines = [('ORBITTE_INSTALLED_NIC ', dict(name=name, pci=str(i)))
                     for i, name in enumerate(self.profile['rails'])]
            lines += [('ORBITTE_INSTALLED_RAIL_MEMORY ', dict(pid=pid, address=base))]
            for phase in range(3):
                lines.append(('ORBITTE_INSTALLED_ROUND ', dict(phase=0, round=phase, opened_qps=2,
                    closed_qps=0, reused_qps=phase * 64, completed_grants=(phase + 1) * 64, opened_mrs=4,
                    read_bytes=(phase + 1) * 16 * payload if role == 'requester' and operation == 1 else 0,
                    write_bytes=(phase + 1) * 16 * payload if role == 'requester' and operation == 2 else 0)))
            if role == 'requester':
                lines += [('ORBITTE_INSTALLED_RAIL_ROUND ', row) for row in summaries]
            lines.append(('ORBITTE_INSTALLED_RESULT ', dict(protocol=installed.PROTOCOL, status='passed',
                role=role, operation=case['operation'], parents=16, payload=payload, rounds=3,
                full_bytes_and_guards=True, locally_drained=True)))
            result = dict(exit_code=0, child_reaped=True, resource_locks_released=True, native_pid=pid)
            maps = '/lib/libibverbs.so.1\n/lib/libmlx5.so.1\n'
            if role == 'requester':
                result['fault_library_sha256'] = 'a' * 64
                maps += '/lib/liborbitte_cq_fault.so\n'
            evidence[role] = {'stdout.log': '\n'.join(prefix + json.dumps(row) for prefix, row in lines),
                              'result.json': json.dumps(result), 'maps.txt': maps}
        evidence['requester']['cq-fault-0.jsonl'] = '\n'.join(map(json.dumps, native))
        return evidence

    def test_exact_read_and_write_ledgers_pass(self):
        for operation in ('read', 'write'):
            self.case['operation'] = operation
            installed.verify(self.profile, self.case, self.evidence())

    def test_native_corruption_cannot_be_hidden_by_successful_public_totals(self):
        good = self.evidence()
        for name, kind, update in [
            ('wrong-rnic', 'context', dict(device='mlx5_9')),
            ('wrong-provider', 'context', dict(provider='/lib/fake.so')),
            ('wrong-cqe-owner', 'rail_complete', dict(qpn=999)),
            ('failed-cqe', 'rail_complete', dict(status=5)),
            ('wrong-length', 'rail_complete', dict(length=9)),
            ('overlapping-slice', 'rail_post', dict(local=2000)),
            ('wrong-remote', 'rail_post', dict(remote=7)),
            ('unaccepted-post', 'rail_post', dict(code=12)),
            ('wrong-native-bytes', 'rail_done', dict(bytes=1)),
            ('premature-cleanup', 'rail_arm', dict(kind='close_context', context=0, code=0)),
            ('failed-destroy', 'destroy_qp', dict(code=16)),
            ('unexpected-loss', 'rail_complete', dict(kind='lost')),
        ]:
            evidence = copy.deepcopy(good)
            rows = list(map(json.loads, evidence['requester']['cq-fault-0.jsonl'].splitlines()))
            next(row for row in rows if row['kind'] == kind).update(update)
            evidence['requester']['cq-fault-0.jsonl'] = '\n'.join(map(json.dumps, rows))
            with self.subTest(control=name), self.assertRaises(ValueError):
                installed.verify(self.profile, self.case, evidence)
        # Serialize each native post/completion, retain correct bytes and forge
        # matching peak receipts: the cross-parent contention proof still fails.
        evidence = copy.deepcopy(good)
        rows = list(map(json.loads, evidence['requester']['cq-fault-0.jsonl'].splitlines()))
        for start in [i for i, row in enumerate(rows) if row['kind'] == 'rail_arm']:
            window = rows[start + 1:start + 129]
            posts = [row for row in window if row['kind'] == 'rail_post']
            completes = {row['wr_id']: row for row in window if row['kind'] == 'rail_complete'}
            rows[start + 1:start + 129] = [row for post in posts for row in (post, completes[post['wr_id']])]
        for index, row in enumerate(rows): row['seq'] = index + 1
        evidence['requester']['cq-fault-0.jsonl'] = '\n'.join(map(json.dumps, rows))
        evidence['requester']['stdout.log'] = evidence['requester']['stdout.log'].replace('"peak": 16', '"peak": 1').replace('"rail_peak": [8, 8]', '"rail_peak": [1, 1]')
        with self.assertRaises(ValueError):
            installed.verify(self.profile, self.case, evidence)

    def test_profile_and_observer_scope_are_bounded(self):
        for change in [dict(parents=12), dict(payload=4 << 20), dict(block=2 << 20), dict(num_lanes=3), dict(mask=1), dict(fault_role='source')]:
            with self.subTest(change=change), self.assertRaises(ValueError):
                installed.validate({**self.case, **change})
        for role in ('source', 'requester'):
            self.assertEqual(fault_scope(dict(ORBITTE_HOST_MODE=self.case['mode'], ORBITTE_SHARED_ROLE=role)), role == 'requester')
