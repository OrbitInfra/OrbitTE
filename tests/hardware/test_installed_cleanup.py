"""Negative controls for the installed consumer's native quarantine proof."""
import copy
import json
import unittest

import installed
import installed_completion
from test_installed_completion import fixture as completion_fixture


def fixture():
    case, evidence = completion_fixture('requester', 'read', 2)
    case.update(mode='installed-cleanup-failure', parents=1, num_lanes=1, mask=1,
                source='a', requester='b', source_kind='cpu', requester_kind='cpu',
                transport='rdma', rdma_events=False, block=1 << 20)
    record = evidence['requester']
    rows = [json.loads(line) for line in record['cq-fault-0.jsonl'].splitlines()]
    rows = [row for row in rows if row.get('qpn') != 21]
    for seq, row in enumerate(rows, 1):
        row['seq'] = seq
        if row['kind'] == 'arm':
            row['expected'] = 1
    prefix = 'ORBITTE_INSTALLED_CQE_PENDING '
    stdout = []
    for line in record['stdout.log'].splitlines():
        if line.startswith(prefix):
            value = json.loads(line[len(prefix):])
            value.update(posts=1, lost=1, sequence=len(rows))
            line = prefix + json.dumps(value)
        stdout.append(line)
    identity = dict(context=0, cq=10, qpn=20)
    for kind, fields in [
        ('arm_cleanup', dict(qpn=20)),
        ('destroy_qp', dict(**identity, code=5)),
        ('cleanup_error', dict(**identity, code=5, attempts=1, syscall_fault=True)),
        ('query_cleanup', dict(qpn=20, code=0, state=6)),
        ('query_cleanup', dict(qpn=20, code=0, state=6)),
    ]:
        rows.append(dict(seq=len(rows) + 1, pid=200, phase=0, kind=kind, **fields))
    proof = dict(phase=0, pid=200, qpn=20, sequence=len(rows), attempts=1, code=5,
                 retained_grants=1, retained_endpoints=1, sq_debt=1,
                 direct_bytes=case['payload'], inflight_bytes=case['payload'], batches=1,
                 unregistered=True, backing_retained=True, post_drop_native_query=True,
                 fixture_owners_dropped=True, batch_busy=True, memory_busy=True,
                 close_attempts=2, locally_drained=False)
    stdout.append('ORBITTE_INSTALLED_CLEANUP ' + json.dumps(proof))
    record['stdout.log'] = '\n'.join(stdout)
    record['cq-fault-0.jsonl'] = '\n'.join(json.dumps(row) for row in rows)
    return case, evidence


class InstalledCleanupEvidence(unittest.TestCase):
    def test_native_failure_is_joined_to_retained_public_owners(self):
        case, evidence = fixture()
        installed.validate(case)
        installed_completion.verify(case, evidence)
        for kind, key, value in [
            ('arm_cleanup', 'qpn', 999), ('destroy_qp', 'code', 0),
            ('cleanup_error', 'attempts', 2), ('cleanup_error', 'syscall_fault', False),
            ('cleanup_error', 'cq', 999), ('query_cleanup', 'qpn', 999),
            ('query_cleanup', 'code', 5), ('query_cleanup', 'state', 3),
        ]:
            broken = copy.deepcopy(evidence)
            rows = [json.loads(line) for line in broken['requester']['cq-fault-0.jsonl'].splitlines()]
            next(row for row in rows if row['kind'] == kind)[key] = value
            broken['requester']['cq-fault-0.jsonl'] = '\n'.join(json.dumps(row) for row in rows)
            with self.subTest(kind=kind, key=key), self.assertRaises(ValueError):
                installed_completion.verify(case, broken)
        for key, value in [('backing_retained', False), ('unregistered', False),
                           ('post_drop_native_query', False), ('fixture_owners_dropped', False),
                           ('memory_busy', False), ('batch_busy', False), ('locally_drained', True),
                           ('direct_bytes', 0), ('inflight_bytes', 0), ('sq_debt', 0),
                           ('batches', 0), ('sequence', 9), ('pid', 999), ('attempts', 2)]:
            broken = copy.deepcopy(evidence)
            lines = broken['requester']['stdout.log'].splitlines()
            prefix, raw = lines[-1].split(' ', 1)
            proof = json.loads(raw)
            proof[key] = value
            lines[-1] = prefix + ' ' + json.dumps(proof)
            broken['requester']['stdout.log'] = '\n'.join(lines)
            with self.subTest(proof=key), self.assertRaises(ValueError):
                installed_completion.verify(case, broken)

    def test_profile_does_not_expand_the_declared_fault(self):
        case, _ = fixture()
        for update in [dict(parents=2), dict(num_lanes=2), dict(mask=3),
                       dict(generations=3), dict(fault_role='source')]:
            with self.subTest(profile=update), self.assertRaises(ValueError):
                installed.validate({**case, **update})


if __name__ == '__main__':
    unittest.main()
