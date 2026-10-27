"""Reject incomplete native dependency and actual-backing evidence."""
import copy
import json
import unittest

import installed
import installed_completion
import supervise
from test_installed_cleanup import fixture as cleanup_fixture


def fixture(kind):
    case, evidence = cleanup_fixture()
    case.update(cleanup_kind=kind, rdma_events=kind != 'cq')
    resource = {'cq': 1, 'channel': 2, 'pd': 3}[kind]
    debt = kind != 'pd'
    record = evidence['requester']
    rows = [json.loads(line) for line in record['cq-fault-0.jsonl'].splitlines()]
    rows = [r for r in rows if r['kind'] in (('context', 'arm', 'post', 'lost') if debt else ('context',))]
    identity = dict(context=0, pd=40, cq=10, channel=30 if case['rdma_events'] else 0, qpn=20)

    def add(name, **fields):
        rows.append(dict(seq=len(rows) + 1, pid=200, phase=0, kind=name, **fields))

    add('arm_dependency', resource=resource, **identity)
    add('destroy_qp', context=0, cq=10, qpn=20, code=0)
    if kind != 'cq':
        for cq in (10, 11):
            add('dependency_release', owner=1, handle=cq, context=0, code=0)
    if kind == 'pd':
        add('dependency_release', owner=2, handle=30, context=0, code=0)
    add('dependency_error', resource=resource, **identity, code=16, attempts=1, blocker=50)
    for _ in range(2):
        add('dependency_query', resource=resource, context=0, blocker=50, code=16 if kind == 'pd' else 0, live=True)
    lines = []
    for line in record['stdout.log'].splitlines():
        if not debt and line.startswith('ORBITTE_INSTALLED_CQE_PENDING '):
            continue
        if line.startswith('ORBITTE_INSTALLED_CLEANUP '):
            prefix, raw = line.split(' ', 1)
            proof = json.loads(raw)
            proof.update(code=16, resource=resource, sequence=len(rows), retained_endpoints=0,
                         retained_cqs=int(kind == 'cq'), retained_channels=int(kind == 'channel'),
                         retained_devices=int(kind == 'pd'), persistent_mrs=int(debt),
                         retained_grants=int(debt), sq_debt=int(debt), batches=int(debt),
                         direct_bytes=case['payload'] * int(debt), inflight_bytes=case['payload'] * int(debt),
                         memory_busy=debt, batch_busy=debt, backing_retained=debt)
            line = prefix + ' ' + json.dumps(proof)
        lines.append(line)
    record['stdout.log'] = '\n'.join(lines)
    record['cq-fault-0.jsonl'] = '\n'.join(map(json.dumps, rows))
    return case, evidence


class InstalledDependencyEvidence(unittest.TestCase):
    def test_native_failure_and_actual_owner_lifetime(self):
        for kind in ('cq', 'channel', 'pd'):
            case, evidence = fixture(kind)
            installed.validate(case)
            installed_completion.verify(case, evidence)
            for event, key, value in [
                ('arm_dependency', 'qpn', 999), ('dependency_error', 'cq', 999),
                ('dependency_error', 'code', 0), ('dependency_error', 'attempts', 2),
                ('dependency_error', 'blocker', 0), ('destroy_qp', 'code', 5),
                ('dependency_query', 'blocker', 999), ('dependency_query', 'live', False),
                ('dependency_query', 'code', 5),
            ]:
                broken = copy.deepcopy(evidence)
                rows = [json.loads(line) for line in broken['requester']['cq-fault-0.jsonl'].splitlines()]
                next(r for r in rows if r['kind'] == event)[key] = value
                broken['requester']['cq-fault-0.jsonl'] = '\n'.join(map(json.dumps, rows))
                with self.subTest(kind=kind, event=event, key=key), self.assertRaises(ValueError):
                    installed_completion.verify(case, broken)
            broken = copy.deepcopy(evidence)
            rows = [json.loads(line) for line in broken['requester']['cq-fault-0.jsonl'].splitlines()]
            rows.append(dict(seq=len(rows) + 1, pid=200, phase=0, kind='close_context', context=0, code=0))
            broken['requester']['cq-fault-0.jsonl'] = '\n'.join(map(json.dumps, rows))
            with self.subTest(kind=kind, premature_parent_close=True), self.assertRaises(ValueError):
                installed_completion.verify(case, broken)
            for key, value in [('backing_retained', kind == 'pd'), ('fixture_owners_dropped', False),
                               ('persistent_mrs', int(kind == 'pd')), ('sq_debt', int(kind == 'pd')),
                               ('locally_drained', True), ('sequence', 999), ('retained_endpoints', 1),
                               ('batches', int(kind == 'pd')), ('post_drop_native_query', False)]:
                broken = copy.deepcopy(evidence)
                lines = broken['requester']['stdout.log'].splitlines()
                prefix, raw = lines[-1].split(' ', 1)
                proof = json.loads(raw); proof[key] = value
                lines[-1] = prefix + ' ' + json.dumps(proof)
                broken['requester']['stdout.log'] = '\n'.join(lines)
                with self.subTest(kind=kind, proof=key), self.assertRaises(ValueError):
                    installed_completion.verify(case, broken)

    def test_fault_scope_and_completion_mode(self):
        for kind in ('cq', 'channel', 'pd'):
            case, _ = fixture(kind)
            env = {'ORBITTE_HOST_MODE': case['mode'], 'ORBITTE_SHARED_ROLE': 'requester',
                   'ORBITTE_CLEANUP_KIND': kind, 'ORBITTE_RDMA_EVENTS': str(int(case['rdma_events']))}
            self.assertTrue(supervise.fault_scope(env))
            self.assertFalse(supervise.fault_scope({**env, 'ORBITTE_SHARED_ROLE': 'source'}))
            for update in [dict(fault_role='source'), dict(parents=2), dict(mask=3),
                           dict(cleanup_kind='device'), dict(generations=3)]:
                with self.subTest(kind=kind, update=update), self.assertRaises(ValueError):
                    installed.validate({**case, **update})
        case, _ = fixture('channel')
        with self.assertRaises(ValueError):
            installed.validate({**case, 'rdma_events': False})
        self.assertFalse(supervise.fault_scope({**env, 'ORBITTE_CLEANUP_KIND': 'channel', 'ORBITTE_RDMA_EVENTS': '0'}))


if __name__ == '__main__':
    unittest.main()
