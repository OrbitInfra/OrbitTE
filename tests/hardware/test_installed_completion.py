"""Negative controls for native completion identity and teardown evidence."""
import copy
import json
import unittest

import installed_completion


def fixture(killed, operation, generations):
    case = dict(fault_role=killed, operation=operation, parents=2, payload=1024, generations=generations)
    target = killed == 'requester'
    evidence = {role: {'stdout.log': '', 'result.json': '{}'} for role in ('source', 'requester')}
    phases = [dict(native_pid=200 + g, exit_code=-9 if target and g + 1 < generations else 0)
              for g in range(generations if target else 1)]
    evidence['requester']['result.json'] = json.dumps(dict(phases=phases, fault_library_sha256='a' * 64))
    logs = [[] for _ in phases]
    def event(g, phase, kind, **fields):
        rows = logs[g]
        row = dict(seq=len(rows) + 1, pid=phases[g]['native_pid'], phase=phase, kind=kind, **fields)
        rows.append(row)
        return row['seq']
    def receipt(role, kind, **fields):
        evidence[role]['stdout.log'] += 'ORBITTE_INSTALLED_' + kind + ' ' + json.dumps(fields) + '\n'
    for g in range(len(phases)):
        for context in range(2):
            event(g, 0, 'context', context=context, provider='/lib/libmlx5.so.1')
        evidence['requester']['maps-' + str(g) + '.txt'] = '/lib/libmlx5.so.1\n/tmp/liborbitte_cq_fault.so\n'
    for phase in range(generations):
        g = phase if target else 0
        for role, address in [('source', 10000), ('requester', 100000)]:
            receipt(role, 'PHASE', phase=phase, pid=phases[g]['native_pid'] if role == 'requester' else 100, address=address)
        if phase + 1 == generations:
            break
        op = 1 if operation == 'read' else 2
        event(g, phase, 'arm', operation=op, expected=2)
        for parent in range(2):
            identity = dict(context=parent, cq=10 + parent, qpn=20 + parent, wr_id=30 + parent)
            # CQ polling may beat the caller's post return. Join by identity.
            event(g, phase, 'lost', **identity, operation=op, status=0)
            sequence = event(g, phase, 'post', **identity, operation=op, code=0, length=1024,
                             local=100000 + parent * 2240 + (1152 if op == 1 else 64),
                             remote=10000 + parent * 2240 + (64 if op == 1 else 1152))
        receipt('requester', 'CQE_PENDING', phase=phase, pid=phases[g]['native_pid'], posts=2, lost=2,
                sequence=sequence, destroyed_qps=0)
        if not target:
            for parent in range(2):
                event(g, phase, 'destroy_qp', context=parent, cq=10 + parent, qpn=20 + parent, code=0)
            sequence = event(g, phase, 'disarm')
            receipt('requester', 'CQE_DRAIN', phase=phase, pid=phases[g]['native_pid'], posts=2, lost=2,
                    sequence=sequence, destroyed_qps=2, write_effect_unknown=op == 2)
    for g, rows in enumerate(logs):
        if phases[g]['exit_code'] == 0:
            for context in range(2):
                event(g, generations - 1, 'close_context', context=context, code=0)
        evidence['requester']['cq-fault-' + str(g) + '.jsonl'] = '\n'.join(json.dumps(row) for row in rows)
    return case, evidence


class InstalledCompletionEvidence(unittest.TestCase):
    def test_native_identity_outcome_and_destroy_proofs(self):
        for killed in ('source', 'requester'):
            for operation in ('read', 'write'):
                for generations in (2, 3):
                    case, evidence = fixture(killed, operation, generations)
                    installed_completion.verify(case, evidence)
                    controls = [('post', 'code', 5), ('post', 'local', 0), ('post', 'remote', 0),
                                ('post', 'length', 0), ('lost', 'qpn', 999), ('lost', 'wr_id', 999),
                                ('lost', 'status', 1), ('lost', 'seq', 999), ('lost', 'pid', 999),
                                ('lost', 'phase', 999), ('arm', 'expected', 999),
                                ('context', 'provider', '/fake/libmlx5.so.1')]
                    if killed == 'source':
                        controls += [('destroy_qp', 'code', 5), ('destroy_qp', 'qpn', 999)]
                    for kind, key, value in controls:
                        broken = copy.deepcopy(evidence)
                        path = 'cq-fault-0.jsonl'
                        rows = [json.loads(line) for line in broken['requester'][path].splitlines()]
                        next(row for row in rows if row['kind'] == kind)[key] = value
                        broken['requester'][path] = '\n'.join(json.dumps(row) for row in rows)
                        with self.subTest(killed=killed, operation=operation, generations=generations, kind=kind, key=key), self.assertRaises(ValueError):
                            installed_completion.verify(case, broken)
                    for label, change in [
                        ('missing-log', lambda r: r['requester'].update({'cq-fault-0.jsonl': ''})),
                        ('extra-log', lambda r: r['source'].update({'cq-fault-0.jsonl': '{}'})),
                        ('unmapped-tool', lambda r: r['requester'].update({'maps-0.txt': '/lib/libmlx5.so.1'})),
                        ('premature-proof', lambda r: r['requester'].update({'stdout.log': r['requester']['stdout.log'].replace('"destroyed_qps": 0', '"destroyed_qps": 1')})),
                        ('write-effect', lambda r: r['requester'].update({'stdout.log': r['requester']['stdout.log'].replace('"write_effect_unknown": true', '"write_effect_unknown": false')})),
                    ]:
                        if label == 'write-effect' and (killed != 'source' or operation != 'write'):
                            continue
                        broken = copy.deepcopy(evidence)
                        change(broken)
                        with self.subTest(control=label), self.assertRaises(ValueError):
                            installed_completion.verify(case, broken)


if __name__ == '__main__':
    unittest.main()
