"""Negative controls for outstanding-RQ installed interruption receipts."""
import copy
import json
import unittest

import installed
import run
import test_installed_restart


class InstalledInterruptionEvidence(unittest.TestCase):
    def test_pending_owners_and_survivor_drain_are_required(self):
        for killed in ('source', 'requester'):
            fixture = test_installed_restart.InstalledRestartEvidence()
            fixture.setUp()
            fixture.case.update(mode='installed-interruption', payload=1 << 20, fault_role=killed)
            case = fixture.case
            evidence = fixture.evidence()
            for role, record in evidence.items():
                source = role == 'source'
                rows = []
                for line in record['stdout.log'].splitlines():
                    prefix, raw = line.split(' ', 1)
                    row = json.loads(raw)
                    if prefix == 'ORBITTE_INSTALLED_KILL':
                        row['rq_debt'] = case['parents'] if source else 0
                    rows.append((prefix, row))
                for phase in range(case['generations'] - 1):
                    start = next(r for p, r in rows if p == 'ORBITTE_INSTALLED_PHASE' and r['phase'] == phase)
                    baseline = next(r for p, r in rows if p == 'ORBITTE_INSTALLED_ROUND' and r['phase'] == phase and r['round'] == 2)
                    pending = dict(phase=phase, pid=start['pid'], held_grants=case['parents'], active_grants=case['parents'],
                                   sq_debt=0, rq_debt=case['parents'] if source else 0,
                                   windows=case['parents'] if source else 0, batches=0 if source else case['parents'],
                                   memory_busy=True, batch_busy=not source, progress_failures=0, progress_recoveries=0,
                                   **{k: baseline[k] for k in ('opened_qps', 'read_bytes', 'write_bytes')})
                    rows.append(('ORBITTE_INSTALLED_PENDING', pending))
                    if role != killed:
                        rows.append(('ORBITTE_INSTALLED_DRAIN', dict(phase=phase, pid=start['pid'],
                                     failed_batches=0 if source else case['parents'], sq_debt=0, rq_debt=0,
                                     grants=0, windows=0, batches=0, persistent_mrs=4,
                                     read_bytes=baseline['read_bytes'], write_bytes=baseline['write_bytes'],
                                     locally_drained=True, bytes_verified=True, payload_completed=False, memory_writable=True,
                                     progress_failures=4, progress_recoveries=4)))
                record['stdout.log'] = '\n'.join(p + ' ' + json.dumps(r) for p, r in rows)
            installed.verify(fixture.profile, case, evidence)
            self.assertEqual(run.restart_plan(case), (killed, 3))
            survivor = 'requester' if killed == 'source' else 'source'
            controls = [
                ('source', 'PENDING', 'rq_debt', 0), ('source', 'PENDING', 'windows', 0),
                ('source', 'PENDING', 'held_grants', 0), ('source', 'PENDING', 'memory_busy', False),
                ('requester', 'PENDING', 'batch_busy', False), ('requester', 'PENDING', 'pid', -1),
                ('requester', 'PENDING', 'read_bytes', -1), (survivor, 'DRAIN', 'batches', 1),
                (survivor, 'DRAIN', 'persistent_mrs', 0), (survivor, 'DRAIN', 'locally_drained', False),
                (survivor, 'DRAIN', 'bytes_verified', False), (survivor, 'DRAIN', 'memory_writable', False),
                (survivor, 'DRAIN', 'progress_recoveries', 0), (survivor, 'DRAIN', 'read_bytes', -1), (survivor, 'DRAIN', 'phase', 99),
            ]
            for role, kind, key, value in controls:
                broken = copy.deepcopy(evidence)
                lines = broken[role]['stdout.log'].splitlines()
                index = next(i for i, line in enumerate(lines) if line.startswith('ORBITTE_INSTALLED_' + kind + ' '))
                prefix, raw = lines[index].split(' ', 1)
                row = json.loads(raw)
                row[key] = value
                lines[index] = prefix + ' ' + json.dumps(row)
                broken[role]['stdout.log'] = '\n'.join(lines)
                with self.subTest(killed=killed, role=role, kind=kind, key=key), self.assertRaises(ValueError):
                    installed.verify(fixture.profile, case, broken)
            with self.assertRaises(ValueError):
                installed.validate({**case, 'payload': case['block'] + 1})


if __name__ == '__main__':
    unittest.main()
