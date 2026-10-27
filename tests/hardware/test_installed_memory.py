"""Reject false installed MR/MW retention, native identity and recovery evidence."""
import copy
import json
import unittest

import installed
import run


class InstalledMemoryEvidence(unittest.TestCase):
    profile = {'rails': ['rail-a', 'rail-b']}

    def case(self, kind='mr', role='requester'):
        return dict(mode='installed-memory-cleanup', cleanup_kind=kind, fault_role=role,
                    transport='rdma', source_kind='cpu', requester_kind='cpu', source='a', requester='b',
                    operation='write', parents=1, payload=65536, block=65536,
                    rdma_events=True, num_lanes=1, mask=2)

    def evidence(self, case):
        evidence = {}
        resource = 1 if case['cleanup_kind'] == 'mr' else 2
        for role, pid in [('source', 11), ('requester', 12)]:
            target = case['fault_role'] == role
            rows = [('ORBITTE_INSTALLED_NIC ', dict(name=name, pci=str(index)))
                    for _ in range(2) for index, name in enumerate(self.profile['rails'])]
            rows += [('ORBITTE_INSTALLED_ROUND ', dict(phase=phase, round=n, opened_qps=1, closed_qps=0,
                     opened_mrs=4, completed_grants=n + 1, reused_qps=n, read_bytes=0,
                     write_bytes=(n + 1) * 65536 if role == 'requester' else 0)) for phase in (0, 1) for n in range(3)]
            rows += [('ORBITTE_INSTALLED_MEMORY_OWNER ', dict(pid=pid, address=1024, target=target)),
                     ('ORBITTE_INSTALLED_MEMORY_REPLACED ', dict(pid=pid, address=2048, endpoint='host:1234',
                      old_epoch='old', new_epoch='new', same_process=True, stale_epoch_before_qp=True)),
                     ('ORBITTE_INSTALLED_MEMORY_RESULT ', dict(protocol=installed.PROTOCOL, pid=pid, target=target,
                      resource=resource, healthy_rounds=6, full_bytes_and_guards=True, fresh_engine_drained=True,
                      fresh_backing_released=True, old_backing_retained=target))]
            record = dict.fromkeys(['cq-fault-0.jsonl', 'cq-fault-1.jsonl', 'cq-fault-2.jsonl'])
            supervisor = dict(native_pid=pid, exit_code=0, child_reaped=True, resource_locks_released=True)
            record['maps.txt'] = '/lib/libibverbs.so.1\n/lib/libmlx5.so.1\n'
            if resource == 2 and role == 'requester':
                rows.append(('ORBITTE_INSTALLED_MEMORY_REQUEST ', dict(pid=pid, failed=True, locally_drained=True)))
            if target:
                supervisor['fault_library_sha256'] = 'a' * 64
                record['maps.txt'] += '/tmp/liborbitte_cq_fault.so\n'
                native = [dict(kind='context', context=i, provider='/lib/libmlx5.so.1') for i in (0, 1)]
                native += [dict(kind='memory_arm', context=0, resource=resource, source=int(role == 'source'), address=1024)]
                if resource == 2:
                    native += [dict(kind='memory_bind', context=0, mr=101, mw=102, qpn=8, wr_id=9, key=10, code=0)]
                identity = dict(resource=resource, context=0, mr=101, mw=102 if resource == 2 else 0, pd=103)
                native += [dict(identity, kind='memory_error', address=1024, length=131264, lkey=7, rkey=10,
                                code=5, attempts=1, syscall_fault=True),
                           dict(identity, kind='memory_retained', code=16, keys_intact=True),
                           dict(identity, kind='memory_retained', code=16, keys_intact=True)]
                native = [dict(row, pid=pid, seq=i + 1, phase=0) for i, row in enumerate(native)]
                proof = {k: v for k, v in identity.items() if k != 'context'}
                proof.update(pid=pid, sequence=native[-2]['seq'], attempts=1, code=5, unregistered=True,
                             close_attempts=2, locally_drained=False, fixture_owners_dropped=True,
                             backing_retained=True, memory_busy=resource == 2, persistent_mrs=1,
                             persistent_mr_bytes=131264, retained_mrs=int(resource == 1), source_mrs=1,
                             retained_windows=int(resource == 2), retained_grants=int(resource == 2),
                             key_indices=4, burned_keys=4, sq_debt=0, rq_debt=0,
                             direct_bytes=65536 if resource == 2 else 0)
                rows.append(('ORBITTE_INSTALLED_MEMORY_RETAINED ', proof))
                record['cq-fault-0.jsonl'] = '\n'.join(map(json.dumps, native))
            record['stdout.log'] = '\n'.join(prefix + json.dumps(row) for prefix, row in rows)
            record['result.json'] = json.dumps(supervisor)
            evidence[role] = record
        return evidence

    def test_retention_and_replacement_require_native_identity_backing_quota_and_reaping(self):
        for kind, role in [('mr', 'requester'), ('mr', 'source'), ('mw', 'source')]:
            case = self.case(kind, role)
            good = self.evidence(case)
            installed.verify(self.profile, case, good)
            self.assertIsNone(run.restart_plan(case))
            for name, file, old, new in [
                ('native-eio', 'cq-fault-0.jsonl', '"code": 5', '"code": 0'),
                ('native-probe', 'cq-fault-0.jsonl', '"code": 16', '"code": 0'),
                ('native-retry', 'cq-fault-0.jsonl', '"attempts": 1', '"attempts": 2'),
                ('native-allocation', 'cq-fault-0.jsonl', '"address": 1024', '"address": 4096'),
                ('native-owner', 'cq-fault-0.jsonl', '"mr": 101', '"mr": 104'),
                ('released-key', 'cq-fault-0.jsonl', '"keys_intact": true', '"keys_intact": false'),
                ('reaped', 'result.json', '"child_reaped": true', '"child_reaped": false'),
                ('foreign-pid', 'result.json', f'"native_pid": {11 if role == "source" else 12}', '"native_pid": 99'),
                ('backing', 'stdout.log', '"backing_retained": true', '"backing_retained": false'),
                ('registration-quota', 'stdout.log', '"persistent_mr_bytes": 131264', '"persistent_mr_bytes": 0'),
                ('false-drain', 'stdout.log', '"locally_drained": false', '"locally_drained": true'),
                ('stale-epoch', 'stdout.log', '"new_epoch": "new"', '"new_epoch": "old"'),
                ('fresh-leak', 'stdout.log', '"fresh_backing_released": true', '"fresh_backing_released": false'),
            ] + ([('grant-quota', 'stdout.log', '"direct_bytes": 65536', '"direct_bytes": 0'),
                  ('key-quota', 'stdout.log', '"key_indices": 4', '"key_indices": 0')]
                 if kind == 'mw' else []):
                bad = copy.deepcopy(good)
                self.assertIn(old, bad[role][file])
                bad[role][file] = bad[role][file].replace(old, new)
                with self.subTest(kind=kind, role=role, control=name), self.assertRaises(ValueError):
                    installed.verify(self.profile, case, bad)

    def test_profile_and_preload_cannot_expand_fault_scope(self):
        for invalid in [dict(cleanup_kind='pd'), dict(fault_role='all'), dict(cleanup_kind='mw'),
                        dict(generations=2), dict(mask=3), dict(num_lanes=2), dict(parents=2),
                        dict(payload=131072), dict(mode='installed-host')]:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                installed.validate({**self.case(), **invalid})
        _, environment = installed.command(dict(nodes={'a': dict(bind='host:0')}, rails=['r0', 'r1'], gid_index=3),
                                            self.case(), 'a', 'source', '/tmp/run')
        self.assertEqual(environment['ORBITTE_CLEANUP_ROLE'], 'requester')
        self.assertNotIn('ORBITTE_RESTART_GENERATIONS', environment)


if __name__ == '__main__':
    unittest.main()
