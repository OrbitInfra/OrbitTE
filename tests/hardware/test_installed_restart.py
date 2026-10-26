"""Controls for stale-authority, persistent-owner and exact-child restart evidence."""
import copy
import json
import unittest

import installed
import run


class InstalledRestartEvidence(unittest.TestCase):
    def setUp(self):
        self.profile = dict(rails=['mlx5_1', 'mlx5_2'])
        self.case = dict(mode='installed-restart', source='a', requester='b',
                         source_kind='cpu', requester_kind='cpu', transport='rdma',
                         operation='read', rdma_events=True, parents=16, payload=(6 << 20) + 1,
                         block=1 << 20, num_lanes=3, mask=3, fault_role='source', generations=3)

    def evidence(self):
        count = self.case['generations']
        result = {}
        stamps = {}
        for role in ('source', 'requester'):
            target = role == self.case['fault_role']
            stamps[role] = [dict(phase=g, pid=(100 if role == 'source' else 200) + (g if target else 0),
                                endpoint=role + ':1', epoch=role + str(g if target else 0),
                                region_id=1, address=1024 + (g if target else 0)) for g in range(count)]
        for role in ('source', 'requester'):
            target = role == self.case['fault_role']
            peer = 'requester' if role == 'source' else 'source'
            rows = []
            for g in range(count if target else 1):
                for index, name in enumerate(self.profile['rails']):
                    rows.append(('ORBITTE_INSTALLED_NIC ', dict(name=name, pci=str(index))))
            process_phases = [dict(generation=g, native_pid=stamps[role][g]['pid'],
                                   exit_code=-9 if target and g + 1 < count else 0,
                                   child_reaped=True, intentional_sigkill=target and g + 1 < count)
                              for g in range(count if target else 1)]
            byte_count = self.case['parents'] * self.case['payload']
            for g in range(count):
                baseline = 0 if target else g * 3 * byte_count
                opened = 0 if target else g * 6
                rows.append(('ORBITTE_INSTALLED_PHASE ', dict(**stamps[role][g],
                             peer_endpoint=stamps[peer][g]['endpoint'], peer_epoch=stamps[peer][g]['epoch'],
                             opened_qps=opened, opened_mrs=4,
                             read_bytes=baseline if role == 'requester' else 0, write_bytes=0,
                             stale_bytes_unchanged=True)))
                for index in range(3):
                    base_round = 0 if target else g * 3
                    rows.append(('ORBITTE_INSTALLED_ROUND ', dict(phase=g, round=index,
                                 opened_qps=opened + 6, closed_qps=opened,
                                 reused_qps=(base_round + index) * 96,
                                 completed_grants=(base_round + index + 1) * 96, opened_mrs=4,
                                 read_bytes=baseline + (index + 1) * byte_count if role == 'requester' else 0,
                                 write_bytes=0)))
                if g + 1 < count:
                    if target:
                        rows.append(('ORBITTE_INSTALLED_KILL ', dict(phase=g, pid=stamps[role][g]['pid'],
                                     cached_qps=6, persistent_mrs=4, sq_debt=0, rq_debt=0, orderly_close=False)))
                    else:
                        rows.append(('ORBITTE_INSTALLED_PEER_EXIT ', dict(phase=g,
                                     survivor_pid=stamps[role][g]['pid'], locally_idle=True)))
                if not target:
                    for old in range(g):
                        rows.append(('ORBITTE_INSTALLED_STALE ', dict(phase=g, previous=old,
                                     old_epoch=stamps[peer][old]['epoch'], new_epoch=stamps[peer][g]['epoch'],
                                     opened_qps=0, posted_wrs=0, remote_effect_none=True, locally_drained=True)))
            rows.append(('ORBITTE_INSTALLED_RESULT ', dict(protocol=installed.PROTOCOL, status='passed',
                         role=role, operation='read', parents=self.case['parents'], payload=self.case['payload'],
                         rounds=3, full_bytes_and_guards=True, locally_drained=True)))
            result[role] = {'stdout.log': '\n'.join(p + json.dumps(r) for p, r in rows),
                            'result.json': json.dumps(dict(status='passed', exit_code=0, child_reaped=True,
                                                           resource_locks_released=True, phases=process_phases)),
                            'restart.json': json.dumps(dict(phases=process_phases, locks_held=True,
                                                            kill_role=self.case['fault_role'], generations=count,
                                                            live_peer=True))}
            for g in range(count if target else 1):
                result[role]['maps-' + str(g) + '.txt'] = '/lib/libibverbs.so.1\n/lib/libmlx5.so.1\n'
        return result

    def test_complete_live_peer_history_and_negative_controls(self):
        for killed in ('source', 'requester'):
            for count in (2, 3):
                self.case.update(fault_role=killed, generations=count)
                good = self.evidence()
                installed.verify(self.profile, self.case, good)
                survivor = 'source' if killed == 'requester' else 'requester'
                controls = [
                    ('missing-stale', survivor, '"phase": 1, "previous": 0', '"phase": 9, "previous": 0'),
                    ('stale-post', survivor, '"posted_wrs": 0', '"posted_wrs": 1'),
                    ('stale-bytes', survivor, '"stale_bytes_unchanged": true', '"stale_bytes_unchanged": false'),
                    ('mr-reopened', survivor, '"opened_mrs": 4', '"opened_mrs": 8'),
                    ('false-drain', survivor, '"locally_drained": true', '"locally_drained": false'),
                    ('unexpected-close', killed, '"orderly_close": false', '"orderly_close": true'),
                    ('wrong-kill-child', killed, '"pid": ' + str(100 if killed == 'source' else 200), '"pid": 9999'),
                ]
                for name, role, before, after in controls:
                    evidence = copy.deepcopy(good)
                    evidence[role]['stdout.log'] = evidence[role]['stdout.log'].replace(before, after)
                    with self.subTest(killed=killed, count=count, control=name), self.assertRaises(ValueError):
                        installed.verify(self.profile, self.case, evidence)
                for name, change in [
                    ('early-lock-release', lambda r: r.update({'restart.json': r['restart.json'].replace('"locks_held": true', '"locks_held": false')})),
                    ('wrong-supervisor-policy', lambda r: r.update({'restart.json': r['restart.json'].replace('"live_peer": true', '"live_peer": false')})),
                    ('missing-provider', lambda r: r.update({'maps-0.txt': ''})),
                    ('unreaped-child', lambda r: r.update({'result.json': r['result.json'].replace('"child_reaped": true', '"child_reaped": false')})),
                ]:
                    evidence = copy.deepcopy(good)
                    change(evidence[killed])
                    with self.subTest(control=name), self.assertRaises(ValueError):
                        installed.verify(self.profile, self.case, evidence)

    def test_profile_selects_only_explicit_installed_restarts(self):
        self.assertEqual(run.restart_plan(self.case), ('source', 3))
        for bad in [dict(generations=1), dict(generations=True), dict(generations=4),
                    dict(fault_role='both'), dict(mode='installed-host')]:
            with self.subTest(profile=bad), self.assertRaises(ValueError):
                installed.validate({**self.case, **bad})


if __name__ == '__main__':
    unittest.main()
