"""Reject plausible-looking but incomplete installed-artifact evidence."""
import copy
import json
import unittest

import installed


class InstalledHostEvidence(unittest.TestCase):
    def setUp(self):
        self.profile = {'rails': ['mlx5_1', 'mlx5_2']}
        self.case = dict(mode='installed-host', transport='rdma', source_kind='cpu',
                         requester_kind='cpu', source='a', requester='b', operation='read',
                         rdma_events=False, parents=16, payload=(6 << 20) + 1,
                         block=1 << 20, num_lanes=3, mask=3)

    def evidence(self):
        result = {}
        for role in ('source', 'requester'):
            rows = [('ORBITTE_INSTALLED_NIC ', dict(name=name, pci=str(index)))
                    for index, name in enumerate(self.profile['rails'])]
            rows += [('ORBITTE_INSTALLED_ROUND ', dict(phase=0, round=index, opened_qps=6, closed_qps=0,
                     reused_qps=index * 96, completed_grants=(index + 1) * 96,
                     opened_mrs=4, read_bytes=(index + 1) * self.case['parents'] * self.case['payload']
                     if role == 'requester' else 0, write_bytes=0)) for index in range(3)]
            rows += [('ORBITTE_INSTALLED_RESULT ', dict(protocol=installed.PROTOCOL,
                     status='passed', role=role, operation='read', parents=self.case['parents'],
                     payload=self.case['payload'], rounds=3,
                     full_bytes_and_guards=True, locally_drained=True))]
            result[role] = {'stdout.log': '\n'.join(prefix + json.dumps(row) for prefix, row in rows),
                            'maps.txt': '/lib/libibverbs.so.1\n/lib/libmlx5.so.1\n',
                            'result.json': json.dumps(dict(exit_code=0, child_reaped=True,
                                                           resource_locks_released=True))}
        return result

    def test_rail_health_requires_both_native_counters_guards_and_restored_link(self):
        self.case.update(mode='installed-rail-health', parents=1, rdma_events=True, fault_netdev='data0')
        installed.validate(self.case)
        evidence = self.evidence()
        for role, record in evidence.items():
            row = dict(role=role, verified_bytes=2*self.case['payload']+192)
            if role == 'requester':
                row.update(blocked_posts=0, probes=1, recoveries=1, fault_rail_bytes=64, spare_rail_bytes=32)
                state = json.loads(record['result.json'])
                record['result.json'] = json.dumps(dict(state, data_link_restored=True))
            record['stdout.log'] += '\nORBITTE_RAIL_HEALTH ' + json.dumps(row) + '\nORBITTE_RAIL_DRAINED ' + role
        installed.verify(self.profile, self.case, evidence)
        for old, new in [('rail_bytes": 32', 'rail_bytes": 0'), ('"probes": 1', '"probes": 2'), ('DRAINED requester', 'DRAINED source')]:
            invalid = copy.deepcopy(evidence)
            invalid['requester']['stdout.log'] = invalid['requester']['stdout.log'].replace(old, new)
            with self.assertRaises(ValueError):
                installed.verify(self.profile, self.case, invalid)
        state = json.loads(evidence['requester']['result.json'])
        evidence['requester']['result.json'] = json.dumps(dict(state, data_link_restored=False))
        with self.assertRaises(ValueError):
            installed.verify(self.profile, self.case, evidence)

    def test_complete_ledger_passes_and_missing_native_or_drain_proof_fails(self):
        good = self.evidence()
        installed.verify(self.profile, self.case, good)
        for name, change in [
            ('missing-result', lambda r: r.update({'stdout.log': r['stdout.log'].split('ORBITTE_INSTALLED_RESULT')[0]})),
            ('wrong-bytes', lambda r: r.update({'stdout.log': r['stdout.log'].replace('"read_bytes": 0', '"read_bytes": 1')})),
            ('unbounded-qp', lambda r: r.update({'stdout.log': r['stdout.log'].replace('"opened_qps": 6', '"opened_qps": 7')})),
            ('missing-round', lambda r: r.update({'stdout.log': '\n'.join(line for line in r['stdout.log'].splitlines() if '"round": 1' not in line)})),
            ('no-provider', lambda r: r.update({'maps.txt': ''})),
            ('no-drain', lambda r: r.update({'stdout.log': r['stdout.log'].replace('"locally_drained": true', '"locally_drained": false')})),
            ('unreaped', lambda r: r.update({'result.json': json.dumps(dict(exit_code=0, child_reaped=False, resource_locks_released=False))})),
        ]:
            evidence = copy.deepcopy(good)
            change(evidence['source'])
            with self.subTest(control=name), self.assertRaises(ValueError):
                installed.verify(self.profile, self.case, evidence)

    def test_selective_gate_rejects_false_cqe_depth_or_capability_evidence(self):
        self.case.update(mode='installed-selective', parents=1, num_lanes=1, mask=2,
                         payload=8 * self.case['block'] + 17, non_atomic=True)
        evidence = self.evidence()
        for role, result in evidence.items():
            result['stdout.log'] = result['stdout.log'].replace('"opened_qps": 6', '"opened_qps": 1')
            for r in range(3):
                n = r + 1 if role == 'requester' else 0
                row = dict(round=r, wrs=8*n, cqes=3*n, atomics=False, init_limit=0, resp_limit=32, init=1, resp=32)
                result['stdout.log'] += '\nORBITTE_SELECTIVE ' + json.dumps(row)
        installed.verify(self.profile, self.case, evidence)
        for old, new in [('"cqes": 3', '"cqes": 8'), ('"wrs": 8', '"wrs": 3'), ('"atomics": false', '"atomics": true'),
                         ('"init": 1', '"init": 0'), ('"resp": 32', '"resp": 16'), ('"round": 1', '"round": 0')]:
            bad = copy.deepcopy(evidence)
            bad['requester']['stdout.log'] = bad['requester']['stdout.log'].replace(old, new)
            with self.subTest(change=new), self.assertRaises(ValueError):
                installed.verify(self.profile, self.case, bad)

    def test_selector_gate_requires_exact_pool_indices_and_per_rail_bytes(self):
        self.case.update(mode='installed-selector', parents=1, num_lanes=1)
        evidence = self.evidence()
        for role, result in evidence.items():
            result['stdout.log'] = result['stdout.log'].replace('"opened_qps": 6', '"opened_qps": 1')
            for r in range(3):
                for i, name in enumerate(self.profile['rails']):
                    count = (r + 1) * self.case['payload'] if role == 'requester' and i == 0 else 0
                    row = dict(round=r, nic=name, indices=1, bytes=count)
                    result['stdout.log'] += '\nORBITTE_SELECTOR ' + json.dumps(row)
        installed.verify(self.profile, self.case, evidence)
        for key, value in [('indices', 2), ('bytes', 1)]:
            bad = copy.deepcopy(evidence)
            log = bad['source']['stdout.log']
            bad['source']['stdout.log'] = log.replace(f'"{key}": ' + str(1 if key == 'indices' else 0), f'"{key}": {value}')
            with self.subTest(key=key), self.assertRaises(ValueError):
                installed.verify(self.profile, self.case, bad)

    def test_profile_cannot_expand_hardware_or_memory_scope(self):
        for invalid in [dict(parents=True), dict(parents=33), dict(payload=64 << 20),
                        dict(parents=32, payload=8 << 20), dict(mask=4), dict(num_lanes=4),
                        dict(source='b'), dict(transport='tcp'), dict(requester_kind='cuda:0')]:
            with self.subTest(profile=invalid), self.assertRaises(ValueError):
                installed.validate({**self.case, **invalid})

    def test_two_host_profile_rejects_same_machine_aliases_and_missing_identity(self):
        profile = dict(cases=[self.case], nodes={
            'a': dict(hostname='host-a', boot_id='boot-a'),
            'b': dict(hostname='host-b', boot_id='boot-b'),
        })
        installed.validate_nodes(profile)
        for name, change in [
            ('same-host', lambda p: p['nodes']['b'].update(hostname='host-a')),
            ('same-boot', lambda p: p['nodes']['b'].update(boot_id='boot-a')),
            ('missing-boot', lambda p: p['nodes']['b'].pop('boot_id')),
            ('empty-host', lambda p: p['nodes']['a'].update(hostname=' ')),
            ('missing-node', lambda p: p['nodes'].pop('b')),
            ('empty-cases', lambda p: p.update(cases=[])),
        ]:
            candidate = copy.deepcopy(profile)
            change(candidate)
            with self.subTest(control=name), self.assertRaises(ValueError):
                installed.validate_nodes(candidate)


if __name__ == '__main__':
    unittest.main()
