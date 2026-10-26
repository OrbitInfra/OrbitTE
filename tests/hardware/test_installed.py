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
                     opened_mrs=4, read_bytes=(index + 1) * 16 * self.case['payload']
                     if role == 'requester' else 0, write_bytes=0)) for index in range(3)]
            rows += [('ORBITTE_INSTALLED_RESULT ', dict(protocol=installed.PROTOCOL,
                     status='passed', role=role, operation='read', parents=16,
                     payload=self.case['payload'], rounds=3,
                     full_bytes_and_guards=True, locally_drained=True))]
            result[role] = {'stdout.log': '\n'.join(prefix + json.dumps(row) for prefix, row in rows),
                            'maps.txt': '/lib/libibverbs.so.1\n/lib/libmlx5.so.1\n',
                            'result.json': json.dumps(dict(exit_code=0, child_reaped=True,
                                                           resource_locks_released=True))}
        return result

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

    def test_profile_cannot_expand_hardware_or_memory_scope(self):
        for invalid in [dict(parents=True), dict(parents=33), dict(payload=64 << 20),
                        dict(parents=32, payload=8 << 20), dict(mask=4), dict(num_lanes=4),
                        dict(source='b'), dict(transport='tcp'), dict(requester_kind='cuda:0')]:
            with self.subTest(profile=invalid), self.assertRaises(ValueError):
                installed.validate({**self.case, **invalid})


if __name__ == '__main__':
    unittest.main()
