"""GPU qualification must prove native addresses, rail use and private-PD drain."""
import copy
import json
import unittest

import gdr


class GpuDirectEvidence(unittest.TestCase):
    def evidence(self):
        profile = dict(rnic='nic0', rails=['nic0', 'nic1'])
        case = dict(source_kind='cuda:0', requester_kind='cuda:0', payload=4096, slice_bytes=17)
        rows = {role: [] for role in ('source', 'requester')}
        for context in range(2):
            for role in rows:
                rows[role] += [dict(kind='context', context=context, device='nic' + str(context)),
                               dict(kind='close_context', context=context, code=0),
                               dict(kind='gdr_mr', context=context, pd=10 + context, mr=1,
                                    cuda_status=0, memory_type=2, access=1, address=1000,
                                    length=4096, lkey=20 + context, rkey=20 + context)]
            for opcode in (0, 4):
                for parent in range(3):
                    pd = 100 + context * 10 + opcode + parent
                    length = 8 + context
                    address = 1100 + parent * 32 + context * 8
                    rows['source'] += [dict(kind='gdr_mr', context=context, pd=pd, mr=pd,
                                          cuda_status=0, memory_type=2, access=3 if opcode == 0 else 5,
                                          address=address, length=length, lkey=pd, rkey=pd),
                                       dict(kind='gdr_qp', context=context, pd=pd, qpn=pd)]
                    rows['requester'] += [dict(kind='gdr_post', context=context, pd=10 + context,
                                             opcode=opcode, code=0, length=length, local=address,
                                             remote=address, lkey=20 + context, rkey=pd),
                                          dict(kind='gdr_post', opcode=2, code=0)]
        evidence = {}
        for role, native in rows.items():
            public = [('ORBITE_MEMORY ', dict(base=1000, length=4096, cuda=True)),
                      ('ORBITE_GPU_MR_STATS ', dict(opened_private_pds=12 if role == 'source' else 0,
                                                   closed_private_pds=12 if role == 'source' else 0,
                                                   active_private_pds=0))]
            evidence[role] = {'stdout.log': '\n'.join(prefix + json.dumps(row) for prefix, row in public),
                              'cq-fault-0.jsonl': '\n'.join(json.dumps(dict(pid=123, **row)) for row in native),
                              'result.json': json.dumps(dict(status='passed', child_reaped=True,
                                                             resource_locks_released=True,
                                                             native_pid=123, fault_library_sha256='abc'))}
        return profile, case, evidence

    def test_reuse_needs_local_only_parents_and_matching_native_invalidations(self):
        profile, case, evidence = self.evidence()
        case.update(gpu_source_reuse=True, expected_grants=12)
        with self.assertRaises(ValueError):
            gdr.verify(profile, case, evidence)  # historical private-PD evidence
        source = evidence['source']
        rows = [json.loads(line) for line in source['cq-fault-0.jsonl'].splitlines()]
        kept = [r for r in rows if r['kind'] != 'gdr_qp' and
                (r['kind'] != 'gdr_mr' or r['access'] == 1)]
        for context in range(2):
            kept.append(dict(pid=123, kind='gdr_qp', context=context, pd=10+context, qpn=90+context))
        for mr in (r for r in rows if r['kind'] == 'gdr_mr' and r['access'] != 1):
            kept.append(dict(pid=123, kind='gdr_invalidation', context=mr['context'],
                             qpn=90+mr['context'], rkey=mr['rkey']))
        source['cq-fault-0.jsonl'] = '\n'.join(map(json.dumps, kept))
        source['stdout.log'] = source['stdout.log'].splitlines()[0] + '\nORBITE_GPU_MR_STATS ' + json.dumps(
            dict(active_private_pds=0, opened_private_pds=0, closed_private_pds=0,
                 opened_keys=12, invalidated_keys=12, reused_connections=8))
        requester = evidence['requester']
        requester['cq-fault-0.jsonl'] = requester['cq-fault-0.jsonl'].replace('"opcode": 2,', '"opcode": 9,')
        gdr.verify(profile, case, evidence)
        for change in [dict(qpn=99), dict(rkey=0), dict(context=0)]:
            changed = copy.deepcopy(kept)
            changed[-1].update(change)
            source['cq-fault-0.jsonl'] = '\n'.join(map(json.dumps, changed))
            with self.subTest(change=change), self.assertRaises(ValueError):
                gdr.verify(profile, case, evidence)

    def test_profile_requires_two_pinned_hosts_and_explicit_gpu_direct_scope(self):
        profile, case, _ = self.evidence()
        profile.update(nodes={'a': dict(hostname='a', boot_id='boot-a'),
                              'b': dict(hostname='b', boot_id='boot-b')},
                       cases=[dict(case, source='a', requester='b', transport='rdma', cuda_memory='direct')])
        gdr.validate_profile(profile)
        with self.assertRaises(ValueError):
            gdr.validate_profile(profile, require_reuse=True)
        positive = copy.deepcopy(profile)
        positive['cases'][0]['gpu_source_reuse'] = True
        gdr.validate_profile(positive, require_reuse=True)
        for changes in [dict(requester='a'), dict(source_kind='cpu', requester_kind='cpu'),
                        dict(cuda_memory='staged'), dict(transport='tcp')]:
            candidate = copy.deepcopy(profile)
            candidate['cases'][0].update(changes)
            with self.subTest(control=changes), self.assertRaises(ValueError):
                gdr.validate_profile(candidate)
        for field, value in [('hostname', 'a'), ('boot_id', 'boot-a'), ('boot_id', '')]:
            candidate = copy.deepcopy(profile)
            candidate['nodes']['b'][field] = value
            with self.subTest(control=field, value=value), self.assertRaises(ValueError):
                gdr.validate_profile(candidate)

    def test_native_gpu_rail_authority_and_lifetime_proofs_cannot_be_omitted(self):
        profile, case, good = self.evidence()
        gdr.verify(profile, case, good)
        controls = [
            ('wrong-process', 'requester', lambda row: row.update(pid=124)),
            ('no-device-registration', 'requester', lambda row: row.update(memory_type=1)
             if row['kind'] == 'gdr_mr' else None),
            ('missing-local-rail', 'source', lambda row: row.update(access=5)
             if row['kind'] == 'gdr_mr' and row['access'] == 1 and row['context'] == 1 else None),
            ('wrong-remote-permission', 'source', lambda row: row.update(access=7)
             if row['kind'] == 'gdr_mr' and row['access'] == 3 else None),
            ('oversized-remote-authority', 'source', lambda row: row.update(length=row['length'] + 1)
             if row['kind'] == 'gdr_mr' and row['access'] != 1 else None),
            ('wrong-source-pd', 'source', lambda row: row.update(pd=999)
             if row['kind'] == 'gdr_qp' else None),
            ('single-rail-posts', 'requester', lambda row: row.update(context=0)
             if row['kind'] == 'gdr_post' and row['opcode'] in (0, 4) else None),
            ('wrong-local-key', 'requester', lambda row: row.update(lkey=999)
             if row['kind'] == 'gdr_post' and row['opcode'] in (0, 4) else None),
            ('wrong-completion-profile', 'requester', lambda row: row.update(opcode=9)
             if row['kind'] == 'gdr_post' and row['opcode'] == 2 else None),
            ('failed-native-close', 'source', lambda row: row.update(code=1)
             if row['kind'] == 'close_context' else None),
        ]
        for name, role, change in controls:
            evidence = copy.deepcopy(good)
            rows = [json.loads(line) for line in evidence[role]['cq-fault-0.jsonl'].splitlines()]
            for row in rows:
                change(row)
            evidence[role]['cq-fault-0.jsonl'] = '\n'.join(map(json.dumps, rows))
            with self.subTest(control=name), self.assertRaises(ValueError):
                gdr.verify(profile, case, evidence)
        for field in ('child_reaped', 'resource_locks_released', 'fault_library_sha256'):
            evidence = copy.deepcopy(good)
            result = json.loads(evidence['source']['result.json'])
            result[field] = False
            evidence['source']['result.json'] = json.dumps(result)
            with self.subTest(control=field), self.assertRaises(ValueError):
                gdr.verify(profile, case, evidence)
        evidence = copy.deepcopy(good)
        evidence['source']['stdout.log'] = evidence['source']['stdout.log'].replace('"active_private_pds": 0', '"active_private_pds": 1')
        with self.assertRaises(ValueError):
            gdr.verify(profile, case, evidence)


if __name__ == '__main__':
    unittest.main()
