import copy
import json
import unittest
import staging
import supervise


def fixture(gpu_role='source'):
    case = dict(source='a', requester='b', source_kind='cpu', requester_kind='cpu', transport='rdma', payload=128, staging_chunk_size=4)
    case[gpu_role + '_kind'] = 'cuda:0'
    native = []
    def emit(kind, **values):
        native.append(dict(seq=len(native)+1, pid=7, tid=9, kind=kind, code=0, **values))
    emit('allocate', address=200, length=256)
    emit('pin', address=100, length=4)
    for direction in ('dtoh', 'htod'):
        for index in range(3):
            for offset, length in ((0, 4), (4, 4), (8, 1)):
                address = 200 + 64 + index * 32 + (11 if gpu_role == 'source' else 5) + offset
                emit(direction, source=100 if direction == 'htod' else address, destination=100 if direction == 'dtoh' else address, length=length, stream=3)
                emit('record', event=4, stream=3)
                emit('wait', event=4)
                emit('pop', context=5)
    emit('unpin', address=100)
    emit('free', address=200)
    proof = {'result.json': json.dumps(dict(status='passed', child_reaped=True, resource_locks_released=True, fault_library_sha256='abc', native_pid=7))}
    evidence = {}
    for role in ('source', 'requester'):
        gpu = role == gpu_role
        stats = dict(allocations=1, reuses=5, chunks=18, bytes=54, reserved_bytes=4) if gpu else dict(allocations=0, reuses=0, chunks=0, bytes=0, reserved_bytes=0)
        evidence[role] = {**proof, 'stdout.log': 'ORBITTE_MEMORY ' + json.dumps(dict(base=200, length=256, cuda=gpu)) + '\nORBITTE_STAGING_STATS ' + json.dumps(stats)}
    return case, evidence, native


class StagingProof(unittest.TestCase):
    def test_native_owner_receipts_and_negative_controls(self):
        faults = (None, 'tail', 'pop', 'free', 'pid', 'reuse', 'record-stream', 'wait-event',
                  'copy-after-free', 'double-free', 'gpu-range', 'gpu-slice', 'gpu-free-early',
                  'gpu-free-missing', 'gpu-double-free', 'gpu-allocation', 'missing-memory')
        for role in ('source', 'requester'):
            for fault in faults:
                with self.subTest(role=role, fault=fault):
                    case, evidence, native = fixture(role)
                    def first(kind):
                        return next(row for row in native if row['kind'] == kind)
                    if fault == 'tail': next(row for row in native if row.get('length') == 1)['length'] = 4
                    if fault == 'pop': first('pop')['kind'] = 'record'
                    if fault == 'free': first('unpin')['address'] = 200
                    if fault == 'pid': first('dtoh')['pid'] = 8
                    if fault == 'record-stream': first('record')['stream'] = 999
                    if fault == 'wait-event': first('wait')['event'] = 999
                    if fault == 'reuse': native.insert(2, copy.deepcopy(first('pin')))
                    if fault == 'gpu-range': first('dtoh')['source'] = 0
                    if fault == 'gpu-slice': first('htod')['destination'] += 1
                    if fault == 'gpu-free-early': native.insert(2, native.pop())
                    if fault == 'gpu-free-missing': native.pop()
                    if fault == 'gpu-allocation': first('allocate')['length'] += 1
                    if fault == 'missing-memory':
                        evidence[role]['stdout.log'] = evidence[role]['stdout.log'].split('\n', 1)[1]
                    if fault in ('copy-after-free', 'double-free', 'gpu-double-free'):
                        extra = native[2:6] if fault == 'copy-after-free' else [first('unpin' if fault == 'double-free' else 'free')]
                        native.extend(copy.deepcopy(extra))
                    for index, row in enumerate(native):
                        row['seq'] = index + 1
                    evidence[role]['cuda-staging.jsonl'] = '\n'.join(json.dumps(row) for row in native)
                    if fault:
                        with self.assertRaises(ValueError): staging.verify(case, evidence)
                    else: staging.verify(case, evidence)

    def test_remote_profile_requires_completed_operations_on_the_source(self):
        for fault in (None, 'missing', 'unpin', 'cross', 'requester', 'host'):
            with self.subTest(fault=fault):
                case, evidence, native = fixture()
                case['remote_staging'] = True
                if fault == 'host': case['source_kind'] = 'cpu'
                evidence['source']['cuda-staging.jsonl'] = '\n'.join(json.dumps(row) for row in native)
                for role in ('source', 'requester'):
                    counts = dict(pins=6, delegates=18, crosses=18, unpins=6) if role == 'source' else dict(pins=0, delegates=0, crosses=0, unpins=0)
                    if fault == 'unpin' and role == 'source': counts['unpins'] -= 1
                    if fault == 'cross' and role == 'source': counts['crosses'] -= 1
                    if fault == 'requester' and role == 'requester': counts['pins'] = 1
                    if fault != 'missing':
                        evidence[role]['stdout.log'] += '\nORBITTE_REMOTE_STAGE_STATS ' + json.dumps(counts)
                if fault:
                    with self.assertRaises(ValueError): staging.verify(case, evidence)
                else: staging.verify(case, evidence)

    def test_observer_scope_is_bounded(self):
        scope = dict(ORBITTE_STAGING_OBSERVE='1', ORBITTE_SHARED_ROLE='source', ORBITTE_STAGING_CHUNK_COUNT='1')
        self.assertTrue(supervise.fault_scope(scope))
        for key, value in [('ORBITTE_SHARED_ROLE', 'other'), ('ORBITTE_HOST_MODE', 'fault'), ('ORBITTE_GDR_OBSERVE', '1'), ('ORBITTE_IPC_HOLD', '/tmp/hold')]:
            self.assertFalse(supervise.fault_scope({**scope, key:value}))
