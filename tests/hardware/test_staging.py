import copy
import json
import unittest
import staging
import supervise


def fixture():
    case = dict(source='a', requester='b', source_kind='cuda:0', requester_kind='cpu', transport='rdma', payload=128, staging_chunk_size=4)
    native = []
    def emit(kind, **values):
        native.append(dict(seq=len(native)+1, pid=7, tid=9, kind=kind, code=0, **values))
    emit('pin', address=100, length=4)
    for direction in ['dtoh'] * 3 + ['htod'] * 3:
        for length in (4, 4, 1):
            emit(direction, source=100 if direction == 'htod' else 200, destination=100 if direction == 'dtoh' else 200, length=length, stream=3)
            emit('record', event=4, stream=3)
            emit('wait', event=4)
            emit('pop', context=5)
    emit('unpin', address=100)
    proof = dict(stdout='', **{'result.json': json.dumps(dict(status='passed', child_reaped=True, resource_locks_released=True, fault_library_sha256='abc', native_pid=7))})
    source = {**proof, 'stdout.log': 'ORBITTE_STAGING_STATS '+json.dumps(dict(allocations=1, reuses=5, chunks=18, bytes=54, reserved_bytes=4))}
    requester = {**proof, 'stdout.log': 'ORBITTE_STAGING_STATS '+json.dumps(dict(allocations=0, reuses=0, chunks=0, bytes=0, reserved_bytes=0))}
    return case, dict(source=source, requester=requester), native


class StagingProof(unittest.TestCase):
    def test_native_owner_receipts_and_negative_controls(self):
        for fault in (None, 'tail', 'pop', 'free', 'pid', 'reuse', 'record-stream', 'wait-event', 'copy-after-free', 'double-free'):
            with self.subTest(fault=fault):
                case, evidence, native = fixture()
                if fault == 'tail': native[9]['length'] = 4
                if fault == 'pop': native[4]['kind'] = 'record'
                if fault == 'free': native[-1]['address'] = 200
                if fault == 'pid': native[1]['pid'] = 8
                if fault == 'record-stream': native[2]['stream'] = 999
                if fault == 'wait-event': native[3]['event'] = 999
                if fault == 'reuse': native.insert(1, copy.deepcopy(native[0]))
                if fault in ('copy-after-free', 'double-free'):
                    extra = copy.deepcopy(native[1:5] if fault == 'copy-after-free' else native[-1:])
                    for row in extra:
                        row['seq'] = len(native) + 1
                        native.append(row)
                evidence['source']['cuda-staging.jsonl'] = '\n'.join(json.dumps(row) for row in native)
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
