import copy
import json
import unittest
import staging
import staging_native
import supervise


def fixture(gpu_role='source', depth=1, cap=None, remote=False, physical=4):
    remote = remote or depth > 1
    pipeline_width = min(depth, cap or depth)
    width = pipeline_width if gpu_role == 'source' else 1
    slot_streams = remote or width > 1
    case = dict(source='a', requester='b', source_kind='cpu', requester_kind='cpu', transport='rdma',
                payload=4 * ((pipeline_width * 2 + 1) * 4 + 24), staging_chunk_size=4)
    case[gpu_role + '_kind'] = 'cuda:0'
    if physical != 4: case[gpu_role + '_chunk_size'] = physical
    if remote:
        case.update(remote_staging=True, source_pipeline_depth=depth, requester_pipeline_depth=cap or depth)
    length = case['payload'] // 4 - 23
    size = case['payload'] + 128
    native = []
    def emit(kind, **values):
        native.append(dict(seq=len(native)+1, pid=7, tid=9, kind=kind, code=0, **values))
    def enter(): emit('push', context=5)
    def leave(): emit('pop', context=5)
    def call(kind, **values):
        if kind in ('htod', 'dtoh', 'wait', 'free', 'unpin', 'stream_destroy', 'event_destroy'):
            emit(kind + '_begin', **values)
        emit(kind, **values)
    def once(kind, **values):
        enter(); call(kind, **values); leave()
    def handles(stream, event, destroy=False):
        enter()
        call('event_destroy' if destroy else 'stream_create', **({'event':event} if destroy else {'stream':stream}))
        call('stream_destroy' if destroy else 'event_create', **({'stream':stream} if destroy else {'event':event}))
        leave()
    def transfer(direction, host, device, count, stream, event, deferred=False):
        enter()
        call(direction, source=host if direction == 'htod' else device,
             destination=host if direction == 'dtoh' else device, length=count, stream=stream)
        call('record', event=event, stream=stream)
        if not deferred: call('wait', event=event)
        leave()
    def full(direction, host):
        once('pin', address=host, length=size)
        transfer(direction, host, 10000, size, 3, 4)
        once('unpin', address=host)
    enter()
    call('allocate', address=10000, length=size)
    call('memset', address=10000, length=size, value=0)
    call('sync')
    leave()
    handles(3, 4)
    full('htod', 20000)
    for slot in range(width):
        once('pin', address=100+slot*8, length=physical)
        if slot_streams: handles(10+slot, 30+slot)
    for direction in (('dtoh', 'htod') if gpu_role == 'source' else ('htod', 'dtoh')):
        for index in range(3):
            pending = []
            for step, offset in enumerate(range(0, length, 4)):
                slot = step % width
                if slot in pending:
                    once('wait', event=30+slot); pending.remove(slot)
                address = 10000+64+index*(case['payload']//4)+(11 if gpu_role == 'source' else 5)+offset
                transfer(direction, 100+slot*8, address, min(4, length-offset),
                         10+slot if slot_streams else 3, 30+slot if slot_streams else 4, slot_streams)
                if slot_streams:
                    if width > 1: pending.append(slot)
                    else: once('wait', event=30+slot)
            # Reverse completion order is legal across distinct slots.
            for slot in reversed(pending): once('wait', event=30+slot)
        if gpu_role == 'requester' and direction == 'htod':
            full('dtoh', 20000)
            for index in range(3):
                once('pin', address=20000, length=length)
                transfer('htod', 20000, 10000+64+index*(case['payload']//4)+5, length, 3, 4)
                once('unpin', address=20000)
    for slot in range(width):
        if slot_streams: handles(10+slot, 30+slot, True)
        once('unpin', address=100+slot*8)
    full('dtoh', 20000)  # Reuse a freed native address with a new allocation generation.
    handles(3, 4, True)
    once('free', address=10000)
    proof = {'result.json': json.dumps(dict(status='passed', child_reaped=True, resource_locks_released=True, fault_library_sha256='abc', native_pid=7))}
    evidence = {}
    for role in ('source', 'requester'):
        gpu = role == gpu_role
        stats = dict(allocations=width, reuses=5*width, chunks=6*((length+3)//4), bytes=6*length, reserved_bytes=physical*width) if gpu else dict(allocations=0, reuses=0, chunks=0, bytes=0, reserved_bytes=0)
        evidence[role] = {**proof, 'stdout.log': 'ORBITTE_MEMORY ' + json.dumps(dict(base=10000, length=size, cuda=gpu)) + '\nORBITTE_STAGING_STATS ' + json.dumps(stats)}
        configured = staging.configured_depth(case, role)
        evidence[role]['stdout.log'] += '\nORBITTE_STAGING_CONFIG '+json.dumps(dict(depth=configured, chunk_count=configured, chunk_size=staging.configured_chunk(case, role)))
        if case.get('remote_staging'):
            counts = dict(pins=6, delegates=stats['chunks'], crosses=stats['chunks'], unpins=6) if role=='source' else dict(pins=0, delegates=0, crosses=0, unpins=0)
            evidence[role]['stdout.log'] += '\nORBITTE_REMOTE_STAGE_STATS '+json.dumps(counts)
    return case, evidence, native


def verify(case, evidence, native, role='source'):
    for index, row in enumerate(native): row['seq'] = index+1
    evidence[role]['cuda-staging.jsonl'] = '\n'.join(json.dumps(row) for row in native)
    staging.verify(case, evidence)


class StagingProof(unittest.TestCase):
    def test_serial_and_interleaved_generations(self):
        for role, depth, cap in [('source', 1, None), ('requester', 1, None), ('source', 2, None),
                                 ('source', 3, None), ('source', 16, None), ('source', 3, 2), ('source', 2, 3)]:
            with self.subTest(role=role, depth=depth, cap=cap):
                case, evidence, native = fixture(role, depth, cap)
                verify(case, evidence, native, role)

    def test_remote_serial_and_asymmetric_physical_slots(self):
        for depth in (1, 3):
            for physical in (4, 7):
                case, evidence, native = fixture(depth=depth, remote=True, physical=physical)
                verify(case, evidence, native)
                broken = copy.deepcopy(evidence)
                broken['source']['stdout.log'] = broken['source']['stdout.log'].replace('"reserved_bytes": '+str(physical*depth), '"reserved_bytes": 0')
                with self.assertRaises(ValueError): staging.verify(case, broken)

    def test_remote_requester_slot_stream_is_distinct_from_setup_and_width(self):
        for depth in (1, 3):
            for physical in (4, 7):
                case, source, source_native = fixture(depth=depth, remote=True)
                _, requester, requester_native = fixture('requester', depth=depth, remote=True, physical=physical)
                case.update(requester_kind='cuda:0', requester_chunk_size=physical)
                evidence = dict(source=source['source'], requester=requester['requester'])
                for role, native in [('source', source_native), ('requester', requester_native)]:
                    evidence[role]['cuda-staging.jsonl'] = '\n'.join(json.dumps(row) for row in native)
                staging.verify(case, evidence)
                broken = copy.deepcopy(evidence)
                rows = [json.loads(line) for line in broken['requester']['cuda-staging.jsonl'].splitlines()]
                next(row for row in rows if row['kind'] == 'pin' and row['address'] == 100)['length'] = 1
                broken['requester']['cuda-staging.jsonl'] = '\n'.join(json.dumps(row) for row in rows)
                with self.assertRaises(ValueError): staging.verify(case, broken)

    def test_native_owner_receipts_and_negative_controls(self):
        faults = ('tail', 'missing-record', 'submission-pop', 'completion-pop', 'pid', 'record-stream',
                  'wait-event', 'old-wait', 'record-thread', 'wait-thread', 'context', 'gpu-slice',
                  'free-missing', 'handle-missing', 'double-free', 'free-before-wait', 'free-before-pop',
                  'destroy-before-wait', 'slot-reuse', 'event-reuse', 'missing-return', 'missing-entry',
                  'return-arguments', 'width', 'stats', 'unpin-count', 'missing-memory',
                  'host-tail', 'extra-conflicting-copy', 'extra-disjoint-copy', 'extra-memset', 'extra-sync')
        for depth in (1, 3):
            for fault in faults:
                with self.subTest(depth=depth, fault=fault):
                    case, evidence, native = fixture(depth=depth)
                    pos = next(i for i,r in enumerate(native) if r['kind']=='dtoh_begin' and r['destination']==100)
                    def at(kind): return next(i for i in range(pos,len(native)) if native[i]['kind']==kind)
                    def first(kind): return native[at(kind)]
                    if fault == 'tail': next(r for r in native[pos:] if r.get('length')==1)['length']=4
                    if fault == 'host-tail':
                        for row in native:
                            if row['kind'] in ('dtoh_begin', 'dtoh') and row['length']==1: row['destination']+=1
                    if fault in ('extra-conflicting-copy', 'extra-disjoint-copy'):
                        begin = next(i for i,r in enumerate(native) if r['kind']=='pin' and r['address']==20000)-1
                        end = next(i for i,r in enumerate(native) if r['kind']=='unpin' and r['address']==20000)+2
                        extra = copy.deepcopy(native[begin:end])
                        for row in extra:
                            row['tid']=99
                            if row['kind']=='pin': row['length']=8
                            if row['kind'] in ('htod_begin','htod'):
                                row['destination']=first('dtoh')['source'] if fault=='extra-conflicting-copy' else 10000
                                row['length']=4
                        where=at('pop')+1
                        native[where:where]=extra
                    if fault in ('extra-memset','extra-sync'):
                        values = dict(address=first('dtoh')['source'], length=4, value=0) if fault=='extra-memset' else {}
                        where=at('pop')+1
                        native[where:where]=[dict(seq=0,pid=7,tid=99,code=0,**row) for row in (
                            dict(kind='push',context=5), dict(kind=fault.removeprefix('extra-'),**values),dict(kind='pop',context=5))]
                    if fault == 'missing-record': native.pop(at('record'))
                    if fault == 'submission-pop': native.pop(at('pop'))
                    if fault == 'completion-pop': native.pop(next(i for i in range(at('wait'),len(native)) if native[i]['kind']=='pop'))
                    if fault == 'pid': first('dtoh')['pid']=8
                    if fault == 'record-stream': first('record')['stream']=999
                    if fault == 'wait-event': first('wait')['event']=999
                    if fault == 'old-wait': native.insert(at('wait')+1, copy.deepcopy(first('wait')))
                    if fault == 'record-thread': first('record')['tid']=99
                    if fault == 'wait-thread': first('wait')['tid']=99
                    if fault == 'context': first('pop')['context']=6
                    if fault == 'gpu-slice':
                        first('dtoh_begin')['source']+=1; first('dtoh')['source']+=1
                    if fault == 'free-missing': native.pop(at('free'))
                    if fault == 'handle-missing': native.pop(at('event_destroy'))
                    if fault == 'double-free': native.insert(at('free')+1, copy.deepcopy(first('free')))
                    if fault in ('free-before-wait','free-before-pop','destroy-before-wait'):
                        where = at('wait_begin') if fault != 'free-before-pop' else at('wait')+1
                        kind = 'stream_destroy_begin' if fault == 'destroy-before-wait' else 'unpin_begin'
                        extra = dict(first(kind), tid=99)
                        native[where:where] = [dict(seq=0,pid=7,tid=99,kind='push',context=5,code=0),extra]
                        # The late return remains later in the trace: entering cleanup already violates ownership.
                    if fault == 'slot-reuse': native[at('record'):at('record')] = copy.deepcopy(native[pos:pos+2])
                    if fault == 'event-reuse': native.insert(at('record')+1, copy.deepcopy(first('record')))
                    if fault == 'missing-return': native.pop(at('dtoh'))
                    if fault == 'missing-entry': native.pop(pos)
                    if fault == 'return-arguments': first('dtoh')['source']+=1
                    if fault == 'width': case.update(remote_staging=True, source_pipeline_depth=2, requester_pipeline_depth=2)
                    if fault == 'stats': evidence['source']['stdout.log'] = evidence['source']['stdout.log'].replace('"allocations": '+str(depth), '"allocations": 99')
                    if fault == 'unpin-count': evidence['source']['stdout.log'] = evidence['source']['stdout.log'].replace('"unpins": 6','"unpins": 5') if depth>1 else ''
                    if fault == 'missing-memory': evidence['source']['stdout.log'] = evidence['source']['stdout.log'].split('\n',1)[1]
                    with self.assertRaises(ValueError): verify(case,evidence,native)

    def test_profile_and_observer_scope_are_bounded(self):
        case, _, _ = fixture(depth=3)
        for value in (0, 17, True, '3', -1):
            with self.subTest(value=value), self.assertRaises(ValueError):
                staging.validate_case({**case, 'source_pipeline_depth':value})
        for depth in (1, 2, 16):
            scope = dict(ORBITTE_STAGING_OBSERVE='1', ORBITTE_SHARED_ROLE='source',
                         ORBITTE_STAGING_CHUNK_COUNT=str(depth), ORBITTE_STAGING_PIPELINE_DEPTH=str(depth))
            self.assertTrue(supervise.fault_scope(scope))
            for key,value in [('ORBITTE_SHARED_ROLE','other'),('ORBITTE_HOST_MODE','fault'),('ORBITTE_GDR_OBSERVE','1'),
                              ('ORBITTE_IPC_HOLD','/tmp/hold'),('ORBITTE_STAGING_CHUNK_COUNT','17')]:
                self.assertFalse(supervise.fault_scope({**scope,key:value}))
