"""CPU controls for the installed IPC evidence validator and native observer."""
import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import nvlink_run


class NativeEvidence(unittest.TestCase):
    def evidence(self, mode='healthy', memory='device'):
        cached = mode.startswith("cache-")
        batched = cached or mode.startswith("batch-")
        mode = mode.removeprefix("cache-").removeprefix("batch-")
        source, requester = [], []

        def add(rows, kind, **values):
            rows.append(dict(kind=kind, code=0, tid=1, seq=len(rows)+1,
                             pid=101 if rows is source else 102, **values))

        add(source, 'allocate', address=1000, length=2 << 20)
        add(source, 'memset', address=1000, length=2 << 20, value=0)
        add(source, 'sync')
        count = 1 if cached else 2 if mode == 'healthy' else 0 if mode == 'stale' else 1
        if count:
            add(requester, 'allocate', address=2000, length=151)
            if batched: add(requester, 'allocate', address=4000, length=151)
        for index in range(count):
            add(source, 'export', address=1000, handle=1234)
            add(requester, 'open', address=3000, handle=1234, flags=1)
            read = mode == 'healthy' and index == 0
            slices = [(10,7),(0,5),(5,5)] if batched else [(0,17)]
            if cached and mode=='healthy': slices *= 5
            for part, (offset,length) in enumerate(slices):
                if cached: read = mode=='healthy' and part < 12
                local_part = part%3 if cached else part
                remote, own = 3067+offset, (2000 if local_part%2 == 0 else 4000)+67+offset
                add(requester, 'copy', source=remote if read else own,
                    destination=own if read else remote, length=length, stream=4)
                add(requester, 'record', event=5, stream=4)
                add(requester, 'pop')
                add(requester, 'wait', event=5)
                add(requester, 'pop')
            if mode != 'requester-exit':
                add(requester, 'close', address=3000)
                add(requester, 'pop')
        if mode != 'requester-exit':
            add(source, 'free', address=1000)
            if count:
                add(requester, 'free', address=2000)
                if batched: add(requester, 'free', address=4000)
        if memory != 'device':
            for row in requester:
                host = memory=='host' or row.get('address')==2000 or any(2000 <= row.get(k,0) < 2151 for k in ('source','destination'))
                if host and row['kind']=='allocate': row.update(kind='pin',flags=1)
                elif host and row['kind']=='free': row['kind']='unpin'
                elif host and row['kind']=='copy': row['kind']='dtoh' if 3000 <= row['source'] < 3200 else 'htod'
        return source, requester

    def test_cached_native_has_one_import_for_concurrent_reads_and_later_write(self):
        for memory in ('device','host','mixed'):
            for mode in ('cache-healthy','cache-graceful','cache-requester-exit'):
                rows = self.evidence(mode,memory)
                result = nvlink_run.validate_native(*rows,17,mode,101,102,memory)
                self.assertEqual(result['imports'],1)
                self.assertEqual(result['copies'],15 if mode=='cache-healthy' else 3)
            for fault in ('missing-copy','extra-open','missing-event','wrong-direction','early-close','wrong-context-restore','unowned-staging'):
                source, requester = self.evidence('cache-healthy',memory)
                copies = [r for r in requester if r['kind'] in ('copy','dtoh','htod')]
                if fault=='missing-copy': requester.remove(copies[3])
                elif fault=='extra-open': requester.insert(4,dict(next(r for r in requester if r['kind']=='open')))
                elif fault=='missing-event': next(r for r in requester if r['kind']=='record')['kind']='ignored'
                elif fault=='wrong-direction': copies[-1]['source'],copies[-1]['destination']=copies[-1]['destination'],copies[-1]['source']
                elif fault=='early-close':
                    close=next(r for r in requester if r['kind']=='close');requester.remove(close);requester.insert(7,close)
                elif fault=='wrong-context-restore':
                    close=next(i for i,r in enumerate(requester) if r['kind']=='close');requester[close+1]['tid']=99
                else:
                    requester.insert(5,dict(pid=102,tid=1,code=0,kind='htod',source=9999999,destination=8888888,length=17,stream=4))
                for i,row in enumerate(requester): row['seq']=i+1
                with self.subTest(memory=memory,fault=fault), self.assertRaises(AssertionError):
                    nvlink_run.validate_native(source,requester,17,'cache-healthy',101,102,memory)

    def test_host_and_mixed_need_actual_pinned_owners_and_direct_copy_apis(self):
        for memory in ('host','mixed'):
            for mode in ('batch-healthy','batch-graceful','batch-requester-exit'):
                nvlink_run.validate_native(*self.evidence(mode,memory),17,mode,101,102,memory)
            for fault in ('pageable','missing-unpin','unpin-owner','wrong-direction','wrong-api','early-unpin','temporary-staging'):
                rows = self.evidence('batch-healthy',memory)
                requester=rows[1]
                if fault=='pageable': next(r for r in requester if r['kind']=='pin')['kind']='allocate'
                elif fault=='missing-unpin': next(r for r in requester if r['kind']=='unpin')['kind']='ignored'
                elif fault=='unpin-owner': next(r for r in requester if r['kind']=='unpin')['address']=9999
                elif fault in ('wrong-direction','wrong-api'):
                    next(r for r in requester if r['kind']=='dtoh')['kind']='htod' if fault=='wrong-direction' else 'copy'
                elif fault=='early-unpin':
                    row=next(r for r in requester if r['kind']=='unpin');requester.remove(row)
                    requester.insert(next(i for i,r in enumerate(requester) if r['kind']=='wait'),row)
                else:
                    where=next(i for i,r in enumerate(requester) if r['kind']=='dtoh')
                    requester.insert(where,dict(kind='pin',code=0,tid=1,pid=102,address=6000,length=151,flags=1))
                    requester.insert(where+1,dict(kind='unpin',code=0,tid=1,pid=102,address=6000))
                for i,row in enumerate(requester):row['seq']=i+1
                with self.subTest(memory=memory,fault=fault),self.assertRaises(AssertionError):
                    nvlink_run.validate_native(*rows,17,'batch-healthy',101,102,memory)

    def test_host_copy_after_close_and_unpin_before_close_restore_are_rejected(self):
        for fault in ('late-copy','early-unpin'):
            source,requester=self.evidence('batch-healthy','host')
            close=max(i for i,r in enumerate(requester) if r['kind']=='close')
            if fault=='late-copy':
                extra=[dict(kind='htod',source=2077,destination=3077,length=7,stream=4),
                       dict(kind='record',event=5,stream=4),dict(kind='pop'),
                       dict(kind='wait',event=5),dict(kind='pop')]
                requester[close+2:close+2]=[dict(pid=102,tid=1,code=0,**r) for r in extra]
            else:
                unpin=next(r for r in requester if r['kind']=='unpin')
                requester.remove(unpin);unpin['tid']=2
                requester.insert(close+1,unpin)
            for i,row in enumerate(requester):row['seq']=i+1
            with self.subTest(fault=fault),self.assertRaises(AssertionError):
                nvlink_run.validate_native(source,requester,17,'batch-healthy',101,102,'host')

    def test_exact_ranges_initialization_drain_and_cleanup_are_required(self):
        for mode in ('healthy', 'graceful', 'requester-exit', 'stale', 'batch-healthy', 'batch-graceful', 'batch-requester-exit'):
            with self.subTest(mode=mode):
                nvlink_run.validate_native(*self.evidence(mode), 17, mode, 101, 102)
        controls = [
            ('driver-error', 1, 'open', {'code': 1}),
            ('wrong-process', 1, 'open', {'pid': 103}),
            ('other-handle', 1, 'open', {'handle': 1235}),
            ('lazy-peer-missing', 1, 'open', {'flags': 0}),
            ('wrong-range', 1, 'copy', {'length': 18}),
            ('wrong-mapping', 1, 'copy', {'source': 1067}),
            ('other-thread', 1, 'record', {'tid': 2}),
            ('other-stream', 1, 'record', {'stream': 3}),
            ('other-event', 1, 'wait', {'event': 6}),
            ('missing-drain', 1, 'wait', {'kind': 'ignored'}),
            ('close-not-free', 1, 'close', {'kind': 'free'}),
            ('wrong-close-owner', 1, 'close', {'address': 1000}),
            ('missing-free', 0, 'free', {'kind': 'ignored'}),
            ('partial-zero', 0, 'memset', {'length': 151}),
            ('uninitialized', 0, 'memset', {'value': 1}),
            ('no-init-drain', 0, 'sync', {'kind': 'ignored'}),
        ]
        for name, side, kind, change in controls:
            rows = copy.deepcopy(self.evidence())
            next(row for row in rows[side] if row['kind'] == kind).update(change)
            with self.subTest(control=name), self.assertRaises(AssertionError):
                nvlink_run.validate_native(*rows, 17, 'healthy', 101, 102)

    def test_early_free_is_not_accepted_as_cleanup(self):
        for side in (0, 1):
            rows = self.evidence()
            free = next(row for row in rows[side] if row['kind'] == 'free')
            rows[side].remove(free)
            rows[side].insert(1, free)
            for index, row in enumerate(rows[side]):
                row['seq'] = index+1
            with self.subTest(side=side), self.assertRaises(AssertionError):
                nvlink_run.validate_native(*rows, 17, 'healthy', 101, 102)
        rows = self.evidence()
        first_close = next(row['seq'] for row in rows[1] if row['kind'] == 'close')
        pop = next(row for row in rows[1] if row['kind'] == 'pop' and row['seq'] > first_close)
        rows[1].remove(pop)
        for index, row in enumerate(rows[1]):
            row['seq'] = index+1
        with self.assertRaises(AssertionError):
            nvlink_run.validate_native(*rows, 17, 'healthy', 101, 102)
        rows = self.evidence()
        last_close = max(row['seq'] for row in rows[1] if row['kind'] == 'close')
        next(row for row in rows[1] if row['kind'] == 'pop' and row['seq'] > last_close)['kind'] = 'ignored'
        with self.assertRaises(AssertionError):
            nvlink_run.validate_native(*rows, 17, 'healthy', 101, 102)

    def test_subbatch_needs_each_slice_drain_and_both_local_owners(self):
        for fault in ('missing-middle-wait', 'wrong-second-owner', 'close-before-last', 'duplicate-free'):
            source, requester = self.evidence('batch-healthy')
            if fault == 'missing-middle-wait':
                [r for r in requester if r['kind']=='wait'][1]['kind']='ignored'
            elif fault == 'wrong-second-owner':
                [r for r in requester if r['kind']=='copy'][1]['destination']=2067
            elif fault == 'duplicate-free':
                [r for r in requester if r['kind']=='free'][1]['address']=2000
            else:
                close=next(r for r in requester if r['kind']=='close')
                requester.remove(close)
                last=[r for r in requester if r['kind']=='copy'][2]
                requester.insert(requester.index(last),close)
                for index,row in enumerate(requester): row['seq']=index+1
            with self.subTest(fault=fault), self.assertRaises(AssertionError):
                nvlink_run.validate_native(source,requester,17,'batch-healthy',101,102)

    def test_live_requester_allocations_precede_copies_and_do_not_overlap(self):
        for mode in ('graceful', 'batch-graceful'):
            for fault in ('late', 'zero', 'negative', 'overflow', 'alias', 'overlap'):
                if fault in ('alias', 'overlap') and not mode.startswith('batch-'):
                    continue
                source, requester = self.evidence(mode)
                allocation = [r for r in requester if r['kind'] == 'allocate'][-1]
                if fault == 'late':
                    requester.remove(allocation)
                    close = next(r for r in requester if r['kind'] == 'close')
                    requester.insert(requester.index(close), allocation)
                    for index, row in enumerate(requester): row['seq'] = index+1
                else:
                    old = allocation['address']
                    new = {'zero': 0, 'negative': -1, 'overflow': (1 << 64)-100,
                           'alias': 2000, 'overlap': 2100}[fault]
                    allocation['address'] = new
                    for row in requester:
                        if row['kind'] == 'free' and row['address'] == old:
                            row['address'] = new
                        elif row['kind'] == 'copy' and old <= row['source'] < old+151:
                            row['source'] += new-old
                with self.subTest(mode=mode, fault=fault), self.assertRaises(AssertionError):
                    nvlink_run.validate_native(source, requester, 17, mode, 101, 102)

    def test_observer_forwards_arguments_outputs_and_native_failures(self):
        root = Path(__file__).resolve().parent
        with tempfile.TemporaryDirectory() as directory:
            tmp = Path(directory)
            library, control, log = tmp/'observer.so', tmp/'control', tmp/'native.jsonl'
            flags = ['gcc', '-std=c11', '-O2', '-Wall', '-Wextra', '-Werror', '-pthread']
            subprocess.run([*flags, '-fPIC', '-shared', str(root/'nvlink_observer.c'), '-ldl', '-o', str(library)], check=True, capture_output=True)
            subprocess.run([*flags, '-rdynamic', str(root/'test_nvlink_observer.c'), '-ldl', '-o', str(control)], check=True, capture_output=True)
            result = subprocess.run([str(control)], env={**os.environ, 'LD_PRELOAD': str(library), 'ORBITTE_IPC_LOG': str(log)}, capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            rows = [json.loads(line) for line in log.read_text().splitlines()]
            self.assertEqual([(r['kind'],r['code']) for r in rows], [('allocate',0),('memset',0),('sync',0),('export',0),('open',0),('open',0),('copy',1),('record',1),('wait',700),('close',0),('close',1),('pop',201),('free',1),('pin',0),('htod',1),('dtoh',700),('unpin',1)])
            self.assertEqual(rows[4]['address'], rows[5]['address'])
            self.assertEqual(rows[4]['handle'], rows[3]['handle'])
            self.assertEqual(rows[6]['source'], 123)
            self.assertEqual(rows[6]['destination'], 456)
            self.assertEqual(rows[6]['length'], 17)
            self.assertEqual(rows[-4]['address'], 0x123456789abc)
            self.assertEqual(rows[-3]['source'], rows[-2]['destination'])
            self.assertEqual(rows[-1]['address'], 0x123456789abc)


if __name__ == '__main__':
    unittest.main()
