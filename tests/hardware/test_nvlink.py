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
    def evidence(self, mode='healthy'):
        batched = mode.startswith("batch-")
        mode = mode.removeprefix("batch-")
        source, requester = [], []

        def add(rows, kind, **values):
            rows.append(dict(kind=kind, code=0, tid=1, seq=len(rows)+1,
                             pid=101 if rows is source else 102, **values))

        add(source, 'allocate', address=1000, length=2 << 20)
        add(source, 'memset', address=1000, length=2 << 20, value=0)
        add(source, 'sync')
        count = 2 if mode == 'healthy' else 0 if mode == 'stale' else 1
        if count:
            add(requester, 'allocate', address=2000, length=151)
            if batched: add(requester, 'allocate', address=4000, length=151)
        for index in range(count):
            add(source, 'export', address=1000, handle=1234)
            add(requester, 'open', address=3000, handle=1234, flags=1)
            read = mode == 'healthy' and index == 0
            for part, (offset,length) in enumerate([(10,7),(0,5),(5,5)] if batched else [(0,17)]):
                remote, own = 3067+offset, (2000 if part%2 == 0 else 4000)+67+offset
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
        return source, requester

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
            self.assertEqual([(r['kind'],r['code']) for r in rows], [('allocate',0),('memset',0),('sync',0),('export',0),('open',0),('open',0),('copy',1),('record',1),('wait',700),('close',0),('close',1),('pop',201),('free',1)])
            self.assertEqual(rows[4]['address'], rows[5]['address'])
            self.assertEqual(rows[4]['handle'], rows[3]['handle'])
            self.assertEqual(rows[6]['source'], 123)
            self.assertEqual(rows[6]['destination'], 456)
            self.assertEqual(rows[6]['length'], 17)


if __name__ == '__main__':
    unittest.main()
