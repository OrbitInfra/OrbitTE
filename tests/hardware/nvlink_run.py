"""Run a locked, bounded, independent-process CUDA IPC correctness case locally."""
import argparse
import csv
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import time


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def records(path):
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def validate_pins(rows, retain):
    active, imported = {}, None
    for row in rows:
        kind = row['kind']
        if kind == 'pin':
            address, length = row['address'], row['length']
            assert row['flags'] == 1 and 0 < address < address+length <= 1 << 64, 'invalid pinned allocation'
            assert all(address+length <= a or a+r['length'] <= address for a,r in active.items()), 'overlapping pinned owners'
            active[address] = row
        elif kind in ('htod', 'dtoh'):
            address = row['source' if kind == 'htod' else 'destination']
            assert any(a <= address < address+row['length'] <= a+r['length'] for a,r in active.items()), 'copy host pointer is not owned pinned memory'
        elif kind == 'unpin':
            pin = active.pop(row['address'], None)
            assert pin, 'unpin without live owner'
            for copy in rows[pin['seq']:row['seq']-1]:
                if copy['kind'] not in ('htod', 'dtoh'): continue
                address = copy['source' if copy['kind'] == 'htod' else 'destination']
                if not pin['address'] <= address < pin['address']+pin['length']: continue
                events = [r for r in rows[copy['seq']:row['seq']-1] if r['kind']=='record' and r['stream']==copy['stream']]
                assert events, 'unpin before copy record'
                event = events[0]
                waits = [r for r in rows[event['seq']:row['seq']-1] if r['kind']=='wait' and r['event']==event['event']]
                assert waits, 'unpin before physical completion'
                assert any(r['kind']=='pop' and r['tid']==copy['tid'] for r in rows[waits[0]['seq']:row['seq']-1]), 'unpin before copy context restored'
        elif kind == 'open' and imported is None:
            imported = list(active.values())
    assert retain or not active, 'pinned allocation leaked'
    return imported or []


def validate_native(source, requester, payload, mode, source_pid, requester_pid, memory="device"):
    cached = mode.startswith("cache-")
    batched = cached or mode.startswith("batch-")
    mode = mode.removeprefix("cache-").removeprefix("batch-")
    for rows, pid in [(source, source_pid), (requester, requester_pid)]:
        assert [row['seq'] for row in rows] == list(range(1, len(rows)+1)), 'native sequence gap'
        assert all(row['pid'] == pid and row['code'] == 0 for row in rows), 'native PID/driver result'
    select = lambda rows, kind: [row for row in rows if row['kind'] == kind]
    expected = 1 if cached else 2 if mode == 'healthy' else 0 if mode == 'stale' else 1
    exports, imports = select(source, 'export'), select(requester, 'open')
    assert len(exports) == len(imports) == expected, 'native IPC count'
    validate_pins(source, False)
    pins = validate_pins(requester, mode == 'requester-exit')
    expected_pins = (2 if batched else 1) if memory == 'host' else 1 if memory == 'mixed' else 0
    assert len(pins) == (0 if mode == 'stale' else expected_pins), 'persistent pinned requester count'
    copies = []
    source_alloc = select(source, 'allocate')
    assert len(source_alloc) == 1 and source_alloc[0]['length'] == ((payload + 134 + (2 << 20)-1) & ~((2 << 20)-1)), 'export allocation extent'
    initialized = select(source, 'memset')
    assert len(initialized) == 1, 'missing full export initialization'
    init = initialized[0]
    assert (init['address'], init['length'], init['value']) == (source_alloc[0]['address'], source_alloc[0]['length'], 0), 'export padding not zero initialized'
    syncs = select(source, 'sync')
    assert syncs and source_alloc[0]['seq'] < init['seq'] < syncs[0]['seq'], 'allocation initialization not synchronized'
    assert all(syncs[0]['seq'] < row['seq'] for row in exports), 'export before initialization drain'
    assert all(row['address'] == source_alloc[0]['address'] for row in exports), 'export base'
    assert [row['handle'] for row in exports] == [row['handle'] for row in imports], 'IPC handle pairing'
    assert all(row['flags'] == 1 for row in imports), 'lazy peer flag'
    local = sorted(select(requester, 'allocate') + pins, key=lambda row: row['seq'])
    assert len(local) == (2 if batched else int(mode != 'stale')), 'requester allocation count'
    if local:
        assert all(row['length'] == payload+134 for row in local), 'requester allocation extent'
    for allocation in source_alloc+local:
        assert 0 < allocation['address'] < allocation['address']+allocation['length'] <= 1 << 64, 'invalid allocation address range'
    for index, allocation in enumerate(local):
        for other in local[index+1:]:
            assert allocation['address']+allocation['length'] <= other['address'] or other['address']+other['length'] <= allocation['address'], 'overlapping live requester allocations'
    def local_device_copy(copy, rows, allocations):
        if copy['kind'] not in ('htod', 'dtoh'): return False
        address = copy['destination' if copy['kind']=='htod' else 'source']
        return any(a['seq'] < copy['seq'] and a['address'] <= address < address+copy['length'] <= a['address']+a['length']
                   and not any(f['address']==a['address'] and a['seq'] < f['seq'] < copy['seq'] for f in select(rows,'free'))
                   for a in allocations)

    closes = select(requester, 'close')
    assert len(closes) == (0 if mode == 'requester-exit' else expected), 'mapping close count'
    for index, opened in enumerate(imports):
        boundary = imports[index+1]['seq'] if index+1 < len(imports) else len(requester)+1
        cycle = [row for row in requester if opened['seq'] <= row['seq'] < boundary]
        end = closes[index]['seq'] if mode != 'requester-exit' else boundary
        interval = [row for row in cycle if row['seq'] < end]
        if not cached: assert not any(r['kind'] in ('pin','allocate') for r in interval), 'temporary allocation inside IPC copy cycle'
        cycle_copies = [r for r in interval if r['kind'] in ('copy','htod','dtoh') and (not cached or not local_device_copy(r,requester,select(local,'allocate')))]
        copies.extend(cycle_copies)
        assert len(cycle_copies) == (15 if cached and mode=='healthy' else 3 if batched else 1), 'copies outside their import cycle'
        read = mode == 'healthy' and index == 0
        width = payload//3
        slices = [(2*width, payload-2*width), (0,width), (width,width)] if batched else [(0,payload)]
        if cached and mode == 'healthy': slices *= 5
        for part, (copy, (offset, length)) in enumerate(zip(cycle_copies, slices)):
            if cached: read = mode == 'healthy' and part < 12
            next_copy = cycle_copies[part+1]['seq'] if part+1 < len(cycle_copies) else boundary
            assert opened['seq'] < copy['seq'] < boundary, 'copy outside its import cycle'
            if not cached: assert copy['tid'] == opened['tid'], 'copy/import thread mismatch'
            allocation = local[(part % 3 if cached else part) % len(local)]
            expected_api = ('dtoh' if read else 'htod') if allocation['kind']=='pin' else 'copy'
            assert copy['kind'] == expected_api, 'wrong native copy API'
            assert allocation['seq'] < copy['seq'], 'copy before requester allocation'
            remote, own = opened['address']+67+offset, allocation['address']+67+offset
            assert (copy['source'], copy['destination'], copy['length']) == ((remote, own, length) if read else (own, remote, length)), 'copy range/direction'
            events = [r for r in select(cycle, 'record') if copy['seq'] < r['seq'] < next_copy and r['stream'] == copy['stream']]
            assert events, 'copy has no completion event'
            event = events[0]
            waits = [r for r in select(cycle, 'wait') if event['seq'] < r['seq'] < next_copy and r['event'] == event['event']]
            assert waits, 'copy has no completed event'
            wait = waits[0]
            pops = [row for row in select(cycle, 'pop') if row['tid'] == copy['tid']]
            assert event['tid'] == wait['tid'] == copy['tid'], 'completion thread mismatch'
            assert not any(copy['seq'] < r['seq'] < event['seq'] and r['kind'] in ('copy','htod','dtoh') for r in cycle), 'completion event belongs to a later copy'
            assert not any(copy['seq'] < r['seq'] < wait['seq'] and r['kind'] in ('pin','allocate','unpin','free') for r in cycle), 'allocation changed before payload drain'
            assert not any(copy['seq'] < p['seq'] < event['seq'] for p in pops), 'copy escaped owning context before event record'
            assert any(wait['seq'] < p['seq'] < next_copy for p in pops), 'copy context not restored before next slice'
            if mode != 'requester-exit':
                close = closes[index]
                assert close['address'] == opened['address'] and wait['seq'] < close['seq'] < boundary, 'close before matching DMA drain'
                assert any(wait['seq'] < p['seq'] < close['seq'] for p in pops), 'copy context not restored'
                if not cached: assert close['tid'] == copy['tid'], 'close/import thread mismatch'
                after_close = [r for r in cycle if r['seq'] > close['seq'] and r['tid'] == close['tid']]
                assert after_close and after_close[0]['kind'] == 'pop', 'close context not immediately restored'
    assert len(copies) == (15 if cached and mode=='healthy' else expected*(3 if batched else 1)), 'native IPC copy count'
    assert len(select(requester,'copy')) == len(select(copies,'copy')), 'device copy outside IPC cycle'
    for rows, allocations in [(source, source_alloc), (requester, select(local,'allocate'))]:
        ipc_sequences = {row['seq'] for row in copies} if rows is requester else set()
        for copy in rows:
            if copy['kind'] not in ('htod','dtoh') or copy['seq'] in ipc_sequences: continue
            assert local_device_copy(copy,rows,allocations), 'host copy outside IPC cycle must use a live local device owner'
    for pin in pins:
        frees = [r for r in select(requester,'unpin') if r['address']==pin['address'] and r['seq'] > pin['seq']]
        assert len(frees) == int(mode != 'requester-exit'), 'pinned requester release/retention'
        if frees:
            final_close = closes[-1]
            restored = [r for r in requester if r['seq'] > final_close['seq'] and r['tid']==final_close['tid']]
            assert restored and restored[0]['kind']=='pop' and restored[0]['seq'] < frees[0]['seq'], 'pinned requester freed before close context restored'
    for rows, allocations in [(source, source_alloc), (requester, select(local,'allocate'))]:
        frees = select(rows, 'free')
        assert len(frees) == (0 if mode == 'requester-exit' else len(allocations)), 'native allocation release/retention'
        assert not frees or sorted(free['address'] for free in frees) == sorted(a['address'] for a in allocations), 'free owner mismatch'
        if frees:
            assert all(row['seq'] < frees[0]['seq'] for row in rows if row['kind'] in ('allocate', 'memset', 'sync', 'export', 'open', 'copy', 'htod', 'dtoh', 'record', 'wait', 'close')), 'allocation freed before its last native use'
            if rows is requester and closes:
                assert any(closes[-1]['seq'] < pop['seq'] < frees[0]['seq'] for pop in select(rows, 'pop')), 'requester allocation freed before close context restoration'
    return {'native_records': len(source)+len(requester), 'exports': len(exports), 'imports': len(imports), 'copies': len(copies), 'closes': len(closes), 'host_copies': sum(r['kind']!='copy' for r in copies), 'pinned_requesters': len(pins)}


def run(args):
    root = args.output.resolve()
    checkout = Path(__file__).resolve().parents[2]
    if (checkout / 'Cargo.toml').is_file() and root.is_relative_to(checkout):
        raise ValueError('results must stay outside the source checkout')
    root.mkdir(parents=True, exist_ok=False)
    manifest = {'status': 'running', 'mode': args.mode, 'memory': args.memory, 'payload': args.payload,
                'host': socket.gethostname(), 'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                'binary_sha256': digest(args.binary), 'observer_sha256': digest(args.observer),
                'started_unix': time.time(), 'children': [], 'performance_measurement': False}
    locks, children, files = [], [], []
    mode = args.mode.removeprefix("cache-").removeprefix("batch-")

    def inventory(name):
        result = subprocess.run(['nvidia-smi', '--query-gpu=index,uuid,memory.used', '--format=csv,noheader,nounits'], text=True, capture_output=True, check=True, timeout=15)
        (root / name).write_text(result.stdout)
        return {int(row[0]): {'uuid': row[1].strip(), 'memory_mib': int(row[2])} for row in csv.reader(result.stdout.splitlines())}

    def wait_file(name, timeout=20):
        limit = time.monotonic()+timeout
        while not (root/name).exists():
            if any(child.poll() is not None for child in children):
                raise RuntimeError('child exited while waiting for '+name)
            if time.monotonic() >= limit: raise TimeoutError(name)
            time.sleep(.005)

    def start(role, ordinal):
        env = {**os.environ, 'CUDA_VISIBLE_DEVICES': ','.join(manifest['gpu_uuids']),
               'LD_PRELOAD': str(args.observer.resolve()), 'ORBITTE_IPC_LOG': str(root/(role+'.native.jsonl'))}
        if role == 'requester' and mode != 'stale': env['ORBITTE_IPC_HOLD'] = str(root/'release-close')
        command = [str(args.binary.resolve()), role, str(ordinal), str(args.payload), str(root), args.mode, args.memory]
        output = (root/(role+'.stdout')).open('w'); error = (root/(role+'.stderr')).open('w')
        files.extend([output,error])
        child = subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL, stdout=output, stderr=error)
        children.append(child)
        manifest['children'].append({'role': role, 'pid': child.pid, 'command': command})
        return child

    try:
        assert manifest['binary_sha256'] == args.binary_sha256, 'binary provenance'
        assert manifest['observer_sha256'] == args.observer_sha256, 'observer provenance'
        before = inventory('gpu-before.csv')
        devices = [int(item) for item in args.devices.split(',')]
        assert len(devices) == 2 and len(set(devices)) == 2
        manifest['gpu_uuids'] = [before[index]['uuid'] for index in devices]
        assert all(before[index]['memory_mib'] == 0 for index in devices), 'selected GPU already in use'
        args.lock_root.mkdir(parents=True, exist_ok=True)
        for uuid in sorted(manifest['gpu_uuids']):
            lock = (args.lock_root/(uuid+'.lock')).open('a+')
            fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB); locks.append(lock)
        manifest['lock_paths'] = [lock.name for lock in locks]
        topo = subprocess.run(['nvidia-smi', 'topo', '-m'], capture_output=True, text=True, check=True, timeout=15)
        (root/'topology.txt').write_text(topo.stdout)
        topology = re.sub(r'\x1b\[[0-9;]*m', '', topo.stdout)
        header = next(line.split() for line in topology.splitlines() if 'GPU0' in line and not line.startswith('GPU0'))
        row = next(line.split() for line in topology.splitlines() if line.startswith('GPU'+str(devices[0])+'\t'))
        link = row[header.index('GPU'+str(devices[1]))+1]
        assert link.startswith('NV') and link[2:].isdigit(), 'selected GPUs do not have an NVLink topology edge'
        manifest['topology_edge'] = link
        source = start('source', 0); wait_file('ready')
        assert int((root/'ready').read_text().splitlines()[0]) == source.pid, 'source PID identity'
        (root/'source.maps').write_text(Path(f'/proc/{source.pid}/maps').read_text())
        requester = start('requester', 1)
        if mode != 'stale':
            wait_file('release-close.ready')
            (root/'requester.maps').write_text(Path(f'/proc/{requester.pid}/maps').read_text())
            if mode == 'graceful':
                (root/'close-source').write_text('close\n'); wait_file('close-pending')
            if mode == 'requester-exit': requester.kill()
            else: (root/'release-close').write_text('release\n')
        code = requester.wait(timeout=35)
        assert code == (-9 if mode == 'requester-exit' else 0), f'requester exit {code}'
        (root/'finish').write_text('requester reaped\n')
        assert source.wait(timeout=35) == 0, 'source failed'
        for entry, child in zip(manifest['children'], children): entry['exit_code'] = child.returncode
        source_log = (root/'source.stdout').read_text()
        assert ('NVLINK_SOURCE_RETAINED' if mode == 'requester-exit' else 'NVLINK_SOURCE_OK') in source_log
        if mode != 'requester-exit':
            assert ('NVLINK_STALE_REJECTED' if mode == 'stale' else 'NVLINK_REQUESTER_OK') in (root/'requester.stdout').read_text()
        manifest['loaded_cuda'] = {}
        for role in ('source', 'requester'):
            maps = root/(role+'.maps')
            if not maps.exists():
                assert role == 'requester' and mode == 'stale'
                continue
            paths = {line.split()[-1] for line in maps.read_text().splitlines() if 'libcuda.so' in line}
            assert len(paths) == 1, 'missing or mixed CUDA driver mapping'
            manifest['loaded_cuda'][role] = {path: digest(path) for path in paths}
        manifest['native'] = validate_native(records(root/'source.native.jsonl'), records(root/'requester.native.jsonl'), args.payload, args.mode, source.pid, requester.pid, args.memory)
        for _ in range(50):
            after = inventory('gpu-after.csv')
            if all(after[index] == before[index] for index in devices): break
            time.sleep(.1)
        assert all(after[index] == before[index] for index in devices), 'GPU allocation baseline not restored'
        manifest['status'] = 'passed'
    except Exception as error:
        manifest.update(status='failed', error=repr(error))
        raise
    finally:
        # Reap the importer before terminating its exporter, including failed runs.
        for child in reversed(children):
            if child.poll() is None: child.kill()
            child.wait(timeout=10)
        for entry, child in zip(manifest['children'], children): entry['exit_code'] = child.returncode
        for handle in files: handle.close()
        for lock in locks: fcntl.flock(lock, fcntl.LOCK_UN); lock.close()
        manifest['all_children_reaped'] = all(child.returncode is not None for child in children)
        manifest['locks_released'] = True
        manifest['finished_unix'] = time.time()
        manifest['files_sha256'] = {path.name: digest(path) for path in root.iterdir() if path.is_file() and path.name != 'manifest.json'}
        (root/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    print(json.dumps({'status': manifest['status'], 'output': str(root), 'native': manifest['native']}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('binary', 'observer', 'output', 'lock-root'): parser.add_argument('--'+name, required=True, type=Path)
    for name in ('binary-sha256', 'observer-sha256', 'devices'): parser.add_argument('--'+name, required=True)
    parser.add_argument('--memory', choices=('device','host','mixed'), default='device')
    parser.add_argument('--payload', type=int, required=True)
    parser.add_argument('--mode', choices=('healthy','graceful','requester-exit','stale','batch-healthy','batch-graceful','batch-requester-exit','cache-healthy','cache-graceful','cache-requester-exit'), default='healthy')
    run(parser.parse_args())
