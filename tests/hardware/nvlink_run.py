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


def validate_native(source, requester, payload, mode, source_pid, requester_pid):
    for rows, pid in [(source, source_pid), (requester, requester_pid)]:
        assert [row['seq'] for row in rows] == list(range(1, len(rows)+1)), 'native sequence gap'
        assert all(row['pid'] == pid and row['code'] == 0 for row in rows), 'native PID/driver result'
    select = lambda rows, kind: [row for row in rows if row['kind'] == kind]
    expected = 2 if mode == 'healthy' else 0 if mode == 'stale' else 1
    exports, imports, copies = select(source, 'export'), select(requester, 'open'), select(requester, 'copy')
    assert len(exports) == len(imports) == len(copies) == expected, 'native IPC/copy count'
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
    local = select(requester, 'allocate')
    assert len(local) == int(mode != 'stale'), 'requester allocation count'
    if local:
        assert local[0]['length'] == payload+134, 'requester allocation extent'
    closes = select(requester, 'close')
    assert len(closes) == (0 if mode == 'requester-exit' else expected), 'mapping close count'
    for index, (opened, copy) in enumerate(zip(imports, copies)):
        read = mode == 'healthy' and index == 0
        remote, own = opened['address']+67, local[0]['address']+67
        assert (copy['source'], copy['destination'], copy['length']) == ((remote, own, payload) if read else (own, remote, payload)), 'copy range/direction'
        assert opened['seq'] < copy['seq'], 'copy before import'
        events = [r for r in select(requester, 'record') if r['seq'] > copy['seq'] and r['stream'] == copy['stream']]
        assert events, 'copy has no completion event'
        event = events[0]
        waits = [r for r in select(requester, 'wait') if r['seq'] > event['seq'] and r['event'] == event['event']]
        assert waits, 'copy has no completed event'
        wait = waits[0]
        pops = select(requester, 'pop')
        assert any(copy['seq'] < p['seq'] < event['seq'] for p in pops) is False, 'copy escaped owning context before event record'
        if mode != 'requester-exit':
            close = closes[index]
            assert close['address'] == opened['address'] and wait['seq'] < close['seq'], 'close before matching DMA drain'
            assert any(wait['seq'] < p['seq'] < close['seq'] for p in pops), 'copy context not restored'
            assert any(p['seq'] > close['seq'] for p in pops), 'close context not restored'
    for rows, allocations in [(source, source_alloc), (requester, local)]:
        frees = select(rows, 'free')
        assert len(frees) == (0 if mode == 'requester-exit' else len(allocations)), 'native allocation release/retention'
        assert all(free['address'] in [a['address'] for a in allocations] for free in frees), 'free owner mismatch'
    return {'native_records': len(source)+len(requester), 'exports': len(exports), 'imports': len(imports), 'copies': len(copies), 'closes': len(closes)}


def run(args):
    root = args.output.resolve()
    checkout = Path(__file__).resolve().parents[2]
    if (checkout / 'Cargo.toml').is_file() and root.is_relative_to(checkout):
        raise ValueError('results must stay outside the source checkout')
    root.mkdir(parents=True, exist_ok=False)
    manifest = {'status': 'running', 'mode': args.mode, 'payload': args.payload,
                'host': socket.gethostname(), 'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                'binary_sha256': digest(args.binary), 'observer_sha256': digest(args.observer),
                'started_unix': time.time(), 'children': [], 'performance_measurement': False}
    locks, children, files = [], [], []

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
        if role == 'requester' and args.mode != 'stale': env['ORBITTE_IPC_HOLD'] = str(root/'release-close')
        command = [str(args.binary.resolve()), role, str(ordinal), str(args.payload), str(root), args.mode]
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
        if args.mode != 'stale':
            wait_file('release-close.ready')
            (root/'requester.maps').write_text(Path(f'/proc/{requester.pid}/maps').read_text())
            if args.mode == 'graceful':
                (root/'close-source').write_text('close\n'); wait_file('close-pending')
            if args.mode == 'requester-exit': requester.kill()
            else: (root/'release-close').write_text('release\n')
        code = requester.wait(timeout=35)
        assert code == (-9 if args.mode == 'requester-exit' else 0), f'requester exit {code}'
        (root/'finish').write_text('requester reaped\n')
        assert source.wait(timeout=35) == 0, 'source failed'
        for entry, child in zip(manifest['children'], children): entry['exit_code'] = child.returncode
        source_log = (root/'source.stdout').read_text()
        assert ('NVLINK_SOURCE_RETAINED' if args.mode == 'requester-exit' else 'NVLINK_SOURCE_OK') in source_log
        if args.mode != 'requester-exit':
            assert ('NVLINK_STALE_REJECTED' if args.mode == 'stale' else 'NVLINK_REQUESTER_OK') in (root/'requester.stdout').read_text()
        manifest['loaded_cuda'] = {}
        for role in ('source', 'requester'):
            maps = root/(role+'.maps')
            if not maps.exists():
                assert role == 'requester' and args.mode == 'stale'
                continue
            paths = {line.split()[-1] for line in maps.read_text().splitlines() if 'libcuda.so' in line}
            assert len(paths) == 1, 'missing or mixed CUDA driver mapping'
            manifest['loaded_cuda'][role] = {path: digest(path) for path in paths}
        manifest['native'] = validate_native(records(root/'source.native.jsonl'), records(root/'requester.native.jsonl'), args.payload, args.mode, source.pid, requester.pid)
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
    parser.add_argument('--payload', type=int, required=True)
    parser.add_argument('--mode', choices=('healthy','graceful','requester-exit','stale'), default='healthy')
    run(parser.parse_args())
