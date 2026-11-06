"""Coordinate two installed fabric consumers using a fresh shared run directory."""
import argparse
import fcntl
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import selectors
import socket
import subprocess
import time
import uuid


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def publish(path, record):
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(record, indent=2) + '\n')
    tmp.replace(path)


def await_file(path):
    end = time.monotonic() + 180
    while not path.exists():
        if time.monotonic() >= end:
            raise TimeoutError(str(path))
        time.sleep(0.2)
    return json.loads(path.read_text())


def output(*args):
    return subprocess.check_output(args, text=True, timeout=20, env={**os.environ, 'LC_ALL': 'C'})


def inventory(gpu):
    raw = output('nvidia-smi', '-q', '-i', gpu)
    lines = raw.splitlines()
    start = next((i for i, line in enumerate(lines) if line.strip() == 'Fabric'), None)
    if start is None:
        raise ValueError('missing GPU Fabric probe')
    base = len(lines[start]) - len(lines[start].lstrip())
    section = []
    for line in lines[start + 1:]:
        if not line.strip():
            continue
        if len(line) - len(line.lstrip()) <= base:
            break
        section.append(line)
    fabric = '\n'.join(section)
    fields = dict((k.strip().lower().replace(' ', ''), v.strip()) for k, v in
                  (line.split(':', 1) for line in section if ':' in line))
    if fields.get('state') != 'Completed' or fields.get('status') != 'Success':
        raise ValueError('fabric manager did not confirm this GPU')
    cluster = uuid.UUID(fields['clusteruuid'])
    clique = int(fields['cliqueid'], 0)
    actual = output('nvidia-smi', '-i', gpu, '--query-gpu=uuid', '--format=csv,noheader').strip()
    if cluster.int == 0 or clique < 0 or actual != gpu:
        raise ValueError('invalid multi-node fabric/GPU identity')
    return {'gpu': actual, 'cluster': cluster.hex, 'clique': clique, 'fabric': fabric,
            'boot': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            'host': socket.gethostname(), 'links': output('nvidia-smi', 'nvlink', '-s', '-i', gpu)}


def counters(gpu):
    raw = output('nvidia-smi', 'nvlink', '-gt', 'd', '-i', gpu)
    values = {(link, direction): int(n.replace(',', '')) for link, direction, n in
              re.findall(r'Link\s+(\d+)\s*:\s*Data\s+(Tx|Rx)\s*:\s*([\d,]+)\s+KiB', raw)}
    if not values or {direction for _, direction in values} != {'Tx', 'Rx'}:
        raise ValueError('require readable NVLink TX and RX payload counters')
    return {f'{link}-{direction}': n for (link, direction), n in values.items()}


def confirm(before, after, pairs):
    if before.keys() != after.keys():
        raise ValueError('NVLink inventory changed during the exercise')
    delta = {k: after[k] - n for k, n in before.items()}
    if any(n < 0 for n in delta.values()):
        raise ValueError('NVLink payload counters reset')
    for direction in ('Tx', 'Rx'):
        if sum(n for k, n in delta.items() if k.endswith(direction)) < 4096 * pairs:
            raise ValueError(f'insufficient NVLink {direction} payload evidence')
    return delta


def run(a):
    art = a.artifact.resolve(strict=True)
    package = json.loads((art / 'manifest.json').read_text())
    binary = art / 'installed/bin/orbite-mnnvl'
    if (package['status'] != 'passed' or package['dirty'] or package['consumer'] != 'mnnvl'
            or digest(binary) != package['binary_sha256']
            or digest(art / Path(package['archive']).name) != package['archive_sha256']
            or digest(Path(__file__)) != package['fixture_files_sha256']['tests/hardware/mnnvl_run.py']):
        raise ValueError('artifact identity or packaged driver does not match')
    shared = a.shared_run.resolve()
    if a.role == 'source':
        shared.mkdir(parents=True, exist_ok=False)
    parts = a.bind.rsplit(':', 1)
    address = ipaddress.ip_address(parts[0].strip('[]'))
    if address.is_loopback or address.is_unspecified:
        raise ValueError('use a concrete address reachable from the other node')
    metadata = inventory(a.gpu)
    metadata.update(commit=package['source_commit'], archive=package['archive_sha256'],
                    binary=package['binary_sha256'], profile=a.profile, numa=a.numa)
    # The cluster/clique pair identifies the partition, independently of the control IP.
    domain = uuid.uuid5(uuid.UUID(metadata['cluster']), str(metadata['clique'])).hex
    common = [str(binary), a.role, a.profile, domain, a.bind, str(a.numa)]
    env = {**os.environ, 'CUDA_VISIBLE_DEVICES': a.gpu, 'LC_ALL': 'C'}
    pairs = 4 if a.profile == 'egm' else 1
    expected = f'PASS {a.role} {pairs} 4194341'
    if a.role == 'requester':
        peer = await_file(shared / 'source-ready.json')
        for k in ('profile', 'cluster', 'clique', 'archive', 'commit'):
            if peer[k] != metadata[k]:
                raise ValueError(f'peer disagrees on {k}')
        if metadata['boot'] == peer['boot'] or metadata['gpu'] == peer['gpu']:
            raise ValueError('require two different hosts and physical GPUs')
        before = counters(a.gpu)
        result = subprocess.run([*common, *peer['grant']], check=True, capture_output=True,
                                text=True, env=env, timeout=150)
        if result.stdout.strip() != expected:
            raise ValueError('missing requester byte and guard receipts')
        after = counters(a.gpu)
        publish(shared / 'requester-done.json', dict(metadata, before=before, after=after,
                                                    delta=confirm(before, after, pairs), receipt=expected))
        if await_file(shared / 'source-done.json')['receipt'] != f'PASS source {pairs} 4194341':
            raise ValueError('missing source verification')
        return
    proc = subprocess.Popen(common, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=env)
    try:
        with selectors.DefaultSelector() as sel:
            sel.register(proc.stdout, selectors.EVENT_READ)
            if not sel.select(60):
                raise TimeoutError('source fabric allocation')
        ready = proc.stdout.readline().split()
        if ready[:1] != ['READY'] or len(ready) != (5 if pairs == 4 else 4):
            raise RuntimeError('missing source registration')
        # Begin both windows after local allocation initialization.
        before = counters(a.gpu)
        publish(shared / 'source-ready.json', dict(metadata, grant=ready[1:], before=before))
        await_file(shared / 'requester-done.json')
        receipt, _ = proc.communicate('verify\n', timeout=75)
        if proc.returncode or receipt.strip() != expected:
            raise RuntimeError('missing source byte, guard or drain receipt')
        after = counters(a.gpu)
        publish(shared / 'source-done.json', dict(metadata, before=before, after=after,
                                                  delta=confirm(before, after, pairs), receipt=expected))
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--role', choices=['source', 'requester'], required=True)
    parser.add_argument('--profile', choices=['device', 'egm'], required=True)
    for flag in ('artifact', 'shared-run', 'gpu-lock'):
        parser.add_argument('--' + flag, type=Path, required=True)
    parser.add_argument('--gpu', required=True)
    parser.add_argument('--bind', required=True)
    parser.add_argument('--numa', type=int, required=True)
    args = parser.parse_args()
    with args.gpu_lock.resolve(strict=True).open('r+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        run(args)
