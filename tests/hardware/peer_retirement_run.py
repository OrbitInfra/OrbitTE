"""Installed RDMA / same-host NVLink / cross-host fabric retirement and restart.

Run only on reserved hardware. Both hosts need the identical installed artifact.
The driver starts two sources and one requester; it never changes device state.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import selectors
import shlex
import subprocess
import time


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main(a):
    out = a.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    artifact = a.artifact.resolve(strict=True)
    manifest = json.loads((artifact / 'manifest.json').read_text())
    binary = artifact / 'installed/bin/orbite-peer-retirement'
    if (manifest['consumer'] != 'peer-retirement' or manifest['status'] != 'passed'
            or manifest['dirty'] or digest(binary) != manifest['binary_sha256']
            or digest(Path(__file__)) != manifest['fixture_files_sha256']['tests/hardware/peer_retirement_run.py']):
        raise ValueError('installed artifact identity mismatch')
    if a.profile == 'nvlink' and a.source_host:
        raise ValueError('legacy NVLink requires two same-host processes')
    if a.profile == 'mnnvl' and (not a.source_host or not a.domain):
        raise ValueError('MNNVL needs a remote host and authoritative fabric domain')
    remote_binary = a.source_binary or str(binary)
    commands, processes, files = [], [], []

    def command(host, words):
        return ['ssh', '-oBatchMode=yes', host, shlex.join(words)] if host else words

    def inventory(label, host, gpu):
        words = ['sh', '-c', 'uname -a; cat /proc/sys/kernel/random/boot_id; '
                 + ('rdma link; ibv_devinfo' if a.profile == 'rdma'
                    else 'nvidia-smi -q -i ' + shlex.quote(gpu) + '; nvidia-smi nvlink -s -i ' + shlex.quote(gpu))]
        result = subprocess.run(command(host, words), check=True, capture_output=True, text=True, timeout=30)
        (out / (label + '.inventory')).write_text(result.stdout + result.stderr)

    def spawn(name, role, host, bind, device, gpu, peers=()):
        words = ['env', 'CUDA_VISIBLE_DEVICES=' + gpu, remote_binary if host else str(binary),
                 role, a.profile, bind, device, a.domain or '-', *peers]
        argv = command(host, words)
        commands.append(argv)
        err = (out / (name + '.stderr')).open('w')
        files.append(err)
        p = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=err,
                             text=True, bufsize=1)
        processes.append(p)
        return p, name

    def line(child, prefix):
        p, name = child
        with selectors.DefaultSelector() as select:
            select.register(p.stdout, selectors.EVENT_READ)
            if not select.select(180):
                raise TimeoutError(name)
            value = p.stdout.readline().strip()
        with (out / (name + '.stdout')).open('a') as log:
            log.write(value + '\n')
        if not value.startswith(prefix):
            raise RuntimeError(f'{name}: expected {prefix}, got {value!r}')
        return value.split()[1:]

    def send(child, text):
        child[0].stdin.write(text + '\n')
        child[0].stdin.flush()

    def finish(child):
        send(child, 'VERIFY')
        line(child, 'PASS source')
        if child[0].wait(timeout=90):
            raise RuntimeError(child[1] + ' failed')

    report = {'source_commit': manifest['source_commit'], 'package_sha256': manifest['archive_sha256'],
              'profile': a.profile, 'commands': commands, 'status': 'failed', 'hardware_run': True}
    try:
        if a.source_host:
            remote_hash = subprocess.check_output(command(a.source_host, ['sha256sum', remote_binary]), text=True, timeout=20).split()[0]
            if remote_hash != manifest['binary_sha256']:
                raise ValueError('source binary differs from requester artifact')
        inventory('source-before', a.source_host, a.source_gpu)
        inventory('requester-before', None, a.gpu)
        first = spawn('source-old', 'source', a.source_host, a.source_bind, a.source_device, a.source_gpu)
        endpoint, epoch = line(first, 'READY ')
        second = spawn('source-other', 'source', a.source_host, a.other_bind, a.source_device, a.source_gpu)
        other_endpoint, other_epoch = line(second, 'READY ')
        requester = spawn('requester', 'requester', None, a.bind, a.device, a.gpu,
                          (endpoint, epoch, other_endpoint, other_epoch))
        line(requester, 'RETIRED')
        finish(first)
        fresh = spawn('source-new', 'source', a.source_host, endpoint, a.source_device, a.source_gpu)
        new_endpoint, new_epoch = line(fresh, 'READY ')
        if new_epoch == epoch or new_endpoint != endpoint:
            raise ValueError('source restart identity mismatch')
        send(requester, f'NEW {new_endpoint} {new_epoch}')
        line(requester, 'PASS requester')
        if requester[0].wait(timeout=90):
            raise RuntimeError('requester failed')
        finish(fresh)
        finish(second)
        inventory('source-after', a.source_host, a.source_gpu)
        inventory('requester-after', None, a.gpu)
        report['status'] = 'passed'
    finally:
        for p in processes:
            if p.poll() is None:
                p.terminate()
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait(timeout=5)
        for f in files:
            f.close()
        (out / 'report.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--artifact', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--profile', choices=('rdma', 'nvlink', 'mnnvl'), required=True)
    p.add_argument('--source-host')
    p.add_argument('--source-binary')
    p.add_argument('--source-bind', required=True)
    p.add_argument('--other-bind', required=True)
    p.add_argument('--bind', required=True)
    p.add_argument('--device', default='-')
    p.add_argument('--source-device', default='-')
    p.add_argument('--gpu', default='0')
    p.add_argument('--source-gpu', default='0')
    p.add_argument('--domain')
    main(p.parse_args())
