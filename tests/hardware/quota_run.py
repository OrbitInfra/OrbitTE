"""Run two packaged RDMA consumers on one locked RNIC and one private shm name."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import uuid
from contextlib import ExitStack

from build_installed import digest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact', type=Path, required=True)
    parser.add_argument('--nic', required=True)
    parser.add_argument('--nic-lock', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    binary = args.artifact / 'installed/bin/orbite-rdma-host'
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = json.loads((args.artifact / 'manifest.json').read_text())
    if (manifest['status'] != 'passed' or manifest['dirty'] or manifest['consumer'] != 'host'
            or digest(binary) != manifest['binary_sha256']
            or digest(args.artifact / Path(manifest['archive']).name) != manifest['archive_sha256']):
        raise ValueError('expected the clean, verified packaged host consumer')
    slots = 'orbite-quota-' + uuid.uuid4().hex
    evidence = {key: manifest[key] for key in ('source_commit', 'binary_sha256', 'archive_sha256')}
    evidence.update(nic=args.nic, slots=slots)
    (args.output / 'installation.json').write_text(json.dumps(evidence, indent=2))
    with args.nic_lock.resolve(strict=True).open('r+') as lock, tempfile.TemporaryDirectory() as ready, ExitStack() as logs:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        children = []
        try:
            for i in range(2):
                env = dict(os.environ, ORBITE_QUOTA_NIC=args.nic, ORBITE_QUOTA_SLOTS='/' + slots,
                           ORBITE_QUOTA_READY_DIR=ready, ORBITE_QUOTA_ID=str(i))
                output = logs.enter_context((args.output / f'participant-{i}.log').open('x'))
                children.append(subprocess.Popen([binary, '--quota'], env=env, stdout=output,
                                                 stderr=subprocess.STDOUT))
            deadline = time.monotonic() + 120
            for child in children:
                if child.wait(timeout=max(0, deadline - time.monotonic())) != 0:
                    raise RuntimeError(f'quota participant failed; see {args.output}')
        finally:
            for child in children:
                if child.poll() is None:
                    child.kill()
                child.wait(timeout=10)
            (Path('/dev/shm') / slots).unlink(missing_ok=True)


if __name__ == '__main__':
    main()
