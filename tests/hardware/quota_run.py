"""Run two packaged RDMA consumers on one locked RNIC and one private shm name."""
import argparse
import fcntl
import os
from pathlib import Path
import subprocess
import tempfile
import time
import uuid
from contextlib import ExitStack


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact', type=Path, required=True)
    parser.add_argument('--nic', required=True)
    parser.add_argument('--nic-lock', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    binary = args.artifact / 'installed/bin/orbitte-rdma-host'
    args.output.mkdir(parents=True, exist_ok=False)
    slots = 'orbitte-quota-' + uuid.uuid4().hex
    with args.nic_lock.open('a') as lock, tempfile.TemporaryDirectory() as ready, ExitStack() as logs:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        children = []
        try:
            for i in range(2):
                env = dict(os.environ, ORBITTE_QUOTA_NIC=args.nic, ORBITTE_QUOTA_SLOTS='/' + slots,
                           ORBITTE_QUOTA_READY_DIR=ready, ORBITTE_QUOTA_ID=str(i))
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
