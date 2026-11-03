"""Run the installed S3 byte and ownership checks on a locked GPU and local NVMe ext4."""
import argparse
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import tempfile


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inventory(directory, gpu):
    def query(*args):
        return subprocess.check_output(args, text=True, timeout=15).strip()

    fs = query('findmnt', '-n', '-o', 'FSTYPE', '-T', str(directory))
    if fs != 'ext4':
        raise ValueError('scratch directory must be on ext4')
    source = query('findmnt', '-n', '-o', 'MAJ:MIN', '-T', str(directory))
    rows = query('lsblk', '-s', '-n', '-r', '-o', 'KNAME,TYPE', '/dev/block/' + source).splitlines()
    disks = sorted({row.split()[0] for row in rows if row.split()[1] == 'disk'})
    if not disks:
        raise ValueError('could not resolve an NVMe namespace backing the ext4 mount')
    for disk in disks:
        controller = re.fullmatch(r'(nvme\d+)n\d+', disk)
        if not controller or (Path('/sys/class/nvme') / controller[1] / 'transport').read_text().strip() != 'pcie':
            raise ValueError('every underlying disk must be a local PCIe NVMe namespace')
    identity = query('nvidia-smi', '-i', gpu, '--query-gpu=uuid,pci.bus_id,name,driver_version',
                     '--format=csv,noheader')
    if identity.split(',', 1)[0].strip() != gpu:
        raise ValueError('expected the selected physical GPU UUID')
    return dict(filesystem=fs, device=source, nvme=disks, gpu=identity)


@contextlib.contextmanager
def installed_case(args, consumer, profile, driver):
    artifact = args.artifact.resolve()
    package = json.loads((artifact / 'manifest.json').read_text())
    binary = artifact / 'installed/bin' / ('orbitte-' + consumer)
    directory = args.directory.resolve(strict=True)
    args.output.mkdir(parents=True, exist_ok=False)
    evidence = dict(status='failed', profile=profile, host=socket.gethostname(),
                    scratch_directory=str(directory), source_commit=package['source_commit'],
                    binary_sha256=sha(binary), archive_sha256=package['archive_sha256'])
    try:
        if package['status'] != 'passed' or package['dirty'] or package['consumer'] != consumer:
            raise ValueError('expected a clean packaged ' + consumer + ' consumer')
        if evidence['binary_sha256'] != package['binary_sha256'] or sha(artifact / Path(package['archive']).name) != package['archive_sha256']:
            raise ValueError('packaged archive or binary digest mismatch')
        if sha(Path(__file__)) != package['fixture_files_sha256']['tests/hardware/storage_cuda_run.py']:
            raise ValueError('packaged driver digest mismatch')
        if sha(driver) != package['fixture_files_sha256']['tests/hardware/' + driver.name]:
            raise ValueError('packaged exercise driver digest mismatch')
        with contextlib.ExitStack() as owners:
            locks = [args.gpu_lock.resolve(strict=True), args.nvme_lock.resolve(strict=True)]
            if os.path.samefile(*locks):
                raise ValueError('GPU and NVMe require separate existing resource locks')
            for path in sorted(locks):
                lock = owners.enter_context(path.open('r+'))
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            evidence.update(locks=[str(p) for p in locks], inventory=inventory(directory, args.gpu))
            scratch = owners.enter_context(tempfile.TemporaryDirectory(dir=directory, prefix='orbitte-' + consumer + '-'))
            command = [str(binary), '--exercise', str(Path(scratch) / 'payload')]
            evidence['command'] = command
            yield command, evidence
            evidence['status'] = 'passed'
    except Exception as error:
        evidence['error'] = str(error)
        raise
    finally:
        (args.output / 'manifest.json').write_text(json.dumps(evidence, indent=2) + '\n')


def run(args):
    with installed_case(args, 'storage-cuda', 'cuda-dram-file', Path(__file__)) as (command, _):
        with (args.output / 'stdout').open('w') as stdout, (args.output / 'stderr').open('w') as stderr:
            result = subprocess.run(command, stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
                                    env={**os.environ, 'CUDA_VISIBLE_DEVICES': args.gpu}, timeout=180)
        expected = 'STORAGE_CUDA_OK staged_bytes=16458 chunks=6 pinned_bytes=16458 direct=true retained=0'
        if result.returncode or (args.output / 'stdout').read_text().strip() != expected:
            raise RuntimeError('installed GPU/file exercise failed; inspect stdout and stderr')


def options(description):
    parser = argparse.ArgumentParser(description=description)
    for name in ('artifact', 'directory', 'gpu-lock', 'nvme-lock', 'output'):
        parser.add_argument('--' + name, required=True, type=Path)
    parser.add_argument('--gpu', required=True, help='physical GPU UUID')
    return parser


if __name__ == '__main__':
    run(options(__doc__).parse_args())
