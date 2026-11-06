"""Run the packaged Python CUDA gate on one locked GPU; never uses the source package."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('artifact', 'gpu-lock', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--gpu', required=True, help='assigned physical GPU UUID')
    parser.add_argument('--python', default=sys.executable, help='wheel-compatible Python with CUDA PyTorch installed')
    args = parser.parse_args()
    artifact, output = args.artifact.resolve(), args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    manifest = json.loads((artifact / 'manifest.json').read_text())
    wheel = Path(manifest['wheel'])
    if (manifest['status'] != 'built' or manifest['consumer'] != 'python-cuda' or manifest['dirty']
            or digest(wheel) != manifest['wheel_sha256']):
        raise ValueError('packaged wheel identity mismatch')
    for name, expected in manifest['fixture_files_sha256'].items():
        if digest(artifact / name) != expected:
            raise ValueError('packaged gate identity mismatch')
    env = {**os.environ, 'CUDA_VISIBLE_DEVICES': args.gpu}
    if env.get('ORBITE_PYTHON_CUDA_SHIM'):
        raise ValueError('the hardware gate cannot use the CPU driver shim')
    evidence = dict(status='running', artifact=manifest['source_commit'], wheel_sha256=digest(wheel), gpu=args.gpu)

    def run(name, command):
        with (output / (name + '.stdout')).open('x') as out, (output / (name + '.stderr')).open('x') as err:
            subprocess.run(command, cwd=output, env=env, stdout=out, stderr=err, check=True, timeout=300)
        return (output / (name + '.stdout')).read_text()

    try:
        with args.gpu_lock.open('r+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            inventory = run('inventory', ['nvidia-smi', '-i', args.gpu,
                            '--query-gpu=uuid,name,driver_version,pci.bus_id', '--format=csv,noheader'])
            if inventory.split(',')[0].strip() != args.gpu:
                raise ValueError('GPU inventory mismatch')
            venv = output / 'venv'
            run('venv', [args.python, '-m', 'venv', '--system-site-packages', str(venv)])
            python = str(venv / 'bin/python')
            run('install', [python, '-m', 'pip', 'install', '--no-index', '--no-deps', '--force-reinstall', str(wheel)])
            result = json.loads(run('gate', [python, '-I', str(artifact / 'python_cuda.py')]))
            if result['status'] != 'passed' or len(result['cases']) != 8:
                raise ValueError('incomplete CUDA gate')
            evidence.update(status='passed', result=result)
    except Exception as error:
        evidence.update(status='failed', error=str(error))
        raise
    finally:
        (output / 'report.json').write_text(json.dumps(evidence, indent=2) + '\n')


if __name__ == '__main__':
    main()
