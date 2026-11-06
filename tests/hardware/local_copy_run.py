"""Compare packaged OrbiTE and frozen OrbitKV on one exclusively allocated CUDA GPU."""
import argparse
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
import subprocess

ORBITKV = '93541a213184cd7a45a9ee92cf0d9ed41870518a'
CASES = ((1, 4096), (16, 4096), (257, 31), (1024, 1024))
SAMPLES = 30


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def checked_rows(text, gpu, blocks, width):
    rows = [json.loads(line) for line in text.splitlines()]
    if len(rows) != 2 or {r['direction'] for r in rows} != {'h2d', 'd2h'}:
        raise ValueError('expected one complete result per direction')
    medians = {}
    for row in rows:
        for key, expected in {'case': 'custom-fragments', 'cuda_device_uuid': gpu,
                              'descriptors': blocks, 'sizes': [width] * blocks,
                              'samples': SAMPLES, 'warmup': 5, 'bytes': blocks * width,
                              'byte_source_guard_oracle': 'passed'}.items():
            if row[key] != expected:
                raise ValueError(f'wrong {key} for {row["direction"]}')
        for backend in ('dma', 'kernel'):
            times = row[f'{backend}_submit_to_drain_ms']
            if len(times) != SAMPLES or not all(math.isfinite(t) and t > 0 for t in times):
                raise ValueError('incomplete or invalid submit-to-drain samples')
            medians[f'{row["direction"]}_{backend}_ms'] = statistics.median(times)
    return medians


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('artifact', 'orbitkv', 'gpu-lock', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--gpu', required=True, help='assigned physical GPU UUID')
    args = parser.parse_args()
    output, artifact, checkout = (p.resolve() for p in (args.output, args.artifact, args.orbitkv))
    if output.is_relative_to(checkout) or output.is_relative_to(artifact):
        raise ValueError('use a fresh external evidence directory')
    output.mkdir(parents=True, exist_ok=False)
    manifest = json.loads((artifact / 'manifest.json').read_text())
    binary = Path(manifest['binary'])
    driver = Path(__file__).resolve()
    if (manifest['status'] != 'passed' or manifest['dirty'] or manifest['consumer'] != 'local-copy'
            or manifest['protocol'] != 'orbite-installed-local-copy'
            or digest(binary) != manifest['binary_sha256']
            or digest(Path(manifest['archive'])) != manifest['archive_sha256']
            or digest(driver) != manifest['fixture_files_sha256']['tests/hardware/local_copy_run.py']):
        raise ValueError('packaged public consumer or driver identity mismatch')
    env = {**os.environ, 'CUDA_VISIBLE_DEVICES': args.gpu}
    evidence = {'status': 'running', 'gpu': args.gpu, 'orbite': manifest['source_commit'],
                'orbitkv': ORBITKV, 'artifact_sha256': manifest['archive_sha256'],
                'scope': 'CPU wall time from submission entry through physical completion on the default stream',
                'cases': []}

    def run(name, command, work=output, timeout=180):
        with (output / f'{name}.stdout').open('x') as stdout, (output / f'{name}.stderr').open('x') as stderr:
            subprocess.run(command, cwd=work, env=env, stdout=stdout, stderr=stderr, timeout=timeout, check=True)
        return (output / f'{name}.stdout').read_text()

    try:
        if run('protocol', [binary, '--protocol']).strip() != manifest['protocol']:
            raise ValueError('installed protocol mismatch')
        # Build in a private copy. No command modifies the supplied OrbitKV checkout.
        source = output / 'orbitkv-src'
        run('clone', ['git', 'clone', '--no-checkout', '--no-hardlinks', checkout, source])
        run('checkout', ['git', 'checkout', '--detach', ORBITKV], source)
        run('mooncake', ['git', 'submodule', 'update', '--init', '--recursive', 'third-party/mooncake'], source, 1800)
        env['CARGO_TARGET_DIR'] = str(output / 'orbitkv-target')
        build = run('build-oracle', ['cargo', 'bench', '--locked', '--no-run', '--message-format=json',
                                     '-p', 'orbitkv-core', '--bench', 'native_kernel_copy',
                                     '--no-default-features', '--features', 'cuda-13,mooncake'], source, 3600)
        executables = [m['executable'] for m in map(json.loads, build.splitlines()) if
                       m.get('reason') == 'compiler-artifact' and m['target']['name'] == 'native_kernel_copy'
                       and m.get('executable')]
        if len(executables) != 1:
            raise ValueError('expected one frozen native_kernel_copy executable')
        oracle = Path(executables[0])
        evidence['orbitkv_binary_sha256'] = digest(oracle)
        # The operator supplies the physical GPU's shared reservation lock.
        with args.gpu_lock.open('r+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            inventory = run('inventory', ['nvidia-smi', '-i', args.gpu, '--query-gpu=uuid,name,driver_version,pci.bus_id',
                                          '--format=csv,noheader'])
            if inventory.split(',')[0].strip() != args.gpu:
                raise ValueError('GPU inventory mismatch')
            for index, (blocks, width) in enumerate(CASES):
                baseline = output / f'orbitkv-{blocks}.jsonl'
                pair = {}
                for name in (('orbitkv', 'orbite') if index % 2 == 0 else ('orbite', 'orbitkv')):
                    if name == 'orbitkv':
                        run(f'oracle-{blocks}', [oracle, '--output', baseline, '--samples', str(SAMPLES),
                                                 '--blocks', str(blocks), '--fragment-bytes', str(width)])
                        data = baseline.read_text()
                    else:
                        data = run(f'orbite-{blocks}', [binary, str(blocks), str(width), str(SAMPLES)])
                    pair[name] = checked_rows(data, args.gpu, blocks, width)
                evidence['cases'].append({'blocks': blocks, 'fragment_bytes': width, 'medians': pair})
        evidence['status'] = 'passed'
    finally:
        (output / 'report.json').write_text(json.dumps(evidence | {'status': 'passed' if evidence['status'] == 'passed' else 'failed'},
                                                       indent=2) + '\n')


if __name__ == '__main__':
    main()
