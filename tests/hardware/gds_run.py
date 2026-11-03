"""Check native GDS bytes and nvidia-fs activity on an isolated, locked NVMe host."""
import json
import os
from pathlib import Path
import re
import subprocess

from storage_cuda_run import installed_case, options


def counters(text):
    result = {}
    for direction, mib in [('Reads', 'readMiB'), ('Writes', 'writeMiB')]:
        match = re.search(rf'^\s*{direction}\s*:\s*(n=.+)$', text, re.MULTILINE)
        if not match:
            raise ValueError('missing nvidia-fs ' + direction + ' I/O counters')
        fields = dict(re.findall(r'(\w+)=(\d+)', match[1]))
        result[direction] = {key: int(fields[key]) for key in ('ok', 'err', mib)}
    return result


def route_deltas(before, after):
    first, last = counters(before), counters(after)
    deltas = {direction: {k: value - first[direction][k] for k, value in row.items()}
              for direction, row in last.items()}
    for direction, mib in [('Reads', 'readMiB'), ('Writes', 'writeMiB')]:
        if deltas[direction][mib] != 2 or deltas[direction]['ok'] < 1 or deltas[direction]['err'] != 0:
            raise RuntimeError('native nvidia-fs byte evidence does not match the exclusive exercise')
    return deltas


def run(args):
    with installed_case(args, 'gds', 'native-gds-nvme', Path(__file__)) as (command, evidence):
        stats = Path('/proc/driver/nvidia-fs/stats')
        config = args.output.resolve() / 'cufile.json'
        config.write_text(json.dumps({'properties': {'allow_compat_mode': False, 'per_buffer_cache_size_kb': 4096}}))
        env = {**os.environ, 'CUDA_VISIBLE_DEVICES': args.gpu, 'CUFILE_ENV_PATH_JSON': str(config)}
        env.pop('CUFILE_FORCE_COMPAT_MODE', None)
        before = stats.read_text()
        (args.output / 'nvidia-fs.before').write_text(before)
        with (args.output / 'stdout').open('w') as stdout, (args.output / 'stderr').open('w') as stderr:
            try:
                result = subprocess.run(command, stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
                                        env=env, timeout=180)
            finally:
                after = stats.read_text()
                (args.output / 'nvidia-fs.after').write_text(after)
        evidence['config'] = str(config)
        evidence['nvidia_fs_deltas'] = route_deltas(before, after)
        if result.returncode or (args.output / 'stdout').read_text().strip() != 'GDS_NATIVE_OK read_bytes=2097152 write_bytes=2097152 batches=2':
            raise RuntimeError('installed GDS exercise failed')



if __name__ == '__main__':
    run(options(__doc__).parse_args())
