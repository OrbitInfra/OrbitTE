"""Run one explicitly configured native peer under owned resource locks."""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time


def write(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def supervise(config_path):
    config = json.loads(config_path.read_text())
    root = config_path.parent
    result = {'hostname': socket.gethostname(), 'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(), 'started_unix': time.time(), 'supervisor_pid': os.getpid()}
    locks = []
    try:
        if result['hostname'] != config['hostname'] or result['boot_id'] != config['boot_id']:
            raise RuntimeError('host/boot identity changed')
        binary = root.parent / 'hardware_peer'
        if hashlib.sha256(binary.read_bytes()).hexdigest() != config['binary_sha256']:
            raise RuntimeError('binary hash mismatch')
        for path in sorted(config['locks']):
            lock = open(path, 'a+')
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            locks.append(lock)
        environment = dict(os.environ)
        environment.update(config.get('env', {}))
        # Protect direct peer connections from process-wide HTTP proxy settings.
        for name in ['http_proxy', 'https_proxy', 'all_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY']:
            environment.pop(name, None)
        result['command'] = [str(binary), *config['arguments']]
        with (root / 'stdout.log').open('wb') as stdout, (root / 'stderr.log').open('wb') as stderr:
            process = subprocess.Popen(result['command'], stdout=stdout, stderr=stderr, env=environment)
            result['native_pid'] = process.pid
            write(root / 'running.json', result)
            try:
                result['exit_code'] = process.wait(timeout=config['timeout_seconds'])
            except subprocess.TimeoutExpired:
                result['timeout'] = True
                # Only this direct child belongs to this test. Never kill job
                # resource holders, arbitrary groups or a discovered process.
                process.terminate()
                try:
                    result['exit_code'] = process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    result['exit_code'] = process.wait(timeout=5)
        result['status'] = 'passed' if result['exit_code'] == 0 else 'failed'
    except Exception as error:
        result.update(status='failed', error=str(error))
    finally:
        result['finished_unix'] = time.time()
        for lock in locks:
            lock.close()
        result['resource_locks_released'] = True
        write(root / 'result.json', result)


if __name__ == '__main__':
    action, config = sys.argv[1:]
    config = Path(config).resolve()
    if action == 'start':
        with (config.parent / 'supervisor.log').open('ab') as log:
            process = subprocess.Popen([sys.executable, __file__, 'supervise', str(config)], stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
        print(json.dumps({'supervisor_pid': process.pid}))
    elif action == 'supervise':
        supervise(config)
    else:
        raise SystemExit('action must be start or supervise')
