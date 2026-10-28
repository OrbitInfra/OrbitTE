"""Run one explicitly configured native peer under owned resource locks."""
import fcntl
import hashlib
import json
import os
import shutil
from pathlib import Path
import socket
import subprocess
import sys
import time


def fault_scope(environment):
    mode = environment.get('ORBITTE_HOST_MODE')
    role = environment.get('ORBITTE_SHARED_ROLE')
    if environment.get('ORBITTE_STAGING_OBSERVE') == '1':
        return (mode is None and role in ('source', 'requester')
                and environment.get('ORBITTE_STAGING_CHUNK_COUNT') == '1'
                and environment.get('ORBITTE_GDR_OBSERVE') is None
                and environment.get('ORBITTE_IPC_HOLD') is None)
    if environment.get('ORBITTE_GDR_OBSERVE') == '1':
        return (mode is None and role in ('source', 'requester')
                and environment.get('ORBITTE_RDMA_CUDA_MEMORY') == 'direct'
                and environment.get('MLX5_SCATTER_TO_CQE') == '0')
    if mode == 'installed-memory-cleanup':
        owner = environment.get('ORBITTE_CLEANUP_KIND')
        return (role in ('source', 'requester') and role == environment.get('ORBITTE_CLEANUP_ROLE')
                and owner in ('mr', 'mw') and (owner != 'mw' or role == 'source'))
    if mode == 'installed-cleanup-failure':
        return (role == 'requester' and environment.get('ORBITTE_CLEANUP_KIND', 'qp') in ('qp', 'cq', 'channel', 'pd')
                and (environment.get('ORBITTE_CLEANUP_KIND') != 'channel' or environment.get('ORBITTE_RDMA_EVENTS') == '1'))
    return (role == 'requester'
            and mode in ('installed-lost-completion', 'installed-inflight-dma', 'installed-rail-accounting'))


def write(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def supervise(config_path):
    config = json.loads(config_path.read_text())
    root = config_path.parent
    result = {'hostname': socket.gethostname(), 'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(), 'started_unix': time.time(), 'supervisor_pid': os.getpid()}
    locks = []
    process = None
    inventory_command = None
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
        if environment.get('ORBITTE_HOST_DISCOVERY') == 'auto':
            inventory = root.parent / 'discovery_inventory.py'
            expected = config.get('inventory_tool_sha256')
            if (not expected or hashlib.sha256(inventory.read_bytes()).hexdigest() != expected
                    or environment.get('ORBITTE_HOST_MODE') != 'installed-rail-accounting'):
                raise ValueError('discovery inventory scope or immutable hash mismatch')
            inventory_command = [sys.executable, str(inventory), *environment['ORBITTE_SHARED_RAILS'].split(',')]
            observed = json.loads(subprocess.check_output(inventory_command, text=True, timeout=10))
            write(root / 'inventory-before.json', observed)
            result['inventory_tool_sha256'] = expected
        restart = config.get('restart_role')
        if restart is not None and restart not in ('source', 'requester'):
            raise ValueError('restart_role must name the configured native peer')
        kill_role = config['restart_kill_role'] if restart else None
        generations = config['restart_generations'] if restart else 1
        if restart and (kill_role not in ('source', 'requester')
                        or type(generations) is not int or generations not in (2, 3)):
            raise ValueError('restart requires an explicit kill role and two or three generations')
        live_peer = config.get('restart_live_peer', False)
        if type(live_peer) is not bool or (live_peer and restart is None):
            raise ValueError('live peer policy requires a restart plan')
        process_generations = 1 if live_peer and restart != kill_role else generations
        result['command'] = [str(binary), *config['arguments']]
        phases = []
        for generation in range(process_generations):
            if restart:
                environment['ORBITTE_RESTART_GENERATION'] = str(generation)
                environment['ORBITTE_RESTART_ROOT'] = str(root)
            fault_hash = config.get('fault_library_sha256')
            if fault_hash:
                library = root.parent / 'liborbitte_cq_fault.so'
                if (hashlib.sha256(library.read_bytes()).hexdigest() != fault_hash
                        or environment.get('LD_PRELOAD') != str(library)
                        or not fault_scope(environment)):
                    raise RuntimeError('fault library scope or immutable hash mismatch')
                environment['ORBITTE_RESTART_GENERATION'] = str(generation)
                environment['ORBITTE_RESTART_ROOT'] = str(root)
                result['fault_library_sha256'] = fault_hash
            elif environment.get('LD_PRELOAD'):
                raise RuntimeError('unexpected unpinned preload library')
            mode = 'ab' if restart else 'wb'
            with (root / 'stdout.log').open(mode) as stdout, (root / 'stderr.log').open(mode) as stderr:
                process = subprocess.Popen(result['command'], stdout=stdout, stderr=stderr, env=environment)
                result['native_pid'] = process.pid
                if restart:
                    result['generation'] = generation
                write(root / 'running.json', result)
                intentional = False
                try:
                    if restart == kill_role and generation + 1 < generations:
                        deadline = time.monotonic() + config['timeout_seconds']
                        marker = root / ('restart-request-' + str(generation) + '.txt')
                        while process.poll() is None:
                            if marker.exists():
                                fields = marker.read_text().split()
                                if len(fields) != 7 or int(fields[0]) != process.pid or int(fields[6]) != generation:
                                    raise RuntimeError('restart marker does not identify this direct child')
                                # The Popen handle owns this child. Keep both leases
                                # through waitpid and the complete second process.
                                process.kill()
                                intentional = True
                                break
                            if time.monotonic() >= deadline:
                                raise subprocess.TimeoutExpired(result['command'], config['timeout_seconds'])
                            time.sleep(0.01)
                    result['exit_code'] = process.wait(timeout=config['timeout_seconds'])
                except subprocess.TimeoutExpired:
                    result['timeout'] = True
                    process.terminate()
                    try:
                        result['exit_code'] = process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        result['exit_code'] = process.wait(timeout=5)
            if restart:
                phases.append(dict(generation=generation, native_pid=process.pid,
                                   exit_code=result['exit_code'], child_reaped=True,
                                   intentional_sigkill=intentional))
                if (root / 'maps.txt').exists():
                    shutil.copy2(root / 'maps.txt', root / ('maps-' + str(generation) + '.txt'))
                write(root / 'restart.json', dict(phases=phases, locks_held=True, kill_role=kill_role, generations=generations, live_peer=live_peer))
                result['phases'] = phases
            expected = -9 if restart == kill_role and generation + 1 < generations else 0
            passed = result['exit_code'] == expected and (expected != -9 or intentional) and not result.get('timeout')
            result['status'] = 'passed' if passed else 'failed'
            if not passed:
                break
    except Exception as error:
        result.update(status='failed', error=str(error))
    finally:
        if process is not None and process.poll() is None:
            # Even SIGKILL cannot prove that an uninterruptible native child
            # exited. Publish failure, then retain the resource leases until
            # waitpid proves termination. Never make a busy GPU/NIC reusable.
            result.update(status='failed', child_reaped=False, resource_locks_released=False)
            write(root / 'result.json', result)
            while process.poll() is None:
                time.sleep(1)
            result['exit_code'] = process.wait()
        if inventory_command is not None:
            try:
                observed = json.loads(subprocess.check_output(inventory_command, text=True, timeout=10))
                write(root / 'inventory-after.json', observed)
            except Exception as error:
                result.update(status='failed', inventory_error=str(error))
        result['child_reaped'] = True
        result['finished_unix'] = time.time()
        for lock in locks:
            lock.close()
        result['resource_locks_released'] = True
        write(root / 'result.json', result)


def start(config):
    with (config.parent / 'supervisor.log').open('ab') as log:
        process = subprocess.Popen([sys.executable, __file__, 'supervise', str(config)], stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
    print(json.dumps({'supervisor_pid': process.pid}), flush=True)
    # Keep an explicit waiter: a container PID 1 need not reap orphaned daemons.
    # The supervised child still has its own session if SSH disconnects.
    return process.wait()


if __name__ == '__main__':
    action, config = sys.argv[1:]
    config = Path(config).resolve()
    if action == 'start':
        raise SystemExit(start(config))
    elif action == 'supervise':
        supervise(config)
    else:
        raise SystemExit('action must be start or supervise')
