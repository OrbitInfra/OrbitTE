"""Deploy isolated native peers from an external, identity-pinned SSH profile.

Controllers collect evidence only. All transfers, byte checks and drain/resource
assertions run inside hardware_peer; this is not a performance benchmark.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import time
import uuid


def restart_plan(case):
    mode = case.get('mode')
    if mode in ('large-cq-busy', 'large-pd-busy'):
        role = case.get('fault_role')
        if role not in ('source', 'requester'):
            raise ValueError('native cleanup faults require an explicit source/requester fault_role')
        return role, 2
    if mode in ('live-reconnect', 'mr-cleanup-error', 'mw-cleanup-error'):
        role = case.get('fault_role')
        if (role not in ('source', 'requester')
                or (mode == 'mw-cleanup-error' and role != 'source')
                or (mode == 'live-reconnect' and role != 'requester')):
            raise ValueError('lifecycle cases require an explicit supported fault_role')
        return None
    if 'fault_role' in case:
        raise ValueError('fault_role is reserved for native cleanup modes')
    if mode in ('large-peer-exit', 'large-peer-restart'):
        return 'source', 3 if mode.endswith('restart') else 2
    if mode in ('large-requester-exit', 'large-requester-restart'):
        return 'requester', 3 if mode.endswith('restart') else 2
    return None


def native_peer_test(profile):
    runner = profile.get('runner')
    if runner == 'orbitte-multirail':
        return 'transport::rdma::workers::tests::hardware::multirail::multirail_peer'
    if runner == 'orbitte-connected':
        return 'transport::rdma::workers::tests::hardware::connected::connected_endpoint_peer'
    if runner == 'orbitte-shared-cq':
        return 'transport::rdma::workers::tests::hardware::shared_cq_peer'
    if runner == 'orbitte-window':
        return 'transport::rdma::workers::tests::hardware::source_window_peer'
    if runner == 'orbitkv-peer-host':
        return 'peer::orbitte::tests::rdma_host::registered_host_rdma_peer'
    if runner is None:
        return None
    raise ValueError('unsupported hardware runner')


def validate_peer_listing(test, listing):
    matches = [line.strip() for line in listing.splitlines()
               if line.strip().endswith((': test', ': benchmark'))]
    if matches != [test + ': test']:
        raise ValueError('native peer selector must resolve exactly one test: ' + repr(matches))


class Controller:
    def __init__(self, profile, binary, output):
        self.profile, self.binary, self.output = profile, binary, output
        self.sha256 = hashlib.sha256(binary.read_bytes()).hexdigest()
        self.remote = '/var/tmp/orbitte-runs/' + output.name + '-' + uuid.uuid4().hex[:12]
        self.commands = []
        self.started = []
        self.connections = []

    def command(self, command, *, allow_failure=False):
        result = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
        self.commands.append({'command': command, 'exit_code': result.returncode, 'stdout': result.stdout, 'stderr': result.stderr})
        (self.output / 'commands.json').write_text(json.dumps(self.commands, indent=2) + '\n')
        if result.returncode and not allow_failure:
            raise RuntimeError(f'command failed: {command!r}: {result.stderr}')
        return result.stdout

    def ssh(self, node, arguments):
        return self.command(['ssh', '-F', self.profile['ssh_config'], self.profile['nodes'][node]['alias'], shlex.join(arguments)])

    def copy(self, node, local, remote):
        # SFTP/scp reads only the explicitly named runner/config files. SSH keys
        # remain in the controller's existing configuration and never in bundles.
        self.command(['scp', '-F', self.profile['ssh_config'], str(local), f'{self.profile["nodes"][node]["alias"]}:{remote}'])

    def deploy(self, node):
        identity = self.ssh(node, ['python3', '-c', "import json,socket;from pathlib import Path;print(json.dumps([socket.gethostname(),Path('/proc/sys/kernel/random/boot_id').read_text().strip()]))"])
        expected = self.profile['nodes'][node]
        if json.loads(identity) != [expected['hostname'], expected['boot_id']]:
            raise RuntimeError('host/boot identity changed before deployment')
        self.ssh(node, ['mkdir', '-m', '700', '-p', self.remote])
        self.copy(node, self.binary, self.remote + '/hardware_peer')
        self.copy(node, Path(__file__).with_name('supervise.py'), self.remote + '/supervise.py')
        self.ssh(node, ['chmod', '700', self.remote + '/hardware_peer'])
        self.ssh(node, ['ldd', self.remote + '/hardware_peer'])

    def collect(self, node, root):
        names = ['running.json', 'stdout.log', 'stderr.log', 'result.json', 'supervisor.log', 'maps.txt', 'restart.json']
        names += ['maps-' + str(g) + '.txt' for g in range(3)]
        names += ['restart-' + kind + '-' + str(g) + '.txt'
                  for kind in ('state', 'request', 'peer-state', 'peer') for g in range(3)]
        script = "import json;from pathlib import Path;p=Path(" + repr(root) + ");print(json.dumps({n:(p/n).read_text() if (p/n).exists() else None for n in " + repr(names) + "}))"
        return json.loads(self.ssh(node, ['python3', '-c', script]))

    def start(self, node, root, kind, arguments, transport, environment=None):
        target = self.profile['nodes'][node]
        locks = []
        if transport == 'rdma':
            if self.profile.get('runner') == 'orbitte-multirail':
                locks.extend(self.rail_locks(node))
            else:
                locks.append(target['nic_lock'])
        environment = {**target.get('env', {}), **(environment or {})}
        if kind != 'cpu':
            locks.append(target['gpu_lock'])
            environment['CUDA_VISIBLE_DEVICES'] = target['gpu_uuid']
        config = {'hostname': target['hostname'], 'boot_id': target['boot_id'], 'binary_sha256': self.sha256, 'arguments': arguments, 'locks': locks, 'env': environment, 'timeout_seconds': 55}
        if 'ORBITTE_RESTART_GENERATIONS' in environment:
            config.update(restart_role=environment['ORBITTE_SHARED_ROLE'],
                          restart_kill_role=environment['ORBITTE_RESTART_KILL_ROLE'],
                          restart_generations=int(environment['ORBITTE_RESTART_GENERATIONS']))
        local = self.output / (Path(root).name + '.json')
        local.write_text(json.dumps(config, indent=2) + '\n')
        self.ssh(node, ['mkdir', '-m', '700', '-p', root])
        self.copy(node, local, root + '/config.json')
        self.started.append((node, root))
        command = ['ssh', '-F', self.profile['ssh_config'], target['alias'], shlex.join(['python3', self.remote + '/supervise.py', 'start', root + '/config.json'])]
        stdout = self.output / (Path(root).name + '-starter.stdout')
        stderr = self.output / (Path(root).name + '-starter.stderr')
        with stdout.open('wb') as output, stderr.open('wb') as errors:
            process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=output, stderr=errors)
        self.connections.append((process, command, stdout, stderr))

    def settle(self):
        pending = list(self.started)
        evidence = {}
        deadline = time.monotonic() + 65
        while pending and time.monotonic() < deadline:
            for node, root in pending[:]:
                try:
                    record = self.collect(node, root)
                    evidence[node + ':' + root] = record
                    if record['result.json'] is not None:
                        result = json.loads(record['result.json'])
                        if result.get('child_reaped') and result.get('resource_locks_released'):
                            pending.remove((node, root))
                except Exception as error:
                    evidence[node + ':' + root] = {'collection_error': str(error)}
            if pending:
                time.sleep(0.5)
        connections = []
        for process, command, stdout, stderr in self.connections:
            try:
                code = process.wait(timeout=max(0.1, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                code = None
            connections.append({'command': command, 'local_ssh_pid': process.pid, 'exit_code': code, 'stdout': str(stdout), 'stderr': str(stderr)})
        reaped = not pending and all(connection['exit_code'] == 0 for connection in connections)
        result = {'all_supervised_children_reaped': reaped, 'unconfirmed': pending, 'evidence': evidence, 'starter_connections': connections}
        (self.output / 'cleanup.json').write_text(json.dumps(result, indent=2) + '\n')
        return reaped

    def rail_locks(self, node):
        rails = self.profile.get('rails')
        if (not isinstance(rails, list) or len(rails) != 2
                or any(not isinstance(name, str) or not name
                       or any(not (char.isascii() and (char.isalnum() or char in '_.-'))
                              for char in name) for name in rails)
                or len(set(rails)) != 2):
            raise ValueError('multirail requires two distinct explicit RNIC names')
        locks = self.profile['nodes'][node].get('nic_locks')
        if (not isinstance(locks, dict) or set(locks) != set(rails)
                or any(not isinstance(path, str) or not path.startswith('/') for path in locks.values())
                or len(set(locks.values())) != 2):
            raise ValueError('each multirail RNIC requires its own absolute lock path')
        return [locks[name] for name in rails]

    def case(self, index, case):
        if self.profile.get('runner') == 'orbitte-multirail':
            return self.multirail_case(index, case)
        source, requester = case['source'], case['requester']
        source_root = self.remote + f'/{index:02}-source'
        requester_root = self.remote + f'/{index:02}-requester'
        consumer = self.profile.get('runner') == 'orbitkv-peer-host'
        window_probe = self.profile.get('runner') == 'orbitte-window'
        shared = self.profile.get('runner') in ['orbitte-shared-cq', 'orbitte-window', 'orbitte-connected']
        if self.profile.get('runner') not in [None, 'orbitkv-peer-host', 'orbitte-shared-cq', 'orbitte-window', 'orbitte-connected']:
            raise ValueError('unsupported hardware runner')
        if consumer and (case['transport'] != 'rdma' or case['source_kind'] != 'cpu' or case['requester_kind'] != 'cpu'):
            raise ValueError('OrbitKV registered-read component gate accepts host RDMA only')

        discovery = case.get('rdma_auto', False)
        if type(discovery) is not bool or (discovery and case['transport'] != 'rdma'):
            raise ValueError('rdma_auto requires a boolean RDMA profile')

        cq_events = case.get('rdma_events', False)
        if type(cq_events) is not bool or (cq_events and (case['transport'] != 'rdma' or consumer)):
            raise ValueError('rdma_events requires a boolean native RDMA profile')

        multi_context = case.get('rdma_multi_context', False)
        if type(multi_context) is not bool or (multi_context and (
                self.profile.get('runner') != 'orbitte-shared-cq' or case.get('duplex')
                or case['transport'] != 'rdma'
                or case['source_kind'] != 'cpu' or case['requester_kind'] != 'cpu')):
            raise ValueError('rdma_multi_context requires a boolean host shared-CQ profile')

        def command(node, role, kind, root, readiness=None):
            if shared:
                if case['transport'] != 'rdma' or kind != 'cpu':
                    raise ValueError('shared CQ gate accepts host RDMA only')
                capacity, window = case['rdma_max_cqes'], case['rdma_max_send_wr']
                fragmented = window_probe and case.get('window_probe') in ['fragment-read', 'fragment-write', 'partial-read', 'partial-write']
                if type(capacity) is not int or not 1 <= capacity <= 4 or type(window) is not int or not 1 <= window <= (3 if fragmented else 2):
                    raise ValueError('invalid shared CQ/SQ profile')
                if fragmented and (capacity != 3 or window != 3 or type(case['payload']) is not int or case['payload'] != 12288):
                    raise ValueError('fragmented fault gate requires three 4096-byte WRs')
                arguments = [native_peer_test(self.profile), '--exact', '--ignored', '--test-threads=1', '--nocapture']
                environment = {
                    'ORBITTE_SHARED_ROLE': role,
                    'ORBITTE_SHARED_BIND': self.profile['nodes'][node]['bind'],
                    'ORBITTE_SHARED_DEVICE': self.profile['rnic'],
                    'ORBITTE_SHARED_GID': str(self.profile['gid_index']),
                    'ORBITTE_SHARED_PAYLOAD': str(case['payload']),
                    'ORBITTE_SHARED_SEED': str(case['seed']),
                    'ORBITTE_SHARED_CQES': str(capacity),
                    'ORBITTE_SHARED_WINDOW': str(window),
                    'ORBITTE_SHARED_MAPS': root + '/maps.txt',
                }
                environment['ORBITTE_RDMA_EVENTS'] = str(int(cq_events))
                if multi_context:
                    environment['ORBITTE_RDMA_MULTI_CONTEXT'] = '1'
                if discovery:
                    environment['ORBITTE_RDMA_DISCOVERY'] = '1'
                if case.get('duplex'):
                    environment['ORBITTE_SHARED_DUPLEX'] = '1'
                    role_capacity = case.get(role + '_rdma_max_cqes', capacity)
                    if type(role_capacity) is not int or not 1 <= role_capacity <= 4:
                        raise ValueError('invalid duplex CQ capacity')
                    environment['ORBITTE_SHARED_CQES'] = str(role_capacity)
                if window_probe:
                    if case.get('window_probe') not in ['revoked', 'parent', 'remote', 'range', 'fragment-read', 'fragment-write', 'partial-read', 'partial-write']:
                        raise ValueError('invalid MW probe')
                    environment['ORBITTE_WINDOW_PROBE'] = case['window_probe']
                if readiness is not None:
                    if len(readiness) != 1:
                        raise ValueError('invalid shared CQ readiness')
                    environment['ORBITTE_SHARED_PEER'] = readiness[0]
                return arguments, environment
            if consumer:
                arguments = [native_peer_test(self.profile), '--exact', '--ignored', '--test-threads=1', '--nocapture']
                environment = {
                    'ORBITKV_ORBITTE_ROLE': role,
                    'ORBITKV_ORBITTE_BIND': self.profile['nodes'][node]['bind'],
                    'ORBITKV_ORBITTE_RDMA_DEVICE': self.profile['rnic'],
                    'ORBITKV_ORBITTE_RDMA_PORT': str(self.profile.get('rdma_port', 1)),
                    'ORBITKV_ORBITTE_RDMA_GID_INDEX': str(self.profile['gid_index']),
                    'ORBITKV_ORBITTE_PAYLOAD': str(case['payload']),
                    'ORBITKV_ORBITTE_SEED': str(case['seed']),
                    'ORBITKV_ORBITTE_MAPS': root + '/maps.txt',
                }
                if readiness is not None:
                    if len(readiness) != 4 or not readiness[0].startswith('orbitte+rdma://'):
                        raise ValueError('invalid OrbitKV RDMA readiness')
                    environment['ORBITKV_ORBITTE_REMOTE'] = json.dumps(readiness)
                return arguments, environment
            arguments = [role, self.profile['nodes'][node]['bind'], self.profile['rnic'], str(self.profile['gid_index']), kind, str(case['payload']), str(case['seed']), case['transport']]
            host_memory = case.get(role + '_host_memory', case.get('host_memory', 'direct'))
            if host_memory not in ['direct', 'staged']:
                raise ValueError('RDMA host memory must be direct or staged')
            environment = {
                'ORBITTE_HARDWARE_MAPS': root + '/maps.txt',
                'ORBITTE_RDMA_HOST_MEMORY': host_memory,
                'ORBITTE_EXPECT_QP_REUSE': str(int(case['transport'] == 'rdma' and all(
                    case.get(r + '_host_memory', case.get('host_memory', 'direct')) == 'direct'
                    and (case[r + '_kind'] == 'cpu' or case[r + '_kind'].startswith('pinned:'))
                    for r in ['source', 'requester']))),
            }
            for key, variable, limit in [('rdma_block_size', 'ORBITTE_RDMA_BLOCK_SIZE', 64 << 20), ('rdma_max_send_wr', 'ORBITTE_RDMA_MAX_SEND_WR', 64), ('rdma_max_cqes', 'ORBITTE_RDMA_MAX_CQES', 4096), ('rdma_max_endpoints', 'ORBITTE_RDMA_MAX_ENDPOINTS', 4096)]:
                value = case.get(key)
                if value is not None:
                    if type(value) is not int or not 1 <= value <= limit:
                        raise ValueError('invalid ' + key)
                    environment[variable] = str(value)
            environment['ORBITTE_RDMA_EVENTS'] = str(int(cq_events))
            if discovery:
                environment['ORBITTE_RDMA_DISCOVERY'] = '1'
            return arguments + (readiness[:3] if readiness else []), environment

        return self.run_case(index, case, command)

    def multirail_case(self, index, case):
        source, requester = case['source'], case['requester']
        for node in [source, requester]:
            self.rail_locks(node)
        if (case.get('transport') != 'rdma'
                or case.get('source_kind') != 'cpu' or case.get('requester_kind') != 'cpu'
                or type(case.get('rdma_events')) is not bool
                or case.get('mode') not in ['healthy', 'cq-error', 'partial-post', 'paused-close', 'mr-rollback',
                                           'concurrent-quota', 'concurrent-endpoints', 'concurrent-close',
                                           'concurrent-cq-error', 'rq-pressure', 'rq-close', 'rq-cq-error',
                                           'large-quota', 'large-close', 'large-cq-error',
                                           'folded-quota', 'folded-close', 'folded-cq-error', 'quarter-quota',
                         'large-qp-error', 'folded-qp-error', 'large-lost-send-cqe', 'folded-lost-send-cqe', 'large-lost-recv-cqe', 'folded-lost-recv-cqe', 'large-peer-exit', 'large-peer-restart', 'large-requester-exit', 'large-requester-restart', 'large-cq-busy', 'large-pd-busy', 'live-reconnect', 'mr-cleanup-error', 'mw-cleanup-error']
                or case.get('operation') not in ['read', 'write']
                or type(case.get('fault_rail')) is not int or case['fault_rail'] not in [0, 1]
                or type(case.get('mask')) is not int or case['mask'] not in [1, 2, 3]):
            raise ValueError('invalid explicit multirail profile')
        if case['mode'] != 'healthy' and case['mask'] != 3:
            raise ValueError('multirail fault controls require both RNICs')
        restart = restart_plan(case)

        def command(node, role, kind, root, readiness=None):
            arguments = [native_peer_test(self.profile), '--exact', '--ignored', '--test-threads=1', '--nocapture']
            environment = {
                'ORBITTE_SHARED_ROLE': role,
                'ORBITTE_SHARED_BIND': self.profile['nodes'][node]['bind'],
                'ORBITTE_SHARED_RAILS': ','.join(self.profile['rails']),
                'ORBITTE_SHARED_GID': str(self.profile['gid_index']),
                'ORBITTE_SHARED_MAPS': root + '/maps.txt',
                'ORBITTE_RDMA_EVENTS': str(int(case['rdma_events'])),
                'ORBITTE_RAIL_MODE': case['mode'],
                'ORBITTE_RAIL_OPERATION': case['operation'],
                'ORBITTE_RAIL_FAULT': str(case['fault_rail']),
                'ORBITTE_RAIL_MASK': str(case['mask']),
                'ORBITTE_RAIL_FAULT_ROLE': str(case.get('fault_role', 'source')),
            }
            if restart is not None:
                environment['ORBITTE_RESTART_KILL_ROLE'] = restart[0]
                environment['ORBITTE_RESTART_GENERATIONS'] = str(restart[1])
            if readiness is not None:
                if len(readiness) != 1:
                    raise ValueError('invalid multirail readiness')
                environment['ORBITTE_SHARED_PEER'] = readiness[0]
            return arguments, environment

        return self.run_case(index, case, command)

    def run_case(self, index, case, command):
        source, requester = case['source'], case['requester']
        source_root = self.remote + f'/{index:02}-source'
        requester_root = self.remote + f'/{index:02}-requester'
        arguments, environment = command(source, 'source', case['source_kind'], source_root)
        self.start(source, source_root, case['source_kind'], arguments, case['transport'], environment)
        started = time.monotonic()
        readiness = None
        while time.monotonic() - started < 25:
            evidence = self.collect(source, source_root)
            if evidence['result.json'] is not None:
                raise RuntimeError('source exited before readiness: ' + json.dumps(evidence))
            for line in (evidence['stdout.log'] or '').splitlines():
                if line.startswith('ORBITTE_READY '):
                    readiness = line.split()[1:]
            if readiness:
                break
            time.sleep(0.2)
        if readiness is None:
            raise RuntimeError('source readiness timeout; supervisor will collect and reap its own child')
        arguments, environment = command(requester, 'requester', case['requester_kind'], requester_root, readiness)
        self.start(requester, requester_root, case['requester_kind'], arguments, case['transport'], environment)
        forwarded = set()
        restart = restart_plan(case)
        while time.monotonic() - started < 65:
            evidence = {role: self.collect(node, root) for role, node, root in [('source', source, source_root), ('requester', requester, requester_root)]}
            if restart is not None:
                running = json.loads(evidence['source']['running.json'] or '{}')
                ready = [line.split()[1] for line in (evidence['source']['stdout.log'] or '').splitlines()
                         if line.startswith('ORBITTE_READY ')]
                generation = running.get('generation')
                if (type(generation) is int and 0 < generation < restart[1]
                        and generation not in forwarded and len(ready) > generation):
                    destination = requester_root + '/restart-peer-' + str(generation) + '.txt'
                    script = "from pathlib import Path;p=Path(" + repr(destination) + ");t=p.with_suffix('.tmp');t.write_text(" + repr(ready[generation]) + ");t.replace(p)"
                    self.ssh(requester, ['python3', '-c', script])
                    forwarded.add(generation)
            if all(record['result.json'] is not None for record in evidence.values()):
                break
            time.sleep(0.3)
        passed = all(record['result.json'] is not None and json.loads(record['result.json'])['status'] == 'passed' and record['maps.txt'] is not None for record in evidence.values())
        # Preserve every successful/failed run verbatim, including supervisors.
        (self.output / f'case-{index:02}.json').write_text(json.dumps({'case': case, 'status': 'passed' if passed else 'failed', 'remote_root': self.remote, 'restart_ready_forwarded': sorted(forwarded), 'evidence': evidence}, indent=2) + '\n')
        if not passed:
            raise RuntimeError(f'case {index} failed; retained evidence in {self.output}')
        print(json.dumps({'case': index, 'status': 'passed', **case}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', required=True, type=Path)
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    profile = json.loads(args.profile.read_text())
    controller = Controller(profile, args.binary.resolve(), args.output.resolve())
    manifest = {'profile': profile, 'binary_sha256': controller.sha256, 'remote_root': controller.remote, 'started_unix': time.time(), 'status': 'running', 'performance_measurement': False}
    try:
        native_test = native_peer_test(profile)
        if native_test is not None:
            listing = controller.command([str(args.binary.resolve()), native_test, '--exact', '--list'])
            validate_peer_listing(native_test, listing)
            manifest['native_test_selection'] = {'test': native_test, 'listing': listing, 'matches': 1}
        for node in sorted({case[role] for case in profile['cases'] for role in ['source', 'requester']}):
            controller.deploy(node)
        for index, case in enumerate(profile['cases']):
            controller.case(index, case)
        manifest['status'] = 'passed'
    except Exception as error:
        manifest.update(status='failed', error=str(error))
        raise
    finally:
        manifest['all_supervised_children_reaped'] = controller.settle()
        if not manifest['all_supervised_children_reaped']:
            manifest['status'] = 'failed'
        manifest['finished_unix'] = time.time()
        (args.output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    if manifest['status'] != 'passed':
        raise SystemExit('hardware evidence or cleanup did not pass')


if __name__ == '__main__':
    main()
