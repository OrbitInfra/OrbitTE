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
        script = "import json;from pathlib import Path;p=Path(" + repr(root) + ");print(json.dumps({n:(p/n).read_text() if (p/n).exists() else None for n in ['running.json','stdout.log','stderr.log','result.json','supervisor.log']}))"
        return json.loads(self.ssh(node, ['python3', '-c', script]))

    def start(self, node, root, kind, arguments):
        target = self.profile['nodes'][node]
        locks = []
        if arguments[7] == 'rdma':
            locks.append(target['nic_lock'])
        environment = {}
        if kind != 'cpu':
            locks.append(target['gpu_lock'])
            environment['CUDA_VISIBLE_DEVICES'] = target['gpu_uuid']
        config = {'hostname': target['hostname'], 'boot_id': target['boot_id'], 'binary_sha256': self.sha256, 'arguments': arguments, 'locks': locks, 'env': environment, 'timeout_seconds': 55}
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

    def case(self, index, case):
        source, requester = case['source'], case['requester']
        source_root = self.remote + f'/{index:02}-source'
        requester_root = self.remote + f'/{index:02}-requester'
        prefix = lambda node, role, kind: [role, self.profile['nodes'][node]['bind'], self.profile['rnic'], str(self.profile['gid_index']), kind, str(case['payload']), str(case['seed']), case['transport']]
        self.start(source, source_root, case['source_kind'], prefix(source, 'source', case['source_kind']))
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
        arguments = prefix(requester, 'requester', case['requester_kind']) + readiness[:3]
        self.start(requester, requester_root, case['requester_kind'], arguments)
        while time.monotonic() - started < 65:
            evidence = {role: self.collect(node, root) for role, node, root in [('source', source, source_root), ('requester', requester, requester_root)]}
            if all(record['result.json'] is not None for record in evidence.values()):
                break
            time.sleep(0.3)
        passed = all(record['result.json'] is not None and json.loads(record['result.json'])['status'] == 'passed' for record in evidence.values())
        # Preserve every successful/failed run verbatim, including supervisors.
        (self.output / f'case-{index:02}.json').write_text(json.dumps({'case': case, 'status': 'passed' if passed else 'failed', 'remote_root': self.remote, 'evidence': evidence}, indent=2) + '\n')
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
