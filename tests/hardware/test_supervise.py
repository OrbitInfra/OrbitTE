"""CPU-only control: an unreaped native child must keep actual resource locks."""
import fcntl
import hashlib
import importlib.util
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('supervise', Path(__file__).with_name('supervise.py'))
supervise = importlib.util.module_from_spec(spec)
spec.loader.exec_module(supervise)


class UnreapedChild:
    pid = 12345

    def __init__(self):
        self.polls = 0

    def wait(self, timeout=None):
        if timeout is not None:
            raise subprocess.TimeoutExpired('owned-test-child', timeout)
        return -9

    def poll(self):
        self.polls += 1
        return None if self.polls <= 2 else -9

    def terminate(self):
        pass

    def kill(self):
        pass


class SupervisionOwnership(unittest.TestCase):
    def test_starter_reaps_supervisor_and_native_peer_before_returning(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            case = root / 'case'
            case.mkdir()
            binary = root / 'hardware_peer'
            binary.write_bytes(b'#!/bin/sh\nexit 0\n')
            binary.chmod(0o700)
            lock = root / 'resource.lock'
            lock.touch()
            config = case / 'config.json'
            config.write_text(json.dumps({'hostname': socket.gethostname(), 'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(), 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(), 'locks': [str(lock)], 'arguments': [], 'timeout_seconds': 2}))
            process = subprocess.run([sys.executable, '-B', str(Path(supervise.__file__)), 'start', str(config)], capture_output=True, text=True, timeout=10)
            self.assertEqual(process.returncode, 0, process.stderr)
            supervisor_pid = json.loads(process.stdout)['supervisor_pid']
            result = json.loads((case / 'result.json').read_text())
            self.assertEqual(result['status'], 'passed')
            self.assertTrue(result['child_reaped'])
            self.assertFalse(Path('/proc', str(supervisor_pid)).exists())
            self.assertFalse(Path('/proc', str(result['native_pid'])).exists())

    def test_kill_timeout_retains_lock_until_child_is_reaped(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            case = root / 'case'
            case.mkdir()
            binary = root / 'hardware_peer'
            binary.write_bytes(b'fixture-not-executed')
            locks = [root / 'nic1.lock', root / 'nic2.lock']
            for lock in locks:
                lock.touch()
            config = case / 'config.json'
            config.write_text(json.dumps({'hostname': socket.gethostname(), 'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(), 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(), 'locks': [str(lock) for lock in locks], 'arguments': [], 'timeout_seconds': 1}))
            observations = []
            original = supervise.write

            def observe(path, value):
                if path.name == 'result.json':
                    for lock in locks:
                        with lock.open('a+') as contender:
                            if value['child_reaped']:
                                fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
                                self.assertTrue(value['resource_locks_released'])
                            else:
                                with self.assertRaises(BlockingIOError):
                                    fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
                                self.assertFalse(value['resource_locks_released'])
                    observations.append(value['child_reaped'])
                original(path, value)

            with patch.object(supervise.subprocess, 'Popen', return_value=UnreapedChild()), patch.object(supervise.time, 'sleep'), patch.object(supervise, 'write', side_effect=observe):
                supervise.supervise(config)
            self.assertEqual(observations, [False, True])
            result = json.loads((case / 'result.json').read_text())
            self.assertEqual(result['status'], 'failed')
            self.assertEqual(result['exit_code'], -9)

    def test_restart_keeps_both_leases_through_owned_kill_reap_and_replacement(self):
        cases = [(role, killed, generations, unexpected, live_peer)
                 for role in ('source', 'requester')
                 for killed in ('source', 'requester')
                 for generations in (2, 3) for unexpected in (False, True) for live_peer in (False, True)]
        for role, killed, generations, unexpected, live_peer in cases:
            with self.subTest(role=role, killed=killed, generations=generations, unexpected=unexpected, live_peer=live_peer), tempfile.TemporaryDirectory() as directory:
                root = Path(directory);case = root / 'case';case.mkdir()
                locks = [root / 'nic1.lock', root / 'nic2.lock']
                for lock in locks: lock.touch()
                binary = root / 'hardware_peer'
                binary.write_text('#!' + sys.executable + '\n' + "import fcntl,json,os,pathlib,time\nroot=pathlib.Path(os.environ['ORBITTE_RESTART_ROOT'])\ngeneration=int(os.environ['ORBITTE_RESTART_GENERATION'])\nif os.environ['UNEXPECTED']=='1':raise SystemExit(7)\nheld=[]\nfor name in json.loads(os.environ['LOCKS']):\n with open(name,'a+') as lock:\n  try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)\n  except BlockingIOError:held.append(name)\n(root/('leases-'+str(generation)+'.json')).write_text(json.dumps(held))\nif os.environ['ROLE']==os.environ['KILLED'] and generation+1<int(os.environ['GENERATIONS']):\n marker=root/('restart-request-'+str(generation)+'.txt')\n temporary=marker.with_suffix('.tmp')\n temporary.write_text(str(os.getpid())+' 127.0.0.1:1 00 1 0 1 '+str(generation))\n temporary.replace(marker)\n while True:time.sleep(1)\n")
                binary.chmod(0o700)
                config = case / 'config.json'
                config.write_text(json.dumps(dict(hostname=socket.gethostname(),
                    boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                    binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                    locks=[str(lock) for lock in locks], arguments=[], timeout_seconds=3,
                    restart_role=role, restart_kill_role=killed, restart_generations=generations, restart_live_peer=live_peer,
                    env=dict(ROLE=role, KILLED=killed, GENERATIONS=str(generations),
                             UNEXPECTED=str(int(unexpected)), LOCKS=json.dumps([str(lock) for lock in locks])))))
                process = subprocess.run([sys.executable, '-B', str(Path(supervise.__file__)), 'start', str(config)],
                                         capture_output=True, text=True, timeout=12)
                self.assertEqual(process.returncode, 0, process.stderr)
                result = json.loads((case / 'result.json').read_text())
                self.assertTrue(result['child_reaped'] and result['resource_locks_released'])
                self.assertEqual(result['status'], 'failed' if unexpected else 'passed')
                self.assertEqual(len(result['phases']), 1 if unexpected or (live_peer and role != killed) else generations)
                for phase in result['phases']:
                    self.assertTrue(phase['child_reaped'])
                    self.assertFalse(Path('/proc', str(phase['native_pid'])).exists())
                    if not unexpected:
                        killed_phase = role == killed and phase['generation'] + 1 < generations
                        self.assertEqual(phase['exit_code'], -9 if killed_phase else 0)
                        self.assertEqual(phase['intentional_sigkill'], killed_phase)
                        self.assertEqual(json.loads((case / ('leases-' + str(phase['generation']) + '.json')).read_text()), [str(lock) for lock in locks])
                for lock in locks:
                    with lock.open('a+') as contender: fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)



if __name__ == '__main__':
    unittest.main()
