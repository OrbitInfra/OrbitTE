"""Keep the resource lease independent of native/test-harness argument layout."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('hardware_run', Path(__file__).with_name('run.py'))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class ResourceSelection(unittest.TestCase):
    def test_host_rdma_test_filter_uses_the_nic_lease_without_a_gpu(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / 'fixture'
            binary.write_bytes(b'not-executed')
            profile = {'ssh_config': 'fixture-ssh', 'nodes': {'a': {'alias': 'fixture', 'hostname': 'fixture', 'boot_id': 'fixture', 'nic_lock': '/fixture/nic', 'gpu_lock': '/fixture/gpu', 'gpu_uuid': 'fixture-gpu', 'env': {'LD_LIBRARY_PATH': '/fixture/compat', 'ROLE': 'node-default'}}}}
            controller = runner.Controller(profile, binary, root)
            controller.ssh = lambda *args: None
            controller.copy = lambda *args: None
            with patch.object(runner.subprocess, 'Popen'):
                controller.start('a', '/fixture/case', 'cpu', ['one::exact::test'], 'rdma', {'ROLE': 'source'})
            config = json.loads((root / 'case.json').read_text())
            self.assertEqual(config['locks'], ['/fixture/nic'])
            self.assertEqual(config['env'], {'ROLE': 'source', 'LD_LIBRARY_PATH': '/fixture/compat'})
            self.assertEqual(config['arguments'], ['one::exact::test'])
            self.assertNotIn('CUDA_VISIBLE_DEVICES', config['env'])


    def test_event_profile_is_explicit_native_only_and_propagates_before_start(self):
        class CapturedStart(Exception):
            pass

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / 'fixture'
            binary.write_bytes(b'not-executed')
            base = {'rnic': 'fixture-rnic', 'gid_index': 0,
                    'nodes': {'a': {'bind': '127.0.0.1:0'}, 'b': {'bind': '127.0.0.1:0'}}}
            case = {'source': 'a', 'requester': 'b', 'source_kind': 'cpu', 'requester_kind': 'cpu',
                    'transport': 'rdma', 'payload': 4096, 'seed': 1, 'rdma_max_cqes': 1, 'rdma_max_send_wr': 1}
            for profile in [None, 'orbitte-shared-cq', 'orbitte-connected']:
                for enabled in [False, True]:
                    with self.subTest(profile=profile, enabled=enabled):
                        controller = runner.Controller({**base, 'runner': profile}, binary, root)
                        observed = []

                        def start(*args):
                            observed.append(args[-1])
                            raise CapturedStart()

                        controller.start = start
                        with self.assertRaises(CapturedStart):
                            controller.case(0, {**case, 'rdma_events': enabled})
                        self.assertEqual(observed[0]['ORBITTE_RDMA_EVENTS'], str(int(enabled)))
            for flags in [{'rdma_events': 1}, {'rdma_events': 'true'}, {'rdma_events': None},
                          {'rdma_events': True, 'transport': 'tcp'}]:
                with self.subTest(flags=flags), self.assertRaises(ValueError):
                    runner.Controller(base, binary, root).case(0, {**case, **flags})
            with self.assertRaises(ValueError):
                runner.Controller({**base, 'runner': 'orbitkv-peer-host'}, binary, root).case(0, {**case, 'rdma_events': True})


    def test_fragment_fault_geometry_is_exact_before_start(self):
        class CapturedStart(Exception):
            pass

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / 'fixture'
            binary.write_bytes(b'not-executed')
            profile = {'runner': 'orbitte-window', 'rnic': 'fixture', 'gid_index': 3,
                       'nodes': {'a': {'bind': '127.0.0.1:0'}, 'b': {'bind': '127.0.0.1:0'}}}
            base = {'source': 'a', 'requester': 'b', 'source_kind': 'cpu', 'requester_kind': 'cpu',
                    'transport': 'rdma', 'payload': 12288, 'seed': 1,
                    'rdma_max_cqes': 3, 'rdma_max_send_wr': 3}
            for probe in ['fragment-read', 'fragment-write', 'partial-read', 'partial-write']:
                for events in [False, True]:
                    controller = runner.Controller(profile, binary, root)

                    def start(*args):
                        self.assertEqual(args[-1]['ORBITTE_WINDOW_PROBE'], probe)
                        self.assertEqual(args[-1]['ORBITTE_RDMA_EVENTS'], str(int(events)))
                        raise CapturedStart()

                    controller.start = start
                    with self.subTest(probe=probe, events=events), self.assertRaises(CapturedStart):
                        controller.case(0, {**base, 'window_probe': probe, 'rdma_events': events})
            for invalid in [{'payload': 4096}, {'payload': 12288.0}, {'rdma_max_cqes': 2},
                            {'rdma_max_send_wr': 2}, {'window_probe': 'range'}, {'window_probe': 'unknown'}]:
                with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                    runner.Controller(profile, binary, root).case(0, {**base, 'window_probe': 'fragment-read', **invalid})


    def test_multi_context_profile_is_exact_before_start(self):
        class CapturedStart(Exception):
            pass

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / 'fixture'
            binary.write_bytes(b'not-executed')
            profile = {'runner': 'orbitte-shared-cq', 'rnic': 'fixture', 'gid_index': 3,
                       'nodes': {'a': {'bind': '127.0.0.1:0'}, 'b': {'bind': '127.0.0.1:0'}}}
            case = {'source': 'a', 'requester': 'b', 'source_kind': 'cpu', 'requester_kind': 'cpu',
                    'transport': 'rdma', 'payload': 4096, 'seed': 1,
                    'rdma_max_cqes': 2, 'rdma_max_send_wr': 2, 'rdma_multi_context': True}
            controller = runner.Controller(profile, binary, root)

            def start(*args):
                self.assertEqual(args[-1]['ORBITTE_RDMA_MULTI_CONTEXT'], '1')
                raise CapturedStart()

            controller.start = start
            with self.assertRaises(CapturedStart):
                controller.case(0, case)
            for invalid in [{'rdma_multi_context': 1}, {'rdma_multi_context': 'true'},
                            {'duplex': True}, {'transport': 'tcp'}, {'requester_kind': 'cuda:0'}]:
                with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                    runner.Controller(profile, binary, root).case(0, {**case, **invalid})
            for selection in [None, 'orbitte-connected', 'orbitte-window', 'orbitkv-peer-host']:
                with self.subTest(selection=selection), self.assertRaises(ValueError):
                    runner.Controller({**profile, 'runner': selection}, binary, root).case(0, case)


if __name__ == '__main__':
    unittest.main()
