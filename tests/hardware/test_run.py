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


if __name__ == '__main__':
    unittest.main()
