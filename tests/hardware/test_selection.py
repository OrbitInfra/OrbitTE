"""Keep renamed native peer selectors from passing with zero matched tests."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('hardware_run', Path(__file__).with_name('run.py'))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class NativePeerSelection(unittest.TestCase):
    def test_known_profiles_select_the_current_exact_native_test(self):
        prefix = 'transport::rdma::workers::tests::hardware::'
        for profile, expected in [(None, None), ('orbitte-shared-cq', prefix + 'shared_cq_peer'),
                                  ('orbitte-window', prefix + 'source_window_peer'),
                                  ('orbitte-connected', prefix + 'connected::connected_endpoint_peer'),
                                  ('orbitte-multirail', prefix + 'multirail::multirail_peer'),
                                  ('orbitkv-peer-host', 'peer::orbitte::tests::rdma_host::registered_host_rdma_peer')]:
            with self.subTest(profile=profile):
                self.assertEqual(runner.native_peer_test({'runner': profile}), expected)
        with self.assertRaises(ValueError):
            runner.native_peer_test({'runner': 'unsupported'})

    def test_zero_ambiguous_or_benchmark_only_matches_are_rejected_before_deploy(self):
        test = runner.native_peer_test({'runner': 'orbitte-shared-cq'})
        runner.validate_peer_listing(test, test + ': test\n\n1 test, 0 benchmarks\n')
        for name, listing in [('zero', '0 tests, 0 benchmarks\n'),
                              ('old-path', 'rdma::cq::tests::hardware::shared_cq_peer: test\n'),
                              ('two', test + ': test\nother: test\n'),
                              ('duplicate', 2 * (test + ': test\n')),
                              ('benchmark', test + ': benchmark\n')]:
            with self.subTest(name=name), self.assertRaises(ValueError):
                runner.validate_peer_listing(test, listing)
