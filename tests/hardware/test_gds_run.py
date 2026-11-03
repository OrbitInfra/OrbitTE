"""CPU controls for the installed native-route evidence reader."""
import unittest

from gds_run import route_deltas


def snapshot(read, write, error=0):
    return (f'Reads   : n=8 ok={read} err={error} readMiB={read}\n'
            f'Writes  : n=9 ok={write} err=0 writeMiB={write}\n')


class NativeCounters(unittest.TestCase):
    def test_matching_deltas_in_both_directions(self):
        self.assertEqual(route_deltas(snapshot(4, 10), snapshot(6, 12))['Writes']['writeMiB'], 2)

    def test_absent_compat_only_mismatched_or_failed_io_is_rejected(self):
        for after in ['', snapshot(4, 10), snapshot(6, 11), snapshot(7, 12), snapshot(6, 12, 1)]:
            with self.subTest(after=after), self.assertRaises((ValueError, RuntimeError)):
                route_deltas(snapshot(4, 10), after)
