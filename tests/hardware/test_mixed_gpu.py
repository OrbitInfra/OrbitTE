"""CPU controls for the installed mixed-route evidence contract."""
import unittest

import mixed_gpu


def profile():
    return {
        'nodes': {n: dict(hostname=n, boot_id='boot-' + n, gpu_uuid='GPU-' + n,
                          gpu_lock='/gpu', nic_lock='/nic') for n in ('a', 'b')},
        'cases': [dict(source='a', requester='b', source_kind='cuda:0', requester_kind='cuda:0',
                       transport='rdma', staged_leg=leg) for leg in sorted(mixed_gpu.LEGS)],
    }


class MixedGPU(unittest.TestCase):
    def test_profile_requires_each_staged_leg_and_distinct_host_owners(self):
        good = profile()
        mixed_gpu.validate(good)
        missing = profile()
        missing['cases'].pop()
        shared = profile()
        shared['nodes']['b']['boot_id'] = shared['nodes']['a']['boot_id']
        for bad in [missing, shared]:
            with self.assertRaises(ValueError):
                mixed_gpu.validate(bad)

    def test_tcp_or_staging_only_bytes_cannot_pass_as_gpu_direct(self):
        case = dict(staged_leg='tcp-pin')
        source = 'ORBITE_MIXED_OK source tcp-pin rdma_read=0 rdma_write=0 private_pds=4 staged_bytes=26 pins=2'
        requester = 'ORBITE_MIXED_OK requester tcp-pin rdma_read=14 rdma_write=14 private_pds=0 staged_bytes=26 pins=0'
        def verify(s, r):
            mixed_gpu.verify(case, {'source': {'stdout.log': s}, 'requester': {'stdout.log': r}})
        verify(source, requester)
        for s, r in [(source.replace('pds=4', 'pds=0'), requester),
                     (source, requester.replace('read=14', 'read=27')),
                     (source, requester.replace('bytes=26', 'bytes=0')),
                     (source.replace('pins=2', 'pins=4'), requester),
                     (source, requester + '\n' + requester)]:
            with self.assertRaises(ValueError):
                verify(s, r)
