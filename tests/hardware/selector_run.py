"""Run the packaged host consumer on both selector routes using a locked host inventory."""
import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', type=Path, required=True, help='two-RNIC installed-host profile')
    parser.add_argument('--artifact', type=Path, required=True, help='build_installed.py host output')
    parser.add_argument('--output', type=Path, required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--selective', action='store_true', help='check WR/CQE counts and QP depth clamps on each rail')
    mode.add_argument('--non-atomic', action='store_true', help='require both hosts to report no HCA atomics')
    mode.add_argument('--fault-netdev', help='exercise a real link down/up on this dedicated requester RoCE data interface')
    args = parser.parse_args()
    profile = json.loads(args.profile.read_text())
    if profile.get('runner') != 'orbite-installed-host' or len(profile['rails']) != 2:
        raise ValueError('expected a two-RNIC installed-host inventory')
    source, requester = (profile['cases'][0][role] for role in ('source', 'requester'))
    profile['cases'] = [dict(mode='installed-selector', source=source, requester=requester,
                             source_kind='cpu', requester_kind='cpu', transport='rdma', operation=op,
                             parents=1, block=65536, payload=262145, mask=3, num_lanes=1,
                             rdma_events=False) for op in ('read', 'write')]
    if args.selective or args.non_atomic:
        profile['cases'] = [dict(case, mode='installed-selective', mask=mask,
                                 payload=8 * case['block'] + 17, non_atomic=args.non_atomic)
                            for case in profile['cases'] for mask in (1, 2)]
    if args.fault_netdev:
        profile['cases'] = [dict(profile['cases'][0], mode='installed-rail-health',
                                 fault_netdev=args.fault_netdev, rdma_events=True)]
    with tempfile.TemporaryDirectory() as work:
        selected = Path(work) / 'profile.json'
        selected.write_text(json.dumps(profile))
        subprocess.run([sys.executable, str(Path(__file__).with_name('run.py')),
                        '--profile', str(selected), '--installation', str(args.artifact / 'manifest.json'),
                        '--binary', str(args.artifact / 'installed/bin/orbite-rdma-host'),
                        '--output', str(args.output)], check=True)


if __name__ == '__main__':
    main()
