"""Read-only host inventory, independent of OrbitTE's discovery implementation."""
import ipaddress
import json
import os
from pathlib import Path
import socket
import sys


def capture(rails, root=Path('/sys')):
    if (len(rails) != 2 or len(set(rails)) != 2
            or any(not name or any(not (c.isascii() and (c.isalnum() or c in '_.-')) for c in name) for name in rails)):
        raise ValueError('inventory requires two explicit locked RNIC names')
    def read(path): return path.read_text().strip()
    devices = []
    for name in sorted(rails):
        path = root / 'class/infiniband' / name
        numa = int(read(path / 'device/numa_node'))
        device = dict(name=name, pci=(path / 'device').resolve().name,
                      numa=numa if numa >= 0 else None, ports=[])
        for port in sorted((path / 'ports').iterdir()):
            row = dict(port=int(port.name), state=read(port / 'state'),
                       link_layer=read(port / 'link_layer'), lid=int(read(port / 'lid'), 0), gids=[])
            for entry in sorted((port / 'gids').iterdir(), key=lambda p: int(p.name)):
                address = ipaddress.IPv6Address(read(entry))
                if address.is_unspecified or address.is_multicast: continue
                netdev = read(port / 'gid_attrs/ndevs' / entry.name)
                if '/' in netdev or netdev in ('.', '..'): raise ValueError('invalid native netdev')
                row['gids'].append(dict(index=int(entry.name), address=str(address),
                    kind=read(port / 'gid_attrs/types' / entry.name), netdev=netdev,
                    ifindex=int(read(root / 'class/net' / netdev / 'ifindex')) if netdev else 0,
                    virtual=(root / 'devices/virtual/net' / netdev).exists() if netdev else False))
            device['ports'].append(row)
        devices.append(device)
    nodes = sorted(int(p.name[4:]) for p in (root / 'devices/system/node').iterdir()
                   if p.name.startswith('node') and p.name[4:].isdigit())
    return dict(hostname=socket.gethostname(), boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                devices=devices, numa_nodes=nodes, allowed_cpus=sorted(os.sched_getaffinity(0)))


if __name__ == '__main__':
    print(json.dumps(capture(sys.argv[1:]), sort_keys=True))
