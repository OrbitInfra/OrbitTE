"""Compare installed public topology to a separately captured physical inventory."""
import ipaddress
import json

from installed_restart import records


def verify(profile, case, evidence):
    for role in ('source', 'requester'):
        record = evidence[role]
        result = json.loads(record['result.json'])
        digest = result.get('inventory_tool_sha256', '')
        before = json.loads(record.get('inventory-before.json') or '{}')
        after = json.loads(record.get('inventory-after.json') or '{}')
        node = profile['nodes'][case[role]]
        if (len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest)
                or not before or before != after or result.get('inventory_error')
                or any(before.get(k) != node[k] for k in ('hostname', 'boot_id'))):
            raise ValueError('discovery inventory is missing, changed or from another host')
        if records(record, 'ORBITTE_INSTALLED_DISCOVERY_CONFIG ') != [dict(
                device=None, port=None, gid_index=None, whitelist=profile['rails'], blacklist=profile['rails'])]:
            raise ValueError('discovery was pinned or escaped its locked devices')
        if records(record, 'ORBITTE_INSTALLED_DISCOVERY_PIN ') != [dict(gid_index=255, unavailable=True)]:
            raise ValueError('strict unavailable GID did not reject fallback')
        nics = records(record, 'ORBITTE_INSTALLED_TOPOLOGY_NIC ')
        if ([n['name'] for n in nics] != sorted(profile['rails'])
                or [n['id'] for n in nics] != [0, 1]
                or [d['name'] for d in before['devices']] != sorted(profile['rails'])
                or len({d['pci'] for d in before['devices']}) != 2):
            raise ValueError('automatic NIC inventory or stable IDs differ')
        for nic, device in zip(nics, before['devices']):
            if (nic['pci'], nic['numa']) != (device['pci'], device['numa']):
                raise ValueError('PCI/NUMA publication differs from the physical inventory')
            ports = device['ports']
            # The frozen RoCE profile has one active physical port and one
            # non-overlay, globally routed RoCEv2 GID. No ranking implementation
            # is imported or reimplemented to turn an ambiguous inventory into a pass.
            if len(ports) != 1 or ports[0]['state'].split(':')[0].strip() != '4' or ports[0]['link_layer'] != 'Ethernet':
                raise ValueError('inventory is outside the frozen active RoCE profile')
            port = ports[0]
            candidates = [g for g in port['gids'] if g['kind'] == 'RoCE v2' and g['netdev'] and g['ifindex'] > 0
                          and not g['virtual'] and not ipaddress.IPv6Address(g['address']).is_link_local]
            if len(candidates) != 1 or any(g['index'] == 255 for g in port['gids']):
                raise ValueError('automatic GID oracle is ambiguous or negative pin is valid')
            gid, attrs = candidates[0], nic['attrs']
            expected = {'rdma.selected': 'true', 'rdma.port': str(port['port']),
                        'rdma.gid_index': str(gid['index']), 'rdma.gid_type': 'RoCE v2',
                        'rdma.net_device': gid['netdev'], 'rdma.net_device_index': str(gid['ifindex']),
                        'rdma.lid': str(port['lid']), 'rdma.link_layer': '2'}
            if (any(attrs.get(k) != v for k, v in expected.items())
                    or ipaddress.IPv6Address(attrs['rdma.gid']) != ipaddress.IPv6Address(gid['address'])):
                raise ValueError('automatic route differs from the independently inventoried GID')
            published = next(n for n in records(record, 'ORBITTE_INSTALLED_NIC ') if n['name'] == nic['name'])
            if published['pci'] != nic['pci'] or ipaddress.IPv6Address(published['gid']) != ipaddress.IPv6Address(gid['address']) or published['gid_type'] != 'RoCE v2':
                raise ValueError('public route and topology snapshots disagree')
        expected_memory = []
        for numa in [*before['numa_nodes'], None]:
            local = [n['id'] for n in nics if numa is not None and n['numa'] == numa]
            remote = [n['id'] for n in nics if n['id'] not in local]
            expected_memory.append(dict(name='*' if numa is None else 'cpu:' + str(numa),
                                        numa=numa, ranks=[local, [], remote]))
        if records(record, 'ORBITTE_INSTALLED_TOPOLOGY_MEMORY ') != expected_memory:
            raise ValueError('memory priority ranks disagree with physical NUMA placement')
