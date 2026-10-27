"""Negative controls for installed automatic discovery and its independent oracle."""
import copy
import json
import unittest

import installed
import installed_discovery
import test_installed_accounting


class InstalledDiscovery(unittest.TestCase):
    def setUp(self):
        fixture = test_installed_accounting.InstalledRailAccounting(); fixture.setUp()
        self.case = {**fixture.case, 'discovery': 'auto'}
        self.profile = {**fixture.profile, 'nodes': {
            'a': dict(hostname='host-a', boot_id='boot-a'), 'b': dict(hostname='host-b', boot_id='boot-b')}}
        self.evidence = fixture.evidence()
        for role, record in self.evidence.items():
            node = self.profile['nodes'][self.case[role]]
            inventory = dict(**node, numa_nodes=[0, 1], allowed_cpus=[0, 1], devices=[])
            rows = [('ORBITTE_INSTALLED_DISCOVERY_CONFIG ', dict(device=None, port=None, gid_index=None,
                     whitelist=self.profile['rails'], blacklist=self.profile['rails'])),
                    ('ORBITTE_INSTALLED_DISCOVERY_PIN ', dict(gid_index=255, unavailable=True))]
            text = record['stdout.log']
            for i, name in enumerate(self.profile['rails']):
                gid = dict(index=3, address='::ffff:10.0.0.' + str(i + 1), kind='RoCE v2',
                           netdev='eth' + str(i), ifindex=10 + i, virtual=False)
                inventory['devices'].append(dict(name=name, pci=str(i), numa=1, ports=[dict(
                    port=1, state='4: ACTIVE', link_layer='Ethernet', lid=0, gids=[gid])]))
                attrs = {'rdma.selected': 'true', 'rdma.port': '1', 'rdma.gid_index': '3',
                         'rdma.gid': gid['address'], 'rdma.gid_type': 'RoCE v2', 'rdma.net_device': gid['netdev'],
                         'rdma.net_device_index': str(gid['ifindex']), 'rdma.lid': '0', 'rdma.link_layer': '2'}
                rows.append(('ORBITTE_INSTALLED_TOPOLOGY_NIC ', dict(id=i, name=name, pci=str(i), numa=1, attrs=attrs)))
                old = 'ORBITTE_INSTALLED_NIC ' + json.dumps(dict(name=name, pci=str(i)))
                text = text.replace(old, 'ORBITTE_INSTALLED_NIC ' + json.dumps(dict(name=name,pci=str(i),gid=gid['address'],gid_type='RoCE v2')))
            for numa, ranks in [(0, [[], [], [0, 1]]), (1, [[0, 1], [], []]), (None, [[], [], [0, 1]])]:
                rows.append(('ORBITTE_INSTALLED_TOPOLOGY_MEMORY ', dict(name='*' if numa is None else 'cpu:' + str(numa), numa=numa, ranks=ranks)))
            record['stdout.log'] = text + '\n' + '\n'.join(prefix + json.dumps(row) for prefix, row in rows)
            record['inventory-before.json'] = record['inventory-after.json'] = json.dumps(inventory)
            result = json.loads(record['result.json']); result['inventory_tool_sha256'] = 'b' * 64
            record['result.json'] = json.dumps(result)

    def test_auto_discovery_keeps_the_complete_native_accounting_contract(self):
        installed.verify(self.profile, self.case, self.evidence)
        profile = {**self.profile, 'gid_index': 3, 'nodes': {**self.profile['nodes'], 'a': {**self.profile['nodes']['a'], 'bind': '127.0.0.1:0'}}}
        _, environment = installed.command(profile, self.case, 'a', 'source', '/tmp/unused')
        self.assertNotIn('ORBITTE_SHARED_GID', environment)
        self.assertEqual(environment['ORBITTE_HOST_DISCOVERY'], 'auto')
        for value in ('pinned', True, None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                installed.validate({**self.case, 'discovery': value})

    def test_inventory_and_public_routes_must_agree_without_fallback(self):
        for name, replace in [
            ('pinned-route', ('"gid_index": null', '"gid_index": 3')),
            ('native-gid-index', ('"rdma.gid_index": "3"', '"rdma.gid_index": "1"')),
            ('native-netdev', ('"rdma.net_device": "eth0"', '"rdma.net_device": "eth9"')),
            ('native-ifindex', ('"rdma.net_device_index": "10"', '"rdma.net_device_index": "99"')),
            ('native-pci', ('"pci": "0", "numa"', '"pci": "9", "numa"')),
            ('numa-rank', ('"ranks": [[0, 1], [], []]', '"ranks": [[], [], [0, 1]]')),
            ('pin-fallback', ('"unavailable": true', '"unavailable": false')),
        ]:
            evidence = copy.deepcopy(self.evidence)
            text = evidence['source']['stdout.log']; self.assertIn(replace[0], text)
            evidence['source']['stdout.log'] = text.replace(*replace)
            with self.subTest(control=name), self.assertRaises(ValueError):
                installed_discovery.verify(self.profile, self.case, evidence)
        for name, change in [
            ('wrong-host', lambda d: d.update(hostname='other')),
            ('ambiguous-gid', lambda d: d['devices'][0]['ports'][0]['gids'].append(dict(d['devices'][0]['ports'][0]['gids'][0], index=4))),
            ('valid-negative-pin', lambda d: d['devices'][0]['ports'][0]['gids'][0].update(index=255)),
            ('down-port', lambda d: d['devices'][0]['ports'][0].update(state='1: DOWN')),
        ]:
            evidence = copy.deepcopy(self.evidence); record = evidence['requester']
            inventory = json.loads(record['inventory-before.json']); change(inventory)
            record['inventory-before.json'] = record['inventory-after.json'] = json.dumps(inventory)
            with self.subTest(control=name), self.assertRaises(ValueError):
                installed_discovery.verify(self.profile, self.case, evidence)
        evidence = copy.deepcopy(self.evidence); evidence['source']['inventory-after.json'] = '{}'
        with self.assertRaises(ValueError): installed_discovery.verify(self.profile, self.case, evidence)
