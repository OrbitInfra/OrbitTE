"""Validate native pinned chunk reuse and completion from the public GPU peer."""
import json
from gdr import require, records


def validate_case(case):
    require(case.get('transport') in ('tcp', 'rdma') and case.get('cuda_memory', 'staged') == 'staged'
            and case.get('source') != case.get('requester'), 'invalid staging transport scope')
    remote = case.get('remote_staging', False)
    require(type(remote) is bool, 'remote_staging must be boolean')
    if remote:
        require(case.get('source_kind') == 'cuda:0' and case.get('host_memory', 'direct') == 'direct'
                and case.get('staging_chunk_size', 0) <= case.get('rdma_block_size', 64 << 10)
                and not case.get('rails'), 'remote staging gate requires remote GPU and one RDMA slice per chunk')
    kinds = [case.get(role + '_kind') for role in ('source', 'requester')]
    require(all(kind in ('cpu', 'cuda:0') for kind in kinds) and 'cuda:0' in kinds, 'invalid staging memory kind')
    chunk = case.get('staging_chunk_size')
    length = case.get('slice_bytes', case['payload'] // 4 - 23)
    require(type(chunk) is int and 0 < chunk < length and length % chunk != 0,
            'staging gate requires multiple chunks and a folded tail')


def verify(case, evidence):
    validate_case(case)
    chunk = case['staging_chunk_size']
    length = case.get('slice_bytes', case['payload'] // 4 - 23)
    sizes = [chunk] * (length // chunk) + [length % chunk]
    for role in ('source', 'requester'):
        proof = evidence[role]
        result = json.loads(proof['result.json'])
        require(result['status'] == 'passed' and result['child_reaped'] and result['resource_locks_released']
                and result.get('fault_library_sha256'), role + ': supervised owner cleanup missing')
        if case.get('remote_staging', False):
            remote = records(proof, 'ORBITTE_REMOTE_STAGE_STATS ')
            expected = dict(pins=6, delegates=6 * len(sizes), crosses=6 * len(sizes), unpins=6) if role == 'source' else dict(pins=0, delegates=0, crosses=0, unpins=0)
            require(remote == [expected], role + ': remote staging operations missing or incomplete')
        stats = records(proof, 'ORBITTE_STAGING_STATS ')
        require(len(stats) == 1, role + ': missing staging counters')
        if case[role + '_kind'] == 'cpu':
            require(all(value == 0 for value in stats[0].values()) and not proof.get('cuda-staging.jsonl'), role + ': CPU path spent CUDA chunks')
            continue
        require(stats[0] == dict(allocations=1, reuses=5, chunks=6 * len(sizes), bytes=6 * length, reserved_bytes=chunk), role + ': chunk accounting mismatch')
        native = [json.loads(line) for line in proof['cuda-staging.jsonl'].splitlines()]
        require(native and all(row['pid'] == result['native_pid'] and row['code'] == 0 for row in native), role + ': failed or foreign CUDA receipt')
        memory = records(proof, 'ORBITTE_MEMORY ')
        require(len(memory) == 1 and memory[0].get('cuda') is True
                and memory[0]['base'] > 0 and memory[0]['length'] == case['payload'] + 128,
                role + ': missing exact GPU allocation identity')
        owner = memory[0]
        allocations = [row for row in native if row['kind'] == 'allocate']
        require(len(allocations) == 1 and allocations[0]['address'] == owner['base']
                and allocations[0]['length'] == owner['length'], role + ': GPU allocation differs from registered owner')
        live = {}
        devices = {}
        transfers = []
        previous = 0
        for row in native:
            require(row['seq'] > previous, role + ': native sequence is not ordered')
            previous = row['seq']
            if row['kind'] == 'allocate':
                require(row['address'] not in devices, role + ': duplicate live GPU allocation')
                devices[row['address']] = row['length']
            elif row['kind'] == 'free':
                require(row['address'] in devices, role + ': device free without live allocation')
                del devices[row['address']]
            elif row['kind'] == 'pin':
                address, size = row['address'], row['length']
                require(address > 0 and size > 0 and all(address + size <= start or start + count <= address for start, count in live.items()), role + ': duplicate/overlapping live pin')
                live[address] = size
            elif row['kind'] == 'unpin':
                require(row['address'] in live, role + ': unpin without a live allocation')
                del live[row['address']]
            elif row['kind'] in ('htod', 'dtoh'):
                address = row['source'] if row['kind'] == 'htod' else row['destination']
                hosts = [start for start, count in live.items() if start <= address and address + row['length'] <= start + count]
                require(row['length'] > 0 and len(hosts) == 1, role + ': copy outside live pinned memory')
                device = row['destination'] if row['kind'] == 'htod' else row['source']
                require(any(start <= device and device + row['length'] <= start + count for start, count in devices.items()),
                        role + ': copy outside live GPU allocation')
                transfers.append((row, hosts[0]))
        require(not live and not devices, role + ': native allocations remain after shutdown')
        device_free = next(row['seq'] for row in native if row['kind'] == 'free' and row['address'] == owner['base'])
        for index, (copy, host) in enumerate(transfers):
            host_free = next(row['seq'] for row in native if row['kind'] == 'unpin'
                             and row['address'] == host and row['seq'] > copy['seq'])
            next_copy = transfers[index + 1][0]['seq'] if index + 1 < len(transfers) else device_free
            end = min(next_copy, host_free, device_free)
            following = [row for row in native if copy['seq'] < row['seq'] < end and row['tid'] == copy['tid']]
            require([row['kind'] for row in following[:3]] == ['record', 'wait', 'pop'],
                    role + ': native reuse/free before event wait/context restore')
            record, wait, _ = following[:3]
            require(record['stream'] == copy['stream'] and record['event'] != 0 and wait['event'] == record['event'],
                    role + ': completion belongs to a different stream/event')
        pins = [row for row in native if row['kind'] == 'pin' and row['length'] == chunk]
        require(len(pins) == 1, role + ': missing persistent physical chunk')
        pin = pins[0]
        # Native addresses may be reused after free; bound this allocation by sequence.
        frees = [row for row in native if row['kind'] == 'unpin' and row['address'] == pin['address'] and row['seq'] > pin['seq']]
        require(frees, role + ': chunk free receipt missing')
        free = frees[0]
        copies = [row for row in native if pin['seq'] < row['seq'] < free['seq'] and (
            (row['kind'] == 'htod' and row['source'] == pin['address']) or
            (row['kind'] == 'dtoh' and row['destination'] == pin['address']))]
        expected_copies = [(owner['base'] + 64 + index * (case['payload'] // 4)
                            + (11 if role == 'source' else 5) + offset, min(chunk, length - offset))
                           for index in range(3) for offset in range(0, length, chunk)]
        for direction in ('htod', 'dtoh'):
            actual_copies = [(row['destination'] if direction == 'htod' else row['source'], row['length'])
                             for row in copies if row['kind'] == direction]
            require(actual_copies == expected_copies,
                    role + ': native ' + direction + ' GPU range/chunk/tail mismatch')
        require(len({row['stream'] for row in copies}) == 1, role + ': allocation copy stream changed')
