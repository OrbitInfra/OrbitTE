"""Validate native pinned chunk reuse and completion from the public GPU peer."""
import json
from gdr import require, records
import staging_native


def configured_depth(case, role):
    return case.get(role + '_pipeline_depth', case.get('staging_pipeline_depth', 1))


def negotiated_depth(case):
    length = case.get('slice_bytes', case['payload'] // 4 - 23)
    chunk = case['staging_chunk_size']
    return min(configured_depth(case, 'source'), configured_depth(case, 'requester'), (length + chunk - 1) // chunk)


def validate_case(case):
    require(case.get('transport') in ('tcp', 'rdma') and case.get('cuda_memory', 'staged') == 'staged'
            and case.get('source') != case.get('requester'), 'invalid staging transport scope')
    remote = case.get('remote_staging', False)
    require(type(remote) is bool, 'remote_staging must be boolean')
    if remote:
        require(case.get('source_kind') == 'cuda:0' and case.get('host_memory', 'direct') == 'direct'
                and case.get('staging_chunk_size', 0) <= case.get('rdma_block_size', 64 << 10)
                and not case.get('rails'), 'remote staging gate requires remote GPU and one RDMA slice per chunk')
    for role in ('source', 'requester'):
        depth = configured_depth(case, role)
        require(type(depth) is int and 1 <= depth <= 16 and (remote or depth == 1), 'invalid staging pipeline depth')
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
        configured = configured_depth(case, role)
        require(records(proof, 'ORBITTE_STAGING_CONFIG ') == [dict(depth=configured, chunk_count=configured, chunk_size=chunk)],
                role + ': installed configuration differs from frozen profile')
        if case.get('remote_staging', False):
            remote = records(proof, 'ORBITTE_REMOTE_STAGE_STATS ')
            expected = dict(pins=6, delegates=6 * len(sizes), crosses=6 * len(sizes), unpins=6) if role == 'source' else dict(pins=0, delegates=0, crosses=0, unpins=0)
            require(remote == [expected], role + ': remote staging operations missing or incomplete')
        stats = records(proof, 'ORBITTE_STAGING_STATS ')
        require(len(stats) == 1, role + ': missing staging counters')
        if case[role + '_kind'] == 'cpu':
            require(all(value == 0 for value in stats[0].values()) and not proof.get('cuda-staging.jsonl'), role + ': CPU path spent CUDA chunks')
            continue
        width = negotiated_depth(case) if case.get('remote_staging') and role == 'source' else 1
        require(stats[0] == dict(allocations=width, reuses=5 * width, chunks=6 * len(sizes), bytes=6 * length, reserved_bytes=chunk * width), role + ': chunk accounting mismatch')
        native = [json.loads(line) for line in proof['cuda-staging.jsonl'].splitlines()]
        ledger = staging_native.validate(native, result['native_pid'])
        memory = records(proof, 'ORBITTE_MEMORY ')
        require(len(memory) == 1 and memory[0].get('cuda') is True
                and memory[0]['base'] > 0 and memory[0]['length'] == case['payload'] + 128,
                role + ': missing exact GPU allocation identity')
        owner = memory[0]
        allocations = [row for row in native if row['kind'] == 'allocate']
        require(len(allocations) == 1 and allocations[0]['address'] == owner['base']
                and allocations[0]['length'] == owner['length'], role + ': GPU allocation differs from registered owner')
        pins = [row for row in ledger['pins'] if row['length'] == chunk]
        require(len(pins) == width, role + ': persistent physical slot count differs from negotiated depth')
        copies = [copy for copy in ledger['copies'] if copy['host'] in pins]
        expected_copies = [(owner['base'] + 64 + index * (case['payload'] // 4)
                            + (11 if role == 'source' else 5) + offset, min(chunk, length - offset))
                           for index in range(3) for offset in range(0, length, chunk)]
        for direction in ('htod', 'dtoh'):
            actual = [(c['row']['destination'] if direction == 'htod' else c['row']['source'], c['row']['length'])
                      for c in copies if c['row']['kind'] == direction]
            require(actual == expected_copies, role + ': native GPU range/chunk/tail mismatch')
        require(len({c['stream_owner']['seq'] for c in copies}) == width,
                role + ': persistent copy stream count differs from negotiated depth')
        require(len([h for h in ledger['handles'] if h['kind'] == 'stream_create']) == (width + 1 if width > 1 else 1)
                and len([h for h in ledger['handles'] if h['kind'] == 'event_create']) == (width + 1 if width > 1 else 1),
                role + ': stream/event creation churn or missing owner')
        for pin in pins:
            uses = [c for c in copies if c['host'] is pin]
            require(len({(c['stream_owner']['seq'], c['event_owner']['seq']) for c in uses}) == 1,
                    role + ': physical slot changed stream/event between generations')
            if width > 1:
                require(uses[0]['stream_owner']['destroyed'] < pin['release_started']
                        and uses[0]['event_owner']['destroyed'] < pin['release_started'],
                        role + ': slot freed before its private stream/event')
        # A receipt interval proves scheduling, not that DMA was still running.
        require(ledger['peak_unconfirmed'] == width, role + ': observed outstanding copy width differs')
