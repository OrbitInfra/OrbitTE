"""Match CUDA copy generations to their live allocations, context and handles.

This validates successful native receipts, not physical DMA overlap or timing.
A wait is consumed once and only its matching context pop permits reuse/free.
"""
from gdr import require


def validate(native, pid):
    stacks, streams, events, hosts, devices = {}, {}, {}, {}, {}
    copies, pins, handles = [], [], []
    pending = []
    peak = 0

    def idle(items, message):
        require(not any(items), message)

    def allocation(table, address, length):
        matches = [v for base, v in table.items() if base <= address and address + length <= base + v['length']]
        require(length > 0 and len(matches) == 1, 'copy outside live allocation')
        require(not matches[0].get('closing'), 'copy used an allocation being freed')
        return matches[0]

    for seq, row in enumerate(native, 1):
        require(row['seq'] == seq and row['pid'] == pid and row['code'] == 0,
                'unordered, foreign or failed native receipt')
        kind = row['kind']
        stack = stacks.setdefault(row['tid'], [])
        if kind == 'push':
            require(row['context'] > 0, 'null context push')
            stack.append(dict(context=row['context'], copies=[], waits=[], initializing=[]))
            continue
        require(stack, 'CUDA operation outside observed owning context')
        frame = stack[-1]
        context = frame['context']
        if kind == 'pop':
            require(row['context'] == context, 'restored a different context')
            for owner in frame['initializing']:
                require(owner.get('synced'), 'allocation escaped before initialization synchronized')
                owner['initialized'] = True
            for copy in frame['copies']:
                require(copy.get('event'), 'copy escaped context before recording completion')
                copy['submitted'] = True
            for copy in frame['waits']:
                require(copy['submitted'] and copy.get('waited'), 'wait escaped before copy submission restored')
                copy['completed'] = seq
                pending.remove(copy)
                streams[copy['row']['stream']]['pending'] = None
                events[copy['event']]['pending'] = None
            stack.pop()
        elif kind in ('allocate', 'pin'):
            table = hosts if kind == 'pin' else devices
            address, length = row['address'], row['length']
            require(address > 0 and 0 < length < (1 << 64) - address
                    and all(address + length <= start or start + v['length'] <= address for start, v in table.items()),
                    'duplicate/overlapping live allocation')
            table[address] = dict(row, context=context)
            if kind == 'pin': pins.append(table[address])
            else: frame['initializing'].append(table[address])
        elif kind in ('free_begin', 'unpin_begin'):
            table = hosts if kind == 'unpin_begin' else devices
            owner = table.get(row['address'])
            require(owner and owner['context'] == context, 'free without its live context owner')
            require(kind == 'unpin_begin' or owner.get('initialized'), 'device freed before initialization restored')
            key = 'host' if kind == 'unpin_begin' else 'device'
            idle((c[key] is owner for c in pending), 'free before matching wait/context restore')
            require(not owner.get('closing'), 'duplicate free invocation')
            owner['closing'] = row['tid']
            owner['release_started'] = seq
        elif kind in ('free', 'unpin'):
            table = hosts if kind == 'unpin' else devices
            owner = table.get(row['address'])
            require(owner and owner.get('closing') == row['tid'] and owner['context'] == context,
                    'free return lacks matching invocation')
            owner['freed'] = seq
            del table[row['address']]
        elif kind in ('stream_create', 'event_create'):
            key = 'stream' if kind == 'stream_create' else 'event'
            table = streams if key == 'stream' else events
            require(row[key] > 0 and row[key] not in table, 'duplicate live native handle')
            table[row[key]] = dict(row, context=context, pending=None, generation=0)
            handles.append(table[row[key]])
        elif kind in ('stream_destroy_begin', 'event_destroy_begin'):
            key = 'stream' if kind == 'stream_destroy_begin' else 'event'
            table = streams if key == 'stream' else events
            owner = table.get(row[key])
            require(owner and owner['context'] == context and owner['pending'] is None,
                    'handle destroyed without its owner or before completion restore')
            require(not owner.get('closing'), 'duplicate handle destruction invocation')
            owner['closing'] = row['tid']
            owner['release_started'] = seq
        elif kind in ('stream_destroy', 'event_destroy'):
            key = 'stream' if kind == 'stream_destroy' else 'event'
            table = streams if key == 'stream' else events
            owner = table.get(row[key])
            require(owner and owner.get('closing') == row['tid'] and owner['context'] == context,
                    'destruction return lacks matching invocation')
            owner['destroyed'] = seq
            del table[row[key]]
        elif kind in ('htod_begin', 'dtoh_begin'):
            stream = streams.get(row['stream'])
            require(stream and stream['context'] == context and stream['pending'] is None and not stream.get('closing'),
                    'copy on foreign, destroyed or pending stream')
            host = allocation(hosts, row['source'] if kind == 'htod_begin' else row['destination'], row['length'])
            device = allocation(devices, row['destination'] if kind == 'htod_begin' else row['source'], row['length'])
            require(host['context'] == device['context'] == context and device.get('initialized'), 'copy context differs from allocation owner')
            idle((c['host'] is host for c in pending), 'pinned slot reused before its copy drained')
            address = row['destination'] if kind == 'htod_begin' else row['source']
            for other in pending:
                previous = other['row']
                previous_address = previous['destination'] if previous['kind'] == 'htod' else previous['source']
                overlaps = address < previous_address + previous['length'] and previous_address < address + row['length']
                require(not (other['device'] is device and overlaps
                             and (kind == 'htod_begin' or previous['kind'] == 'htod')),
                        'GPU range has a pending conflicting write')
            copy = dict(row={**row, 'kind': kind.removesuffix('_begin')}, host=host, device=device, stream_owner=stream, submitted=False)
            stream['pending'] = copy
            pending.append(copy)
            copies.append(copy)
            frame['copies'].append(copy)
            peak = max(peak, len(pending))
        elif kind in ('htod', 'dtoh'):
            stream = streams.get(row['stream'])
            copy = stream['pending'] if stream else None
            require(copy and copy in frame['copies'] and not copy.get('returned')
                    and all(row[key] == copy['row'][key] for key in ('pid', 'tid', 'kind', 'source', 'destination', 'length', 'stream')),
                    'copy return differs from invocation')
            copy['returned'] = seq
        elif kind == 'record':
            stream, event = streams.get(row['stream']), events.get(row['event'])
            require(stream and event and stream['context'] == event['context'] == context,
                    'record belongs to a different stream/event/context')
            copy = stream['pending']
            require(copy is not None and copy in frame['copies'] and not copy.get('event')
                    and copy.get('returned') and event['pending'] is None and not event.get('closing'), 'record overwrote a live generation or lacks this thread copy')
            event['generation'] += 1
            event['pending'] = copy
            copy.update(event=row['event'], event_owner=event, generation=event['generation'], recorded=row['seq'])
        elif kind == 'wait_begin':
            event = events.get(row['event'])
            require(event and event['context'] == context and event['pending'] is not None,
                    'wait has no matching live event generation')
            copy = event['pending']
            require(not copy.get('wait_started') and (copy['submitted'] or copy in frame['copies']),
                    'duplicate wait or submission context not restored')
            copy['wait_started'] = seq
            frame['waits'].append(copy)
        elif kind == 'wait':
            event = events.get(row['event'])
            copy = event['pending'] if event else None
            require(copy and copy in frame['waits'] and not copy.get('waited'),
                    'wait return lacks matching invocation/generation/thread')
            copy['waited'] = seq
        elif kind == 'memset':
            owner = devices.get(row['address'])
            require(owner and owner in frame['initializing'] and not owner.get('zeroed')
                    and row['length'] == owner['length'] and row['value'] == 0,
                    'memset outside fresh allocation initialization')
            owner['zeroed'] = True
        elif kind == 'sync':
            require(frame['initializing'] and all(o.get('zeroed') and not o.get('synced') for o in frame['initializing']),
                    'context sync outside fresh allocation initialization')
            for owner in frame['initializing']: owner['synced'] = True
        else:
            require(False, 'unexpected native operation in staging profile')
    require(not pending and not hosts and not devices and not streams and not events
            and all(not stack for stack in stacks.values()), 'native owners or contexts remain after shutdown')
    return dict(copies=copies, pins=pins, handles=handles, peak_unconfirmed=peak)
