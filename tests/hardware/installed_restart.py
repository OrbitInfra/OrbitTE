"""Reconcile live-engine restart evidence with exact supervised process lifetimes."""
import json

from installed import PROTOCOL


def records(record, prefix):
    return [json.loads(line[len(prefix):]) for line in record['stdout.log'].splitlines()
            if line.startswith(prefix)]


def verify(profile, case, evidence):
    lost = case['mode'] in ('installed-lost-completion', 'installed-cleanup-failure')
    interrupted = case['mode'] in ('installed-interruption', 'installed-lost-completion', 'installed-cleanup-failure')
    count, killed = case['generations'], case['fault_role']
    peer_phases = {}
    for role in ('source', 'requester'):
        record = evidence[role]
        target = role == killed
        supervisor = json.loads(record['result.json'])
        restart = json.loads(record['restart.json'])
        phases = supervisor.get('phases', [])
        expected_count = count if target else 1
        if (supervisor.get('status') != 'passed' or supervisor.get('exit_code') != 0
                or not supervisor.get('child_reaped') or not supervisor.get('resource_locks_released')
                or supervisor.get('timeout') or len(phases) != expected_count
                or restart.get('phases') != phases or restart.get('kill_role') != killed
                or restart.get('generations') != count or restart.get('live_peer') is not True
                or restart.get('locks_held') is not True
                or len({p['native_pid'] for p in phases}) != expected_count):
            raise ValueError('restart supervisor did not retain and reap the exact process sequence')
        for index, phase in enumerate(phases):
            intentional = target and index + 1 < count
            if (phase['generation'] != index or not phase['child_reaped']
                    or phase['exit_code'] != (-9 if intentional else 0)
                    or phase['intentional_sigkill'] != intentional):
                raise ValueError('unexpected restart child exit')
            maps = record.get('maps-' + str(index) + '.txt') or ''
            if 'libibverbs' not in maps or 'libmlx5' not in maps or 'libcuda' in maps:
                raise ValueError('missing native provider evidence for a process generation')
        nics = records(record, 'ORBITTE_INSTALLED_NIC ')
        if len(nics) != expected_count * 2:
            raise ValueError('missing installed RNIC inventory')
        for index in range(expected_count):
            pair = nics[index * 2:index * 2 + 2]
            if ({row['name'] for row in pair} != set(profile['rails'])
                    or len({row['pci'] for row in pair}) != 2):
                raise ValueError('installed process escaped locked RNICs')
        history = records(record, 'ORBITTE_INSTALLED_PHASE ')
        peer_phases[role] = history
        if len(history) != count or [r['phase'] for r in history] != list(range(count)):
            raise ValueError('missing or duplicate connection generation')
        identity = lambda row: (row['pid'], row['endpoint'], row['epoch'], row['region_id'], row['address'])
        if target:
            if (len({row['epoch'] for row in history}) != count
                    or len({row['endpoint'] for row in history}) != 1
                    or len({row['region_id'] for row in history}) != 1):
                raise ValueError('replacement did not reuse its address with fresh authority')
        elif len({identity(row) for row in history}) != 1:
            raise ValueError('survivor replaced its engine, process or registration')
        rounds = records(record, 'ORBITTE_INSTALLED_ROUND ')
        if [(row['phase'], row['round']) for row in rounds] != [
                (phase, index) for phase in range(count) for index in range(3)]:
            raise ValueError('incomplete installed restart rounds')
        last = None
        for phase, start in enumerate(history):
            if (start['pid'] != phases[phase if target else 0]['native_pid']
                    or start['stale_bytes_unchanged'] is not True):
                raise ValueError('public engine identity or stale-byte proof disagrees')
            for key in ('opened_qps', 'read_bytes', 'write_bytes'):
                expected = last[key] if last is not None and not target else 0
                if start[key] != expected:
                    raise ValueError('surviving counters reset or replacement inherited counters')
            if start['opened_mrs'] != 4:
                raise ValueError('persistent MR publication was lost across recovery')
            previous = last if not target else None
            for index, row in enumerate(rounds[phase * 3:phase * 3 + 3]):
                if (not 1 <= row['opened_qps'] - start['opened_qps'] <= case['mask'].bit_count() * case['num_lanes']
                        or not 0 <= row['opened_qps'] - row['closed_qps'] <= 2 * case['num_lanes']
                        or row['opened_mrs'] != 4
                        or row['completed_grants'] <= (previous['completed_grants'] if previous else 0)
                        or (index and row['reused_qps'] <= previous['reused_qps'])):
                    raise ValueError('restart QP/MR or reusable grant bound failed')
                for operation in ('read', 'write'):
                    key = operation + '_bytes'
                    expected = ((index + 1) * case['parents'] * case['payload']
                                if role == 'requester' and case['operation'] == operation else 0)
                    if row[key] != start[key] + expected:
                        raise ValueError('restarted native byte ledger disagrees')
                previous = row
            last = previous
        kills = records(record, 'ORBITTE_INSTALLED_KILL ')
        if len(kills) != (count - 1 if target else 0):
            raise ValueError('missing exact-child kill point')
        for phase, row in enumerate(kills):
            if (row['phase'] != phase or row['pid'] != phases[phase]['native_pid']
                    or not 1 <= row['cached_qps'] <= 2 * case['num_lanes']
                    or (row['persistent_mrs'] < 1 if case['mode'] == 'installed-cleanup-failure' else row['persistent_mrs'] != 4) or row['sq_debt'] != (case['parents'] if lost and role == 'requester' else 0) or row['rq_debt'] != (case['parents'] if interrupted and role == 'source' else 0)
                    or row['orderly_close'] is not False):
                raise ValueError('kill point did not retain idle native resources')
        exits = records(record, 'ORBITTE_INSTALLED_PEER_EXIT ')
        expected_exits = [] if target else [dict(phase=g, survivor_pid=phases[0]['native_pid'], locally_idle=True)
                                          for g in range(count - 1)]
        if exits != expected_exits:
            raise ValueError('surviving engine did not observe each peer death')
        stale = records(record, 'ORBITTE_INSTALLED_STALE ')
        expected_stale = [] if target else [(g, old) for g in range(1, count) for old in range(g)]
        if [(r['phase'], r['previous']) for r in stale] != expected_stale:
            raise ValueError('an old authority generation was not rejected')
        for row in stale:
            if (row['opened_qps'] != 0 or row['posted_wrs'] != 0
                    or row['remote_effect_none'] is not True or row['locally_drained'] is not True
                    or row['old_epoch'] != history[row['previous']]['peer_epoch']
                    or row['new_epoch'] != history[row['phase']]['peer_epoch']):
                raise ValueError('old authority crossed the transport fence')
        if records(record, 'ORBITTE_INSTALLED_RESULT ') != [dict(
                protocol=PROTOCOL, status='passed', role=role, operation=case['operation'],
                parents=case['parents'], payload=case['payload'], rounds=3,
                full_bytes_and_guards=True, locally_drained=True)]:
            raise ValueError('missing final installed close and byte proof')
    for source, requester in zip(peer_phases['source'], peer_phases['requester']):
        for local, remote in ((source, requester), (requester, source)):
            if (local['peer_epoch'] != remote['epoch'] or local['peer_endpoint'] != remote['endpoint']):
                raise ValueError('peer connection identities disagree')

    if interrupted:
        from installed_interruption import verify as verify_interruption
        verify_interruption(case, evidence)

    if lost:
        from installed_completion import verify as verify_completion
        verify_completion(case, evidence)
