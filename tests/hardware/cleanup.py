"""Reject incomplete native QP/channel failure and restart evidence."""
import json


MODES = {'large-qp-cleanup-error': ('qp', 5), 'large-channel-busy': ('channel', 16)}


def records(evidence, prefix):
    return [json.loads(line[len(prefix):]) for line in evidence['stdout.log'].splitlines()
            if line.startswith(prefix)]


def verify(case, evidence):
    kind, code = MODES[case['mode']]
    target = case['fault_role']
    for role in ('source', 'requester'):
        item = evidence[role]
        supervisor = json.loads(item['result.json'])
        phases = supervisor.get('phases', [])
        if (supervisor.get('status') != 'passed' or not supervisor.get('child_reaped')
                or not supervisor.get('resource_locks_released') or len(phases) != 2
                or [p.get('generation') for p in phases] != [0, 1]
                or len({p.get('native_pid') for p in phases}) != 2):
            raise ValueError('cleanup peers must reap two distinct process generations')
        for index, phase in enumerate(phases):
            killed = role == target and index == 0
            if (not phase.get('child_reaped') or phase.get('intentional_sigkill') != killed
                    or phase.get('exit_code') != (-9 if killed else 0)):
                raise ValueError('cleanup supervisor did not own the exact faulted child')
        if not all(name in (item.get('maps.txt') or '') for name in ('libibverbs', 'libmlx5')):
            raise ValueError('native provider maps are missing')
        errors = records(item, 'ORBITTE_NATIVE_CLEANUP_ERROR ')
        proofs = records(item, 'ORBITTE_CLEANUP_PROOF ')
        retained = records(item, 'ORBITTE_NATIVE_RETENTION ')
        if role == target:
            if not len(errors) == len(proofs) == len(retained) == 1:
                raise ValueError('missing or duplicated native cleanup receipt')
            error, proof, owner = errors[0], proofs[0], retained[0]
            if any(row.get('kind') != kind for row in (error, proof, owner)):
                raise ValueError('cleanup receipt belongs to another owner')
            if ((error.get('code'), error.get('attempts'), proof.get('code'), proof.get('attempts'),
                 owner.get('native_error'), owner.get('native_attempts')) != (code, 1, code, 1, code, 1)
                    or proof.get('syscall_fault_injection') != (kind == 'qp')):
                raise ValueError('cleanup failure code, mechanism or one-attempt contract changed')
            if (not proof.get('backing_retained') or not proof.get('native_probe_after_drop')
                    or not owner.get('backing_retained') or not owner.get('observer_owners_removed')
                    or not owner.get('post_drop_native_blocker_alive') or owner.get('drained') is not False
                    or owner.get('retained_grants', 0) <= 0
                    or owner.get('sq_debt', 0) + owner.get('rq_debt', 0) <= 0):
                raise ValueError('cleanup failure lost native debt or its backing owner')
            if (kind == 'qp' and proof.get('retained_endpoints', 0) <= 0
                    or kind == 'channel' and (proof.get('retained_channels', 0) <= 0
                                             or proof.get('retained_endpoints') != 0)):
                raise ValueError('cleanup proof retained the wrong resource boundary')
        elif errors or proofs or retained:
            raise ValueError('cleanup fault escaped the selected process')
        final = [row for row in records(item, 'ORBITTE_RESTART_RESULT ') if row.get('generation') == 1]
        healthy = [row for row in records(item, 'ORBITTE_CONCURRENT_RESULT ') if row.get('mode') == 'large-quota']
        if (len(final) != 1 or not final[0].get('drained')
                or len(healthy) != 1 or healthy[0].get('rounds') != 2
                or not healthy[0].get('all_resources_drained') or not healthy[0].get('allocation_released')):
            raise ValueError('replacement generation did not transfer and drain')
    stale = records(evidence['requester'], 'ORBITTE_RESTART_STALE ')
    if (len(stale) != 1 or stale[0].get('opened_qps') != 0
            or not stale[0].get('remote_effect_none') or not stale[0].get('same_endpoint')
            or stale[0].get('old_epoch') == stale[0].get('new_epoch')):
        raise ValueError('replacement did not reject old authority before opening QPs')
