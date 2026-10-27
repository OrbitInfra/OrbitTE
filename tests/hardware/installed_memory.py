"""Reconcile real MR/MW teardown errors with installed ownership and replacement."""
import json

from installed import PROTOCOL
from installed_restart import records


def one(record, suffix):
    rows = records(record, 'ORBITTE_INSTALLED_MEMORY_' + suffix + ' ')
    if len(rows) != 1:
        raise ValueError('missing or duplicate memory ' + suffix)
    return rows[0]


def verify(profile, case, evidence):
    resource = 1 if case['cleanup_kind'] == 'mr' else 2
    for role in ('source', 'requester'):
        record = evidence[role]
        target = role == case['fault_role']
        supervisor = json.loads(record['result.json'])
        pid = supervisor.get('native_pid')
        if (type(pid) is not int or pid <= 0 or supervisor.get('exit_code') != 0
                or not supervisor.get('child_reaped') or not supervisor.get('resource_locks_released')
                or supervisor.get('phases') or supervisor.get('timeout')):
            raise ValueError('memory cleanup peer did not reap its original child')
        maps = record.get('maps.txt') or ''
        if 'libibverbs' not in maps or 'libmlx5' not in maps or 'libcuda' in maps:
            raise ValueError('memory cleanup peer lacks its host providers')
        digest = supervisor.get('fault_library_sha256', '')
        if target:
            if (len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest)
                    or 'liborbitte_cq_fault.so' not in maps):
                raise ValueError('memory target lacks its pinned native observer')
        elif (digest or 'liborbitte_cq_fault.so' in maps
              or any(record.get('cq-fault-' + str(g) + '.jsonl') for g in range(3))):
            raise ValueError('memory fault escaped its declared process')
        nics = records(record, 'ORBITTE_INSTALLED_NIC ')
        for group in (nics[:2], nics[2:]):
            if len(group) != 2 or {r['name'] for r in group} != set(profile['rails']) or len({r['pci'] for r in group}) != 2:
                raise ValueError('memory fixture lacks both old and new locked contexts')
        owner, replaced, result = (one(record, name) for name in ('OWNER', 'REPLACED', 'RESULT'))
        if (any(r['pid'] != pid for r in (owner, replaced, result)) or owner['target'] is not target
                or type(owner['address']) is not int or owner['address'] <= 0
                or replaced['old_epoch'] == replaced['new_epoch'] or not replaced['old_epoch']
                or not replaced['new_epoch'] or not replaced['endpoint']
                or type(replaced['address']) is not int or replaced['address'] <= 0
                or (target and replaced['address'] == owner['address'])
                or replaced['same_process'] is not True or replaced['stale_epoch_before_qp'] is not True):
            raise ValueError('replacement reused stale process, epoch, allocation or QPs')
        expected = dict(protocol=PROTOCOL, pid=pid, target=target, resource=resource,
                        healthy_rounds=6, full_bytes_and_guards=True, fresh_engine_drained=True,
                        fresh_backing_released=True, old_backing_retained=target)
        if result != expected:
            raise ValueError('memory cleanup lacks exact final ownership and byte proof')
        rounds = records(record, 'ORBITTE_INSTALLED_ROUND ')
        if [(r['phase'], r['round']) for r in rounds] != [(p, n) for p in (0, 1) for n in range(3)]:
            raise ValueError('memory cleanup omitted old or replacement rounds')
        for index, row in enumerate(rounds):
            count = row['round'] + 1
            if (row['opened_qps'] != 1 or row['closed_qps'] != 0 or row['opened_mrs'] != 4
                    or row['completed_grants'] != count
                    or (row['round'] > 0 and row['reused_qps'] <= rounds[index - 1]['reused_qps'])):
                raise ValueError('memory cleanup violated persistent MR/QP reuse')
            for operation in ('read', 'write'):
                value = count * case['payload'] if role == 'requester' and case['operation'] == operation else 0
                if row[operation + '_bytes'] != value:
                    raise ValueError('memory cleanup byte ledger differs from declared workload')
        request = records(record, 'ORBITTE_INSTALLED_MEMORY_REQUEST ')
        if request != ([dict(pid=pid, failed=True, locally_drained=True)] if resource == 2 and role == 'requester' else []):
            raise ValueError('MW cleanup did not fail the requester with local drain')
        if not target:
            if records(record, 'ORBITTE_INSTALLED_MEMORY_RETAINED '):
                raise ValueError('healthy peer unexpectedly retained memory')
            continue
        proof = one(record, 'RETAINED')
        fixed = dict(pid=pid, resource=resource, attempts=1, code=5, unregistered=True,
                     close_attempts=2, locally_drained=False, fixture_owners_dropped=True,
                     backing_retained=True, memory_busy=resource == 2)
        if (any(type(proof.get(k)) is not type(v) or proof[k] != v for k, v in fixed.items())
                or proof['persistent_mrs'] < 1 or proof['persistent_mr_bytes'] < 2 * case['payload'] + 192):
            raise ValueError('failed native cleanup released backing or registration charges')
        if resource == 1:
            if (proof['retained_mrs'] != 1 or proof['retained_windows'] != 0
                    or proof['retained_grants'] != 0 or proof['sq_debt'] != 0 or proof['rq_debt'] != 0):
                raise ValueError('idle MR failure manufactured or lost completion debt')
        elif (proof['retained_windows'] != 1 or proof['retained_grants'] != 1
              or proof['direct_bytes'] != case['payload'] or proof['source_mrs'] < 1
              or proof['key_indices'] < 1 or proof['burned_keys'] < 1 or proof['retained_mrs'] != 0):
            raise ValueError('MW failure released its grant, key domain, parent MR or byte quota')
        rows = [json.loads(line) for line in (record.get('cq-fault-0.jsonl') or '').splitlines()]
        if not rows or len(rows) > 4096 or any(record.get('cq-fault-' + str(g) + '.jsonl') for g in (1, 2)):
            raise ValueError('missing, unbounded or extra native memory receipts')
        active = set()
        for index, row in enumerate(rows):
            if row['seq'] != index + 1 or row['pid'] != pid or row['phase'] != 0:
                raise ValueError('native memory sequence or process identity differs')
            if row['kind'] == 'context':
                if row['context'] in active or 'libmlx5' not in row['provider'] or row['provider'] not in maps:
                    raise ValueError('memory observer lacks its actual provider')
                active.add(row['context'])
            elif row['kind'] == 'close_context':
                if row['code'] != 0 or row['context'] not in active:
                    raise ValueError('memory context destruction failed unexpectedly')
                active.remove(row['context'])
            elif row['kind'] in ('memory_arm', 'memory_bind', 'memory_error', 'memory_retained', 'destroy_qp'):
                if row['context'] not in active or (row['kind'] == 'destroy_qp' and row['code'] != 0):
                    raise ValueError('memory operation escaped its live context')
            else:
                raise ValueError('unexpected native memory receipt')
        select = lambda kind: [r for r in rows if r['kind'] == kind]
        arms, errors, retained, binds = [select('memory_' + suffix) for suffix in ('arm', 'error', 'retained', 'bind')]
        if len(arms) != 1 or len(errors) != 1 or len(retained) != 2 or len(binds) != resource - 1:
            raise ValueError('missing native error, post-drop probe or unexpected retry')
        arm, error = arms[0], errors[0]
        if (not arm['seq'] < error['seq'] < retained[0]['seq'] < retained[1]['seq']
                or arm['resource'] != resource or arm['source'] != int(role == 'source')
                or arm['address'] != owner['address'] or error['address'] != owner['address']
                or error['length'] != 2 * case['payload'] + 192
                or error['code'] != 5 or error['attempts'] != 1 or error['syscall_fault'] is not True
                or error['lkey'] <= 0 or error['rkey'] <= 0 or error['context'] not in active
                or error['context'] != arm['context'] or proof['sequence'] != retained[0]['seq']):
            raise ValueError('native memory error does not match its retained allocation')
        for receipt in [proof, *retained]:
            if any(receipt[k] != error[k] for k in ('resource', 'mr', 'mw', 'pd')):
                raise ValueError('native memory owner changed after failed destruction')
        if error['mr'] <= 0 or error['pd'] <= 0 or (error['mw'] > 0) != (resource == 2):
            raise ValueError('native error lacks its actual MR/MW/PD')
        if any(r['code'] != 16 or r['keys_intact'] is not True or r['context'] != error['context'] for r in retained):
            raise ValueError('post-drop native PD no longer owns its dependencies')
        if binds and (not arm['seq'] < binds[0]['seq'] < error['seq']
                      or any(binds[0][k] != error[k] for k in ('context', 'mr', 'mw'))
                      or binds[0]['code'] != 0 or binds[0]['key'] != error['rkey']):
            raise ValueError('MW cleanup does not belong to the actual bound grant')
