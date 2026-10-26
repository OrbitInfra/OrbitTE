"""Reconcile real pending RQ debt with failed batches and surviving owners."""
from installed_restart import records


def verify(case, evidence):
    count = case['parents']
    for role in ('source', 'requester'):
        source = role == 'source'
        target = role == case['fault_role']
        record = evidence[role]
        pending = records(record, 'ORBITTE_INSTALLED_PENDING ')
        drains = records(record, 'ORBITTE_INSTALLED_DRAIN ')
        history = records(record, 'ORBITTE_INSTALLED_PHASE ')
        rounds = records(record, 'ORBITTE_INSTALLED_ROUND ')
        expected = list(range(case['generations'] - 1))
        if ([r['phase'] for r in pending] != expected
                or [r['phase'] for r in drains] != ([] if target else expected)):
            raise ValueError('missing exact pending or survivor drain phase')
        for phase, row in enumerate(pending):
            baseline = rounds[phase * 3 + 2]
            if (row['pid'] != history[phase]['pid'] or row['held_grants'] != count
                    or row['active_grants'] != count or row['sq_debt'] != 0
                    or row['rq_debt'] != (count if source else 0)
                    or row['windows'] != (count if source else 0)
                    or row['batches'] != (0 if source else count)
                    or row['progress_failures'] != row['progress_recoveries']
                    or row['memory_busy'] is not True or row['batch_busy'] is not (not source)
                    or any(row[key] != baseline[key] for key in ('opened_qps', 'read_bytes', 'write_bytes'))):
                raise ValueError('interruption did not hold real RQ and memory/batch owners before payload')
        for phase, row in enumerate(drains):
            if (row['pid'] != history[phase]['pid'] or row['persistent_mrs'] != 4
                    or row['progress_failures'] != row['progress_recoveries']
                    or row['progress_failures'] < pending[phase]['progress_failures']
                    or row['failed_batches'] != (0 if source else count)
                    or any(row[key] != 0 for key in ('sq_debt', 'rq_debt', 'grants', 'windows', 'batches'))
                    or any(row[key] is not True for key in ('locally_drained', 'bytes_unchanged', 'memory_writable'))
                    or any(row[key] != pending[phase][key] for key in ('read_bytes', 'write_bytes'))):
                raise ValueError('survivor did not release temporary owners with zero payload effect')
