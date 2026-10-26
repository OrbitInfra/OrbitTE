"""The hardware gate must fail when native ownership evidence is incomplete."""
import copy
import json
import unittest

import cleanup


def evidence(kind, code, target):
    result = {}
    for role in ('source', 'requester'):
        rows = [('ORBITTE_RESTART_RESULT ', dict(generation=1, drained=True)),
                ('ORBITTE_CONCURRENT_RESULT ', dict(mode='large-quota', rounds=2,
                                                   all_resources_drained=True, allocation_released=True))]
        if role == target:
            rows += [('ORBITTE_NATIVE_CLEANUP_ERROR ', dict(kind=kind, code=code, attempts=1)),
                     ('ORBITTE_CLEANUP_PROOF ', dict(kind=kind, code=code, attempts=1,
                        backing_retained=True, native_probe_after_drop=True, syscall_fault_injection=kind == 'qp',
                        retained_endpoints=int(kind == 'qp'), retained_channels=int(kind == 'channel'))),
                     ('ORBITTE_NATIVE_RETENTION ', dict(kind=kind, native_error=code, native_attempts=1,
                        backing_retained=True, observer_owners_removed=True, post_drop_native_blocker_alive=True,
                        drained=False, retained_grants=1, sq_debt=1, rq_debt=0))]
        if role == 'requester':
            rows += [('ORBITTE_RESTART_STALE ', dict(opened_qps=0, remote_effect_none=True,
                        same_endpoint=True, old_epoch='old', new_epoch='new'))]
        result[role] = {'stdout.log': ''.join(prefix + json.dumps(row) + '\n' for prefix, row in rows),
                        'maps.txt': 'libibverbs libmlx5',
                        'result.json': json.dumps(dict(status='passed', child_reaped=True,
                            resource_locks_released=True, phases=[dict(generation=i, native_pid=10 + i,
                                child_reaped=True, intentional_sigkill=role == target and i == 0,
                                exit_code=-9 if role == target and i == 0 else 0) for i in range(2)]))}
    return result


class CleanupEvidence(unittest.TestCase):
    def test_missing_wrong_retried_or_unowned_receipts_fail(self):
        for mode, (kind, code) in cleanup.MODES.items():
            for target in ('source', 'requester'):
                case = dict(mode=mode, fault_role=target)
                valid = evidence(kind, code, target)
                cleanup.verify(case, valid)
                mutations = [
                    ('ORBITTE_CLEANUP_PROOF ', None, None),
                    ('ORBITTE_NATIVE_CLEANUP_ERROR ', 'code', 0),
                    ('ORBITTE_NATIVE_CLEANUP_ERROR ', 'attempts', 2),
                    ('ORBITTE_CLEANUP_PROOF ', 'backing_retained', False),
                    ('ORBITTE_CLEANUP_PROOF ', 'native_probe_after_drop', False),
                    ('ORBITTE_NATIVE_RETENTION ', 'sq_debt', 0),
                    ('ORBITTE_NATIVE_RETENTION ', 'observer_owners_removed', False),
                    ('ORBITTE_NATIVE_RETENTION ', 'drained', True),
                    ('ORBITTE_CONCURRENT_RESULT ', 'rounds', 0),
                ]
                for prefix, field, value in mutations:
                    bad = copy.deepcopy(valid)
                    rows = []
                    for line in bad[target]['stdout.log'].splitlines():
                        if line.startswith(prefix):
                            if field is None:
                                continue
                            data = json.loads(line[len(prefix):])
                            data[field] = value
                            line = prefix + json.dumps(data)
                        rows.append(line)
                    bad[target]['stdout.log'] = '\n'.join(rows)
                    with self.subTest(mode=mode, target=target, field=field), self.assertRaises(ValueError):
                        cleanup.verify(case, bad)
                for field in ('child_reaped', 'resource_locks_released'):
                    bad = copy.deepcopy(valid)
                    data = json.loads(bad[target]['result.json'])
                    data[field] = False
                    bad[target]['result.json'] = json.dumps(data)
                    with self.subTest(field=field), self.assertRaises(ValueError):
                        cleanup.verify(case, bad)
