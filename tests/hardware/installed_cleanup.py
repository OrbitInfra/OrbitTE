"""Reconcile one retained native QP with public backing and budget ownership."""
from installed_restart import records


def verify(case, evidence, logs):
    if (case['fault_role'], case['generations'], case['parents']) != ('requester', 2, 1):
        raise ValueError('cleanup proof escaped the frozen single-QP profile')
    proofs = records(evidence['requester'], 'ORBITTE_INSTALLED_CLEANUP ')
    if len(proofs) != 1 or records(evidence['source'], 'ORBITTE_INSTALLED_CLEANUP '):
        raise ValueError('cleanup proof missing, duplicated or in the wrong peer')
    rows = logs[0]
    select = lambda kind: [r for r in rows if r['kind'] == kind]
    posts, arms = select('post'), select('arm_cleanup')
    errors, destroys, queries = select('cleanup_error'), select('destroy_qp'), select('query_cleanup')
    if not len(posts) == len(arms) == len(errors) == len(destroys) == 1 or len(queries) != 2:
        raise ValueError('missing native cleanup call or unexpected retry')
    post, arm, error, destroy = posts[0], arms[0], errors[0], destroys[0]
    proof = proofs[0]
    pending = records(evidence['requester'], 'ORBITTE_INSTALLED_CQE_PENDING ')[0]
    if (not pending['sequence'] < arm['seq'] < destroy['seq'] < error['seq'] < queries[0]['seq'] < queries[1]['seq']
            or proof['sequence'] != queries[1]['seq']
            or any(r['phase'] != 0 or r['pid'] != pending['pid'] or r['qpn'] != post['qpn']
                   for r in [arm, error, destroy, *queries, proof])):
        raise ValueError('cleanup call, post-drop query and retained QP identity disagree')
    for row in (destroy, error):
        if any(row[k] != post[k] for k in ('context', 'cq', 'qpn')) or row['code'] != 5:
            raise ValueError('native EIO did not belong to the payload QP')
    if (error['attempts'] != 1 or error['syscall_fault'] is not True
            or any(r['code'] != 0 or type(r['state']) is not int or not 0 <= r['state'] <= 6 for r in queries)):
        raise ValueError('cleanup must fail once and retain a queryable native QP')
    # close fences admission and calls destroy directly; ERR is not a prerequisite.
    expected = dict(attempts=1, code=5, retained_grants=1, retained_endpoints=1,
                    sq_debt=1, direct_bytes=case['payload'], inflight_bytes=case['payload'], batches=1,
                    unregistered=True, backing_retained=True, post_drop_native_query=True,
                    fixture_owners_dropped=True, batch_busy=True, memory_busy=True,
                    close_attempts=2, locally_drained=False)
    if any(type(proof.get(k)) is not type(v) or proof[k] != v for k, v in expected.items()):
        raise ValueError('cleanup falsely released actual backing, quotas or completion debt')
    if any(r['kind'] in ('arm_cleanup', 'cleanup_error', 'query_cleanup') for r in logs[1]):
        raise ValueError('cleanup fault escaped into the replacement process')
