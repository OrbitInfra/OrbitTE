"""Reconcile real dependency failures with payload debt and allocation lifetimes."""
from installed_restart import records


def verify(case, evidence, logs):
    kind = {'cq': 1, 'channel': 2, 'pd': 3}[case['cleanup_kind']]
    debt = kind != 3
    proofs = records(evidence['requester'], 'ORBITTE_INSTALLED_CLEANUP ')
    if len(proofs) != 1 or records(evidence['source'], 'ORBITTE_INSTALLED_CLEANUP '):
        raise ValueError('dependency proof missing or in the wrong process')
    rows = logs[0]
    select = lambda name: [r for r in rows if r['kind'] == name]
    arms, errors, queries = select('arm_dependency'), select('dependency_error'), select('dependency_query')
    if len(arms) != 1 or len(errors) != 1 or len(queries) != 2:
        raise ValueError('missing dependency or repeated native destructor')
    arm, error, proof = arms[0], errors[0], proofs[0]
    history = records(evidence['requester'], 'ORBITTE_INSTALLED_PHASE ')
    if (not arm['seq'] < error['seq'] < queries[0]['seq'] < queries[1]['seq']
            or proof['sequence'] != queries[1]['seq']
            or any(r['pid'] != history[0]['pid'] or r['phase'] != 0 for r in (arm, error, *queries, proof))
            or any(r['resource'] != kind for r in (arm, error, *queries, proof))
            or any(error[key] != arm[key] for key in ('context', 'pd', 'cq', 'channel', 'qpn'))
            or error['code'] != 16 or error['attempts'] != 1 or error['blocker'] == 0
            or proof['qpn'] != arm['qpn']
            or any(r['blocker'] != error['blocker'] or r['context'] != arm['context']
                   or r['code'] != (16 if kind == 3 else 0) or r['live'] is not True for r in queries)):
        raise ValueError('native dependency identity, EBUSY or post-drop probe disagrees')
    destroys = select('destroy_qp')
    if (len(destroys) != 1 or destroys[0]['code'] != 0
            or not arm['seq'] < destroys[0]['seq'] < error['seq']
            or any(destroys[0][key] != arm[key] for key in ('context', 'cq', 'qpn'))):
        raise ValueError('payload QP did not close before the dependency failed')
    releases = select('dependency_release')
    if any(r['context'] != arm['context'] or r['code'] != 0 for r in releases):
        raise ValueError('dependency releases escaped the selected context')
    if kind in (2, 3):
        cqs = [r for r in releases if r['owner'] == 1]
        if (len(cqs) != 2 or len({r['handle'] for r in cqs}) != 2
                or arm['cq'] not in {r['handle'] for r in cqs}
                or any(not destroys[0]['seq'] < r['seq'] < error['seq'] for r in cqs)):
            raise ValueError('engine CQs did not close before channel/PD failure')
        channels = [r for r in releases if r['owner'] == 2]
        if kind == 2 and channels:
            raise ValueError('failed completion channel was released')
        if kind == 3 and (len(channels) != int(case['rdma_events']) or
                         any(r['handle'] != arm['channel'] or not max(c['seq'] for c in cqs) < r['seq'] < error['seq']
                             for r in channels)):
            raise ValueError('completion channel did not close before PD failure')
    if debt:
        posts = select('post')
        pending = records(evidence['requester'], 'ORBITTE_INSTALLED_CQE_PENDING ')
        if (len(posts) != 1 or len(pending) != 1 or pending[0]['sequence'] >= arm['seq']
                or any(posts[0][key] != arm[key] for key in ('context', 'cq', 'qpn'))):
            raise ValueError('retained dependency does not belong to the lost payload')
    elif (select('arm') or select('post') or select('lost')
          or records(evidence['requester'], 'ORBITTE_INSTALLED_PENDING ')):
        raise ValueError('idle PD teardown acquired payload debt')
    expected = dict(attempts=1, code=16, retained_grants=int(debt), retained_endpoints=0,
                    sq_debt=int(debt), direct_bytes=case['payload'] * int(debt),
                    inflight_bytes=case['payload'] * int(debt), batches=int(debt),
                    unregistered=True, backing_retained=debt, post_drop_native_query=True,
                    fixture_owners_dropped=True, batch_busy=debt, memory_busy=debt,
                    close_attempts=2, locally_drained=False)
    if (any(type(proof.get(k)) is not type(v) or proof[k] != v for k, v in expected.items())
            or (proof['persistent_mrs'] > 0) != debt
            or (kind == 1 and proof['retained_cqs'] < 1)
            or (kind == 2 and (proof['retained_cqs'] != 0 or proof['retained_channels'] < 1))
            or (kind == 3 and (proof['retained_cqs'] != 0 or proof['retained_channels'] != 0
                              or proof['retained_devices'] < 1))):
        raise ValueError('dependency failure kept or released the wrong memory/budget owners')
    if any(r['kind'].startswith('dependency_') or r['kind'] == 'arm_dependency' for r in logs[1]):
        raise ValueError('dependency fault escaped into the replacement')
