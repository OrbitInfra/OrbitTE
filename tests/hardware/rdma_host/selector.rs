//! Installed two-RNIC selector case. Both NICs are globally enabled in each run.
use orbit_te::{
    BatchReport, Error, IntentType, RemoteEffect, RemoteSegment, TransferEngine, TransferOp,
    TransferRequest, Transport, TransportSelectorConfig,
};

use super::{Case, Result};

pub(super) fn config(case: &Case) -> Result<TransportSelectorConfig> {
    let [read, write] = case.rails.as_slice() else {
        return Err("two locked rails required".into());
    };
    Ok(TransportSelectorConfig::from_json(&format!(
        r#"{{
      "transports":{{"rdma":{{"endpoint":{{"qp_pools":[
        {{"name":"fg","num_qp":1,"service_level":3,"traffic_class":40}},
        {{"name":"bg","num_qp":1,"service_level":5,"traffic_class":96}}]}}}}}},
      "policy":[
        {{"name":"read","segment_type":"memory","intent_type":"foreground_get","devices":["{read}"],"qp_pool":"fg","transports":["rdma"]}},
        {{"name":"write","segment_type":"memory","devices":["{write}"],"qp_pool":"bg","transports":["rdma"]}}]
    }}"#
    ))?)
}

pub(super) fn transfer(
    case: &Case,
    engine: &TransferEngine,
    peer: &RemoteSegment,
    op: TransferOp,
    mut request: TransferRequest,
) -> Result<BatchReport> {
    request.transport = Transport::Unspec;
    request.intent = IntentType::ForegroundGet;
    if !case.read {
        request = request.with_policy_name("write")?;
    }
    let half = request.length / 2;
    let mut tail = request.clone();
    tail.local_address += half as u64;
    tail.remote_address += half as u64;
    tail.length -= half;
    request.length = half;
    let aliases = [tail, request];
    let deadline = std::time::Instant::now();
    let expired = aliases.clone().map(|r| r.with_deadline(deadline));
    let before = engine
        .stats()?
        .rdma
        .ok_or("missing native counters")?
        .post_calls;
    let dropped = engine.submit_and_wait(op, peer, &expired, super::TIMEOUT)?;
    if !dropped.failure.as_ref().is_some_and(|f| {
        matches!(f.error, Error::DeadlineInfeasible)
            && f.request_index == 0
            && f.affected_requests == 2
            && f.remote_effect == RemoteEffect::None
    }) || dropped.transferred_bytes != 0
        || !dropped.locally_drained
        || engine
            .stats()?
            .rdma
            .ok_or("missing native counters")?
            .post_calls
            != before
    {
        return Err(
            format!("expired aliases crossed the native dispatch boundary: {dropped:?}").into(),
        );
    }
    // The one-owner cap rejects two independent requests. Both public pieces
    // must fit one physical transfer and satisfy the ordinary per-rail proof.
    let report = engine.submit_and_wait(op, peer, &aliases, super::TIMEOUT)?;
    if report.completed_requests != 2 {
        return Err(
            format!("merged selector transfer did not complete both aliases: {report:?}").into(),
        );
    }
    Ok(report)
}

pub(super) fn evidence(case: &Case, engine: &TransferEngine, round: usize) -> Result<()> {
    let index = usize::from(!case.read);
    let stats = engine.stats()?;
    let indices = stats
        .rdma
        .ok_or("missing RDMA stats")?
        .connected_pool_indices;
    if stats.unresolved_policy_devices != 0 || indices != 1 << index {
        return Err("selector resolved the wrong device or native QP index".into());
    }
    let nics = engine.nic_load_stats()?;
    if nics.len() != 2 {
        return Err("both physical NIC owners are required".into());
    }
    for name in &case.rails {
        let nic = nics
            .iter()
            .find(|n| &n.device_name == name)
            .ok_or("missing selected NIC")?;
        let bytes = nic.completed_bytes;
        let expected = if !case.source && name == &case.rails[index] {
            ((round + 1) * case.parents * case.payload) as u64
        } else {
            0
        };
        if bytes != expected {
            return Err(
                format!("unexpected native byte count on {name}: {bytes} != {expected}").into(),
            );
        }
        println!(
            "ORBITTE_SELECTOR {{\"round\":{round},\"nic\":{name:?},\"bytes\":{bytes},\"indices\":{indices}}}"
        );
    }
    Ok(())
}
