//! Installed two-RNIC selector case. Both NICs are globally enabled in each run.
use orbit_te::{IntentType, TransferEngine, TransferRequest, Transport, TransportSelectorConfig};

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

pub(super) fn request(case: &Case, mut request: TransferRequest) -> Result<TransferRequest> {
    request.transport = Transport::Unspec;
    request.intent = IntentType::ForegroundGet;
    if !case.read {
        request = request.with_policy_name("write")?;
    }
    Ok(request)
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
