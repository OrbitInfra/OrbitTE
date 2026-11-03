//! Each installed process owns its NIC leases and a receiver-issued window.
//! Both processes attach to the same priority object before either sends.
use super::*;
use orbit_te::{
    EngineConfig, Permission, Priority, ReceiverCreditConfig, transport::rdma::RdmaConfig,
};
use std::{fs, path::PathBuf};

pub(super) fn run() -> Result<()> {
    let nic = env::var("ORBITTE_QUOTA_NIC")?;
    let slots = env::var("ORBITTE_QUOTA_SLOTS")?;
    let rendezvous = PathBuf::from(env::var("ORBITTE_QUOTA_READY_DIR")?);
    let id: usize = env::var("ORBITTE_QUOTA_ID")?.parse()?;
    if id > 1 {
        return Err("invalid quota participant".into());
    }
    let rdma = RdmaConfig {
        shared_quota_shm_path: Some(slots),
        ..RdmaConfig::new(nic.clone())
    };
    let config = EngineConfig {
        rdma: Some(rdma),
        ..Default::default()
    };
    let source = TransferEngine::new(EngineConfig {
        receiver_credit: Some(ReceiverCreditConfig {
            bytes_per_sender: 8,
            requests_per_sender: 1,
            ..Default::default()
        }),
        ..config.clone()
    })?;
    let requester = TransferEngine::new(EngineConfig {
        max_receiver_credit_sessions: 1,
        ..config
    })?;
    let remote = source.register_memory(vec![0xab; 10].into(), Permission::RemoteReadWrite)?;
    let local = requester.register_memory(vec![0xef; 10].into(), Permission::LocalOnly)?;
    let peer = requester.open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)?;
    requester.connect_receiver_credit(&peer, 1, TIMEOUT)?;
    fs::write(rendezvous.join(id.to_string()), b"ready")?;
    let until = Instant::now() + TIMEOUT;
    while !rendezvous.join((1 - id).to_string()).try_exists()? {
        if Instant::now() >= until {
            return Err("other quota participant never became ready".into());
        }
        thread::sleep(Duration::from_millis(5));
    }
    let request = TransferRequest::new(
        Transport::Rdma,
        local.base_address() + 1,
        remote.base_address() + 1,
        8,
    )
    .with_priority(Priority::Low);
    for op in [TransferOp::Read, TransferOp::Write] {
        local.write(0, &[0xef; 10])?;
        remote.write(0, &[0xab; 10])?;
        let report =
            requester.submit_and_wait(op, &peer, std::slice::from_ref(&request), TIMEOUT)?;
        if !report.is_complete() || !report.locally_drained || report.transferred_bytes != 8 {
            return Err(format!("native quota transfer failed: {report:?}").into());
        }
        let (dst, guard, value) = if op == TransferOp::Read {
            (&local, 0xef, 0xab)
        } else {
            (&remote, 0xab, 0xef)
        };
        let mut actual = [0; 10];
        dst.read(0, &mut actual)?;
        if actual[0] != guard || actual[9] != guard || actual[1..9] != [value; 8] {
            return Err("RDMA payload or guards changed".into());
        }
    }
    let nics = requester.nic_load_stats()?;
    let rail = nics
        .iter()
        .find(|n| n.device_name == nic)
        .ok_or("configured RNIC missing")?;
    let credit = requester
        .receiver_credit_stats(&peer)
        .ok_or("sender window missing")?;
    if rail.inflight_bytes != 0
        || rail.allocated_bytes != 16
        || rail.completed_bytes != 16
        || (
            credit.consumed_bytes,
            credit.completed_bytes,
            credit.completed_requests,
        ) != (16, 16, 2)
    {
        return Err("NIC allocation, byte receipts or completed credit disagree".into());
    }
    requester.disconnect_receiver_credit(&peer, TIMEOUT)?;
    let issuer = source
        .receiver_credit_issuer_stats()
        .ok_or("issuer missing")?;
    if issuer.active_senders != 0 || issuer.reserved_bytes != 0 || issuer.retired_senders != 1 {
        return Err("receiver window did not retire".into());
    }
    println!(
        "quota participant {id}: NIC {nic}, 16 allocated and completed bytes, two credited owners, zero outstanding"
    );
    requester.close(TIMEOUT)?;
    source.close(TIMEOUT)?;
    Ok(())
}
