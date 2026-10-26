//! Public batches held before payload authorization, then exact-child death.

use std::sync::Arc;

use orbit_te::{Batch, BatchState, Error, RemoteEffect, TaskState};

use super::gate::GrantGate;
use super::*;

pub(super) fn pending(
    case: &Case,
    engine: &Arc<TransferEngine>,
    memory: &RegisteredMemory,
    peer: &RemoteSegment,
    stream: &mut TcpStream,
    gate: Option<&GrantGate>,
    phase: usize,
) -> Result<Vec<Batch>> {
    if let Some(gate) = gate {
        gate.arm()?;
    }
    let baseline = idle(engine)?;
    barrier(stream, "interruption-armed")?;
    let mut batches = Vec::new();
    if !case.source {
        for parent in 0..case.parents {
            let mut batch = engine.allocate_batch(1)?;
            batch.submit(
                if case.read {
                    TransferOp::Read
                } else {
                    TransferOp::Write
                },
                peer,
                &[TransferRequest {
                    transport: Transport::Rdma,
                    local_address: memory.base_address()
                        + (parent * case.bank()
                            + if case.read { case.destination() } else { GUARD })
                            as u64,
                    remote_address: peer.regions()[0].base_address()
                        + (parent * case.bank()
                            + if case.read { GUARD } else { case.destination() })
                            as u64,
                    length: case.payload,
                }],
                None,
                TIMEOUT,
            )?;
            batch.seal()?;
            batches.push(batch);
        }
    }
    if let Some(gate) = gate {
        let deadline = Instant::now() + Duration::from_secs(5);
        while gate.held()? != case.parents {
            if Instant::now() >= deadline {
                return Err(format!(
                    "not all grants reached the gate: {} / {}; {:?}",
                    gate.held()?,
                    case.parents,
                    engine.stats()?
                )
                .into());
            }
            thread::sleep(Duration::from_millis(1));
        }
    }
    barrier(stream, "interruption-held")?;
    let stats = engine.stats()?;
    let rdma = stats.rdma.ok_or("missing RDMA counters")?;
    let count = case.parents;
    if rdma.active_grants != count
        || rdma.persistent_mrs != 4
        || rdma.opened_connections != baseline.opened_connections
        || rdma.completed_read_bytes != baseline.completed_read_bytes
        || rdma.completed_write_bytes != baseline.completed_write_bytes
        || rdma.work_requests != 0
        || (case.source
            && (stats.active_source_operations != count
                || rdma.receive_work_requests != count
                || rdma.receive_credits != count
                || rdma.active_windows != count))
        || (!case.source
            && (stats.allocated_batches != count
                || stats.requester_workers != count
                || stats.inflight_batches != count
                || rdma.receive_work_requests != 0))
    {
        return Err(format!("interruption lacks outstanding owners: {stats:?}").into());
    }
    for batch in &mut batches {
        if batch.task_status(0)?.state != TaskState::Running
            || batch.status()?.locally_drained
            || !matches!(batch.wait(Duration::from_millis(1)), Err(Error::Timeout))
            || !matches!(batch.free(), Err(Error::BatchBusy))
        {
            return Err("pending batch was prematurely completed or freed".into());
        }
    }
    let offset = if case.source == case.read {
        GUARD
    } else {
        case.destination()
    };
    if !matches!(memory.write(offset, &[0]), Err(Error::Unavailable(_))) {
        return Err("pending DMA loan did not exclude CPU writes".into());
    }
    println!(
        "ORBITTE_INSTALLED_PENDING {{\"phase\":{phase},\"pid\":{},\"held_grants\":{count},\"active_grants\":{},\"sq_debt\":{},\"rq_debt\":{},\"windows\":{},\"batches\":{},\"opened_qps\":{},\"read_bytes\":{},\"write_bytes\":{},\"memory_busy\":true,\"batch_busy\":{}}}",
        std::process::id(),
        rdma.active_grants,
        rdma.work_requests,
        rdma.receive_work_requests,
        rdma.active_windows,
        batches.len(),
        rdma.opened_connections,
        rdma.completed_read_bytes,
        rdma.completed_write_bytes,
        !case.source
    );
    barrier(stream, "interruption-verified")?;
    Ok(batches)
}

pub(super) fn drained(
    case: &Case,
    engine: &TransferEngine,
    memory: &RegisteredMemory,
    mut batches: Vec<Batch>,
    gate: Option<&GrantGate>,
    phase: usize,
) -> Result<()> {
    if let Some(gate) = gate {
        gate.abort()?;
    }
    let failed = batches.len();
    for batch in &mut batches {
        let result = batch.wait(TIMEOUT)?;
        let task = batch.task_status(0)?;
        if result.state != BatchState::Failed
            || !result.locally_drained
            || result.transferred_bytes != 0
            || task.state != TaskState::Failed
            || !task.locally_drained
            || task
                .failure
                .as_ref()
                .is_none_or(|failure| failure.remote_effect != RemoteEffect::None)
        {
            return Err(format!(
                "interrupted batch did not drain with no effect: {result:?} {task:?}"
            )
            .into());
        }
        batch.free()?;
    }
    let rdma = idle(engine)?;
    let stats = engine.stats()?;
    if rdma.persistent_mrs != 4
        || rdma.opened_persistent_mrs != 4
        || rdma.retained_grants != 0
        || rdma.retained_endpoints != 0
        || stats.allocated_batches != 0
    {
        return Err(format!("interruption lost or retained owners: {stats:?}").into());
    }
    let mut expected = case.bytes(case.source, phase * ROUNDS + ROUNDS - 1);
    if case.source != case.read {
        for parent in 0..case.parents {
            for index in 0..case.payload {
                expected[parent * case.bank() + case.destination() + index] =
                    pattern(!case.source, phase * ROUNDS + ROUNDS - 1, parent, index);
            }
        }
    }
    let mut actual = vec![0; case.length()];
    memory.read(0, &mut actual)?;
    if actual != expected {
        return Err("interrupted payload changed backing bytes".into());
    }
    memory.write(0, &expected)?;
    println!(
        "ORBITTE_INSTALLED_DRAIN {{\"phase\":{phase},\"pid\":{},\"failed_batches\":{failed},\"sq_debt\":{},\"rq_debt\":{},\"grants\":{},\"windows\":{},\"batches\":{},\"persistent_mrs\":{},\"read_bytes\":{},\"write_bytes\":{},\"locally_drained\":true,\"bytes_unchanged\":true,\"memory_writable\":true}}",
        std::process::id(),
        rdma.work_requests,
        rdma.receive_work_requests,
        rdma.active_grants,
        rdma.active_windows,
        stats.allocated_batches,
        rdma.persistent_mrs,
        rdma.completed_read_bytes,
        rdma.completed_write_bytes
    );
    Ok(())
}
