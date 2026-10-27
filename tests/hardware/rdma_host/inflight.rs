//! Interrupt an accepted payload, then inspect bytes only after native drain.

use std::ffi::{c_char, c_void};

use orbit_te::{BatchState, RemoteEffect, TaskState};

use super::*;

#[derive(Debug, Default)]
#[repr(C)]
struct Snapshot {
    version: u64,
    armed: u64,
    posts: u64,
    modifications: u64,
    failed: u64,
    completed: u64,
    errors: u64,
    qpn: u64,
    wr_id: u64,
    destroyed: u64,
    sequence: u64,
}

type Command = unsafe extern "C" fn(u32, u32, *mut Snapshot) -> i32;
#[link(name = "dl")]
unsafe extern "C" {
    fn dlsym(handle: *mut c_void, name: *const c_char) -> *mut c_void;
}

struct InflightFault(Command);
impl InflightFault {
    fn arm(case: &Case) -> Result<Self> {
        // SAFETY: the supervisor pins the library exporting this exact ABI.
        let symbol = unsafe { dlsym(std::ptr::null_mut(), c"orbitte_inflight_fault".as_ptr()) };
        if symbol.is_null() {
            return Err("missing native in-flight fault control".into());
        }
        // SAFETY: inflight.c defines this signature and versioned output layout.
        let fault = Self(unsafe { std::mem::transmute::<*mut c_void, Command>(symbol) });
        let snapshot = fault.command(1, if case.read { 1 } else { 2 })?;
        if snapshot.armed != 1 || snapshot.posts != 0 {
            return Err(format!("invalid in-flight arm: {snapshot:?}").into());
        }
        Ok(fault)
    }

    fn command(&self, command: u32, op: u32) -> Result<Snapshot> {
        let mut snapshot = Snapshot::default();
        // SAFETY: snapshot is a live output with the exact C ABI layout.
        let code = unsafe { (self.0)(command, op, &mut snapshot) };
        if code != 0 || snapshot.version != 1 || snapshot.errors != 0 {
            return Err(format!("invalid native in-flight receipt ({code}): {snapshot:?}").into());
        }
        Ok(snapshot)
    }

    fn drained(&self, case: &Case) -> Result<()> {
        let native = self.command(2, 0)?;
        if native.armed != 0
            || native.posts != 1
            || native.modifications != 1
            || native.failed != 1
            || native.completed != 0
            || native.destroyed != 1
        {
            return Err(format!("in-flight payload lacks native drain: {native:?}").into());
        }
        println!(
            "ORBITTE_INSTALLED_DMA_NATIVE {{\"pid\":{},\"qpn\":{},\"wr_id\":{},\"sequence\":{},\"posts\":1,\"modifications\":1,\"failed\":1,\"completed\":0,\"destroyed\":1,\"write_effect_unknown\":{}}}",
            std::process::id(), native.qpn, native.wr_id, native.sequence, !case.read
        );
        Ok(())
    }
}

fn bytes(case: &Case, memory: &RegisteredMemory) -> Result<()> {
    let initial = case.bytes(case.source, ROUNDS);
    let mut actual = vec![0; case.length()];
    memory.read(0, &mut actual)?;
    let destination = case.source != case.read;
    let mut copied = 0;
    let mut unchanged = 0;
    let mut ambiguous = 0;
    for (index, (&value, &before)) in actual.iter().zip(&initial).enumerate() {
        if destination && (case.destination()..case.destination() + case.payload).contains(&index) {
            let expected = pattern(!case.source, ROUNDS, 0, index - case.destination());
            if expected == before && value == before {
                ambiguous += 1;
            } else if value == expected {
                copied += 1;
            } else if value == before {
                unchanged += 1;
            } else {
                return Err(format!("interrupted payload has unexpected byte at {index}").into());
            }
        } else if value != before {
            return Err(format!("interrupted DMA changed a guard/input byte at {index}").into());
        }
    }
    if destination && (copied == 0 || unchanged == 0) {
        return Err(format!(
            "actual partial DMA not demonstrated: copied={copied}, unchanged={unchanged}, ambiguous={ambiguous}"
        )
        .into());
    }
    memory.write(0, &initial)?;
    println!(
        "ORBITTE_INSTALLED_DMA_BYTES {{\"pid\":{},\"destination\":{destination},\"copied\":{copied},\"unchanged\":{unchanged},\"ambiguous\":{ambiguous},\"full_bytes_and_guards\":true,\"memory_writable\":true}}",
        std::process::id()
    );
    Ok(())
}

pub(super) fn run(case: &Case) -> Result<()> {
    if case.parents != 1
        || case.lanes != 1
        || !matches!(case.mask, 1 | 2)
        || case.payload != case.block
    {
        return Err("in-flight profile requires one parent, one QP and one payload WR".into());
    }
    let bind = env::var("ORBITTE_SHARED_BIND")?.parse()?;
    let (engine, memory) = create(case, bind)?;
    let mut stream = control(case.source, bind, None)?;
    let peer = open_peer(case, &engine, &memory, &mut stream, engine.endpoint())?;
    println!(
        "ORBITTE_INSTALLED_DMA_OWNER {{\"pid\":{},\"address\":{}}}",
        std::process::id(),
        memory.base_address()
    );
    rounds(case, &engine, &memory, &peer, &mut stream, 0)?;
    memory.write(0, &case.bytes(case.source, ROUNDS))?;
    let fault = if case.source {
        None
    } else {
        Some(InflightFault::arm(case)?)
    };
    barrier(&mut stream, "dma-armed")?;
    if !case.source {
        let mut batch = engine.allocate_batch(1)?;
        batch.submit(
            if case.read {
                TransferOp::Read
            } else {
                TransferOp::Write
            },
            &peer,
            &[TransferRequest {
                transport: Transport::Rdma,
                local_address: memory.base_address()
                    + if case.read { case.destination() } else { GUARD } as u64,
                remote_address: peer.regions()[0].base_address()
                    + if case.read { GUARD } else { case.destination() } as u64,
                length: case.payload,
            }],
            None,
            TIMEOUT,
        )?;
        batch.seal()?;
        let outcome = batch.wait(TIMEOUT)?;
        let task = batch.task_status(0)?;
        if outcome.state != BatchState::Failed
            || !outcome.locally_drained
            || outcome.transferred_bytes != 0
            || task.state != TaskState::Failed
            || !task.locally_drained
            || task.failure.as_ref().is_none_or(|failure| {
                failure.remote_effect
                    != if case.read {
                        RemoteEffect::None
                    } else {
                        RemoteEffect::Unknown
                    }
            })
        {
            return Err(
                format!("interrupted batch outcome is incorrect: {outcome:?} {task:?}").into(),
            );
        }
        batch.free()?;
    }
    // Both peers drain before either side reads or reinitializes payload bytes.
    let deadline = Instant::now() + TIMEOUT;
    let rdma = loop {
        let rdma = idle(&engine)?;
        if rdma.progress_failures == rdma.progress_recoveries {
            break rdma;
        }
        if Instant::now() >= deadline {
            return Err(format!("in-flight fault did not recover progress: {rdma:?}").into());
        }
        thread::sleep(Duration::from_millis(1));
    };
    let stats = engine.stats()?;
    if rdma.persistent_mrs != 4
        || rdma.opened_persistent_mrs != 4
        || rdma.retained_grants != 0
        || rdma.retained_endpoints != 0
        || stats.allocated_batches != 0
        || stats.inflight_bytes != 0
        || rdma.direct_bytes != 0
        || rdma.closed_connections == 0
        || rdma.completion_errors > 1
        || (!case.source && rdma.completion_errors != 1)
    {
        return Err(format!("in-flight fault lost or retained owners: {stats:?}").into());
    }
    if let Some(fault) = fault {
        fault.drained(case)?;
    }
    println!(
        "ORBITTE_INSTALLED_DMA_DRAIN {{\"pid\":{},\"sq_debt\":{},\"rq_debt\":{},\"grants\":{},\"windows\":{},\"batches\":{},\"direct_bytes\":{},\"inflight_bytes\":{},\"persistent_mrs\":{},\"closed_qps\":{},\"completion_errors\":{},\"progress_failures\":{},\"progress_recoveries\":{},\"locally_drained\":true}}",
        std::process::id(), rdma.work_requests, rdma.receive_work_requests,
        rdma.active_grants, rdma.active_windows, stats.allocated_batches,
        rdma.direct_bytes, stats.inflight_bytes, rdma.persistent_mrs, rdma.closed_connections,
        rdma.completion_errors, rdma.progress_failures, rdma.progress_recoveries
    );
    barrier(&mut stream, "dma-drained")?;
    bytes(case, &memory)?;
    barrier(&mut stream, "dma-bytes-verified")?;
    rounds(case, &engine, &memory, &peer, &mut stream, 1)?;
    maps()?;
    close(&engine, memory)?;
    barrier(&mut stream, "closed")?;
    result(case)
}
