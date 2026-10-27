//! Installed MR/MW retention and same-process engine replacement.

use orbit_te::{Error, RemoteEffect};
use std::ffi::{CString, c_char, c_void};

use super::*;

#[derive(Debug, Default)]
#[repr(C)]
struct Snapshot {
    version: u64,
    armed: u64,
    kind: u64,
    attempts: u64,
    code: u64,
    mr: u64,
    mw: u64,
    pd: u64,
    lkey: u64,
    rkey: u64,
    sequence: u64,
}
type Command = unsafe extern "C" fn(u32, u32, u32, u64, *const c_char, *mut Snapshot) -> i32;
#[link(name = "dl")]
unsafe extern "C" {
    fn dlsym(handle: *mut c_void, name: *const c_char) -> *mut c_void;
}
struct MemoryFault(Command);
impl MemoryFault {
    fn arm(case: &Case, memory: &RegisteredMemory, window: bool) -> Result<Self> {
        // SAFETY: the supervisor pins the observer exporting this versioned ABI.
        let symbol = unsafe { dlsym(std::ptr::null_mut(), c"orbitte_memory_fault".as_ptr()) };
        if symbol.is_null() {
            return Err("missing native memory cleanup control".into());
        }
        // SAFETY: memory.c defines this exact signature and output layout.
        let fault = Self(unsafe { std::mem::transmute::<*mut c_void, Command>(symbol) });
        let rail = CString::new(case.rails[usize::from(case.mask == 2)].as_str())?;
        let mut snapshot = Snapshot::default();
        // SAFETY: all pointers are live for this synchronous call; C copies their values.
        let code = unsafe {
            (fault.0)(
                1,
                if window { 2 } else { 1 },
                u32::from(case.source),
                memory.base_address(),
                rail.as_ptr(),
                &mut snapshot,
            )
        };
        if code != 0 || snapshot.version != 1 || snapshot.armed != 1 || snapshot.attempts != 0 {
            return Err(format!("native memory arm failed ({code}): {snapshot:?}").into());
        }
        Ok(fault)
    }
    fn retained(&self) -> Result<Snapshot> {
        let mut snapshot = Snapshot::default();
        // SAFETY: command 2 ignores input selectors and writes one ABI-sized snapshot.
        let code = unsafe { (self.0)(2, 0, 0, 0, std::ptr::null(), &mut snapshot) };
        if code != 0 || snapshot.version != 1 || snapshot.attempts != 1 || snapshot.code != 5 {
            return Err(format!("native memory retention failed ({code}): {snapshot:?}").into());
        }
        Ok(snapshot)
    }
}

fn fail_window(
    case: &Case,
    engine: &TransferEngine,
    memory: &RegisteredMemory,
    peer: &RemoteSegment,
    stream: &mut TcpStream,
) -> Result<()> {
    memory.write(0, &case.bytes(case.source, ROUNDS))?;
    barrier(stream, "memory-fault-armed")?;
    if !case.source {
        let report = engine.submit_and_wait(
            if case.read {
                TransferOp::Read
            } else {
                TransferOp::Write
            },
            peer,
            &[TransferRequest {
                transport: Transport::Rdma,
                local_address: memory.base_address()
                    + if case.read { case.destination() } else { GUARD } as u64,
                remote_address: peer.regions()[0].base_address()
                    + if case.read { GUARD } else { case.destination() } as u64,
                length: case.payload,
            }],
            TIMEOUT,
        )?;
        if report.is_complete() || !report.locally_drained || report.failure.is_none() {
            return Err(format!("MW cleanup reported false completion: {report:?}").into());
        }
        println!(
            "ORBITTE_INSTALLED_MEMORY_REQUEST {{\"pid\":{},\"failed\":true,\"locally_drained\":true}}",
            std::process::id()
        );
    }
    barrier(stream, "memory-requester-failed")
}

fn retain(
    case: &Case,
    engine: TransferEngine,
    memory: RegisteredMemory,
    backing: &std::sync::Weak<cleanup::Backing>,
    fault: &MemoryFault,
    window: bool,
) -> Result<()> {
    if window {
        let deadline = Instant::now() + TIMEOUT;
        while engine
            .stats()?
            .rdma
            .ok_or("missing RDMA stats")?
            .retained_windows
            != 1
        {
            if Instant::now() >= deadline {
                return Err("MW owner did not enter quarantine".into());
            }
            thread::sleep(Duration::from_millis(1));
        }
        let offset = if case.read { GUARD } else { case.destination() };
        if !matches!(memory.write(offset, &[0]), Err(Error::Unavailable(_))) {
            return Err("failed MW cleanup released its actual DMA loan".into());
        }
        engine.unregister_memory(&memory)?;
    } else if !matches!(
        engine.unregister_memory(&memory),
        Err(Error::Unquiesced {
            operation: "ibv_dereg_mr",
            code: 5
        })
    ) {
        return Err("MR retirement did not report native EIO".into());
    }
    for _ in 0..2 {
        if !matches!(
            engine.close(Duration::from_millis(50)),
            Err(Error::Timeout | Error::Unquiesced { .. })
        ) {
            return Err("failed memory cleanup falsely drained engine".into());
        }
    }
    let stats = engine.stats()?;
    let rdma = stats.rdma.ok_or("missing RDMA stats")?;
    if rdma.persistent_mrs == 0
        || rdma.persistent_mr_bytes < case.length()
        || if window {
            rdma.retained_windows != 1
                || rdma.retained_grants != 1
                || rdma.direct_bytes != case.payload
                || rdma.persistent_source_mrs == 0
                || rdma.window_key_indices == 0
                || rdma.burned_window_keys == 0
        } else {
            rdma.retained_persistent_mrs != 1
                || rdma.active_grants != 0
                || rdma.work_requests != 0
                || rdma.receive_work_requests != 0
        }
    {
        return Err(format!("native memory failure lost its owners: {stats:?}").into());
    }
    drop(memory);
    drop(engine);
    if backing.upgrade().is_none() {
        return Err("native memory owner released actual backing".into());
    }
    let native = fault.retained()?;
    println!(
        "ORBITTE_INSTALLED_MEMORY_RETAINED {{\"pid\":{},\"resource\":{},\"sequence\":{},\"mr\":{},\"mw\":{},\"pd\":{},\"attempts\":1,\"code\":5,\"persistent_mrs\":{},\"persistent_mr_bytes\":{},\"retained_mrs\":{},\"source_mrs\":{},\"retained_windows\":{},\"retained_grants\":{},\"key_indices\":{},\"burned_keys\":{},\"sq_debt\":{},\"rq_debt\":{},\"direct_bytes\":{},\"memory_busy\":{window},\"unregistered\":true,\"close_attempts\":2,\"locally_drained\":false,\"fixture_owners_dropped\":true,\"backing_retained\":true}}",
        std::process::id(),
        native.kind,
        native.sequence,
        native.mr,
        native.mw,
        native.pd,
        rdma.persistent_mrs,
        rdma.persistent_mr_bytes,
        rdma.retained_persistent_mrs,
        rdma.persistent_source_mrs,
        rdma.retained_windows,
        rdma.retained_grants,
        rdma.window_key_indices,
        rdma.burned_window_keys,
        rdma.work_requests,
        rdma.receive_work_requests,
        rdma.direct_bytes,
    );
    Ok(())
}

pub(super) fn run(case: &Case) -> Result<()> {
    let window = match env::var("ORBITTE_CLEANUP_KIND")?.as_str() {
        "mr" => false,
        "mw" => true,
        _ => return Err("unsupported memory owner".into()),
    };
    let fault_source = match env::var("ORBITTE_CLEANUP_ROLE")?.as_str() {
        "source" => true,
        "requester" => false,
        _ => return Err("invalid memory fault role".into()),
    };
    if case.parents != 1
        || case.lanes != 1
        || !matches!(case.mask, 1 | 2)
        || case.payload > case.block
        || (window && !fault_source)
    {
        return Err("memory cleanup requires one QP; only source grants own MWs".into());
    }
    let target = case.source == fault_source;
    let bind = env::var("ORBITTE_SHARED_BIND")?.parse()?;
    let (engine, memory, backing) = cleanup::create(case, bind)?;
    let endpoint = engine.endpoint();
    let epoch = engine.source_epoch();
    let address = memory.base_address();
    let mut stream = control(case.source, bind, None)?;
    let peer = open_peer(case, &engine, &memory, &mut stream, endpoint)?;
    println!(
        "ORBITTE_INSTALLED_MEMORY_OWNER {{\"pid\":{},\"address\":{address},\"target\":{target}}}",
        std::process::id()
    );
    rounds(case, &engine, &memory, &peer, &mut stream, 0)?;
    let fault = if target {
        Some(MemoryFault::arm(case, &memory, window)?)
    } else {
        None
    };
    if window {
        fail_window(case, &engine, &memory, &peer, &mut stream)?;
    }
    barrier(&mut stream, "memory-retire")?;
    if let Some(fault) = &fault {
        retain(case, engine, memory, &backing, fault, window)?;
    } else {
        idle(&engine)?;
        close(&engine, memory)?;
        drop(engine);
        if backing.upgrade().is_some() {
            return Err("healthy old engine retained backing".into());
        }
    }
    barrier(&mut stream, "memory-retired")?;
    let (engine, memory, fresh_backing) = cleanup::create(case, endpoint)?;
    if engine.endpoint() != endpoint
        || engine.source_epoch() == epoch
        || memory.base_address() == address && target
    {
        return Err("replacement reused a quarantined engine identity or allocation".into());
    }
    barrier(&mut stream, "memory-replaced")?;
    if !matches!(
        engine.open_segment(peer.endpoint(), peer.source_epoch(), TIMEOUT),
        Err(Error::EpochMismatch)
    ) {
        return Err("replacement accepted stale peer epoch".into());
    }
    if !case.source {
        let report = engine.submit_and_wait(
            if case.read {
                TransferOp::Read
            } else {
                TransferOp::Write
            },
            &peer,
            &[TransferRequest {
                transport: Transport::Rdma,
                local_address: memory.base_address() + GUARD as u64,
                remote_address: peer.regions()[0].base_address() + GUARD as u64,
                length: case.payload,
            }],
            TIMEOUT,
        )?;
        if report.transferred_bytes != 0
            || !report.locally_drained
            || report.failure.as_ref().is_none_or(|f| {
                !matches!(f.error, Error::EpochMismatch) || f.remote_effect != RemoteEffect::None
            })
        {
            return Err(format!("stale segment escaped generation check: {report:?}").into());
        }
    }
    if engine
        .stats()?
        .rdma
        .ok_or("missing RDMA stats")?
        .opened_connections
        != 0
    {
        return Err("stale epoch opened native QPs".into());
    }
    println!(
        "ORBITTE_INSTALLED_MEMORY_REPLACED {{\"pid\":{},\"endpoint\":\"{endpoint}\",\"old_epoch\":\"{epoch}\",\"new_epoch\":\"{}\",\"address\":{},\"same_process\":true,\"stale_epoch_before_qp\":true}}",
        std::process::id(),
        engine.source_epoch(),
        memory.base_address()
    );
    let peer = open_peer(case, &engine, &memory, &mut stream, endpoint)?;
    rounds(case, &engine, &memory, &peer, &mut stream, 1)?;
    close(&engine, memory)?;
    drop(engine);
    if fresh_backing.upgrade().is_some() || backing.upgrade().is_some() != target {
        return Err("replacement mixed fresh and quarantined backing owners".into());
    }
    if let Some(fault) = &fault {
        fault.retained()?;
    }
    maps()?;
    println!(
        "ORBITTE_INSTALLED_MEMORY_RESULT {{\"protocol\":\"{PROTOCOL}\",\"pid\":{},\"target\":{target},\"resource\":{},\"healthy_rounds\":6,\"full_bytes_and_guards\":true,\"fresh_engine_drained\":true,\"fresh_backing_released\":true,\"old_backing_retained\":{target}}}",
        std::process::id(),
        if window { 2 } else { 1 }
    );
    Ok(())
}
