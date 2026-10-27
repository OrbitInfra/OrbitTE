//! Public backing and budget ownership after native destruction fails.

use std::cell::UnsafeCell;
use std::ffi::{c_char, c_void};
use std::ptr::NonNull;
use std::sync::{Arc, Weak};

use orbit_te::{Batch, BatchState, Error, HostMemory, Permission, RemoteEffect};

use super::*;

pub(super) struct Backing(Box<[UnsafeCell<u8>]>);
// SAFETY: only HostMemory accesses these bytes; its DMA loans exclude CPU copies.
unsafe impl Sync for Backing {}

pub(super) fn create(
    case: &Case,
    bind: SocketAddr,
) -> Result<(TransferEngine, RegisteredMemory, Weak<Backing>)> {
    let engine = resources::engine(case, bind)?;
    let owner = Arc::new(Backing(
        (0..case.length()).map(|_| UnsafeCell::new(0)).collect(),
    ));
    let weak = Arc::downgrade(&owner);
    let address = NonNull::new(owner.0.as_ptr().cast_mut().cast::<u8>()).ok_or("empty backing")?;
    // SAFETY: the exact allocation is stable until the retained owner drops;
    // no fixture reference reads or writes it outside this HostMemory.
    let memory = unsafe { HostMemory::from_raw_parts(address, case.length(), owner) }?;
    let registered = engine.register_host_memory(
        Arc::new(memory),
        0,
        case.length(),
        Permission::RemoteReadWrite,
    )?;
    resources::inventory(case, &engine)?;
    Ok((engine, registered, weak))
}

#[derive(Debug, Default)]
#[repr(C)]
struct Snapshot {
    version: u64,
    armed: u64,
    attempts: u64,
    code: u64,
    qpn: u64,
    live: u64,
    sequence: u64,
}

type Command = unsafe extern "C" fn(u32, *mut Snapshot) -> i32;
#[link(name = "dl")]
unsafe extern "C" {
    fn dlsym(handle: *mut c_void, name: *const c_char) -> *mut c_void;
}

enum NativeFault {
    Qp(Command),
    Dependency(dependency::DependencyFault),
}
pub(super) struct CleanupFault(NativeFault);
impl CleanupFault {
    pub(super) fn arm(case: &Case) -> Result<Self> {
        let kind = env::var("ORBITTE_CLEANUP_KIND").unwrap_or_else(|_| "qp".into());
        if kind != "qp" {
            return Ok(Self(NativeFault::Dependency(
                dependency::DependencyFault::arm(case, &kind)?,
            )));
        }
        // SAFETY: the supervisor pins the test library, which exports this ABI.
        let symbol = unsafe { dlsym(std::ptr::null_mut(), c"orbitte_cleanup_fault".as_ptr()) };
        if symbol.is_null() {
            return Err("missing native cleanup control".into());
        }
        // SAFETY: cq_fault.c defines this exact signature and snapshot layout.
        let fault = Self(NativeFault::Qp(unsafe {
            std::mem::transmute::<*mut c_void, Command>(symbol)
        }));
        let receipt = fault.command(1)?;
        if receipt.armed != 1 || receipt.attempts != 0 || receipt.qpn == 0 {
            return Err(format!("invalid cleanup arm: {receipt:?}").into());
        }
        Ok(fault)
    }

    fn command(&self, command: u32) -> Result<Snapshot> {
        let NativeFault::Qp(native) = self.0 else {
            return Err("wrong native cleanup ABI".into());
        };
        let mut snapshot = Snapshot::default();
        // SAFETY: snapshot is a live ABI-sized output owned by this call.
        let code = unsafe { native(command, &mut snapshot) };
        if code != 0 || snapshot.version != 1 {
            return Err(format!("invalid cleanup receipt ({code}): {snapshot:?}").into());
        }
        Ok(snapshot)
    }

    fn resource(&self) -> u32 {
        match &self.0 {
            NativeFault::Qp(_) => 0,
            NativeFault::Dependency(fault) => fault.kind,
        }
    }

    fn retained(&self) -> Result<Snapshot> {
        if let NativeFault::Dependency(fault) = &self.0 {
            let s = fault.retained()?;
            return Ok(Snapshot {
                version: s.version,
                armed: s.armed,
                attempts: s.attempts,
                code: s.code,
                qpn: s.qpn,
                live: s.live,
                sequence: s.sequence,
            });
        }
        let snapshot = self.command(2)?;
        if snapshot.armed != 1 || snapshot.attempts != 1 || snapshot.code != 5 || snapshot.live != 1
        {
            return Err(format!("native cleanup did not retain its QP: {snapshot:?}").into());
        }
        Ok(snapshot)
    }
}

pub(super) fn retain(
    case: &Case,
    engine: Arc<TransferEngine>,
    memory: RegisteredMemory,
    mut batches: Vec<Batch>,
    backing: Weak<Backing>,
    fault: CleanupFault,
    state: &str,
) -> Result<()> {
    let resource = fault.resource();
    let debt = resource != 3;
    let code = if resource == 0 { 5 } else { 16 };
    if batches.len() != usize::from(debt) {
        return Err("cleanup phase has the wrong pending batch count".into());
    }
    for batch in &mut batches {
        let outcome = batch.wait(TIMEOUT)?;
        let task = batch.task_status(0)?;
        if outcome.state != BatchState::Failed
            || outcome.locally_drained
            || task.locally_drained
            || outcome.transferred_bytes != 0
            || task.failure.as_ref().is_none_or(|failure| {
                !matches!(failure.error, Error::Unquiesced { code: native, .. } if native == code)
                    || failure.remote_effect
                        != if case.read {
                            RemoteEffect::None
                        } else {
                            RemoteEffect::Unknown
                        }
            })
            || !matches!(batch.free(), Err(Error::BatchBusy))
        {
            return Err(format!(
                "cleanup failure falsely drained public batch: {outcome:?} {task:?}"
            )
            .into());
        }
    }
    let offset = if case.read { case.destination() } else { GUARD };
    if debt && !matches!(memory.write(offset, &[0]), Err(Error::Unavailable(_))) {
        return Err("failed native cleanup released its DMA loan".into());
    }
    // Remove registry ownership before proving that quarantine retains backing.
    engine.unregister_memory(&memory)?;
    for _ in 0..2 {
        if !matches!(
            engine.close(Duration::from_millis(50)),
            Err(Error::Timeout | Error::Unquiesced { .. })
        ) {
            return Err("repeated close claimed unproven drain".into());
        }
    }
    let stats = engine.stats()?;
    let rdma = stats.rdma.ok_or("missing RDMA counters")?;
    let count = usize::from(debt);
    if stats.allocated_batches != count
        || stats.inflight_batches != count
        || stats.inflight_bytes != count * case.payload
        || rdma.retained_grants != count
        || rdma.retained_endpoints != usize::from(resource == 0)
        || rdma.active_endpoints != usize::from(resource == 0)
        || rdma.retained_work_requests != count
        || rdma.work_requests != count
        || rdma.direct_bytes != count * case.payload
        || (rdma.persistent_mrs > 0) != debt
        || (resource == 1 && rdma.retained_cqs == 0)
        || (resource == 2 && (rdma.retained_cqs != 0 || rdma.retained_completion_channels == 0))
        || (resource == 3
            && (rdma.retained_devices == 0
                || rdma.active_cqs != 0
                || rdma.active_completion_channels != 0
                || rdma.active_windows != 0
                || rdma.receive_work_requests != 0
                || rdma.active_message_mrs != 0
                || stats.active_source_operations != 0))
    {
        return Err(format!("failed cleanup lost native or budget owners: {stats:?}").into());
    }
    fault.retained()?;
    maps()?;
    drop(batches);
    drop(memory);
    drop(engine);
    if backing.upgrade().is_some() != debt {
        return Err("native dependency retained the wrong backing lifetime".into());
    }
    let native = fault.retained()?;
    println!(
        "ORBITTE_INSTALLED_CLEANUP {{\"phase\":0,\"pid\":{},\"qpn\":{},\"attempts\":{},\"code\":{},\"sequence\":{},\"retained_grants\":{},\"retained_endpoints\":{},\"sq_debt\":{},\"direct_bytes\":{},\"inflight_bytes\":{},\"batches\":{},\"unregistered\":true,\"resource\":{resource},\"retained_cqs\":{},\"retained_channels\":{},\"retained_devices\":{},\"persistent_mrs\":{},\"backing_retained\":{debt},\"post_drop_native_query\":true,\"fixture_owners_dropped\":true,\"batch_busy\":{debt},\"memory_busy\":{debt},\"close_attempts\":2,\"locally_drained\":false}}",
        std::process::id(),
        native.qpn,
        native.attempts,
        native.code,
        native.sequence,
        rdma.retained_grants,
        rdma.retained_endpoints,
        rdma.work_requests,
        rdma.direct_bytes,
        stats.inflight_bytes,
        stats.allocated_batches,
        rdma.retained_cqs,
        rdma.retained_completion_channels,
        rdma.retained_devices,
        rdma.persistent_mrs,
    );
    println!(
        "ORBITTE_INSTALLED_KILL {{\"phase\":0,\"pid\":{},\"cached_qps\":{},\"persistent_mrs\":{},\"sq_debt\":{},\"rq_debt\":{},\"orderly_close\":false}}",
        std::process::id(),
        rdma.active_endpoints,
        rdma.persistent_mrs,
        rdma.work_requests,
        rdma.receive_work_requests,
    );
    restart::request_kill(0, state)
}
