//! Python entry points use the existing retained owners, not a second coordinator.
use crate::{batch, boundary::*, engine, memory::Opt, records, segments::Target, support};
use orbite::{EngineConfig, IntentType};
use pyo3::{create_exception, exceptions::PyException, prelude::*};
mod export;
use std::{
    ffi::c_void,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

create_exception!(orbite, TentException, PyException);
macro_rules! exception {
    ($($name:ident),* $(,)?) => {$(create_exception!(orbite, $name, TentException);)*};
}
exception!(
    InvalidArgumentError,
    AddressNotRegisteredError,
    DeviceNotFoundError,
    InvalidEntryError,
    RdmaError,
    CudaError,
    MetadataError,
    RpcServiceError,
    InternalError,
    NotImplementedError
);

fn error(code: Code, operation: &str) -> PyErr {
    let message = format!("{operation}: native status {}", code.0);
    match code.0 {
        1 => InvalidArgumentError::new_err(message),
        3 => AddressNotRegisteredError::new_err(message),
        4 => DeviceNotFoundError::new_err(message),
        5 => InvalidEntryError::new_err(message),
        100 => RdmaError::new_err(message),
        101 => CudaError::new_err(message),
        131 => MetadataError::new_err(message),
        132 | 134 => RpcServiceError::new_err(message),

        200 => NotImplementedError::new_err(message),
        _ => InternalError::new_err(message),
    }
}
fn run<T: Send>(py: Python<'_>, name: &str, f: impl FnOnce() -> Result<T> + Send) -> PyResult<T> {
    py.detach(|| guard(Err(INTERNAL), f))
        .map_err(|e| error(e, name))
}
fn options(v: &Bound<'_, PyAny>) -> PyResult<Opt> {
    Opt::fields(
        &v.getattr("location")?.extract::<String>()?,
        v.getattr("perm")?.extract()?,
        v.getattr("type")?.extract()?,
        v.getattr("shm_path")?.extract()?,
        v.getattr("shm_offset")?.extract()?,
        v.getattr("internal")?.extract()?,
    )
    .map_err(|e| error(e, "memory options"))
}
fn bounded(v: &Bound<'_, PyAny>, maximum: usize) -> PyResult<usize> {
    let len = v.len()?;
    if len > maximum {
        return Err(error(FULL, "sequence capacity"));
    }
    Ok(len)
}
fn notification(
    v: Option<&Bound<'_, PyAny>>,
    maximum: usize,
) -> PyResult<Option<orbite::Notification>> {
    v.map(|v| {
        let name: String = v.getattr("name")?.extract()?;
        let message: String = v.getattr("msg")?.extract()?;
        if name
            .len()
            .checked_add(message.len())
            .is_none_or(|n| n > maximum)
        {
            return Err(error(FULL, "notification capacity"));
        }
        Ok(orbite::Notification { name, message })
    })
    .transpose()
}
#[pyclass(name = "_Engine", module = "orbite._native")]
struct PythonEngine {
    handle: usize,
    closed: AtomicBool,
    close_result: Mutex<Option<Result<()>>>,
    regions: usize,
    requests: usize,
    notification_bytes: usize,
}
impl PythonEngine {
    fn with<T: Send>(
        &self,
        py: Python<'_>,
        name: &str,
        f: impl FnOnce(&engine::Engine) -> Result<T> + Send,
    ) -> PyResult<T> {
        run(py, name, || {
            if self.closed.load(Ordering::Acquire) {
                return Err(INVALID);
            }
            engine::with(self.handle, f)
        })
    }
}
impl Drop for PythonEngine {
    fn drop(&mut self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            // At interpreter shutdown do not enter a finalizing interpreter or
            // release exporters underneath native work. The bounded engine slot is
            // the quarantine owner if Python can no longer safely detach.
            let _ = Python::try_attach(|py| {
                py.detach(|| {
                    guard((), || {
                        let _ = engine::destroy(self.handle);
                    });
                })
            });
        }
    }
}

#[pymethods]
impl PythonEngine {
    #[new]
    #[pyo3(signature = (config_path=None))]
    fn new(py: Python<'_>, config_path: Option<String>) -> PyResult<Self> {
        // Load without altering process environment. Preserve the diagnostic
        // text (not server URIs) for the P2P migration rejection.
        let cfg = py
            .detach(|| {
                guard(
                    Err(orbite::Error::Internal("Python constructor panicked")),
                    || match config_path {
                        Some(path) => EngineConfig::from_tent_file(path),
                        None => EngineConfig::from_tent_env(),
                    },
                )
            })
            .map_err(|e| error(Code::from(&e), &e.to_string()))?;
        let (regions, requests, notification_bytes) = (
            cfg.max_registered_regions,
            cfg.max_batch_requests,
            cfg.max_notification_bytes,
        );
        let handle = run(py, "construct engine", || {
            let h = engine::create_using(|| Ok(cfg))?;
            if let Err(err) = engine::with(h, |_| Ok(())) {
                let _ = engine::destroy(h);
                return Err(err);
            }
            Ok(h)
        })?;
        Ok(Self {
            handle,
            closed: AtomicBool::new(false),
            close_result: Mutex::new(None),
            regions,
            requests,
            notification_bytes,
        })
    }
    fn available(&self, py: Python<'_>) -> bool {
        self.with(py, "available", |_| Ok(())).is_ok()
    }
    fn get_segment_name(&self, py: Python<'_>) -> PyResult<String> {
        self.with(py, "get_segment_name", |e| Ok(e.name()))
    }
    fn get_rpc_server_address(&self, py: Python<'_>) -> PyResult<String> {
        self.with(py, "get_rpc_server_address", |e| {
            Ok(e.core.endpoint().ip().to_string())
        })
    }
    fn get_rpc_server_port(&self, py: Python<'_>) -> PyResult<u16> {
        self.with(py, "get_rpc_server_port", |e| Ok(e.core.endpoint().port()))
    }
    fn get_local_topology(&self, py: Python<'_>) -> PyResult<String> {
        self.with(py, "get_local_topology", support::local_topology)
    }
    fn export_local_segment(&self, py: Python<'_>) -> PyResult<String> {
        self.with(py, "export_local_segment", |_| Err(UNSUPPORTED))
    }
    fn import_remote_segment(&self, py: Python<'_>, shared_handle: &str) -> PyResult<u64> {
        let _ = shared_handle;
        self.with(py, "import_remote_segment", |_| Err(UNSUPPORTED))
    }
    fn open_segment(&self, py: Python<'_>, name: String) -> PyResult<u64> {
        if name.len() > 4096 || name.contains('\0') {
            return Err(error(INVALID, "segment name"));
        }
        self.with(py, "open_segment", |e| e.open(name))
    }
    fn close_segment(&self, py: Python<'_>, id: u64) -> PyResult<()> {
        self.with(py, "close_segment", |e| e.close_segment(id))
    }
    #[allow(clippy::type_complexity)]
    fn get_segment_info(
        &self,
        py: Python<'_>,
        id: u64,
    ) -> PyResult<(i32, Vec<(u64, u64, String)>)> {
        self.with(py, "get_segment_info", |e| {
            let (kind, rows) = e.info(id)?;
            let rows = rows
                .into_iter()
                .map(|b| {
                    let bytes: Vec<_> = b
                        .location
                        .iter()
                        .take_while(|&&n| n != 0)
                        .map(|&n| u8::from_ne_bytes(n.to_ne_bytes()))
                        .collect();
                    Ok((
                        b.base,
                        b.length,
                        String::from_utf8(bytes).map_err(|_| INTERNAL)?,
                    ))
                })
                .collect::<Result<_>>()?;
            Ok((kind, rows))
        })
    }
    fn allocate(&self, py: Python<'_>, n: usize, opts: &Bound<'_, PyAny>) -> PyResult<usize> {
        let opt = options(opts)?;
        self.with(py, "allocate_local_memory", |e| {
            Ok(lock(&e.memory)?.allocate(&e.core, n, opt)? as usize)
        })
    }
    fn free(&self, py: Python<'_>, addr: usize) -> PyResult<()> {
        self.with(py, "free_local_memory", |e| {
            lock(&e.memory)?.free(&e.core, addr as *mut c_void)
        })
    }
    fn abandon_memory(&self, py: Python<'_>, addr: usize) -> PyResult<()> {
        self.with(py, "MemoryGuard exit", |e| {
            lock(&e.memory)?.defer_free(&e.core, addr)
        })
    }
    #[pyo3(signature = (addrs, sizes, opts, owners=None))]
    fn register(
        &self,
        py: Python<'_>,
        addrs: &Bound<'_, PyAny>,
        sizes: &Bound<'_, PyAny>,
        opts: &Bound<'_, PyAny>,
        owners: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let count = bounded(addrs, self.regions)?;
        if bounded(sizes, self.regions)? != count
            || owners.is_some_and(|v| v.len().ok() != Some(count))
        {
            return Err(error(INVALID, "registration list sizes"));
        }
        let opt = options(opts)?;
        let mut addresses = Vec::with_capacity(count);
        let mut lengths = Vec::with_capacity(count);
        let mut retained = Vec::with_capacity(count);
        for i in 0..count {
            let address: usize = addrs.get_item(i)?.extract()?;
            let length: usize = sizes.get_item(i)?.extract()?;
            let owner = if let Some(owners) = owners {
                let owner = owners.get_item(i)?;
                if owner.is_none() {
                    None
                } else {
                    let (base, size, owner) = export::get(&owner)?;
                    if address < base
                        || address
                            .checked_add(length)
                            .is_none_or(|end| end > base.saturating_add(size))
                    {
                        return Err(error(INVALID, "range outside exporter"));
                    }
                    Some(owner)
                }
            } else {
                None
            };
            addresses.push(address);
            lengths.push(length);
            retained.push(owner);
        }
        self.with(py, "register_local_memory", |e| {
            let addresses: Vec<_> = addresses.into_iter().map(|n| n as *mut c_void).collect();
            lock(&e.memory)?.register_owned(&e.core, &addresses, &lengths, opt, &retained)
        })
    }
    fn register_buffer(
        &self,
        py: Python<'_>,
        value: &Bound<'_, PyAny>,
        opts: &Bound<'_, PyAny>,
    ) -> PyResult<usize> {
        let (address, length, owner) = export::get(value)?;
        let opt = options(opts)?;
        self.with(py, "register_buffer", |e| {
            lock(&e.memory)?.register_owned(
                &e.core,
                &[address as *mut c_void],
                &[length],
                opt,
                &[Some(owner)],
            )?;
            Ok(address)
        })
    }
    fn unregister(
        &self,
        py: Python<'_>,
        addrs: &Bound<'_, PyAny>,
        sizes: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let count = bounded(addrs, self.regions)?;
        let size_count = bounded(sizes, self.regions)?;
        if size_count != 0 && size_count != count {
            return Err(error(INVALID, "unregister list sizes"));
        }
        let addresses: Vec<usize> = addrs.extract()?;
        let sizes: Vec<usize> = sizes.extract()?;
        self.with(py, "unregister_local_memory", |e| {
            let addresses: Vec<_> = addresses.into_iter().map(|n| n as *mut c_void).collect();
            let mut memory = lock(&e.memory)?;
            memory.unregister(
                &e.core,
                &addresses,
                (!sizes.is_empty()).then_some(sizes.as_slice()),
            )?;
            memory.reap_deferred(&e.core);
            Ok(())
        })
    }
    fn allocate_transfer_batch(&self, py: Python<'_>, n: usize) -> PyResult<u64> {
        self.with(py, "allocate_transfer_batch", |e| e.alloc_batch(n))
    }
    fn free_transfer_batch(&self, py: Python<'_>, id: u64) -> PyResult<()> {
        self.with(py, "free_transfer_batch", |e| e.free_batch(id))
    }
    fn abandon_batch(&self, py: Python<'_>, id: u64) -> PyResult<()> {
        self.with(py, "BatchGuard release", |e| {
            // Batch::drop fences logical admission and moves running ownership to
            // its existing deferred retirement owner. It is not a drain receipt.
            let b = lock(&e.batches)?.remove(&id).ok_or(INVALID)?;
            drop(b);
            Ok(())
        })
    }
    #[pyo3(signature = (id, rows, note=None))]
    fn submit_transfer(
        &self,
        py: Python<'_>,
        id: u64,
        rows: &Bound<'_, PyAny>,
        note: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let n = bounded(rows, self.requests)?;
        let note = notification(note, self.notification_bytes)?;
        // Extract while attached; no Python object is touched by the worker.
        let mut decoded = Vec::with_capacity(n);
        for i in 0..n {
            let r = rows.get_item(i)?;
            let policy: Option<String> = r.getattr("policy_name")?.extract()?;
            if policy.as_ref().is_some_and(|p| p.len() > 256) {
                return Err(error(INVALID, "policy name"));
            }
            decoded.push(Row {
                opcode: r.getattr("opcode")?.extract()?,
                source: r.getattr("source")?.extract()?,
                target: r.getattr("target_id")?.extract()?,
                offset: r.getattr("target_offset")?.extract()?,
                length: r.getattr("length")?.extract()?,
                priority: r.getattr("priority")?.extract()?,
                hint: r.getattr("transport_hint")?.extract()?,
                policy,
                deadline: r.getattr("deadline_ns")?.extract()?,
                intent: r.getattr("intent_type")?.extract()?,
            });
        }
        self.with(py, "submit_transfer", |e| {
            let clock = decoded
                .iter()
                .any(|r| r.deadline != 0)
                .then(support::clock_pair)
                .transpose()?;
            let rows: Vec<_> = decoded
                .iter()
                .map(|r| {
                    (
                        records::Request {
                            opcode: r.opcode,
                            source: r.source as *mut c_void,
                            target_id: r.target,
                            target_offset: r.offset,
                            length: r.length,
                            priority: r.priority,
                            transport_hint: r.hint,
                        },
                        r,
                    )
                })
                .collect();
            e.submit_rows(id, &rows, note, |(raw, r)| {
                let intent = match r.intent {
                    0 => IntentType::Unspec,
                    1 => IntentType::ForegroundGet,
                    2 => IntentType::BackgroundPrefetch,
                    3 => IntentType::Migration,
                    4 => IntentType::Checkpoint,
                    5 => IntentType::WeightLoading,
                    6 => IntentType::StagingInternal,
                    _ => return Err(INVALID),
                };
                let policy = r
                    .policy
                    .as_ref()
                    .map(|p| {
                        e.defaults
                            .iter()
                            .find(|v| v.as_str() == p)
                            .cloned()
                            .map(Ok)
                            .unwrap_or_else(|| e.core.resolve_policy_name(p))
                    })
                    .transpose()?;
                let deadline = if r.deadline == 0 {
                    None
                } else {
                    let (now, ns) = clock.ok_or(INTERNAL)?;
                    Some(
                        if r.deadline >= ns {
                            now.checked_add(Duration::from_nanos(r.deadline - ns))
                        } else {
                            now.checked_sub(Duration::from_nanos(ns - r.deadline))
                        }
                        .ok_or(INVALID)?,
                    )
                };
                Ok((
                    raw,
                    batch::Fields {
                        policy,
                        deadline,
                        intent,
                        cancellable: false,
                    },
                ))
            })
        })
    }
    fn cancel_transfer(&self, py: Python<'_>, id: u64, task: usize) -> PyResult<()> {
        self.with(py, "cancel_transfer", |e| {
            lock(e.batch(id)?.as_ref())?.cancel(task)?;
            Ok(())
        })
    }
    fn send_notifi(&self, py: Python<'_>, id: u64, note: &Bound<'_, PyAny>) -> PyResult<()> {
        let n = notification(Some(note), self.notification_bytes)?
            .ok_or_else(|| error(INVALID, "notification"))?;
        self.with(py, "send_notifi", |e| {
            if id == 0 {
                return Ok(e.core.send_local_notification(&n)?);
            }
            match e.target(id)? {
                Target::Peer(p) => Ok(e.core.send_notification(&p, &n, e.timeout())?),
                _ => Err(UNSUPPORTED),
            }
        })
    }
    fn recv_notifi(&self, py: Python<'_>) -> PyResult<Vec<(String, String)>> {
        self.with(py, "recv_notifi", |e| {
            Ok(e.core
                .receive_notifications()?
                .into_iter()
                .map(|n| (n.name, n.message))
                .collect())
        })
    }
    fn get_transfer_status(&self, py: Python<'_>, id: u64, task: usize) -> PyResult<(i32, u64)> {
        self.with(py, "get_transfer_status", |e| {
            let b = e.batch(id)?;
            let b = lock(&b)?;
            let tasks = b.observe_tasks(Some(task))?;
            let s = batch::status(tasks.first().ok_or(INVALID)?);
            Ok((s.status, s.transferred_bytes))
        })
    }
    fn get_transfer_status_list(&self, py: Python<'_>, id: u64) -> PyResult<Vec<(i32, u64)>> {
        self.with(py, "get_transfer_status_list", |e| {
            Ok(lock(e.batch(id)?.as_ref())?
                .observe_tasks(None)?
                .iter()
                .map(|t| {
                    let s = batch::status(t);
                    (s.status, s.transferred_bytes)
                })
                .collect())
        })
    }
    fn get_transfer_status_overall(&self, py: Python<'_>, id: u64) -> PyResult<(i32, u64)> {
        self.with(py, "get_transfer_status_overall", |e| {
            let s = batch::overall(&lock(e.batch(id)?.as_ref())?.observe_tasks(None)?);
            Ok((s.status, s.transferred_bytes))
        })
    }
    fn progress_batch(&self, py: Python<'_>, id: u64) -> PyResult<(i32, u64)> {
        self.with(py, "progress_batch", |e| {
            let b = e.batch(id)?;
            let b = lock(&b)?;
            e.core.progress_batch(b.id()?)?;
            let s = batch::overall(&b.observe_tasks(None)?);
            Ok((s.status, s.transferred_bytes))
        })
    }
    fn wait(&self, py: Python<'_>, id: u64, timeout: f64) -> PyResult<(i32, u64)> {
        let duration =
            Duration::try_from_secs_f64(timeout).map_err(|_| error(INVALID, "wait timeout"))?;
        self.with(py, "wait", |e| {
            let deadline = Instant::now().checked_add(duration).ok_or(INVALID)?;
            let b = e.batch(id)?;
            let mut polls = 0u64;
            loop {
                {
                    // Never hold the per-batch lock across a wait: another Python
                    // thread must be able to cancel, observe or retire this batch.
                    let b = lock(&b)?;
                    e.core.progress_batch(b.id()?)?;
                    let tasks = b.observe_tasks(None)?;
                    if tasks.iter().any(|t| {
                        t.failure
                            .as_ref()
                            .is_some_and(|f| matches!(f.error, orbite::Error::Unquiesced { .. }))
                    }) {
                        return Err(UNKNOWN);
                    }
                    if tasks.iter().all(|t| t.state.is_terminal()) && b.locally_drained()? {
                        let s = batch::overall(&tasks);
                        return Ok((s.status, s.transferred_bytes));
                    }
                }
                if Instant::now() >= deadline {
                    return Err(Code(132));
                }
                orbite::wait_before_next_poll(polls);
                polls = polls.saturating_add(1);
            }
        })
    }
    #[pyo3(signature = (id, task=None))]
    fn terminal_result(
        &self,
        py: Python<'_>,
        id: u64,
        task: Option<usize>,
    ) -> PyResult<(i32, bool)> {
        self.with(py, "terminal_result", |e| {
            let (code, known) =
                batch::terminal(&lock(e.batch(id)?.as_ref())?.observe_tasks(task)?)?;
            Ok((code, known != 0))
        })
    }
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        run(py, "close", || {
            let mut finished = lock(&self.close_result)?;
            if let Some(result) = *finished {
                return result;
            }
            self.closed.store(true, Ordering::Release);
            let result = guard(Err(INTERNAL), || engine::destroy(self.handle));
            *finished = Some(result);
            result
        })
    }
}
struct Row {
    opcode: i32,
    source: usize,
    target: u64,
    offset: u64,
    length: u64,
    priority: i32,
    hint: i32,
    policy: Option<String>,
    deadline: u64,
    intent: i32,
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PythonEngine>()?;
    macro_rules! add { ($($e:ident),*) => { $(m.add(stringify!($e), m.py().get_type::<$e>())?;)* }; }
    add!(
        TentException,
        InvalidArgumentError,
        AddressNotRegisteredError,
        DeviceNotFoundError,
        InvalidEntryError,
        RdmaError,
        CudaError,
        MetadataError,
        RpcServiceError,
        InternalError,
        NotImplementedError
    );
    Ok(())
}

#[cfg(test)]
#[path = "../tests/unit/python.rs"]
mod tests;
