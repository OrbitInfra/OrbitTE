//! C symbols and validation of caller-provided storage.
use crate::abi::{
    Handle, NicStat, NotifyInfo, NotifyRecord, Options, Request, SegmentInfo, Status,
};
use crate::boundary::*;
use crate::memory::Opt;
use crate::{batch, engine, segments};
use orbit_te::Notification;
use std::ffi::{c_char, c_int, c_void};
use std::ptr;

#[unsafe(no_mangle)]
unsafe extern "C" fn tent_load_config_from_file(path: *const c_char) {
    void_call("tent_load_config_from_file", || {
        engine::set_document(text(path, (1 << 20) + 1)?)
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_set_config(key: *const c_char, value: *const c_char) {
    void_call("tent_set_config", || {
        engine::set_field(text(key, 1025)?, text(value, (1 << 20) + 1)?)
    })
}
#[unsafe(no_mangle)]
extern "C" fn tent_create_engine() -> Handle {
    guard(ptr::null_mut(), || {
        engine::create().unwrap_or(ptr::null_mut())
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_destroy_engine(h: Handle) {
    void_call("tent_destroy_engine", || engine::destroy(h))
}
#[unsafe(no_mangle)]
extern "C" fn tent_available(h: Handle) -> c_int {
    guard(0, || i32::from(engine::with(h, |_| Ok(())).is_ok()))
}

#[unsafe(no_mangle)]
unsafe extern "C" fn tent_segment_name(h: Handle, buf: *mut c_char, len: usize) -> c_int {
    call(|| engine::with(h, |e| strncpy(buf, len, &e.name())))
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_rpc_server_addr_port(
    h: Handle,
    buf: *mut c_char,
    len: usize,
    port: *mut u16,
) -> c_int {
    call(|| {
        checked(port, 1)?;
        engine::with(h, |e| {
            let addr = e.core.endpoint();
            strncpy(buf, len, &addr.ip().to_string())?;
            put(port, addr.port())
        })
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_open_segment(h: Handle, id: *mut u64, name: *const c_char) -> c_int {
    call(|| {
        checked(id, 1)?;
        let name = text(name, 4097)?;
        engine::with(h, |e| put(id, e.open(name)?))
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_close_segment(h: Handle, id: u64) -> c_int {
    call(|| engine::with(h, |e| e.close_segment(id)))
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_get_segment_info(h: Handle, id: u64, info: *mut SegmentInfo) -> c_int {
    call(|| {
        checked(info, 1)?;
        engine::with(h, |e| {
            let (kind, data) = e.info(id)?;
            let count = i32::try_from(data.len()).map_err(|_| FULL)?;
            put(
                info,
                SegmentInfo {
                    kind,
                    count,
                    buffers: records(data)?,
                },
            )
        })
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_free_segment_info(info: *mut SegmentInfo) {
    void_call("tent_free_segment_info", || {
        if !info.is_null() {
            checked(info, 1)?;
            // SAFETY: the caller passes the outstanding malloc array exactly once, as in TENT.
            unsafe {
                libc::free((*info).buffers.cast());
            }
        }
        Ok(())
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_allocate_memory(
    h: Handle,
    out: *mut *mut c_void,
    size: usize,
    location: *const c_char,
) -> c_int {
    call(|| {
        checked(out, 1)?;
        let opt = Opt::simple(&text(location, 64)?)?;
        engine::with(h, |e| {
            put(out, lock(&e.memory)?.allocate(&e.core, size, opt)?)
        })
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_allocate_memory_ex(
    h: Handle,
    out: *mut *mut c_void,
    size: usize,
    opts: *mut Options,
) -> c_int {
    call(|| {
        checked(out, 1)?;
        let opt = Opt::read(opts)?;
        engine::with(h, |e| {
            put(out, lock(&e.memory)?.allocate(&e.core, size, opt)?)
        })
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_free_memory(h: Handle, p: *mut c_void) -> c_int {
    call(|| {
        checked(p, 1)?;
        engine::with(h, |e| lock(&e.memory)?.free(&e.core, p))
    })
}
fn register(h: Handle, p: *mut c_void, n: usize, o: Opt) -> Result<()> {
    engine::with(h, |e| lock(&e.memory)?.register(&e.core, &[p], &[n], o))
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_register_memory(h: Handle, p: *mut c_void, n: usize) -> c_int {
    call(|| register(h, p, n, Opt::default()))
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_register_memory_with_perm(
    h: Handle,
    p: *mut c_void,
    n: usize,
    perm: c_int,
) -> c_int {
    call(|| register(h, p, n, Opt::perms(perm)?))
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_register_memory_ex(
    h: Handle,
    p: *mut c_void,
    n: usize,
    opt: *mut Options,
) -> c_int {
    call(|| register(h, p, n, Opt::read(opt)?))
}
fn register_batch(
    h: Handle,
    ptrs: *const *mut c_void,
    sizes: *const usize,
    n: usize,
    o: Opt,
) -> Result<()> {
    engine::with(h, |e| {
        if n > e.core.config().max_batch_requests {
            return Err(FULL);
        }
        lock(&e.memory)?.register(&e.core, input(ptrs, n)?, input(sizes, n)?, o)
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_register_memory_batch(
    h: Handle,
    p: *mut *mut c_void,
    s: *mut usize,
    n: usize,
    perm: c_int,
) -> c_int {
    call(|| register_batch(h, p, s, n, Opt::perms(perm)?))
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_register_memory_batch_ex(
    h: Handle,
    p: *mut *mut c_void,
    s: *mut usize,
    n: usize,
    o: *mut Options,
) -> c_int {
    call(|| register_batch(h, p, s, n, Opt::read(o)?))
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_unregister_memory(h: Handle, p: *mut c_void, n: usize) -> c_int {
    call(|| {
        engine::with(h, |e| {
            lock(&e.memory)?.unregister(&e.core, &[p], Some(&[n]))
        })
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_unregister_memory_batch(
    h: Handle,
    p: *mut *mut c_void,
    s: *mut usize,
    n: usize,
) -> c_int {
    call(|| {
        engine::with(h, |e| {
            if n > e.core.config().max_batch_requests {
                return Err(FULL);
            }
            let sizes = if s.is_null() {
                None
            } else {
                Some(input(s, n)?)
            };
            lock(&e.memory)?.unregister(&e.core, input(p, n)?, sizes)
        })
    })
}
#[unsafe(no_mangle)]
extern "C" fn tent_allocate_batch(h: Handle, n: usize) -> u64 {
    guard(0, || {
        if h.is_null() {
            u64::MAX
        } else {
            engine::with(h, |e| e.alloc_batch(n)).unwrap_or(0)
        }
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_free_batch(h: Handle, id: u64) -> c_int {
    call(|| engine::with(h, |e| e.free_batch(id)))
}
fn message(name: *const c_char, msg: *const c_char, limit: usize) -> Result<Notification> {
    Ok(Notification {
        name: text(name, limit + 1)?,
        message: text(msg, limit + 1)?,
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_submit(h: Handle, id: u64, p: *mut Request, n: usize) -> c_int {
    call(|| {
        engine::with(h, |e| {
            if n > e.core.config().max_batch_requests {
                return Err(FULL);
            }
            e.submit(id, input(p, n)?, None)
        })
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_submit_notif(
    h: Handle,
    id: u64,
    p: *mut Request,
    n: usize,
    name: *const c_char,
    msg: *const c_char,
) -> c_int {
    call(|| {
        engine::with(h, |e| {
            if n > e.core.config().max_batch_requests {
                return Err(FULL);
            }
            e.submit(
                id,
                input(p, n)?,
                Some(message(name, msg, e.core.config().max_notification_bytes)?),
            )
        })
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_send_notifs(
    h: Handle,
    id: u64,
    name: *const c_char,
    msg: *const c_char,
) -> c_int {
    call(|| {
        engine::with(h, |e| {
            let m = message(name, msg, e.core.config().max_notification_bytes)?;
            if id == 0 {
                return Ok(e.core.send_local_notification(&m)?);
            }
            match e.target(id)? {
                segments::Target::Peer(peer) => {
                    Ok(e.core.send_notification(&peer, &m, e.timeout())?)
                }
                _ => Err(UNSUPPORTED),
            }
        })
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_recv_notifs(h: Handle, info: *mut NotifyInfo) -> c_int {
    call(|| {
        checked(info, 1)?;
        put(
            info,
            NotifyInfo {
                count: 0,
                records: ptr::null_mut(),
            },
        )?;
        engine::with(h, |e| {
            let list = e.core.receive_notifications()?;
            let count = i32::try_from(list.len()).map_err(|_| FULL)?;
            let result = list
                .iter()
                .map(|n| NotifyRecord {
                    handle: 0,
                    name: fixed(&n.name),
                    msg: fixed(&n.message),
                })
                .collect();
            put(
                info,
                NotifyInfo {
                    count,
                    records: records(result)?,
                },
            )
        })
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_free_notifs(info: *mut NotifyInfo) {
    void_call("tent_free_notifs", || {
        if !info.is_null() {
            checked(info, 1)?;
            // SAFETY: records is null or the outstanding C allocation returned by recv.
            unsafe {
                libc::free((*info).records.cast());
            }
            put(
                info,
                NotifyInfo {
                    count: 0,
                    records: ptr::null_mut(),
                },
            )?;
        }
        Ok(())
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_task_status(h: Handle, id: u64, index: usize, out: *mut Status) -> c_int {
    call(|| {
        checked(out, 1)?;
        engine::with(h, |e| {
            let b = e.batch(id)?;
            let t = lock(&b)?.task_status(index)?;
            put(out, batch::status(&t))
        })
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_cancel_task(h: Handle, id: u64, index: usize) -> c_int {
    call(|| {
        engine::with(h, |e| {
            let b = e.batch(id)?;
            lock(&b)?.cancel(index)?;
            Ok(())
        })
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_overall_status(h: Handle, id: u64, out: *mut Status) -> c_int {
    call(|| {
        checked(out, 1)?;
        engine::with(h, |e| {
            let b = e.batch(id)?;
            let list = lock(&b)?.task_statuses()?;
            put(out, batch::overall(&list))
        })
    })
}
fn copy_list<T>(
    out: *mut T,
    count: *mut usize,
    items: impl ExactSizeIterator<Item = T>,
) -> Result<()> {
    let count = checked(count, 1)?;
    // SAFETY: count is the caller's initialized in/out capacity.
    let n = items.len().min(unsafe { count.read() });
    checked(out, n)?;
    let total = items.len();
    for (i, v) in items.take(n).enumerate() {
        // SAFETY: i is below both the supplied capacity and number available.
        unsafe {
            out.add(i).write(v);
        }
    }
    put(count.as_ptr(), total)
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_task_status_list(
    h: Handle,
    id: u64,
    out: *mut Status,
    count: *mut usize,
) -> c_int {
    call(|| {
        engine::with(h, |e| {
            let b = e.batch(id)?;
            let list = lock(&b)?.task_statuses()?;
            copy_list(out, count, list.iter().map(batch::status))
        })
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn tent_get_nic_load_stats(
    h: Handle,
    out: *mut NicStat,
    count: *mut usize,
) -> c_int {
    call(|| {
        engine::with(h, |e| {
            #[cfg(feature = "rdma")]
            {
                let stats = e.core.nic_load_stats()?;
                copy_list(
                    out,
                    count,
                    stats.iter().map(|s| NicStat {
                        device_name: fixed(&s.device_name),
                        inflight_bytes: s.inflight_bytes as u64,
                        ewma_bandwidth_bps: s.ewma_bandwidth_bps,
                    }),
                )
            }
            #[cfg(not(feature = "rdma"))]
            {
                let _ = e;
                copy_list(out, count, std::iter::empty())
            }
        })
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn tent_terminal_result(
    h: Handle,
    id: u64,
    task: usize,
    code: *mut c_int,
    known: *mut c_int,
) -> c_int {
    call(|| {
        if !code.is_null() {
            put(code, UNKNOWN.0)?;
        }
        if !known.is_null() {
            put(known, 0)?;
        }
        checked(code, 1)?;
        checked(known, 1)?;
        if code == known {
            return Err(INVALID);
        }
        engine::with(h, |e| {
            let batch = e.batch(id)?;
            let tasks = lock(&batch)?.observe_tasks((task != usize::MAX).then_some(task))?;
            let (value, certainty) = batch::terminal(&tasks)?;
            put(code, value)?;
            put(known, certainty)
        })
    })
}
