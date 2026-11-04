//! Versioned operations needed by the C++ source adapters.
use crate::{
    abi::{Handle, Request, Status},
    batch::{self, Fields},
    boundary::*,
    engine,
    segments::Target,
};
use orbit_te::{EngineConfig, Error, IntentType};
use std::{
    ffi::{c_char, c_int},
    time::{Duration, Instant},
};

#[repr(C)]
struct RequestV1 {
    struct_size: u32,
    abi_version: u32,
    request: Request,
    policy_name: *const c_char,
    policy_name_len: usize,
    deadline_ns: u64,
    intent_type: i32,
    reserved: u32,
}

#[unsafe(no_mangle)]
unsafe extern "C" fn orbitte_create_v1(source: u32, config: *const c_char) -> Handle {
    guard(std::ptr::null_mut(), || {
        engine::create_using(|| {
            Ok(match source {
                0 if config.is_null() => EngineConfig::from_tent_env()?,
                1 => EngineConfig::from_tent_json(&text(config, (1 << 20) + 1)?)?,
                2 => EngineConfig::from_tent_file(text(config, 4097)?)?,
                _ => return Err(INVALID),
            })
        })
        .unwrap_or(std::ptr::null_mut())
    })
}

// Sample once per submit so equal absolute deadlines remain equal for merging.
fn clock_pair() -> Result<(Instant, u64)> {
    let now = Instant::now();
    let mut stamp = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: stamp is writable and CLOCK_MONOTONIC takes no additional arguments.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut stamp) } != 0 {
        return Err(INTERNAL);
    }
    let ns = u64::try_from(stamp.tv_sec)
        .ok()
        .and_then(|v| v.checked_mul(1_000_000_000))
        .and_then(|v| v.checked_add(u64::try_from(stamp.tv_nsec).ok()?))
        .ok_or(INTERNAL)?;
    Ok((now, ns))
}

fn request_rows<'a>(rows: *const RequestV1, count: usize) -> Result<&'a [RequestV1]> {
    if count == 0 {
        return Ok(&[]);
    }
    checked(rows.cast_mut(), count)?;
    for i in 0..count {
        // Do not form a full Rust reference until each C record reports the
        // installed size. Older or truncated headers can stop at the size word.
        let header = rows.wrapping_add(i).cast::<u32>();
        // SAFETY: the caller supplies the size word, then the storage it declares.
        // Prior records have the expected stride and checked() bounded the extent.
        if unsafe { header.read() } as usize != size_of::<RequestV1>() {
            return Err(INVALID);
        }
        // SAFETY: the verified record includes an initialized second header word.
        if unsafe { header.add(1).read() } != 1 {
            return Err(INVALID);
        }
    }
    input(rows, count)
}

#[unsafe(no_mangle)]
unsafe extern "C" fn orbitte_submit_v1(
    h: Handle,
    id: u64,
    rows: *const RequestV1,
    count: usize,
    note: *const Notice,
    flags: u32,
) -> c_int {
    call(|| {
        engine::with(h, |e| {
            if count > e.core.config().max_batch_requests {
                return Err(FULL);
            }
            if flags > 1 {
                return Err(INVALID);
            }
            let rows = request_rows(rows, count)?;
            let notify = if note.is_null() {
                None
            } else {
                Some(notice(note, e.core.config().max_notification_bytes)?)
            };
            let clock = rows
                .iter()
                .any(|r| r.deadline_ns != 0)
                .then(clock_pair)
                .transpose()?;
            e.submit_rows(id, rows, notify, |r| {
                if r.reserved != 0 {
                    return Err(INVALID);
                }
                let intent = match r.intent_type {
                    0 => IntentType::Unspec,
                    1 => IntentType::ForegroundGet,
                    2 => IntentType::BackgroundPrefetch,
                    3 => IntentType::Migration,
                    4 => IntentType::Checkpoint,
                    5 => IntentType::WeightLoading,
                    6 => IntentType::StagingInternal,
                    _ => return Err(INVALID),
                };
                let policy = if r.policy_name.is_null() && r.policy_name_len == 0 {
                    None
                } else {
                    if !(1..=256).contains(&r.policy_name_len) {
                        return Err(INVALID);
                    }
                    let name = std::str::from_utf8(input(r.policy_name.cast(), r.policy_name_len)?)
                        .map_err(|_| INVALID)?;
                    Some(match e.defaults.iter().find(|p| p.as_str() == name) {
                        Some(p) => p.clone(),
                        None => e.core.resolve_policy_name(name)?,
                    })
                };
                let deadline = if r.deadline_ns == 0 {
                    None
                } else {
                    let (now, ns) = clock.ok_or(INTERNAL)?;
                    Some(
                        if r.deadline_ns >= ns {
                            now.checked_add(Duration::from_nanos(r.deadline_ns - ns))
                        } else {
                            now.checked_sub(Duration::from_nanos(ns - r.deadline_ns))
                        }
                        .ok_or(INVALID)?,
                    )
                };
                Ok((
                    &r.request,
                    Fields {
                        cancellable: flags == 1,
                        policy,
                        intent,
                        deadline,
                    },
                ))
            })
        })
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn orbitte_progress_v1(h: Handle, id: u64, out: *mut Status) -> c_int {
    call(|| {
        checked(out, 1)?;
        engine::with(h, |e| {
            let batch = e.batch(id)?;
            let batch = lock(&batch)?;
            e.core.progress_batch(batch.id()?)?;
            put(out, batch::overall(&batch.observe_tasks(None)?))
        })
    })
}

#[repr(C)]
struct TaskV1 {
    status: Status,
    drained: u32,
    unquiesced: u32,
}
#[unsafe(no_mangle)]
unsafe extern "C" fn orbitte_tasks_v1(
    h: Handle,
    id: u64,
    out: *mut TaskV1,
    capacity: usize,
) -> c_int {
    call(|| {
        engine::with(h, |e| {
            let b = e.batch(id)?;
            let tasks = lock(&b)?.observe_tasks(None)?;
            if capacity != tasks.len() {
                return Err(INVALID);
            }
            let p = checked(out, capacity)?;
            for (i, t) in tasks.iter().enumerate() {
                let v = TaskV1 {
                    status: batch::status(t),
                    drained: u32::from(t.locally_drained),
                    unquiesced: u32::from(
                        t.failure
                            .as_ref()
                            .is_some_and(|f| matches!(f.error, Error::Unquiesced { .. })),
                    ),
                };
                // SAFETY: the C++ owner supplies capacity initialized slots or writable output
                // storage. i is strictly below the checked extent.
                unsafe {
                    p.as_ptr().add(i).write(v);
                }
            }
            Ok(())
        })
    })
}
fn drained(batch: &orbit_te::Batch) -> Result<()> {
    if !batch.locally_drained()? {
        if batch.observe_tasks(None)?.iter().any(|t| {
            t.failure
                .as_ref()
                .is_some_and(|f| matches!(f.error, Error::Unquiesced { .. }))
        }) {
            return Err(UNKNOWN);
        }
        return Err(FULL);
    }
    Ok(())
}
#[unsafe(no_mangle)]
extern "C" fn orbitte_drained_v1(h: Handle, id: u64) -> c_int {
    call(|| {
        engine::with(h, |e| {
            let b = e.batch(id)?;
            let b = lock(&b)?;
            drained(&b)
        })
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn orbitte_scatter_peer_v1(
    h: Handle,
    name: *const c_char,
    id: *mut u64,
) -> c_int {
    call(|| {
        checked(id, 1)?;
        engine::with(h, |e| put(id, e.open_scoped(text(name, 4097)?, true)?))
    })
}
#[unsafe(no_mangle)]
extern "C" fn orbitte_reap_v1(h: Handle, id: u64) -> c_int {
    call(|| {
        engine::with(h, |e| {
            let holder = e.batch(id)?;
            let mut batch = lock(&holder)?;
            drained(&batch)?;
            batch.free()?;
            lock(&e.batches)?.remove(&id);
            Ok(())
        })
    })
}

#[unsafe(no_mangle)]
extern "C" fn orbitte_probe_v1(h: Handle, id: u64) -> c_int {
    call(|| {
        engine::with(h, |e| match e.target(id)? {
            Target::Peer(_) => Ok(()),
            Target::File(_) => Err(UNSUPPORTED),
        })
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn orbitte_limits_v1(h: Handle, out: *mut usize) -> c_int {
    call(|| engine::with(h, |e| put(out, e.core.config().max_batch_requests)))
}

#[cfg(feature = "rdma")]
fn rdma_topology(e: &engine::Engine) -> Result<String> {
    use serde_json::json;
    let Some(t) = e.core.topology()? else {
        return Ok("{}".into());
    };
    if t.nic_list.is_empty() && t.mem_list.is_empty() {
        return Ok("{}".into());
    }
    let nics:Vec<_>=t.nic_list.iter().map(|n|{
        let mut row=json!({"name":n.name,"pci_bus_id":n.pci_bus_id.as_deref().unwrap_or(""),"type":0,"numa_node":n.numa_node.map(i64::from).unwrap_or(-1)});
        if !n.device_attrs.is_empty(){row["device_attrs"]=json!(n.device_attrs);}
        row
    }).collect();
    let mems:Vec<_>=t.mem_list.iter().map(|m| json!({"name":m.name,"pci_bus_id":m.pci_bus_id.as_deref().unwrap_or(""),"numa_node":m.numa_node.map(i64::from).unwrap_or(-1),"type":match m.kind {orbit_te::MemType::Host=>0,orbit_te::MemType::Cuda=>1},"device_list":{"rank0":m.device_list[0],"rank1":m.device_list[1],"rank2":m.device_list[2]}})).collect();
    Ok(json!({"nics":nics,"mems":mems}).to_string())
}

fn local_topology(e: &engine::Engine) -> Result<String> {
    #[cfg(feature = "rdma")]
    {
        let selected = rdma_topology(e)?;
        if selected != "{}" {
            return Ok(selected);
        }
    }
    let nodes = e.core.local_numa_nodes();
    if nodes.is_empty() {
        return Ok("{}".into());
    }
    let mems: Vec<_> = nodes
        .into_iter()
        .map(|id| {
            serde_json::json!({
                "name":format!("cpu:{id}"),"type":0,"numa_node":id,"pci_bus_id":"",
                "device_list":{"rank0":[],"rank1":[],"rank2":[]}
            })
        })
        .collect();
    Ok(serde_json::json!({"mems":mems,"nics":[]}).to_string())
}
#[unsafe(no_mangle)]
unsafe extern "C" fn orbitte_topology_v1(h: Handle, buf: *mut c_char, size: *mut usize) -> c_int {
    call(|| {
        engine::with(h, |e| {
            let size = checked(size, 1)?;
            // SAFETY: the caller passes an initialized capacity and writable output count.
            let capacity = unsafe { size.read() };
            let json = local_topology(e)?;
            let needed = json.len() + 1;
            put(size.as_ptr(), needed)?;
            if needed > 1 << 20 || capacity < needed {
                return Err(FULL);
            }
            strncpy(buf, needed, &json)
        })
    })
}

#[repr(C)]
#[derive(Copy, Clone)]
struct Notice {
    name: *const c_char,
    name_len: usize,
    message: *const c_char,
    message_len: usize,
}
fn notice(p: *const Notice, limit: usize) -> Result<orbit_te::Notification> {
    let n = input(p, 1)?[0];
    if n.name_len
        .checked_add(n.message_len)
        .is_none_or(|v| v > limit)
    {
        return Err(FULL);
    }
    let string = |p: *const c_char, n| -> Result<String> {
        if n == 0 {
            return Ok(String::new());
        }
        Ok(std::str::from_utf8(input(p.cast(), n)?)
            .map_err(|_| INVALID)?
            .to_owned())
    };
    Ok(orbit_te::Notification {
        name: string(n.name, n.name_len)?,
        message: string(n.message, n.message_len)?,
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn orbitte_notify_v1(h: Handle, id: u64, note: *const Notice) -> c_int {
    call(|| {
        engine::with(h, |e| {
            let n = notice(note, e.core.config().max_notification_bytes)?;
            if id == 0 {
                return Ok(e.core.send_local_notification(&n)?);
            }
            match e.target(id)? {
                Target::Peer(p) => Ok(e.core.send_notification(&p, &n, e.timeout())?),
                _ => Err(UNSUPPORTED),
            }
        })
    })
}
#[unsafe(no_mangle)]
unsafe extern "C" fn orbitte_recv_v1(h: Handle, out: *mut *mut u8, len: *mut usize) -> c_int {
    call(|| {
        checked(out, 1)?;
        checked(len, 1)?;
        put(out, std::ptr::null_mut())?;
        put(len, 0)?;
        engine::with(h, |e| {
            let notes = e.core.receive_notifications()?;
            let mut bytes = Vec::new();
            for n in notes {
                let name = u32::try_from(n.name.len()).map_err(|_| FULL)?;
                let msg = u32::try_from(n.message.len()).map_err(|_| FULL)?;
                bytes
                    .try_reserve(8 + n.name.len() + n.message.len())
                    .map_err(|_| FULL)?;
                bytes.extend(name.to_le_bytes());
                bytes.extend(msg.to_le_bytes());
                bytes.extend(n.name.bytes());
                bytes.extend(n.message.bytes());
            }
            let size = bytes.len();
            put(out, records(bytes)?)?;
            put(len, size)
        })
    })
}

#[unsafe(no_mangle)]
extern "C" fn orbitte_close_v1(h: Handle) -> c_int {
    call(|| engine::with(h, |e| Ok(e.core.close(e.timeout())?)))
}

#[unsafe(no_mangle)]
extern "C" fn orbitte_abandon_v1(h: Handle, id: u64) -> c_int {
    call(|| {
        engine::with(h, |e| {
            // Dropping Batch closes logical admission and delegates outstanding work
            // to its existing physical retirement owner.
            let batch = lock(&e.batches)?.remove(&id).ok_or(INVALID)?;
            drop(batch);
            Ok(())
        })
    })
}

#[cfg(test)]
#[path = "../tests/unit/cpp_v1.rs"]
mod tests;
