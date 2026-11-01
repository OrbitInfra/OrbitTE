#![allow(unsafe_code)]

use super::*;
use crate::transport::rdma::{discovery::RdmaAddress, sys, tests as fixture};
use crate::{Priority, Topology, TransferOp};

#[derive(Default)]
struct Gate {
    hold: bool,
    entered: bool,
    events: bool,
    posted: Vec<u64>,
}
static SERIAL: Mutex<()> = Mutex::new(());
static GATE: OnceLock<(Mutex<Gate>, Condvar)> = OnceLock::new();

pub(super) fn before_tick(state: &ProgressState) {
    if state.config.device.as_deref() != Some("priority_copy") {
        return;
    }
    let (lock, changed) = GATE.get_or_init(Default::default);
    let mut gate = lock.lock().unwrap();
    while gate.hold {
        gate.entered = true;
        changed.notify_all();
        gate = changed.wait(gate).unwrap();
    }
}

fn discover(_: &RdmaConfig) -> Result<(Topology, Vec<RdmaAddress>)> {
    let (mut topology, mut routes) = fixture::discover(&RdmaConfig::new("priority_copy"))?;
    topology.nic_list.push(topology.nic_list[0].clone());
    topology.nic_list[1].name = "priority_copy_peer".into();
    routes.push(routes[0].clone());
    routes[1].device = topology.nic_list[1].name.clone();
    topology.rank();
    Ok((topology, routes))
}

unsafe extern "C" fn post(
    handles: *mut sys::ConnectionHandles,
    memory: *const sys::MemoryHandles,
    requests: *const sys::WorkRequest,
    count: usize,
    local: u64,
    length: usize,
    remote: u64,
    rkey: u32,
    write: i32,
    result: *mut sys::PostResult,
) -> i32 {
    let events = {
        let mut gate = GATE.get_or_init(Default::default).0.lock().unwrap();
        gate.posted.push(local);
        gate.events
    };
    let verbs = if events {
        &crate::transport::rdma::cq::tests::VERBS
    } else {
        &fixture::VERBS
    };
    unsafe {
        (verbs.post)(
            handles, memory, requests, count, local, length, remote, rkey, write, result,
        )
    }
}

struct Release;
impl Drop for Release {
    fn drop(&mut self) {
        let (lock, changed) = GATE.get().unwrap();
        lock.lock().unwrap().hold = false;
        changed.notify_all();
    }
}

#[test]
fn shared_worker_dispatches_priority_then_optional_deadline_across_contexts() {
    let _serial = SERIAL.lock().unwrap();
    static VERBS: sys::Verbs = sys::Verbs {
        discover,
        post,
        ..fixture::VERBS
    };
    static EVENT_VERBS: sys::Verbs = sys::Verbs {
        discover,
        post,
        ..crate::transport::rdma::cq::tests::VERBS
    };
    for (arbitrate, events) in [(false, false), (true, false), (false, true), (true, true)] {
        let mut runtime = if events {
            fixture::event_runtime("priority_copy")
        } else {
            fixture::runtime("priority_copy")
        };
        runtime.config.block_size = 64 << 10;
        runtime.config.block_size = 64 << 10;
        runtime.config.max_cqes = 16;
        runtime.config.max_endpoints = 8;
        runtime.config.max_staging_bytes = 128;
        runtime.config.max_cqes = 1;
        runtime.config.deadline_bw_arbitration = arbitrate;
        runtime.config.priority_promotion_timeout = Duration::from_secs(60);
        runtime.verbs = if events { &EVENT_VERBS } else { &VERBS };
        let mut grants: Vec<_> = (0..5)
            .map(|_| runtime.staging_endpoint(8, 0).unwrap())
            .collect();
        assert!(!Arc::ptr_eq(&grants[0].progress, &grants[1].progress));
        assert!(Arc::ptr_eq(&grants[2].progress, &grants[4].progress));
        for grant in &mut grants {
            grant.connect(&grant.descriptor()).unwrap();
        }
        let (lock, changed) = GATE.get_or_init(Default::default);
        *lock.lock().unwrap() = Gate {
            hold: true,
            events,
            ..Default::default()
        };
        let release = Release;
        grants[0].progress.shared.wake();
        let state = lock.lock().unwrap();
        let (state, timeout) = changed
            .wait_timeout_while(state, Duration::from_secs(3), |gate| !gate.entered)
            .unwrap();
        assert!(!timeout.timed_out());
        drop(state);
        let end = Instant::now() + Duration::from_secs(20);
        let mut addresses = Vec::new();
        for (index, (grant, priority)) in grants
            .iter()
            .zip([
                Priority::Low,
                Priority::Medium,
                Priority::High,
                Priority::Low,
                Priority::High,
            ])
            .enumerate()
        {
            let descriptor = grant.descriptor();
            addresses.push(descriptor.address);
            grant
                .progress
                .submit(
                    &grant.record,
                    grant.claim,
                    descriptor,
                    TransferOp::Read,
                    priority,
                    if index == 4 {
                        end - Duration::from_secs(10)
                    } else {
                        end
                    },
                )
                .unwrap();
        }
        drop(release);
        for grant in &grants {
            grant
                .progress
                .wait(&grant.record, grant.claim, end)
                .unwrap();
        }
        let expected = if arbitrate {
            [4, 2, 1, 0, 3]
        } else {
            [2, 4, 1, 0, 3]
        };
        assert_eq!(
            lock.lock().unwrap().posted,
            expected.map(|index| addresses[index]),
            "arbitrate={arbitrate}"
        );
        assert_eq!(runtime.stats().unwrap().work_requests, 0);
        for grant in &mut grants {
            grant.close().unwrap();
        }
        drop(grants);
        runtime.close(end).unwrap();
        assert_eq!(runtime.stats().unwrap().progress_workers, 0);
    }
}

#[test]
fn public_single_and_sliced_requests_preserve_priority_before_native_post() {
    use crate::engine::tests::{rdma_fixture_with_runtime, source_runtime};
    use crate::{Permission, TransferRequest, Transport};
    let _serial = SERIAL.lock().unwrap();
    for (lanes, priority, count) in [(1, Priority::Medium, 1), (2, Priority::Low, 4)] {
        let engine = |mode| {
            let mut runtime = fixture::runtime(mode);
            runtime.config.num_lanes = lanes;
            runtime.config.block_size = 64 << 10;
            runtime.config.max_cqes = 16;
            runtime.config.max_endpoints = 8;
            runtime.config.max_staging_bytes = 128;
            runtime.config.block_size = 2;
            rdma_fixture_with_runtime(runtime)
        };
        let requester = engine("priority_copy");
        let source = engine("copy");
        let local = requester
            .register_memory(vec![0; 8].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let remote = source
            .register_memory(vec![0x71; 8].into_boxed_slice(), Permission::RemoteRead)
            .unwrap();
        let timeout = Duration::from_secs(5);
        let peer = requester
            .open_segment(source.endpoint(), source.source_epoch(), timeout)
            .unwrap();
        let request = TransferRequest {
            priority,
            remote_staging: false,
            transport: Transport::Rdma,
            local_address: local.base_address(),
            remote_address: remote.base_address(),
            length: 8,
        };
        for transport in [Transport::Tcp, Transport::Nvlink] {
            let mut unsupported = request.clone();
            unsupported.transport = transport;
            assert!(matches!(
                requester.submit_and_wait(TransferOp::Read, &peer, &[unsupported], timeout),
                Err(Error::Unavailable("request priority requires RDMA"))
            ));
        }
        thread::scope(|scope| {
            *GATE.get_or_init(Default::default).0.lock().unwrap() = Gate {
                hold: true,
                ..Default::default()
            };
            let release = Release;
            let transfer = scope.spawn(|| {
                requester
                    .submit_and_wait(TransferOp::Read, &peer, &[request], timeout)
                    .unwrap()
            });
            let deadline = Instant::now() + Duration::from_secs(3);
            let observed = loop {
                let lanes: Vec<_> = source_runtime(&requester)
                    .context_set
                    .lock()
                    .unwrap()
                    .slots
                    .iter()
                    .flat_map(|slot| slot.cq_list.iter().flatten().cloned())
                    .collect();
                let mut observed = Vec::new();
                for lane in lanes {
                    for record in lane.records() {
                        if let Some(work) = record.queued_work() {
                            observed.push((work.priority, work.length));
                        }
                    }
                }
                if observed.len() == count || Instant::now() >= deadline {
                    break observed;
                }
                thread::yield_now();
            };
            drop(release);
            let report = transfer.join().unwrap();
            assert!(report.is_complete(), "{report:?}");
            assert_eq!(observed.len(), count);
            assert!(observed.iter().all(|&(value, _)| value == priority));
            assert_eq!(observed.iter().map(|&(_, bytes)| bytes).sum::<usize>(), 8);
        });
        let mut bytes = [0; 8];
        local.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, [0x71; 8]);
        requester.close_source(timeout).unwrap();
        source.close_source(timeout).unwrap();
    }
}

#[test]
fn promotion_preserves_tier_fifo_across_context_rotation_and_recollection() {
    use crate::transport::rdma::promotion_policy;
    assert!(
        RdmaConfig {
            priority_promotion_timeout: Duration::ZERO,
            ..Default::default()
        }
        .validate()
        .is_ok()
    );
    let instant = Instant::now();
    assert!(promotion_policy::timed_out(
        instant,
        instant,
        Duration::ZERO
    ));
    let _serial = SERIAL.lock().unwrap();
    static VERBS: sys::Verbs = sys::Verbs {
        discover,
        post,
        ..fixture::VERBS
    };
    for per_entry in [false, true] {
        let mut runtime = fixture::runtime("priority_copy");
        runtime.config.block_size = 64 << 10;
        runtime.config.max_cqes = 16;
        runtime.config.max_endpoints = 8;
        runtime.config.max_staging_bytes = 128;
        runtime.config.priority_promotion_timeout = Duration::from_secs(60);
        runtime.verbs = &VERBS;
        let mut grants: Vec<_> = (0..4)
            .map(|_| runtime.staging_endpoint(8, 0).unwrap())
            .collect();
        for grant in &mut grants {
            grant.connect(&grant.descriptor()).unwrap();
        }
        let (lock, changed) = GATE.get_or_init(Default::default);
        *lock.lock().unwrap() = Gate {
            hold: true,
            ..Default::default()
        };
        let release = Release;
        grants[0].progress.shared.wake();
        let (gate, timeout) = changed
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(3), |g| !g.entered)
            .unwrap();
        assert!(!timeout.timed_out());
        drop(gate);
        let end = Instant::now() + Duration::from_secs(10);
        for (i, grant) in grants.iter().enumerate() {
            let priority = if i % 2 == 0 {
                Priority::Medium
            } else {
                Priority::Low
            };
            grant
                .progress
                .submit(
                    &grant.record,
                    grant.claim,
                    grant.descriptor(),
                    TransferOp::Read,
                    priority,
                    end,
                )
                .unwrap();
        }
        let work: Vec<_> = grants
            .iter()
            .map(|g| g.record.queued_work().unwrap())
            .collect();
        let cutoff = work[1].enqueued + work[2].enqueued.duration_since(work[1].enqueued) / 2;
        let mut queue = PostQueue::new(4);
        let now = Instant::now();
        let timeout = now.duration_since(cutoff);
        assert!(promotion_policy::timed_out(work[1].enqueued, now, timeout));
        assert!(!promotion_policy::timed_out(work[2].enqueued, now, timeout));
        assert!(!promotion_policy::timed_out(now, work[0].enqueued, timeout));
        assert!(promotion_policy::timed_out(now, now + timeout, timeout));
        let lanes: Vec<_> = (0..2)
            .map(|i| Lane {
                state: Arc::clone(&grants[i].progress.shared),
                records: vec![
                    Arc::clone(&grants[i + 2].record),
                    Arc::clone(&grants[i].record),
                ],
                cursor: 0,
            })
            .collect();
        struct Snapshots(Vec<Lane>);
        impl Drop for Snapshots {
            fn drop(&mut self) {
                // Borrowed snapshots never attached a lane: release fields
                // without invoking the real worker's detach protocol.
                for lane in self.0.drain(..) {
                    let mut snapshot = std::mem::ManuallyDrop::new(lane);
                    unsafe {
                        std::ptr::drop_in_place(&mut snapshot.records);
                        std::ptr::drop_in_place(&mut snapshot.state);
                    }
                }
            }
        }
        let snapshots = Snapshots(lanes);
        let lanes = &snapshots.0;
        queue.collect(lanes, 1);
        queue.promote(lanes, now, timeout, per_entry);
        if per_entry {
            assert_eq!(
                grants
                    .iter()
                    .map(|g| g.record.queued_work().unwrap().priority)
                    .collect::<Vec<_>>(),
                [
                    Priority::High,
                    Priority::Medium,
                    Priority::Medium,
                    Priority::Low
                ]
            );
            // The next check must append fresh MEDIUM before the previously promoted LOW.
            queue.clear();
            queue.collect(lanes, 0);
            queue.promote(lanes, now + promotion_policy::CHECK_INTERVAL, timeout, true);
        }
        for cursor in [0, 1] {
            queue.clear();
            queue.collect(lanes, cursor);
        }
        let old = &work[0];
        assert!(!grants[0].record.promote(
            old.namespace + 1,
            Priority::High,
            Priority::High,
            now,
            0
        ));
        assert!(queue.has_blocked(Priority::High));
        queue.dispatch(lanes, false, Priority::High);
        let expected: Vec<_> = if per_entry { vec![0, 2, 1] } else { vec![0, 2] };
        assert_eq!(
            lock.lock().unwrap().posted,
            expected
                .iter()
                .map(|&i| grants[i].descriptor().address)
                .collect::<Vec<_>>()
        );
        drop(snapshots);
        drop(release);
        for grant in &grants {
            grant
                .progress
                .wait(&grant.record, grant.claim, end)
                .unwrap();
        }
        for grant in &mut grants {
            grant.close().unwrap();
        }
        drop(grants);
        runtime.close(end).unwrap();
    }
}

#[test]
fn event_worker_wakes_for_promotion_and_shared_slots_without_a_cqe() {
    use crate::transport::rdma::shared_quota::SharedSlotManager;
    let _serial = SERIAL.lock().unwrap();
    static VERBS: sys::Verbs = sys::Verbs {
        discover,
        post,
        ..crate::transport::rdma::cq::tests::VERBS
    };
    for promotion in [true, false] {
        let name = std::ffi::CString::new(format!(
            "/orbitte-worker-slots-{}-{promotion}",
            std::process::id()
        ))
        .unwrap();
        struct Remove(std::ffi::CString);
        impl Drop for Remove {
            fn drop(&mut self) {
                unsafe {
                    libc::shm_unlink(self.0.as_ptr());
                }
            }
        }
        let cleanup = Remove(name);
        let mut clock = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        assert_eq!(
            unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut clock) },
            0
        );
        let nanos = clock.tv_sec as u64 * 1_000_000_000 + clock.tv_nsec as u64;
        // A long HIGH slot isolates the promotion timer from slot rotation.
        let interval = if promotion {
            Duration::from_nanos(nanos / 3)
        } else {
            Duration::from_millis(5)
        };
        let name = cleanup.0.to_str().unwrap();
        let slots = SharedSlotManager::attach(name, interval).unwrap();
        let mut runtime = fixture::event_runtime("priority_copy");
        runtime.config.block_size = 64 << 10;
        runtime.config.shared_quota_shm_path = Some(name.into());
        runtime.config.slot_rotation_interval = interval;
        runtime.config.priority_promotion_timeout = if promotion {
            Duration::from_millis(10)
        } else {
            Duration::from_secs(60)
        };
        runtime.config.priority_promotion_per_entry = true;
        runtime.verbs = &VERBS;
        let mut grant = runtime.staging_endpoint(8, 0).unwrap();
        grant.connect(&grant.descriptor()).unwrap();
        let (lock, changed) = GATE.get_or_init(Default::default);
        *lock.lock().unwrap() = Gate {
            hold: true,
            events: true,
            ..Default::default()
        };
        let release = Release;
        grant.progress.shared.wake();
        let (gate, timeout) = changed
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(3), |g| !g.entered)
            .unwrap();
        assert!(!timeout.timed_out());
        drop(gate);
        while slots.current_slot().unwrap().0 != Priority::High {
            thread::sleep(slots.current_slot().unwrap().1);
        }
        let end = Instant::now() + Duration::from_secs(3);
        grant
            .progress
            .submit(
                &grant.record,
                grant.claim,
                grant.descriptor(),
                TransferOp::Read,
                Priority::Low,
                end,
            )
            .unwrap();
        drop(release);
        // The only possible initial progress is a timer; no payload CQE exists yet.
        grant
            .progress
            .wait(
                &grant.record,
                grant.claim,
                Instant::now() + Duration::from_secs(1),
            )
            .unwrap();
        assert_eq!(lock.lock().unwrap().posted, [grant.descriptor().address]);
        if promotion {
            assert_eq!(slots.current_slot().unwrap().0, Priority::High);
        }
        grant.close().unwrap();
        drop(grant);
        runtime.close(end).unwrap();
        assert_eq!(runtime.stats().unwrap().work_requests, 0);
    }
}
