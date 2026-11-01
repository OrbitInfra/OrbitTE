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
        runtime.config.max_endpoints = 8;
        runtime.config.max_staging_bytes = 128;
        runtime.config.max_cqes = 1;
        runtime.config.deadline_bw_arbitration = arbitrate;
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
