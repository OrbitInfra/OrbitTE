//! Public cold contention and bootstrap rollback; fake verbs prove ownership only.
use super::*;
use crate::engine::tests::rdma_fixture_with_config;
use crate::transport::rdma::tests::{pause, runtime};
use std::sync::Arc;

const PARENTS: usize = 8;
const LENGTH: usize = 32;
const WAIT: Duration = Duration::from_secs(10);

fn engine(mode: &str, lanes: usize) -> TransferEngine {
    let mut runtime = runtime(mode);
    runtime.config.num_lanes = lanes;
    runtime.config.max_endpoints = 32;
    runtime.config.max_direct_bytes = 4096;
    runtime.config.max_staging_bytes = 4096;
    runtime.config.max_cqes = 64;
    runtime.config.block_size = 8;
    rdma_fixture_with_config(
        runtime,
        crate::EngineConfig {
            handler_threads: PARENTS + 1,
            handler_timeout: WAIT,
            ..Default::default()
        },
    )
}

#[test]
fn simultaneous_first_parents_wait_for_fixed_width_then_share_without_reopening() {
    for width in [1, 3] {
        for operation in [TransferOp::Read, TransferOp::Write] {
            let source = engine("direct:copy", width);
            let requester = Arc::new(engine("direct:async_copy_cold_open_pause", width));
            let local = requester
                .register_memory(
                    vec![0x63; PARENTS * LENGTH].into_boxed_slice(),
                    Permission::LocalOnly,
                )
                .unwrap();
            let remote = source
                .register_memory(
                    vec![0x29; PARENTS * LENGTH].into_boxed_slice(),
                    Permission::RemoteReadWrite,
                )
                .unwrap();
            let peer = requester
                .open_segment(source.endpoint(), source.source_epoch(), WAIT)
                .unwrap();
            let runtime = source_runtime(&requester);
            let pause = pause(runtime);
            for round in 0..2 {
                let mut handles = Vec::new();
                for parent in 0..PARENTS {
                    let requester = Arc::clone(&requester);
                    let peer = peer.clone();
                    let request = TransferRequest {
                        remote_staging: false,
                        transport: Transport::Rdma,
                        local_address: local.base_address() + (parent * LENGTH) as u64,
                        remote_address: remote.base_address() + (parent * LENGTH) as u64,
                        length: LENGTH,
                    };
                    handles.push(thread::spawn(move || {
                        requester
                            .submit_and_wait(operation, &peer, &[request], WAIT)
                            .unwrap()
                    }));
                }
                if round == 0 {
                    let until = Instant::now() + WAIT;
                    loop {
                        let stats = runtime.stats().unwrap();
                        if stats.active_grants == PARENTS && stats.active_endpoints == width {
                            break;
                        }
                        assert!(
                            Instant::now() < until,
                            "cold parents did not reach their construction reservations: {stats:?}"
                        );
                        thread::sleep(Duration::from_millis(1));
                    }
                    assert_eq!(runtime.stats().unwrap().opened_connections, 0);
                    assert_eq!(source.stats().unwrap().rdma.unwrap().opened_connections, 0);
                    let (lock, changed) = &*pause;
                    lock.lock().unwrap().1 = true;
                    changed.notify_all();
                }
                for handle in handles {
                    let report = handle.join().unwrap();
                    assert!(report.is_complete() && report.locally_drained, "{report:?}");
                }
                for engine in [&*requester, &source] {
                    let stats = engine.stats().unwrap().rdma.unwrap();
                    assert_eq!(
                        (stats.opened_connections, stats.active_endpoints),
                        (width as u64, width)
                    );
                    assert_eq!((stats.closed_connections, stats.active_grants), (0, 0));
                }
                let mut bytes = vec![0; PARENTS * LENGTH];
                let target = if operation == TransferOp::Read {
                    &local
                } else {
                    &remote
                };
                target.read(0, &mut bytes).unwrap();
                assert_eq!(
                    bytes,
                    vec![
                        if operation == TransferOp::Read {
                            0x29
                        } else {
                            0x63
                        };
                        PARENTS * LENGTH
                    ]
                );
            }
            requester.close(WAIT).unwrap();
            source.close(WAIT).unwrap();
        }
    }
}

#[test]
fn failed_cold_accept_rolls_back_its_slot_before_retry() {
    let source = engine("direct:copy", 1);
    let memory = source
        .register_memory(
            vec![0x29; 8].into_boxed_slice(),
            Permission::RemoteReadWrite,
        )
        .unwrap();
    for attempt in 0..3 {
        let wire = request(source.source_epoch(), memory.id());
        let mut channel =
            ControlChannel::connect(source.endpoint(), deadline(WAIT).unwrap()).unwrap();
        channel.write_all(&wire.encode()).unwrap();
        let (paths, width) = path::read_paths(&mut channel, source.source_epoch()).unwrap();
        let requester = identity(9, 8, 7);
        Hello {
            requester,
            source: None,
            expected_domain: paths[0].domain,
            direct: true,
            qp_width: width,
            previous_grant: 0,
        }
        .write(&mut channel)
        .unwrap();
        assert_eq!(
            Reply::read(&mut channel, source.source_epoch())
                .unwrap()
                .length,
            OFFER_SIZE as u64
        );
        let mut offer = [0; OFFER_SIZE];
        channel.read_exact(&mut offer).unwrap();
        let local = Identity::decode(&offer[..IDENTITY_SIZE]).unwrap();
        let mut binding =
            Binding::new(&wire, local, requester, u64_at(&offer, IDENTITY_SIZE), 1).unwrap();
        binding.0[BINDING_SIZE - 9] ^= 1;
        binding.write(&mut channel, ACCEPT).unwrap();
        assert!(Reply::read(&mut channel, source.source_epoch()).is_err());
        let stats = source.stats().unwrap().rdma.unwrap();
        assert_eq!(
            (stats.opened_connections, stats.closed_connections),
            (attempt + 1, attempt + 1)
        );
        assert_eq!(
            (
                stats.active_grants,
                stats.active_endpoints,
                stats.completed_window_binds
            ),
            (0, 0, 0)
        );
    }
    source.close(WAIT).unwrap();
}

#[test]
fn simultaneous_bidirectional_first_contact_does_not_wait_on_the_opposite_role() {
    let a = Arc::new(engine("direct:copy", 1));
    let b = Arc::new(engine("direct:copy", 1));
    let am = a
        .register_memory(
            vec![0x29; PARENTS * LENGTH].into_boxed_slice(),
            Permission::RemoteRead,
        )
        .unwrap();
    let bm = b
        .register_memory(
            vec![0x63; PARENTS * LENGTH].into_boxed_slice(),
            Permission::RemoteRead,
        )
        .unwrap();
    let ad = a
        .register_memory(
            vec![0; PARENTS * LENGTH].into_boxed_slice(),
            Permission::LocalOnly,
        )
        .unwrap();
    let bd = b
        .register_memory(
            vec![0; PARENTS * LENGTH].into_boxed_slice(),
            Permission::LocalOnly,
        )
        .unwrap();
    let ab = a
        .open_segment(b.endpoint(), b.source_epoch(), WAIT)
        .unwrap();
    let ba = b
        .open_segment(a.endpoint(), a.source_epoch(), WAIT)
        .unwrap();
    let start = Arc::new(std::sync::Barrier::new(2 * PARENTS));
    let mut handles = Vec::new();
    for (engine, peer, local, remote) in [(&a, &ab, &ad, &bm), (&b, &ba, &bd, &am)] {
        for parent in 0..PARENTS {
            let engine = Arc::clone(engine);
            let peer = peer.clone();
            let start = Arc::clone(&start);
            let request = TransferRequest {
                remote_staging: false,
                transport: Transport::Rdma,
                local_address: local.base_address() + (parent * LENGTH) as u64,
                remote_address: remote.base_address() + (parent * LENGTH) as u64,
                length: LENGTH,
            };
            handles.push(thread::spawn(move || {
                start.wait();
                engine
                    .submit_and_wait(TransferOp::Read, &peer, &[request], WAIT)
                    .unwrap()
            }));
        }
    }
    for handle in handles {
        let report = handle.join().unwrap();
        assert!(report.is_complete() && report.locally_drained, "{report:?}");
    }
    for (engine, target, value) in [(&a, &ad, 0x63), (&b, &bd, 0x29)] {
        let mut bytes = vec![0; PARENTS * LENGTH];
        target.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, vec![value; PARENTS * LENGTH]);
        let stats = engine.stats().unwrap().rdma.unwrap();
        assert_eq!((stats.opened_connections, stats.closed_connections), (2, 0));
        engine.close(WAIT).unwrap();
    }
}
