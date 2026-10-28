use super::*;
use crate::transport::rdma::tests::{events, on_requester_close};
use std::sync::{Arc, Mutex};

fn transfer(
    requester: &TransferEngine,
    source: &TransferEngine,
    local: &crate::RegisteredMemory,
    remote: &crate::RegisteredMemory,
    operation: TransferOp,
    offset: usize,
    length: usize,
) {
    let peer = requester
        .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
        .unwrap();
    let report = requester
        .submit_and_wait(
            operation,
            &peer,
            &[TransferRequest {
                transport: Transport::Rdma,
                local_address: local.base_address() + offset as u64,
                remote_address: remote.base_address() + offset as u64,
                length,
            }],
            TIMEOUT,
        )
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
}

fn bytes(memory: &crate::RegisteredMemory) -> Vec<u8> {
    let mut actual = vec![0; memory.len()];
    memory.read(0, &mut actual).unwrap();
    actual
}

#[test]
fn connected_pair_keeps_actual_qps_but_releases_every_grant_and_old_registration() {
    for mode in ["direct:copy", "events:direct:copy"] {
        for operation in [TransferOp::Read, TransferOp::Write] {
            let source = rdma_fixture(mode);
            let requester = rdma_fixture(mode);
            let mut local = requester
                .register_memory(vec![0x63; 8].into_boxed_slice(), Permission::LocalOnly)
                .unwrap();
            let mut remote = source
                .register_memory(
                    vec![0x29; 8].into_boxed_slice(),
                    Permission::RemoteReadWrite,
                )
                .unwrap();
            let observations = Arc::new(Mutex::new(Vec::new()));
            for round in 0..4 {
                let observed = Arc::clone(&observations);
                on_requester_close(move |endpoint, result| {
                    result.as_ref().unwrap();
                    assert!(endpoint.released);
                    let state = endpoint.record.lock().unwrap();
                    let resources = state.resources.as_ref().unwrap();
                    assert!(resources.grant.is_none());
                    assert!(state.phase == super::super::super::Phase::Ready);
                    assert!(!resources.connection.lock().unwrap().handles.qp.is_null());
                    let connection = resources.connection.lock().unwrap();
                    observed.lock().unwrap().push((
                        connection.handles.qp as usize,
                        endpoint.incarnation().unwrap(),
                        endpoint.local.connection_bytes(),
                        endpoint.cached_peer.unwrap(),
                        endpoint.claim,
                        Arc::clone(&connection.send_queue),
                    ));
                    drop(connection);
                    drop(state);
                    if round != 0 {
                        endpoint
                            .progress
                            .retire_idle(&endpoint.record, endpoint.claim - 1, endpoint.deadline)
                            .unwrap();
                        assert!(
                            !endpoint
                                .record
                                .terminal
                                .load(std::sync::atomic::Ordering::Acquire)
                        );
                        assert!(
                            endpoint.record.lock().unwrap().phase
                                == super::super::super::Phase::Ready
                        );
                    }
                });
                let (offset, length) = if round % 2 == 0 { (0, 8) } else { (2, 4) };
                local.write(0, &[0x63 + round; 8]).unwrap();
                remote.write(0, &[0x29 + round; 8]).unwrap();
                transfer(
                    &requester, &source, &local, &remote, operation, offset, length,
                );
                let expected = if operation == TransferOp::Read {
                    0x29 + round
                } else {
                    0x63 + round
                };
                let target = if operation == TransferOp::Read {
                    &local
                } else {
                    &remote
                };
                let actual = bytes(target);
                assert!(
                    actual[offset..offset + length]
                        .iter()
                        .all(|v| *v == expected)
                );
                for engine in [&requester, &source] {
                    let stats = engine.stats().unwrap().rdma.unwrap();
                    assert_eq!(
                        (
                            stats.active_endpoints,
                            stats.idle_endpoints,
                            stats.opened_connections,
                            stats.closed_connections
                        ),
                        (1, 1, 1, 0)
                    );
                    assert_eq!(stats.reused_connections, round as u64);
                    assert_eq!(stats.completed_reusable_grants, round as u64 + 1);
                    assert_eq!(
                        (
                            stats.active_grants,
                            stats.active_windows,
                            stats.direct_bytes,
                            stats.message_bytes,
                            stats.active_message_mrs,
                            stats.work_requests,
                            stats.receive_work_requests
                        ),
                        (0, 0, 0, 0, 0, 0, 0)
                    );
                    assert_eq!(
                        events(source_runtime(engine))
                            .lock()
                            .unwrap()
                            .iter()
                            .filter(|e| **e == "open_endpoint")
                            .count(),
                        1
                    );
                    assert!(
                        !events(source_runtime(engine))
                            .lock()
                            .unwrap()
                            .contains(&"destroy_qp")
                    );
                }
                if round == 1 {
                    // Actual parent deregistration while the native QP remains alive.
                    requester.unregister_memory(&local).unwrap();
                    source.unregister_memory(&remote).unwrap();
                    assert_eq!(requester.stats().unwrap().rdma.unwrap().persistent_mrs, 0);
                    assert_eq!(source.stats().unwrap().rdma.unwrap().persistent_mrs, 0);
                    local = requester
                        .register_memory(vec![0; 8].into_boxed_slice(), Permission::LocalOnly)
                        .unwrap();
                    remote = source
                        .register_memory(vec![0; 8].into_boxed_slice(), Permission::RemoteReadWrite)
                        .unwrap();
                }
            }
            let observed = observations.lock().unwrap();
            for pair in observed.windows(2) {
                assert_eq!(pair[0].0, pair[1].0);
                assert!(Arc::ptr_eq(&pair[0].5, &pair[1].5));
                assert_eq!(pair[0].5.outstanding(), 0);
                assert!(!pair[0].5.drained());
                assert_eq!(pair[0].1, pair[1].1);
                assert_eq!(pair[0].2, pair[1].2);
                assert!(pair[0].3.same_pair(&pair[1].3));
                assert!(pair[0].3.generation < pair[1].3.generation);
                assert_eq!(pair[0].4 + 1, pair[1].4);
            }
            for engine in [&requester, &source] {
                engine.close(TIMEOUT).unwrap();
                let stats = engine.stats().unwrap().rdma.unwrap();
                assert_eq!((stats.opened_connections, stats.closed_connections), (1, 1));
                assert_eq!(
                    (
                        stats.active_completion_channels,
                        stats.completion_event_sources
                    ),
                    (0, 0)
                );
                if mode.starts_with("events:") {
                    assert_eq!(stats.opened_completion_channels, 1);
                    assert!(stats.completion_events > 0 && stats.cq_notification_arms >= 2);
                } else {
                    assert_eq!(stats.opened_completion_channels, 0);
                }
                assert_eq!(
                    (
                        stats.active_endpoints,
                        stats.idle_endpoints,
                        stats.active_grants,
                        stats.active_cqs,
                        stats.persistent_mrs,
                        stats.active_devices,
                        stats.progress_workers
                    ),
                    (0, 0, 0, 0, 0, 0, 0)
                );
            }
        }
    }
}

#[test]
fn one_slot_cache_evicts_other_capability_and_reconnects_only_after_explicit_cache_miss() {
    let source = rdma_fixture("direct:copy");
    let requester = rdma_fixture("direct:copy");
    let local = requester
        .register_memory(vec![0x63; 8].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let remote = source
        .register_memory(
            vec![0x29; 8].into_boxed_slice(),
            Permission::RemoteReadWrite,
        )
        .unwrap();
    transfer(&requester, &source, &local, &remote, TransferOp::Read, 0, 8);
    // Evict only the source half; requester must first retire its stale half.
    let source_runtime = source_runtime(&source);
    source_runtime
        .progress(source_runtime.device().unwrap())
        .unwrap()
        .evict_idle(|_| true, deadline(TIMEOUT).unwrap())
        .unwrap();
    transfer(&requester, &source, &local, &remote, TransferOp::Read, 0, 8);
    for op in [TransferOp::Write, TransferOp::Read] {
        transfer(&requester, &source, &local, &remote, op, 0, 8);
    }
    for engine in [&requester, &source] {
        let stats = engine.stats().unwrap().rdma.unwrap();
        assert_eq!(
            (
                stats.active_endpoints,
                stats.idle_endpoints,
                stats.active_grants
            ),
            (1, 1, 0)
        );
        assert_eq!(stats.opened_connections, 4);
        assert_eq!(stats.closed_connections, 3);
        assert_eq!(stats.completed_reusable_grants, 4);
        engine.close(TIMEOUT).unwrap();
        assert_eq!(engine.stats().unwrap().rdma.unwrap().active_endpoints, 0);
    }
}

#[test]
fn only_one_claimant_can_own_a_ready_pair_and_old_handle_cannot_close_its_next_grant() {
    let source = rdma_fixture("direct:copy");
    let requester = rdma_fixture("direct:copy");
    let local = requester
        .register_memory(vec![0x63; 8].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let remote = source
        .register_memory(
            vec![0x29; 8].into_boxed_slice(),
            Permission::RemoteReadWrite,
        )
        .unwrap();
    let captured = Arc::new(Mutex::new(None));
    let capture = Arc::clone(&captured);
    on_requester_close(move |endpoint, result| {
        result.as_ref().unwrap();
        *capture.lock().unwrap() = Some((
            Arc::clone(&endpoint.record),
            endpoint.cached_peer.unwrap(),
            endpoint.local,
            endpoint.claim,
        ));
    });
    transfer(&requester, &source, &local, &remote, TransferOp::Read, 0, 8);
    let runtime = source_runtime(&requester);
    let progress = runtime.progress(runtime.device().unwrap()).unwrap();
    let barrier = std::sync::Barrier::new(3);
    let mut winners = thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..2 {
            let progress = &progress;
            let barrier = &barrier;
            handles.push(scope.spawn(move || {
                barrier.wait();
                progress
                    .claim(|_| true, deadline(TIMEOUT).unwrap())
                    .unwrap()
            }));
        }
        barrier.wait();
        handles
            .into_iter()
            .filter_map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(winners.len(), 1);
    let winner = winners.pop().unwrap();
    let mut winner = runtime
        .endpoint(&local, 0, 8, 0, deadline(TIMEOUT).unwrap(), Some(winner))
        .unwrap();
    let current_remote = winner
        .record
        .lock()
        .unwrap()
        .resources
        .as_ref()
        .unwrap()
        .connection
        .lock()
        .unwrap()
        .remote
        .unwrap();
    let (record, peer, descriptor, old_claim) = captured.lock().unwrap().take().unwrap();
    assert_eq!(winner.claim, old_claim + 1);
    let mut stale = RdmaGrant {
        record,
        progress: Arc::clone(&progress),
        local: descriptor,
        incarnation: Some(peer.local),
        config: runtime.config.clone(),
        deadline: deadline(TIMEOUT).unwrap(),
        output: None,
        claim: old_claim,
        released: false,
        cached_peer: Some(peer),
        endpoint: None,
        construction: None,
    };
    let log = events(runtime);
    let before = log.lock().unwrap().clone();
    assert!(matches!(
        stale.fill(
            &local,
            0,
            #[cfg(feature = "cuda")]
            &runtime.proxy
        ),
        Err(Error::Unavailable("stale RDMA grant handle"))
    ));
    winner
        .fill(
            &local,
            0,
            #[cfg(feature = "cuda")]
            &runtime.proxy,
        )
        .unwrap();
    assert!(matches!(
        stale.connect(&current_remote),
        Err(Error::Unavailable("stale RDMA grant handle"))
    ));
    assert!(winner.record.lock().unwrap().phase == super::super::super::Phase::Init);
    winner.connect(&current_remote).unwrap();
    assert!(matches!(
        stale.grant(),
        Err(Error::Unavailable("stale RDMA grant handle"))
    ));
    assert!(matches!(
        stale.transfer(
            &current_remote,
            TransferOp::Read,
            deadline(TIMEOUT).unwrap()
        ),
        Err(Error::Unavailable("stale RDMA grant handle"))
    ));
    assert!(winner.record.lock().unwrap().phase == super::super::super::Phase::Connected);
    stale.close().unwrap();
    drop(stale);
    assert_eq!(
        *log.lock().unwrap(),
        before,
        "old close/Drop must issue no native calls"
    );
    assert_eq!(runtime.stats().unwrap().active_endpoints, 1);
    assert!(
        winner
            .record
            .lock()
            .unwrap()
            .resources
            .as_ref()
            .unwrap()
            .grant
            .is_some()
    );
    winner.close().unwrap();
    drop(winner);
    requester.close(TIMEOUT).unwrap();
    source.close(TIMEOUT).unwrap();
}

#[test]
fn completion_requires_data_drain_exact_payload_key_and_exact_current_native_binding() {
    use crate::memory::MemoryRegistry;
    let runtime = crate::transport::rdma::tests::runtime("direct:ok");
    let registry = MemoryRegistry::new(SourceEpoch([4; 16]), 1, 8);
    let memory = registry
        .register(vec![0x33; 8].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let mut endpoint = runtime
        .endpoint(&memory, 0, 8, 0, deadline(TIMEOUT).unwrap(), None)
        .unwrap();
    let descriptor = endpoint.descriptor();
    endpoint.connect(&descriptor).unwrap();
    let requester = Identity::local(&endpoint, SourceEpoch([4; 16])).unwrap();
    let mut request = request(SourceEpoch([7; 16]), 1);
    request.operation = Operation::RdmaRead;
    let binding = Binding::new(&request, identity(7, 8, 9), requester, 1, 1).unwrap();
    assert!(endpoint.finish_sent(&binding.0).is_err());
    endpoint
        .transfer(&descriptor, TransferOp::Read, deadline(TIMEOUT).unwrap())
        .unwrap();
    assert!(endpoint.finish_sent(&binding.0).is_err());
    assert!(memory.read(0, &mut [0; 8]).is_err());
    let posts = runtime.stats().unwrap().post_calls;
    assert!(
        endpoint
            .send_completion(&binding.0, descriptor.rkey ^ 1)
            .is_err()
    );
    assert_eq!(runtime.stats().unwrap().post_calls, posts);
    endpoint
        .send_completion(&binding.0, descriptor.rkey)
        .unwrap();
    for index in [
        10,
        11,
        28,
        36,
        44,
        REQUEST_SIZE,
        REQUEST_SIZE + 16,
        REQUEST_SIZE + 32,
        REQUEST_SIZE + IDENTITY_SIZE,
        REQUEST_SIZE + IDENTITY_SIZE + 16,
        BINDING_SIZE - 9,
        BINDING_SIZE - 1,
    ] {
        let mut changed = binding.0;
        changed[index] ^= 1;
        assert!(
            endpoint.finish_sent(&changed).is_err(),
            "binding byte {index}"
        );
        assert!(memory.read(0, &mut [0; 8]).is_err());
    }
    endpoint.finish_sent(&binding.0).unwrap();
    memory.read(0, &mut [0; 8]).unwrap();
    let stats = runtime.stats().unwrap();
    assert_eq!(
        (
            stats.active_endpoints,
            stats.active_grants,
            stats.message_bytes,
            stats.active_message_mrs
        ),
        (1, 0, 0, 0)
    );
    endpoint.close().unwrap();
    drop(endpoint);
    runtime.close(deadline(TIMEOUT).unwrap()).unwrap();
}

#[test]
fn wrong_ack_or_ready_retires_requester_without_replaying_a_completed_write() {
    for (phase, index) in [
        (ACK, 0),
        (ACK, BINDING_SIZE - 9),
        (ACK, BINDING_SIZE),
        (READY, 0),
        (READY, BINDING_SIZE - 9),
        (READY, BINDING_SIZE),
    ] {
        let source = rdma_fixture("direct:copy");
        let requester = rdma_fixture("direct:copy");
        let local = requester
            .register_memory(vec![0x63; 8].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let remote = source
            .register_memory(
                vec![0x29; 8].into_boxed_slice(),
                Permission::RemoteReadWrite,
            )
            .unwrap();
        let mut peer = requester
            .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        peer.endpoint = listener.local_addr().unwrap();
        let source_address = source.endpoint();
        let proxy = thread::spawn(move || {
            let mut client = ControlChannel {
                stream: listener.accept().unwrap().0,
                deadline: deadline(TIMEOUT).unwrap(),
                stopping: None,
            };
            let mut server =
                ControlChannel::connect(source_address, deadline(TIMEOUT).unwrap()).unwrap();
            let mut relay = |length: usize, to_server: bool| {
                let mut bytes = vec![0; length];
                if to_server {
                    client.read_exact(&mut bytes).unwrap();
                    server.write_all(&bytes).unwrap();
                } else {
                    server.read_exact(&mut bytes).unwrap();
                    client.write_all(&bytes).unwrap();
                }
            };
            relay(REQUEST_SIZE, true);
            relay(37 + path::PATH_SIZE, false);
            relay(HELLO_SIZE, true);
            relay(36 + OFFER_SIZE, false);
            relay(1 + BINDING_SIZE + DESCRIPTOR_SIZE, true);
            relay(36 + 1 + BINDING_SIZE + DESCRIPTOR_SIZE, false);
            relay(1 + BINDING_SIZE, true);
            let mut final_frames = [0; 36 + 2 * (1 + BINDING_SIZE)];
            server.read_exact(&mut final_frames).unwrap();
            let start = 36 + if phase == READY { 1 + BINDING_SIZE } else { 0 };
            final_frames[start + index] ^= 1;
            client.write_all(&final_frames).unwrap();
        });
        let report = requester
            .submit_and_wait(
                TransferOp::Write,
                &peer,
                &[TransferRequest {
                    transport: Transport::Rdma,
                    local_address: local.base_address(),
                    remote_address: remote.base_address(),
                    length: 8,
                }],
                TIMEOUT,
            )
            .unwrap();
        proxy.join().unwrap();
        let failure = report.failure.unwrap();
        assert!(
            matches!(failure.error, Error::Protocol(_)),
            "phase {phase} byte {index}: {failure:?}"
        );
        assert_eq!(failure.remote_effect, RemoteEffect::Unknown);
        assert!(report.locally_drained);
        assert_eq!(bytes(&remote), [0x63; 8]);
        let stats = requester.stats().unwrap().rdma.unwrap();
        assert_eq!(
            (
                stats.active_endpoints,
                stats.idle_endpoints,
                stats.active_grants
            ),
            (0, 0, 0)
        );
        assert_eq!(
            (
                stats.opened_connections,
                stats.closed_connections,
                stats.completed_writes,
                stats.completed_sends
            ),
            (1, 1, 1, 1)
        );
        requester.close(TIMEOUT).unwrap();
        source.close(TIMEOUT).unwrap();
    }
}

#[test]
fn source_never_returns_ready_for_only_tcp_done_or_only_native_receive() {
    use crate::memory::MemoryRegistry;
    for native_receive in [false, true] {
        let source = rdma_fixture("direct:async_copy");
        let remote = source
            .register_memory(
                vec![0x29; 8].into_boxed_slice(),
                Permission::RemoteReadWrite,
            )
            .unwrap();
        let requester = crate::transport::rdma::tests::runtime("direct:copy");
        let registry = MemoryRegistry::new(SourceEpoch([9; 16]), 1, 8);
        let local = registry
            .register(vec![0x63; 8].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let mut endpoint = requester
            .endpoint(&local, 0, 8, 0, deadline(TIMEOUT).unwrap(), None)
            .unwrap();
        let wire = request(source.source_epoch(), remote.id());
        let mut channel =
            ControlChannel::connect(source.endpoint(), deadline(TIMEOUT).unwrap()).unwrap();
        channel.write_all(&wire.encode()).unwrap();
        let (paths, _) = path::read_paths(&mut channel, source.source_epoch()).unwrap();
        let local_identity = Identity::local(&endpoint, SourceEpoch([9; 16])).unwrap();
        Hello {
            requester: local_identity,
            source: None,
            expected_domain: paths[0].domain,
            direct: true,
            qp_width: 1,
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
        assert_eq!(offer[OFFER_SIZE - 1], 1);
        let binding = Binding::new(
            &wire,
            Identity::decode(&offer[..IDENTITY_SIZE]).unwrap(),
            local_identity,
            u64_at(&offer, IDENTITY_SIZE),
            1,
        )
        .unwrap();
        binding.write(&mut channel, ACCEPT).unwrap();
        channel.write_all(&endpoint.connection()).unwrap();
        let mut descriptor = [0; DESCRIPTOR_SIZE];
        descriptor.copy_from_slice(&offer[IDENTITY_SIZE + 8..OFFER_SIZE - 1]);
        endpoint
            .connect(&Descriptor::decode_connection(&descriptor).unwrap())
            .unwrap();
        assert_eq!(
            Reply::read(&mut channel, source.source_epoch())
                .unwrap()
                .status,
            9
        );
        binding.read(&mut channel, GRANT).unwrap();
        channel.read_exact(&mut descriptor).unwrap();
        let grant = Descriptor::decode(&descriptor).unwrap();
        if native_receive {
            endpoint
                .transfer(&grant, TransferOp::Write, deadline(TIMEOUT).unwrap())
                .unwrap();
            endpoint.send_completion(&binding.0, grant.rkey).unwrap();
            endpoint.finish_sent(&binding.0).unwrap();
            let end = std::time::Instant::now() + TIMEOUT;
            while source.stats().unwrap().rdma.unwrap().completed_receives != 1 {
                assert!(std::time::Instant::now() < end);
                thread::yield_now();
            }
        } else {
            binding.write(&mut channel, DONE).unwrap();
        }
        let stats = source.stats().unwrap().rdma.unwrap();
        assert_eq!(
            (
                stats.active_endpoints,
                stats.active_grants,
                stats.idle_endpoints,
                stats.active_windows
            ),
            (1, 1, 0, 1)
        );
        assert_eq!(stats.completed_reusable_grants, 0);
        assert!(
            remote.read(0, &mut [0; 8]).is_err(),
            "only one half of the receipt must keep the exact source loan"
        );
        drop(channel);
        if !native_receive {
            crate::transport::rdma::context::tests::emit_async(source_runtime(&source), 3);
        }
        endpoint.close().unwrap();
        drop(endpoint);
        requester.close(deadline(TIMEOUT).unwrap()).unwrap();
        source.close(TIMEOUT).unwrap();
        let stats = source.stats().unwrap().rdma.unwrap();
        assert_eq!(
            (
                stats.active_endpoints,
                stats.active_grants,
                stats.idle_endpoints
            ),
            (0, 0, 0)
        );
        assert_eq!((stats.opened_connections, stats.closed_connections), (1, 1));
        assert_eq!(stats.completed_reusable_grants, 0);
    }
}

#[test]
fn invalid_rendezvous_and_unknown_cached_pair_never_create_new_authority() {
    for case in 0..7 {
        let source = rdma_fixture("direct:copy");
        let remote = source
            .register_memory(
                vec![0x29; 8].into_boxed_slice(),
                Permission::RemoteReadWrite,
            )
            .unwrap();
        let runtime = source_runtime(&source);
        let domain = runtime.device().unwrap().domain().unwrap();
        let mut hello = Hello {
            requester: identity(9, 8, 7),
            source: None,
            expected_domain: domain,
            direct: true,
            qp_width: 1,
            previous_grant: 0,
        };
        match case {
            0 => hello.expected_domain = [0xff; 16],
            1 => hello.requester.epoch = SourceEpoch([0; 16]),
            6 => {}
            _ => {
                hello.source = Some(Identity {
                    epoch: source.source_epoch(),
                    connection: ConnectionId {
                        domain,
                        generation: 999,
                    },
                });
                hello.expected_domain = domain;
                hello.previous_grant = 1;
                if case == 2 {
                    hello.direct = false;
                }
                if case == 3 {
                    hello.previous_grant = 0;
                }
                if case == 5 {
                    hello.source.as_mut().unwrap().epoch.0[0] ^= 1;
                }
            }
        }
        let wire = request(source.source_epoch(), remote.id());
        let mut channel =
            ControlChannel::connect(source.endpoint(), deadline(TIMEOUT).unwrap()).unwrap();
        channel.write_all(&wire.encode()).unwrap();
        path::read_paths(&mut channel, source.source_epoch()).unwrap();
        if case == 6 {
            let mut bytes = [0; HELLO_SIZE];
            bytes[0] = 5;
            bytes[1..1 + IDENTITY_SIZE].copy_from_slice(&hello.requester.encode());
            channel.write_all(&bytes).unwrap();
        } else {
            hello.write(&mut channel).unwrap();
        }
        let reply = Reply::read(&mut channel, source.source_epoch());
        if case == 4 {
            assert_eq!(reply.unwrap().status, CACHE_MISS);
        } else if case == 5 {
            assert!(matches!(reply, Err(Error::EpochMismatch)), "case {case}");
        } else {
            assert!(matches!(reply, Err(Error::Protocol(_))), "case {case}");
        }
        let stats = source.stats().unwrap().rdma.unwrap();
        assert_eq!(
            (
                stats.opened_connections,
                stats.opened_windows,
                stats.burned_window_keys,
                stats.completed_window_binds
            ),
            (0, 0, 0, 0)
        );
        remote.read(0, &mut [0; 8]).unwrap();
        drop(channel);
        source.close(TIMEOUT).unwrap();
    }
}

#[test]
fn reused_offer_and_fresh_after_miss_cannot_replay_grants_or_refresh_the_expected_pd() {
    for changed in ["grant", "connection", "domain", "table_domain"] {
        let changed_domain = changed == "domain";
        let source = rdma_fixture("direct:copy");
        let requester = rdma_fixture("direct:copy");
        let local = requester
            .register_memory(vec![0x63; 8].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let remote = source
            .register_memory(
                vec![0x29; 8].into_boxed_slice(),
                Permission::RemoteReadWrite,
            )
            .unwrap();
        let mut peer = requester
            .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        peer.endpoint = listener.local_addr().unwrap();
        let target = source.endpoint();
        let epoch = source.source_epoch();
        let proxy = thread::spawn(move || {
            let mut old_grant = 0;
            for attempt in 0..if changed_domain { 3 } else { 2 } {
                let mut client = ControlChannel {
                    stream: listener.accept().unwrap().0,
                    deadline: deadline(TIMEOUT).unwrap(),
                    stopping: None,
                };
                let mut server =
                    ControlChannel::connect(target, deadline(TIMEOUT).unwrap()).unwrap();
                let mut hello = [0; REQUEST_SIZE + HELLO_SIZE];
                client.read_exact(&mut hello[..REQUEST_SIZE]).unwrap();
                server.write_all(&hello[..REQUEST_SIZE]).unwrap();
                let (mut paths, width) = path::read_paths(&mut server, epoch).unwrap();
                if changed == "table_domain" && attempt == 1 {
                    paths[0].domain = [0xff; 16];
                }
                path::write_paths(&mut client, epoch, &paths, width).unwrap();
                if changed == "table_domain" && attempt == 1 {
                    expect_closed_without_next_phase(&mut client);
                    break;
                }
                client.read_exact(&mut hello[REQUEST_SIZE..]).unwrap();
                server.write_all(&hello[REQUEST_SIZE..]).unwrap();
                let reply = Reply::read(&mut server, epoch).unwrap();
                client.write_all(&reply.encode()).unwrap();
                if changed_domain && attempt == 1 {
                    assert_eq!(reply.status, CACHE_MISS);
                    assert_eq!(hello[REQUEST_SIZE] & 2, 2);
                    continue;
                }
                assert_eq!(reply.length, OFFER_SIZE as u64);
                let mut offer = [0; OFFER_SIZE];
                server.read_exact(&mut offer).unwrap();
                if attempt == 0 {
                    old_grant = u64_at(&offer, IDENTITY_SIZE);
                } else if changed_domain {
                    assert_eq!(hello[REQUEST_SIZE] & 2, 0);
                    assert_eq!(
                        &hello[REQUEST_SIZE + HELLO_SIZE - 24..REQUEST_SIZE + HELLO_SIZE - 8],
                        &offer[16..32]
                    );
                    offer[16..32].fill(0xff);
                } else if changed == "connection" {
                    let generation = u64_at(&offer, 32).checked_add(1).unwrap();
                    offer[32..40].copy_from_slice(&generation.to_be_bytes());
                } else {
                    offer[IDENTITY_SIZE..IDENTITY_SIZE + 8]
                        .copy_from_slice(&old_grant.to_be_bytes());
                }
                client.write_all(&offer).unwrap();
                if attempt != 0 {
                    expect_closed_without_next_phase(&mut client);
                    break;
                }
                let mut relay = |length: usize, to_server: bool| {
                    let mut bytes = vec![0; length];
                    if to_server {
                        client.read_exact(&mut bytes).unwrap();
                        server.write_all(&bytes).unwrap();
                    } else {
                        server.read_exact(&mut bytes).unwrap();
                        client.write_all(&bytes).unwrap();
                    }
                };
                relay(1 + BINDING_SIZE + DESCRIPTOR_SIZE, true);
                relay(36 + 1 + BINDING_SIZE + DESCRIPTOR_SIZE, false);
                relay(1 + BINDING_SIZE, true);
                relay(36 + 2 * (1 + BINDING_SIZE), false);
            }
        });
        let write = || {
            requester
                .submit_and_wait(
                    TransferOp::Write,
                    &peer,
                    &[TransferRequest {
                        transport: Transport::Rdma,
                        local_address: local.base_address(),
                        remote_address: remote.base_address(),
                        length: 8,
                    }],
                    TIMEOUT,
                )
                .unwrap()
        };
        assert!(write().is_complete());
        if changed_domain {
            let runtime = source_runtime(&source);
            assert!(
                runtime
                    .progress(runtime.device().unwrap())
                    .unwrap()
                    .evict_idle(|_| true, deadline(TIMEOUT).unwrap())
                    .unwrap()
            );
        }
        local.write(0, &[0x72; 8]).unwrap();
        let report = write();
        proxy.join().unwrap();
        assert!(report.locally_drained);
        let failure = report.failure.unwrap();
        assert!(matches!(failure.error, Error::Protocol(_)), "{failure:?}");
        assert_eq!(failure.remote_effect, RemoteEffect::None);
        source.close(TIMEOUT).unwrap();
        assert_eq!(bytes(&remote), [0x63; 8]);
        assert_eq!(
            source.stats().unwrap().rdma.unwrap().completed_window_binds,
            1
        );
        let stats = requester.stats().unwrap().rdma.unwrap();
        if changed == "table_domain" {
            assert_eq!(
                stats.opened_direct_endpoints, 1,
                "a replacement table must fail before creating another grant"
            );
        }
        assert_eq!(
            (
                stats.completed_writes,
                stats.completed_sends,
                stats.reused_connections
            ),
            (1, 1, u64::from(changed != "table_domain"))
        );
        assert_eq!(stats.opened_connections, if changed_domain { 2 } else { 1 });
        assert_eq!(stats.active_grants, 0);
        assert_eq!(stats.idle_endpoints, usize::from(changed == "table_domain"));
        requester.close(TIMEOUT).unwrap();
        let stats = requester.stats().unwrap().rdma.unwrap();
        assert_eq!(stats.opened_connections, stats.closed_connections);
    }
}

#[test]
fn a_real_idle_source_pair_requires_both_identities_last_grant_and_operation() {
    let source = rdma_fixture("direct:copy");
    let requester = rdma_fixture("direct:copy");
    let local = requester
        .register_memory(vec![0x63; 8].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let remote = source
        .register_memory(
            vec![0x29; 8].into_boxed_slice(),
            Permission::RemoteReadWrite,
        )
        .unwrap();
    let captured = Arc::new(Mutex::new(None));
    let capture = Arc::clone(&captured);
    on_requester_close(move |endpoint, result| {
        result.as_ref().unwrap();
        *capture.lock().unwrap() = endpoint.cached_peer;
    });
    transfer(&requester, &source, &local, &remote, TransferOp::Read, 0, 8);
    let peer = captured.lock().unwrap().take().unwrap();
    for changed in [
        "source connection",
        "requester epoch",
        "requester domain",
        "requester connection",
        "previous grant",
        "operation",
    ] {
        let mut hello = Hello {
            requester: Identity {
                epoch: requester.source_epoch(),
                connection: peer.local,
            },
            source: Some(Identity {
                epoch: source.source_epoch(),
                connection: peer.connection,
            }),
            expected_domain: peer.connection.domain,
            direct: true,
            qp_width: 1,
            previous_grant: peer.generation,
        };
        let mut wire = request(source.source_epoch(), remote.id());
        wire.operation = Operation::RdmaRead;
        match changed {
            "source connection" => hello.source.as_mut().unwrap().connection.generation += 1,
            "requester epoch" => hello.requester.epoch.0[0] ^= 1,
            "requester domain" => hello.requester.connection.domain[0] ^= 1,
            "requester connection" => hello.requester.connection.generation += 1,
            "previous grant" => hello.previous_grant += 1,
            "operation" => wire.operation = Operation::RdmaWrite,
            _ => unreachable!(),
        }
        let mut channel =
            ControlChannel::connect(source.endpoint(), deadline(TIMEOUT).unwrap()).unwrap();
        channel.write_all(&wire.encode()).unwrap();
        path::read_paths(&mut channel, source.source_epoch()).unwrap();
        hello.write(&mut channel).unwrap();
        let reply = Reply::read(&mut channel, source.source_epoch()).unwrap();
        assert_eq!((reply.status, reply.length), (CACHE_MISS, 0), "{changed}");
        drop(channel);
        // CACHE_MISS can reach the requester before the handler drops its
        // temporary admission lease. Observe handler completion before charges.
        let end = Instant::now() + TIMEOUT;
        while source.stats().unwrap().active_source_operations != 0 {
            assert!(Instant::now() < end, "cache-miss handler did not finish");
            thread::sleep(Duration::from_millis(1));
        }
        let stats = source.stats().unwrap().rdma.unwrap();
        assert_eq!(
            (
                stats.opened_connections,
                stats.opened_windows,
                stats.burned_window_keys,
                stats.completed_window_binds,
                stats.reused_connections
            ),
            (1, 1, 1, 1, 0),
            "{changed} must not create fresh authority or claim the actual idle pair"
        );
        assert_eq!(
            (
                stats.active_grants,
                stats.direct_bytes,
                stats.idle_endpoints
            ),
            (0, 0, 1)
        );
    }
    transfer(&requester, &source, &local, &remote, TransferOp::Read, 0, 8);
    assert_eq!(bytes(&local), [0x29; 8]);
    for engine in [&requester, &source] {
        let stats = engine.stats().unwrap().rdma.unwrap();
        assert_eq!((stats.opened_connections, stats.reused_connections), (1, 1));
        engine.close(TIMEOUT).unwrap();
    }
}

#[test]
fn ready_reuse_revalidates_each_peer_route_before_claim_without_replacing_the_pd() {
    for source_fault in [false, true] {
        let source = rdma_fixture("direct:copy");
        let requester = rdma_fixture("direct:copy");
        assert_eq!(source.topology().unwrap(), None);
        let local = requester
            .register_memory(vec![0x63; 8].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let remote = source
            .register_memory(
                vec![0x29; 8].into_boxed_slice(),
                Permission::RemoteReadWrite,
            )
            .unwrap();
        transfer(&requester, &source, &local, &remote, TransferOp::Read, 0, 8);
        let faulted = if source_fault { &source } else { &requester };
        let runtime = source_runtime(faulted);
        let before = runtime.stats().unwrap();
        assert_eq!((before.opened_connections, before.idle_endpoints), (1, 1));
        crate::transport::rdma::context::tests::change_route(runtime, |route| {
            route.net_device_index += 1
        });
        let peer = requester
            .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
            .unwrap();
        let report = requester
            .submit_and_wait(
                TransferOp::Read,
                &peer,
                &[TransferRequest {
                    transport: Transport::Rdma,
                    local_address: local.base_address(),
                    remote_address: remote.base_address(),
                    length: 8,
                }],
                TIMEOUT,
            )
            .unwrap();
        assert!(!report.is_complete());
        let after = runtime.stats().unwrap();
        assert_eq!(
            (
                after.opened_devices,
                after.opened_connections,
                after.reused_connections
            ),
            (
                before.opened_devices,
                before.opened_connections,
                before.reused_connections
            )
        );
        for engine in [&requester, &source] {
            let close = engine.close(TIMEOUT);
            assert!(
                !matches!(close, Err(Error::Unquiesced { .. } | Error::Timeout)),
                "{close:?}"
            );
            let stats = source_runtime(engine).stats().unwrap();
            assert_eq!(
                (
                    stats.active_devices,
                    stats.active_endpoints,
                    stats.active_grants,
                    stats.active_windows,
                    stats.persistent_mrs
                ),
                (0, 0, 0, 0, 0)
            );
        }
    }
}

#[test]
fn unattached_shared_claim_cannot_evict_or_fence_a_live_sibling() {
    for attached in [false, true] {
        let source = rdma_fixture("direct:copy");
        let requester = rdma_fixture("direct:copy");
        let local = requester
            .register_memory(vec![0x63; 8].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let remote = source
            .register_memory(
                vec![0x29; 8].into_boxed_slice(),
                Permission::RemoteReadWrite,
            )
            .unwrap();
        transfer(&requester, &source, &local, &remote, TransferOp::Read, 0, 8);
        let runtime = source_runtime(&requester);
        let end = deadline(TIMEOUT).unwrap();
        let mut active = if attached {
            let cached = runtime
                .claim(runtime.device().unwrap(), |peer| !peer.source, end)
                .unwrap()
                .unwrap();
            Some(
                runtime
                    .endpoint(&local, 0, 8, 0, end, Some(cached))
                    .unwrap(),
            )
        } else {
            None
        };
        let owner = runtime.context_set.lock().unwrap().slots[0]
            .endpoints
            .snapshot()
            .unwrap()
            .remove(0);
        let (lane, record, peer) = owner.candidates(&|_| true).unwrap().remove(0);
        let placeholder = lane.share(&record, peer, end).unwrap();
        assert_eq!(runtime.stats().unwrap().idle_endpoints, 0);
        assert!(
            !lane.evict_idle(|_| true, end).unwrap(),
            "a shared active claim makes the physical QP busy"
        );
        drop(placeholder);
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.active_endpoints,
                stats.closed_connections,
                stats.idle_endpoints
            ),
            (1, 0, usize::from(!attached))
        );
        assert!(record.connection.require_live().is_ok());
        if let Some(grant) = &mut active {
            grant.close().unwrap();
        }
        drop(active);
        requester.close(TIMEOUT).unwrap();
        source.close(TIMEOUT).unwrap();
    }
}
