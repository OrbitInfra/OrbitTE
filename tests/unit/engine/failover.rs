use super::*;
use crate::Permission;
use crate::test_support::EVENTUAL_PROGRESS as WAIT;
#[cfg(feature = "rdma")]
use crate::{BatchState, TaskState};

#[test]
fn missing_descriptor_refreshes_once_in_the_original_epoch_before_admission() -> Result<()> {
    let source = TransferEngine::new(Default::default())?;
    let requester = TransferEngine::new(Default::default())?;
    let empty = requester.open_segment(source.endpoint(), source.source_epoch(), WAIT)?;
    let remote =
        source.register_memory(vec![8, 1, 2, 3, 4, 9].into(), Permission::RemoteReadWrite)?;
    let local = requester.register_memory(vec![0xee; 6].into(), Permission::LocalOnly)?;
    let r = TransferRequest::new(
        Transport::Tcp,
        local.base_address() + 1,
        remote.base_address() + 1,
        4,
    );
    assert!(
        requester
            .submit_and_wait(TransferOp::Read, &empty, std::slice::from_ref(&r), WAIT)?
            .is_complete()
    );
    let mut bytes = [0; 6];
    local.read(0, &mut bytes)?;
    assert_eq!(bytes, [0xee, 1, 2, 3, 4, 0xee]);
    let mut stale = empty.clone();
    stale.epoch = SourceEpoch::generate()?;
    requester.bind_test_peer(&mut stale)?;
    assert!(matches!(
        requester.submit_and_wait(TransferOp::Write, &stale, &[r], WAIT),
        Err(Error::EpochMismatch)
    ));
    remote.read(0, &mut bytes)?;
    assert_eq!(bytes, [8, 1, 2, 3, 4, 9]);
    Ok(())
}

#[cfg(feature = "rdma")]
mod native {
    use super::*;
    use crate::engine::tests::rdma_fixture_with_config;
    use crate::transport::rdma::tests as verbs;

    struct Pair {
        _source: TransferEngine,
        engine: Arc<TransferEngine>,
        peer: RemoteSegment,
        local: RegisteredMemory,
        remote: RegisteredMemory,
    }
    impl Pair {
        fn new(mode: &str, budget: usize, auto: bool, queued: bool) -> Result<Self> {
            let source = rdma_fixture_with_config(
                verbs::runtime(if mode == "direct:copy_fault_partial" {
                    "direct:copy"
                } else {
                    "copy"
                }),
                Default::default(),
            );
            let cfg = EngineConfig {
                max_failover_attempts: budget,
                enable_auto_failover_on_poll: auto,
                runtime_queue: queued.then_some(Default::default()),
                ..Default::default()
            };
            let r = verbs::runtime(mode);
            let engine = Arc::new(rdma_fixture_with_config(r, cfg));
            let local = engine.register_memory(vec![0xee; 12].into(), Permission::LocalOnly)?;
            let remote = source.register_memory(
                (0..12u8).collect::<Vec<_>>().into(),
                Permission::RemoteReadWrite,
            )?;
            let peer = engine.open_segment(source.endpoint(), source.source_epoch(), WAIT)?;
            Ok(Self {
                _source: source,
                engine,
                peer,
                local,
                remote,
            })
        }
        fn request(&self, offset: u64, length: usize) -> TransferRequest {
            TransferRequest::new(
                Transport::Rdma,
                self.local.base_address() + offset,
                self.remote.base_address() + offset,
                length,
            )
        }
        fn bytes(&self, moved: bool) -> Result<()> {
            let mut got = [0; 12];
            self.local.read(0, &mut got)?;
            let mut expected = [0xee; 12];
            if moved {
                expected[2..10].copy_from_slice(&[2, 3, 4, 5, 6, 7, 8, 9]);
            }
            assert_eq!(got, expected);
            self._source.close_source(WAIT)?;
            self.remote.read(0, &mut got)?;
            assert_eq!(got, std::array::from_fn(|i| i as u8));
            Ok(())
        }
    }

    #[test]
    fn classic_scatter_never_replays_a_rejected_rdma_owner_on_tcp() -> Result<()> {
        let p = Pair::new("direct:connect", 3, true, true)?;
        let b = p.engine.submit_async(
            TransferOp::Read,
            &p.peer,
            &[p.request(2, 8).requiring_post_submit_cancellation()],
            None,
            WAIT,
        )?;
        assert_eq!(b.wait(WAIT)?.state, BatchState::Failed);
        let task = b.task_status(0)?;
        assert_eq!((task.transport, task.failover_count), (Transport::Rdma, 0));
        p.bytes(false)
    }
    #[test]
    fn mixed_send_receive_debt_keeps_unposted_constructors_fail_stop() {
        let error = verbs::mixed_debt_constructor_error();
        assert!(
            matches!(error, Error::Internal("RDMA close with unresolved work")),
            "{error:?}"
        );
        assert!(
            !retryable(&error),
            "an unposted constructor must not replay this anomaly"
        );
    }

    #[test]
    fn submit_rejection_walks_the_pinned_policy_once_per_physical_owner() -> Result<()> {
        for (budget, queued) in [(0, false), (1, false), (3, true)] {
            let p = Pair::new("direct:connect", budget, false, queued)?;
            let batch = p.engine.submit_async(
                TransferOp::Read,
                &p.peer,
                &[p.request(6, 4), p.request(2, 4)],
                None,
                WAIT,
            )?;
            let success = budget != 0;
            assert_eq!(
                batch.wait(WAIT)?.state,
                if success {
                    BatchState::Completed
                } else {
                    BatchState::Failed
                }
            );
            let stats = p.engine.stats()?.rdma.unwrap();
            assert_eq!(stats.post_calls, 0);
            for task in batch.task_statuses()? {
                assert_eq!(
                    (task.physical_owner, task.transport, task.failover_count),
                    (
                        0,
                        if success {
                            Transport::Tcp
                        } else {
                            Transport::Rdma
                        },
                        usize::from(success)
                    )
                );
            }
            p.bytes(success)?;
        }
        let p = Pair::new("direct:connect", 1, true, false)?;
        let started = Instant::now();
        let batch = p.engine.submit_async(
            TransferOp::Read,
            &p.peer,
            &[p.request(2, 4), p.request(7, 3)],
            None,
            WAIT,
        )?;
        let waited = batch.wait(WAIT);
        // A completed count alone cannot distinguish a failed attempt from a
        // publication/alias bug. Keep the terminal errors and physical owners
        // in any future failure, without polling early or relaxing the receipt.
        assert_eq!(
            waited.as_ref().ok().map(|s| s.completed_tasks),
            Some(2),
            "after {:?}: wait: {waited:?}; tasks: {:?}; requester: {:?}; source: {:?}",
            started.elapsed(),
            batch.task_statuses(),
            p.engine.stats(),
            p._source.stats(),
        );
        for (i, t) in batch.task_statuses()?.iter().enumerate() {
            assert_eq!((t.physical_owner, t.failover_count), (i, 1));
        }
        let mut bytes = [0; 12];
        p.local.read(0, &mut bytes)?;
        assert_eq!(bytes, [0xee, 0xee, 2, 3, 4, 5, 0xee, 7, 8, 9, 0xee, 0xee]);
        Ok(())
    }

    #[test]
    fn source_receive_retirement_keeps_owners_and_allows_unposted_policy_fallback() -> Result<()> {
        struct Hold(Arc<(Mutex<(bool, bool)>, std::sync::Condvar)>);
        impl Drop for Hold {
            fn drop(&mut self) {
                self.0.0.lock().unwrap().1 = true;
                self.0.1.notify_all();
            }
        }
        let runtime = verbs::runtime_with_config(crate::transport::rdma::RdmaConfig {
            max_endpoints: 2,
            max_staging_bytes: 16,
            host_memory: crate::transport::rdma::RdmaHostMemory::Staged,
            ..crate::transport::rdma::RdmaConfig::new("copy_qp_pause")
        });
        let pause = verbs::pause(&runtime);
        let source = rdma_fixture_with_config(runtime, Default::default());
        let requester = Arc::new(rdma_fixture_with_config(
            verbs::runtime("direct:connect"),
            EngineConfig {
                max_failover_attempts: 1,
                ..Default::default()
            },
        ));
        let local = requester.register_memory(vec![0xee; 12].into(), Permission::LocalOnly)?;
        let remote = source.register_memory(
            (0..12u8).collect::<Vec<_>>().into(),
            Permission::RemoteReadWrite,
        )?;
        let peer = requester.open_segment(source.endpoint(), source.source_epoch(), WAIT)?;
        let request = |offset, length| {
            TransferRequest::new(
                Transport::Rdma,
                local.base_address() + offset,
                remote.base_address() + offset,
                length,
            )
        };
        let hold = Hold(pause);
        // The first connect rejects before any payload post. Its source RQ
        // must retire; pause native destruction while TCP completes the read.
        let first =
            requester.submit_async(TransferOp::Read, &peer, &[request(2, 4)], None, WAIT)?;
        assert_eq!(first.wait(WAIT)?.completed_tasks, 1);
        {
            let (state, timed) = hold
                .0
                .1
                .wait_timeout_while(hold.0.0.lock().unwrap(), WAIT, |s| !s.0)
                .unwrap();
            assert!(!timed.timed_out() && state.0);
        }
        let before = source.stats()?.rdma.unwrap();
        assert_eq!(
            (
                before.active_grants,
                before.staging_bytes,
                before.receive_work_requests,
                before.closed_connections
            ),
            (1, 4, 1, 0)
        );
        // A new physical owner, with two public aliases, encounters that lane
        // before its close receipt. It must walk the pinned policy once.
        let next = requester.submit_async(
            TransferOp::Read,
            &peer,
            &[request(6, 4), request(2, 4)],
            None,
            WAIT,
        )?;
        let completed = next.wait(WAIT)?;
        let tasks = next.task_statuses()?;
        let pending = source.stats()?.rdma.unwrap();
        drop(hold);
        source.close_source(WAIT)?;
        assert_eq!(completed.completed_tasks, 2, "{tasks:?}");
        for task in tasks {
            assert_eq!(
                (task.physical_owner, task.transport, task.failover_count),
                (0, Transport::Tcp, 1)
            );
        }
        assert_eq!(
            (
                pending.active_grants,
                pending.staging_bytes,
                pending.receive_work_requests,
                pending.closed_connections
            ),
            (1, 4, 1, 0)
        );
        let mut bytes = [0; 12];
        local.read(0, &mut bytes)?;
        assert_eq!(bytes, [0xee, 0xee, 2, 3, 4, 5, 6, 7, 8, 9, 0xee, 0xee]);
        remote.read(0, &mut bytes)?;
        assert_eq!(bytes, std::array::from_fn(|i| i as u8));
        assert_eq!(requester.stats()?.rdma.unwrap().post_calls, 0);
        Ok(())
    }

    #[test]
    fn polling_progress_wait_and_alias_cancel_decide_one_drained_attempt() -> Result<()> {
        for decision in [
            "scalar", "vector", "overall", "progress", "wait", "auto", "cancel",
        ] {
            let auto = decision == "auto";
            let p = Pair::new("direct:wc", 1, auto, false)?;
            let batch = p.engine.submit_async(
                TransferOp::Read,
                &p.peer,
                &[p.request(6, 4), p.request(2, 4)],
                None,
                WAIT,
            )?;
            batch.reach_retry();
            // The terminal-result extension must neither authorize nor decline this retry.
            for task in batch.observe_tasks(None)? {
                assert!(!task.state.is_terminal());
                assert_eq!(task.failover_count, 0);
            }
            match decision {
                "scalar" => assert_eq!(batch.task_status(1)?.state, TaskState::Failed),
                "vector" => assert!(
                    batch
                        .task_statuses()?
                        .iter()
                        .all(|t| t.state == TaskState::Failed)
                ),
                "overall" => {
                    batch.status()?;
                }
                "progress" => {
                    p.engine.progress_batch(batch.id()?)?;
                }
                "auto" => {
                    batch.task_status(1)?;
                }
                "cancel" => assert!(batch.cancel(1)?),
                _ => {}
            }
            let completed = matches!(decision, "wait" | "progress" | "auto");
            let status = batch.wait(WAIT)?;
            assert_eq!(
                status.state,
                if completed {
                    BatchState::Completed
                } else if decision == "cancel" {
                    BatchState::Cancelled
                } else {
                    BatchState::Failed
                },
                "{decision}"
            );
            p.engine.progress_batch(batch.id()?)?;
            for t in batch.task_statuses()? {
                assert_eq!(
                    (t.physical_owner, t.failover_count),
                    (0, usize::from(completed))
                );
            }
            p.bytes(completed)?;
        }
        Ok(())
    }

    #[test]
    fn partial_rdma_read_replays_only_after_the_real_close_receipt() -> Result<()> {
        let p = Pair::new("direct:pause_copy_fault_partial", 1, true, false)?;
        let pause = verbs::pause(super::super::super::tests::source_runtime(&p.engine));
        std::thread::scope(|scope| -> Result<()> {
            let run = scope.spawn(|| {
                p.engine
                    .submit_and_wait(TransferOp::Read, &p.peer, &[p.request(2, 8)], WAIT)
            });
            let (gate, changed) = &*pause;
            let (guard, timed) = changed
                .wait_timeout_while(gate.lock().unwrap(), WAIT, |g| !g.0)
                .unwrap();
            assert!(!timed.timed_out());
            drop(guard);
            let retained = matches!(p.local.read(2, &mut [0; 8]), Err(Error::Unavailable(_)));
            gate.lock().unwrap().1 = true;
            changed.notify_all();
            assert!(
                retained,
                "a still-posting RDMA slice must exclude CPU access"
            );
            let report = run.join().unwrap()?;
            assert!(report.is_complete(), "{report:?}");
            Ok(())
        })?;
        p.bytes(true)?;
        Ok(())
    }

    #[test]
    fn ambiguous_write_and_unquiesced_read_never_start_a_fallback() -> Result<()> {
        for (mode, op) in [
            ("direct:copy_fault_partial", TransferOp::Write),
            ("direct:wc_close0", TransferOp::Read),
        ] {
            let p = Pair::new(mode, 3, true, false)?;
            let b = p
                .engine
                .submit_async(op, &p.peer, &[p.request(2, 8)], None, WAIT)?;
            assert_eq!(b.wait(WAIT)?.state, BatchState::Failed);
            let s = b.task_status(0)?;
            assert_eq!((s.transport, s.failover_count), (Transport::Rdma, 0));
            let f = s.failure.unwrap();
            if op == TransferOp::Write {
                assert_eq!(f.remote_effect, RemoteEffect::Unknown);
                let mut bytes = [0; 12];
                p._source.close_source(WAIT)?;
                p.remote.read(0, &mut bytes)?;
                assert_eq!(bytes, [0, 1, 0xee, 0xee, 0xee, 0xee, 6, 7, 8, 9, 10, 11]);
            } else {
                assert!(matches!(f.error, Error::Unquiesced { .. }));
                assert!(!s.locally_drained);
                assert!(p.local.read(2, &mut [0; 8]).is_err());
                let mut guard = [0; 2];
                p.local.read(0, &mut guard)?;
                assert_eq!(guard, [0xee; 2]);
                p.local.read(10, &mut guard)?;
                assert_eq!(guard, [0xee; 2]);
            }
        }
        Ok(())
    }
    #[test]
    fn tcp_faults_keep_a_successful_sibling_and_never_replay_a_lost_write_ack() -> Result<()> {
        use crate::runtime::control_plane::{Operation, REQUEST_SIZE, Reply, Request};
        use std::io::{Read, Write};
        use std::net::TcpListener;
        for lost in [false, true] {
            let p = Pair::new("direct:copy", 3, true, false)?;
            let listener = TcpListener::bind("127.0.0.1:0")?;
            let mut peer = RemoteSegment {
                endpoint: listener.local_addr()?,
                ..p.peer.clone()
            };
            p.engine.bind_test_peer(&mut peer)?;
            let epoch = peer.source_epoch();
            let server = std::thread::spawn(move || {
                let mut image = [0x2a; 12];
                for op in [
                    Operation::TcpWrite,
                    Operation::RdmaWrite,
                    Operation::TcpWrite,
                ] {
                    let (mut stream, _) = listener.accept().unwrap();
                    stream.set_read_timeout(Some(WAIT)).unwrap();
                    let mut header = [0; REQUEST_SIZE];
                    stream.read_exact(&mut header).unwrap();
                    let r = Request::decode(&header).unwrap();
                    assert!(r.operation == op);
                    if op == Operation::RdmaWrite {
                        stream
                            .write_all(
                                &Reply {
                                    status: 11,
                                    epoch,
                                    length: 0,
                                }
                                .encode(),
                            )
                            .unwrap();
                        continue;
                    }
                    stream
                        .write_all(
                            &Reply {
                                status: 9,
                                epoch,
                                length: 0,
                            }
                            .encode(),
                        )
                        .unwrap();
                    stream
                        .read_exact(&mut image[r.offset as usize..(r.offset + r.length) as usize])
                        .unwrap();
                    if r.offset == 2 || !lost {
                        stream
                            .write_all(
                                &Reply {
                                    status: 0,
                                    epoch,
                                    length: 0,
                                }
                                .encode(),
                            )
                            .unwrap();
                    }
                }
                image
            });
            let mut first = p.request(2, 2);
            first.transport = Transport::Tcp;
            let b = p.engine.submit_async(
                TransferOp::Write,
                &peer,
                &[first, p.request(6, 2), p.request(8, 2)],
                None,
                WAIT,
            )?;
            assert_eq!(b.wait(WAIT)?.completed_tasks, if lost { 1 } else { 3 });
            let tasks = b.task_statuses()?;
            assert_eq!(
                (tasks[0].transport, tasks[0].failover_count),
                (Transport::Tcp, 0)
            );
            for t in &tasks[1..] {
                assert_eq!(
                    (t.transport, t.physical_owner, t.failover_count),
                    (Transport::Tcp, 1, 1)
                );
            }
            if lost {
                assert_eq!(
                    tasks[1].failure.as_ref().unwrap().remote_effect,
                    RemoteEffect::Unknown
                );
            }
            assert_eq!(
                server.join().unwrap(),
                [
                    0x2a, 0x2a, 0xee, 0xee, 0x2a, 0x2a, 0xee, 0xee, 0xee, 0xee, 0x2a, 0x2a
                ]
            );
        }
        Ok(())
    }
    #[cfg(feature = "metrics")]
    #[test]
    fn metrics_attribute_each_retry_and_one_merged_outcome_to_the_real_transport() -> Result<()> {
        for (mode, reason) in [("direct:connect", "submit"), ("direct:wc", "poll")] {
            for budget in [0, 1] {
                let p = Pair::new(mode, budget, true, false)?;
                let b = p.engine.submit_async(
                    TransferOp::Read,
                    &p.peer,
                    &[p.request(2, 4), p.request(6, 4)],
                    None,
                    WAIT,
                )?;
                assert_eq!(
                    b.wait(WAIT)?.completed_tasks,
                    if budget == 0 { 0 } else { 2 }
                );
                p.bytes(budget != 0)?;
                let m = p.engine.metrics().unwrap();
                let j = m.json();
                let text = m.prometheus();
                assert_eq!(j["tent_logical_requests_started_total"], 2);
                assert_eq!(j["tent_logical_request_outcomes_total"], 2);
                assert_eq!(j["tent_logical_requests_inflight"], 0);
                assert_eq!(j["tent_transport_attempt_outcomes_total"], 1 + budget);
                assert_eq!(j["tent_inflight_attempts"], 0);
                let outcome = if budget == 0 { "failed" } else { "success" };
                assert!(text.contains(&format!(r#"tent_logical_request_outcomes_total{{operation="read",outcome="{outcome}"}} 2"#)));
                assert!(text.contains(r#"tent_transport_attempt_outcomes_total{transport="rdma",operation="read",outcome="failed"} 1"#));
                assert_eq!(j["tent_transport_attempts_total"], 1 + budget);
                assert_eq!(j["tent_transport_attempt_failures_total"], 1);
                assert_eq!(j["tent_transport_attempt_latency_us"]["count"], 1 + budget);
                assert_eq!(j["tent_read_requests_total"], 1);
                assert_eq!(j["tent_read_failures_total"], usize::from(budget == 0));
                assert_eq!(j["tent_read_bytes_total"], 8 * budget);
                assert_eq!(j["tent_transport_failover_total"], budget);
                let nics = p.engine.nic_load_stats()?;
                assert_eq!(j["orbite_rdma_inflight_bytes"], 0);
                let charged = nics.iter().map(|nic| nic.allocated_bytes).sum::<u64>();
                assert!(
                    charged >= 8,
                    "the original RDMA parent owned its exact slice charge"
                );
                assert_eq!(j["orbite_rdma_allocated_bytes_total"], charged);
                assert_eq!(j["tent_inflight_attempts"], 0);
                if budget == 0 {
                    assert!(text.contains(&format!(
                        "tent_task_failures_total{{transport=\"rdma\",reason=\"{reason}\"}} 1"
                    )));
                } else {
                    assert!(
                        text.contains("tent_transport_failover_total{from=\"rdma\",to=\"tcp\"} 1")
                    );
                    assert!(text.contains("tent_read_bytes_total{transport=\"tcp\"} 8"));
                }
            }
        }
        Ok(())
    }
}

#[test]
fn a_refresh_cannot_rebind_an_admitted_write_to_a_replaced_registration() -> Result<()> {
    let src = TransferEngine::new(Default::default())?;
    let dst = TransferEngine::new(Default::default())?;
    let backing = Arc::new(HostMemory::new(vec![0x45; 8].into())?);
    let old = src.register_host_memory(backing.clone(), 0, 8, Permission::RemoteReadWrite)?;
    let peer = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    src.unregister_memory(&old)?;
    let new = src.register_host_memory(backing, 0, 8, Permission::RemoteReadWrite)?;
    assert_ne!(old.id(), new.id());
    let local = dst.register_memory(vec![0x17; 8].into(), Permission::LocalOnly)?;
    let r = TransferRequest::new(
        Transport::Tcp,
        local.base_address() + 2,
        old.base_address() + 2,
        4,
    );
    let f = dst
        .submit_and_wait(TransferOp::Write, &peer, &[r], WAIT)?
        .failure
        .unwrap();
    assert!(matches!(f.error, Error::RegionNotFound));
    assert_eq!(f.remote_effect, RemoteEffect::None);
    let mut bytes = [0; 8];
    new.read(0, &mut bytes)?;
    assert_eq!(bytes, [0x45; 8]);
    Ok(())
}

#[test]
fn foreground_and_worker_describe_a_same_port_restart_only_with_its_new_epoch() -> Result<()> {
    let requester = TransferEngine::new(Default::default())?;
    // Keep the restart port outside automatic allocation and reserved against
    // other restart fixtures for the entire drop/rebind interval.
    let (listener, _port_guard) = crate::test_ports::guarded_port();
    let endpoint = listener.local_addr()?;
    drop(listener);
    let config = EngineConfig {
        bind_addr: endpoint,
        ..Default::default()
    };
    let create_source = || {
        // A concurrent test's posix_spawn can temporarily inherit this listener
        // between fork and exec, even with CLOEXEC. Wait for that OS reference
        // to close; the UDP guard still excludes competing fixture identities.
        // This bounds fixture setup only, not transfer deadlines or drain.
        let until = std::time::Instant::now() + WAIT;
        loop {
            match TransferEngine::new(config.clone()) {
                Err(Error::Io(error))
                    if cfg!(target_os = "linux")
                        && error.kind() == std::io::ErrorKind::AddrInUse
                        && std::time::Instant::now() < until =>
                {
                    std::thread::sleep(Duration::from_millis(1));
                }
                result => return result,
            }
        }
    };
    let (endpoint, old) = {
        let source = create_source()?;
        (source.endpoint(), source.source_epoch())
    };
    let source = create_source()?;
    assert_eq!(source.endpoint(), endpoint);
    assert_ne!(source.source_epoch(), old);
    let remote = source.register_memory(vec![1, 3, 5, 7].into(), Permission::RemoteReadWrite)?;
    std::thread::scope(|s| -> Result<()> {
        let read = || -> Result<()> {
            assert!(matches!(
                requester.open_segment(endpoint, old, WAIT),
                Err(Error::EpochMismatch)
            ));
            let peer = requester.open_segment(endpoint, source.source_epoch(), WAIT)?;
            assert_eq!(peer.regions()[0].id(), remote.id());
            Ok(())
        };
        let worker = s.spawn(read);
        read()?;
        worker.join().unwrap()
    })?;
    let mut bytes = [0; 4];
    remote.read(0, &mut bytes)?;
    assert_eq!(bytes, [1, 3, 5, 7]);
    Ok(())
}

#[test]
fn r1c_poll_and_wake_cannot_publish_a_task_before_its_submission() -> Result<()> {
    use std::io::{Read, Write};
    let e = Arc::new(TransferEngine::new(EngineConfig {
        enable_progress_worker: true,
        ..Default::default()
    })?);
    let source = TransferEngine::new(Default::default())?;
    let remote = source.register_memory(vec![8, 1, 2, 3, 4, 9].into(), Permission::RemoteRead)?;
    let local = e.register_memory(vec![0xcc; 6].into(), Permission::LocalOnly)?;
    let peer = e.open_segment(source.endpoint(), source.source_epoch(), WAIT)?;
    let req = TransferRequest::new(
        Transport::Tcp,
        local.base_address() + 1,
        remote.base_address() + 1,
        4,
    );
    let mut pending = peer.clone();
    pending.regions.clear();
    let barrier = std::net::TcpListener::bind("127.0.0.1:0")?;
    pending.endpoint = barrier.local_addr()?;
    e.bind_test_peer(&mut pending)?;
    let mut batch = e.allocate_batch(1)?;
    let id = batch.id()?;
    std::thread::scope(|s| -> Result<()> {
        let submit = s.spawn(|| {
            batch.submit(
                TransferOp::Read,
                &pending,
                std::slice::from_ref(&req),
                None,
                WAIT,
            )
        });
        let (mut conn, _) = barrier.accept()?;
        conn.set_read_timeout(Some(WAIT))?;
        conn.read_exact(&mut [0; crate::runtime::control_plane::REQUEST_SIZE])?;
        // Preparation is inside descriptor refresh. Polling must observe the
        // previous group set and leave this future task entirely unpublished.
        let before = e.progress_batch(id)?;
        conn.write_all(&[0; crate::runtime::control_plane::REPLY_SIZE])?;
        assert!(submit.join().unwrap().is_err());
        assert_eq!(
            (before.submitted_tasks, before.completed_tasks, before.state),
            (0, 0, crate::BatchState::Open)
        );
        Ok(())
    })?;
    assert!(batch.task_status(0).is_err());
    batch.submit(TransferOp::Read, &peer, &[req], None, WAIT)?;
    batch.seal()?;
    let status = batch.wait(WAIT)?;
    assert_eq!(
        (
            status.submitted_tasks,
            status.completed_tasks,
            status.transferred_bytes
        ),
        (1, 1, 4)
    );
    let mut seen = [0; 6];
    local.read(0, &mut seen)?;
    assert_eq!(seen, [0xcc, 1, 2, 3, 4, 0xcc]);
    batch.free()?;
    Ok(())
}
