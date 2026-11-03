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
        let batch = p.engine.submit_async(
            TransferOp::Read,
            &p.peer,
            &[p.request(2, 4), p.request(7, 3)],
            None,
            WAIT,
        )?;
        assert_eq!(batch.wait(WAIT)?.completed_tasks, 2);
        for (i, t) in batch.task_statuses()?.iter().enumerate() {
            assert_eq!((t.physical_owner, t.failover_count), (i, 1));
        }
        let mut bytes = [0; 12];
        p.local.read(0, &mut bytes)?;
        assert_eq!(bytes, [0xee, 0xee, 2, 3, 4, 5, 0xee, 7, 8, 9, 0xee, 0xee]);
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
            let peer = RemoteSegment {
                endpoint: listener.local_addr()?,
                ..p.peer.clone()
            };
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
