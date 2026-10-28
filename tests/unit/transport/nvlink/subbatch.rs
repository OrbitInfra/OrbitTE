use super::*;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;
use std::time::Instant;

use crate::cuda::CudaMemory;
use crate::{RemoteRegion, TaskState, TransferRequest, Transport};

pub(crate) fn run_case(
    local: Vec<Arc<CudaMemory>>,
    remote: Arc<CudaMemory>,
    case: &str,
    asynchronous: bool,
) {
    let timeout = Duration::from_secs(3);
    let cancelled = case.starts_with("cancel-");
    let uncertain = matches!(
        case,
        "wait-second" | "pop-second" | "close" | "cancel-close" | "cancel-restore-close"
    );
    let clean_failure = case == "copy-second" || cancelled;
    let loss = matches!(case, "ack-loss" | "prefix-ack-loss" | "cancel-ack-loss");
    let complete = !uncertain && !clean_failure && !loss;
    let engine = Arc::new(
        TransferEngine::new(EngineConfig {
            nvlink: true,
            max_transfer_bytes: if matches!(case, "cap" | "prefix-ack-loss") {
                40
            } else {
                128
            },
            max_inflight_bytes: 2 << 20,
            ..EngineConfig::default()
        })
        .unwrap(),
    );
    let registrations = engine
        .register_cuda_memory_batch(
            &local
                .iter()
                .map(|m| (m.clone(), 5, 100))
                .collect::<Vec<_>>(),
            Permission::LocalOnly,
        )
        .unwrap();
    let weak: Vec<_> = local.iter().map(Arc::downgrade).collect();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let epoch = SourceEpoch([0x55; 16]);
    let mut peer = RemoteSegment {
        endpoint: listener.local_addr().unwrap(),
        epoch,
        regions: vec![RemoteRegion {
            epoch,
            id: 7,
            address: remote.base_address() + 5,
            length: 100,
            permission: Permission::RemoteReadWrite,
        }],
    };
    if case == "region" {
        peer.regions[0].length = 64;
        peer.regions.push(RemoteRegion {
            epoch,
            id: 8,
            address: remote.base_address() + 69,
            length: 59,
            permission: Permission::RemoteReadWrite,
        });
    }
    let offsets = if case == "overlap" {
        [11, 18, 11]
    } else {
        [43, 11, 67]
    };
    let requests: Vec<_> = (0..3)
        .map(|i| TransferRequest {
            transport: Transport::Nvlink,
            local_address: registrations[i].base_address() + 7,
            remote_address: remote.base_address() + 5 + offsets[i],
            length: 17,
        })
        .collect();
    let expected = match case {
        "cap" => vec![(7, 43, 17, false), (7, 11, 17, false), (7, 67, 17, false)],
        "prefix-ack-loss" => vec![(7, 43, 17, false), (7, 11, 17, false)],
        "source-cap" => vec![
            (7, 11, 73, true),
            (7, 43, 17, false),
            (7, 11, 73, true),
            (7, 11, 17, false),
            (7, 67, 17, false),
        ],
        "context" => vec![(7, 11, 49, false), (7, 67, 17, false)],
        "region" => vec![(7, 11, 49, false), (8, 3, 17, false)],
        "overlap" => vec![(7, 11, 24, false)],
        _ => vec![(7, 11, 73, false)],
    };
    let requester_epoch = engine.source_epoch();
    let remote_owner = remote.clone();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let case_owned = case.to_string();
    let source = std::thread::spawn(move || {
        for (group, (region, offset, length, reject)) in expected.into_iter().enumerate() {
            let until = Instant::now() + timeout;
            let (mut socket, _) = loop {
                match listener.accept() {
                    Ok(socket) => break socket,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && Instant::now() < until =>
                    {
                        std::thread::sleep(Duration::from_millis(2))
                    }
                    Err(error) => panic!("missing group {group}: {error}"),
                }
            };
            socket.set_read_timeout(Some(timeout)).unwrap();
            socket.set_write_timeout(Some(timeout)).unwrap();
            let mut wire = [0; 52];
            socket.read_exact(&mut wire).unwrap();
            assert_eq!(
                wire,
                Request {
                    operation: Operation::NvlinkWrite,
                    epoch,
                    region,
                    offset,
                    length
                }
                .encode()
            );
            let mut hello = [0; HELLO_SIZE];
            socket.read_exact(&mut hello).unwrap();
            assert_eq!(hello[36..], requester_epoch.0);
            if group == 0 {
                ready_tx.send(()).unwrap();
                release_rx.recv_timeout(timeout).unwrap();
            }
            if reject {
                socket
                    .write_all(
                        &Reply {
                            status: 6,
                            epoch,
                            length: 0,
                        }
                        .encode(),
                    )
                    .unwrap();
                continue;
            }
            let mut export = IpcExport::new(remote_owner.clone()).unwrap();
            let mut grant = [0; GRANT_SIZE];
            grant[..4].copy_from_slice(b"NVL1");
            let mut identity = Identity::local().unwrap();
            identity.process = SourceEpoch([0x77; 16]);
            identity.encode(&mut grant[4..36]);
            grant[36..88].copy_from_slice(&wire);
            grant[88..104].copy_from_slice(&requester_epoch.0);
            grant[104..120].fill(0x66);
            grant[120..128].copy_from_slice(&(export.extent as u64).to_be_bytes());
            grant[128..136]
                .copy_from_slice(&(offset + if region == 7 { 5 } else { 69 }).to_be_bytes());
            grant[136..].copy_from_slice(&export.handle);
            socket
                .write_all(
                    &Reply {
                        status: 0,
                        epoch,
                        length: GRANT_SIZE as u64,
                    }
                    .encode(),
                )
                .unwrap();
            export.expose();
            socket.write_all(&grant).unwrap();
            let mut receipt = [0; RECEIPT_SIZE];
            if uncertain {
                assert_eq!(
                    socket.read(&mut receipt).unwrap(),
                    0,
                    "uncertain group released source"
                );
            } else {
                socket.read_exact(&mut receipt).unwrap();
                assert_eq!(receipt[..BINDING_SIZE], grant[36..120]);
                assert_eq!(receipt[BINDING_SIZE], u8::from(clean_failure));
                export.finish();
                if case_owned == "ack-hold" {
                    ready_tx.send(()).unwrap();
                    release_rx.recv_timeout(timeout).unwrap();
                }
                if matches!(case_owned.as_str(), "ack-loss" | "cancel-ack-loss")
                    || (case_owned == "prefix-ack-loss" && group == 1)
                {
                    continue;
                }
                socket
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
    });
    let prefix = if (clean_failure && !uncertain && !loss) || case == "prefix-ack-loss" {
        1
    } else {
        0
    };
    let affected = if case == "ack-loss" || case == "close" {
        3
    } else if matches!(case, "wait-second" | "pop-second") {
        2
    } else {
        1
    };
    if asynchronous {
        let tcp_source = TransferEngine::new(EngineConfig::default()).unwrap();
        let tcp_remote = tcp_source
            .register_memory(vec![0x39; 1].into_boxed_slice(), Permission::RemoteRead)
            .unwrap();
        let tcp_local = engine
            .register_memory(vec![0; 1].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let tcp_peer = engine
            .open_segment(tcp_source.endpoint(), tcp_source.source_epoch(), timeout)
            .unwrap();
        let tcp_request = TransferRequest {
            transport: Transport::Tcp,
            local_address: tcp_local.base_address(),
            remote_address: tcp_remote.base_address(),
            length: 1,
        };
        let mut batch = engine.allocate_batch(5).unwrap();
        assert_eq!(
            batch
                .submit(
                    TransferOp::Read,
                    &tcp_peer,
                    std::slice::from_ref(&tcp_request),
                    None,
                    timeout
                )
                .unwrap(),
            0..1
        );
        assert_eq!(
            batch
                .submit(TransferOp::Write, &peer, &requests, None, timeout)
                .unwrap(),
            1..4
        );
        assert_eq!(
            batch
                .submit(
                    TransferOp::Read,
                    &tcp_peer,
                    std::slice::from_ref(&tcp_request),
                    None,
                    timeout
                )
                .unwrap(),
            4..5
        );
        batch.seal().unwrap();
        ready_rx.recv_timeout(timeout).unwrap();
        if cancelled {
            batch.cancel(2).unwrap();
        }
        release_tx.send(()).unwrap();
        if case == "ack-hold" {
            ready_rx.recv_timeout(timeout).unwrap();
            assert!(
                batch.task_statuses().unwrap()[1..4]
                    .iter()
                    .all(|task| task.state == TaskState::Running && task.transferred_bytes == 0)
            );
            assert_eq!(engine.stats().unwrap().inflight_bytes, 52);
            assert!(matches!(
                engine.close_requester(Duration::from_millis(10)),
                Err(Error::Timeout)
            ));
            release_tx.send(()).unwrap();
        }
        let status = batch.wait(timeout).unwrap();
        let submission = batch.submissions().unwrap().remove(1);
        assert_eq!(
            submission.completed_requests,
            if complete { 3 } else { prefix },
            "{case}"
        );
        assert_eq!(status.locally_drained, !uncertain, "{case}");
        let tasks = batch.task_statuses().unwrap();
        assert_eq!(tasks[0].state, TaskState::Completed);
        assert_eq!(tasks[4].state, TaskState::Completed);
        assert_eq!(tasks[0].transferred_bytes + tasks[4].transferred_bytes, 2);
        for (i, task) in tasks[1..4].iter().enumerate() {
            let expected = if complete || i < prefix {
                TaskState::Completed
            } else if i < prefix + affected {
                if cancelled && !uncertain && !loss {
                    TaskState::Cancelled
                } else {
                    TaskState::Failed
                }
            } else if task.cancel_requested {
                TaskState::Cancelled
            } else {
                TaskState::Skipped
            };
            assert_eq!(task.state, expected, "{case}, task {i}");
            assert_eq!(
                task.locally_drained,
                !uncertain || i >= prefix + affected,
                "{case}, task {i}"
            );
            if expected == TaskState::Failed {
                assert_eq!(task.failure.as_ref().unwrap().affected_requests, affected);
                assert_eq!(task.failure.as_ref().unwrap().request_index, 1 + prefix);
                assert!(Arc::ptr_eq(
                    task.failure.as_ref().unwrap(),
                    submission.failure.as_ref().unwrap()
                ));
                assert_eq!(
                    task.failure.as_ref().unwrap().remote_effect,
                    RemoteEffect::Unknown
                );
            }
        }
        assert_eq!(batch.free().is_ok(), !uncertain);
    } else {
        release_tx.send(()).unwrap();
        let report = engine
            .submit_and_wait(TransferOp::Write, &peer, &requests, timeout)
            .unwrap();
        assert_eq!(report.is_complete(), complete, "{case}: {report:?}");
        assert_eq!(
            report.completed_requests,
            if complete { 3 } else { prefix },
            "{case}: {report:?}"
        );
        assert_eq!(report.transferred_bytes, 17 * report.completed_requests);
        assert_eq!(report.locally_drained, !uncertain);
        if let Some(failure) = report.failure {
            assert_eq!(
                (failure.request_index, failure.affected_requests),
                (prefix, affected),
                "{case}"
            );
        }
    }
    source.join().unwrap();
    let stats = engine.stats().unwrap();
    assert_eq!(
        stats.nvlink.unwrap().allocation_bytes,
        if uncertain { 2 << 20 } else { 0 }
    );
    assert_eq!(
        stats.nvlink.unwrap().retained_imports,
        usize::from(uncertain)
    );
    assert_eq!(stats.inflight_bytes, if uncertain { 51 } else { 0 });
    if !uncertain {
        let mut actual = [0; 128];
        remote.read(0, &mut actual).unwrap();
        let mut expected = [0x5a; 128];
        let copied = if clean_failure {
            1
        } else if case == "prefix-ack-loss" {
            2
        } else {
            3
        };
        for (i, offset) in offsets.iter().take(copied).enumerate() {
            expected[*offset as usize + 5..*offset as usize + 22].fill((i + 1) as u8);
        }
        if case == "copy-second" {
            expected[16] = 2;
        }
        assert_eq!(
            actual, expected,
            "source full bytes and untouched gaps: {case}"
        );
    } else {
        assert!(remote.read(0, &mut [0; 1]).is_err());
        for memory in local.iter().take(if cancelled {
            1
        } else if case == "close" {
            3
        } else {
            2
        }) {
            assert!(memory.read(0, &mut [0; 1]).is_err());
        }
    }
    for registered in &registrations {
        engine.unregister_memory(registered).unwrap();
    }
    drop(registrations);
    drop(local);
    drop(engine);
    for owner in weak {
        assert_eq!(
            owner.upgrade().is_some(),
            uncertain,
            "actual local backing: {case}"
        );
    }
}
