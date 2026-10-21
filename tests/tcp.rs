use std::io::{BufRead, BufReader, Read, Write};
use std::net::SocketAddr;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

use orbit_te::{
    EngineConfig, Error, HostMemory, Notification, NotificationEffect, NotificationStatus,
    Permission, RemoteEffect, RemoteSegment, SourceEpoch, TransferEngine, TransferOp,
    TransferRequest,
};

const TIMEOUT: Duration = Duration::from_secs(5);

mod common;

fn request(
    local: &orbit_te::RegisteredMemory,
    remote: orbit_te::RemoteRegion,
    local_offset: usize,
    remote_offset: usize,
    length: usize,
) -> TransferRequest {
    TransferRequest {
        local: local.clone(),
        local_offset,
        remote,
        remote_offset,
        length,
    }
}

fn region(peer: &RemoteSegment, id: u64) -> orbit_te::RemoteRegion {
    *peer
        .regions()
        .iter()
        .find(|region| region.id() == id)
        .expect("exported registration")
}

fn request_addresses(
    engine: &TransferEngine,
    peer: &RemoteSegment,
    local_address: u64,
    remote_address: u64,
    length: usize,
) -> TransferRequest {
    let (local, local_offset) = engine.resolve_local_address(local_address, length).unwrap();
    let (remote, remote_offset) = peer.resolve_address(remote_address, length).unwrap();
    TransferRequest {
        local,
        local_offset,
        remote,
        remote_offset,
        length,
    }
}

#[test]
fn batched_read_write_range_guards_and_retired_descriptors() {
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    let data: Vec<_> = (0..128).map(|index| index as u8).collect();
    let export = source
        .register_memory(data.clone().into_boxed_slice(), Permission::RemoteReadWrite)
        .unwrap();
    let requester = TransferEngine::new(EngineConfig::default()).unwrap();
    let destination = requester
        .register_memory(vec![0xee; 128].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let peer = requester
        .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
        .unwrap();
    let remote = region(&peer, export.id());
    let requests = [
        request(&destination, remote, 8, 16, 32),
        request(&destination, remote, 64, 0, 48),
    ];
    let report = requester
        .submit_and_wait(TransferOp::Read, &peer, &requests, TIMEOUT)
        .unwrap();
    assert!(report.is_complete());
    assert_eq!(
        (report.completed_requests, report.transferred_bytes),
        (2, 80)
    );
    let mut bytes = [0; 128];
    destination.read(0, &mut bytes).unwrap();
    let mut expected = [0xee; 128];
    expected[8..40].copy_from_slice(&data[16..48]);
    expected[64..112].copy_from_slice(&data[..48]);
    assert_eq!(bytes, expected);

    destination.write(0, &[0xa7; 16]).unwrap();
    assert!(
        requester
            .submit_and_wait(
                TransferOp::Write,
                &peer,
                &[request(&destination, remote, 0, 48, 16)],
                TIMEOUT
            )
            .unwrap()
            .is_complete()
    );
    export.read(0, &mut bytes).unwrap();
    assert_eq!(&bytes[48..64], &[0xa7; 16]);
    assert_eq!(&bytes[..48], &data[..48]);
    assert_eq!(&bytes[64..], &data[64..]);

    source.unregister_memory(&export).unwrap();
    let report = requester
        .submit_and_wait(TransferOp::Read, &peer, &requests, TIMEOUT)
        .unwrap();
    let failure = report.failure.unwrap();
    assert!(matches!(failure.error, Error::RegionNotFound));
    assert_eq!(failure.remote_effect, RemoteEffect::None);
    let refreshed = requester
        .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
        .unwrap();
    assert!(refreshed.regions().is_empty());
    source.close_source(TIMEOUT).unwrap();
    assert!(matches!(
        requester.open_segment(source.endpoint(), source.source_epoch(), TIMEOUT),
        Err(Error::SourceClosed)
    ));
}

#[test]
fn whole_batch_validation_prevents_early_effects_and_budgets_are_enforced() {
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    let export = source
        .register_memory(vec![0; 8].into_boxed_slice(), Permission::RemoteReadWrite)
        .unwrap();
    let readonly = source
        .register_memory(vec![0; 8].into_boxed_slice(), Permission::RemoteRead)
        .unwrap();
    let requester = TransferEngine::new(EngineConfig {
        max_batch_requests: 2,
        max_inflight_bytes: 8,
        ..EngineConfig::default()
    })
    .unwrap();
    let local = requester
        .register_memory(vec![7; 8].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let peer = requester
        .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
        .unwrap();
    let valid = request(&local, region(&peer, export.id()), 0, 0, 8);
    let mut invalid = valid.clone();
    invalid.remote_offset = usize::MAX;
    assert!(matches!(
        requester.submit_and_wait(TransferOp::Write, &peer, &[valid.clone(), invalid], TIMEOUT),
        Err(Error::InvalidRange)
    ));
    assert!(matches!(
        requester.submit_and_wait(
            TransferOp::Write,
            &peer,
            &[valid.clone(), valid.clone()],
            TIMEOUT
        ),
        Err(Error::ResourceExhausted)
    ));
    assert!(matches!(
        requester.submit_and_wait(
            TransferOp::Write,
            &peer,
            &[valid.clone(), valid.clone(), valid.clone()],
            TIMEOUT
        ),
        Err(Error::ResourceExhausted)
    ));
    assert!(matches!(
        requester.submit_and_wait(
            TransferOp::Write,
            &peer,
            &[request(&local, region(&peer, readonly.id()), 0, 0, 8)],
            TIMEOUT
        ),
        Err(Error::PermissionDenied)
    ));
    let mut bytes = [9; 8];
    export.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [0; 8]);
    assert!(
        requester
            .submit_and_wait(TransferOp::Read, &peer, &[], TIMEOUT)
            .unwrap()
            .is_complete()
    );
    requester.unregister_memory(&local).unwrap();
    assert!(matches!(
        requester.submit_and_wait(TransferOp::Read, &peer, &[valid], TIMEOUT),
        Err(Error::RegionNotFound)
    ));
}

#[test]
fn execution_failure_preserves_completed_prefix_and_skips_later_requests() {
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    let first = source
        .register_memory(vec![1; 8].into_boxed_slice(), Permission::RemoteRead)
        .unwrap();
    let retired = source
        .register_memory(vec![2; 8].into_boxed_slice(), Permission::RemoteRead)
        .unwrap();
    let third = source
        .register_memory(vec![3; 8].into_boxed_slice(), Permission::RemoteRead)
        .unwrap();
    let requester = TransferEngine::new(EngineConfig::default()).unwrap();
    let local = requester
        .register_memory(vec![0; 24].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let peer = requester
        .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
        .unwrap();
    source.unregister_memory(&retired).unwrap();
    let requests = [
        request(&local, region(&peer, first.id()), 0, 0, 8),
        request(&local, region(&peer, retired.id()), 8, 0, 8),
        request(&local, region(&peer, third.id()), 16, 0, 8),
    ];
    let report = requester
        .submit_and_notify(
            TransferOp::Read,
            &peer,
            &requests,
            &Notification {
                name: "scope".into(),
                message: "complete".into(),
            },
            TIMEOUT,
        )
        .unwrap();
    assert_eq!(
        (report.completed_requests, report.transferred_bytes),
        (1, 8)
    );
    assert_eq!(report.failure.unwrap().request_index, 1);
    assert!(matches!(report.notification, NotificationStatus::Skipped));
    assert!(source.receive_notifications().unwrap().is_empty());
    let mut bytes = [0; 24];
    local.read(0, &mut bytes).unwrap();
    assert_eq!(&bytes[..8], &[1; 8]);
    assert_eq!(&bytes[8..], &[0; 16]);
}

// Invoked by the real two-process gate below. It remains inert under --ignored
// unless the parent supplied the explicit fixture environment.
#[test]
#[ignore = "subprocess fixture for two_process_restart_fences_old_authority"]
fn peer_process_fixture() {
    let Ok(bind) = std::env::var("ORBITTE_TEST_PEER_BIND") else {
        return;
    };
    let source = TransferEngine::new(EngineConfig {
        bind_addr: bind.parse().unwrap(),
        ..EngineConfig::default()
    })
    .unwrap();
    let data = (0..65536)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let fixture = common::external_memory(&data);
    let memory = source
        .register_host_memory(fixture.memory, 0, data.len(), Permission::RemoteReadWrite)
        .unwrap();
    println!(
        "ORBITTE_PEER {} {} {} {}",
        source.endpoint(),
        source.source_epoch(),
        memory.id(),
        memory.base_address()
    );
    std::io::stdout().flush().unwrap();
    let mut command = [0];
    std::io::stdin().read_exact(&mut command).unwrap();
    if command[0] == b'V' {
        let mut bytes = [0; 256];
        memory.read(128, &mut bytes).unwrap();
        assert_eq!(
            bytes, [0xa7; 256],
            "independent source did not observe the WRITE"
        );
        assert_eq!(
            source.receive_notifications().unwrap(),
            [
                Notification {
                    name: "read".into(),
                    message: "1024".into()
                },
                Notification {
                    name: "write".into(),
                    message: "256".into()
                },
                Notification {
                    name: "确认".into(),
                    message: String::new()
                },
            ]
        );
    }
    assert!(source.receive_notifications().unwrap().is_empty());
    source.close_source(TIMEOUT).unwrap();
    source.unregister_memory(&memory).unwrap();
    drop(memory);
    assert!(fixture.owner.upgrade().is_none());
    assert_eq!(fixture.drops.load(std::sync::atomic::Ordering::SeqCst), 1);
}

struct PeerProcess {
    child: Child,
    stdin: ChildStdin,
    endpoint: SocketAddr,
    epoch: SourceEpoch,
    region: u64,
    address: u64,
}

impl PeerProcess {
    fn start(bind: SocketAddr) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "peer_process_fixture",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("ORBITTE_TEST_PEER_BIND", bind.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else {
                    break;
                };
                // Keep draining after readiness; closing stdout would make the
                // child test harness fail while printing its final result.
                let _ = sender.send(line);
            }
        });
        let mut peer = Self {
            child,
            stdin,
            endpoint: bind,
            epoch: "00000000000000000000000000000000".parse().unwrap(),
            region: 0,
            address: 0,
        };
        let line = wait_marker(&receiver);
        let mut fields = line.split_whitespace();
        peer.endpoint = fields.next().unwrap().parse().unwrap();
        peer.epoch = fields.next().unwrap().parse().unwrap();
        peer.region = fields.next().unwrap().parse().unwrap();
        peer.address = fields.next().unwrap().parse().unwrap();
        peer
    }

    fn finish(&mut self, verify: bool) {
        self.stdin
            .write_all(if verify { b"V" } else { b"Q" })
            .unwrap();
        let deadline = Instant::now() + TIMEOUT;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success(), "source process failed: {status}");
                break;
            }
            assert!(Instant::now() < deadline, "source process did not drain");
            thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for PeerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn wait_marker(receiver: &Receiver<String>) -> String {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let line = receiver
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("source process readiness");
        if let Some((_, fields)) = line.split_once("ORBITTE_PEER ") {
            return fields.to_owned();
        }
    }
}

#[test]
fn two_process_restart_fences_old_authority() {
    let mut process = PeerProcess::start("127.0.0.1:0".parse().unwrap());
    let requester = TransferEngine::new(EngineConfig::default()).unwrap();
    let destination = common::external_memory(&[0; 1024]);
    let local = requester
        .register_host_memory(destination.memory, 0, 1024, Permission::LocalOnly)
        .unwrap();
    let peer = requester
        .open_segment(process.endpoint, process.epoch, TIMEOUT)
        .unwrap();
    let remote = region(&peer, process.region);
    assert_eq!(remote.base_address(), process.address);
    let requests = [
        request_addresses(
            &requester,
            &peer,
            local.base_address(),
            process.address + 8192,
            512,
        ),
        request_addresses(
            &requester,
            &peer,
            local.base_address() + 512,
            process.address + 16384,
            512,
        ),
    ];
    let report = requester
        .submit_and_notify(
            TransferOp::Read,
            &peer,
            &requests,
            &Notification {
                name: "read".into(),
                message: "1024".into(),
            },
            TIMEOUT,
        )
        .unwrap();
    assert_eq!(
        (report.completed_requests, report.transferred_bytes),
        (2, 1024)
    );
    assert!(report.is_complete());
    assert!(matches!(report.notification, NotificationStatus::Delivered));
    let mut bytes = [0; 1024];
    local.read(0, &mut bytes).unwrap();
    for (index, byte) in bytes.iter().enumerate() {
        let remote_offset = if index < 512 {
            8192 + index
        } else {
            16384 + index - 512
        };
        assert_eq!(*byte, (remote_offset % 251) as u8, "byte {index}");
    }
    local.write(0, &[0xa7; 256]).unwrap();
    assert!(
        requester
            .submit_and_notify(
                TransferOp::Write,
                &peer,
                &[request_addresses(
                    &requester,
                    &peer,
                    local.base_address(),
                    process.address + 128,
                    256
                )],
                &Notification {
                    name: "write".into(),
                    message: "256".into()
                },
                TIMEOUT
            )
            .unwrap()
            .is_complete()
    );
    requester
        .send_notification(
            &peer,
            &Notification {
                name: "确认".into(),
                message: String::new(),
            },
            TIMEOUT,
        )
        .unwrap();
    process.finish(true);

    let mut replacement = PeerProcess::start(process.endpoint);
    assert_ne!(process.epoch, replacement.epoch);
    let stale = requester
        .send_notification(
            &peer,
            &Notification {
                name: "stale".into(),
                message: String::new(),
            },
            TIMEOUT,
        )
        .unwrap_err();
    assert!(matches!(stale.error, Error::EpochMismatch));
    assert_eq!(stale.effect, NotificationEffect::None);
    assert!(matches!(
        requester.open_segment(replacement.endpoint, process.epoch, TIMEOUT),
        Err(Error::EpochMismatch)
    ));
    let report = requester
        .submit_and_wait(TransferOp::Read, &peer, &requests, TIMEOUT)
        .unwrap();
    let failure = report.failure.unwrap();
    assert!(matches!(failure.error, Error::EpochMismatch));
    assert_eq!(failure.remote_effect, RemoteEffect::None);
    let refreshed = requester
        .open_segment(replacement.endpoint, replacement.epoch, TIMEOUT)
        .unwrap();
    assert!(
        requester
            .submit_and_wait(
                TransferOp::Read,
                &refreshed,
                &[request_addresses(
                    &requester,
                    &refreshed,
                    local.base_address(),
                    replacement.address,
                    256
                )],
                TIMEOUT
            )
            .unwrap()
            .is_complete()
    );
    local.read(0, &mut bytes[..256]).unwrap();
    for (index, byte) in bytes[..256].iter().enumerate() {
        assert_eq!(*byte, (index % 251) as u8);
    }
    replacement.finish(false);
}

#[test]
fn concurrent_batches_use_independent_destinations_and_release_admission() {
    use std::sync::{Arc, Barrier};

    let source = TransferEngine::new(EngineConfig {
        handler_threads: 8,
        ..EngineConfig::default()
    })
    .unwrap();
    let data = (0..32768)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let export = source
        .register_memory(data.clone().into_boxed_slice(), Permission::RemoteRead)
        .unwrap();
    let requester = Arc::new(TransferEngine::new(EngineConfig::default()).unwrap());
    let peer = requester
        .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
        .unwrap();
    let start = Arc::new(Barrier::new(8));
    let mut workers = Vec::new();
    for index in 0..8 {
        let engine = Arc::clone(&requester);
        let start = Arc::clone(&start);
        let peer = peer.clone();
        let remote = region(&peer, export.id());
        workers.push(thread::spawn(move || {
            let local = engine
                .register_memory(vec![0; 4096].into_boxed_slice(), Permission::LocalOnly)
                .unwrap();
            let transfer = request(&local, remote, 0, index * 4096, 4096);
            start.wait();
            for _ in 0..4 {
                assert!(
                    engine
                        .submit_and_wait(
                            TransferOp::Read,
                            &peer,
                            std::slice::from_ref(&transfer),
                            TIMEOUT
                        )
                        .unwrap()
                        .is_complete()
                );
            }
            let mut bytes = vec![0; 4096];
            local.read(0, &mut bytes).unwrap();
            engine.unregister_memory(&local).unwrap();
            bytes
        }));
    }
    for (index, worker) in workers.into_iter().enumerate() {
        assert_eq!(
            worker.join().unwrap(),
            data[index * 4096..(index + 1) * 4096]
        );
    }
    source.close_source(TIMEOUT).unwrap();
    let stats = requester.stats().unwrap();
    assert_eq!(
        (
            stats.inflight_batches,
            stats.inflight_bytes,
            stats.retained_regions,
            stats.retained_bytes
        ),
        (0, 0, 0, 0)
    );
}
