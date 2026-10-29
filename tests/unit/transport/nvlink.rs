use super::*;
use std::sync::Arc;
use std::time::Duration;

use crate::{EngineConfig, Permission, RemoteSegment, TransferEngine};

#[test]
fn ipc_mapping_budget_charges_physical_extent_and_retains_failed_closes() {
    let budget = MappingBudget::new(2 << 20);
    assert!(matches!(
        budget.reserve((2 << 20) + 1, true),
        Err(Error::ResourceExhausted)
    ));
    let export = budget.reserve(2 << 20, true).unwrap();
    assert!(matches!(
        budget.reserve(1, false),
        Err(Error::ResourceExhausted)
    ));
    assert_eq!(budget.stats().unwrap().allocation_bytes, 2 << 20);
    drop(export);
    let import = budget.reserve(2 << 20, false).unwrap();
    import.retain();
    let stats = budget.stats().unwrap();
    assert_eq!(
        (
            stats.imports,
            stats.retained_imports,
            stats.allocation_bytes
        ),
        (1, 1, 2 << 20)
    );
    assert!(budget.reserve(1, true).is_err());
}

#[test]
fn ipc_grant_validation_binds_request_epochs_and_allocation_extent() {
    let transport = NVLinkTransport::new(2 << 20, 0).unwrap();
    let request = Request {
        operation: Operation::NvlinkRead,
        epoch: SourceEpoch([1; 16]),
        region: 3,
        offset: 7,
        length: 17,
    };
    let requester = SourceEpoch([2; 16]);
    let mut grant = [0; GRANT_SIZE];
    grant[..4].copy_from_slice(b"NVL1");
    Identity {
        machine: transport.identity.machine,
        process: SourceEpoch([3; 16]),
    }
    .encode(&mut grant[4..36]);
    grant[36..88].copy_from_slice(&request.encode());
    grant[88..104].copy_from_slice(&requester.0);
    grant[120..128].copy_from_slice(&128u64.to_be_bytes());
    grant[128..136].copy_from_slice(&11u64.to_be_bytes());
    assert!(
        transport
            .validate_grant(&grant, &request, requester)
            .is_ok()
    );
    for index in [
        0,
        4,
        36 + 10,
        36 + 12,
        36 + 28,
        36 + 36,
        36 + 44,
        88,
        120,
        128,
    ] {
        let mut bad = grant;
        bad[index] ^= 0xff;
        assert!(
            transport.validate_grant(&bad, &request, requester).is_err(),
            "byte {index}"
        );
    }
}

pub(crate) fn source_case(memory: Arc<crate::cuda::CudaMemory>, case: &str) {
    let engine = TransferEngine::new(EngineConfig {
        nvlink: true,
        max_inflight_bytes: if case == "budget" {
            (2 << 20) - 1
        } else {
            2 << 20
        },
        handler_timeout: Duration::from_secs(2),
        ..EngineConfig::default()
    })
    .unwrap();
    let registered = engine
        .register_cuda_memory_batch(&[(memory.clone(), 5, 64)], Permission::RemoteReadWrite)
        .unwrap()
        .remove(0);
    let request = Request {
        operation: Operation::NvlinkWrite,
        epoch: engine.source_epoch(),
        region: registered.id(),
        offset: 7,
        length: 17,
    };
    let mut channel = ControlChannel::connect(
        engine.endpoint(),
        crate::engine::deadline(Duration::from_secs(5)).unwrap(),
    )
    .unwrap();
    channel.write_all(&request.encode()).unwrap();
    let mut identity = Identity::local().unwrap();
    if case != "same-process" {
        identity.process = SourceEpoch([0x99; 16]);
    }
    if case == "machine" {
        identity.machine = SourceEpoch([0x88; 16]);
    }
    let mut hello = [0; HELLO_SIZE];
    hello[..4].copy_from_slice(b"NVL1");
    identity.encode(&mut hello[4..36]);
    hello[36..].fill(9);
    channel.write_all(&hello).unwrap();
    let reply = Reply::read(&mut channel, engine.source_epoch());
    if matches!(case, "machine" | "same-process" | "budget") {
        assert!(reply.is_err(), "{case}");
        engine.close_source(Duration::from_secs(1)).unwrap();
        assert_eq!(engine.stats().unwrap().nvlink.unwrap().allocation_bytes, 0);
        memory.read(100, &mut [0; 1]).unwrap();
        return;
    }
    assert_eq!(
        reply
            .unwrap_or_else(|error| panic!("{case}: {error:?}"))
            .length,
        GRANT_SIZE as u64
    );
    let mut grant = [0; GRANT_SIZE];
    if case == "partial-grant" {
        channel.read_exact(&mut grant[..1]).unwrap();
    } else {
        channel.read_exact(&mut grant).unwrap();
        assert_eq!(crate::runtime::control_plane::u64_at(&grant, 120), 2 << 20);
        assert_eq!(crate::runtime::control_plane::u64_at(&grant, 128), 12);
        assert!(memory.read(100, &mut [0; 1]).is_err());
        assert!(matches!(
            engine.close_source(Duration::from_millis(1)),
            Err(Error::Timeout)
        ));
    }
    let mut receipt = [0; RECEIPT_SIZE];
    receipt[..BINDING_SIZE].copy_from_slice(&grant[36..120]);
    match case {
        "abort" => receipt[BINDING_SIZE] = 1,
        "nonce" => receipt[68] ^= 1,
        "operation" => receipt[10] ^= 1,
        "epoch" => receipt[52] ^= 1,
        _ => (),
    }
    if case == "partial" {
        channel.write_all(&receipt[..20]).unwrap();
    } else if case == "timeout" {
        // The server must close after its receipt deadline, while the client
        // still has time remaining. Closing the client first would only test EOF.
        assert!(matches!(
            Reply::read(&mut channel, engine.source_epoch()),
            Err(Error::Io(_))
        ));
    } else if case != "partial-grant" {
        channel.write_all(&receipt).unwrap();
    }
    let clean = matches!(case, "success" | "abort");
    if clean {
        assert_eq!(
            Reply::read(&mut channel, engine.source_epoch())
                .unwrap()
                .status,
            0
        );
    }
    drop(channel);
    let close = engine.close_source(Duration::from_millis(300));
    assert_eq!(close.is_ok(), clean, "{case}: {close:?}");
    let stats = engine.stats().unwrap();
    if clean {
        assert_eq!(stats.nvlink.unwrap().allocation_bytes, 0);
        memory.read(100, &mut [0; 1]).unwrap();
    } else {
        let ipc = stats.nvlink.unwrap();
        assert_eq!(
            (ipc.exports, ipc.retained_exports, ipc.allocation_bytes),
            (1, 1, 2 << 20)
        );
        assert_eq!(stats.active_source_operations, 1);
        assert!(memory.read(100, &mut [0; 1]).is_err());
    }
}

pub(crate) fn requester_case(
    local: Arc<crate::cuda::CudaMemory>,
    remote: Arc<crate::cuda::CudaMemory>,
    case: &str,
    asynchronous: bool,
) {
    use crate::{RemoteRegion, TransferRequest, Transport};
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let uncertain = matches!(case, "wait" | "close" | "restore-close");
    let success = case == "success";
    let engine = Arc::new(
        TransferEngine::new(EngineConfig {
            nvlink: true,
            max_inflight_bytes: if case == "budget" {
                (2 << 20) - 1
            } else {
                2 << 20
            },
            ..EngineConfig::default()
        })
        .unwrap(),
    );
    let registered = engine
        .register_cuda_memory_batch(&[(local.clone(), 5, 64)], Permission::LocalOnly)
        .unwrap()
        .remove(0);
    let local_weak = Arc::downgrade(&local);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let epoch = SourceEpoch([0x55; 16]);
    let peer = RemoteSegment {
        endpoint: listener.local_addr().unwrap(),
        epoch,
        regions: vec![RemoteRegion {
            epoch,
            id: 7,
            address: remote.base_address(),
            length: remote.len(),
            permission: Permission::RemoteReadWrite,
        }],
    };
    let wire = Request {
        operation: Operation::NvlinkWrite,
        epoch,
        region: 7,
        offset: 11,
        length: 17,
    };
    let mut export = IpcExport::new(remote).unwrap();
    let requester_epoch = engine.source_epoch();
    let source = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut request = [0; 52];
        socket.read_exact(&mut request).unwrap();
        assert_eq!(request, wire.encode());
        let mut hello = [0; HELLO_SIZE];
        socket.read_exact(&mut hello).unwrap();
        assert_eq!(&hello[36..], requester_epoch.0);
        let mut grant = [0; GRANT_SIZE];
        grant[..4].copy_from_slice(b"NVL1");
        let mut identity = Identity::local().unwrap();
        identity.process = SourceEpoch([0x77; 16]);
        identity.encode(&mut grant[4..36]);
        grant[36..88].copy_from_slice(&request);
        grant[88..104].copy_from_slice(&requester_epoch.0);
        grant[104..120].fill(0x66);
        grant[120..128].copy_from_slice(&(export.extent as u64).to_be_bytes());
        grant[128..136].copy_from_slice(&11u64.to_be_bytes());
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
                "uncertain mapping sent a release receipt"
            );
        } else {
            socket.read_exact(&mut receipt).unwrap();
            assert_eq!(&receipt[..BINDING_SIZE], &grant[36..120]);
            assert_eq!(receipt[BINDING_SIZE], u8::from(!success));
            export.finish();
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
    });
    let request = TransferRequest {
        transport: Transport::Nvlink,
        local_address: registered.base_address() + 7,
        remote_address: peer.regions[0].address + 11,
        length: 17,
    };
    let (failed, drained, effect) = if asynchronous {
        let mut batch = engine.allocate_batch(1).unwrap();
        batch
            .submit(
                TransferOp::Write,
                &peer,
                &[request],
                None,
                Duration::from_secs(2),
            )
            .unwrap();
        batch.seal().unwrap();
        let status = batch.wait(Duration::from_secs(2)).unwrap();
        let task = batch.task_status(0).unwrap();
        let result = (
            task.failure.is_some(),
            status.locally_drained,
            task.failure.as_ref().map(|f| f.remote_effect),
        );
        assert_eq!(batch.free().is_ok(), !uncertain);
        drop(batch);
        result
    } else {
        let report = engine
            .submit_and_wait(TransferOp::Write, &peer, &[request], Duration::from_secs(2))
            .unwrap();
        (
            report.failure.is_some(),
            report.locally_drained,
            report.failure.as_ref().map(|f| f.remote_effect),
        )
    };
    source.join().unwrap();
    assert_eq!(failed, !success, "{case}");
    assert_eq!(drained, !uncertain, "{case}");
    if !success {
        assert_eq!(
            effect,
            Some(if case == "budget" {
                RemoteEffect::None
            } else {
                RemoteEffect::Unknown
            }),
            "{case}"
        );
    }
    engine.unregister_memory(&registered).unwrap();
    drop(registered);
    drop(local);
    let stats = engine.stats().unwrap();
    let ipc = stats.nvlink.unwrap();
    assert_eq!(
        stats.inflight_bytes,
        if uncertain { 17 } else { 0 },
        "{case}"
    );
    assert_eq!(
        ipc.allocation_bytes,
        if uncertain { 2 << 20 } else { 0 },
        "{case}"
    );
    assert_eq!(ipc.retained_imports, usize::from(uncertain), "{case}");
    drop(engine);
    assert_eq!(
        local_weak.upgrade().is_some(),
        uncertain,
        "actual owner after dropping public handles: {case}"
    );
}

#[path = "nvlink/subbatch.rs"]
pub(crate) mod subbatch;
