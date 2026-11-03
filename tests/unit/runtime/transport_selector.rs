use std::sync::Arc;
use std::time::Duration;

use super::*;
use crate::segment::ProcessIdentity;
use crate::{EngineConfig, Permission, SourceEpoch, TransferEngine, TransferOp, TransferRequest};

const TIMEOUT: Duration = Duration::from_secs(3);

fn region(kind: MemoryType, ipc_exportable: bool) -> RemoteRegion {
    RemoteRegion {
        epoch: SourceEpoch([1; 16]),
        id: 1,
        address: 1,
        length: 64,
        permission: Permission::RemoteReadWrite,
        memory_type: kind,
        ipc_exportable,
    }
}

fn context(remote: &RemoteRegion) -> SelectionContext<'_> {
    SelectionContext {
        local_memory: MemoryType::Cpu,
        local_ipc: false,
        remote,
        local_segment: SegmentMetadata {
            rdma: true,
            ..Default::default()
        },
        remote_segment: SegmentMetadata {
            rdma: true,
            ..Default::default()
        },
        transfer_size: 16,
        priority: Priority::High,
        remote_staging: false,
    }
}

#[test]
fn transport_selector_first_match_bounds_priority_and_hard_hint() {
    let remote = region(MemoryType::Cpu, false);
    let mut context = context(&remote);
    let selector = TransportSelector::new(
        &TransportSelectorConfig::from_json(
            r#"{"policy":[
        {"name":"cuda", "segment_type":"memory", "remote_memory":"cuda", "transports":["nvlink"]},
        {"name":"small", "segment_type":"memory", "local_memory":"cpu", "remote_memory":"cpu",
         "min_size":8, "max_size":16, "priority":"high", "transports":["tcp","rdma"]},
        {"name":"bulk", "segment_type":"memory", "transports":["rdma"]}
    ]}"#,
        )
        .unwrap(),
    )
    .unwrap();
    for size in [8, 16] {
        context.transfer_size = size;
        assert_eq!(
            selector.select(&context, Transport::Unspec).unwrap(),
            Transport::Tcp
        );
        assert_eq!(
            selector.select(&context, Transport::Rdma).unwrap(),
            Transport::Rdma
        );
    }
    for size in [7, 17] {
        context.transfer_size = size;
        assert_eq!(
            selector.select(&context, Transport::Unspec).unwrap(),
            Transport::Rdma
        );
        assert!(selector.select(&context, Transport::Tcp).is_err());
    }
    context.transfer_size = 16;
    context.priority = Priority::Medium;
    assert_eq!(
        selector.select(&context, Transport::Unspec).unwrap(),
        Transport::Rdma
    );
    context.priority = Priority::High;
    context.local_memory = MemoryType::Cuda;
    assert_eq!(
        selector.select(&context, Transport::Unspec).unwrap(),
        Transport::Rdma
    );
    context.local_segment.rdma = false;
    assert!(selector.select(&context, Transport::Unspec).is_err());
    context.local_memory = MemoryType::Cpu;
    assert!(selector.select(&context, Transport::Rdma).is_err());
    assert_eq!(
        selector.select(&context, Transport::Unspec).unwrap(),
        Transport::Tcp
    );
    let remote = region(MemoryType::Cuda, false);
    context.remote = &remote;
    // The first matching rule has no eligible NVLink candidate. Do not try small/bulk.
    assert!(selector.select(&context, Transport::Unspec).is_err());
    let no_match = TransportSelector::new(&TransportSelectorConfig {
        policy: vec![SelectionPolicy {
            min_size: Some(100),
            ..Default::default()
        }],
    })
    .unwrap();
    assert!(no_match.select(&context, Transport::Tcp).is_err());
}

#[test]
fn transport_selector_uses_both_owners_and_preserves_staging_intent() {
    let remote = region(MemoryType::Cuda, true);
    let mut context = context(&remote);
    context.local_ipc = true;
    let identity = ProcessIdentity {
        machine: SourceEpoch([2; 16]),
        process: SourceEpoch([3; 16]),
    };
    context.local_segment.nvlink = Some(identity);
    context.remote_segment.nvlink = Some(ProcessIdentity {
        process: SourceEpoch([4; 16]),
        ..identity
    });
    let select =
        |context: &SelectionContext<'_>| TransportSelector::select_default(context).unwrap();
    assert_eq!(select(&context), Transport::Nvlink);
    context.local_ipc = false;
    assert_eq!(select(&context), Transport::Rdma);
    context.local_ipc = true;
    let ordinary = region(MemoryType::Cuda, false);
    context.remote = &ordinary;
    assert_eq!(select(&context), Transport::Rdma);
    context.remote = &remote;
    context.remote_segment.nvlink = Some(identity);
    assert_eq!(select(&context), Transport::Rdma);
    context.remote_segment.nvlink = Some(ProcessIdentity {
        machine: SourceEpoch([5; 16]),
        process: SourceEpoch([4; 16]),
    });
    assert_eq!(select(&context), Transport::Rdma);
    context.remote_segment.nvlink = None;
    context.remote_segment.rdma = false;
    assert_eq!(select(&context), Transport::Tcp);
    context.priority = Priority::Medium;
    assert!(TransportSelector::select_default(&context).is_err());
    context.priority = Priority::High;
    context.remote_staging = true;
    assert!(TransportSelector::select_default(&context).is_err());
    context.remote_segment.remote_staging = true;
    assert_eq!(
        TransportSelector::select_default(&context).is_ok(),
        cfg!(feature = "cuda")
    );
    let host = region(MemoryType::Cpu, false);
    context.remote = &host;
    assert!(TransportSelector::select_default(&context).is_err());
}

#[test]
fn transport_selector_rejects_invalid_or_unconsumed_configuration() {
    for (case, policy) in [
        (
            "positional policy",
            r#"["x","memory","*","*",0,8,0,["tcp"]]"#,
        ),
        ("file", r#"{"segment_type":"file"}"#),
        (
            "bad memory",
            r#"{"segment_type":"memory","local_memory":"npu"}"#,
        ),
        ("bad priority", r#"{"segment_type":"memory","priority":3}"#),
        (
            "inverted bounds",
            r#"{"segment_type":"memory","min_size":2,"max_size":1}"#,
        ),
        (
            "null memory",
            r#"{"segment_type":"memory","remote_memory":null}"#,
        ),
        ("null bound", r#"{"segment_type":"memory","max_size":null}"#),
        (
            "devices",
            r#"{"segment_type":"memory","devices":["mlx5_0"]}"#,
        ),
        ("pool", r#"{"segment_type":"memory","qp_pool":"bulk"}"#),
        (
            "intent",
            r#"{"segment_type":"memory","intent_type":"checkpoint"}"#,
        ),
        (
            "provider",
            r#"{"segment_type":"memory","transports":["iouring"]}"#,
        ),
        (
            "duplicate",
            r#"{"segment_type":"memory","transports":["tcp","tcp"]}"#,
        ),
    ] {
        assert!(
            TransportSelectorConfig::from_json(&format!(r#"{{"policy":[{policy}]}}"#)).is_err(),
            "{case}"
        );
    }
    assert!(TransportSelectorConfig::from_json("[]").is_err());
    assert!(TransportSelectorConfig::from_json(&" ".repeat((1 << 20) + 1)).is_err());
    assert!(
        TransportSelector::new(&TransportSelectorConfig {
            policy: vec![SelectionPolicy {
                transports: vec![Transport::Unspec],
                ..Default::default()
            }]
        })
        .is_err()
    );
}

#[test]
fn transport_selector_public_sync_async_and_atomic_preflight() {
    let source = TransferEngine::new(EngineConfig::default()).unwrap();
    let remote = source
        .register_memory(vec![0; 8].into_boxed_slice(), Permission::RemoteReadWrite)
        .unwrap();
    let requester = Arc::new(
        TransferEngine::new(EngineConfig {
            selector: Some(
                TransportSelectorConfig::from_json(
                    r#"{"policy":[
            {"name":"host", "segment_type":"memory", "local_memory":"cpu", "remote_memory":"cpu",
             "max_size":4, "transports":["rdma","tcp"]},
            {"name":"large", "segment_type":"memory", "transports":["rdma"]}
        ]}"#,
                )
                .unwrap(),
            ),
            ..Default::default()
        })
        .unwrap(),
    );
    let local = requester
        .register_memory(vec![7; 8].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let peer = requester
        .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
        .unwrap();
    assert_eq!(peer.regions()[0].memory_type(), MemoryType::Cpu);
    assert!(!peer.metadata.rdma);
    let make_request = |transport, length| {
        TransferRequest::new(
            transport,
            local.base_address(),
            remote.base_address(),
            length,
        )
    };
    let request = make_request(Transport::Unspec, 4);
    let oversized = make_request(Transport::Unspec, 8);
    assert!(
        requester
            .submit_and_wait(
                TransferOp::Write,
                &peer,
                &[request.clone(), oversized],
                TIMEOUT
            )
            .is_err()
    );
    let mut bytes = [0; 8];
    remote.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [0; 8]);
    assert_eq!(requester.stats().unwrap().inflight_batches, 0);
    assert!(
        requester
            .submit_and_wait(
                TransferOp::Write,
                &peer,
                &[make_request(Transport::Rdma, 4)],
                TIMEOUT
            )
            .is_err()
    );
    let report = requester
        .submit_and_wait(
            TransferOp::Write,
            &peer,
            std::slice::from_ref(&request),
            TIMEOUT,
        )
        .unwrap();
    assert!(report.is_complete());
    remote.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [7, 7, 7, 7, 0, 0, 0, 0]);
    local.write(0, &[0; 8]).unwrap();
    let mut batch = requester.allocate_batch(1).unwrap();
    batch
        .submit(TransferOp::Read, &peer, &[request], None, TIMEOUT)
        .unwrap();
    batch.seal().unwrap();
    batch.wait(TIMEOUT).unwrap();
    local.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [7, 7, 7, 7, 0, 0, 0, 0]);
    batch.free().unwrap();
    // Unconfigured explicit callers keep their existing route, while Unspec also works.
    let direct = TransferEngine::new(EngineConfig::default()).unwrap();
    let target = direct
        .register_memory(vec![0; 4].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let request = TransferRequest::new(
        Transport::Unspec,
        target.base_address(),
        remote.base_address(),
        4,
    );
    assert!(
        direct
            .submit_and_wait(TransferOp::Read, &peer, &[request], TIMEOUT)
            .unwrap()
            .is_complete()
    );
}
