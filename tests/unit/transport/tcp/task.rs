use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use super::*;
use crate::{
    EngineConfig, HostMemory, Permission, RemoteRegion, SourceEpoch, TransferEngine, Transport,
};

#[test]
fn fragmented_read_commits_only_the_full_payload_and_eof_preserves_guards() {
    for truncate in [false, true] {
        let engine = TransferEngine::new(EngineConfig::default()).unwrap();
        let local = engine
            .register_memory(vec![0xcc; 25].into_boxed_slice(), Permission::LocalOnly)
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let epoch = SourceEpoch([7; 16]);
        let peer = RemoteSegment {
            endpoint: listener.local_addr().unwrap(),
            epoch,
            regions: vec![],
        };
        let request = PreparedTransfer {
            #[cfg(feature = "rdma")]
            priority: Default::default(),
            #[cfg(feature = "cuda")]
            remote_staging: false,
            transport: Transport::Tcp,
            local: local.clone(),
            local_offset: 3,
            remote: RemoteRegion {
                epoch,
                id: 1,
                address: 1,
                length: 19,
                permission: Permission::RemoteReadWrite,
            },
            remote_offset: 0,
            length: 19,
        };
        #[cfg(feature = "cuda")]
        let proxy =
            crate::runtime::proxy_manager::ProxyManager::new(32, 1, crate::StagingLocation::Any)
                .unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut task = TcpTask::new(
            TransferOp::Read,
            &peer,
            &request,
            deadline,
            #[cfg(feature = "cuda")]
            &proxy,
        )
        .unwrap();
        let mut channel = ControlChannel::connect(peer.endpoint, deadline).unwrap();
        let (mut server, _) = listener.accept().unwrap();
        server.set_nodelay(true).unwrap();
        let poll = |task: &mut TcpTask, channel: &mut ControlChannel<'_>| {
            task.progress(
                channel,
                &mut false,
                #[cfg(feature = "cuda")]
                &proxy,
            )
        };
        assert!(!poll(&mut task, &mut channel).unwrap());
        let mut frame = [0; REQUEST_SIZE];
        server.read_exact(&mut frame).unwrap();
        assert_eq!(frame, task.frame);
        for byte in (Reply {
            status: 0,
            epoch,
            length: 19,
        })
        .encode()
        {
            server.write_all(&[byte]).unwrap();
            assert!(!poll(&mut task, &mut channel).unwrap());
        }
        let payload: Vec<u8> = (0..19).map(|i| i * 11 + 3).collect();
        server.write_all(&payload[..11]).unwrap();
        while task.position != 11 {
            assert!(!poll(&mut task, &mut channel).unwrap());
        }
        let mut observed = [0; 25];
        local.read(0, &mut observed).unwrap();
        assert_eq!(observed, [0xcc; 25]);
        if truncate {
            drop(server);
            let error = loop {
                match poll(&mut task, &mut channel) {
                    Err(error) => break error,
                    Ok(done) => assert!(!done),
                }
            };
            assert!(matches!(error, Error::Io(_)));
        } else {
            server.write_all(&payload[11..]).unwrap();
            while !poll(&mut task, &mut channel).unwrap() {
                std::thread::yield_now();
            }
        }
        local.read(0, &mut observed).unwrap();
        let mut expected = [0xcc; 25];
        if !truncate {
            expected[3..22].copy_from_slice(&payload);
        }
        assert_eq!(observed, expected);
        assert_eq!(task.effect(), RemoteEffect::None);
    }
}

#[test]
fn write_backpressure_retains_snapshot_and_registration_until_terminal_failure() {
    let size = 16 << 20;
    let engine = TransferEngine::new(EngineConfig::default()).unwrap();
    let host = Arc::new(HostMemory::new(vec![0x57; size].into_boxed_slice()).unwrap());
    let weak = Arc::downgrade(&host);
    let local = engine
        .register_host_memory(host.clone(), 0, size, Permission::LocalOnly)
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let epoch = SourceEpoch([8; 16]);
    let peer = RemoteSegment {
        endpoint: listener.local_addr().unwrap(),
        epoch,
        regions: vec![],
    };
    let request = PreparedTransfer {
        #[cfg(feature = "rdma")]
        priority: Default::default(),
        #[cfg(feature = "cuda")]
        remote_staging: false,
        transport: Transport::Tcp,
        local: local.clone(),
        local_offset: 0,
        remote: RemoteRegion {
            epoch,
            id: 1,
            address: 1,
            length: size,
            permission: Permission::RemoteReadWrite,
        },
        remote_offset: 0,
        length: size,
    };
    #[cfg(feature = "cuda")]
    let proxy =
        crate::runtime::proxy_manager::ProxyManager::new(32, 1, crate::StagingLocation::Any)
            .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut task = TcpTask::new(
        TransferOp::Write,
        &peer,
        &request,
        deadline,
        #[cfg(feature = "cuda")]
        &proxy,
    )
    .unwrap();
    local.write(0, &[0xcc; 16]).unwrap();
    let mut channel = ControlChannel::connect(peer.endpoint, deadline).unwrap();
    let (mut server, _) = listener.accept().unwrap();
    server.set_nodelay(true).unwrap();
    server
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let poll = |task: &mut TcpTask, channel: &mut ControlChannel<'_>, advanced: &mut bool| {
        task.progress(
            channel,
            advanced,
            #[cfg(feature = "cuda")]
            &proxy,
        )
    };
    assert!(!poll(&mut task, &mut channel, &mut false).unwrap());
    let mut frame = [0; REQUEST_SIZE];
    server.read_exact(&mut frame).unwrap();
    assert_eq!(task.effect(), RemoteEffect::None);
    server
        .write_all(
            &(Reply {
                status: 9,
                epoch,
                length: 0,
            })
            .encode(),
        )
        .unwrap();
    let mut stalled = false;
    for _ in 0..4096 {
        let before = task.position;
        let mut advanced = false;
        assert!(!poll(&mut task, &mut channel, &mut advanced).unwrap());
        if matches!(task.state, State::Payload) && !advanced && task.position > 0 {
            stalled = true;
            break;
        }
        if matches!(task.state, State::Payload) && task.position >= before {
            assert!(task.position - before <= 64 << 10);
        }
    }
    assert!(
        stalled && task.position < size,
        "the receiver must still be refusing payload"
    );
    assert_eq!(task.effect(), RemoteEffect::Unknown);
    engine.unregister_memory(&local).unwrap();
    drop(local);
    drop(request);
    drop(host);
    assert!(
        weak.upgrade().is_some(),
        "the actual registration belongs to TcpTask"
    );
    channel.deadline = Instant::now();
    assert!(matches!(
        poll(&mut task, &mut channel, &mut false),
        Err(Error::Timeout)
    ));
    drop(channel);
    let mut received = Vec::new();
    server.read_to_end(&mut received).unwrap();
    assert!(!received.is_empty() && received.len() < size);
    assert!(
        received.iter().all(|byte| *byte == 0x57),
        "the original WRITE snapshot survives backpressure"
    );
    drop(task);
    assert!(weak.upgrade().is_none());
}
