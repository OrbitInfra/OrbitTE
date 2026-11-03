use std::time::Instant;

use super::*;
use crate::ReceiverCreditConfig;
use crate::runtime::control_plane::{ControlChannel, Operation, Reply, Request};
#[cfg(feature = "rdma")]
use crate::runtime::proxy_manager::ProxyManager;

const TIMEOUT: Duration = Duration::from_secs(5);

fn engine(transport: Transport, receiver: bool) -> TransferEngine {
    let depth = if receiver { 2 } else { 4 };
    let config = EngineConfig {
        remote_staging: true,
        staging_chunk_size: if receiver { 3 } else { 7 },
        staging_chunk_count: depth,
        staging_pipeline_depth: depth,
        handler_threads: 2,
        max_receiver_credit_sessions: usize::from(!receiver),
        receiver_credit: receiver.then_some(ReceiverCreditConfig {
            max_staging_slots: 2,
            max_consumer_slots: 1,
            staging_slots_per_sender: 2,
            consumer_slots_per_sender: 1,
            ..Default::default()
        }),
        ..Default::default()
    };
    #[cfg(feature = "rdma")]
    if transport == Transport::Rdma {
        let mut runtime = crate::transport::rdma::tests::runtime("direct:copy");
        runtime.proxy = ProxyManager::new(
            config.staging_chunk_size,
            depth,
            crate::StagingLocation::Any,
        )
        .unwrap();
        return crate::engine::tests::rdma_fixture_with_config(runtime, config);
    }
    let _ = transport;
    TransferEngine::new(config).unwrap()
}

#[test]
fn automatic_staging_credit_uses_negotiated_depth_and_committed_groups_finish_after_fence() {
    for transport in [
        Transport::Tcp,
        #[cfg(feature = "rdma")]
        Transport::Rdma,
    ] {
        for operation in [TransferOp::Read, TransferOp::Write] {
            let source = engine(transport, true);
            let requester = Arc::new(engine(transport, false));
            let a = allocation(&[0x57; 16]);
            let gpu = memory(&a);
            let remote = source
                .register_cuda_memory_batch(&[(gpu.clone(), 0, 16)], Permission::RemoteReadWrite)
                .unwrap()
                .remove(0);
            let local = requester
                .register_memory(vec![0xcc; 6].into_boxed_slice(), Permission::LocalOnly)
                .unwrap();
            let peer = requester
                .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
                .unwrap();
            requester
                .connect_receiver_credit(&peer, 7, TIMEOUT)
                .unwrap();
            // An unbound Pin cannot bypass an enabled source owner gate.
            let mut raw =
                ControlChannel::connect(source.endpoint(), Instant::now() + TIMEOUT).unwrap();
            raw.write_all(
                &Request {
                    operation: Operation::PinRead,
                    epoch: source.source_epoch(),
                    region: remote.id(),
                    offset: 0,
                    length: 5,
                }
                .encode(),
            )
            .unwrap();
            raw.write_all(&[0, 4]).unwrap();
            raw.write_all(&7u64.to_be_bytes()).unwrap();
            raw.write_all(&[1]).unwrap();
            raw.write_all(&[0; 24]).unwrap();
            assert!(matches!(
                Reply::read(&mut raw, source.source_epoch()),
                Err(Error::Unavailable(_))
            ));
            assert_eq!(source.stats().unwrap().staging.allocations, 0);
            drop(raw);
            let requests = [(0, 0, 5), (5, 8, 1)].map(|(local_offset, remote_offset, length)| {
                TransferRequest::new(
                    transport,
                    local.base_address() + local_offset,
                    remote.base_address() + remote_offset,
                    length,
                )
                .with_remote_staging(true)
            });
            a.query_hold.store(true, Ordering::Release);
            let mut batch = requester
                .submit_async(operation, &peer, &requests, None, TIMEOUT)
                .unwrap();
            let until = Instant::now() + crate::test_support::EVENTUAL_PROGRESS;
            while a.event_not_ready.load(Ordering::Acquire) == 0 {
                assert!(Instant::now() < until, "Pin did not reach CUDA completion");
                std::thread::yield_now();
            }
            let stats = source.receiver_credit_issuer_stats().unwrap();
            assert_eq!(
                (stats.active_staging_slots, stats.active_consumer_slots),
                (2, 1)
            );
            assert_eq!(source.stats().unwrap().staging.active_chunks, 2);
            assert!(matches!(
                requester.disconnect_receiver_credit(&peer, TIMEOUT),
                Err(Error::BatchBusy)
            ));
            a.query_hold.store(false, Ordering::Release);
            assert_eq!(
                batch.wait(TIMEOUT).unwrap().state,
                BatchState::Completed,
                "{transport:?}/{operation:?}"
            );
            batch.free().unwrap();
            let stats = source.receiver_credit_issuer_stats().unwrap();
            assert_eq!(
                (stats.active_staging_slots, stats.active_consumer_slots),
                (0, 0)
            );
            assert_eq!(source.stats().unwrap().staging.remote_unpins, 2);
            let mut bytes = [0; 6];
            local.read(0, &mut bytes).unwrap();
            assert_eq!(
                bytes,
                [if operation == TransferOp::Read {
                    0x57
                } else {
                    0xcc
                }; 6]
            );
            let mut actual = [0; 16];
            gpu.read(0, &mut actual).unwrap();
            let mut expected = [0x57; 16];
            if operation == TransferOp::Write {
                expected[..5].fill(0xcc);
                expected[8] = 0xcc;
            }
            assert_eq!(actual, expected);
            requester
                .disconnect_receiver_credit(&peer, TIMEOUT)
                .unwrap();
            let stats = source.receiver_credit_issuer_stats().unwrap();
            assert_eq!(
                (stats.reserved_staging_slots, stats.reserved_consumer_slots),
                (0, 0)
            );
            requester.close(TIMEOUT).unwrap();
            source.close(TIMEOUT).unwrap();
        }
    }
}

#[test]
fn partial_native_pin_retains_only_constructed_staging_and_the_consumer() {
    let source = engine(Transport::Tcp, true);
    let requester = engine(Transport::Tcp, false);
    let a = allocation(&[0x57; 16]);
    let remote = source
        .register_cuda_memory_batch(&[(memory(&a), 0, 16)], Permission::RemoteRead)
        .unwrap()
        .remove(0);
    let local = requester
        .register_memory(vec![0xcc; 5].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let peer = requester
        .open_segment(source.endpoint(), source.source_epoch(), TIMEOUT)
        .unwrap();
    requester
        .connect_receiver_credit(&peer, 7, TIMEOUT)
        .unwrap();
    a.pin_error.store(1, Ordering::Release); // Driver returned backing despite failure.
    let report = requester
        .submit_and_wait(
            TransferOp::Read,
            &peer,
            &[TransferRequest::new(
                Transport::Tcp,
                local.base_address(),
                remote.base_address(),
                5,
            )
            .with_remote_staging(true)],
            TIMEOUT,
        )
        .unwrap();
    assert!(!report.is_complete());
    let stats = source.receiver_credit_issuer_stats().unwrap();
    assert_eq!(
        (stats.active_staging_slots, stats.retained_staging_slots),
        (1, 1)
    );
    assert_eq!(
        (stats.active_consumer_slots, stats.retained_consumer_slots),
        (1, 1)
    );
    assert_eq!(source.stats().unwrap().staging.retained_chunks, 1);
    assert_eq!(a.pins.lock().unwrap().len(), 1);
    assert!(matches!(
        requester.disconnect_receiver_credit(&peer, TIMEOUT),
        Err(Error::BatchBusy)
    ));
    requester.close(TIMEOUT).unwrap();
}
