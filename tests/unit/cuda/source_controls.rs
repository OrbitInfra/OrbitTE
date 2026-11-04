use super::*;

#[test]
fn source_session_isolates_delegate_drain_and_releases_capacity_only_on_confirmation() {
    for transport in [
        Transport::Tcp,
        #[cfg(feature = "rdma")]
        Transport::Rdma,
    ] {
        for uncertain in [false, true] {
            // Leave a physical chunk free. The refused request must exhaust
            // session capacity independently of pinned bytes and the GPU range.
            let source = engine(transport, 2);
            let requester = engine(transport, 1);
            let initial: [u8; 64] = std::array::from_fn(|i| (3 * i + 7) as u8);
            let device = allocation(&initial);
            let gpu = memory(&device);
            let remote = register(&source, gpu.clone());
            let local = requester
                .register_memory(vec![0xc3; 16].into_boxed_slice(), Permission::LocalOnly)
                .unwrap();
            let peer = requester
                .open_segment(source.endpoint(), source.source_epoch(), EVENTUAL_PROGRESS)
                .unwrap();
            let transfer = |op| {
                requester.submit_and_wait(
                    op,
                    &peer,
                    &[TransferRequest::new(
                        transport,
                        local.base_address() + 4,
                        remote.base_address() + 32,
                        8,
                    )
                    .with_remote_staging(true)],
                    EVENTUAL_PROGRESS,
                )
            };

            let (mut channel, nonce) = pin_on(&source, &remote, 1, transport);
            frame(&mut channel, nonce, &remote, Operation::Delegate, 0);
            receipt(&mut channel, nonce);
            let (entered, waiting) = std::sync::mpsc::sync_channel(0);
            let (resume, blocked) = std::sync::mpsc::sync_channel(0);
            *device.event_gate.lock().unwrap() = Some((entered, blocked));
            if uncertain {
                device.event_wait_fail_at.store(1, Ordering::Release);
                device.sync_error.store(700, Ordering::Release);
            }
            drop(channel);
            waiting.recv_timeout(EVENTUAL_PROGRESS).unwrap();
            // The event cannot finish until after both independent RPCs return.
            let metadata =
                requester.open_segment(source.endpoint(), source.source_epoch(), EVENTUAL_PROGRESS);
            let busy = transfer(TransferOp::Write);
            let held = source.stats().unwrap();
            assert!(
                resume.send(()).is_ok(),
                "Describe and the competing Pin must finish while the source Delegate is held"
            );
            let metadata = metadata.unwrap();
            assert_eq!(
                metadata.regions().len(),
                1,
                "a private stage appeared in Describe"
            );
            assert_eq!(metadata.regions()[0].base_address(), remote.base_address());
            let report = busy.unwrap();
            assert_eq!(
                (report.completed_requests, report.transferred_bytes),
                (0, 0)
            );
            let failure = report.failure.unwrap();
            assert!(
                matches!(failure.error, Error::ResourceExhausted),
                "{failure:?}"
            );
            assert_eq!(failure.remote_effect, crate::RemoteEffect::None);
            assert_eq!(
                (
                    held.staging.active_chunks,
                    held.staging.remote_pins,
                    held.active_source_operations
                ),
                (1, 1, 1)
            );

            let deadline = Instant::now() + EVENTUAL_PROGRESS;
            loop {
                let stats = source.stats().unwrap();
                if (uncertain && stats.staging.retained_chunks == 1)
                    || (!uncertain && stats.active_source_operations == 0)
                {
                    break;
                }
                assert!(Instant::now() < deadline);
                std::thread::yield_now();
            }
            if uncertain {
                // Another disjoint Pin still has no slot, but Describe is live.
                assert!(matches!(
                    transfer(TransferOp::Read).unwrap().failure.unwrap().error,
                    Error::ResourceExhausted
                ));
                assert_eq!(
                    requester
                        .open_segment(source.endpoint(), source.source_epoch(), EVENTUAL_PROGRESS)
                        .unwrap()
                        .regions()
                        .len(),
                    1
                );
                assert!(matches!(
                    source.close_source(Duration::ZERO),
                    Err(Error::Timeout)
                ));
                assert_eq!(device.pins.lock().unwrap().len(), 1);
            } else {
                let mut host = [0xc3; 16];
                for _ in 0..4 {
                    assert!(transfer(TransferOp::Read).unwrap().is_complete());
                    local.read(0, &mut host).unwrap();
                    assert_eq!(&host[4..12], &initial[32..40]);
                    assert_eq!(host[..4], [0xc3; 4]);
                    assert_eq!(host[12..], [0xc3; 4]);
                }
                local.write(4, &[0x42; 8]).unwrap();
                assert!(transfer(TransferOp::Write).unwrap().is_complete());
                let mut expected = initial;
                expected[32..40].fill(0x42);
                let mut got = [0; 64];
                gpu.read(0, &mut got).unwrap();
                assert_eq!(got, expected);
                source.close(EVENTUAL_PROGRESS).unwrap();
                assert!(device.pins.lock().unwrap().is_empty());
            }
            requester.close(EVENTUAL_PROGRESS).unwrap();
        }
    }
}

#[cfg(feature = "rdma")]
#[test]
fn source_session_panic_fences_the_slot_and_preserves_the_control_worker() {
    use crate::platform::numa::{NATIVE, Numa};
    static FAULT: Numa = Numa {
        nodes: || panic!("injected source location lookup"),
        ..NATIVE
    };
    let mut native = crate::transport::rdma::tests::runtime("direct:copy");
    let mut pool = ProxyManager::new(3, 2, crate::StagingLocation::Cpu(0)).unwrap();
    Arc::get_mut(&mut pool).unwrap().numa = &FAULT;
    native.proxy = pool;
    let source = crate::engine::tests::rdma_fixture_with_config(native, config(2));
    let requester = engine(Transport::Tcp, 1);
    let device = allocation(&[0x67; 64]);
    let gpu = memory(&device);
    let remote = register(&source, gpu.clone());
    let local = requester
        .register_memory(vec![0x31; 16].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let peer = requester
        .open_segment(source.endpoint(), source.source_epoch(), EVENTUAL_PROGRESS)
        .unwrap();
    let submit = || {
        requester
            .submit_and_wait(
                TransferOp::Write,
                &peer,
                &[TransferRequest::new(
                    Transport::Tcp,
                    local.base_address() + 2,
                    remote.base_address() + 7,
                    8,
                )
                .with_remote_staging(true)],
                EVENTUAL_PROGRESS,
            )
            .unwrap()
    };
    let first = submit().failure.unwrap();
    assert!(matches!(first.error, Error::Io(_)), "{first:?}");
    assert_eq!(first.remote_effect, crate::RemoteEffect::None);
    assert!(matches!(
        submit().failure.unwrap().error,
        Error::ResourceExhausted
    ));
    assert_eq!(
        requester
            .open_segment(source.endpoint(), source.source_epoch(), EVENTUAL_PROGRESS)
            .unwrap()
            .regions()
            .len(),
        1
    );
    let stats = source.stats().unwrap();
    assert_eq!(
        (
            stats.active_source_operations,
            stats.staging.remote_pins,
            stats.staging.allocations
        ),
        (1, 0, 0)
    );
    let mut bytes = [0; 64];
    gpu.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [0x67; 64]);
    assert!(matches!(
        source.close_source(Duration::ZERO),
        Err(Error::Timeout)
    ));
    requester.close(EVENTUAL_PROGRESS).unwrap();
}
