use super::*;

#[test]
fn cached_ipc_rotating_working_set_reclaims_lru_and_flush_keeps_admission_open() {
    for operation in [TransferOp::Read, TransferOp::Write] {
        let dst = allocation(&[0xa5; 128]);
        let local = owned(&dst, false);
        let source = engine();
        let requester = engine();
        let registered = register(&requester, &local);
        let relay = Relay::new(source.endpoint());
        let remotes: Vec<_> = (0..3)
            .map(|i| {
                let device = allocation(&[0x51 + i; 128]);
                let memory = owned(&device, true);
                let region = register(&source, &memory);
                (device, memory, region)
            })
            .collect();
        // Reusing A protects it when B is older, then reopening B evicts C.
        for (index, opens, closes, idle) in [
            (0, 1, 0, vec![0]),
            (1, 2, 0, vec![0, 1]),
            (0, 2, 0, vec![0, 1]),
            (2, 3, 1, vec![0, 2]),
            (0, 3, 1, vec![0, 2]),
            (1, 4, 2, vec![0, 1]),
        ] {
            local.write(0, &[0xa5; 128]).unwrap();
            let region = &remotes[index].2;
            let report = requester
                .submit_and_wait(
                    operation,
                    &relay.peer(&source, region),
                    &[request(&registered, region)],
                    TIMEOUT,
                )
                .unwrap();
            assert!(report.is_complete() && report.locally_drained, "{report:?}");
            let mut actual = [0; 128];
            local.read(0, &mut actual).unwrap();
            let mut expected = [0xa5; 128];
            if operation == TransferOp::Read {
                expected[7..24].fill(0x51 + index as u8);
            }
            assert_eq!(actual, expected);
            assert_eq!(event_count(&dst, "ipc-open"), opens);
            assert_eq!(event_count(&dst, "ipc-close"), closes);
            for (i, (_, memory, _)) in remotes.iter().enumerate() {
                assert_eq!(memory.read(0, &mut [0]).is_err(), idle.contains(&i));
            }
            for engine in [&requester, &source] {
                let stats = engine.stats().unwrap().nvlink.unwrap();
                assert_eq!(stats.allocation_bytes, idle.len() * (2 << 20));
                assert_eq!(stats.imports + stats.exports, idle.len());
            }
        }
        requester.flush_nvlink_cache(TIMEOUT).unwrap();
        assert_eq!(event_count(&dst, "ipc-close"), 4);
        for (i, (_, memory, _)) in remotes.iter().enumerate() {
            let mut actual = [0; 128];
            memory.read(0, &mut actual).unwrap();
            let mut expected = [0x51 + i as u8; 128];
            if operation == TransferOp::Write {
                expected[11..28].fill(0xa5);
            }
            assert_eq!(actual, expected);
        }
        assert_eq!(source.stats().unwrap().nvlink.unwrap().exports, 0);
        assert_eq!(requester.stats().unwrap().nvlink.unwrap().imports, 0);
        let region = &remotes[0].2;
        assert!(
            requester
                .submit_and_wait(
                    operation,
                    &relay.peer(&source, region),
                    &[request(&registered, region)],
                    TIMEOUT,
                )
                .unwrap()
                .is_complete()
        );
        assert_eq!(event_count(&dst, "ipc-open"), 5);
        requester.close(TIMEOUT).unwrap();
        source.close(TIMEOUT).unwrap();
    }
}

#[test]
fn cached_ipc_replacement_reserves_target_until_receipt_without_blocking_hot_keys() {
    let dst = allocation(&[0xa5; 128]);
    let local = owned(&dst, false);
    let source = engine();
    let requester = engine();
    let registered = register(&requester, &local);
    let relay = Relay::new(source.endpoint());
    let remotes: Vec<_> = (0..3)
        .map(|_| {
            let device = allocation(&[0x5a; 128]);
            let memory = owned(&device, true);
            let region = register(&source, &memory);
            (device, memory, region)
        })
        .collect();
    for (_, _, region) in &remotes[..2] {
        assert!(
            requester
                .submit_and_wait(
                    TransferOp::Read,
                    &relay.peer(&source, region),
                    &[request(&registered, region)],
                    TIMEOUT,
                )
                .unwrap()
                .is_complete()
        );
    }
    let (entered, reached) = mpsc::sync_channel(1);
    let (resume, released) = mpsc::channel();
    *relay.close_gate.lock().unwrap() = Some((entered, released));
    let peer = relay.peer(&source, &remotes[2].2);
    let cold = request(&registered, &remotes[2].2);
    let replacing = {
        let (engine, peer, request) = (requester.clone(), peer.clone(), cold.clone());
        thread::spawn(move || {
            engine
                .submit_and_wait(TransferOp::Read, &peer, &[request], TIMEOUT)
                .unwrap()
        })
    };
    reached.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(event_count(&dst, "ipc-close"), 1);
    assert_eq!(event_count(&remotes[2].0, "ipc-export"), 0);
    // The old physical permit is released, but the cache slot is still charged.
    assert_eq!(requester.stats().unwrap().nvlink.unwrap().imports, 1);
    let context = local.allocation.lock().unwrap().context.clone();
    let host = Arc::new(HostMemory::from_cuda_pinned(Arc::new(
        PinnedAllocation::new(&context, 128).unwrap(),
    )));
    let waiting = requester
        .register_host_memory(host, 0, 128, Permission::LocalOnly)
        .unwrap();
    let timed_out = requester
        .submit_and_wait(
            TransferOp::Read,
            &peer,
            &[request(&waiting, &remotes[2].2)],
            Duration::from_millis(5),
        )
        .unwrap();
    assert!(matches!(timed_out.failure.unwrap().error, Error::Timeout));
    // A duplicate cold miss must not evict the second idle entry. Its hit can
    // progress while the first close receipt is held outside the cache lock.
    assert!(
        requester
            .submit_and_wait(
                TransferOp::Read,
                &relay.peer(&source, &remotes[1].2),
                &[request(&waiting, &remotes[1].2)],
                TIMEOUT,
            )
            .unwrap()
            .is_complete()
    );
    assert_eq!(event_count(&dst, "ipc-open"), 2);
    assert_eq!(event_count(&dst, "ipc-close"), 1);
    resume.send(()).unwrap();
    assert!(replacing.join().unwrap().is_complete());
    assert!(
        requester
            .submit_and_wait(TransferOp::Read, &peer, &[cold], TIMEOUT)
            .unwrap()
            .is_complete()
    );
    assert_eq!(event_count(&dst, "ipc-open"), 3);
    requester.close(TIMEOUT).unwrap();
    source.close(TIMEOUT).unwrap();
}

#[test]
fn cached_ipc_reclaim_failure_retains_capacity_and_only_retries_close_receipts() {
    for native_failure in [false, true] {
        let dst = allocation(&[0xa5; 128]);
        let local = owned(&dst, false);
        let source = engine_with_limit(1);
        let requester = engine_with_limit(1);
        let registered = register(&requester, &local);
        let relay = Relay::new(source.endpoint());
        let remotes: Vec<_> = (0..2)
            .map(|_| {
                let device = allocation(&[0x5a; 128]);
                let memory = owned(&device, true);
                let region = register(&source, &memory);
                (device, memory, region)
            })
            .collect();
        let transfer = |index: usize| {
            let region = &remotes[index].2;
            requester
                .submit_and_wait(
                    TransferOp::Read,
                    &relay.peer(&source, region),
                    &[request(&registered, region)],
                    TIMEOUT,
                )
                .unwrap()
        };
        assert!(transfer(0).is_complete());
        if native_failure {
            dst.ipc_close_error.store(1, Ordering::Release);
        } else {
            relay.fault.store(2, Ordering::Release);
        }
        let failed = transfer(1);
        assert!(failed.failure.is_some());
        assert_eq!(failed.locally_drained, !native_failure);
        assert_eq!(event_count(&dst, "ipc-close"), 1);
        assert_eq!(event_count(&remotes[1].0, "ipc-export"), 0);
        dst.ipc_close_error.store(0, Ordering::Release);
        let retry = transfer(1);
        assert_eq!(retry.is_complete(), !native_failure, "{retry:?}");
        assert_eq!(event_count(&dst, "ipc-close"), 1);
        if native_failure {
            assert!(matches!(
                retry.failure.unwrap().error,
                Error::ResourceExhausted
            ));
            assert!(matches!(
                requester.flush_nvlink_cache(TIMEOUT),
                Err(Error::Unquiesced { .. })
            ));
            assert_eq!(
                requester.stats().unwrap().nvlink.unwrap().retained_imports,
                1
            );
            assert_eq!(event_count(&dst, "ipc-open"), 1);
            assert!(remotes[0].1.read(0, &mut [0]).is_err());
            assert!(matches!(
                source.close_source(Duration::from_millis(5)),
                Err(Error::Timeout)
            ));
        } else {
            remotes[0].1.read(0, &mut [0]).unwrap();
            assert_eq!(event_count(&dst, "ipc-open"), 2);
            requester.flush_nvlink_cache(TIMEOUT).unwrap();
            requester.close(TIMEOUT).unwrap();
            source.close(TIMEOUT).unwrap();
        }
    }
}
