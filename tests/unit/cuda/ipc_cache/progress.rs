use super::*;

#[test]
fn ipc_shared_poller_completes_independent_groups_while_one_event_is_pending() {
    let src_a = allocation(&[0x5a; 128]);
    let src_b = allocation(&[0x3c; 128]);
    let dst = allocation(&[0xa5; 128]);
    let remote_a = owned(&src_a, true);
    let remote_b = owned(&src_b, true);
    let local = owned(&dst, false);
    let source = engine();
    let requester = engine();
    let exported_a = register(&source, &remote_a);
    let exported_b = register(&source, &remote_b);
    let registered = register(&requester, &local);
    let context = local.allocation.lock().unwrap().context.clone();
    let host = Arc::new(HostMemory::from_cuda_pinned(Arc::new(
        PinnedAllocation::new(&context, 128).unwrap(),
    )));
    host.write(0, &[0xa5; 128]).unwrap();
    let host_registered = requester
        .register_host_memory(host.clone(), 0, 128, Permission::LocalOnly)
        .unwrap();
    let relay = Relay::new(source.endpoint());
    let req = request(&registered, &exported_a);
    dst.query_hold_address
        .store(req.local_address as usize, Ordering::Release);
    let mut batch = requester
        .submit_async(
            TransferOp::Read,
            &relay.peer(&source, &exported_a),
            &[req],
            None,
            TIMEOUT,
        )
        .unwrap();
    super::super::ipc::wait_query(&dst);
    assert!(batch.cancel(0).unwrap());
    assert!(matches!(
        batch.wait(Duration::from_millis(5)),
        Err(Error::Timeout)
    ));
    assert!(local.read(100, &mut [0]).is_err());
    // Conflicting NVLink preparation waits for the real owner, within its deadline.
    let conflict = requester.submit_and_wait(
        TransferOp::Read,
        &relay.peer(&source, &exported_b),
        &[request(&registered, &exported_b)],
        Duration::from_millis(5),
    );
    assert!(matches!(conflict, Err(Error::Timeout)), "{conflict:?}");
    dst.query_hold.store(true, Ordering::Release);
    let mut second_batch = requester
        .submit_async(
            TransferOp::Read,
            &relay.peer(&source, &exported_b),
            &[request(&host_registered, &exported_b)],
            None,
            TIMEOUT,
        )
        .unwrap();
    let deadline = Instant::now() + TIMEOUT;
    while requester.stats().unwrap().nvlink.unwrap().pending_groups != 2 {
        assert!(
            Instant::now() < deadline,
            "both groups must enter the shared poller"
        );
        thread::sleep(Duration::from_millis(1));
    }
    dst.query_hold.store(false, Ordering::Release);
    let second = second_batch.wait(TIMEOUT).unwrap();
    assert!(
        second.state == BatchState::Completed && second.locally_drained,
        "{second:?}"
    );
    second_batch.free().unwrap();
    let mut bytes = [0; 128];
    host.read(0, &mut bytes).unwrap();
    let mut expected = [0xa5; 128];
    expected[7..24].fill(0x3c);
    assert_eq!(bytes, expected);
    let stats = requester.stats().unwrap().nvlink.unwrap();
    assert_eq!(
        (
            stats.pending_groups,
            stats.progress_workers,
            stats.stream_slots,
            stats.active_streams
        ),
        (1, 1, 2, 1)
    );
    assert_eq!(event_count(&dst, "event-sync"), 0);
    assert_eq!(event_count(&dst, "stream-sync"), 0);
    assert!(matches!(
        requester.close_requester(Duration::from_millis(5)),
        Err(Error::Timeout)
    ));
    dst.query_hold_address.store(0, Ordering::Release);
    let result = batch.wait(TIMEOUT).unwrap();
    assert!(
        result.state == BatchState::Completed && result.locally_drained,
        "{result:?}"
    );
    batch.free().unwrap();
    local.read(0, &mut bytes).unwrap();
    expected[7..24].fill(0x5a);
    assert_eq!(bytes, expected);
    requester.close_requester(TIMEOUT).unwrap();
    source.close_source(TIMEOUT).unwrap();
    let stats = requester.stats().unwrap().nvlink.unwrap();
    assert_eq!(
        (
            stats.pending_groups,
            stats.progress_workers,
            stats.stream_slots
        ),
        (0, 0, 0)
    );
}

#[test]
fn ipc_pending_deadline_and_query_restore_failure_retain_actual_owners() {
    for restore_failure in [false, true] {
        let src = allocation(&[0x5a; 128]);
        let dst = allocation(&[0xa5; 128]);
        let remote = owned(&src, true);
        let local = owned(&dst, false);
        let source = engine();
        let requester = engine();
        let exported = register(&source, &remote);
        let registered = register(&requester, &local);
        let relay = Relay::new(source.endpoint());
        dst.query_hold.store(true, Ordering::Release);
        dst.pop_after_not_ready
            .store(restore_failure, Ordering::Release);
        let result = requester
            .submit_and_wait(
                TransferOp::Read,
                &relay.peer(&source, &exported),
                &[request(&registered, &exported)],
                Duration::from_millis(100),
            )
            .unwrap();
        assert!(
            !result.locally_drained && !result.is_complete(),
            "{restore_failure}: {result:?}"
        );
        assert!(matches!(
            result.failure.unwrap().error,
            Error::Unquiesced { .. }
        ));
        assert!(dst.event_not_ready.load(Ordering::Acquire) > 0);
        let stats = requester.stats().unwrap().nvlink.unwrap();
        assert_eq!(
            (
                stats.pending_groups,
                stats.active_streams,
                stats.retained_streams
            ),
            (0, 0, 1)
        );
        assert_eq!(event_count(&dst, "event-sync"), 0);
        assert_eq!(event_count(&dst, "stream-sync"), 0);
        assert_eq!(event_count(&dst, "ipc-close"), 0);
        assert!(local.read(0, &mut [0]).is_err());
        assert!(requester.close_requester(Duration::from_millis(5)).is_err());
        let weak = Arc::downgrade(&local);
        drop(registered);
        drop(local);
        drop(requester);
        assert!(weak.upgrade().is_some());
        assert_eq!(event_count(&dst, "free"), 0);
        STACK.with(|stack| stack.borrow_mut().clear());
    }
}
