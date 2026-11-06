use super::*;
use crate::test_support::EVENTUAL_PROGRESS as WAIT;

#[test]
fn peer_retirement_nvlink_waits_for_event_and_preserves_other_imports() {
    for fail in [false, true] {
        let src = allocation(&[0x5a; 128]);
        let src2 = allocation(&[0x6b; 128]);
        let dst = allocation(&[0xa5; 128]);
        let remote = owned(&src, true);
        let remote2 = owned(&src2, true);
        let local = owned(&dst, false);
        let source = engine();
        let other = engine();
        let requester = engine_with_limit(4);
        let r = register(&source, &remote);
        let rr = register(&other, &remote2);
        let l = register(&requester, &local);
        let relay = Relay::new(source.endpoint());
        let second = Relay::new(other.endpoint());
        let p = relay.peer(&source, &r);
        let pp = second.peer(&other, &rr);
        let q = request(&l, &r);
        dst.query_hold.store(true, Ordering::Release);
        let active = requester
            .submit_async(TransferOp::Read, &p, std::slice::from_ref(&q), None, WAIT)
            .unwrap();
        super::super::ipc::wait_query(&dst);
        assert!(matches!(
            requester.close_segment(p.endpoint, p.epoch, Duration::ZERO),
            Err(Error::Unquiesced { .. })
        ));
        assert_eq!(requester.stats().unwrap().inflight_bytes, 17);
        assert!(matches!(
            requester.submit_and_wait(TransferOp::Read, &p, &[q], WAIT),
            Err(Error::PeerClosed)
        ));
        assert_eq!(event_count(&dst, "ipc-close"), 0);
        dst.query_hold.store(false, Ordering::Release);
        assert_eq!(active.wait(WAIT).unwrap().completed_tasks, 1);
        let mut actual = [0; 128];
        local.read(0, &mut actual).unwrap();
        let mut want = [0xa5; 128];
        want[7..24].fill(0x5a);
        assert_eq!(actual, want);
        assert!(
            requester
                .submit_and_wait(TransferOp::Read, &pp, &[request(&l, &rr)], WAIT)
                .unwrap()
                .is_complete()
        );
        assert_eq!(requester.stats().unwrap().nvlink.unwrap().imports, 2);
        if fail {
            dst.ipc_close_error.store(1, Ordering::Release);
        }
        for _ in 0..2 {
            let result = requester.close_segment(p.endpoint, p.epoch, WAIT);
            if fail {
                assert!(matches!(result, Err(Error::Unquiesced { .. })));
            } else {
                result.unwrap();
            }
        }
        assert_eq!(event_count(&dst, "ipc-close"), 1);
        dst.ipc_close_error.store(0, Ordering::Release);
        let n = requester.stats().unwrap().nvlink.unwrap();
        assert_eq!(n.retained_imports, usize::from(fail));
        assert!(
            requester
                .submit_and_wait(TransferOp::Read, &pp, &[request(&l, &rr)], WAIT)
                .unwrap()
                .is_complete()
        );
        assert_eq!(event_count(&dst, "ipc-open"), 2);
        local.read(0, &mut actual).unwrap();
        want[7..24].fill(0x6b);
        assert_eq!(actual, want);
        if fail {
            assert!(matches!(
                remote.read(0, &mut actual),
                Err(Error::Unavailable(_))
            ));
        } else {
            remote.read(0, &mut actual).unwrap();
            assert_eq!(actual, [0x5a; 128]);
        }
        requester
            .close_segment(pp.endpoint, pp.epoch, WAIT)
            .unwrap();
        assert_eq!(event_count(&dst, "ipc-close"), 2);
        if fail {
            assert!(matches!(
                requester.close_requester(WAIT),
                Err(Error::Unquiesced { .. })
            ));
        }
    }
}
