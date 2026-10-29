use super::*;
use crate::runtime::proxy_manager::worker_shard::Workers;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Duration;

#[test]
fn caller_unwind_disconnects_full_completion_queue_before_waiting_for_terminal() {
    let workers = Workers::default();
    let owner = Arc::new(vec![0x47; 16]);
    let weak = Arc::downgrade(&owner);
    let (send, commands) = mpsc::sync_channel(1);
    let (complete, completed) = mpsc::sync_channel(1);
    let (entered, ready) = mpsc::sync_channel(1);
    let worker = workers
        .submit(
            TransferOp::Read,
            Instant::now() + Duration::from_secs(2),
            Arc::new(AtomicBool::new(false)),
            move || {
                assert_eq!(owner[0], 0x47);
                complete.send(0).unwrap();
                entered.send(()).unwrap();
                assert!(
                    complete.send(1).is_err(),
                    "caller must disconnect completion receiver"
                );
                assert!(commands.recv_timeout(Duration::from_secs(1)).is_err());
                (Ok(()), RemoteEffect::None)
            },
        )
        .unwrap();
    let released_before_outer_drop = Arc::new(AtomicBool::new(false));
    struct OuterOwner {
        weak: std::sync::Weak<Vec<u8>>,
        observed: Arc<AtomicBool>,
    }
    impl Drop for OuterOwner {
        fn drop(&mut self) {
            self.observed
                .store(self.weak.upgrade().is_none(), Ordering::Release);
        }
    }
    let result = catch_unwind(AssertUnwindSafe(|| {
        let _outer = OuterOwner {
            weak,
            observed: Arc::clone(&released_before_outer_drop),
        };
        let _session = Session {
            send,
            completed,
            worker,
        };
        ready.recv_timeout(Duration::from_secs(1)).unwrap();
        panic!("injected caller unwind");
    }));
    assert!(result.is_err());
    assert!(
        released_before_outer_drop.load(Ordering::Acquire),
        "outer owners dropped before task terminal"
    );
    workers
        .close(Instant::now() + Duration::from_secs(1))
        .unwrap();
}
