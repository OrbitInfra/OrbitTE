use super::*;
use std::{cell::RefCell, sync::mpsc, time::Duration};

const EVENTUAL_PROGRESS: Duration = Duration::from_secs(30);
thread_local! {
    static COPY_HOLD: RefCell<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>> = const { RefCell::new(None) };
}
pub(super) fn copy_entered() {
    COPY_HOLD.with(|slot| {
        if let Some((entered, resume)) = slot.borrow_mut().take() {
            entered.send(()).unwrap();
            resume.recv_timeout(EVENTUAL_PROGRESS).unwrap();
        }
    });
}

#[test]
fn synchronous_copy_excludes_concurrent_alias_publication() {
    let e = TransferEngine::new(Default::default()).unwrap();
    let peer = TransferEngine::new(Default::default()).unwrap();
    let mut memory = Memory::default();
    let src = memory.allocate(&e, 66, Opt::default()).unwrap() as usize;
    let dst = memory.allocate(&e, 66, Opt::default()).unwrap() as usize;
    for (address, value) in [(src, 0x51), (dst, 0x27)] {
        let Some(MemoryBacking::Host(h)) = &memory.alloc[&address].memory else {
            panic!("expected host fixture");
        };
        h.write(0, &[value; 66]).unwrap();
    }
    let (entered, held) = mpsc::sync_channel(1);
    let (resume, release) = mpsc::sync_channel(1);
    let mut other = std::thread::scope(|scope| {
        let copying = scope.spawn(|| {
            COPY_HOLD.with(|slot| *slot.borrow_mut() = Some((entered, release)));
            memory
                .plugin_copy((dst + 1) as *mut c_void, (src + 1) as *mut c_void, 64)
                .unwrap();
        });
        held.recv_timeout(EVENTUAL_PROGRESS).unwrap();
        // This is the same lock resolve() must acquire before upgrading an
        // allocation's weak backing to publish a registration in another engine.
        assert!(matches!(
            spans().try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ));
        let (attempting, attempted) = mpsc::sync_channel(1);
        let peer = &peer;
        let alias = scope.spawn(move || {
            let mut other = Memory::default();
            attempting.send(()).unwrap();
            other
                .register(peer, &[dst as *mut c_void], &[66], Opt::default())
                .unwrap();
            other
        });
        attempted.recv_timeout(EVENTUAL_PROGRESS).unwrap();
        resume.send(()).unwrap();
        copying.join().unwrap();
        alias.join().unwrap()
    });
    assert_eq!(
        memory.plugin_copy(dst as *mut c_void, src as *mut c_void, 64),
        Err(FULL)
    );
    assert_eq!(memory.free(&e, dst as *mut c_void), Err(FULL));
    other
        .unregister(&peer, &[dst as *mut c_void], Some(&[66]))
        .unwrap();
    let Some(MemoryBacking::Host(h)) = &memory.alloc[&dst].memory else {
        panic!("expected host fixture");
    };
    let mut actual = [0; 66];
    h.read(0, &mut actual).unwrap();
    assert_eq!(actual[0], 0x27);
    assert_eq!(actual[65], 0x27);
    assert_eq!(&actual[1..65], &[0x51; 64]);
    memory.close(&e).unwrap();
}
