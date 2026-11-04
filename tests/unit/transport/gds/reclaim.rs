use super::*;
use crate::runtime::transport_selector::SelectionResult;
use crate::transport::gds::tests::{Fixture, Mock};
use crate::{EngineConfig, Transport};

#[test]
fn consecutive_poll_errors_quarantine_once_and_pending_resets_the_streak() -> Result<()> {
    let mock = Mock::new(GdsMode::NativeOnly);
    let f = Fixture::new(&mock, EngineConfig::default(), 8)?;
    let local = f.host(8)?;
    let (lease, offset) = f.engine.files.admit(&f.segment, TransferOp::Write, 0, 8)?;
    let request = FileRequest {
        parts: crate::segment::file::FileParts::One(crate::segment::file::FilePart {
            local: Some(local.clone()),
            buffer: None,
            local_offset: 0,
            length: 8,
        }),
        selection: SelectionResult::plain(Transport::Gds),
        lease,
        offset,
        length: 8,
    };
    let provider = f
        .engine
        .gds
        .as_ref()
        .unwrap()
        .resolve(&EngineConfig::default())
        .unwrap();
    let mut live = LiveBatch::prepare(provider, TransferOp::Write, std::slice::from_ref(&request))?;
    // Enter deferred polling at the real shim boundary with an accepted IO,
    // bypassing only the original caller's elapsed destruction window.
    let handle = mock.setup(1)?;
    live.handle = Some(handle);
    live.active = 0..1;
    live.submitted = true;
    mock.submit(handle, &mut live.params)?;
    mock.held.store(true, Ordering::Release);
    let poll_error = |batch: &mut LiveBatch| {
        mock.poll_error_once.store(true, Ordering::Release);
        assert!(!batch.collect());
    };
    for _ in 0..4095 {
        poll_error(&mut live);
    }
    assert!(!live.permanent);
    assert!(!live.collect());
    assert_eq!(live.reclaim_failures, 0);
    for _ in 0..4096 {
        poll_error(&mut live);
    }
    assert!(live.permanent);
    let polls = mock.polls.load(Ordering::Acquire);
    mock.held.store(false, Ordering::Release);
    for _ in 0..100 {
        assert!(!live.collect());
    }
    assert_eq!(mock.polls.load(Ordering::Acquire), polls);
    assert_eq!(mock.destroys.load(Ordering::Acquire), 0);
    assert!(local.read(0, &mut [0]).is_err());
    // Keep the uncertain handle, fd and loan just as permanent quarantine does.
    std::mem::forget((live, request));
    Ok(())
}
