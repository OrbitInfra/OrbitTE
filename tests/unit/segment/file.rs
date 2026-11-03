use std::fs;
use std::path::PathBuf;
use std::thread;

use super::*;
use crate::{EngineConfig, SourceEpoch, TransferEngine};

struct TempFile {
    dir: PathBuf,
    path: PathBuf,
}

impl TempFile {
    fn new(bytes: &[u8]) -> Self {
        let dir =
            std::env::temp_dir().join(format!("orbitte-file-{}", SourceEpoch::generate().unwrap()));
        fs::create_dir(&dir).unwrap();
        let path = dir.join("a %20 file");
        fs::write(&path, bytes).unwrap();
        Self { dir, path }
    }

    fn uri(&self) -> String {
        format!("file://{}", self.path.display())
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[path = "file/isolation.rs"]
mod isolation;

#[cfg(not(any(feature = "io-uring", feature = "gds")))]
#[test]
fn file_hints_without_storage_features_reject_before_opening_the_fd() -> Result<()> {
    use crate::{TransferRequest, Transport};

    let temp = TempFile::new(b"file");
    let engine = TransferEngine::new(EngineConfig::default())?;
    let segment = engine.open_file_segment(&temp.uri(), Permission::RemoteReadWrite)?;
    let memory = engine.register_memory(b"host".to_vec().into(), Permission::LocalOnly)?;
    for hint in [Transport::IoUring, Transport::Gds, Transport::Unspec] {
        let request = [TransferRequest::new(hint, memory.base_address(), 0, 4)];
        for op in [TransferOp::Read, TransferOp::Write] {
            assert!(matches!(
                engine.submit_and_wait(op, &segment, &request, Duration::from_secs(5)),
                Err(Error::Unavailable(_))
            ));
        }
    }
    assert!(segment.0.upgrade().unwrap().lock()?.file.is_none());
    let info = engine.file_segment_info(&segment)?;
    assert_eq!((info.segment_type, info.length), (SegmentType::File, 4));
    let mut bytes = [0; 4];
    memory.read(0, &mut bytes)?;
    assert_eq!(&bytes, b"host");
    assert_eq!(fs::read(&temp.path)?, b"file");
    engine.close_file_segment(&segment, Duration::ZERO)
}

#[test]
fn open_info_refresh_and_close_follow_the_file_descriptor() -> Result<()> {
    let temp = TempFile::new(b"abc");
    let engine = TransferEngine::new(EngineConfig {
        max_file_segments: 1,
        ..Default::default()
    })?;
    let segment = engine.open_file_segment(&temp.uri(), Permission::RemoteRead)?;
    let info = engine.file_segment_info(&segment)?;
    assert_eq!(info.segment_type, SegmentType::File);
    assert_eq!(info.path.as_ref(), temp.path.to_str().unwrap());
    assert_eq!(
        (info.offset, info.length, info.permission),
        (0, 3, Permission::RemoteRead)
    );
    let alias = engine.open_file_segment(&temp.uri(), Permission::RemoteRead)?;
    assert!(Weak::ptr_eq(&segment.0, &alias.0));
    assert!(matches!(
        engine.open_file_segment(&temp.uri(), Permission::RemoteReadWrite),
        Err(Error::PermissionDenied)
    ));
    assert!(matches!(
        engine.open_file_segment("file://another", Permission::RemoteRead),
        Err(Error::ResourceExhausted)
    ));

    fs::write(&temp.path, b"abcdef")?;
    assert_eq!(engine.file_segment_info(&alias)?.length, 3);
    assert_eq!(engine.refresh_file_segment(&segment)?.length, 6);
    assert_eq!(engine.file_segment_info(&alias)?.length, 6);
    assert_eq!(info.length, 3);
    fs::rename(&temp.path, temp.dir.join("original"))?;
    fs::write(&temp.path, b"x")?;
    assert_eq!(engine.refresh_file_segment(&alias)?.length, 6);

    let other = TransferEngine::new(EngineConfig::default())?;
    assert!(matches!(
        other.close_file_segment(&alias, Duration::ZERO),
        Err(Error::FileSegmentClosed)
    ));
    engine.close_file_segment(&alias, Duration::ZERO)?;
    let reopened = engine.open_file_segment(&temp.uri(), Permission::RemoteReadWrite)?;
    assert_eq!(engine.file_segment_info(&reopened)?.length, 1);
    assert!(matches!(
        engine.file_segment_info(&segment),
        Err(Error::FileSegmentClosed)
    ));
    assert!(matches!(
        engine.close_file_segment(&segment, Duration::ZERO),
        Err(Error::FileSegmentClosed)
    ));
    engine.close_file_segment(&reopened, Duration::ZERO)
}

#[test]
fn concurrent_path_opens_share_one_slot_and_one_fd() -> Result<()> {
    use std::sync::Barrier;

    let temp = TempFile::new(b"shared");
    let engine = TransferEngine::new(EngineConfig {
        max_file_segments: 1,
        ..Default::default()
    })?;
    let barrier = Barrier::new(4);
    let handles = thread::scope(|scope| {
        let workers: Vec<_> = (0..4)
            .map(|_| {
                scope.spawn(|| -> Result<_> {
                    barrier.wait();
                    let segment = engine.open_file_segment(&temp.uri(), Permission::RemoteRead)?;
                    let (lease, _) = engine.files.admit(&segment, TransferOp::Read, 0, 1)?;
                    Ok((segment, Arc::downgrade(&lease.file)))
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Result<Vec<_>>>()
    })?;
    let (first_handle, first_fd) = &handles[0];
    for (handle, fd) in &handles[1..] {
        assert!(Weak::ptr_eq(&first_handle.0, &handle.0));
        assert!(Weak::ptr_eq(first_fd, fd));
    }
    engine.close_file_segment(first_handle, Duration::ZERO)?;
    assert!(first_fd.upgrade().is_none());
    Ok(())
}

#[test]
fn lookup_is_lazy_retriable_and_limited_to_regular_files() -> Result<()> {
    let temp = TempFile::new(b"");
    let engine = TransferEngine::new(EngineConfig::default())?;
    for name in ["", "file://", "tcp://file", "file://x\0y"] {
        assert!(
            engine
                .open_file_segment(name, Permission::RemoteRead)
                .is_err()
        );
    }
    assert!(matches!(
        engine.open_file_segment(&temp.uri(), Permission::LocalOnly),
        Err(Error::PermissionDenied)
    ));
    fs::remove_file(&temp.path)?;
    let segment = engine.open_file_segment(&temp.uri(), Permission::RemoteRead)?;
    assert!(
        matches!(engine.file_segment_info(&segment), Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound)
    );
    fs::create_dir(&temp.path)?;
    assert!(matches!(
        engine.file_segment_info(&segment),
        Err(Error::InvalidConfig(_))
    ));
    fs::remove_dir(&temp.path)?;
    fs::write(&temp.path, b"")?;
    assert_eq!(engine.file_segment_info(&segment)?.length, 0);
    assert!(matches!(
        engine.resolve_file_offset(&segment, TransferOp::Read, 0, 1),
        Err(Error::InvalidRange)
    ));
    engine.close_file_segment(&segment, Duration::ZERO)
}

#[test]
fn file_offsets_check_current_extent_overflow_and_access() -> Result<()> {
    let temp = TempFile::new(b"01234567");
    let engine = TransferEngine::new(EngineConfig::default())?;
    let segment = engine.open_file_segment(&temp.uri(), Permission::RemoteRead)?;
    for (offset, length) in [(0, 8), (3, 5), (7, 1)] {
        assert_eq!(
            engine.resolve_file_offset(&segment, TransferOp::Read, offset, length)?,
            offset
        );
    }
    for (offset, length) in [(0, 0), (7, 2), (8, 1), (u64::MAX, 1), (2, usize::MAX)] {
        assert!(matches!(
            engine.resolve_file_offset(&segment, TransferOp::Read, offset, length),
            Err(Error::InvalidRange)
        ));
    }
    assert!(matches!(
        engine.resolve_file_offset(&segment, TransferOp::Write, 0, 1),
        Err(Error::PermissionDenied)
    ));
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        let (lease, offset) = engine.files.admit(&segment, TransferOp::Read, 3, 3)?;
        let mut bytes = [0; 3];
        lease.file().read_exact_at(&mut bytes, offset)?;
        assert_eq!(bytes, *b"345");
        assert!(lease.file().write_at(b"x", offset).is_err());
        assert_eq!(fs::read(&temp.path)?, b"01234567");
    }
    fs::write(&temp.path, b"12")?;
    assert_eq!(engine.file_segment_info(&segment)?.length, 8);
    assert!(matches!(
        engine.resolve_file_offset(&segment, TransferOp::Read, 2, 1),
        Err(Error::InvalidRange)
    ));
    fs::write(&temp.path, b"1234")?;
    assert_eq!(
        engine.resolve_file_offset(&segment, TransferOp::Read, 3, 1)?,
        3
    );
    engine.close_file_segment(&segment, Duration::ZERO)?;
    let segment = engine.open_file_segment(&temp.uri(), Permission::RemoteWrite)?;
    assert!(matches!(
        engine.resolve_file_offset(&segment, TransferOp::Read, 0, 1),
        Err(Error::PermissionDenied)
    ));
    assert_eq!(
        engine.resolve_file_offset(&segment, TransferOp::Write, 1, 2)?,
        1
    );
    assert_eq!(fs::read(&temp.path)?, b"1234");
    Ok(())
}

#[cfg(unix)]
#[test]
fn close_drains_admitted_io_on_the_original_fd() -> Result<()> {
    use std::os::unix::fs::FileExt;
    use std::sync::mpsc;

    let temp = TempFile::new(b"ABCDEFGH");
    let engine = TransferEngine::new(EngineConfig {
        max_file_segments: 1,
        ..Default::default()
    })?;
    let segment = engine.open_file_segment(&temp.uri(), Permission::RemoteReadWrite)?;
    let (lease, offset) = engine.files.admit(&segment, TransferOp::Write, 2, 3)?;
    let fd = Arc::downgrade(&lease.file);
    let original = temp.dir.join("original");
    fs::rename(&temp.path, &original)?;
    fs::write(&temp.path, b"new file")?;
    let (release, wait) = mpsc::channel();
    thread::scope(|scope| -> Result<()> {
        let worker = scope.spawn(move || -> Result<()> {
            wait.recv_timeout(crate::test_support::EVENTUAL_PROGRESS)
                .unwrap();
            lease.file().write_all_at(b"xyz", offset)?;
            Ok(())
        });
        assert!(matches!(
            engine.close_file_segment(&segment, Duration::ZERO),
            Err(Error::Timeout)
        ));
        assert!(matches!(
            engine.open_file_segment(&temp.uri(), Permission::RemoteReadWrite),
            Err(Error::FileSegmentClosed)
        ));
        assert!(matches!(
            engine.resolve_file_offset(&segment, TransferOp::Read, 0, 1),
            Err(Error::FileSegmentClosed)
        ));
        assert!(matches!(
            engine.open_file_segment("file://another", Permission::RemoteRead),
            Err(Error::ResourceExhausted)
        ));
        assert!(fd.upgrade().is_some());
        let closer = scope.spawn(|| engine.close_file_segment(&segment, Duration::from_secs(5)));
        release.send(()).unwrap();
        worker.join().unwrap()?;
        closer.join().unwrap()
    })?;
    assert!(fd.upgrade().is_none());
    assert_eq!(fs::read(&original)?, b"ABxyzFGH");
    assert_eq!(fs::read(&temp.path)?, b"new file");
    let reopened = engine.open_file_segment(&temp.uri(), Permission::RemoteRead)?;
    engine.close_file_segment(&reopened, Duration::ZERO)
}

#[test]
fn requester_close_drains_files_and_fences_new_opens() -> Result<()> {
    let temp = TempFile::new(b"x");
    let engine = TransferEngine::new(EngineConfig::default())?;
    let segment = engine.open_file_segment(&temp.uri(), Permission::RemoteRead)?;
    let (lease, _) = engine.files.admit(&segment, TransferOp::Read, 0, 1)?;
    let fd = Arc::downgrade(&lease.file);
    assert!(matches!(
        engine.close_requester(Duration::ZERO),
        Err(Error::Timeout)
    ));
    assert!(matches!(
        engine.open_file_segment(&temp.uri(), Permission::RemoteRead),
        Err(Error::RequesterClosed)
    ));
    assert!(fd.upgrade().is_some());
    drop(lease);
    engine.close_requester(Duration::ZERO)?;
    assert!(fd.upgrade().is_none());
    Ok(())
}
