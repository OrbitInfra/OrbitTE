//! Local file descriptors, corresponding to TENT's SegmentManager file entries.
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, Weak};
use std::time::{Duration, Instant};

use crate::engine::remaining;
use crate::{Error, Permission, Result, SegmentType, TransferOp};

const DESCRIPTOR_TTL: Duration = Duration::from_secs(3600);

/// An engine-local handle for a `file://` name. All aliases close together.
#[derive(Clone, Debug)]
pub struct FileSegment(Weak<Entry>);

/// Snapshot of TENT's single regular-file buffer. Its base offset is zero.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileSegmentInfo {
    pub segment_type: SegmentType,
    pub path: Arc<str>,
    pub offset: u64,
    pub length: u64,
    pub permission: Permission,
    /// Whether a direct fd is usable. None until the first file transfer.
    #[cfg(any(feature = "io-uring", feature = "gds"))]
    pub direct_io: Option<bool>,
    /// Effective cuFile policy at handle registration.
    #[cfg(feature = "gds")]
    pub gds_mode: Option<crate::GdsMode>,
}

#[derive(Default)]
pub(crate) struct FileSegments {
    entries: RwLock<HashMap<Arc<str>, Arc<Entry>>>,
}

struct Entry {
    path: Arc<str>,
    permission: Permission,
    state: Mutex<State>,
    drained: Condvar,
}

#[derive(Default)]
struct State {
    closing: bool,
    active: usize,
    file: Option<Arc<File>>,
    size: Option<(Instant, u64)>,
    #[cfg(any(feature = "io-uring", feature = "gds"))]
    direct: Option<direct::DirectFile>,
    #[cfg(feature = "gds")]
    gds: Option<crate::transport::gds::FileHandle>,
    #[cfg(feature = "gds")]
    gds_open_uncertain: Option<i32>,
}

impl Entry {
    fn lock(&self) -> Result<MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| Error::Internal("file segment lock poisoned"))
    }

    fn pin(self: &Arc<Self>) -> Result<FileLease> {
        let mut state = self.lock()?;
        if state.closing {
            return Err(Error::FileSegmentClosed);
        }
        let file = match &state.file {
            Some(file) => Arc::clone(file),
            None => {
                if !std::fs::metadata(self.path.as_ref())?.is_file() {
                    return Err(Error::InvalidConfig("file segment requires a regular file"));
                }
                let mut options = OpenOptions::new();
                options
                    .read(self.permission.allows(false))
                    .write(self.permission.allows(true));
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.custom_flags(libc::O_NONBLOCK);
                }
                let file = Arc::new(options.open(self.path.as_ref())?);
                let metadata = file.metadata()?;
                if !metadata.is_file() {
                    return Err(Error::InvalidConfig("file segment requires a regular file"));
                }
                state.size = Some((Instant::now(), metadata.len()));
                state.file = Some(Arc::clone(&file));
                file
            }
        };
        state.active = state
            .active
            .checked_add(1)
            .ok_or(Error::ResourceExhausted)?;
        Ok(FileLease {
            file,
            permit: FilePermit(Arc::clone(self)),
        })
    }
}

/// An executor holds this until its I/O is physically complete. Field order drops
/// the fd reference before the permit can wake a closer.
pub(crate) struct FileLease {
    file: Arc<File>,
    permit: FilePermit,
}

#[cfg(any(feature = "io-uring", feature = "gds"))]
pub(crate) struct FileRequest {
    pub(crate) local: crate::RegisteredMemory,
    pub(crate) selection: crate::runtime::transport_selector::SelectionResult,
    #[cfg(feature = "gds")]
    pub(crate) buffer: Option<crate::transport::gds::BufferLease>,
    pub(crate) local_offset: usize,
    pub(crate) lease: FileLease,
    pub(crate) offset: u64,
    pub(crate) length: usize,
}

struct FilePermit(Arc<Entry>);

impl Drop for FilePermit {
    fn drop(&mut self) {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.active -= 1;
        if state.active == 0 && state.closing {
            self.0.drained.notify_all();
        }
    }
}

impl FileLease {
    pub(crate) fn file(&self) -> &File {
        &self.file
    }

    fn length(&self, refresh: bool) -> Result<u64> {
        let mut state = self.permit.0.lock()?;
        match state.size {
            Some((at, length)) if !refresh && at.elapsed() < DESCRIPTOR_TTL => Ok(length),
            _ => {
                let length = self.file().metadata()?.len();
                state.size = Some((Instant::now(), length));
                Ok(length)
            }
        }
    }
}

impl FileSegments {
    pub(crate) fn open(
        &self,
        name: &str,
        permission: Permission,
        limit: usize,
    ) -> Result<FileSegment> {
        let path = name
            .strip_prefix("file://")
            .filter(|path| !path.is_empty() && path.len() <= 4096 && !path.contains('\0'))
            .ok_or(Error::InvalidConfig(
                "expected file:// followed by a path of 1..=4096 bytes",
            ))?;
        if permission == Permission::LocalOnly {
            return Err(Error::PermissionDenied);
        }
        let entry = {
            let mut entries = self
                .entries
                .write()
                .map_err(|_| Error::Internal("file table lock poisoned"))?;
            if let Some(entry) = entries.get(path) {
                Arc::clone(entry)
            } else {
                if entries.len() >= limit {
                    return Err(Error::ResourceExhausted);
                }
                let path: Arc<str> = path.into();
                let entry = Arc::new(Entry {
                    path: Arc::clone(&path),
                    permission,
                    state: Mutex::default(),
                    drained: Condvar::new(),
                });
                entries.insert(path, Arc::clone(&entry));
                entry
            }
        };
        if entry.lock()?.closing {
            return Err(Error::FileSegmentClosed);
        }
        if permission != entry.permission {
            return Err(Error::PermissionDenied);
        }
        Ok(FileSegment(Arc::downgrade(&entry)))
    }

    fn entry(&self, segment: &FileSegment) -> Result<Arc<Entry>> {
        let entry = segment.0.upgrade().ok_or(Error::FileSegmentClosed)?;
        let entries = self
            .entries
            .read()
            .map_err(|_| Error::Internal("file table lock poisoned"))?;
        if entries
            .get(&entry.path)
            .is_some_and(|current| Arc::ptr_eq(current, &entry))
        {
            Ok(entry)
        } else {
            Err(Error::FileSegmentClosed)
        }
    }

    pub(crate) fn info(&self, segment: &FileSegment, refresh: bool) -> Result<FileSegmentInfo> {
        let entry = self.entry(segment)?;
        let length = entry.pin()?.length(refresh)?;
        #[cfg(any(feature = "io-uring", feature = "gds"))]
        let state = entry.lock()?;
        Ok(FileSegmentInfo {
            segment_type: SegmentType::File,
            path: Arc::clone(&entry.path),
            offset: 0,
            length,
            permission: entry.permission,
            #[cfg(any(feature = "io-uring", feature = "gds"))]
            direct_io: state.direct.as_ref().map(|direct| direct.file.is_some()),
            #[cfg(feature = "gds")]
            gds_mode: state.gds.as_ref().map(|h| h.mode),
        })
    }

    pub(crate) fn admit(
        &self,
        segment: &FileSegment,
        operation: TransferOp,
        offset: u64,
        length: usize,
    ) -> Result<(FileLease, u64)> {
        let entry = self.entry(segment)?;
        if !entry.permission.allows(operation == TransferOp::Write) {
            return Err(Error::PermissionDenied);
        }
        let end = offset
            .checked_add(u64::try_from(length).map_err(|_| Error::InvalidRange)?)
            .ok_or(Error::InvalidRange)?;
        if length == 0 {
            return Err(Error::InvalidRange);
        }
        let lease = entry.pin()?;
        if end > lease.length(true)? {
            return Err(Error::InvalidRange);
        }
        Ok((lease, offset))
    }

    pub(crate) fn close(
        &self,
        segment: &FileSegment,
        deadline: Instant,
        #[cfg(feature = "gds")] gds: Option<&crate::transport::gds::GdsTransport>,
    ) -> Result<()> {
        self.close_entry(
            &self.entry(segment)?,
            deadline,
            #[cfg(feature = "gds")]
            gds,
        )
    }

    fn close_entry(
        &self,
        entry: &Arc<Entry>,
        deadline: Instant,
        #[cfg(feature = "gds")] gds: Option<&crate::transport::gds::GdsTransport>,
    ) -> Result<()> {
        let mut state = entry.lock()?;
        state.closing = true;
        while state.active != 0 {
            #[cfg(feature = "gds")]
            if let Some(gds) = gds {
                drop(state);
                gds.cleanup();
                state = entry.lock()?;
                if state.active == 0 {
                    break;
                }
            }
            let wait = remaining(deadline)?;
            #[cfg(feature = "gds")]
            let wait = if gds.is_some() {
                wait.min(Duration::from_millis(10))
            } else {
                wait
            };
            state = entry
                .drained
                .wait_timeout(state, wait)
                .map_err(|_| Error::Internal("file segment lock poisoned"))?
                .0;
        }
        #[cfg(feature = "gds")]
        if let Some(code) = state.gds_open_uncertain {
            return Err(Error::Unquiesced {
                operation: "cuFileHandleRegister",
                code,
            });
        }
        #[cfg(feature = "gds")]
        if let Some(handle) = &mut state.gds {
            handle.close()?;
            state.gds.take();
        }
        state.file.take();
        #[cfg(any(feature = "io-uring", feature = "gds"))]
        state.direct.take();
        drop(state);
        let mut entries = self
            .entries
            .write()
            .map_err(|_| Error::Internal("file table lock poisoned"))?;
        // A second closer may outlive removal and a fresh open of the same name.
        if entries
            .get(&entry.path)
            .is_some_and(|current| Arc::ptr_eq(current, entry))
        {
            entries.remove(&entry.path);
        }
        Ok(())
    }

    pub(crate) fn close_all(&self, deadline: Instant) -> Result<()> {
        // Requester admission has closed and all public file lookups have returned.
        let entries: Vec<_> = self
            .entries
            .read()
            .map_err(|_| Error::Internal("file table lock poisoned"))?
            .values()
            .cloned()
            .collect();
        for entry in entries {
            self.close_entry(
                &entry,
                deadline,
                #[cfg(feature = "gds")]
                None,
            )?;
        }
        Ok(())
    }
}

#[cfg(any(feature = "io-uring", feature = "gds"))]
mod direct;

#[cfg(test)]
#[path = "../../tests/unit/segment/file.rs"]
mod tests;
