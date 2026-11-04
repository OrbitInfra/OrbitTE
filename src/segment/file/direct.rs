use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;

use super::*;

pub(super) struct DirectFile {
    pub(super) file: Option<Arc<File>>,
    memory_alignment: usize,
    offset_alignment: u64,
}

impl DirectFile {
    #[allow(unsafe_code)]
    fn open(buffered: &File, permission: Permission) -> Self {
        let mut mode = Self {
            file: None,
            memory_alignment: 4096,
            offset_alignment: 4096,
        };
        // Reopen the retained inode, even after the original name was unlinked or replaced.
        let Ok(file) = OpenOptions::new()
            .read(permission.allows(false))
            .write(permission.allows(true))
            .custom_flags(libc::O_DIRECT)
            .open(format!("/proc/self/fd/{}", buffered.as_raw_fd()))
        else {
            return mode;
        };
        let mut fs = std::mem::MaybeUninit::<libc::statfs>::uninit();
        // SAFETY: file is live and fstatfs initializes fs on success.
        if unsafe { libc::fstatfs(file.as_raw_fd(), fs.as_mut_ptr()) } != 0 {
            return mode;
        }
        // SAFETY: fstatfs succeeded above. tmpfs can accept O_DIRECT without a direct path.
        if unsafe { fs.assume_init() }.f_type == libc::TMPFS_MAGIC {
            return mode;
        }
        let mut stat = std::mem::MaybeUninit::<libc::statx>::uninit();
        // SAFETY: AT_EMPTY_PATH queries this live fd, with writable space for one statx.
        if unsafe {
            libc::statx(
                file.as_raw_fd(),
                c"".as_ptr(),
                libc::AT_EMPTY_PATH,
                libc::STATX_DIOALIGN,
                stat.as_mut_ptr(),
            )
        } == 0
        {
            // SAFETY: statx succeeded and initialized the output.
            let stat = unsafe { stat.assume_init() };
            if stat.stx_mask & libc::STATX_DIOALIGN != 0 {
                if !stat.stx_dio_mem_align.is_power_of_two() || stat.stx_dio_offset_align == 0 {
                    return mode;
                }
                mode.memory_alignment = stat.stx_dio_mem_align as usize;
                mode.offset_alignment = u64::from(stat.stx_dio_offset_align);
            }
        }
        mode.file = Some(Arc::new(file));
        mode
    }
}

impl FileLease {
    /// Linux validates DIO alignment for each vector element independently.
    #[cfg(feature = "io-uring")]
    pub(crate) fn file_for_iovecs(&self, offset: u64, iovecs: &[libc::iovec]) -> Result<Arc<File>> {
        let entry = &self.permit.0;
        let mut state = entry.lock()?;
        let direct = state
            .direct
            .get_or_insert_with(|| DirectFile::open(self.file(), entry.permission));
        if let Some(file) = &direct.file {
            if offset % direct.offset_alignment == 0
                && iovecs.iter().all(|iov| {
                    iov.iov_base as usize % direct.memory_alignment == 0
                        && iov.iov_len as u64 % direct.offset_alignment == 0
                })
            {
                return Ok(Arc::clone(file));
            }
        }
        Ok(Arc::clone(&self.file))
    }

    #[cfg(all(feature = "cuda", feature = "io-uring"))]
    pub(crate) fn file_for_buffer(
        &self,
        offset: u64,
        length: usize,
        address: usize,
    ) -> Result<Arc<File>> {
        let (file, alignment) = self.io_file(offset, length)?;
        if address % alignment == 0 {
            Ok(file)
        } else {
            Ok(Arc::clone(&self.file))
        }
    }

    /// Select before building SQEs. A direct-capable file can still require a
    /// buffered request for its offset or length.
    #[cfg(feature = "io-uring")]
    pub(crate) fn io_file(&self, offset: u64, length: usize) -> Result<(Arc<File>, usize)> {
        let entry = &self.permit.0;
        let mut state = entry.lock()?;
        let direct = state
            .direct
            .get_or_insert_with(|| DirectFile::open(self.file(), entry.permission));
        if let Some(file) = &direct.file {
            if offset % direct.offset_alignment == 0 && length as u64 % direct.offset_alignment == 0
            {
                return Ok((Arc::clone(file), direct.memory_alignment.max(4096)));
            }
        }
        Ok((Arc::clone(&self.file), 4096))
    }
}

#[cfg(feature = "gds")]
impl FileLease {
    pub(crate) fn gds_vector_aligned(&self, offset: u64, parts: &FileParts) -> Result<bool> {
        // cuFile needs real buffers even for page-aligned extents. A padding
        // element selects staging before opening a direct fd or probing cuFile.
        let aligned = |memory_alignment, offset_alignment| {
            parts.iter().all(|part| {
                part.local.as_ref().is_some_and(|m| {
                    (m.base_address() + part.local_offset as u64) % memory_alignment == 0
                        && part.length as u64 % offset_alignment == 0
                })
            })
        };
        if offset % 4096 != 0 || !aligned(4096, 4096) {
            return Ok(false);
        }
        let entry = &self.permit.0;
        let mut state = entry.lock()?;
        let direct = state
            .direct
            .get_or_insert_with(|| DirectFile::open(self.file(), entry.permission));
        Ok(direct.file.is_some()
            && offset % direct.offset_alignment == 0
            && aligned(direct.memory_alignment as u64, direct.offset_alignment))
    }

    pub(crate) fn gds_handle(
        &self,
        driver: &Arc<crate::transport::gds::Driver>,
        mode: crate::GdsMode,
    ) -> Result<usize> {
        let entry = &self.permit.0;
        let mut state = entry.lock()?;
        if let Some(code) = state.gds_open_uncertain {
            return Err(Error::Unquiesced {
                operation: "cuFileHandleRegister",
                code,
            });
        }
        if let Some(handle) = &state.gds {
            if handle.pid == driver.pid() {
                return Ok(handle.raw);
            }
        }
        let direct = state
            .direct
            .get_or_insert_with(|| DirectFile::open(self.file(), entry.permission));
        let fd = direct
            .file
            .as_ref()
            .ok_or(Error::Unavailable(
                "GDS requires an O_DIRECT file descriptor",
            ))?
            .clone();
        let handle = crate::transport::gds::FileHandle::new(driver, fd, mode).inspect_err(|e| {
            if let Error::Unquiesced { code, .. } = e {
                state.gds_open_uncertain = Some(*code);
            }
        })?;
        let raw = handle.raw;
        state.gds = Some(handle);
        Ok(raw)
    }
}
