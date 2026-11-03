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
    /// Select before building SQEs. A direct-capable file can still require a
    /// buffered request for its offset or length.
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
