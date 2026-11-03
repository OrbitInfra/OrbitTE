use std::alloc::{Layout, alloc, dealloc};
use std::fs::File;
use std::os::fd::AsRawFd;
use std::ptr::NonNull;
use std::sync::Arc;

use io_uring::{opcode, squeue, types};

use super::FileRequest;
use crate::memory::DmaLease;
use crate::{Error, Result, TransferOp};

pub(super) struct Bounce {
    ptr: NonNull<u8>,
    layout: Layout,
}

impl Bounce {
    #[allow(unsafe_code)]
    pub(super) fn new(length: usize, alignment: usize) -> Result<Self> {
        let layout = Layout::from_size_align(length, alignment).map_err(|_| Error::InvalidRange)?;
        // SAFETY: file admission validated nonzero length, and Layout checked alignment.
        let ptr = NonNull::new(unsafe { alloc(layout) }).ok_or(Error::ResourceExhausted)?;
        Ok(Self { ptr, layout })
    }
}

impl Drop for Bounce {
    #[allow(unsafe_code)]
    fn drop(&mut self) {
        // SAFETY: same allocation and layout, dropped only before submission or after drain.
        unsafe {
            dealloc(self.ptr.as_ptr(), self.layout);
        }
    }
}

pub(super) struct Task {
    file: Arc<File>,
    memory: Option<DmaLease>,
    length: usize,
    bounce: Option<Bounce>,
    offset: u64,
    pub(super) result: Option<i32>,
    pub(super) late: bool,
}

impl Task {
    #[allow(unsafe_code)]
    pub(super) fn new(
        operation: TransferOp,
        request: &FileRequest,
        allocate: fn(usize, usize) -> Result<Bounce>,
    ) -> Result<(Self, libc::iovec)> {
        let (file, alignment) = request.lease.io_file(request.offset, request.length)?;
        let memory = request
            .local
            .dma_lease(request.local_offset, request.length)?;
        let bounce = if memory.address().as_ptr() as usize % alignment != 0 {
            let bounce = allocate(request.length, alignment)?;
            if operation == TransferOp::Write {
                // SAFETY: the exclusive loan covers all initialized WRITE bytes.
                // The fresh bounce is disjoint and has exactly that capacity.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        memory.address().as_ptr(),
                        bounce.ptr.as_ptr(),
                        request.length,
                    );
                }
            }
            Some(bounce)
        } else {
            None
        };
        let iov = libc::iovec {
            iov_base: bounce
                .as_ref()
                .map_or(memory.address(), |bounce| bounce.ptr)
                .as_ptr()
                .cast(),
            iov_len: memory.len(),
        };
        Ok((
            Self {
                file,
                memory: Some(memory),
                length: request.length,
                bounce,
                offset: request.offset,
                result: None,
                late: false,
            },
            iov,
        ))
    }

    pub(super) fn bytes(&self) -> usize {
        self.result.unwrap_or(0).max(0) as usize
    }

    pub(super) fn observed_error(&self, operation: TransferOp) -> Option<Error> {
        match self.result? {
            code if code < 0 => Some(Error::Backend {
                operation: "io_uring CQE",
                code,
            }),
            bytes if bytes as usize != self.length => Some(Error::Io(std::io::Error::new(
                if operation == TransferOp::Read {
                    std::io::ErrorKind::UnexpectedEof
                } else {
                    std::io::ErrorKind::WriteZero
                },
                "short io_uring completion",
            ))),
            _ if self.late => Some(Error::Timeout),
            _ => None,
        }
    }

    pub(super) fn entry(
        &self,
        operation: TransferOp,
        iov: &libc::iovec,
        index: usize,
    ) -> squeue::Entry {
        let fd = types::Fd(self.file.as_raw_fd());
        let entry = match operation {
            TransferOp::Read => opcode::Readv::new(fd, iov, 1).offset(self.offset).build(),
            TransferOp::Write => opcode::Writev::new(fd, iov, 1).offset(self.offset).build(),
        };
        entry.user_data(index as u64)
    }

    #[allow(unsafe_code)]
    pub(super) fn complete(&mut self, operation: TransferOp, result: i32, late: bool) -> bool {
        let Some(memory) = &self.memory else {
            return false;
        };
        if result > 0 && result as usize > memory.len() {
            return false;
        }
        self.result = Some(result);
        self.late = late;
        if operation == TransferOp::Read && result > 0 {
            if let Some(bounce) = &self.bounce {
                // SAFETY: this CQE proves the kernel wrote this prefix and stopped using
                // the iovec. The bound above and the exclusive loan protect both ranges.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        bounce.ptr.as_ptr(),
                        memory.address().as_ptr(),
                        result as usize,
                    );
                }
            }
        }
        // Publish only after releasing this CQE's exclusive range. Another task
        // in the same ring can still be using its own disjoint range.
        self.bounce.take();
        self.memory.take();
        true
    }
}
