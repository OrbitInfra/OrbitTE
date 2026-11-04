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
    pub(super) ptr: NonNull<u8>,
    layout: Layout,
}

#[allow(unsafe_code)]
// SAFETY: Bounce uniquely owns stable aligned bytes. Moving it never moves those
// bytes. Pool leases or the task’s CQE protect access for the entire native use.
unsafe impl Send for Bounce {}
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

enum Scratch {
    Slot(super::pool::Slot),
    Pageable {
        buffer: Bounce,
        _charge: super::pool::Charge,
    },
}
impl Scratch {
    fn address(&self) -> NonNull<u8> {
        match self {
            Self::Slot(slot) => slot.ptr,
            Self::Pageable { buffer, .. } => buffer.ptr,
        }
    }
}

enum HostMemory {
    One(DmaLease),
    Vector {
        _loans: Vec<DmaLease>,
        iovecs: Box<[libc::iovec]>,
    },
}

pub(super) struct Task {
    file: Arc<File>,
    memory: Option<HostMemory>,
    length: usize,
    bounce: Option<Scratch>,
    offset: u64,
    pub(super) result: Option<i32>,
    pub(super) late: bool,
}

impl Task {
    #[allow(unsafe_code)]
    pub(super) fn new(
        operation: TransferOp,
        request: &FileRequest,
        transport: &super::IoUringTransport,
    ) -> Result<(Self, libc::iovec)> {
        if request.parts.vectored() {
            let (loans, spans) = request.parts.borrow_ranges(|m, o, n| m.dma_lease(o, n))?;
            let iovecs: Box<[_]> = spans
                .iter()
                .zip(request.parts.iter())
                .map(|(span, part)| libc::iovec {
                    iov_base: loans[span.loan]
                        .address()
                        .as_ptr()
                        .wrapping_add(span.offset)
                        .cast(),
                    iov_len: part.length,
                })
                .collect();
            let file = request.lease.file_for_iovecs(request.offset, &iovecs)?;
            // Round planning needs one length per SQE. entry() supplies the actual iovec array.
            let span = libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: request.length,
            };
            return Ok((
                Self {
                    file,
                    memory: Some(HostMemory::Vector {
                        _loans: loans,
                        iovecs,
                    }),
                    length: request.length,
                    bounce: None,
                    offset: request.offset,
                    result: None,
                    late: false,
                },
                span,
            ));
        }
        let part = &request.parts[0];
        let memory = part.local.dma_lease(part.local_offset, part.length)?;
        // CUDA-owned host memory is directly accessible after dma_lease consumes
        // its producer event. Use buffered I/O for an unaligned view, without a copy.
        #[cfg(feature = "cuda")]
        let direct_host = if part.local.is_cuda_pinned_host() {
            Some((
                request.lease.file_for_buffer(
                    request.offset,
                    request.length,
                    memory.address().as_ptr() as usize,
                )?,
                1,
            ))
        } else {
            None
        };
        #[cfg(not(feature = "cuda"))]
        let direct_host = None;
        let host_pinned = direct_host.is_some();
        let (file, alignment) = match direct_host {
            Some(descriptor) => descriptor,
            None => request.lease.io_file(request.offset, request.length)?,
        };
        let staging_alignment = if !host_pinned && transport.pool.enabled {
            alignment.max(4096)
        } else {
            alignment
        };
        let bounce = if memory.address().as_ptr() as usize % staging_alignment != 0 {
            let bounce = match transport.pool.acquire(
                request.length,
                staging_alignment,
                super::pool::Access::Host,
                super::pool::Slab::host,
            )? {
                Some(slot) => Scratch::Slot(slot),
                None => {
                    let charge = transport.pool.charge(request.length)?;
                    Scratch::Pageable {
                        buffer: (transport.allocate)(request.length, staging_alignment)?,
                        _charge: charge,
                    }
                }
            };
            if operation == TransferOp::Write {
                // SAFETY: the exclusive loan covers all initialized WRITE bytes.
                // The fresh bounce is disjoint and has exactly that capacity.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        memory.address().as_ptr(),
                        bounce.address().as_ptr(),
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
                .map_or(memory.address(), Scratch::address)
                .as_ptr()
                .cast(),
            iov_len: memory.len(),
        };
        Ok((
            Self {
                file,
                memory: Some(HostMemory::One(memory)),
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
        cqe_error(operation, self.result?, self.length, self.late)
    }

    pub(super) fn entry(
        &self,
        operation: TransferOp,
        iov: &libc::iovec,
        index: usize,
    ) -> squeue::Entry {
        let iovecs = match &self.memory {
            Some(HostMemory::Vector { iovecs, .. }) => &**iovecs,
            _ => std::slice::from_ref(iov),
        };
        entry(&self.file, self.offset, operation, iovecs, index)
    }
    #[allow(unsafe_code)]
    pub(super) fn complete(&mut self, operation: TransferOp, result: i32, late: bool) -> bool {
        let Some(memory) = &self.memory else {
            return false;
        };
        if result > 0 && result as usize > self.length {
            return false;
        }
        self.result = Some(result);
        self.late = late;
        if operation == TransferOp::Read && result > 0 {
            if let (Some(bounce), HostMemory::One(memory)) = (&self.bounce, memory) {
                // SAFETY: this CQE proves the kernel wrote this prefix and stopped using
                // the iovec. The bound above and the exclusive loan protect both ranges.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        bounce.address().as_ptr(),
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

pub(super) fn entry(
    file: &File,
    offset: u64,
    operation: TransferOp,
    iov: &[libc::iovec],
    index: usize,
) -> squeue::Entry {
    let fd = types::Fd(file.as_raw_fd());
    let entry = match operation {
        TransferOp::Read => opcode::Readv::new(fd, iov.as_ptr(), iov.len() as u32)
            .offset(offset)
            .build(),
        TransferOp::Write => opcode::Writev::new(fd, iov.as_ptr(), iov.len() as u32)
            .offset(offset)
            .build(),
    };
    entry.user_data(index as u64)
}

pub(super) fn cqe_error(
    operation: TransferOp,
    result: i32,
    length: usize,
    late: bool,
) -> Option<Error> {
    match result {
        code if code < 0 => Some(Error::Backend {
            operation: "io_uring CQE",
            code,
        }),
        bytes if bytes as usize != length => Some(Error::Io(std::io::Error::new(
            if operation == TransferOp::Read {
                std::io::ErrorKind::UnexpectedEof
            } else {
                std::io::ErrorKind::WriteZero
            },
            "short io_uring completion",
        ))),
        _ if late => Some(Error::Timeout),
        _ => None,
    }
}
