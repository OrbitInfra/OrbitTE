use super::*;
use crate::segment::file::{FileLease, FilePart, FileParts, FileRequest};

impl TransferEngine {
    pub(super) fn file_available(
        &self,
        transport: Transport,
        _layout: Option<(&FileParts, &FileLease, u64)>,
    ) -> Result<bool> {
        match transport {
            #[cfg(feature = "io-uring")]
            Transport::IoUring => Ok(self.io_uring.as_ref().is_some_and(|io| io.available())),
            #[cfg(feature = "gds")]
            Transport::Gds => {
                let Some(provider) = &self.gds else {
                    return Ok(false);
                };
                if let Some((parts, lease, offset)) = _layout {
                    if parts.vectored() && !lease.gds_vector_aligned(offset, parts)? {
                        return Ok(false);
                    }
                }
                let Some(gds) = provider.resolve(&self.config) else {
                    return Ok(false);
                };
                if gds.mode().is_err() {
                    return Ok(false);
                }
                if let Some((parts, _, _)) = _layout {
                    for part in parts.iter() {
                        gds.register(&part.local)?;
                        if !crate::transport::gds::BufferSlot::supports(&part.local) {
                            return Ok(false);
                        }
                    }
                }
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    pub(super) fn prepare_file(
        &self,
        segment: &FileSegment,
        operation: TransferOp,
        request: &TransferRequest,
        bound: Option<u8>,
        priority: Priority,
    ) -> Result<FileRequest> {
        if request.remote_staging {
            return Err(Error::Unavailable(
                "file I/O does not support remote staging",
            ));
        }
        let resolve = |&(address, length): &(u64, usize)| {
            let (local, local_offset) = self.source.registry.resolve_address(address, length)?;
            Ok(FilePart {
                local,
                local_offset,
                length,
                #[cfg(feature = "gds")]
                buffer: None,
            })
        };
        let parts = match &request.file_ranges {
            Some(v) => FileParts::Vector(v.iter().map(resolve).collect::<Result<_>>()?),
            None => FileParts::One(resolve(&(request.local_address, request.length))?),
        };
        let (lease, offset) =
            self.files
                .admit(segment, operation, request.remote_address, request.length)?;
        let local = if parts
            .iter()
            .any(|p| p.local.memory_type() == crate::MemoryType::Cuda)
        {
            crate::MemoryType::Cuda
        } else {
            crate::MemoryType::Cpu
        };
        let error = std::cell::Cell::new(None);
        let selection = TransportSelector::select(
            self.selector.as_ref(),
            Filters {
                segment: crate::SegmentType::File,
                local,
                remote: crate::MemoryType::Cpu,
                size: request.length,
                priority,
                same_machine: true,
            },
            request,
            bound,
            |t| {
                self.file_available(t, Some((&parts, &lease, offset)))
                    .unwrap_or_else(|e| {
                        error.set(Some(e));
                        false
                    })
            },
        );
        if let Some(error) = error.into_inner() {
            return Err(error);
        }
        Ok(FileRequest {
            parts,
            lease,
            offset,
            length: request.length,
            selection: selection?,
        })
    }
}
