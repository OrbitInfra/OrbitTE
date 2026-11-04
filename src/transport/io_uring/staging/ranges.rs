use super::*;
use crate::memory::DmaLease;

enum Loan {
    Gpu(StagingCopy),
    Host(DmaLease),
}
struct Fragment {
    start: usize,
    end: usize,
    owner: usize,
    offset: usize,
}
struct Run {
    context: usize,
    lender: usize,
    end: usize,
}

/// Keep every fragment borrowed across all rounds. Each chunk uses one CUDA
/// context's stream and can include host bytes and any number of its GPU ranges.
pub(super) struct StagedRanges {
    fragments: Vec<Fragment>,
    loans: Vec<Loan>,
    runs: Vec<Run>,
}

impl StagedRanges {
    pub(super) fn new(request: &FileRequest, proxy: &Arc<ProxyManager>) -> Result<Self> {
        let mut fragments = Vec::with_capacity(request.parts.len());
        let mut runs: Vec<Run> = Vec::new();
        let mut start = 0;
        let (loans, spans) = request.parts.borrow_ranges(|memory, offset, length| {
            if memory.memory_type() == crate::MemoryType::Cuda {
                let (gpu, offset) = memory.cuda_range(offset, length)?;
                Ok(Loan::Gpu(gpu.staging_loan(offset, length, proxy)?))
            } else {
                Ok(Loan::Host(memory.dma_lease(offset, length)?))
            }
        })?;
        for (part, span) in request.parts.iter().zip(spans) {
            let owner = span.loan;
            let offset = span.offset;
            let end = start + part.length;
            if let Loan::Gpu(copy) = &loans[owner] {
                let context = copy.file_context();
                if runs.last().is_none_or(|r| r.context != context) {
                    runs.push(Run {
                        context,
                        lender: owner,
                        end,
                    });
                }
            }
            if let Some(run) = runs.last_mut() {
                run.end = end;
            }
            fragments.push(Fragment {
                start,
                end,
                owner,
                offset,
            });
            start = end;
        }
        Ok(Self {
            fragments,
            runs,
            loans,
        })
    }

    pub(super) fn pieces(&self, chunk: usize) -> usize {
        let mut start = 0;
        self.runs
            .iter()
            .map(|run| {
                let bytes = run.end - start;
                start = run.end;
                bytes.div_ceil(chunk)
            })
            .sum()
    }

    pub(super) fn lender(&self, offset: usize) -> Result<(&StagingCopy, usize)> {
        let index = self.runs.partition_point(|run| run.end <= offset);
        let run = self
            .runs
            .get(index)
            .ok_or(Error::Internal("missing file staging context"))?;
        match &self.loans[run.lender] {
            Loan::Gpu(copy) => Ok((copy, run.end - offset)),
            Loan::Host(_) => Err(Error::Internal("missing CUDA staging owner")),
        }
    }

    #[allow(unsafe_code)]
    pub(super) fn transfer(
        &mut self,
        stage: &mut DeviceStage,
        offset: usize,
        length: usize,
        to_device: bool,
        completed: &mut usize,
    ) -> Result<()> {
        let (address, capacity) = stage.address()?;
        if length > capacity {
            return Err(Error::InvalidRange);
        }
        let index = self.fragments.partition_point(|part| part.end <= offset);
        let mut consumed = 0;
        for part in &self.fragments[index..] {
            let local = offset + consumed - part.start;
            let len = (part.end - part.start - local).min(length - consumed);
            let local = local + part.offset;
            match &mut self.loans[part.owner] {
                Loan::Gpu(copy) => {
                    copy.check_access()?;
                    stage.copy(copy, consumed, local, len, to_device)?;
                }
                Loan::Host(loan) => {
                    // SAFETY: the private pinned chunk and exclusive host range are
                    // disjoint and both outlive the copy. The bounds above cover len bytes.
                    unsafe {
                        let host = loan.address().as_ptr().add(local);
                        let slot = address.as_ptr().add(consumed);
                        let (src, dst) = if to_device {
                            (slot, host)
                        } else {
                            (host, slot)
                        };
                        std::ptr::copy_nonoverlapping(src, dst, len);
                    }
                }
            }
            consumed += len;
            if to_device {
                *completed += len;
            }
            if consumed == length {
                return Ok(());
            }
        }
        Err(Error::InvalidRange)
    }

    pub(super) fn publish_failure(&self) {
        for loan in &self.loans {
            if let Loan::Gpu(copy) = loan {
                copy.publish_failure();
            }
        }
    }
}
