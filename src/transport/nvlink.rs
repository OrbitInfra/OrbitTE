//! TENT NVLink transport boundary: same-host IPC relocation and CUDA completion.

use std::sync::{Arc, OnceLock};

mod budget;
mod subbatch;
use budget::MappingBudget;
pub use budget::NVLinkStats;
use subbatch::{NVLinkSubBatch, subbatch_len};

use crate::cuda::ipc::IpcExport;
use crate::engine::{PreparedSubmission, remaining};
use crate::memory::checked_range;
use crate::runtime::control_plane::{ControlChannel, Operation, Reply, Request};
use crate::{
    Error, RegisteredMemory, RemoteEffect, Result, SourceEpoch, TransferFailure, TransferOp,
};

const HELLO_SIZE: usize = 52;
const GRANT_SIZE: usize = 200;
const BINDING_SIZE: usize = 84;
const RECEIPT_SIZE: usize = BINDING_SIZE + 1;

#[derive(Clone, Copy)]
struct Identity {
    machine: SourceEpoch,
    process: SourceEpoch,
}

impl Identity {
    fn local() -> Result<Self> {
        static IDENTITY: OnceLock<(u32, Identity)> = OnceLock::new();
        if let Some((pid, identity)) = IDENTITY.get() {
            if *pid != std::process::id() {
                return Err(Error::Unavailable("CUDA IPC after fork"));
            }
            return Ok(*identity);
        }
        let machine = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?
            .trim()
            .replace('-', "")
            .parse()?;
        let candidate = Self {
            machine,
            process: SourceEpoch::generate()?,
        };
        let (_, identity) = IDENTITY.get_or_init(|| (std::process::id(), candidate));
        Ok(*identity)
    }

    fn encode(&self, bytes: &mut [u8]) {
        bytes[..16].copy_from_slice(&self.machine.0);
        bytes[16..32].copy_from_slice(&self.process.0);
    }

    fn decode(bytes: &[u8]) -> Self {
        let mut machine = [0; 16];
        let mut process = [0; 16];
        machine.copy_from_slice(&bytes[..16]);
        process.copy_from_slice(&bytes[16..32]);
        Self {
            machine: SourceEpoch(machine),
            process: SourceEpoch(process),
        }
    }

    fn validate_peer(&self, peer: Self) -> Result<()> {
        if self.machine != peer.machine {
            return Err(Error::Unavailable(
                "CUDA IPC requires the same physical host",
            ));
        }
        if self.process == peer.process {
            return Err(Error::Unavailable("CUDA IPC requires a different process"));
        }
        Ok(())
    }
}

/// Legacy IPC is cooperative: exported handles authorize a whole allocation
/// until every participating process has closed it. It is not remote revocation.
pub(crate) struct NVLinkTransport {
    identity: Identity,
    budget: Arc<MappingBudget>,
}

impl NVLinkTransport {
    pub(crate) fn new(max_bytes: usize) -> Result<Self> {
        Ok(Self {
            identity: Identity::local()?,
            budget: MappingBudget::new(max_bytes),
        })
    }

    pub(crate) fn stats(&self) -> Result<NVLinkStats> {
        self.budget.stats()
    }

    pub(crate) fn transfer(
        &self,
        submission: &PreparedSubmission,
        first: usize,
        requester: SourceEpoch,
        max_bytes: usize,
        mut begin: impl FnMut(usize) -> bool,
    ) -> std::result::Result<usize, TransferFailure> {
        let requests = &submission.requests[first..];
        let mut count = subbatch_len(requests, max_bytes);
        loop {
            let mut retry_single = false;
            let requests = &requests[..count];
            let operation = submission.operation;
            let peer = &submission.peer;
            let deadline = submission.deadline;
            let start = requests.iter().map(|r| r.remote_offset).min().unwrap_or(0);
            let end = requests
                .iter()
                .map(|r| r.remote_offset + r.length)
                .max()
                .unwrap_or(0);
            let mut effect = RemoteEffect::None;
            let mut copied = 0;
            let mut attempted = 0;
            let mut acknowledged = false;
            let result = (|| {
                let (memory, _) = requests[0]
                    .local
                    .cuda_range(requests[0].local_offset, requests[0].length)?;
                memory.validate_ipc_requester()?;
                let wire = Request {
                    operation: if operation == TransferOp::Read {
                        Operation::NvlinkRead
                    } else {
                        Operation::NvlinkWrite
                    },
                    epoch: peer.epoch,
                    region: requests[0].remote.id,
                    offset: start as u64,
                    length: (end - start) as u64,
                };
                let mut channel = ControlChannel::connect(peer.endpoint, deadline)?;
                channel.write_all(&wire.encode())?;
                let mut hello = [0; HELLO_SIZE];
                hello[..4].copy_from_slice(b"NVL1");
                self.identity.encode(&mut hello[4..36]);
                hello[36..].copy_from_slice(&requester.0);
                channel.write_all(&hello)?;
                let reply = match Reply::read(&mut channel, peer.epoch) {
                    Err(Error::ResourceExhausted) if count > 1 => {
                        // A typed rejection before any grant cannot have exposed a
                        // mapping or copied bytes. The peer may have a smaller cap.
                        retry_single = true;
                        return Err(Error::ResourceExhausted);
                    }
                    other => other?,
                };
                if reply.status != 0 || reply.length != GRANT_SIZE as u64 {
                    return Err(Error::Protocol("NVLink grant size/status"));
                }
                let mut grant = [0; GRANT_SIZE];
                channel.read_exact(&mut grant)?;
                let (binding, handle, remote_offset, extent) =
                    self.validate_grant(&grant, &wire, requester)?;
                let copy = (|| {
                    remaining(deadline)?;
                    let permit = self.budget.reserve(extent, false)?;
                    if operation == TransferOp::Write {
                        effect = RemoteEffect::Unknown;
                    }
                    let mut batch = NVLinkSubBatch::new(&memory, permit)?;
                    let copied_result = (|| {
                        batch.mapping.open(handle)?;
                        for (index, request) in requests.iter().enumerate() {
                            // The engine began the first task before acquiring the grant.
                            if index != 0 && !begin(index) {
                                return Err(Error::Cancelled);
                            }
                            remaining(deadline)?;
                            attempted = index + 1;
                            let (memory, offset) = request
                                .local
                                .cuda_range(request.local_offset, request.length)?;
                            batch.mapping.copy(
                                &memory,
                                remote_offset + (request.remote_offset - start),
                                offset,
                                request.length,
                                operation == TransferOp::Write,
                            )?;
                            copied += 1;
                        }
                        Ok(())
                    })();
                    // Closing the single import proves completion of every touched
                    // local owner. A failure retains the import and its entire charge.
                    batch.close()?;
                    copied_result
                })();
                if matches!(copy, Err(Error::Unquiesced { .. })) {
                    return copy;
                }
                let mut receipt = [0; RECEIPT_SIZE];
                receipt[..BINDING_SIZE].copy_from_slice(&binding);
                receipt[BINDING_SIZE] = u8::from(copy.is_err());
                channel.write_all(&receipt)?;
                let reply = Reply::read(&mut channel, peer.epoch)?;
                if reply.status != 0 || reply.length != 0 {
                    return Err(Error::Protocol("NVLink close acknowledgement"));
                }
                acknowledged = true;
                copy
            })();
            if retry_single {
                count = 1;
                continue;
            }
            return result.map(|()| count).map_err(|error| TransferFailure {
                // Without the shared close ACK, none of this group's attempted
                // requests is confirmed. Never call an already copied WRITE skipped.
                request_index: if acknowledged { copied } else { 0 },
                affected_requests: if acknowledged { 1 } else { attempted.max(1) },
                remote_effect: if acknowledged && matches!(error, Error::Cancelled) {
                    RemoteEffect::None
                } else {
                    effect
                },
                error,
            });
        }
    }

    fn validate_grant(
        &self,
        grant: &[u8; GRANT_SIZE],
        request: &Request,
        requester: SourceEpoch,
    ) -> Result<([u8; BINDING_SIZE], [u8; 64], usize, usize)> {
        if &grant[..4] != b"NVL1" {
            return Err(Error::Protocol("NVLink grant version"));
        }
        self.identity
            .validate_peer(Identity::decode(&grant[4..36]))?;
        let mut binding = [0; BINDING_SIZE];
        binding.copy_from_slice(&grant[36..120]);
        if binding[..52] != request.encode() || binding[52..68] != requester.0 {
            return Err(Error::Protocol("NVLink grant request/epoch binding"));
        }
        let extent = usize::try_from(crate::runtime::control_plane::u64_at(grant, 120))
            .map_err(|_| Error::InvalidRange)?;
        if extent > isize::MAX as usize {
            return Err(Error::InvalidRange);
        }
        let offset = usize::try_from(crate::runtime::control_plane::u64_at(grant, 128))
            .map_err(|_| Error::InvalidRange)?;
        checked_range(
            offset,
            usize::try_from(request.length).map_err(|_| Error::InvalidRange)?,
            extent,
        )?;
        let mut handle = [0; 64];
        handle.copy_from_slice(&grant[136..200]);
        Ok((binding, handle, offset, extent))
    }

    pub(crate) fn serve(
        &self,
        channel: &mut ControlChannel<'_>,
        epoch: SourceEpoch,
        request: &Request,
        memory: &RegisteredMemory,
        offset: usize,
        length: usize,
    ) -> Result<()> {
        let mut hello = [0; HELLO_SIZE];
        channel.read_exact(&mut hello)?;
        if &hello[..4] != b"NVL1" {
            return Err(Error::Protocol("NVLink hello version"));
        }
        self.identity
            .validate_peer(Identity::decode(&hello[4..36]))?;
        let (memory, allocation_offset) = memory.cuda_range(offset, length)?;
        let mut export = IpcExport::new(memory)?;
        let permit = self.budget.reserve(export.extent, true)?;
        let mut grant = [0; GRANT_SIZE];
        grant[..4].copy_from_slice(b"NVL1");
        self.identity.encode(&mut grant[4..36]);
        grant[36..88].copy_from_slice(&request.encode());
        grant[88..104].copy_from_slice(&hello[36..52]);
        grant[104..120].copy_from_slice(&SourceEpoch::generate()?.0);
        grant[120..128].copy_from_slice(&(export.extent as u64).to_be_bytes());
        grant[128..136].copy_from_slice(&(allocation_offset as u64).to_be_bytes());
        grant[136..].copy_from_slice(&export.handle);
        channel.reply(epoch, 0, GRANT_SIZE as u64)?;
        export.expose();
        let receipt = (|| {
            channel.write_all(&grant)?;
            let mut receipt = [0; RECEIPT_SIZE];
            channel.read_exact(&mut receipt)?;
            if receipt[..BINDING_SIZE] != grant[36..120] || receipt[BINDING_SIZE] > 1 {
                return Err(Error::Protocol("NVLink close receipt binding"));
            }
            Ok(())
        })();
        if receipt.is_err() {
            permit.retain();
            return Err(Error::Unquiesced {
                operation: "CUDA IPC source close receipt",
                code: -1,
            });
        }
        export.finish();
        drop(permit);
        channel.reply(epoch, 0, 0)
    }
}

#[cfg(test)]
#[path = "../../tests/unit/transport/nvlink.rs"]
pub(crate) mod tests;
