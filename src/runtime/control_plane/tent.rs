//! Explicit frozen-TENT P2P control and ordinary TCP payload profile.
//! Uses the existing control workers, admission and registered backing owners.

mod metadata;
pub(crate) mod wire;

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::{ControlChannel, buffer};
use crate::engine::{PreparedTransfer, Source, remaining};
use crate::{
    Error, Notification, NotificationEffect, NotificationFailure, RemoteEffect, RemoteSegment,
    Result, SourceEpoch, TransferOp, Transport,
};

const DESCRIBE: i32 = 1;
const BOOTSTRAP: i32 = 2;
const WRITE: i32 = 3;
const READ: i32 = 4;
const NOTIFY: i32 = 5;
const PROBE: i32 = 6;
const MAX_ERROR: usize = 512;
// JSON escaping can expand each configured notification byte sixfold.
const MAX_NOTIFY_WIRE: usize = 6 * (64 << 10) + 1024;

fn response(channel: &mut ControlChannel<'_>, limit: usize) -> Result<Vec<u8>> {
    let mut header = [0; wire::RESPONSE];
    channel.read_exact(&mut header)?;
    let size = wire::response_header(&header, limit)?;
    let mut body = [0; wire::RETURN.len()];
    channel.read_exact(&mut body)?;
    wire::return_body(&body)?;
    let mut payload = buffer(size)?;
    channel.read_exact(&mut payload)?;
    Ok(payload)
}

pub(crate) fn open_segment(
    endpoint: SocketAddr,
    epoch: SourceEpoch,
    deadline: Instant,
) -> Result<RemoteSegment> {
    let mut channel = ControlChannel::connect(endpoint, deadline)?;
    channel.write_all(&wire::request(DESCRIBE, 0)?)?;
    metadata::decode(&response(&mut channel, metadata::LIMIT)?, endpoint, epoch)
}

fn check_snapshot(peer: &RemoteSegment, current: &RemoteSegment) -> Result<()> {
    if peer.metadata.tent != current.metadata.tent {
        if let Some(identity) = &peer.identity {
            identity.fence();
        }
        return Err(Error::EpochMismatch);
    }
    Ok(())
}

fn recheck(peer: &RemoteSegment, channel: &mut ControlChannel<'_>) -> Result<RemoteSegment> {
    channel.write_all(&wire::request(DESCRIBE, 0)?)?;
    let bytes = response(channel, metadata::LIMIT)?;
    let current = match metadata::decode(&bytes, peer.endpoint, peer.epoch) {
        Ok(current) => current,
        Err(error) => {
            // A changed/incompatible descriptor is a fence too. A connection
            // failure above does not pretend to identify a new incarnation.
            if let Some(identity) = &peer.identity {
                identity.fence();
            }
            return Err(error);
        }
    };
    check_snapshot(peer, &current)?;
    Ok(current)
}

pub(crate) fn refresh(peer: &RemoteSegment, deadline: Instant) -> Result<RemoteSegment> {
    recheck(peer, &mut ControlChannel::connect(peer.endpoint, deadline)?)
}

pub(crate) fn transfer(
    operation: TransferOp,
    peer: &RemoteSegment,
    request: &PreparedTransfer,
    deadline: Instant,
    posted: &mut bool,
    #[cfg(feature = "cuda")] proxy: &Arc<crate::runtime::proxy_manager::ProxyManager>,
) -> std::result::Result<(), (Error, RemoteEffect)> {
    let mut effect = RemoteEffect::None;
    let result = (|| {
        let mut descriptor = [0; 16];
        let address = request
            .remote
            .address
            .checked_add(request.remote_offset as u64)
            .ok_or(Error::InvalidRange)?;
        descriptor[..8].copy_from_slice(&address.to_le_bytes());
        descriptor[8..].copy_from_slice(&(request.length as u64).to_le_bytes());
        // Snapshot WRITE bytes before sending any frame. READ commits only a full
        // validated response; the network can never access the local destination.
        let payload = if operation == TransferOp::Write {
            let mut bytes = buffer(request.length)?;
            #[cfg(feature = "cuda")]
            proxy.read(&request.local, request.local_offset, &mut bytes, deadline)?;
            #[cfg(not(feature = "cuda"))]
            request.local.read(request.local_offset, &mut bytes)?;
            Some(bytes)
        } else {
            None
        };
        let mut channel = ControlChannel::connect(peer.endpoint, deadline)?;
        if payload.is_some() {
            // Stay on this connection after Describe. A changed snapshot is an
            // extra fence, not proof of freshness: frozen TENT has no wire epoch.
            recheck(peer, &mut channel)?;
        }
        channel.write_all(&wire::request(
            if payload.is_some() { WRITE } else { READ },
            16 + payload.as_ref().map_or(0, Vec::len),
        )?)?;
        channel.write_all(&descriptor)?;
        if let Some(bytes) = payload {
            *posted = true;
            effect = RemoteEffect::Unknown;
            channel.write_all(&bytes)?;
            if !response(&mut channel, MAX_ERROR)?.is_empty() {
                return Err(Error::Backend {
                    operation: "TENT TCP WRITE rejected",
                    code: 135,
                });
            }
        } else {
            *posted = true;
            let bytes = response(&mut channel, request.length.max(MAX_ERROR))?;
            if bytes.len() != request.length {
                return Err(Error::Backend {
                    operation: "TENT TCP READ rejected",
                    code: 135,
                });
            }
            remaining(deadline)?;
            #[cfg(feature = "cuda")]
            proxy.write(&request.local, request.local_offset, &bytes, deadline)?;
            #[cfg(not(feature = "cuda"))]
            request.local.write(request.local_offset, &bytes)?;
        }
        Ok(())
    })();
    result.map_err(|error| (error, effect))
}

#[derive(Deserialize, Serialize)]
struct Message {
    name: String,
    msg: String,
}

pub(crate) fn notify(
    endpoint: SocketAddr,
    message: &Notification,
    deadline: Instant,
) -> std::result::Result<(), NotificationFailure> {
    let mut effect = NotificationEffect::None;
    let outcome = (|| {
        let bytes = serde_json::to_vec(&Message {
            name: message.name.clone(),
            msg: message.message.clone(),
        })
        .map_err(|_| Error::Internal("TENT notification serialization"))?;
        let mut channel = ControlChannel::connect(endpoint, deadline)?;
        channel.write_all(&wire::request(NOTIFY, bytes.len())?)?;
        effect = NotificationEffect::Unknown;
        channel.write_all(&bytes)?;
        if !response(&mut channel, MAX_ERROR)?.is_empty() {
            return Err(Error::Backend {
                operation: "TENT notification rejected",
                code: 135,
            });
        }
        Ok(())
    })();
    outcome.map_err(|error| NotificationFailure { error, effect })
}

/// A persistent TENT client may issue multiple requests on its pooled socket.
/// A fixed worker owns it; idle/partial requests cannot exceed handler_timeout.
pub(super) fn serve(
    mut channel: ControlChannel<'_>,
    source: &Arc<Source>,
    endpoint: SocketAddr,
    max_bytes: usize,
    timeout: Duration,
) -> Result<()> {
    loop {
        let mut header = [0; wire::REQUEST];
        channel.read_exact(&mut header)?;
        let (sequence, size) =
            wire::request_header(&header, (max_bytes + 16).max(MAX_NOTIFY_WIRE))?;
        let mut argument = [0; wire::ARGUMENT];
        channel.read_exact(&mut argument)?;
        let function = wire::argument(&argument)?;
        let admission = source.admit();
        // Consume the bounded attachment before replying, even on rejection, so
        // unread input cannot turn a well-formed error reply into a TCP reset.
        let mut payload = buffer(size)?;
        channel.read_exact(&mut payload)?;
        let Ok(admission) = admission else {
            channel.write_all(&wire::error_response(sequence))?;
            return Ok(());
        };
        let reply = match function {
            DESCRIBE if payload.is_empty() => metadata::encode(source, endpoint)?,
            PROBE if payload.is_empty() => Vec::new(),
            WRITE | READ => data(
                source,
                function,
                &payload,
                max_bytes,
                channel.deadline,
                admission,
            )?,
            NOTIFY => notification(source, &payload),
            BOOTSTRAP => {
                let mut b: metadata::Bootstrap = serde_json::from_slice(&payload)
                    .map_err(|_| Error::Protocol("TENT RDMA bootstrap JSON"))?;
                if b.qp_num.len() > 4096
                    || b.local_nic_path.len() > 1024
                    || b.peer_nic_path.len() > 1024
                {
                    return Err(Error::ResourceExhausted);
                }
                b.qp_num.clear();
                b.local_gid.clear();
                b.notify_qp_num = 0;
                b.reply_msg = "TENT RDMA bootstrap interop is unavailable; revocable grants require OrbiTE framing".into();
                serde_json::to_vec(&b)
                    .map_err(|_| Error::Internal("TENT bootstrap serialization"))?
            }
            _ => {
                // No silent subscribe/pin/delegate success without an owner.
                channel.write_all(&wire::error_response(sequence))?;
                channel.deadline = Instant::now().checked_add(timeout).ok_or(Error::Timeout)?;
                continue;
            }
        };
        channel.write_all(&wire::response(sequence, reply.len())?)?;
        channel.write_all(&wire::RETURN)?;
        channel.write_all(&reply)?;
        channel.deadline = Instant::now().checked_add(timeout).ok_or(Error::Timeout)?;
    }
}

fn notification(source: &Source, payload: &[u8]) -> Vec<u8> {
    let result = (|| {
        let m: Message = serde_json::from_slice(payload)
            .map_err(|_| Error::Protocol("malformed notification"))?;
        let permit = source.notifications.reserve(m.name.len(), m.msg.len())?;
        source.notifications.deliver(
            permit,
            Notification {
                name: m.name,
                message: m.msg,
            },
        )
    })();
    match result {
        Ok(()) => Vec::new(),
        Err(Error::Protocol(_)) => b"malformed notification: invalid JSON fields".to_vec(),
        Err(_) => b"notify callback failed: not accepted".to_vec(),
    }
}

fn data(
    source: &Arc<Source>,
    function: i32,
    payload: &[u8],
    max_bytes: usize,
    deadline: Instant,
    admission: crate::engine::SourcePermit,
) -> Result<Vec<u8>> {
    let mut requested = 0u64;
    let mut retained = None;
    let result = (|| {
        let head = payload.get(..16).ok_or(Error::InvalidRange)?;
        let mut addr = [0; 8];
        addr.copy_from_slice(&head[..8]);
        let mut len = [0; 8];
        len.copy_from_slice(&head[8..]);
        requested = u64::from_le_bytes(len);
        let length = usize::try_from(requested).map_err(|_| Error::InvalidRange)?;
        if length == 0
            || length > max_bytes
            || payload.len() != 16 + if function == WRITE { length } else { 0 }
        {
            return Err(Error::InvalidRange);
        }
        let (memory, offset) = source
            .registry
            .resolve_address(u64::from_le_bytes(addr), length)?;
        if memory.0.internal
            || !memory.0.descriptor.profile.permits(Transport::Tcp)
            || !memory.permission().allows(function == WRITE)
        {
            return Err(Error::PermissionDenied);
        }
        retained = Some(memory.clone());
        remaining(deadline)?;
        if function == WRITE {
            #[cfg(feature = "cuda")]
            source
                .proxy
                .write(&memory, offset, &payload[16..], deadline)?;
            #[cfg(not(feature = "cuda"))]
            memory.write(offset, &payload[16..])?;
            Ok(Vec::new())
        } else {
            let mut bytes = buffer(length)?;
            #[cfg(feature = "cuda")]
            source.proxy.read(&memory, offset, &mut bytes, deadline)?;
            #[cfg(not(feature = "cuda"))]
            memory.read(offset, &mut bytes)?;
            Ok(bytes)
        }
    })();
    match result {
        Ok(bytes) => Ok(bytes),
        Err(e @ Error::Unquiesced { .. }) => {
            // ProxyManager keeps native buffers; this keeps registration and admission.
            std::mem::forget((retained, admission));
            Err(e)
        }
        Err(error) => {
            let mut bytes = match error {
                Error::PermissionDenied => b"TENT TCP failed: remote permission denied".to_vec(),
                Error::RegionNotFound => {
                    b"TENT TCP failed: address not in registered buffer".to_vec()
                }
                _ => b"TENT TCP failed: invalid range or unavailable copy".to_vec(),
            };
            // TENT READ distinguishes a service error from bytes solely by length.
            if function == READ && bytes.len() as u64 == requested {
                bytes.push(b' ');
            }
            Ok(bytes)
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/runtime/tent.rs"]
mod tests;
