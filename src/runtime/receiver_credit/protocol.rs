use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::engine::Source;
use crate::runtime::control_plane::{ControlChannel, Operation, Reply, Request, Stop, u64_at};
use crate::{Error, Result, SourceEpoch};

const REPORT_SIZE: usize = 64;
const SNAPSHOT_SIZE: usize = 72;

#[derive(Clone, Copy, Debug)]
pub(super) struct CreditReport {
    pub(super) sender: SourceEpoch,
    pub(super) epoch: u64,
    pub(super) qos_class: u32,
    pub(super) committed: [u64; 2],
    pub(super) completed: [u64; 2],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CreditSnapshot {
    pub(super) epoch: u64,
    pub(super) sequence: u64,
    pub(super) qos_class: u32,
    pub(super) window: [u64; 2],
    pub(super) grants: [u64; 2],
}

impl CreditReport {
    fn encode(self) -> [u8; REPORT_SIZE] {
        let mut bytes = [0; REPORT_SIZE];
        bytes[..16].copy_from_slice(&self.sender.0);
        bytes[16..24].copy_from_slice(&self.epoch.to_be_bytes());
        bytes[24..28].copy_from_slice(&self.qos_class.to_be_bytes());
        for (i, value) in self.committed.into_iter().chain(self.completed).enumerate() {
            bytes[32 + i * 8..40 + i * 8].copy_from_slice(&value.to_be_bytes());
        }
        bytes
    }

    fn decode(bytes: &[u8; REPORT_SIZE]) -> Result<Self> {
        if bytes[28..32] != [0; 4] || bytes[..16] == [0; 16] {
            return Err(Error::Protocol("invalid receiver credit sender report"));
        }
        let mut sender = [0; 16];
        sender.copy_from_slice(&bytes[..16]);
        Ok(Self {
            sender: SourceEpoch(sender),
            epoch: u64_at(bytes, 16),
            qos_class: u32::from_be_bytes(
                bytes[24..28]
                    .try_into()
                    .map_err(|_| Error::Protocol("credit class"))?,
            ),
            committed: [u64_at(bytes, 32), u64_at(bytes, 40)],
            completed: [u64_at(bytes, 48), u64_at(bytes, 56)],
        })
    }
}

impl CreditSnapshot {
    fn encode(self, receiver: SourceEpoch) -> [u8; SNAPSHOT_SIZE] {
        let mut bytes = [0; SNAPSHOT_SIZE];
        bytes[..2].copy_from_slice(&1u16.to_be_bytes());
        bytes[4..8].copy_from_slice(&self.qos_class.to_be_bytes());
        bytes[8..24].copy_from_slice(&receiver.0);
        for (i, value) in [
            self.epoch,
            self.sequence,
            self.window[0],
            self.window[1],
            self.grants[0],
            self.grants[1],
        ]
        .into_iter()
        .enumerate()
        {
            bytes[24 + i * 8..32 + i * 8].copy_from_slice(&value.to_be_bytes());
        }
        bytes
    }

    fn decode(bytes: &[u8; SNAPSHOT_SIZE], receiver: SourceEpoch) -> Result<Self> {
        if bytes[..4] != [0, 1, 0, 0] || bytes[8..24] != receiver.0 {
            return Err(Error::Protocol(
                "receiver credit snapshot version or identity",
            ));
        }
        let snapshot = Self {
            epoch: u64_at(bytes, 24),
            sequence: u64_at(bytes, 32),
            qos_class: u32::from_be_bytes(
                bytes[4..8]
                    .try_into()
                    .map_err(|_| Error::Protocol("credit class"))?,
            ),
            window: [u64_at(bytes, 40), u64_at(bytes, 48)],
            grants: [u64_at(bytes, 56), u64_at(bytes, 64)],
        };
        if snapshot.epoch == 0
            || snapshot.sequence == 0
            || snapshot.window.contains(&0)
            || snapshot
                .window
                .iter()
                .zip(snapshot.grants)
                .any(|(window, grant)| *window > grant)
        {
            return Err(Error::Protocol("invalid receiver credit snapshot"));
        }
        Ok(snapshot)
    }
}

pub(super) fn synchronize(
    endpoint: SocketAddr,
    receiver: SourceEpoch,
    report: CreditReport,
    close: bool,
    deadline: Instant,
    cancelled: &(dyn Fn() -> bool + Sync),
) -> Result<CreditSnapshot> {
    if cancelled() {
        return Err(Error::Cancelled);
    }
    // std's connect cannot be interrupted; bound that phase, then retain the
    // caller's original deadline and poll cancellation during all socket I/O.
    let connect_deadline = deadline.min(Instant::now() + Duration::from_millis(250));
    let connected = ControlChannel::connect(endpoint, connect_deadline);
    if cancelled() {
        return Err(Error::Cancelled);
    }
    let mut channel = connected?;
    channel.deadline = deadline;
    channel.stopping = Some(Stop::Cancelled(cancelled));
    channel.write_all(
        &Request {
            operation: if close {
                Operation::CreditClose
            } else {
                Operation::CreditSync
            },
            epoch: receiver,
            region: 0,
            offset: 0,
            length: 0,
        }
        .encode(),
    )?;
    channel.write_all(&report.encode())?;
    let reply = Reply::read(&mut channel, receiver)?;
    if reply.status != 0 || reply.length != SNAPSHOT_SIZE as u64 {
        return Err(Error::Protocol("receiver credit reply size or status"));
    }
    let mut bytes = [0; SNAPSHOT_SIZE];
    channel.read_exact(&mut bytes)?;
    CreditSnapshot::decode(&bytes, receiver)
}

pub(crate) fn serve(channel: &mut ControlChannel<'_>, source: &Source, close: bool) -> Result<()> {
    let issuer = source
        .credit_issuer
        .as_ref()
        .ok_or(Error::Unavailable("receiver credit issuer is disabled"))?;
    let mut bytes = [0; REPORT_SIZE];
    channel.read_exact(&mut bytes)?;
    let report = CreditReport::decode(&bytes)?;
    let admission = if close {
        None
    } else {
        match source.admit() {
            Ok(permit) => Some(permit),
            Err(Error::SourceClosed) => None,
            Err(error) => return Err(error),
        }
    };
    let snapshot = issuer.synchronize(report, close, admission.is_some())?;
    channel.reply(source.epoch, 0, SNAPSHOT_SIZE as u64)?;
    channel.write_all(&snapshot.encode(source.epoch))
}
