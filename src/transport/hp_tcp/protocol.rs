use crate::{Error, Result, TransferOp};

const MAGIC: [u8; 4] = *b"MCTP";
const VERSION: u16 = 1;
pub(super) const REQUEST: usize = 48;
pub(super) const RESPONSE: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub(super) enum Status {
    Ok,
    BadVersion,
    BadOpcode,
    BadLength,
    Range,
    Permission,
    Stale,
    Closing,
    Internal,
}

impl Status {
    pub(super) fn error(self) -> Error {
        match self {
            Self::Range | Self::Stale => Error::RegionNotFound,
            Self::Permission => Error::PermissionDenied,
            Self::Closing => Error::SourceClosed,
            _ => Error::Protocol("invalid HP-TCP payload frame"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Request {
    pub op: TransferOp,
    pub id: u64,
    pub registration: u64,
    pub address: u64,
    pub length: u64,
}

fn word(bytes: &[u8], at: usize) -> u64 {
    let mut word = [0; 8];
    word.copy_from_slice(&bytes[at..at + 8]);
    u64::from_be_bytes(word)
}

fn header<const N: usize>() -> [u8; N] {
    let mut bytes = [0; N];
    bytes[..4].copy_from_slice(&MAGIC);
    bytes[4..6].copy_from_slice(&VERSION.to_be_bytes());
    bytes
}

impl Request {
    pub fn encode(self) -> [u8; REQUEST] {
        let mut b = header();
        b[6] = match self.op {
            TransferOp::Read => 1,
            TransferOp::Write => 2,
        };
        for (slot, n) in b[8..40].chunks_exact_mut(8).zip([
            self.id,
            self.registration,
            self.address,
            self.length,
        ]) {
            slot.copy_from_slice(&n.to_be_bytes());
        }
        b
    }

    pub fn decode(b: &[u8]) -> std::result::Result<Self, Status> {
        if b.len() != REQUEST || b[..4] != MAGIC {
            return Err(Status::Internal);
        }
        if b[4..6] != VERSION.to_be_bytes() {
            return Err(Status::BadVersion);
        }
        if b[7] != 0 || word(b, 40) != 0 {
            return Err(Status::Internal);
        }
        let op = match b[6] {
            1 => TransferOp::Read,
            2 => TransferOp::Write,
            _ => return Err(Status::BadOpcode),
        };
        let address = word(b, 24);
        let length = word(b, 32);
        if length == 0 || address.checked_add(length).is_none() {
            return Err(Status::BadLength);
        }
        Ok(Self {
            op,
            id: word(b, 8),
            registration: word(b, 16),
            address,
            length,
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct Response {
    pub status: Status,
    pub id: u64,
    pub committed: u64,
}
impl Response {
    pub fn encode(&self) -> [u8; RESPONSE] {
        let mut b = header();
        b[6..8].copy_from_slice(&(self.status as u16).to_be_bytes());
        b[8..16].copy_from_slice(&self.id.to_be_bytes());
        b[16..24].copy_from_slice(&self.committed.to_be_bytes());
        b
    }
    pub fn decode(b: &[u8]) -> Result<Self> {
        if b.len() != RESPONSE
            || b[..4] != MAGIC
            || b[4..6] != VERSION.to_be_bytes()
            || word(b, 24) != 0
        {
            return Err(Error::Protocol("invalid HP-TCP response header"));
        }
        let status = match u16::from_be_bytes([b[6], b[7]]) {
            0 => Status::Ok,
            1 => Status::BadVersion,
            2 => Status::BadOpcode,
            3 => Status::BadLength,
            4 => Status::Range,
            5 => Status::Permission,
            6 => Status::Stale,
            7 => Status::Closing,
            8 => Status::Internal,
            _ => return Err(Error::Protocol("unknown HP-TCP status")),
        };
        Ok(Self {
            status,
            id: word(b, 8),
            committed: word(b, 16),
        })
    }
}
