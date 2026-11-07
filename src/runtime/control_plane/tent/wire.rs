//! The narrow coro_rpc/struct_pack dialect used by frozen TENT's process(int).
//! Sizes, hashes and serialization bytes are checked against the S16 native corpus.

use crate::{Error, Result};

pub(crate) const REQUEST: usize = 20;
pub(crate) const RESPONSE: usize = 16;
pub(crate) const ARGUMENT: usize = 11;
pub(crate) const RETURN: [u8; 7] = [0xeb, 0xcf, 1, 0x89, 4, 0xfa, 0];
const MAGIC: u8 = 0x15;
const VERSION: u8 = 0;
const FUNCTION: u32 = 0xff74660d;
const INT_PREFIX: [u8; 7] = [9, 0x40, 0xa5, 0x55, 4, 1, 0];

fn word(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

fn prefix(b: &[u8]) -> Result<()> {
    if b[0] != MAGIC {
        return Err(Error::Protocol("TENT RPC magic mismatch"));
    }
    if b[1] != VERSION {
        return Err(Error::Protocol("TENT RPC version mismatch"));
    }
    if b[3] != 0 {
        return Err(Error::Protocol("TENT RPC message type"));
    }
    Ok(())
}

pub(crate) fn request(function: i32, length: usize) -> Result<[u8; REQUEST + ARGUMENT]> {
    let mut b = [0; REQUEST + ARGUMENT];
    b[0] = MAGIC;
    b[1] = VERSION;
    b[8..12].copy_from_slice(&FUNCTION.to_le_bytes());
    b[12..16].copy_from_slice(&(ARGUMENT as u32).to_le_bytes());
    b[16..20].copy_from_slice(
        &u32::try_from(length)
            .map_err(|_| Error::ResourceExhausted)?
            .to_le_bytes(),
    );
    b[20..27].copy_from_slice(&INT_PREFIX);
    b[27..].copy_from_slice(&function.to_le_bytes());
    Ok(b)
}

pub(crate) fn request_header(b: &[u8; REQUEST], limit: usize) -> Result<(u32, usize)> {
    prefix(b)?;
    if b[2] != 0 || word(b, 8) != FUNCTION || word(b, 12) != ARGUMENT as u32 {
        return Err(Error::Protocol(
            "unsupported TENT RPC function/serialization",
        ));
    }
    let length = word(b, 16) as usize;
    if length > limit {
        return Err(Error::ResourceExhausted);
    }
    Ok((word(b, 4), length))
}

pub(crate) fn argument(b: &[u8; ARGUMENT]) -> Result<i32> {
    if b[..7] != INT_PREFIX {
        return Err(Error::Protocol("TENT RPC integer serialization"));
    }
    Ok(i32::from_le_bytes([b[7], b[8], b[9], b[10]]))
}

pub(crate) fn response(sequence: u32, length: usize) -> Result<[u8; RESPONSE]> {
    let mut b = [0; RESPONSE];
    b[0] = MAGIC;
    b[1] = VERSION;
    b[4..8].copy_from_slice(&sequence.to_le_bytes());
    b[8..12].copy_from_slice(&(RETURN.len() as u32).to_le_bytes());
    b[12..].copy_from_slice(
        &u32::try_from(length)
            .map_err(|_| Error::ResourceExhausted)?
            .to_le_bytes(),
    );
    Ok(b)
}

pub(crate) fn response_header(b: &[u8; RESPONSE], limit: usize) -> Result<usize> {
    prefix(b)?;
    if word(b, 4) != 0 {
        return Err(Error::Protocol("TENT RPC response sequence"));
    }
    if b[2] != 0 {
        return Err(Error::Backend {
            operation: "TENT RPC service",
            code: 135,
        });
    }
    if word(b, 8) != RETURN.len() as u32 {
        return Err(Error::Protocol("TENT RPC return serialization size"));
    }
    let length = word(b, 12) as usize;
    if length > limit {
        return Err(Error::ResourceExhausted);
    }
    Ok(length)
}

pub(crate) fn return_body(b: &[u8; RETURN.len()]) -> Result<()> {
    if *b != RETURN {
        return Err(Error::Protocol("TENT RPC return serialization"));
    }
    Ok(())
}

pub(crate) fn error_response(sequence: u32) -> Vec<u8> {
    // Frozen struct_pack string encoding, deliberately bounded and not peer-derived.
    let message = b"unsupported TENT RPC in OrbiTE TCP profile";
    let mut b = vec![MAGIC, VERSION, 10, 0];
    b.extend_from_slice(&sequence.to_le_bytes());
    b.extend_from_slice(&((9 + message.len()) as u32).to_le_bytes());
    b.extend_from_slice(&0u32.to_le_bytes());
    b.extend_from_slice(&[
        0x77,
        0xfa,
        0xcf,
        0x9d,
        4,
        0x80,
        0x0c,
        0,
        message.len() as u8,
    ]);
    b.extend_from_slice(message);
    b
}

#[cfg(test)]
#[path = "../../../../tests/unit/runtime/tent/wire.rs"]
mod tests;
