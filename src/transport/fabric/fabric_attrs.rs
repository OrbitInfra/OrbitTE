//! TENT's provider/chunk attributes with checked arithmetic and bounded decoding.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};

pub(crate) const MAX_WIRE: usize = 1 << 20;
pub(crate) const MAX_NICS: usize = 16;
pub(crate) const MAX_CHUNKS: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Nic {
    pub name: String,
    pub addr: String,
    #[serde(default = "unknown_numa")]
    pub numa: i32,
}
fn unknown_numa() -> i32 {
    -1
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PeerAttr {
    pub version: u32,
    pub provider: String,
    pub virt_addr: bool,
    pub nics: Vec<Nic>,
}
impl PeerAttr {
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_WIRE {
            return Err(Error::ResourceExhausted);
        }
        let p: Self =
            serde_json::from_slice(bytes).map_err(|_| Error::Protocol("fabric peer attributes"))?;
        if p.version != 1
            || !matches!(p.provider.as_str(), "tcp;ofi_rxm" | "efa")
            || p.nics.is_empty()
            || p.nics.len() > MAX_NICS
        {
            return Err(Error::Protocol("fabric provider, version or NIC count"));
        }
        for n in &p.nics {
            if n.name.is_empty() || n.name.len() > 255 || n.numa < -1 || unhex(&n.addr)?.is_empty()
            {
                return Err(Error::Protocol("fabric NIC attributes"));
            }
        }
        Ok(p)
    }
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        encode(self)
    }
}
pub(crate) fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>> {
    let bytes =
        serde_json::to_vec(v).map_err(|_| Error::Internal("fabric attribute serialization"))?;
    if bytes.len() > MAX_WIRE {
        return Err(Error::ResourceExhausted);
    }
    Ok(bytes)
}
pub(crate) fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    bytes
        .iter()
        .flat_map(|b| {
            [
                DIGITS[(b >> 4) as usize] as char,
                DIGITS[(b & 15) as usize] as char,
            ]
        })
        .collect()
}
pub(crate) fn unhex(s: &str) -> Result<Vec<u8>> {
    if s.len() % 2 != 0 || s.len() > 2048 {
        return Err(Error::Protocol("fabric NIC address"));
    }
    let digit = |b: u8| match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    };
    s.as_bytes()
        .chunks_exact(2)
        .map(|p| {
            digit(p[0])
                .zip(digit(p[1]))
                .map(|(a, b)| a * 16 + b)
                .ok_or(Error::Protocol("fabric NIC address"))
        })
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Range {
    pub off: u64,
    pub len: u64,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Chunk {
    pub off: u64,
    pub len: u64,
    pub nics: Vec<usize>,
    pub keys: Vec<u64>,
}
impl Chunk {
    pub fn key_for(&self, nic: usize) -> Option<u64> {
        self.nics
            .iter()
            .position(|&n| n == nic)
            .and_then(|i| self.keys.get(i).copied())
    }
    pub fn range(&self) -> Range {
        Range {
            off: self.off,
            len: self.len,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct BufferAttr {
    pub chunks: Vec<Chunk>,
}
impl BufferAttr {
    #[cfg(test)]
    pub(crate) fn decode(bytes: &[u8], length: u64, nics: usize) -> Result<Self> {
        if bytes.len() > MAX_WIRE {
            return Err(Error::ResourceExhausted);
        }
        let b: Self = serde_json::from_slice(bytes)
            .map_err(|_| Error::Protocol("fabric buffer attributes"))?;
        b.validate(length, nics)?;
        Ok(b)
    }
    pub(crate) fn validate(&self, length: u64, nics: usize) -> Result<()> {
        if self.chunks.is_empty() || self.chunks.len() > MAX_CHUNKS {
            return Err(Error::Protocol("fabric chunk count"));
        }
        let mut end = 0u64;
        for c in &self.chunks {
            if c.off != end
                || c.len == 0
                || c.nics.is_empty()
                || c.nics.len() > MAX_NICS
                || c.nics.len() != c.keys.len()
                || c.nics.iter().any(|&n| n >= nics)
                || c.nics
                    .iter()
                    .enumerate()
                    .any(|(i, n)| c.nics[..i].contains(n))
            {
                return Err(Error::Protocol("inconsistent fabric buffer chunks"));
            }
            end = end
                .checked_add(c.len)
                .ok_or(Error::Protocol("fabric chunk overflow"))?;
        }
        if end != length {
            return Err(Error::Protocol("fabric chunks do not cover the buffer"));
        }
        Ok(())
    }
    pub(crate) fn ranges(&self) -> Vec<Range> {
        self.chunks.iter().map(Chunk::range).collect()
    }
}

pub(crate) fn plan_chunks(length: u64, limit: u64) -> Result<Vec<Range>> {
    let limit = if limit == 0 { length } else { limit };
    if length == 0 || length.div_ceil(limit) > MAX_CHUNKS as u64 {
        return Err(Error::ResourceExhausted);
    }
    let mut ranges = Vec::new();
    let mut off = 0;
    while off < length {
        let len = limit.min(length - off);
        ranges.push(Range { off, len });
        off += len;
    }
    Ok(ranges)
}
pub(crate) fn assign_nics(
    chunks: &[Range],
    nics: usize,
    page: u64,
    budget: u64,
) -> Result<Vec<Vec<usize>>> {
    if nics == 0 || nics > MAX_NICS || chunks.is_empty() || chunks.len() > MAX_CHUNKS {
        return Err(Error::InvalidRange);
    }
    let page = if page == 0 { 4096 } else { page };
    let pages: Vec<_> = chunks.iter().map(|c| c.len.div_ceil(page)).collect();
    let total = pages
        .iter()
        .try_fold(0u64, |n, p| n.checked_add(*p))
        .ok_or(Error::InvalidRange)?;
    if budget == 0 || total <= budget {
        return Ok(vec![(0..nics).collect(); chunks.len()]);
    }
    if chunks.len() <= nics {
        if pages.iter().any(|&p| p > budget) {
            return Err(Error::ResourceExhausted);
        }
        let mut next = 0;
        return Ok((0..chunks.len())
            .map(|c| {
                let count = nics / chunks.len() + usize::from(c < nics % chunks.len());
                let assigned = (next..next + count).collect();
                next += count;
                assigned
            })
            .collect());
    }
    let mut used = vec![0u64; nics];
    let mut assignments = Vec::new();
    for (i, p) in pages.into_iter().enumerate() {
        let n = i % nics;
        used[n] = used[n]
            .checked_add(p)
            .filter(|&n| n <= budget)
            .ok_or(Error::ResourceExhausted)?;
        assignments.push(vec![n]);
    }
    Ok(assignments)
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Span {
    pub off: u64,
    pub len: u64,
    pub local: usize,
    pub remote: usize,
}
pub(crate) fn cut_spans(
    length: u64,
    local_start: u64,
    local: &[Range],
    remote_start: u64,
    remote: &[Range],
    max: u64,
    bound: usize,
) -> Result<Vec<Span>> {
    let max = if max == 0 { u64::MAX } else { max };
    let find = |rs: &[Range], off: u64| {
        rs.iter()
            .enumerate()
            .find(|(_, r)| off >= r.off && off - r.off < r.len)
            .map(|(i, r)| (i, r.len - (off - r.off)))
    };
    let mut done = 0;
    let mut spans = Vec::new();
    while done < length {
        if spans.len() == bound {
            return Err(Error::ResourceExhausted);
        }
        let (l, ln) = find(
            local,
            local_start.checked_add(done).ok_or(Error::InvalidRange)?,
        )
        .ok_or(Error::InvalidRange)?;
        let (r, rn) = find(
            remote,
            remote_start.checked_add(done).ok_or(Error::InvalidRange)?,
        )
        .ok_or(Error::InvalidRange)?;
        let len = (length - done).min(ln).min(rn).min(max);
        spans.push(Span {
            off: done,
            len,
            local: l,
            remote: r,
        });
        done += len;
    }
    Ok(spans)
}
