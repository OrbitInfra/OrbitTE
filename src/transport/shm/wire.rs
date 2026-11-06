use crate::{Error, MemoryType, RemoteRegion, Result, SourceEpoch};

pub(crate) const HEADER: usize = 8;
pub(crate) const RECORD: usize = 80;
// Locks beyond the data address space fence allocation and registration lifetimes.
pub(super) const GATES: i64 = 1 << 61;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(super) struct Name {
    pub ns: u64,
    pub pid: u32,
    pub nonce: SourceEpoch,
}
#[cfg(target_os = "linux")]
impl Name {
    pub(super) fn basename(self) -> String {
        format!(
            "orbite-shm-{:016x}-{:08x}-{}",
            self.ns, self.pid, self.nonce
        )
    }
    pub(super) fn parse(s: &str) -> Option<Self> {
        let mut fields = s.strip_prefix("orbite-shm-")?.split('-');
        let ns = u64::from_str_radix(fields.next()?, 16).ok()?;
        let pid = u32::from_str_radix(fields.next()?, 16).ok()?;
        let nonce = fields.next()?.parse().ok()?;
        let n = Self { ns, pid, nonce };
        (pid != 0 && pid <= i32::MAX as u32 && fields.next().is_none() && n.basename() == s)
            .then_some(n)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct Descriptor {
    pub id: u64,
    pub offset: u64,
    pub size: u64,
    pub gate: u64,
    pub dev: u64,
    pub ino: u64,
    pub(super) name: Name,
}
#[derive(Clone, Debug, Default)]
pub(crate) struct Manifest {
    pub cxl: bool,
    pub entries: Vec<Descriptor>,
}
impl Manifest {
    pub fn get(&self, id: u64) -> Option<&Descriptor> {
        self.entries
            .binary_search_by_key(&id, |r| r.id)
            .ok()
            .map(|i| &self.entries[i])
    }
    pub fn wire_len(&self) -> usize {
        HEADER + self.entries.len() * RECORD
    }
    pub fn encode(&self, bytes: &mut [u8]) {
        bytes[..4].copy_from_slice(&(self.entries.len() as u32).to_be_bytes());
        bytes[4] = u8::from(self.cxl);
        bytes[5..8].fill(0);
        for (d, b) in self
            .entries
            .iter()
            .zip(bytes[HEADER..].chunks_exact_mut(RECORD))
        {
            for (n, to) in [d.id, d.offset, d.size, d.gate, d.dev, d.ino, d.name.ns]
                .into_iter()
                .zip(b[..56].chunks_exact_mut(8))
            {
                to.copy_from_slice(&n.to_be_bytes());
            }
            b[56..60].copy_from_slice(&d.name.pid.to_be_bytes());
            b[60..64].fill(0);
            b[64..80].copy_from_slice(&d.name.nonce.0);
        }
    }
    pub fn decode(bytes: &[u8], region_count: usize) -> Result<(Self, usize)> {
        let bad = || Error::Protocol("invalid SHM descriptor");
        if bytes.len() < HEADER || bytes[4] > 1 || bytes[5..8] != [0; 3] {
            return Err(bad());
        }
        let count = u32::from_be_bytes(bytes[..4].try_into().map_err(|_| bad())?) as usize;
        if count > region_count {
            return Err(bad());
        }
        let end = HEADER + count * RECORD;
        let mut entries = Vec::with_capacity(count);
        let mut previous = 0;
        for b in bytes.get(HEADER..end).ok_or_else(bad)?.chunks_exact(RECORD) {
            let word = |at| {
                let mut v = [0; 8];
                v.copy_from_slice(&b[at..at + 8]);
                u64::from_be_bytes(v)
            };
            let id = word(0);
            let offset = word(8);
            let size = word(16);
            let gate = word(24);
            let pid = u32::from_be_bytes(b[56..60].try_into().map_err(|_| bad())?);
            if id <= previous
                || offset >= size
                || size > GATES as u64
                || size > isize::MAX as u64
                || gate == 0
                || gate > (i64::MAX as u64 - GATES as u64 - 3) / 3
                || pid == 0
                || pid > i32::MAX as u32
                || b[60..64] != [0; 4]
            {
                return Err(bad());
            }
            entries.push(Descriptor {
                id,
                offset,
                size,
                gate,
                dev: word(32),
                ino: word(40),
                name: Name {
                    ns: word(48),
                    pid,
                    nonce: SourceEpoch(b[64..80].try_into().map_err(|_| bad())?),
                },
            });
            previous = id;
        }
        Ok((
            Self {
                cxl: bytes[4] != 0,
                entries,
            },
            end,
        ))
    }
    pub fn validate(&self, regions: &[RemoteRegion]) -> Result<()> {
        for d in &self.entries {
            let r = regions
                .binary_search_by_key(&d.id, |r| r.id())
                .ok()
                .map(|i| regions[i])
                .ok_or(Error::Protocol("missing SHM registration"))?;
            if r.memory_type() != MemoryType::Cpu
                || r.permission() == crate::Permission::LocalOnly
                || d.offset
                    .checked_add(r.len() as u64)
                    .is_none_or(|end| end > d.size)
            {
                return Err(Error::Protocol("invalid SHM registration range"));
            }
        }
        Ok(())
    }
}

#[cfg(target_os = "linux")]
impl Descriptor {
    pub(crate) fn object(&self) -> (u64, u64) {
        (self.dev, self.ino)
    }
}
