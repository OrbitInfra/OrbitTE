use crate::{
    abi::Buffer,
    boundary::*,
    engine::{self, Engine},
};
use orbit_te::{FileSegment, Permission, RemoteSegment, SourceEpoch, TransferTarget};
use std::{
    collections::HashMap,
    net::{SocketAddr, ToSocketAddrs},
    sync::{Arc, Mutex},
};

enum Entry {
    Peer {
        addr: SocketAddr,
        epoch: Mutex<Option<SourceEpoch>>,
    },
    File(FileSegment),
}
#[derive(Default)]
pub(crate) struct Segments {
    entries: HashMap<u64, (String, Arc<Entry>)>,
}
#[derive(Clone)]
pub(crate) enum Target {
    Peer(RemoteSegment),
    File(FileSegment),
}
impl Target {
    pub fn view(&self) -> TransferTarget<'_> {
        match self {
            Self::Peer(s) => s.into(),
            Self::File(s) => s.into(),
        }
    }
}

impl Engine {
    pub fn name(&self) -> String {
        self.core.endpoint().to_string()
    }
    pub fn open(&self, name: String) -> Result<u64> {
        if name.is_empty() || name == self.name() {
            return Ok(0);
        }
        let mut table = lock(&self.segments)?;
        if let Some((&id, _)) = table.entries.iter().find(|(_, v)| v.0 == name) {
            return Ok(id);
        }
        if table.entries.len() >= self.core.config().max_registered_regions {
            return Err(FULL);
        }
        let entry = if name.starts_with("file://") {
            Entry::File(
                self.core
                    .open_file_segment(&name, Permission::RemoteReadWrite)?,
            )
        } else {
            let addr = name
                .to_socket_addrs()
                .map_err(|_| INVALID)?
                .next()
                .ok_or(INVALID)?;
            Entry::Peer {
                addr,
                epoch: Mutex::new(None),
            }
        };
        let id = engine::id()? as u64;
        table.entries.insert(id, (name, Arc::new(entry)));
        Ok(id)
    }
    pub fn close_segment(&self, id: u64) -> Result<()> {
        if id == 0 {
            return Ok(());
        }
        let mut table = lock(&self.segments)?;
        if let Entry::File(f) = table.entries.get(&id).ok_or(INVALID)?.1.as_ref() {
            self.core.close_file_segment(f, self.timeout())?;
        }
        table.entries.remove(&id);
        Ok(())
    }
    pub fn target(&self, id: u64) -> Result<Target> {
        let core = &self.core;
        if id == 0 {
            return Ok(Target::Peer(core.open_segment(
                core.endpoint(),
                core.source_epoch(),
                self.timeout(),
            )?));
        }
        let entry = lock(&self.segments)?
            .entries
            .get(&id)
            .ok_or(INVALID)?
            .1
            .clone();
        match &*entry {
            Entry::File(f) => Ok(Target::File(f.clone())),
            Entry::Peer { addr, epoch } => {
                let expected = {
                    let mut pin = lock(epoch)?;
                    match *pin {
                        Some(epoch) => epoch,
                        None => {
                            let first = core
                                .discover_peer_epoch(*addr, self.timeout())
                                .map_err(metadata_error)?;
                            *pin = Some(first);
                            first
                        }
                    }
                };
                Ok(Target::Peer(
                    core.open_segment(*addr, expected, self.timeout())
                        .map_err(metadata_error)?,
                ))
            }
        }
    }
    pub fn info(&self, id: u64) -> Result<(i32, Vec<Buffer>)> {
        if id == 0 {
            return Ok((0, lock(&self.memory)?.info()?));
        }
        match self.target(id)? {
            Target::Peer(s) => Ok((
                0,
                s.regions()
                    .iter()
                    .map(|r| Buffer {
                        base: r.base_address(),
                        length: r.len() as u64,
                        location: fixed(r.location_name().as_str()),
                    })
                    .collect(),
            )),
            Target::File(f) => {
                let info = self.core.file_segment_info(&f)?;
                Ok((
                    1,
                    vec![Buffer {
                        base: info.offset,
                        length: info.length,
                        location: fixed("*"),
                    }],
                ))
            }
        }
    }
}
fn metadata_error(e: orbit_te::Error) -> Code {
    match e {
        orbit_te::Error::Io(ref io)
            if matches!(
                io.kind(),
                std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::AddrNotAvailable
                    | std::io::ErrorKind::NetworkUnreachable
                    | std::io::ErrorKind::HostUnreachable
            ) =>
        {
            UNSENT
        }
        e => e.into(),
    }
}
