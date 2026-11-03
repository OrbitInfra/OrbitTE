use crate::{Error, Result};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, ToSocketAddrs};

pub(crate) const MAX_RAILS: usize = 16;

/// Typed TENT transports/hp_tcp settings. The engine always retains its control listener.
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HpTcpConfig {
    pub enable: bool,
    pub bind_address: String,
    pub advertise_address: String,
    pub rail_addresses: Vec<Ipv4Addr>,
    pub port: u16,
    pub worker_count: usize,
    pub connections_per_peer: usize,
    pub max_outstanding_tasks: u64,
    pub max_outstanding_bytes: u64,
    pub max_transfer_bytes: u64,
    pub connect_timeout_ms: u64,
    pub progress_timeout_ms: u64,
    pub idle_connection_timeout_ms: u64,
}

impl Default for HpTcpConfig {
    fn default() -> Self {
        Self {
            enable: false,
            bind_address: String::new(),
            advertise_address: String::new(),
            rail_addresses: Vec::new(),
            port: 0,
            worker_count: 16,
            connections_per_peer: 4,
            max_outstanding_tasks: 4096,
            max_outstanding_bytes: 1 << 32,
            max_transfer_bytes: 1 << 30,
            connect_timeout_ms: 2000,
            progress_timeout_ms: 30000,
            idle_connection_timeout_ms: 60000,
        }
    }
}

impl HpTcpConfig {
    pub(crate) fn validate(&self, tcp: bool) -> Result<()> {
        if !self.enable {
            return Ok(());
        }
        if tcp {
            return Err(Error::InvalidConfig(
                "tcp and hp_tcp payloads cannot both be enabled",
            ));
        }
        if !(1..=256).contains(&self.worker_count)
            || !(1..=4096).contains(&self.connections_per_peer)
            || [
                self.max_outstanding_tasks,
                self.max_outstanding_bytes,
                self.max_transfer_bytes,
                self.connect_timeout_ms,
                self.progress_timeout_ms,
                self.idle_connection_timeout_ms,
            ]
            .contains(&0)
        {
            return Err(Error::InvalidConfig("invalid HP-TCP limits"));
        }
        let bind = self.bind_ip()?;
        if self.rail_addresses.len() > MAX_RAILS.min(self.connections_per_peer)
            || self
                .rail_addresses
                .iter()
                .enumerate()
                .any(|(i, r)| r.is_unspecified() || self.rail_addresses[..i].contains(r))
            || (!self.rail_addresses.is_empty()
                && !bind.is_unspecified()
                && self.rail_addresses.as_slice() != [bind])
        {
            return Err(Error::InvalidConfig(
                "HP-TCP requires unique IPv4 rails and a matching listener",
            ));
        }
        for ms in [
            self.connect_timeout_ms,
            self.progress_timeout_ms,
            self.idle_connection_timeout_ms,
        ] {
            if std::time::Instant::now()
                .checked_add(std::time::Duration::from_millis(ms))
                .is_none()
            {
                return Err(Error::InvalidConfig(
                    "HP-TCP timer exceeds the monotonic clock",
                ));
            }
        }
        Ok(())
    }
    fn bind_ip(&self) -> Result<Ipv4Addr> {
        if self.bind_address.is_empty() {
            Ok(Ipv4Addr::UNSPECIFIED)
        } else {
            self.bind_address
                .parse()
                .map_err(|_| Error::InvalidConfig("HP-TCP bind_address must be IPv4"))
        }
    }
    pub(crate) fn bind(&self, control: SocketAddr) -> Result<(TcpListener, Endpoint)> {
        let listener = TcpListener::bind((self.bind_ip()?, self.port))?;
        listener.set_nonblocking(true)?;
        let mut rails = self.rail_addresses.clone();
        if rails.is_empty() {
            let ip = if self.advertise_address.is_empty() {
                match control {
                    SocketAddr::V4(a) => *a.ip(),
                    _ => {
                        return Err(Error::InvalidConfig(
                            "HP-TCP requires an IPv4 advertise address",
                        ));
                    }
                }
            } else {
                (self.advertise_address.as_str(), 0)
                    .to_socket_addrs()?
                    .find_map(|a| match a {
                        SocketAddr::V4(v) => Some(*v.ip()),
                        _ => None,
                    })
                    .ok_or(Error::InvalidConfig("no IPv4 HP-TCP advertise address"))?
            };
            rails.push(ip);
        }
        let endpoint = Endpoint::new(
            listener.local_addr()?.port(),
            self.max_transfer_bytes,
            &rails,
        )?;
        Ok((listener, endpoint))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Endpoint {
    pub port: u16,
    pub max_bytes: u64,
    count: u8,
    rails: [Ipv4Addr; MAX_RAILS],
}
impl Endpoint {
    pub(crate) const MAX_WIRE: usize = 12 + 4 * MAX_RAILS;
    pub(super) fn new(port: u16, max_bytes: u64, addresses: &[Ipv4Addr]) -> Result<Self> {
        if port == 0
            || max_bytes == 0
            || addresses.is_empty()
            || addresses.len() > MAX_RAILS
            || addresses
                .iter()
                .enumerate()
                .any(|(i, a)| a.is_unspecified() || addresses[..i].contains(a))
        {
            return Err(Error::Protocol("invalid HP-TCP endpoints"));
        }
        let mut rails = [Ipv4Addr::UNSPECIFIED; MAX_RAILS];
        rails[..addresses.len()].copy_from_slice(addresses);
        Ok(Self {
            port,
            max_bytes,
            count: addresses.len() as u8,
            rails,
        })
    }
    pub(crate) fn rails(&self) -> &[Ipv4Addr] {
        &self.rails[..self.count as usize]
    }
    pub(crate) fn wire_len(&self) -> usize {
        12 + self.rails().len() * 4
    }
    pub(crate) fn encode(&self, b: &mut [u8]) {
        b[..2].copy_from_slice(&self.port.to_be_bytes());
        b[2] = self.count;
        b[3] = 0;
        b[4..12].copy_from_slice(&self.max_bytes.to_be_bytes());
        for (out, rail) in b[12..].chunks_exact_mut(4).zip(self.rails()) {
            out.copy_from_slice(&rail.octets());
        }
    }
    pub(crate) fn decode(b: &[u8]) -> Result<Self> {
        if b.len() < 12
            || b[3] != 0
            || b[2] as usize > MAX_RAILS
            || b.len() != 12 + b[2] as usize * 4
        {
            return Err(Error::Protocol("invalid HP-TCP endpoint descriptor"));
        }
        let mut limit = [0; 8];
        limit.copy_from_slice(&b[4..12]);
        let mut addresses = [Ipv4Addr::UNSPECIFIED; MAX_RAILS];
        for (slot, ip) in addresses.iter_mut().zip(b[12..].chunks_exact(4)) {
            *slot = Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3]);
        }
        Self::new(
            u16::from_be_bytes([b[0], b[1]]),
            u64::from_be_bytes(limit),
            &addresses[..usize::from(b[2])],
        )
    }
}
