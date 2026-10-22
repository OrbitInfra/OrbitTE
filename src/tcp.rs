use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::engine::{PreparedTransfer, Source, deadline, remaining};
use crate::memory::checked_range;
use crate::{
    EngineConfig, Error, Notification, NotificationEffect, NotificationFailure, Permission,
    RemoteEffect, RemoteRegion, RemoteSegment, Result, SourceEpoch, TransferOp, Transport,
};

const MAGIC: &[u8; 8] = b"ORBITTE\0";
const VERSION: u16 = 5;
const REQUEST_SIZE: usize = 52;
const REPLY_SIZE: usize = 36;
const MAX_REGIONS: usize = 4096;
const REGION_SIZE: usize = 32;
const IO_POLL: Duration = Duration::from_millis(25);

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
enum Operation {
    Describe = 0,
    Read = 1,
    Write = 2,
    Notify = 3,
    RdmaRead = 4,
    RdmaWrite = 5,
}

struct Request {
    operation: Operation,
    epoch: SourceEpoch,
    region: u64,
    offset: u64,
    length: u64,
}

impl Request {
    fn encode(&self) -> [u8; REQUEST_SIZE] {
        let mut bytes = [0; REQUEST_SIZE];
        bytes[..8].copy_from_slice(MAGIC);
        bytes[8..10].copy_from_slice(&VERSION.to_be_bytes());
        bytes[10] = self.operation as u8;
        bytes[12..28].copy_from_slice(&self.epoch.0);
        bytes[28..36].copy_from_slice(&self.region.to_be_bytes());
        bytes[36..44].copy_from_slice(&self.offset.to_be_bytes());
        bytes[44..52].copy_from_slice(&self.length.to_be_bytes());
        bytes
    }

    fn decode(bytes: &[u8; REQUEST_SIZE]) -> Result<Self> {
        validate_prefix(bytes)?;
        let operation = match bytes[10] {
            0 => Operation::Describe,
            1 => Operation::Read,
            2 => Operation::Write,
            3 => Operation::Notify,
            4 => Operation::RdmaRead,
            5 => Operation::RdmaWrite,
            _ => return Err(Error::Protocol("unknown operation")),
        };
        let request = Self {
            operation,
            epoch: epoch_at(bytes),
            region: u64_at(bytes, 28),
            offset: u64_at(bytes, 36),
            length: u64_at(bytes, 44),
        };
        if operation == Operation::Describe
            && (request.region != 0 || request.offset != 0 || request.length != 0)
        {
            return Err(Error::Protocol(
                "descriptor request contains a memory range",
            ));
        }
        if operation == Operation::Notify && request.region != 0 {
            return Err(Error::Protocol("notification contains a registration ID"));
        }
        Ok(request)
    }
}

fn validate_prefix(bytes: &[u8]) -> Result<()> {
    if &bytes[..8] != MAGIC || u16::from_be_bytes([bytes[8], bytes[9]]) != VERSION || bytes[11] != 0
    {
        return Err(Error::Protocol("magic, version or reserved flags"));
    }
    Ok(())
}

fn epoch_at(bytes: &[u8]) -> SourceEpoch {
    let mut epoch = [0; 16];
    epoch.copy_from_slice(&bytes[12..28]);
    SourceEpoch(epoch)
}

fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    let mut value = [0; 8];
    value.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_be_bytes(value)
}

struct Reply {
    status: u8,
    epoch: SourceEpoch,
    length: u64,
}

impl Reply {
    fn encode(&self) -> [u8; REPLY_SIZE] {
        let mut bytes = [0; REPLY_SIZE];
        bytes[..8].copy_from_slice(MAGIC);
        bytes[8..10].copy_from_slice(&VERSION.to_be_bytes());
        bytes[10] = self.status;
        bytes[12..28].copy_from_slice(&self.epoch.0);
        bytes[28..36].copy_from_slice(&self.length.to_be_bytes());
        bytes
    }

    fn read(channel: &mut Channel<'_>, expected: SourceEpoch) -> Result<Self> {
        let mut bytes = [0; REPLY_SIZE];
        channel.read_exact(&mut bytes)?;
        validate_prefix(&bytes)?;
        let reply = Self {
            status: bytes[10],
            epoch: epoch_at(&bytes),
            length: u64_at(&bytes, 28),
        };
        if reply.epoch != expected {
            return Err(Error::EpochMismatch);
        }
        if reply.status != 0 && reply.length != 0 {
            return Err(Error::Protocol("error/ready reply contains payload"));
        }
        match reply.status {
            0 | 9 => Ok(reply),
            1 => Err(Error::EpochMismatch),
            2 => Err(Error::RegionNotFound),
            3 => Err(Error::PermissionDenied),
            4 => Err(Error::InvalidRange),
            5 => Err(Error::SourceClosed),
            6 => Err(Error::ResourceExhausted),
            7 => Err(Error::Protocol("request rejected by source")),
            8 => Err(Error::Internal("source handler failed")),
            10 => Err(Error::Unavailable("remote RDMA is not configured/compiled")),
            11 => Err(Error::Backend {
                operation: "remote backend",
                code: -1,
            }),
            _ => Err(Error::Protocol("unknown reply status")),
        }
    }
}

fn error_status(error: &Error) -> u8 {
    match error {
        Error::EpochMismatch => 1,
        Error::RegionNotFound => 2,
        Error::PermissionDenied => 3,
        Error::InvalidRange => 4,
        Error::SourceClosed => 5,
        Error::ResourceExhausted => 6,
        Error::Protocol(_) => 7,
        Error::Unavailable(_) => 10,
        Error::Backend { .. } | Error::Unquiesced { .. } => 11,
        _ => 8,
    }
}

fn buffer(length: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| Error::ResourceExhausted)?;
    bytes.resize(length, 0);
    Ok(bytes)
}

pub(crate) fn open_segment(
    endpoint: SocketAddr,
    epoch: SourceEpoch,
    deadline: Instant,
) -> Result<RemoteSegment> {
    let mut channel = Channel::connect(endpoint, deadline)?;
    channel.write_all(
        &Request {
            operation: Operation::Describe,
            epoch,
            region: 0,
            offset: 0,
            length: 0,
        }
        .encode(),
    )?;
    let reply = Reply::read(&mut channel, epoch)?;
    if reply.status != 0
        || reply.length < 4
        || reply.length > (4 + MAX_REGIONS * REGION_SIZE) as u64
    {
        return Err(Error::Protocol("descriptor payload length"));
    }
    let mut bytes = buffer(reply.length as usize)?;
    channel.read_exact(&mut bytes)?;
    let count = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    if count > MAX_REGIONS || bytes.len() != 4 + count * REGION_SIZE {
        return Err(Error::Protocol("descriptor count"));
    }
    let mut regions = Vec::new();
    regions
        .try_reserve_exact(count)
        .map_err(|_| Error::ResourceExhausted)?;
    let mut previous = 0;
    for entry in bytes[4..].chunks_exact(REGION_SIZE) {
        let id = u64_at(entry, 0);
        let address = u64_at(entry, 8);
        let length = usize::try_from(u64_at(entry, 16))
            .map_err(|_| Error::Protocol("region length overflows host"))?;
        if id <= previous
            || address == 0
            || length == 0
            || address.checked_add(length as u64).is_none()
            || entry[25..].iter().any(|b| *b != 0)
        {
            return Err(Error::Protocol("invalid or unordered region descriptor"));
        }
        regions.push(RemoteRegion {
            epoch,
            id,
            address,
            length,
            permission: Permission::decode(entry[24])?,
        });
        previous = id;
    }
    Ok(RemoteSegment {
        endpoint,
        epoch,
        regions,
    })
}

pub(crate) fn notify(
    peer: &RemoteSegment,
    notification: &Notification,
    deadline: Instant,
) -> std::result::Result<(), NotificationFailure> {
    let mut effect = NotificationEffect::None;
    let result = (|| {
        let mut channel = Channel::connect(peer.endpoint, deadline)?;
        channel.write_all(
            &Request {
                operation: Operation::Notify,
                epoch: peer.epoch,
                region: 0,
                offset: notification.name.len() as u64,
                length: notification.message.len() as u64,
            }
            .encode(),
        )?;
        let ready = Reply::read(&mut channel, peer.epoch)?;
        if ready.status != 9 {
            return Err(Error::Protocol("notification missing ready response"));
        }
        effect = NotificationEffect::Unknown;
        channel.write_all(notification.name.as_bytes())?;
        channel.write_all(notification.message.as_bytes())?;
        // Even empty strings require a body byte before the receiver may commit.
        channel.write_all(&[0])?;
        let receipt = Reply::read(&mut channel, peer.epoch)?;
        if receipt.status != 0 || receipt.length != 0 {
            return Err(Error::Protocol(
                "notification acknowledgement length/status",
            ));
        }
        Ok(())
    })();
    result.map_err(|error| NotificationFailure { error, effect })
}

pub(crate) fn transfer(
    operation: TransferOp,
    peer: &RemoteSegment,
    request: &PreparedTransfer,
    source: &Source,
    deadline: Instant,
) -> std::result::Result<(), (Error, RemoteEffect)> {
    if request.transport == Transport::Rdma {
        #[cfg(feature = "rdma")]
        return rdma_transfer(operation, peer, request, source, deadline);
        #[cfg(not(feature = "rdma"))]
        return Err((
            Error::Unavailable("build with --features rdma"),
            RemoteEffect::None,
        ));
    }
    let _ = source;
    let mut effect = RemoteEffect::None;
    let result = (|| {
        let mut bytes = buffer(request.length)?;
        if operation == TransferOp::Write {
            request.local.read(request.local_offset, &mut bytes)?;
        }
        let mut channel = Channel::connect(peer.endpoint, deadline)?;
        let operation_code = if operation == TransferOp::Read {
            Operation::Read
        } else {
            Operation::Write
        };
        channel.write_all(
            &Request {
                operation: operation_code,
                epoch: peer.epoch,
                region: request.remote.id,
                offset: request.remote_offset as u64,
                length: request.length as u64,
            }
            .encode(),
        )?;
        let reply = Reply::read(&mut channel, peer.epoch)?;
        if operation == TransferOp::Read {
            if reply.status != 0 || reply.length != request.length as u64 {
                return Err(Error::Protocol("READ payload length/status"));
            }
            channel.read_exact(&mut bytes)?;
            remaining(deadline)?;
            request.local.write(request.local_offset, &bytes)?;
        } else {
            if reply.status != 9 {
                return Err(Error::Protocol("WRITE missing ready response"));
            }
            effect = RemoteEffect::Unknown;
            channel.write_all(&bytes)?;
            let reply = Reply::read(&mut channel, peer.epoch)?;
            if reply.status != 0 || reply.length != 0 {
                return Err(Error::Protocol("WRITE acknowledgement length/status"));
            }
        }
        Ok(())
    })();
    result.map_err(|error| (error, effect))
}

#[cfg(feature = "rdma")]
fn rdma_transfer(
    operation: TransferOp,
    peer: &RemoteSegment,
    request: &PreparedTransfer,
    source: &Source,
    deadline: Instant,
) -> std::result::Result<(), (Error, RemoteEffect)> {
    use crate::rdma::{DESCRIPTOR_SIZE, Descriptor};
    let mut effect = RemoteEffect::None;
    let result = (|| {
        let runtime = source
            .rdma
            .as_ref()
            .ok_or(Error::Unavailable("RDMA is not configured"))?;
        let mut endpoint = runtime.endpoint(
            &request.local,
            request.local_offset,
            request.length,
            0,
            deadline,
        )?;
        let transfer = (|| {
            if operation == TransferOp::Write {
                endpoint.fill(&request.local, request.local_offset)?;
            }
            let mut channel = Channel::connect(peer.endpoint, deadline)?;
            channel.write_all(
                &Request {
                    operation: if operation == TransferOp::Read {
                        Operation::RdmaRead
                    } else {
                        Operation::RdmaWrite
                    },
                    epoch: peer.epoch,
                    region: request.remote.id,
                    offset: request.remote_offset as u64,
                    length: request.length as u64,
                }
                .encode(),
            )?;
            let reply = Reply::read(&mut channel, peer.epoch)?;
            if reply.status != 0 || reply.length != DESCRIPTOR_SIZE as u64 {
                return Err(Error::Protocol("RDMA bootstrap length/status"));
            }
            let mut descriptor = [0; DESCRIPTOR_SIZE];
            channel.read_exact(&mut descriptor)?;
            let remote = Descriptor::decode(&descriptor)?;
            channel.write_all(&endpoint.descriptor().encode())?;
            endpoint.connect(&remote)?;
            let ready = Reply::read(&mut channel, peer.epoch)?;
            if ready.status != 9 {
                return Err(Error::Protocol("RDMA missing ready response"));
            }
            // A peer may expose direct application backing. Once a WRITE is
            // attempted it can change remote bytes even without DONE or ACK.
            if operation == TransferOp::Write {
                effect = RemoteEffect::Unknown;
            }
            endpoint.transfer(&remote, operation, deadline)?;
            endpoint.close()?;
            channel.write_all(&[1])?;
            let receipt = Reply::read(&mut channel, peer.epoch)?;
            if receipt.status != 0 || receipt.length != 0 {
                return Err(Error::Protocol("RDMA completion acknowledgement"));
            }
            Ok(())
        })();
        #[cfg(test)]
        crate::rdma::tests::before_requester_close(&mut endpoint, &transfer);
        endpoint.close()?;
        transfer?;
        if operation == TransferOp::Read {
            remaining(deadline)?;
            endpoint.copy_back(&request.local, request.local_offset)?;
        }
        Ok(())
    })();
    result.map_err(|error| (error, effect))
}

#[cfg(feature = "rdma")]
fn rdma_serve(
    channel: &mut Channel<'_>,
    source: &Source,
    request: &Request,
    memory: &crate::RegisteredMemory,
    offset: usize,
    length: usize,
) -> Result<()> {
    use crate::rdma::{DESCRIPTOR_SIZE, Descriptor};
    let runtime = source
        .rdma
        .as_ref()
        .ok_or(Error::Unavailable("RDMA is not configured"))?;
    let writing = request.operation == Operation::RdmaWrite;
    let mut endpoint = runtime.endpoint(
        memory,
        offset,
        length,
        if writing { 2 } else { 1 },
        channel.deadline,
    )?;
    let transfer = (|| {
        if !writing {
            endpoint.fill(memory, offset)?;
        }
        channel.reply(source, 0, DESCRIPTOR_SIZE as u64)?;
        channel.write_all(&endpoint.descriptor().encode())?;
        let mut bytes = [0; DESCRIPTOR_SIZE];
        channel.read_exact(&mut bytes)?;
        endpoint.connect(&Descriptor::decode(&bytes)?)?;
        channel.reply(source, 9, 0)?;
        let mut done = [0];
        channel.read_exact(&mut done)?;
        if done != [1] {
            return Err(Error::Protocol("RDMA completion marker"));
        }
        Ok(())
    })();
    endpoint.close()?;
    transfer?;
    if writing {
        remaining(channel.deadline)?;
        endpoint.copy_back(memory, offset)?;
    }
    channel.reply(source, 0, 0)
}

struct Channel<'a> {
    stream: TcpStream,
    deadline: Instant,
    stopping: Option<&'a AtomicBool>,
}

impl Channel<'_> {
    fn connect(endpoint: SocketAddr, deadline: Instant) -> Result<Self> {
        let stream = TcpStream::connect_timeout(&endpoint, remaining(deadline)?)?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            deadline,
            stopping: None,
        })
    }

    fn poll_timeout(&self) -> Result<Duration> {
        if self
            .stopping
            .is_some_and(|flag| flag.load(Ordering::Acquire))
        {
            return Err(Error::SourceClosed);
        }
        Ok(remaining(self.deadline)?.min(IO_POLL))
    }

    fn read_exact(&mut self, mut bytes: &mut [u8]) -> Result<()> {
        while !bytes.is_empty() {
            self.stream.set_read_timeout(Some(self.poll_timeout()?))?;
            match self.stream.read(bytes) {
                Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into()),
                Ok(count) => bytes = &mut bytes[count..],
                Err(error) if retryable(&error) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    fn write_all(&mut self, mut bytes: &[u8]) -> Result<()> {
        while !bytes.is_empty() {
            self.stream.set_write_timeout(Some(self.poll_timeout()?))?;
            match self.stream.write(bytes) {
                Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero).into()),
                Ok(count) => bytes = &bytes[count..],
                Err(error) if retryable(&error) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    fn reply(&mut self, source: &Source, status: u8, length: u64) -> Result<()> {
        self.write_all(
            &Reply {
                epoch: source.epoch,
                status,
                length,
            }
            .encode(),
        )
    }
}

fn retryable(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    )
}

pub(crate) struct TcpTransport {
    endpoint: SocketAddr,
    stopping: Arc<AtomicBool>,
    acceptor: Option<JoinHandle<()>>,
    workers: Vec<JoinHandle<()>>,
}

impl TcpTransport {
    pub(crate) fn start(source: Arc<Source>, config: &EngineConfig) -> Result<Self> {
        let listener = TcpListener::bind(config.bind_addr)?;
        listener.set_nonblocking(true)?;
        let endpoint = listener.local_addr()?;
        let stopping = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = mpsc::sync_channel::<(TcpStream, Instant)>(config.handler_threads);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut transport = Self {
            endpoint,
            stopping,
            acceptor: None,
            workers: Vec::new(),
        };
        for index in 0..config.handler_threads {
            let receiver = Arc::clone(&receiver);
            let source = Arc::clone(&source);
            let stopping = Arc::clone(&transport.stopping);
            let max_bytes = config.max_transfer_bytes;
            let worker = thread::Builder::new()
                .name(format!("orbitte-tcp-{index}"))
                .spawn(move || {
                    loop {
                        if stopping.load(Ordering::Acquire) {
                            return;
                        }
                        let request = match receiver.lock() {
                            Ok(queue) => queue.recv_timeout(IO_POLL),
                            Err(_) => return,
                        };
                        let (stream, deadline) = match request {
                            Ok(request) => request,
                            Err(mpsc::RecvTimeoutError::Timeout) => continue,
                            Err(mpsc::RecvTimeoutError::Disconnected) => return,
                        };
                        if stopping.load(Ordering::Acquire) {
                            return;
                        }
                        let mut channel = Channel {
                            stream,
                            deadline,
                            stopping: Some(&stopping),
                        };
                        // A failed connection is isolated and closed, never replayed.
                        let _ = serve(&mut channel, &source, max_bytes);
                    }
                })?;
            transport.workers.push(worker);
        }
        let stopping = Arc::clone(&transport.stopping);
        let timeout = config.handler_timeout;
        transport.acceptor = Some(thread::Builder::new().name("orbitte-accept".into()).spawn(
            move || {
                while !stopping.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let Ok(deadline) = deadline(timeout) else {
                                continue;
                            };
                            // The queue and worker count bound sockets and payloads.
                            let _ = sender.try_send((stream, deadline));
                        }
                        Err(error) if retryable(&error) => thread::sleep(Duration::from_millis(2)),
                        Err(_) => break,
                    }
                }
            },
        )?);
        Ok(transport)
    }

    pub(crate) fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }
}

impl Drop for TcpTransport {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        if let Some(acceptor) = self.acceptor.take() {
            let _ = acceptor.join();
        }
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

fn serve(channel: &mut Channel<'_>, source: &Arc<Source>, max_bytes: usize) -> Result<()> {
    let mut bytes = [0; REQUEST_SIZE];
    channel.read_exact(&mut bytes)?;
    let request = match Request::decode(&bytes) {
        Ok(request) => request,
        Err(error) => {
            channel.reply(source, error_status(&error), 0)?;
            return Ok(());
        }
    };
    if request.epoch != source.epoch {
        channel.reply(source, 1, 0)?;
        return Ok(());
    }
    let admission = match source.admit() {
        Ok(admission) => admission,
        Err(error) => {
            channel.reply(source, error_status(&error), 0)?;
            return Ok(());
        }
    };
    let mut retained_memory = None;
    let prepared = (|| {
        if request.operation == Operation::Describe {
            let regions = source.registry.descriptors()?;
            let mut payload = buffer(4 + regions.len() * REGION_SIZE)?;
            payload[..4].copy_from_slice(&(regions.len() as u32).to_be_bytes());
            for (region, entry) in regions
                .iter()
                .zip(payload[4..].chunks_exact_mut(REGION_SIZE))
            {
                entry[..8].copy_from_slice(&region.id.to_be_bytes());
                entry[8..16].copy_from_slice(&region.address.to_be_bytes());
                entry[16..24].copy_from_slice(&(region.length as u64).to_be_bytes());
                entry[24] = region.permission as u8;
            }
            channel.reply(source, 0, payload.len() as u64)?;
            channel.write_all(&payload)?;
            return Ok(());
        }
        if request.operation == Operation::Notify {
            let name_length =
                usize::try_from(request.offset).map_err(|_| Error::ResourceExhausted)?;
            let message_length =
                usize::try_from(request.length).map_err(|_| Error::ResourceExhausted)?;
            let reservation = source.notifications.reserve(name_length, message_length)?;
            let mut name = buffer(name_length)?;
            let mut message = buffer(message_length)?;
            channel.reply(source, 9, 0)?;
            channel.read_exact(&mut name)?;
            channel.read_exact(&mut message)?;
            let mut marker = [0; 1];
            channel.read_exact(&mut marker)?;
            if marker != [0] {
                return Err(Error::Protocol("invalid notification commit marker"));
            }
            let name = String::from_utf8(name)
                .map_err(|_| Error::Protocol("notification name is not UTF-8"))?;
            let message = String::from_utf8(message)
                .map_err(|_| Error::Protocol("notification message is not UTF-8"))?;
            remaining(channel.deadline)?;
            reservation.commit(Notification { name, message })?;
            channel.reply(source, 0, 0)?;
            return Ok(());
        }
        let offset = usize::try_from(request.offset).map_err(|_| Error::InvalidRange)?;
        let length = usize::try_from(request.length).map_err(|_| Error::InvalidRange)?;
        if length > max_bytes {
            return Err(Error::ResourceExhausted);
        }
        let memory = source.registry.get(request.region)?;
        checked_range(offset, length, memory.len())?;
        if !memory.0.descriptor.permission.allows(matches!(
            request.operation,
            Operation::Write | Operation::RdmaWrite
        )) {
            return Err(Error::PermissionDenied);
        }
        retained_memory = Some(memory);
        let memory = retained_memory
            .as_ref()
            .ok_or(Error::Internal("missing source memory"))?;
        if matches!(
            request.operation,
            Operation::RdmaRead | Operation::RdmaWrite
        ) {
            #[cfg(feature = "rdma")]
            return rdma_serve(channel, source, &request, memory, offset, length);
            #[cfg(not(feature = "rdma"))]
            return Err(Error::Unavailable("build with --features rdma"));
        }
        let mut payload = buffer(length)?;
        if request.operation == Operation::Read {
            memory.read(offset, &mut payload)?;
            channel.reply(source, 0, length as u64)?;
            channel.write_all(&payload)?;
        } else {
            channel.reply(source, 9, 0)?;
            channel.read_exact(&mut payload)?;
            remaining(channel.deadline)?;
            memory.write(offset, &payload)?;
            channel.reply(source, 0, 0)?;
        }
        Ok(())
    })();
    if let Err(error) = prepared {
        if matches!(error, Error::Unquiesced { .. }) {
            // A native failure cannot release source admission or its actual
            // registered backing. Source close remains pending, even after EOF.
            std::mem::forget((retained_memory, Arc::clone(source), admission));
        }
        // After an I/O error the connection cannot safely carry another frame.
        if !matches!(error, Error::Io(_) | Error::Timeout | Error::SourceClosed) {
            channel.reply(source, error_status(&error), 0)?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/unit/tcp.rs"]
mod tests;
