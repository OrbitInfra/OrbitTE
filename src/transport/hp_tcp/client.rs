use super::{
    Endpoint, HpTcpConfig,
    protocol::{RESPONSE, Request, Response, Status},
    task::{self, STEP},
    workers::Workers,
};
use crate::{Error, RegisteredMemory, RemoteEffect, Result, SourceEpoch, TransferOp};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};
use tokio::{
    net::{TcpSocket, TcpStream},
    sync::{Mutex as AsyncMutex, watch},
};

#[derive(Clone, Copy, Hash, Eq, PartialEq, Debug)]
struct Key {
    peer: SourceEpoch,
    id: usize,
    remote: SocketAddrV4,
}
struct Lane {
    stream: AsyncMutex<Option<TcpStream>>,
    source: Option<Ipv4Addr>,
}
struct Entry {
    lane: Arc<Lane>,
    active: usize,
    last: Instant,
}
#[derive(Default)]
struct State {
    next: u64,
    rotation: HashMap<SourceEpoch, u64>,
    lanes: HashMap<Key, Entry>,
}
pub(super) struct Client {
    state: Arc<Mutex<State>>,
    config: HpTcpConfig,
}

struct Slice {
    key: Key,
    offset: usize,
    len: usize,
}
fn slices(
    sequence: u64,
    peer: SourceEpoch,
    end: Endpoint,
    op: TransferOp,
    len: usize,
    connections: usize,
) -> Vec<Slice> {
    let rails = end.rails();
    let n = if op == TransferOp::Read && len / rails.len() >= STEP {
        rails.len()
    } else {
        1
    };
    let mut offset = 0;
    (0..n)
        .map(|i| {
            let lane = if n == 1 {
                (sequence % connections as u64) as usize
            } else {
                let per = 1 + (connections - 1 - i) / n;
                i + (sequence % per as u64) as usize * n
            };
            let key = Key {
                peer,
                id: lane,
                remote: SocketAddrV4::new(rails[lane % rails.len()], end.port),
            };
            let length = len / n + usize::from(i < len % n);
            let piece = Slice {
                key,
                offset,
                len: length,
            };
            offset += length;
            piece
        })
        .collect()
}

pub(super) type Outcome = std::result::Result<(), (Error, RemoteEffect)>;
impl Client {
    pub(super) fn new(config: HpTcpConfig) -> Self {
        Self {
            state: Arc::default(),
            config,
        }
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) fn submit(
        &self,
        workers: &Workers,
        peer: SourceEpoch,
        end: Endpoint,
        frame: Request,
        mem: &RegisteredMemory,
        base: usize,
        deadline: Instant,
        cancel: Option<&watch::Sender<bool>>,
    ) -> Result<Receipt> {
        let len = usize::try_from(frame.length).map_err(|_| Error::InvalidRange)?;
        if self.config.rail_addresses.len().max(1) != end.rails().len()
            || frame.length > self.config.max_transfer_bytes.min(end.max_bytes)
        {
            return Err(Error::Unavailable(
                "HP-TCP peer rail count or transfer limit",
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("HP-TCP lane registry lock"))?;
        let id = state.next.checked_add(1).ok_or(Error::ResourceExhausted)?;
        let seq = *state.rotation.get(&peer).unwrap_or(&id);
        let pieces = slices(
            seq,
            peer,
            end,
            frame.op,
            len,
            self.config.connections_per_peer,
        );
        let missing = pieces
            .iter()
            .filter(|s| !state.lanes.contains_key(&s.key))
            .count();
        let max = self
            .config
            .max_outstanding_tasks
            .max(self.config.connections_per_peer as u64);
        if state.lanes.len().saturating_add(missing) as u64 > max {
            return Err(Error::ResourceExhausted);
        }
        state.next = id;
        state.rotation.insert(peer, seq.wrapping_add(1));
        let count = pieces.len();
        let (done, results) = mpsc::sync_channel(count);
        let siblings = watch::Sender::new(false);
        // All lanes and the owner's task/byte reservation are admitted before publishing its slices.
        for piece in pieces {
            let key = piece.key;
            let worker = key.id.wrapping_add(usize::from(peer.0[0]));
            let registry = self.state.clone();
            let idle = Duration::from_millis(self.config.idle_connection_timeout_ms);
            let entry = state.lanes.entry(key).or_insert_with(|| {
                let registry = registry.clone();
                workers.execute(worker, async move {
                    loop {
                        tokio::time::sleep(idle).await;
                        let mut state = registry.lock().unwrap_or_else(|p| p.into_inner());
                        let Some(entry) = state.lanes.get(&key) else {
                            break;
                        };
                        if entry.active == 0 && entry.last.elapsed() >= idle {
                            state.lanes.remove(&key);
                            if !state.lanes.keys().any(|k| k.peer == key.peer) {
                                state.rotation.remove(&key.peer);
                            }
                            break;
                        }
                    }
                });
                let source = self
                    .config
                    .rail_addresses
                    .get(key.id % end.rails().len())
                    .copied();
                Entry {
                    lane: Arc::new(Lane {
                        stream: AsyncMutex::default(),
                        source,
                    }),
                    active: 0,
                    last: Instant::now(),
                }
            });
            entry.active += 1;
            let lane = entry.lane.clone();
            let frame = Request {
                id,
                address: frame.address + piece.offset as u64,
                length: piece.len as u64,
                ..frame
            };
            let base = base + piece.offset;
            let memory = mem.clone();
            let done = done.clone();
            let siblings = siblings.clone();
            let mut sibling_cancel = siblings.subscribe();
            let public = cancel.map(watch::Sender::subscribe);
            let connect = Duration::from_millis(self.config.connect_timeout_ms);
            let progress = Duration::from_millis(self.config.progress_timeout_ms);
            workers.execute(worker, async move {
                let mut started = false;
                let outcome = tokio::select! {biased;
                    _=cancelled(public)=>Err(Error::Cancelled),
                    _=sibling_cancel.wait_for(|v|*v)=>Err(Error::Cancelled),
                    _=tokio::time::sleep_until(deadline.into())=>Err(Error::Timeout),
                    res=lane.transfer(key,connect,progress,frame,&memory,base,&mut started)=>res,
                };
                if outcome.is_err() {
                    siblings.send_replace(true);
                }
                drop(memory);
                {
                    let mut state = registry.lock().unwrap_or_else(|p| p.into_inner());
                    if let Some(entry) = state.lanes.get_mut(&key) {
                        entry.active -= 1;
                        entry.last = Instant::now();
                    }
                }
                let _ = done.send((outcome, started));
            });
        }
        drop(state);
        drop(done);
        Ok(Receipt {
            count,
            results,
            op: frame.op,
        })
    }
}

pub(crate) struct Receipt {
    count: usize,
    results: mpsc::Receiver<(Result<()>, bool)>,
    op: TransferOp,
}
impl Receipt {
    pub(crate) fn wait(self, posted: &mut bool) -> Outcome {
        let mut error = None;
        for _ in 0..self.count {
            let (res, started) = self.results.recv().map_err(|_| {
                (
                    Error::Unquiesced {
                        operation: "HP-TCP lane completion",
                        code: -1,
                    },
                    if self.op == TransferOp::Write {
                        RemoteEffect::Unknown
                    } else {
                        RemoteEffect::None
                    },
                )
            })?;
            *posted |= started;
            if let Err(e) = res {
                if error.is_none() || matches!(error, Some(Error::Cancelled)) {
                    error = Some(e);
                }
            }
        }
        error.map_or(Ok(()), |e| {
            Err((
                e,
                if *posted && self.op == TransferOp::Write {
                    RemoteEffect::Unknown
                } else {
                    RemoteEffect::None
                },
            ))
        })
    }
}

async fn cancelled(rx: Option<watch::Receiver<bool>>) {
    match rx {
        Some(mut rx) => {
            let _ = rx.wait_for(|v| *v).await;
        }
        None => std::future::pending().await,
    }
}

impl Lane {
    #[allow(clippy::too_many_arguments)]
    async fn transfer(
        &self,
        key: Key,
        connect: Duration,
        progress: Duration,
        frame: Request,
        mem: &RegisteredMemory,
        base: usize,
        posted: &mut bool,
    ) -> Result<()> {
        let mut slot = self.stream.lock().await;
        // On any incomplete frame (including future cancellation) drop the dirty stream.
        let mut socket = match slot.take() {
            Some(s) => s,
            None => {
                let socket = TcpSocket::new_v4()?;
                if let Some(ip) = self.source {
                    socket.bind(SocketAddrV4::new(ip, 0).into())?;
                }
                let s = tokio::time::timeout(connect, socket.connect(key.remote.into()))
                    .await
                    .map_err(|_| Error::Timeout)??;
                s.set_nodelay(true)?;
                s
            }
        };
        task::write(&mut socket, &frame.encode(), progress, posted).await?;
        let len = frame.length as usize;
        let mut buf = crate::runtime::control_plane::buffer(len.min(STEP))?;
        if frame.op == TransferOp::Write {
            let mut offset = 0;
            while offset < len {
                let n = (len - offset).min(buf.len());
                mem.read(base + offset, &mut buf[..n])?;
                task::write(&mut socket, &buf[..n], progress, posted).await?;
                offset += n;
            }
        }
        let mut bytes = [0; RESPONSE];
        task::read(&mut socket, &mut bytes, progress).await?;
        let reply = Response::decode(&bytes)?;
        if reply.id != frame.id {
            return Err(Error::Protocol("HP-TCP response request ID"));
        }
        if reply.status != Status::Ok {
            return Err(reply.status.error());
        }
        if reply.committed != frame.length {
            return Err(Error::Protocol("HP-TCP committed length"));
        }
        if frame.op == TransferOp::Read {
            let mut offset = 0;
            while offset < len {
                let n = (len - offset).min(buf.len());
                task::read(&mut socket, &mut buf[..n], progress).await?;
                mem.write(base + offset, &buf[..n])?;
                offset += n;
            }
        }
        *slot = Some(socket);
        Ok(())
    }
}
