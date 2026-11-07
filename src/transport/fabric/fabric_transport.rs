//! Native epoch-scoped grants around TENT's host RMA and domain progress model.
use super::{
    FabricConfig, FabricStats,
    fabric_attrs::{self as attrs, BufferAttr, Chunk, PeerAttr},
    fabric_context::{Context, Counters, Loan, Mr, Op, Task},
    sys,
};
use crate::{
    Error, MemoryType, RegisteredMemory, RemoteEffect, RemoteSegment, Result, SourceEpoch,
    TransferOp,
    engine::PreparedTransfer,
    runtime::{
        control_plane::{ControlChannel, Operation, Reply, Request},
        segment_manager::PeerKey,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread::JoinHandle,
    time::Instant,
};

pub(crate) struct FabricTransport {
    config: FabricConfig,
    pub(super) contexts: Vec<Arc<Context>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    pub peer: PeerAttr,
    counters: Arc<Counters>,
    nonce: AtomicU64,
    cursor: AtomicUsize,
    stopped: AtomicBool,
}
impl FabricTransport {
    pub(crate) fn new(config: FabricConfig) -> Result<Arc<Self>> {
        Self::with_api(config, sys::Api::load()?)
    }
    pub(super) fn with_api(config: FabricConfig, api: Arc<sys::Api>) -> Result<Arc<Self>> {
        config.validate()?;
        let list = api.discover(&config)?;
        let counters = Arc::new(Counters::default());
        let mut domains = BTreeMap::new();
        for (ptr, info) in list.entries()? {
            let name = sys::string(&info.name)?;
            if !config.devices.is_empty()
                && !config
                    .devices
                    .iter()
                    .any(|d| *d == name || format!("{d}-rdm") == name)
            {
                continue;
            }
            match domains.get(&name) {
                Some((_, prev))
                    if {
                        let prev: &sys::Info = prev;
                        prev.ipv4 != 0 || info.ipv4 == 0
                    } => {}
                _ => {
                    domains.insert(name, (ptr, info));
                }
            }
        }
        if config.devices.is_empty() && domains.len() > 1 {
            domains.remove("lo");
        }
        if domains.len() > config.max_domains {
            return Err(Error::ResourceExhausted);
        }
        let mut contexts = Vec::new();
        for (_, (ptr, info)) in domains {
            match Context::open(api.clone(), ptr, info, &config, counters.clone()) {
                Ok(c) => contexts.push(c),
                Err(e @ Error::Unquiesced { .. }) => return Err(e),
                Err(e) => log::warn!("libfabric domain unavailable: {e}"),
            }
        }
        let first = contexts
            .first()
            .ok_or(Error::Unavailable("no eligible libfabric domain"))?;
        let virt_addr = first.info.virt_addr != 0;
        if contexts
            .iter()
            .any(|c| (c.info.virt_addr != 0) != virt_addr)
        {
            return Err(Error::Unavailable(
                "inconsistent libfabric MR address modes",
            ));
        }
        let peer = PeerAttr {
            version: 1,
            provider: if config.tcp() { "tcp;ofi_rxm" } else { "efa" }.into(),
            virt_addr,
            nics: contexts.iter().map(|c| c.nic.clone()).collect(),
        };
        let owner = Arc::new(Self {
            config,
            contexts,
            workers: Mutex::new(Vec::new()),
            peer,
            counters,
            nonce: AtomicU64::new(1),
            cursor: AtomicUsize::new(0),
            stopped: AtomicBool::new(false),
        });
        for (i, c) in owner.contexts.iter().enumerate() {
            owner
                .workers
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(c.worker(i)?);
        }
        Ok(owner)
    }
    pub(crate) fn stats(&self) -> FabricStats {
        self.counters.stats()
    }
    pub(crate) fn close_peer(&self, peer: PeerKey) -> Result<()> {
        let mut error = None;
        for c in &self.contexts {
            if let Err(e) = c.close_peer(peer) {
                error.get_or_insert(e);
            }
        }
        error.map_or(Ok(()), Err)
    }
    pub(crate) fn close(&self) -> Result<()> {
        self.stopped.store(true, Ordering::Release);
        for c in &self.contexts {
            c.stop();
        }
        let mut workers = self.workers.lock().unwrap_or_else(|p| p.into_inner());
        let mut error = None;
        for worker in workers.drain(..) {
            if worker.join().is_err() {
                error.get_or_insert(sys::uncertain("fabric worker terminated", libc::EIO));
            }
        }
        for c in &self.contexts {
            if let Err(e) = c.close() {
                error.get_or_insert(e);
            }
        }
        error.map_or(Ok(()), Err)
    }
    pub(crate) fn ready(&self) -> Result<()> {
        if self.stopped.load(Ordering::Acquire) {
            Err(Error::Unavailable("libfabric transport is closed"))
        } else {
            Ok(())
        }
    }
}
impl Drop for FabricTransport {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            log::error!("libfabric retained cleanup: {e}");
        }
    }
}

struct Buffer {
    loan: Arc<Loan>,
    attr: BufferAttr,
    mrs: Vec<Vec<(usize, Arc<Mr>)>>,
}
impl Buffer {
    fn create(
        owner: &FabricTransport,
        memory: &RegisteredMemory,
        offset: usize,
        length: usize,
        remote: i32,
    ) -> Result<Self> {
        owner.ready()?;
        if memory.memory_type() != MemoryType::Cpu {
            return Err(Error::Unavailable("libfabric requires host DRAM"));
        }
        let loan = Arc::new(Loan(memory.dma_lease(offset, length)?));
        // Base pages conservatively bound EFA PTEs, including offsets into a page.
        let pte = if owner.config.tcp() {
            0
        } else {
            owner.config.max_pte_entries
        };
        let pte_limit = if pte == 0 {
            u64::MAX
        } else {
            pte.saturating_sub(1)
                .checked_mul(4096)
                .ok_or(Error::InvalidRange)?
        };
        let limit = if owner.config.max_mr_size == 0 {
            pte_limit
        } else {
            (owner.config.max_mr_size as u64).min(pte_limit)
        };
        if limit == 0 {
            return Err(Error::ResourceExhausted);
        }
        let ranges = attrs::plan_chunks(length as u64, limit)?;
        let assignments = attrs::assign_nics(&ranges, owner.contexts.len(), 4096, pte)?;
        let jobs: Vec<_> = ranges
            .iter()
            .enumerate()
            .flat_map(|(i, r)| assignments[i].iter().map(move |&n| (i, n, *r)))
            .collect();
        if jobs.len() > attrs::MAX_CHUNKS {
            return Err(Error::ResourceExhausted);
        }
        let next = AtomicUsize::new(0);
        let output = Mutex::new(Vec::with_capacity(jobs.len()));
        // TENT bounds concurrent registration per buffer. Each scoped thread is
        // joined before publication or rollback, and every result has one owner.
        let mut thread_error = None;
        std::thread::scope(|scope| {
            let mut workers = Vec::new();
            for _ in 0..owner.config.max_register_threads.min(jobs.len()) {
                let next = &next;
                let output = &output;
                let jobs = &jobs;
                let loan = &loan;
                match std::thread::Builder::new()
                    .name("orbite-fabric-register".into())
                    .spawn_scoped(scope, move || {
                        loop {
                            let i = next.fetch_add(1, Ordering::Relaxed);
                            let Some(&(chunk, nic, r)) = jobs.get(i) else {
                                break;
                            };
                            let result = owner.contexts[nic].register(
                                loan.clone(),
                                r.off as usize,
                                r.len as usize,
                                remote,
                            );
                            output
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .push((chunk, nic, result));
                        }
                    }) {
                    Ok(w) => workers.push(w),
                    Err(e) => {
                        thread_error = Some(Error::Io(e));
                        break;
                    }
                }
            }
            for worker in workers {
                if worker.join().is_err() {
                    thread_error = Some(Error::Internal("fabric registration worker terminated"));
                }
            }
        });
        let mut results = output.into_inner().unwrap_or_else(|p| p.into_inner());
        let mut buffer = Self {
            loan,
            attr: BufferAttr {
                chunks: ranges
                    .iter()
                    .map(|r| Chunk {
                        off: r.off,
                        len: r.len,
                        nics: Vec::new(),
                        keys: Vec::new(),
                    })
                    .collect(),
            },
            mrs: vec![Vec::new(); ranges.len()],
        };
        let mut error = thread_error;
        results.sort_by_key(|(c, n, _)| (*c, *n));
        for (i, n, result) in results {
            match result {
                Ok(mr) => {
                    buffer.attr.chunks[i].nics.push(n);
                    buffer.attr.chunks[i].keys.push(mr.key);
                    buffer.mrs[i].push((n, mr));
                }
                Err(e) => {
                    if error.is_none() || matches!(e, Error::Unquiesced { .. }) {
                        error = Some(e);
                    }
                }
            }
        }
        if let Some(e) = error {
            buffer.close()?;
            return Err(e);
        }
        Ok(buffer)
    }
    fn close(&self) -> Result<()> {
        let mut error = None;
        for (_, mr) in self.mrs.iter().flatten() {
            if let Err(e) = mr.close() {
                error.get_or_insert(e);
            }
        }
        error.map_or(Ok(()), Err)
    }
}

#[derive(Serialize, Deserialize)]
struct Grant {
    peer: PeerAttr,
    buffer: BufferAttr,
    address: u64,
    nonce: u64,
}
fn read_grant(
    channel: &mut ControlChannel<'_>,
    epoch: SourceEpoch,
    length: usize,
) -> Result<Grant> {
    let reply = Reply::read(channel, epoch)?;
    if reply.status != 0 || reply.length == 0 || reply.length > attrs::MAX_WIRE as u64 {
        return Err(Error::Protocol("fabric grant length"));
    }
    let mut bytes = vec![0; reply.length as usize];
    channel.read_exact(&mut bytes)?;
    let grant: Grant =
        serde_json::from_slice(&bytes).map_err(|_| Error::Protocol("fabric grant"))?;
    PeerAttr::decode(&grant.peer.encode()?)?;
    grant
        .buffer
        .validate(length as u64, grant.peer.nics.len())?;
    if grant.nonce == 0
        || (grant.peer.virt_addr && grant.address == 0)
        || (!grant.peer.virt_addr && grant.address != 0)
        || grant.address.checked_add(length as u64).is_none()
    {
        return Err(Error::Protocol("fabric grant range"));
    }
    Ok(grant)
}
fn finish(
    channel: &mut ControlChannel<'_>,
    epoch: SourceEpoch,
    nonce: u64,
    length: usize,
    unposted: bool,
) -> Result<()> {
    let mut bytes = [0; 24];
    bytes[..8].copy_from_slice(&nonce.to_be_bytes());
    bytes[8] = u8::from(unposted);
    bytes[16..].copy_from_slice(&(if unposted { 0 } else { length as u64 }).to_be_bytes());
    channel.write_all(&bytes)?;
    let reply = Reply::read(channel, epoch)?;
    if reply.status != 0 || reply.length != 0 {
        return Err(Error::Protocol("fabric release receipt"));
    }
    Ok(())
}
impl FabricTransport {
    pub(crate) fn serve(
        &self,
        channel: &mut ControlChannel<'_>,
        epoch: SourceEpoch,
        request: &Request,
        memory: &RegisteredMemory,
        offset: usize,
        length: usize,
    ) -> Result<()> {
        let write = request.operation == Operation::FabricWrite;
        let nonce = self
            .nonce
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| Error::ResourceExhausted)?;
        let buffer = Buffer::create(self, memory, offset, length, if write { 2 } else { 1 })?;
        let grant = Grant {
            peer: self.peer.clone(),
            buffer: buffer.attr.clone(),
            address: if self.peer.virt_addr {
                buffer.loan.address().as_ptr() as u64
            } else {
                0
            },
            nonce,
        };
        let payload = match attrs::encode(&grant) {
            Ok(v) => v,
            Err(e) => {
                buffer.close()?;
                return Err(e);
            }
        };
        let mut retained = Some(buffer);
        let result = (|| {
            channel.reply(epoch, 0, payload.len() as u64)?;
            channel.write_all(&payload)?;
            let mut receipt = [0; 24];
            channel.read_exact(&mut receipt)?;
            if receipt[..8] != nonce.to_be_bytes()
                || receipt[8] > 1
                || receipt[9..16] != [0; 7]
                || receipt[16..] != (if receipt[8] == 1 { 0 } else { length as u64 }).to_be_bytes()
            {
                return Err(Error::Protocol("fabric completion binding"));
            }
            let buffer = retained
                .take()
                .ok_or(Error::Internal("fabric source owner missing"))?;
            buffer.close()?;
            drop(buffer);
            channel.reply(epoch, 0, 0)
        })();
        if let Err(e) = result {
            // A grant may already be visible even when its final write failed.
            // EOF, timeout and cancel cannot revoke RMA access or free a loan.
            self.counters.retain();
            std::mem::forget(retained);
            log::warn!("libfabric source grant retained: {e}");
            return Err(sys::uncertain(
                "fabric source grant has no release receipt",
                libc::EIO,
            ));
        }
        Ok(())
    }
    pub(crate) fn transfer(
        &self,
        operation: TransferOp,
        peer: &RemoteSegment,
        request: &PreparedTransfer,
        until: Instant,
        posted: &mut bool,
    ) -> std::result::Result<(), (Error, RemoteEffect)> {
        let effect = |posted: bool| {
            if posted && operation == TransferOp::Write {
                RemoteEffect::Unknown
            } else {
                RemoteEffect::None
            }
        };
        let result = (|| {
            self.ready()?;
            let buffer = Buffer::create(
                self,
                &request.local,
                request.local_offset,
                request.length,
                0,
            )?;
            let handshake = (|| {
                let mut channel = ControlChannel::connect(peer.endpoint, until)?;
                channel.write_all(
                    &Request {
                        operation: if operation == TransferOp::Write {
                            Operation::FabricWrite
                        } else {
                            Operation::FabricRead
                        },
                        epoch: peer.epoch,
                        region: request.remote.id,
                        offset: request.remote_offset as u64,
                        length: request.length as u64,
                    }
                    .encode(),
                )?;
                let grant = read_grant(&mut channel, peer.epoch, request.length)?;
                Ok::<_, Error>((channel, grant))
            })();
            let (mut channel, grant) = match handshake {
                Ok(v) => v,
                Err(e) => {
                    buffer.close()?;
                    return Err(e);
                }
            };
            let prepared = self.prepare(
                peer.into(),
                operation,
                &buffer,
                &grant,
                request.length,
                until,
            );
            let tasks = match prepared {
                Ok(tasks) => tasks,
                Err(e) => {
                    if finish(&mut channel, peer.epoch, grant.nonce, request.length, true).is_err()
                    {
                        std::mem::forget(buffer);
                        return Err(sys::uncertain("fabric unposted grant release", libc::EIO));
                    }
                    buffer.close()?;
                    return Err(e);
                }
            };
            for (c, task) in &tasks {
                c.commit(task.clone());
            }
            let mut error = None;
            for (_, task) in &tasks {
                if let Err(e) = task.wait(posted) {
                    error.get_or_insert(e);
                }
            }
            if *posted && error.is_some() {
                // CQ errors prove local termination, not remote visibility. No
                // failed posted attempt grants permission to revoke or replay.
                self.counters.retain();
                std::mem::forget((buffer, tasks));
                return Err(sys::uncertain(
                    "fabric posted attempt failed or did not drain",
                    libc::EIO,
                ));
            }
            channel.deadline = Instant::now()
                .checked_add(self.config.quiesce())
                .ok_or(Error::InvalidRange)?;
            if finish(
                &mut channel,
                peer.epoch,
                grant.nonce,
                request.length,
                !*posted,
            )
            .is_err()
            {
                self.counters.retain();
                std::mem::forget((buffer, tasks));
                return Err(sys::uncertain(
                    "fabric remote release unconfirmed",
                    libc::EIO,
                ));
            }
            drop(tasks);
            buffer.close()?;
            error.map_or(Ok(()), Err)
        })();
        result.map_err(|e| (e, effect(*posted)))
    }
    fn prepare(
        &self,
        peer: PeerKey,
        operation: TransferOp,
        local: &Buffer,
        remote: &Grant,
        length: usize,
        until: Instant,
    ) -> Result<Vec<(Arc<Context>, Arc<Task>)>> {
        if remote.peer.provider != self.peer.provider
            || remote.peer.virt_addr != self.peer.virt_addr
        {
            return Err(Error::Unavailable(
                "fabric peer provider or MR mode differs",
            ));
        }
        let max_message = self
            .contexts
            .iter()
            .map(|c| c.info.max_message)
            .min()
            .ok_or(Error::Unavailable("fabric domain"))?;
        let spans = attrs::cut_spans(
            length as u64,
            0,
            &local.attr.ranges(),
            0,
            &remote.buffer.ranges(),
            max_message.min(self.config.slice_size as u64),
            self.config.max_pending_ops,
        )?;
        let mut groups: Vec<Vec<Op>> = (0..self.contexts.len()).map(|_| Vec::new()).collect();
        for span in spans {
            let choices = &local.mrs[span.local];
            let turn = self.cursor.fetch_add(1, Ordering::Relaxed);
            let (nic, mr) = &choices[turn % choices.len()];
            let remote_chunk = &remote.buffer.chunks[span.remote];
            let remote_nic = if remote_chunk.nics.contains(nic) {
                *nic
            } else {
                remote_chunk.nics[turn % remote_chunk.nics.len()]
            };
            let address = attrs::unhex(&remote.peer.nics[remote_nic].addr)?;
            let fi_addr = self.contexts[*nic].peer(peer, &address)?;
            let base = if remote.peer.virt_addr {
                remote.address + span.off
            } else {
                span.off - remote_chunk.off
            };
            groups[*nic].push(Op::new(
                mr.clone(),
                span.off as usize - mr.off,
                span.len as usize,
                fi_addr,
                base,
                remote_chunk
                    .key_for(remote_nic)
                    .ok_or(Error::Protocol("fabric missing remote NIC key"))?,
            )?);
        }
        let mut tasks = Vec::new();
        for (c, ops) in self.contexts.iter().zip(groups) {
            if !ops.is_empty() {
                tasks.push((c.clone(), c.prepare(ops, operation, peer, until)?));
            }
        }
        Ok(tasks)
    }
}
