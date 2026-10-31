use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Instant;

use crate::engine::remaining;

use super::{RdmaConfig, RdmaContext, RdmaStats, sys};
use crate::{Error, RegisteredMemory, Result};

pub(crate) struct LocalBufferManager {
    state: Mutex<Cache>,
    changed: Condvar,
    usage: Arc<Mutex<RdmaStats>>,
    max_regions: usize,
    max_bytes: usize,
    verbs: &'static sys::Verbs,
}

#[derive(Default)]
struct Cache {
    entries: Vec<Entry>,
    closing: usize,
    closed: bool,
    failure: Option<(&'static str, i32)>,
}

struct Entry {
    owner: Arc<MemoryRegion>,
    creating: bool,
    borrowers: usize,
    retiring: bool,
    published: bool,
}

struct MemoryRegion {
    context: Weak<RdmaContext>,
    key: Weak<crate::memory::Registration>,
    window_bind: bool,
    state: Mutex<State>,
    verbs: &'static sys::Verbs,
}

enum State {
    Open(Box<Resources>),
    Closed,
    Retained { code: i32 },
}

struct Resources {
    handles: sys::MemoryHandles,
    device: Arc<RdmaContext>,
    memory: RegisteredMemory,
    window_bind: bool,
    usage: Arc<Mutex<RdmaStats>>,
}

// Only the MR field is used by MemoryRegion; its mutex serializes provider calls.
unsafe impl Send for Resources {}

impl Drop for Resources {
    fn drop(&mut self) {
        let mut usage = self
            .usage
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        usage.persistent_source_mrs -= usize::from(self.window_bind);
        usage.persistent_mrs -= 1;
        usage.persistent_mr_bytes -= self.memory.len();
    }
}

pub(super) struct MrLease {
    cache: Arc<LocalBufferManager>,
    owner: Arc<MemoryRegion>,
    released: bool,
    created: bool,
}

impl LocalBufferManager {
    pub(super) fn new(
        config: &RdmaConfig,
        usage: Arc<Mutex<RdmaStats>>,
        verbs: &'static sys::Verbs,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::default(),
            changed: Condvar::new(),
            usage,
            max_regions: config.max_cached_mrs,
            max_bytes: config.max_cached_mr_bytes,
            verbs,
        })
    }

    pub(super) fn acquire(
        self: &Arc<Self>,
        device: Arc<RdmaContext>,
        memory: &RegisteredMemory,
        window_bind: bool,
        deadline: Instant,
    ) -> Result<MrLease> {
        self.acquire_inner(device, memory, window_bind, deadline, true)?
            .ok_or(Error::Internal("blocking MR acquisition returned pending"))
    }

    pub(super) fn poll_acquire(
        self: &Arc<Self>,
        device: Arc<RdmaContext>,
        memory: &RegisteredMemory,
        window_bind: bool,
        deadline: Instant,
    ) -> Result<Option<MrLease>> {
        self.acquire_inner(device, memory, window_bind, deadline, false)
    }

    fn acquire_inner(
        self: &Arc<Self>,
        device: Arc<RdmaContext>,
        memory: &RegisteredMemory,
        window_bind: bool,
        deadline: Instant,
        blocking: bool,
    ) -> Result<Option<MrLease>> {
        #[cfg(test)]
        tests::publication::before_acquire(self, &device, memory, window_bind);
        let context = Arc::downgrade(&device);
        loop {
            let mut cache = self
                .state
                .lock()
                .map_err(|_| Error::Internal("MR cache lock poisoned"))?;
            if let Some((operation, code)) = cache.failure {
                return Err(Error::Unquiesced { operation, code });
            }
            if cache.closed {
                return Err(Error::Unavailable("MR cache is closed"));
            }
            remaining(deadline)?;
            if let Some(index) = cache.entries.iter().position(|entry| {
                Weak::ptr_eq(&entry.owner.context, &context)
                    && entry.owner.matches(memory)
                    && entry.owner.window_bind == window_bind
            }) {
                if cache.entries[index].creating {
                    #[cfg(test)]
                    tests::publication::observe_pending(self, &device, memory, window_bind);
                    if !blocking {
                        return Ok(None);
                    }
                    drop(
                        self.changed
                            .wait_timeout(cache, remaining(deadline)?)
                            .map_err(|_| Error::Internal("MR cache lock poisoned"))?,
                    );
                    continue;
                }
                let borrowers = cache.entries[index]
                    .borrowers
                    .checked_add(1)
                    .ok_or(Error::ResourceExhausted)?;
                let mut entry = cache.entries.remove(index);
                entry.borrowers = borrowers;
                entry.retiring |= !memory.is_active();
                let owner = Arc::clone(&entry.owner);
                cache.entries.push(entry);
                let mut usage = self
                    .usage
                    .lock()
                    .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
                usage.reused_persistent_mrs += 1;
                usage.reused_source_mrs += u64::from(window_bind);
                drop(usage);
                return Ok(Some(MrLease {
                    cache: Arc::clone(self),
                    owner,
                    released: false,
                    created: false,
                }));
            }
            let mut usage = self
                .usage
                .lock()
                .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
            let bytes = usage
                .persistent_mr_bytes
                .checked_add(memory.len())
                .ok_or(Error::ResourceExhausted)?;
            if memory.len() > self.max_bytes {
                return Err(Error::ResourceExhausted);
            }
            if usage.persistent_mrs >= self.max_regions || bytes > self.max_bytes {
                drop(usage);
                let index = cache
                    .entries
                    .iter()
                    .position(|entry| !entry.creating && entry.borrowers == 0 && !entry.published)
                    .ok_or(Error::ResourceExhausted)?;
                let entry = cache.entries.remove(index);
                cache.closing += 1;
                drop(cache);
                self.finish_close(entry.owner)?;
                self.usage
                    .lock()
                    .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?
                    .evicted_persistent_mrs += 1;
                continue;
            }
            cache
                .entries
                .try_reserve(1)
                .map_err(|_| Error::ResourceExhausted)?;
            usage.persistent_mrs += 1;
            usage.persistent_source_mrs += usize::from(window_bind);
            usage.persistent_mr_bytes = bytes;
            drop(usage);
            let owner = Arc::new(MemoryRegion {
                context: context.clone(),
                key: Arc::downgrade(&memory.0),
                window_bind,
                state: Mutex::new(State::Open(Box::new(Resources {
                    handles: sys::MemoryHandles::default(),
                    device: Arc::clone(&device),
                    memory: memory.clone(),
                    window_bind,
                    usage: Arc::clone(&self.usage),
                }))),
                verbs: self.verbs,
            });
            cache.entries.push(Entry {
                owner: Arc::clone(&owner),
                creating: true,
                borrowers: 1,
                retiring: !memory.is_active(),
                published: false,
            });
            drop(cache);
            let initialized = owner.open();
            #[cfg(test)]
            tests::publication::before_publish(self, &device, memory, window_bind);
            let mut cache = self
                .state
                .lock()
                .map_err(|_| Error::Internal("MR cache lock poisoned"))?;
            let index = cache
                .entries
                .iter()
                .position(|entry| Arc::ptr_eq(&entry.owner, &owner))
                .ok_or(Error::Internal("MR construction owner missing"))?;
            if let Err(error) = initialized {
                cache.entries.remove(index);
                cache.closing += 1;
                self.changed.notify_all();
                drop(cache);
                self.finish_close(owner)?;
                return Err(error);
            }
            cache.entries[index].creating = false;
            cache.entries[index].retiring |= cache.closed || !memory.is_active();
            self.changed.notify_all();
            return Ok(Some(MrLease {
                cache: Arc::clone(self),
                owner,
                released: false,
                created: true,
            }));
        }
    }

    pub(super) fn publish(
        self: &Arc<Self>,
        devices: &[Arc<RdmaContext>],
        memories: &[RegisteredMemory],
        deadline: Instant,
    ) -> Result<()> {
        let mut leases = Vec::new();
        let result = (|| {
            for memory in memories {
                for device in devices {
                    for window_bind in [false, true] {
                        if window_bind
                            && (memory.permission() == crate::Permission::LocalOnly
                                || memory.is_cuda())
                        {
                            continue;
                        }
                        leases
                            .try_reserve(1)
                            .map_err(|_| Error::ResourceExhausted)?;
                        leases.push(self.acquire(
                            Arc::clone(device),
                            memory,
                            window_bind,
                            deadline,
                        )?);
                    }
                }
            }
            let mut cache = self
                .state
                .lock()
                .map_err(|_| Error::Internal("MR cache lock poisoned"))?;
            if let Some((operation, code)) = cache.failure {
                return Err(Error::Unquiesced { operation, code });
            }
            if cache.closed {
                return Err(Error::Unavailable("MR cache is closed"));
            }
            if leases.iter().any(|lease| {
                !cache
                    .entries
                    .iter()
                    .any(|entry| Arc::ptr_eq(&entry.owner, &lease.owner))
            }) {
                return Err(Error::Internal("MR publication owner missing"));
            }
            for entry in &mut cache.entries {
                if leases
                    .iter()
                    .any(|lease| Arc::ptr_eq(&entry.owner, &lease.owner))
                {
                    entry.published = true;
                }
            }
            Ok(())
        })();
        if result.is_err() {
            let mut cache = self.state.lock().unwrap_or_else(|p| p.into_inner());
            for lease in &leases {
                if lease.created {
                    if let Some(entry) = cache
                        .entries
                        .iter_mut()
                        .find(|entry| Arc::ptr_eq(&entry.owner, &lease.owner))
                    {
                        entry.retiring = true;
                        entry.published = false;
                    }
                }
            }
        }
        let mut result = result;
        for lease in &mut leases {
            if let Err(error) = lease.release() {
                if result.is_ok() || matches!(error, Error::Unquiesced { .. }) {
                    result = Err(error);
                }
            }
        }
        result
    }

    pub(crate) fn retire(&self, memory: &RegisteredMemory) -> Result<()> {
        loop {
            let mut cache = self
                .state
                .lock()
                .map_err(|_| Error::Internal("MR cache lock poisoned"))?;
            if let Some((operation, code)) = cache.failure {
                return Err(Error::Unquiesced { operation, code });
            }
            if memory.is_active() {
                return Ok(());
            }
            // Retire every context and capability for this registration identity.
            for entry in &mut cache.entries {
                if entry.owner.matches(memory) {
                    entry.retiring = true;
                }
            }
            let Some(index) = cache.entries.iter().position(|entry| {
                entry.owner.matches(memory) && !entry.creating && entry.borrowers == 0
            }) else {
                return Ok(());
            };
            let entry = cache.entries.remove(index);
            cache.closing += 1;
            drop(cache);
            self.finish_close(entry.owner)?;
        }
    }

    fn release(&self, owner: &Arc<MemoryRegion>) -> Result<()> {
        let mut cache = self
            .state
            .lock()
            .map_err(|_| Error::Internal("MR cache lock poisoned"))?;
        let index = cache
            .entries
            .iter()
            .position(|entry| Arc::ptr_eq(&entry.owner, owner))
            .ok_or(Error::Internal("MR borrow owner missing"))?;
        let closed = cache.closed;
        let entry = &mut cache.entries[index];
        entry.borrowers -= 1;
        if entry.borrowers != 0 || (!closed && !entry.retiring && owner.active()) {
            return Ok(());
        }
        let entry = cache.entries.remove(index);
        cache.closing += 1;
        drop(cache);
        self.finish_close(entry.owner)
    }

    fn finish_close(&self, owner: Arc<MemoryRegion>) -> Result<()> {
        let result = owner.close();
        let mut cache = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        cache.closing -= 1;
        if let Err(Error::Unquiesced { operation, code }) = &result {
            cache.failure = Some((*operation, *code));
        }
        self.changed.notify_all();
        result
    }

    pub(super) fn close(&self, deadline: Instant) -> Result<()> {
        loop {
            let mut cache = self
                .state
                .lock()
                .map_err(|_| Error::Internal("MR cache lock poisoned"))?;
            cache.closed = true;
            if let Some((operation, code)) = cache.failure {
                return Err(Error::Unquiesced { operation, code });
            }
            if cache.closing != 0 || cache.entries.iter().any(|entry| entry.creating) {
                drop(
                    self.changed
                        .wait_timeout(cache, remaining(deadline)?)
                        .map_err(|_| Error::Internal("MR cache lock poisoned"))?,
                );
                continue;
            }
            if cache.entries.iter().any(|entry| entry.borrowers != 0) {
                return Err(Error::Timeout);
            }
            let Some(entry) = cache.entries.pop() else {
                return Ok(());
            };
            cache.closing += 1;
            drop(cache);
            self.finish_close(entry.owner)?;
        }
    }
}

impl Drop for LocalBufferManager {
    fn drop(&mut self) {
        let _ = self.close(Instant::now());
    }
}

impl MemoryRegion {
    fn matches(&self, memory: &RegisteredMemory) -> bool {
        Weak::ptr_eq(&self.key, &Arc::downgrade(&memory.0))
    }
    fn active(&self) -> bool {
        self.key
            .upgrade()
            .is_some_and(|memory| RegisteredMemory(memory).is_active())
    }
    fn open(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("MR lock poisoned"))?;
        let State::Open(resources) = &mut *state else {
            return Err(Error::Unavailable("MR is closed or retained"));
        };
        resources.device.register_memory(
            &mut resources.handles,
            &resources.memory,
            self.window_bind,
        )?;
        let mut usage = resources
            .usage
            .lock()
            .map_err(|_| Error::Internal("RDMA budget lock poisoned"))?;
        usage.opened_persistent_mrs += 1;
        usage.opened_source_mrs += u64::from(self.window_bind);
        Ok(())
    }
    fn handle(&self) -> Result<*mut std::ffi::c_void> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("MR lock poisoned"))?;
        let State::Open(resources) = &*state else {
            return Err(Error::Unavailable("MR is closed or retained"));
        };
        Ok(resources.handles.mr)
    }
    fn close(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        match &mut *state {
            State::Closed => return Ok(()),
            State::Retained { code } => {
                return Err(Error::Unquiesced {
                    operation: "ibv_dereg_mr",
                    code: *code,
                });
            }
            State::Open(resources) => {
                // SAFETY: no borrower can post through this MR. Successful QP
                // destruction or no descriptor exposure precedes the last release.
                let code = unsafe { (self.verbs.deregister)(&mut resources.handles) };
                if code != 0 {
                    let mut usage = resources
                        .usage
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner());
                    usage.retained_persistent_mrs += 1;
                    usage.retained_source_mrs += usize::from(self.window_bind);
                    drop(usage);
                    let previous = std::mem::replace(&mut *state, State::Retained { code });
                    std::mem::forget(previous);
                    return Err(Error::Unquiesced {
                        operation: "ibv_dereg_mr",
                        code,
                    });
                }
            }
        }
        *state = State::Closed;
        Ok(())
    }
}
impl Drop for MemoryRegion {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
impl MrLease {
    pub(super) fn handle(&self) -> Result<*mut std::ffi::c_void> {
        self.owner.handle()
    }
    pub(super) fn release(&mut self) -> Result<()> {
        if self.released {
            return Ok(());
        }
        self.released = true;
        self.cache.release(&self.owner)
    }
}
impl Drop for MrLease {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/transport/rdma/buffers.rs"]
pub(super) mod tests;
