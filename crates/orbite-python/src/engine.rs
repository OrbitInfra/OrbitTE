use crate::{boundary::*, memory::Memory, records::Handle, segments::Segments};
use orbite::{Batch, EngineConfig, TransferEngine};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, OnceLock, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
};

pub(crate) struct Engine {
    pub core: Arc<TransferEngine>,
    pub defaults: [orbite::PolicyName; 2],
    pub memory: Mutex<Memory>,
    pub segments: Mutex<Segments>,
    pub batches: Mutex<HashMap<u64, Arc<Mutex<Batch>>>>,
}
impl Engine {
    fn new(cfg: EngineConfig) -> Result<Self> {
        Ok(Self {
            core: Arc::new(TransferEngine::new(cfg)?),
            defaults: [
                orbite::PolicyName::new("memory_default")?,
                orbite::PolicyName::new("file_default")?,
            ],
            memory: Mutex::default(),
            segments: Mutex::default(),
            batches: Mutex::default(),
        })
    }
    pub fn timeout(&self) -> std::time::Duration {
        self.core.config().handler_timeout
    }
    pub fn batch(&self, id: u64) -> Result<Arc<Mutex<Batch>>> {
        lock(&self.batches)?.get(&id).cloned().ok_or(INVALID)
    }
}

// A private Python engine token is never reused. Slots retain uncertain owners
// through close and interpreter shutdown. No Python-provided pointer is dereferenced.
struct Slot {
    live: RwLock<bool>,
    engine: OnceLock<Result<Engine>>,
}
fn slots() -> &'static Mutex<HashMap<usize, Arc<Slot>>> {
    static TABLE: OnceLock<Mutex<HashMap<usize, Arc<Slot>>>> = OnceLock::new();
    TABLE.get_or_init(Mutex::default)
}
pub(crate) fn id() -> Result<usize> {
    static NEXT: AtomicUsize = AtomicUsize::new(1);
    let mut n = NEXT.load(Ordering::Relaxed);
    loop {
        let next = n.checked_add(1).ok_or(FULL)?;
        match NEXT.compare_exchange_weak(n, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return Ok(n),
            Err(now) => n = now,
        }
    }
}
pub(crate) fn create_using(config: impl FnOnce() -> Result<EngineConfig>) -> Result<Handle> {
    // Charge the slot before binding sockets or starting workers.
    let key = id()?;
    let slot = Arc::new(Slot {
        live: RwLock::new(true),
        engine: OnceLock::new(),
    });
    {
        let mut table = lock(slots())?;
        if table.len() >= 1024 {
            return Err(FULL);
        }
        table.insert(key, slot.clone());
    }
    let Some(result) = guard(None, || Some(config().and_then(Engine::new))) else {
        lock(slots())?.remove(&key);
        return Err(INTERNAL);
    };
    if let Err(code) = &result {
        guard((), || log::warn!("Python engine unavailable ({})", code.0));
    }
    slot.engine.set(result).map_err(|_| INTERNAL)?;
    Ok(key)
}
pub(crate) fn with<T>(handle: Handle, f: impl FnOnce(&Engine) -> Result<T>) -> Result<T> {
    let slot = lock(slots())?.get(&handle).cloned().ok_or(INVALID)?;
    let live = slot.live.read().map_err(|_| INTERNAL)?;
    if !*live {
        return Err(INVALID);
    }
    f(slot.engine.get().ok_or(INVALID)?.as_ref().map_err(|c| *c)?)
}
pub(crate) fn destroy(h: Handle) -> Result<()> {
    if h == 0 {
        return Ok(());
    }
    let key = h;
    let slot = lock(slots())?.get(&key).cloned().ok_or(INVALID)?;
    let mut live = slot.live.write().map_err(|_| INTERNAL)?;
    if !*live {
        return Err(INVALID);
    }
    if slot.engine.get().is_none() {
        return Err(INVALID);
    }
    *live = false;
    // On uncertain retirement the closed slot and all its owners remain charged.
    if let Some(Ok(e)) = slot.engine.get() {
        lock(&e.batches)?.clear();
        e.core.close(e.timeout())?;
        lock(&e.memory)?.close(&e.core)?;
    }
    lock(slots())?.remove(&key);
    Ok(())
}
