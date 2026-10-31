use std::collections::HashMap;
use std::sync::{OnceLock, Weak, mpsc};

use super::*;

static OBSERVERS: OnceLock<Mutex<HashMap<usize, Weak<mpsc::Sender<String>>>>> = OnceLock::new();

pub(crate) struct RetirementHold {
    pub(crate) pause: Arc<(Mutex<(bool, bool)>, Condvar)>,
    pub(crate) observed: mpsc::Receiver<String>,
    _sender: Arc<mpsc::Sender<String>>,
    key: usize,
}

impl RetirementHold {
    pub(crate) fn new(runtime: &RdmaTransport) -> Self {
        let pause = super::super::context::tests::pause_device(&runtime.device_at(2).unwrap());
        let key = Arc::as_ptr(&runtime.usage) as usize;
        let (send, observed) = mpsc::channel();
        let send = Arc::new(send);
        assert!(
            OBSERVERS
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .insert(key, Arc::downgrade(&send))
                .is_none()
        );
        Self {
            pause,
            observed,
            _sender: send,
            key,
        }
    }

    pub(crate) fn release(&self) {
        self.pause.0.lock().unwrap().1 = true;
        self.pause.1.notify_all();
    }

    pub(crate) fn assert_independent_workers(&self, runtime: &RdmaTransport) {
        let cache = runtime.context_set.lock().unwrap();
        let workers: Vec<_> = cache
            .slots
            .iter()
            .flat_map(|slot| slot.cq_list.iter().flatten())
            .filter(|lane| !lane.records().is_empty())
            .map(|lane| *lane.worker_id.get().unwrap())
            .collect();
        assert_eq!(workers.len(), 2);
        assert_ne!(
            workers[0], workers[1],
            "provider hold needs separate worker capacity"
        );
    }
}

impl Drop for RetirementHold {
    fn drop(&mut self) {
        self.release();
        OBSERVERS.get().unwrap().lock().unwrap().remove(&self.key);
    }
}

pub(super) fn observe(grant: &RdmaGrant, result: &Result<()>) {
    if let Err(error) = result {
        let key = Arc::as_ptr(&grant.progress.shared.usage) as usize;
        let sender = OBSERVERS
            .get()
            .and_then(|map| map.lock().unwrap().get(&key).and_then(Weak::upgrade));
        if let Some(sender) = sender {
            let _ = sender.send(format!("{error:?}"));
        }
    }
}
