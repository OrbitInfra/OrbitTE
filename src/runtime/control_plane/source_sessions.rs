//! Source Pin sessions own their socket through Unpin. A blocked Delegate or
//! peer cannot occupy the workers that serve Describe and credit messages.
use super::*;
use crate::RegisteredMemory;
use crate::engine::SourcePermit;
use std::panic::{AssertUnwindSafe, catch_unwind};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

struct Pin {
    stream: TcpStream,
    deadline: Instant,
    request: Request,
    hello: [u8; PIN_HELLO_SIZE],
    memory: RegisteredMemory,
    admission: SourcePermit,
    slot: OwnedSemaphorePermit,
}

#[derive(Clone)]
pub(super) struct Sessions {
    sender: mpsc::SyncSender<Pin>,
    available: Arc<Semaphore>,
}

impl Sessions {
    pub(super) fn start(
        source: &Arc<Source>,
        stopping: &Arc<AtomicBool>,
        limit: usize,
        workers: &mut Vec<JoinHandle<()>>,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::sync_channel::<Pin>(limit);
        let receiver = Arc::new(Mutex::new(receiver));
        let available = Arc::new(Semaphore::new(limit));
        for index in 0..limit {
            let receiver = Arc::clone(&receiver);
            let source = Arc::clone(source);
            let stopping = Arc::clone(stopping);
            workers.push(
                thread::Builder::new()
                    .name(format!("orbitte-source-stage-{index}"))
                    .spawn(move || {
                        loop {
                            let pin = receiver.lock().unwrap_or_else(|p| p.into_inner()).recv();
                            let Ok(pin) = pin else { return };
                            pin.run(&source, &stopping);
                        }
                    })?,
            );
        }
        Ok(Self { sender, available })
    }

    pub(super) fn submit(
        &self,
        mut channel: ControlChannel<'_>,
        epoch: SourceEpoch,
        request: Request,
        hello: [u8; PIN_HELLO_SIZE],
        memory: RegisteredMemory,
        admission: SourcePermit,
    ) -> Result<()> {
        // The charge includes dispatch, execution and retained sessions. Never
        // wait for another session to release a slot while handling its peer.
        let Ok(slot) = Arc::clone(&self.available).try_acquire_owned() else {
            drop((memory, admission));
            return channel.reply(epoch, error_status(&Error::ResourceExhausted), 0);
        };
        let pin = Pin {
            stream: channel.stream,
            deadline: channel.deadline,
            request,
            hello,
            memory,
            admission,
            slot,
        };
        let error = match self.sender.try_send(pin) {
            Ok(()) => return Ok(()),
            Err(mpsc::TrySendError::Full(pin)) => {
                channel.stream = pin.stream;
                Error::ResourceExhausted
            }
            Err(mpsc::TrySendError::Disconnected(pin)) => {
                channel.stream = pin.stream;
                Error::SourceClosed
            }
        };
        channel.reply(epoch, error_status(&error), 0)
    }
}

impl Pin {
    fn run(self, source: &Source, stopping: &AtomicBool) {
        let mut channel = ControlChannel {
            stream: self.stream,
            deadline: self.deadline,
            stopping: Some(Stop::Source(stopping)),
            nonblocking: false,
        };
        if channel.poll_timeout().is_err() {
            return;
        }
        let mut slot = Some(self.slot);
        let result = catch_unwind(AssertUnwindSafe(|| {
            source.proxy.serve_remote(
                &mut channel,
                source,
                &self.request,
                &self.memory,
                &self.hello,
                || drop(slot.take()),
            )
        }))
        .unwrap_or_else(|_| {
            log::error!("source staging handler panicked");
            Err(Error::Unquiesced {
                operation: "source staging handler panic",
                code: -1,
            })
        });
        if matches!(result, Err(Error::Unquiesced { .. })) {
            // A closed socket cannot revoke a posted WR or CUDA copy. The
            // session slot, actual backing and source admission remain charged.
            if let Some(slot) = slot {
                slot.forget();
            }
            std::mem::forget((self.memory, self.admission));
        }
    }
}
