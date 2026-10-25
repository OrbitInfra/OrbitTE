use super::*;
use std::sync::mpsc;
use std::time::Duration;

fn stats(monitor: &Monitor) -> RdmaStats {
    let mut stats = RdmaStats::default();
    monitor.stats(&mut stats);
    stats
}

type AckGate = Arc<(Mutex<(bool, bool)>, Condvar)>;

struct Native {
    reader: Mutex<UnixStream>,
    acknowledgments: Mutex<Vec<String>>,
    gate: Option<AckGate>,
}

unsafe extern "C" fn get(context: *mut c_void, event: *mut sys::AsyncEvent) -> i32 {
    let native = unsafe { &*context.cast::<Native>() };
    let mut byte = [0];
    match native.reader.lock().unwrap().read(&mut byte) {
        Ok(1) => {
            unsafe {
                (*event).kind = byte[0] as u32;
            };
            0
        }
        Err(error) => error.raw_os_error().unwrap_or(libc::EIO),
        _ => libc::EIO,
    }
}

unsafe extern "C" fn ack(context: *mut c_void, _: *mut sys::AsyncEvent) {
    let native = unsafe { &*context.cast::<Native>() };
    if let Some(gate) = &native.gate {
        let mut state = gate.0.lock().unwrap();
        state.0 = true;
        gate.1.notify_all();
        while !state.1 {
            state = gate.1.wait(state).unwrap();
        }
    }
    native
        .acknowledgments
        .lock()
        .unwrap()
        .push(thread::current().name().unwrap().into());
}

struct Registered {
    registration: Registration,
    native: Box<Native>,
    writer: Option<UnixStream>,
}

struct ReleaseAck(AckGate);
impl Drop for ReleaseAck {
    fn drop(&mut self) {
        self.0.0.lock().unwrap_or_else(|p| p.into_inner()).1 = true;
        self.0.1.notify_all();
    }
}

impl Drop for Registered {
    fn drop(&mut self) {
        if let Some(gate) = &self.native.gate {
            drop(ReleaseAck(Arc::clone(gate)));
        }
        // Native memory/fd owners survive the consumer even when a guard fails.
        let _ = self.registration.stop();
    }
}

fn attach(monitor: &Arc<Monitor>, gate: Option<AckGate>) -> Registered {
    let (reader, writer) = UnixStream::pair().unwrap();
    reader.set_nonblocking(true).unwrap();
    let fd = reader.as_raw_fd();
    let mut native = Box::new(Native {
        reader: Mutex::new(reader),
        acknowledgments: Mutex::default(),
        gate,
    });
    let registration = monitor
        .register(
            Source::Async {
                context: NonNull::from(native.as_mut()).cast(),
                fd,
                verbs: sys::AsyncVerbs { get, ack },
            },
            Arc::default(),
            Arc::default(),
        )
        .unwrap();
    Registered {
        registration,
        native,
        writer: Some(writer),
    }
}

#[test]
fn one_monitor_handles_many_sources_and_waits_for_ack_before_detaching() {
    let monitor = Arc::new(Monitor::default());
    let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
    let mut sources: Vec<_> = (0..4).map(|_| attach(&monitor, None)).collect();
    let mut gated = attach(&monitor, Some(Arc::clone(&gate)));
    assert_eq!(stats(&monitor).monitor_workers, 1);
    assert_eq!(stats(&monitor).async_event_sources, 5);
    assert!(matches!(
        monitor.close(Instant::now() + Duration::from_secs(2)),
        Err(Error::Unquiesced {
            code: libc::EBUSY,
            ..
        })
    ));
    gated
        .writer
        .as_mut()
        .unwrap()
        .write_all(&[ibverbs_sys::IBV_EVENT_COMM_EST as u8])
        .unwrap();
    let until = Instant::now() + Duration::from_secs(2);
    let mut state = gate.0.lock().unwrap();
    while !state.0 {
        state = gate
            .1
            .wait_timeout(state, until.saturating_duration_since(Instant::now()))
            .unwrap()
            .0;
        assert!(Instant::now() < until);
    }
    drop(state);
    assert!(matches!(
        gated.registration.watch.source.try_lock(),
        Err(std::sync::TryLockError::WouldBlock)
    ));
    thread::scope(|scope| {
        let release = ReleaseAck(Arc::clone(&gate));
        let (entered, waiting) = mpsc::sync_channel(1);
        let registration = &mut gated.registration;
        let detached = scope.spawn(move || {
            entered.send(()).unwrap();
            registration.stop().unwrap();
        });
        waiting.recv().unwrap();
        drop(release);
        detached.join().unwrap();
    });
    assert_eq!(
        *gated.native.acknowledgments.lock().unwrap(),
        ["orbitte-rdma-monitor"]
    );
    for source in &mut sources {
        source
            .writer
            .as_mut()
            .unwrap()
            .write_all(&[ibverbs_sys::IBV_EVENT_COMM_EST as u8])
            .unwrap();
    }
    while sources
        .iter()
        .any(|source| source.native.acknowledgments.lock().unwrap().is_empty())
    {
        assert!(Instant::now() < until);
        thread::yield_now();
    }
    for source in &mut sources {
        assert_eq!(
            *source.native.acknowledgments.lock().unwrap(),
            ["orbitte-rdma-monitor"]
        );
        source.registration.stop().unwrap();
    }
    monitor.close(until).unwrap();
    monitor.close(Instant::now()).unwrap();
    let stats = stats(&monitor);
    assert_eq!(
        (
            stats.monitor_workers,
            stats.opened_monitor_workers,
            stats.async_event_sources
        ),
        (0, 1, 0)
    );
}

#[test]
fn a_lost_source_does_not_stop_other_devices_or_allow_native_close() {
    let monitor = Arc::new(Monitor::default());
    let mut failed = attach(&monitor, None);
    let mut healthy = attach(&monitor, None);
    drop(failed.writer.take());
    let until = Instant::now() + Duration::from_secs(2);
    while stats(&monitor).async_event_sources != 1 {
        assert!(Instant::now() < until);
        thread::yield_now();
    }
    assert!(matches!(
        failed.registration.stop(),
        Err(Error::Unquiesced { .. })
    ));
    assert!(failed.registration.watch.state.check().is_err());
    healthy
        .writer
        .as_mut()
        .unwrap()
        .write_all(&[ibverbs_sys::IBV_EVENT_COMM_EST as u8])
        .unwrap();
    while healthy.native.acknowledgments.lock().unwrap().is_empty() {
        assert!(Instant::now() < until);
        thread::yield_now();
    }
    assert!(healthy.registration.watch.state.check().is_ok());
    healthy.registration.stop().unwrap();
    monitor.close(until).unwrap();
}

pub(super) fn observe(source: &Source) {
    if std::env::var_os("ORBITTE_SHARED_RAILS").is_none() {
        return;
    }
    use std::collections::BTreeSet;
    use std::sync::OnceLock;
    type Observation = (String, usize, i32, String);
    static SEEN: OnceLock<Mutex<BTreeSet<Observation>>> = OnceLock::new();
    let (kind, handle) = match source {
        Source::Async { context, .. } => ("async", context.as_ptr() as usize),
        Source::Completion { channel, .. } => ("completion", channel.as_ptr() as usize),
    };
    let current = thread::current();
    assert_eq!(
        current.name(),
        Some("orbitte-rdma-monitor"),
        "native events escaped the transport monitor"
    );
    let thread = format!("{:?}", current.id());
    if SEEN.get_or_init(Default::default).lock().unwrap().insert((
        kind.into(),
        handle,
        source.fd(),
        thread.clone(),
    )) {
        println!(
            "ORBITTE_MONITOR_SOURCE {{\"kind\":\"{kind}\",\"handle\":{handle},\"fd\":{},\"thread\":\"{thread}\"}}",
            source.fd()
        );
    }
}
