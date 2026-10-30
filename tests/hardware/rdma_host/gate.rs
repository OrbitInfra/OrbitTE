//! Test-only wire-v21 relay. Hold READY after the source posts its completion RQ.
//! Payload opcodes are rejected; RDMA bytes never pass through this listener.

use std::net::Shutdown;
use std::sync::{Arc, Condvar, Mutex};

use super::*;

#[derive(Default)]
struct State {
    armed: bool,
    held: usize,
    serial: u64,
    stopped: bool,
}

#[derive(Default)]
struct Control {
    state: Mutex<State>,
    changed: Condvar,
}

pub(super) struct GrantGate {
    address: SocketAddr,
    control: Arc<Control>,
    worker: Option<thread::JoinHandle<Result<()>>>,
}

impl GrantGate {
    pub(super) fn new(bind: SocketAddr, upstream: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(bind)?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let control = Arc::new(Control::default());
        let shared = Arc::clone(&control);
        let worker = thread::spawn(move || {
            let mut handlers = Vec::new();
            let mut failure = None;
            while !shared
                .state
                .lock()
                .map_err(|_| "gate lock poisoned")?
                .stopped
            {
                while let Some(index) = handlers.iter().position(thread::JoinHandle::is_finished) {
                    let handle: thread::JoinHandle<Result<()>> = handlers.swap_remove(index);
                    if let Err(error) = handle.join().map_err(|_| "relay panicked")? {
                        failure.get_or_insert(error);
                    }
                }
                match listener.accept() {
                    Ok((client, _)) if handlers.len() < 64 => {
                        let shared = Arc::clone(&shared);
                        handlers.push(thread::spawn(move || relay(client, upstream, &shared)));
                    }
                    Ok(_) => {
                        failure.get_or_insert_with(|| "relay handler bound exceeded".into());
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            for handle in handlers {
                if let Err(error) = handle.join().map_err(|_| "relay panicked")? {
                    failure.get_or_insert(error);
                }
            }
            failure.map_or(Ok(()), Err)
        });
        Ok(Self {
            address,
            control,
            worker: Some(worker),
        })
    }

    pub(super) fn address(&self) -> SocketAddr {
        self.address
    }

    pub(super) fn arm(&self) -> Result<()> {
        let mut state = self
            .control
            .state
            .lock()
            .map_err(|_| "gate lock poisoned")?;
        if state.armed || state.held != 0 || state.stopped {
            return Err("gate still owns a previous interruption".into());
        }
        state.armed = true;
        Ok(())
    }

    pub(super) fn held(&self) -> Result<usize> {
        Ok(self
            .control
            .state
            .lock()
            .map_err(|_| "gate lock poisoned")?
            .held)
    }

    pub(super) fn abort(&self) -> Result<()> {
        let mut state = self
            .control
            .state
            .lock()
            .map_err(|_| "gate lock poisoned")?;
        state.armed = false;
        state.serial += 1;
        self.control.changed.notify_all();
        let deadline = Instant::now() + TIMEOUT;
        while state.held != 0 {
            if Instant::now() >= deadline {
                return Err("relay did not release its sockets".into());
            }
            state = self
                .control
                .changed
                .wait_timeout(state, Duration::from_millis(10))
                .map_err(|_| "gate lock poisoned")?
                .0;
        }
        Ok(())
    }

    pub(super) fn close(mut self) -> Result<()> {
        self.abort()?;
        self.control
            .state
            .lock()
            .map_err(|_| "gate lock poisoned")?
            .stopped = true;
        self.worker
            .take()
            .ok_or("relay already closed")?
            .join()
            .map_err(|_| "relay panicked")?
    }
}

impl Drop for GrantGate {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            if let Ok(mut state) = self.control.state.lock() {
                state.stopped = true;
                state.armed = false;
                state.serial += 1;
                self.control.changed.notify_all();
            }
            let _ = worker.join();
        }
    }
}

fn frame<const N: usize>(stream: &mut TcpStream) -> Result<[u8; N]> {
    let mut bytes = [0; N];
    stream.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn header(bytes: &[u8]) -> Result<(u8, usize)> {
    if bytes[..8] != *b"ORBITTE\0" || bytes[8..10] != 21u16.to_be_bytes() || bytes[11] != 0 {
        return Err("relay only supports the frozen wire-v21 header".into());
    }
    Ok((
        bytes[10],
        usize::try_from(u64::from_be_bytes(bytes[28..36].try_into()?))?,
    ))
}

fn forward(source: &mut TcpStream, destination: &mut TcpStream, length: usize) -> Result<()> {
    if length > 16384 {
        return Err("oversized relay control frame".into());
    }
    let mut bytes = vec![0; length];
    source.read_exact(&mut bytes)?;
    destination.write_all(&bytes)?;
    Ok(())
}

fn binding(source: &mut TcpStream, destination: &mut TcpStream, phase: u8) -> Result<()> {
    let bytes = frame::<142>(source)?;
    if bytes[0] != phase {
        return Err("unexpected relay binding phase".into());
    }
    destination.write_all(&bytes)?;
    Ok(())
}

fn relay(mut client: TcpStream, upstream: SocketAddr, control: &Control) -> Result<()> {
    client.set_read_timeout(Some(TIMEOUT))?;
    client.set_write_timeout(Some(TIMEOUT))?;
    let request = frame::<52>(&mut client)?;
    let (operation, _) = header(&request)?;
    if !matches!(operation, 0 | 4 | 5) {
        return Err("relay rejects payload and sliced opcodes".into());
    }
    let mut source = TcpStream::connect_timeout(&upstream, TIMEOUT)?;
    source.set_read_timeout(Some(TIMEOUT))?;
    source.set_write_timeout(Some(TIMEOUT))?;
    source.write_all(&request)?;
    let reply = frame::<36>(&mut source)?;
    let (status, length) = header(&reply)?;
    client.write_all(&reply)?;
    if status != 0 {
        return Ok(());
    }
    if operation != 0 && (!(188..=1 + 64 * 187).contains(&length) || (length - 1) % 187 != 0) {
        return Err("invalid relay path metadata size".into());
    }
    forward(&mut source, &mut client, length)?;
    if operation == 0 {
        return Ok(());
    }
    forward(&mut client, &mut source, 106)?;
    let offer = frame::<36>(&mut source)?;
    let (status, length) = header(&offer)?;
    client.write_all(&offer)?;
    if status != 0 {
        return Ok(());
    }
    if length != 89 {
        return Err("invalid relay offer size".into());
    }
    forward(&mut source, &mut client, 89)?;
    binding(&mut client, &mut source, 1)?;
    forward(&mut client, &mut source, 40)?;
    let ready = frame::<36>(&mut source)?;
    if header(&ready)? != (9, 0) {
        return Err("missing source ready at relay".into());
    }
    let mut state = control.state.lock().map_err(|_| "gate lock poisoned")?;
    if state.armed {
        let serial = state.serial;
        state.held += 1;
        control.changed.notify_all();
        let deadline = Instant::now() + TIMEOUT;
        while state.armed && state.serial == serial && !state.stopped {
            if Instant::now() >= deadline {
                state.held -= 1;
                control.changed.notify_all();
                return Err("armed relay was not interrupted".into());
            }
            state = control
                .changed
                .wait_timeout(state, Duration::from_millis(10))
                .map_err(|_| "gate lock poisoned")?
                .0;
        }
        let _ = client.shutdown(Shutdown::Both);
        let _ = source.shutdown(Shutdown::Both);
        state.held -= 1;
        control.changed.notify_all();
        return Ok(());
    }
    drop(state);
    client.write_all(&ready)?;
    binding(&mut source, &mut client, 2)?;
    forward(&mut source, &mut client, 40)?;
    binding(&mut client, &mut source, 3)?;
    let ack = frame::<36>(&mut source)?;
    if header(&ack)? != (0, 0) {
        return Err("invalid relay ACK".into());
    }
    client.write_all(&ack)?;
    binding(&mut source, &mut client, 4)?;
    binding(&mut source, &mut client, 5)
}

#[cfg(test)]
#[path = "gate_tests.rs"]
mod tests;

#[test]
fn relay_tracks_the_public_engines_current_wire() {
    let source = TransferEngine::new(orbit_te::EngineConfig::default()).unwrap();
    let requester = TransferEngine::new(orbit_te::EngineConfig::default()).unwrap();
    let memory = source
        .register_memory(
            vec![1; 17].into_boxed_slice(),
            orbit_te::Permission::RemoteRead,
        )
        .unwrap();
    let gate = GrantGate::new("127.0.0.1:0".parse().unwrap(), source.endpoint()).unwrap();
    let peer = requester
        .open_segment(gate.address(), source.source_epoch(), TIMEOUT)
        .unwrap();
    assert_eq!(peer.regions().len(), 1);
    assert_eq!(peer.regions()[0].base_address(), memory.base_address());
    gate.close().unwrap();
    requester.close(TIMEOUT).unwrap();
    source.close(TIMEOUT).unwrap();
}
