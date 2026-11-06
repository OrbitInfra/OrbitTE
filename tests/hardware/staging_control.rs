//! Packaged public-API GPU staging gate. The loopback relay holds a real Pin
//! reply while a second connection describes the source and probes its budget.
use orbite::transport::rdma::RdmaConfig;
use orbite::{
    EngineConfig, Error, Permission, RemoteEffect, TransferEngine, TransferOp, TransferRequest,
    Transport, cuda::CudaMemory,
};
use std::error::Error as StdError;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

const WAIT: Duration = Duration::from_secs(20);

fn forward(mut from: TcpStream, mut to: TcpStream) -> io::Result<()> {
    io::copy(&mut from, &mut to)?;
    to.shutdown(Shutdown::Write)
}

fn relay(
    listener: TcpListener,
    source: SocketAddr,
    pinned: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
) -> io::Result<()> {
    // Only Describe and the first staged submission use this listener. Both
    // legs forward the engine's bytes without decoding a private wire layout.
    for hold in [false, true] {
        let (client, _) = listener.accept()?;
        let mut server = TcpStream::connect_timeout(&source, WAIT)?;
        for socket in [&client, &server] {
            socket.set_read_timeout(Some(WAIT))?;
            socket.set_write_timeout(Some(WAIT))?;
        }
        let from_client = client.try_clone()?;
        let to_source = server.try_clone()?;
        thread::scope(|scope| {
            let outgoing = scope.spawn(move || forward(from_client, to_source));
            let mut client = client;
            if hold {
                let mut first = [0];
                server.read_exact(&mut first)?;
                pinned.send(()).map_err(io::Error::other)?;
                release.recv_timeout(WAIT).map_err(io::Error::other)?;
                client.write_all(&first)?;
            }
            forward(server, client)?;
            outgoing
                .join()
                .map_err(|_| io::Error::other("outgoing relay stopped"))?
        })?;
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn StdError>> {
    if std::env::args().nth(1).as_deref() == Some("--protocol") {
        println!("orbite-installed-staging-control");
        return Ok(());
    }
    let args: Vec<_> = std::env::args().skip(1).collect();
    let (transport, rdma) = match args.as_slice() {
        [] => (Transport::Tcp, None),
        [mode, device, gid] if mode == "--rdma" => {
            let mut rdma = RdmaConfig::new(device);
            rdma.gid_index = Some(gid.parse()?);
            (Transport::Rdma, Some(rdma))
        }
        _ => return Err("expected no arguments or --rdma DEVICE GID_INDEX".into()),
    };
    let source = TransferEngine::new(EngineConfig {
        rdma: rdma.clone(),
        handler_threads: 1,
        handler_timeout: WAIT,
        remote_staging: true,
        staging_chunk_size: 8,
        staging_chunk_count: 2,
        ..Default::default()
    })?;
    let requester = TransferEngine::new(EngineConfig {
        rdma,
        max_failover_attempts: 0,
        ..Default::default()
    })?;
    let initial: [u8; 64] = std::array::from_fn(|i| (i * 3 + 1) as u8);
    let gpu = Arc::new(CudaMemory::allocate(0, initial.len())?);
    gpu.write(0, &initial)?;
    let remote = source
        .register_cuda_memory_batch(
            &[(gpu.clone(), 0, initial.len())],
            Permission::RemoteReadWrite,
        )?
        .remove(0);
    let local =
        requester.register_memory(vec![0xb7; 24].into_boxed_slice(), Permission::LocalOnly)?;
    let contender =
        requester.register_memory(vec![0x93; 24].into_boxed_slice(), Permission::LocalOnly)?;
    let request = |offset| {
        TransferRequest::new(
            transport,
            local.base_address() + 4,
            remote.base_address() + offset,
            16,
        )
        .with_remote_staging(true)
    };
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let through_relay = listener.local_addr()?;
    let (pinned, seen) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    let origin = source.endpoint();
    let epoch = source.source_epoch();
    let relay = thread::spawn(move || relay(listener, origin, pinned, wait));
    let peer = requester.open_segment(through_relay, epoch, WAIT)?;
    thread::scope(|scope| -> Result<(), Box<dyn StdError>> {
        let first = scope
            .spawn(|| requester.submit_and_wait(TransferOp::Write, &peer, &[request(8)], WAIT));
        seen.recv_timeout(WAIT)?;
        let deadline = Instant::now() + WAIT;
        while source.stats()?.staging.remote_pins == 0 {
            if Instant::now() >= deadline {
                return Err("Pin reply lacked its source owner".into());
            }
            thread::sleep(Duration::from_millis(1));
        }
        let probe = (|| -> Result<(), Box<dyn StdError>> {
            let direct = requester.open_segment(origin, epoch, WAIT)?;
            if direct.regions().len() != 1
                || direct.regions()[0].base_address() != remote.base_address()
            {
                return Err("Describe published an unexpected registration".into());
            }
            let denied = TransferRequest::new(
                transport,
                contender.base_address() + 4,
                remote.base_address() + 40,
                16,
            )
            .with_remote_staging(true);
            let busy = requester.submit_and_wait(TransferOp::Read, &direct, &[denied], WAIT)?;
            let mut untouched = [0; 24];
            contender.read(0, &mut untouched)?;
            if untouched != [0x93; 24] {
                return Err("rejected READ touched its destination".into());
            }
            if busy.completed_requests != 0
                || busy.transferred_bytes != 0
                || !busy.failure.is_some_and(|f| {
                    matches!(f.error, Error::ResourceExhausted)
                        && f.remote_effect == RemoteEffect::None
                })
            {
                return Err("second Pin did not reject before payload".into());
            }
            let stats = source.stats()?.staging;
            if (
                stats.active_chunks,
                stats.allocations,
                stats.remote_pins,
                stats.remote_crosses,
            ) != (1, 1, 1, 0)
            {
                return Err("the held Pin did not keep exactly its own resources".into());
            }
            Ok(())
        })();
        release.send(())?;
        let first = first.join().map_err(|_| "staging submission panicked")??;
        probe?;
        if !first.is_complete() || first.transferred_bytes != 16 {
            return Err("staged WRITE failed".into());
        }
        Ok(())
    })?;
    relay.join().map_err(|_| "relay panicked")??;
    let mut expected = initial;
    expected[8..24].fill(0xb7);
    let mut bytes = [0; 64];
    gpu.read(0, &mut bytes)?;
    if bytes != expected {
        return Err("GPU WRITE payload or guards changed".into());
    }
    let direct = requester.open_segment(origin, epoch, WAIT)?;
    let read = requester.submit_and_wait(TransferOp::Read, &direct, &[request(40)], WAIT)?;
    let mut host = [0; 24];
    local.read(0, &mut host)?;
    let mut expected_host = [0xb7; 24];
    expected_host[4..20].copy_from_slice(&initial[40..56]);
    if !read.is_complete() || read.transferred_bytes != 16 || host != expected_host {
        return Err("READ after Unpin or its host guards failed".into());
    }
    requester.close(WAIT)?;
    source.close(WAIT)?;
    let stats = source.stats()?.staging;
    if (
        stats.remote_pins,
        stats.remote_unpins,
        stats.reserved_chunks,
    ) != (2, 2, 0)
    {
        return Err("source sessions did not drain".into());
    }
    if transport == Transport::Rdma {
        let native = requester
            .stats()?
            .rdma
            .ok_or("missing requester verbs evidence")?;
        if native.completed_write_bytes != 16 || native.completed_read_bytes != 16 {
            return Err("guarded transfers did not use the selected RNIC".into());
        }
    }
    println!("{transport:?} GPU staging control isolation, guarded WRITE/READ and drain passed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relay_forwards_describe_then_holds_and_resumes_the_next_response() {
        let source = TcpListener::bind("127.0.0.1:0").unwrap();
        let destination = source.local_addr().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let (pinned, seen) = mpsc::channel();
        let (release, wait) = mpsc::channel();
        thread::scope(|scope| {
            let echo = scope.spawn(|| {
                for _ in 0..2 {
                    let (mut client, _) = source.accept().unwrap();
                    client.set_read_timeout(Some(WAIT)).unwrap();
                    let mut request = [0; 4];
                    client.read_exact(&mut request).unwrap();
                    client.write_all(&request).unwrap();
                }
            });
            let forwarding = scope.spawn(|| relay(listener, destination, pinned, wait));
            for (i, bytes) in [*b"desc", *b"data"].into_iter().enumerate() {
                let mut client = TcpStream::connect_timeout(&endpoint, WAIT).unwrap();
                client.set_read_timeout(Some(WAIT)).unwrap();
                client.write_all(&bytes).unwrap();
                client.shutdown(Shutdown::Write).unwrap();
                if i == 1 {
                    seen.recv_timeout(WAIT).unwrap();
                    client.set_nonblocking(true).unwrap();
                    assert_eq!(
                        client.peek(&mut [0]).unwrap_err().kind(),
                        io::ErrorKind::WouldBlock
                    );
                    client.set_nonblocking(false).unwrap();
                    release.send(()).unwrap();
                }
                let mut got = [0; 4];
                client.read_exact(&mut got).unwrap();
                assert_eq!(got, bytes);
            }
            echo.join().unwrap();
            forwarding.join().unwrap().unwrap();
        });
    }
}
