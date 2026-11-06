//! Installed requester and source with a real healthy RNIC baseline. The relay
//! closes exactly one bootstrap before either peer can post a payload.
use super::*;
use orbite::{EngineConfig, Permission, transport::rdma::RdmaConfig};
use std::sync::Arc;

pub(super) fn run() -> Result<()> {
    let nic = env::var("ORBITE_FAILOVER_NIC")?;
    let config = EngineConfig {
        rdma: Some(RdmaConfig::new(nic)),
        ..Default::default()
    };
    let src = TransferEngine::new(config.clone())?;
    let dst = Arc::new(TransferEngine::new(config)?);
    let remote = src.register_memory(
        (0..16u8).collect::<Vec<_>>().into(),
        Permission::RemoteReadWrite,
    )?;
    let local = dst.register_memory(vec![0xcc; 16].into(), Permission::LocalOnly)?;
    let epoch = src.source_epoch();
    let request = TransferRequest::new(
        Transport::Rdma,
        local.base_address() + 3,
        remote.base_address() + 3,
        8,
    );
    let peer = dst.open_segment(src.endpoint(), epoch, TIMEOUT)?;
    if !dst
        .submit_and_wait(
            TransferOp::Read,
            &peer,
            std::slice::from_ref(&request),
            TIMEOUT,
        )?
        .is_complete()
    {
        return Err("healthy native baseline failed".into());
    }
    let before = dst.stats()?.rdma.ok_or("no RDMA counters")?;
    if before.completed_read_bytes != 8 {
        return Err("baseline did not move eight native bytes".into());
    }
    local.write(0, &[0xcc; 16])?;
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let endpoint = listener.local_addr()?;
    let upstream = src.endpoint();
    listener.set_nonblocking(true)?;
    let relay = thread::spawn(move || -> Result<()> {
        let until = Instant::now() + TIMEOUT;
        for expected in [0u8, 4, 1] {
            let mut client = loop {
                match listener.accept() {
                    Ok((s, _)) => break s,
                    Err(e)
                        if e.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < until =>
                    {
                        thread::sleep(Duration::from_millis(1))
                    }
                    Err(e) => return Err(e.into()),
                }
            };
            client.set_nonblocking(false)?;
            client.set_read_timeout(Some(TIMEOUT))?;
            client.set_write_timeout(Some(TIMEOUT))?;
            let mut frame = [0u8; 52];
            client.read_exact(&mut frame)?;
            if frame[10] != expected {
                return Err("unexpected relay operation".into());
            }
            if expected == 4 {
                continue;
            }
            let mut source = TcpStream::connect_timeout(&upstream, TIMEOUT)?;
            source.set_read_timeout(Some(TIMEOUT))?;
            source.set_write_timeout(Some(TIMEOUT))?;
            source.write_all(&frame)?;
            let mut reply = [0u8; 36];
            source.read_exact(&mut reply)?;
            let n = u64::from_be_bytes(reply[28..].try_into()?);
            if n > 65536 {
                return Err("reply exceeded gate bound".into());
            }
            let mut bytes = vec![0; n as usize];
            source.read_exact(&mut bytes)?;
            client.write_all(&reply)?;
            client.write_all(&bytes)?;
        }
        Ok(())
    });
    let endpoint = dst.open_segment(endpoint, epoch, TIMEOUT)?;
    let batch = dst.submit_async(TransferOp::Read, &endpoint, &[request], None, TIMEOUT)?;
    batch.wait(TIMEOUT)?;
    let task = batch.task_status(0)?;
    relay.join().map_err(|_| "failover relay panicked")??;
    let mut bytes = [0; 16];
    local.read(0, &mut bytes)?;
    let mut expected = [0xcc; 16];
    expected[3..11].copy_from_slice(&[3, 4, 5, 6, 7, 8, 9, 10]);
    if bytes != expected
        || task.transport != Transport::Tcp
        || task.failover_count != 1
        || task.transferred_bytes != 8
        || dst.stats()?.rdma.ok_or("no native stats")?.post_calls != before.post_calls
    {
        return Err("fallback route, payload guard or native post count disagreed".into());
    }
    println!("native baseline bytes=8 bootstrap rejections=1 tcp fallback bytes=8");
    drop(batch);
    dst.close(TIMEOUT)?;
    src.close(TIMEOUT)?;
    Ok(())
}
