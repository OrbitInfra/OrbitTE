//! Installed multi-node fabric exercise. The driver supplies a verified fabric domain and GPU.
use orbit_te::cuda::CudaMemory;
use orbit_te::{
    EngineConfig, Error, HostMemory, MnnvlConfig, Permission, RegisteredMemory, Result,
    SourceEpoch, TransferEngine, TransferOp, TransferRequest, Transport,
};
use std::{
    io::{self, BufRead, Write},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

const N: usize = (4 << 20) + 37;
const SIZE: usize = N + 32;
const WAIT: Duration = Duration::from_secs(60);
fn image(slot: usize, round: usize, guard: u8, offset: usize) -> Vec<u8> {
    let mut b = vec![guard; SIZE];
    for (i, v) in b[offset..offset + N].iter_mut().enumerate() {
        *v = ((i * 23 + slot * 71 + round * 97) % 251) as u8
    }
    b
}
fn verify(m: &RegisteredMemory, expected: &[u8]) -> Result<()> {
    let mut bytes = vec![0; m.len()];
    m.read(0, &mut bytes)?;
    if bytes != expected {
        return Err(Error::Protocol("MNNVL payload or guard mismatch"));
    }
    Ok(())
}
struct Buffers {
    device: Arc<CudaMemory>,
    host: Option<Arc<HostMemory>>,
    regions: Vec<RegisteredMemory>,
}
impl Buffers {
    fn new(e: &TransferEngine, egm: bool, numa: u32, permission: Permission) -> Result<Self> {
        let device = e.allocate_mnnvl(0, SIZE)?;
        let mut regions = e.register_cuda_memory_batch(&[(device.clone(), 0, SIZE)], permission)?;
        let host = egm.then(|| e.allocate_egm(numa, SIZE)).transpose()?;
        if let Some(memory) = &host {
            regions.push(e.register_host_memory(memory.clone(), 0, SIZE, permission)?);
        }
        Ok(Self {
            device,
            host,
            regions,
        })
    }

    fn release(self, e: &TransferEngine) -> Result<()> {
        for region in self.regions {
            e.unregister_memory(&region)?;
        }
        e.free_mnnvl(self.device)?;
        if let Some(host) = self.host {
            e.free_egm(host)?;
        }
        Ok(())
    }
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.as_slice() == ["--protocol"] {
        println!("orbitte-installed-mnnvl");
        return Ok(());
    }
    let [role, mode, domain, bind, numa, peer @ ..] = args.as_slice() else {
        return Err(Error::InvalidConfig(
            "expected role, profile, fabric domain, bind address and NUMA node",
        ));
    };
    let egm = match mode.as_str() {
        "device" => false,
        "egm" => true,
        _ => return Err(Error::InvalidConfig("unknown fabric profile")),
    };
    let numa = numa
        .parse()
        .map_err(|_| Error::InvalidConfig("invalid GPU NUMA node"))?;
    let engine = Arc::new(TransferEngine::new(EngineConfig {
        tcp: false,
        max_failover_attempts: 0,
        bind_addr: bind
            .parse()
            .map_err(|_| Error::InvalidConfig("invalid bind address"))?,
        mnnvl: Some(MnnvlConfig {
            fabric_domain: Some(domain.parse::<SourceEpoch>()?),
            egm,
            ..Default::default()
        }),
        ..Default::default()
    })?);
    match role.as_str() {
        "source" => {
            let backing = Buffers::new(&engine, egm, numa, Permission::RemoteReadWrite)?;
            let regs = &backing.regions;
            for (i, r) in regs.iter().enumerate() {
                r.write(0, &image(i, 0, 0xa5, 11))?
            }
            let addresses: Vec<_> = regs.iter().map(|r| r.base_address().to_string()).collect();
            println!(
                "READY {} {} {}",
                engine.endpoint(),
                engine.source_epoch(),
                addresses.join(" ")
            );
            io::stdout().flush()?;
            if io::stdin().lock().lines().next().transpose()?.as_deref() != Some("verify") {
                return Err(Error::Protocol("missing requester receipt"));
            }
            engine.close_source(WAIT)?;
            for (slot, r) in regs.iter().enumerate() {
                verify(r, &image(slot, regs.len(), 0xa5, 11))?;
            }
            backing.release(&engine)?;
            engine.close(WAIT)?;
            println!("PASS source {} {N}", if egm { 4 } else { 1 });
        }
        "requester" => {
            let [endpoint, epoch, addresses @ ..] = peer else {
                return Err(Error::InvalidConfig("missing source registration"));
            };
            let remote = engine.open_segment(
                endpoint
                    .parse::<SocketAddr>()
                    .map_err(|_| Error::InvalidConfig("invalid peer address"))?,
                epoch.parse()?,
                WAIT,
            )?;
            let backing = Buffers::new(&engine, egm, numa, Permission::LocalOnly)?;
            let local = &backing.regions;
            if addresses.len() != local.len() {
                return Err(Error::Protocol("wrong fabric directions"));
            }
            for (slot, address) in addresses.iter().enumerate() {
                let address = address
                    .parse::<u64>()
                    .map_err(|_| Error::Protocol("invalid source address"))?
                    .checked_add(11)
                    .ok_or(Error::InvalidRange)?;
                for (round, l) in local.iter().enumerate() {
                    l.write(0, &vec![0xc7; SIZE])?;
                    let q =
                        TransferRequest::new(Transport::Mnnvl, l.base_address() + 7, address, N);
                    for op in [TransferOp::Read, TransferOp::Write] {
                        if op == TransferOp::Write {
                            l.write(0, &image(slot, round + 1, 0xc7, 7))?
                        }
                        let b = engine.submit_async(
                            op,
                            &remote,
                            std::slice::from_ref(&q),
                            None,
                            WAIT,
                        )?;
                        if b.wait(WAIT)?.completed_tasks != 1 {
                            return Err(Error::Protocol("incomplete fabric batch"));
                        }
                        let t = b.task_status(0)?;
                        if t.transport != Transport::Mnnvl
                            || t.failover_count != 0
                            || !t.locally_drained
                            || t.transferred_bytes != N
                        {
                            return Err(Error::Protocol("wrong executing fabric owner"));
                        }
                        verify(
                            l,
                            &image(slot, round + usize::from(op == TransferOp::Write), 0xc7, 7),
                        )?;
                    }
                }
            }
            engine.close(WAIT)?;
            backing.release(&engine)?;
            println!("PASS requester {} {N}", if egm { 4 } else { 1 });
        }
        _ => return Err(Error::InvalidConfig("unknown fabric role")),
    };
    Ok(())
}
