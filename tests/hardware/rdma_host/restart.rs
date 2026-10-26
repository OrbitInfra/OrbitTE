//! Repeated peer death with one live engine, persistent MRs and stale authority.

use std::path::PathBuf;
use std::sync::Arc;

use orbit_te::{Error, RemoteEffect};

use super::*;

fn file(kind: &str, phase: usize) -> Result<PathBuf> {
    Ok(
        PathBuf::from(env::var("ORBITTE_RESTART_ROOT")?)
            .join(format!("restart-{kind}-{phase}.txt")),
    )
}

fn wait_peer(phase: usize) -> Result<SocketAddr> {
    let path = file("peer", phase)?;
    let deadline = Instant::now() + TIMEOUT;
    while !path.exists() {
        if Instant::now() >= deadline {
            return Err("replacement control address was not announced".into());
        }
        thread::sleep(Duration::from_millis(10));
    }
    Ok(std::fs::read_to_string(path)?.parse()?)
}

fn peer_exited(stream: &mut TcpStream) -> Result<()> {
    match stream.read(&mut [0]) {
        Ok(0) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
            ) =>
        {
            Ok(())
        }
        other => Err(format!("expected peer exit, not a timeout: {other:?}").into()),
    }
}

fn stale(
    case: &Case,
    engine: &TransferEngine,
    memory: &RegisteredMemory,
    old: &RemoteSegment,
    new: &RemoteSegment,
    phase: usize,
    previous: usize,
) -> Result<()> {
    if old.endpoint() != new.endpoint() || old.source_epoch() == new.source_epoch() {
        return Err("replacement did not rebind the address with fresh authority".into());
    }
    let before = idle(engine)?;
    if !matches!(
        engine.open_segment(old.endpoint(), old.source_epoch(), TIMEOUT),
        Err(Error::EpochMismatch)
    ) {
        return Err("old epoch reopened the replacement".into());
    }
    let report = engine.submit_and_wait(
        if case.read {
            TransferOp::Read
        } else {
            TransferOp::Write
        },
        old,
        &[TransferRequest {
            transport: Transport::Rdma,
            local_address: memory.base_address()
                + if case.read { case.destination() } else { GUARD } as u64,
            remote_address: old.regions()[0].base_address()
                + if case.read { GUARD } else { case.destination() } as u64,
            length: case.payload,
        }],
        TIMEOUT,
    )?;
    let failure = report.failure.ok_or("stale request succeeded")?;
    let after = idle(engine)?;
    if !matches!(failure.error, Error::EpochMismatch)
        || failure.remote_effect != RemoteEffect::None
        || report.transferred_bytes != 0
        || !report.locally_drained
        || before.opened_connections != after.opened_connections
        || before.posted_work_requests != after.posted_work_requests
    {
        return Err(format!("stale request crossed authority fence: {failure:?}").into());
    }
    println!(
        "ORBITTE_INSTALLED_STALE {{\"phase\":{phase},\"previous\":{previous},\"old_epoch\":\"{}\",\"new_epoch\":\"{}\",\"opened_qps\":0,\"posted_wrs\":0,\"remote_effect_none\":true,\"locally_drained\":true}}",
        old.source_epoch(),
        new.source_epoch()
    );
    Ok(())
}

fn terminate(engine: &TransferEngine, phase: usize, state: &str, pending: bool) -> Result<()> {
    let stats = if pending {
        engine.stats()?.rdma.ok_or("missing RDMA counters")?
    } else {
        idle(engine)?
    };
    if stats.active_endpoints == 0 || stats.persistent_mrs != 4 {
        return Err("kill point lacks cached QPs and persistent MRs".into());
    }
    maps()?;
    println!(
        "ORBITTE_INSTALLED_KILL {{\"phase\":{phase},\"pid\":{},\"cached_qps\":{},\"persistent_mrs\":{},\"sq_debt\":{},\"rq_debt\":{},\"orderly_close\":false}}",
        std::process::id(),
        stats.active_endpoints,
        stats.persistent_mrs,
        stats.work_requests,
        stats.receive_work_requests
    );
    std::io::stdout().flush()?;
    let marker = file("request", phase)?;
    let temporary = marker.with_extension("tmp");
    std::fs::write(&temporary, format!("{state} {phase}\n"))?;
    std::fs::rename(temporary, marker)?;
    loop {
        thread::park_timeout(Duration::from_secs(1));
    }
}

pub(super) fn run(case: &Case) -> Result<()> {
    let mode = env::var("ORBITTE_HOST_MODE")?;
    let lost_completion = mode == "installed-lost-completion";
    let interrupted = mode == "installed-interruption" || lost_completion;
    if interrupted && case.payload > case.block {
        return Err("interruption relay requires one slice per parent".into());
    }
    let killed_source = match env::var("ORBITTE_RESTART_KILL_ROLE")?.as_str() {
        "source" => true,
        "requester" => false,
        _ => return Err("invalid restart role".into()),
    };
    let generations = number("ORBITTE_RESTART_GENERATIONS")?;
    let generation = number("ORBITTE_RESTART_GENERATION")?;
    let target = case.source == killed_source;
    if !(2..=3).contains(&generations) || generation >= generations || (!target && generation != 0)
    {
        return Err("invalid live-peer restart profile".into());
    }
    let bind = if generation == 0 {
        env::var("ORBITTE_SHARED_BIND")?.parse()?
    } else {
        let state = std::fs::read_to_string(file("state", 0)?)?;
        state
            .split_whitespace()
            .nth(1)
            .ok_or("missing original service address")?
            .parse()?
    };
    let (engine, memory) = create(case, bind)?;
    let engine = Arc::new(engine);
    let gate = if interrupted && !lost_completion && case.source {
        let gate_bind = if generation == 0 {
            bind
        } else {
            std::fs::read_to_string(file("gate", 0)?)?.parse()?
        };
        let gate = gate::GrantGate::new(gate_bind, engine.endpoint())?;
        std::fs::write(file("gate", generation)?, gate.address().to_string())?;
        Some(gate)
    } else {
        None
    };
    let fault = if lost_completion && !case.source {
        Some(completion::CompletionFault::load()?)
    } else {
        None
    };
    let advertised = gate
        .as_ref()
        .map_or(engine.endpoint(), gate::GrantGate::address);
    let state = format!(
        "{} {} {} {} {} {}",
        std::process::id(),
        engine.endpoint(),
        engine.source_epoch(),
        memory.id(),
        memory.base_address(),
        memory.len()
    );
    std::fs::write(file("state", generation)?, &state)?;
    let mut old_peers: Vec<RemoteSegment> = Vec::new();
    let end = if target { generation + 1 } else { generations };
    for phase in generation..end {
        let peer_address = if !case.source && phase > 0 {
            Some(wait_peer(phase)?)
        } else {
            None
        };
        // The control listener uses an ephemeral port, independent of the
        // engine's service address that the replacement must reuse.
        let mut stream = control(
            case.source,
            env::var("ORBITTE_SHARED_BIND")?.parse()?,
            peer_address,
        )?;
        barrier(
            &mut stream,
            &format!("restart:{killed_source}:{generations}:{phase}"),
        )?;
        let peer = open_peer(case, &engine, &memory, &mut stream, advertised)?;
        let region = &peer.regions()[0];
        let peer_state = format!(
            "{} {} {} {} {}",
            peer.endpoint(),
            peer.source_epoch(),
            region.id(),
            region.base_address(),
            region.len()
        );
        if target && phase > 0 && std::fs::read_to_string(file("peer-state", 0)?)? != peer_state {
            return Err("surviving engine or registration changed across peer death".into());
        }
        std::fs::write(file("peer-state", phase)?, &peer_state)?;
        let original = case.bytes(case.source, phase * ROUNDS);
        memory.write(0, &original)?;
        barrier(&mut stream, "stale-ready")?;
        for (previous, old) in old_peers.iter().enumerate() {
            stale(case, &engine, &memory, old, &peer, phase, previous)?;
        }
        barrier(&mut stream, "stale-finished")?;
        let mut actual = vec![0; case.length()];
        memory.read(0, &mut actual)?;
        if actual != original {
            return Err("stale authority changed a local or replacement byte".into());
        }
        let before = idle(&engine)?;
        println!(
            "ORBITTE_INSTALLED_PHASE {{\"phase\":{phase},\"pid\":{},\"endpoint\":\"{}\",\"epoch\":\"{}\",\"region_id\":{},\"address\":{},\"peer_endpoint\":\"{}\",\"peer_epoch\":\"{}\",\"opened_qps\":{},\"opened_mrs\":{},\"read_bytes\":{},\"write_bytes\":{},\"stale_bytes_unchanged\":true}}",
            std::process::id(),
            advertised,
            engine.source_epoch(),
            memory.id(),
            memory.base_address(),
            peer.endpoint(),
            peer.source_epoch(),
            before.opened_connections,
            before.opened_persistent_mrs,
            before.completed_read_bytes,
            before.completed_write_bytes
        );
        rounds(case, &engine, &memory, &peer, &mut stream, phase)?;
        maps()?;
        if phase + 1 < generations {
            let pending = if interrupted {
                interruption::pending(
                    case,
                    &engine,
                    &memory,
                    &peer,
                    &mut stream,
                    (gate.as_ref(), fault.as_ref()),
                    phase,
                )?
            } else {
                Vec::new()
            };
            barrier(&mut stream, "kill-ready")?;
            if target {
                // Keep public batch handles and all native owners alive until SIGKILL.
                let outcome = terminate(&engine, phase, &state, interrupted);
                drop(pending);
                return outcome;
            }
            peer_exited(&mut stream)?;
            if interrupted {
                interruption::drained(
                    case,
                    &engine,
                    &memory,
                    pending,
                    (gate.as_ref(), fault.as_ref()),
                    phase,
                )?;
            }
            idle(&engine)?;
            println!(
                "ORBITTE_INSTALLED_PEER_EXIT {{\"phase\":{phase},\"survivor_pid\":{},\"locally_idle\":true}}",
                std::process::id()
            );
            old_peers.push(peer);
        } else {
            close(&engine, memory)?;
            if let Some(gate) = gate {
                gate.close()?;
            }
            barrier(&mut stream, "closed")?;
            return result(case);
        }
    }
    Err("restart sequence did not reach a terminal outcome".into())
}
