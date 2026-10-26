//! Exact-child death, repeated RPC rebinding and generation-scoped authority.
use super::*;
use std::net::SocketAddr;
use std::path::PathBuf;

fn file(name: &str, generation: usize) -> PathBuf {
    PathBuf::from(env::var("ORBITTE_RESTART_ROOT").unwrap())
        .join(format!("restart-{name}-{generation}.txt"))
}

pub(super) fn generation(mode: &str) -> Option<usize> {
    if !matches!(
        mode,
        "large-peer-exit"
            | "large-peer-restart"
            | "large-requester-exit"
            | "large-requester-restart"
            | "large-cq-busy"
            | "large-pd-busy"
            | "large-qp-cleanup-error"
            | "large-channel-busy"
    ) {
        return None;
    }
    let generation = env::var("ORBITTE_RESTART_GENERATION")
        .unwrap()
        .parse()
        .unwrap();
    assert!(generation < generations());
    Some(generation)
}

pub(super) fn generations() -> usize {
    let count = env::var("ORBITTE_RESTART_GENERATIONS")
        .unwrap()
        .parse()
        .unwrap();
    assert!(matches!(count, 2 | 3));
    count
}

pub(super) fn dying(source: bool, generation: Option<usize>) -> bool {
    generation.is_some_and(|g| g + 1 < generations())
        && (source == (env::var("ORBITTE_RESTART_KILL_ROLE").as_deref() == Ok("source")))
}

pub(super) fn interrupted(mode: &str, generation: Option<usize>) -> bool {
    generation.is_some_and(|g| g + 1 < generations())
        && matches!(
            mode,
            "large-peer-exit"
                | "large-peer-restart"
                | "large-requester-exit"
                | "large-requester-restart"
        )
}

pub(super) fn cleanup(mode: &str, generation: Option<usize>) -> Option<&'static str> {
    if generation.is_none_or(|g| g + 1 == generations()) {
        return None;
    }
    match mode {
        "large-cq-busy" => Some("cq"),
        "large-pd-busy" => Some("pd"),
        "large-qp-cleanup-error" => Some("qp"),
        "large-channel-busy" => Some("channel"),
        _ => None,
    }
}

pub(super) fn mode(mode: &str, generation: Option<usize>) -> &str {
    if generation.is_some_and(|g| g + 1 == generations()) {
        return "large-quota";
    }
    match mode {
        "large-cq-busy" | "large-qp-cleanup-error" | "large-channel-busy"
            if env::var("ORBITTE_RESTART_KILL_ROLE").as_deref() == Ok("source") =>
        {
            "large-lost-recv-cqe"
        }
        "large-cq-busy" | "large-qp-cleanup-error" | "large-channel-busy" => "large-lost-send-cqe",
        "large-pd-busy" => "large-quota",
        "large-peer-restart" => "large-peer-exit",
        "large-requester-restart" => "large-requester-exit",
        _ => mode,
    }
}

pub(super) fn state(engine: &TransferEngine, memory: &crate::RegisteredMemory, generation: usize) {
    std::fs::write(
        file("state", generation),
        format!(
            "{} {} {} {} {} {}\n",
            std::process::id(),
            engine.endpoint(),
            engine.source_epoch(),
            memory.id(),
            memory.base_address(),
            memory.len()
        ),
    )
    .unwrap();
}

pub(super) fn bind(generation: Option<usize>) -> SocketAddr {
    if generation.is_some_and(|g| g > 0) {
        let text = std::fs::read_to_string(file("state", 0)).unwrap();
        return text.split_whitespace().nth(1).unwrap().parse().unwrap();
    }
    env::var("ORBITTE_SHARED_BIND").unwrap().parse().unwrap()
}

pub(super) fn control(source: bool, generation: Option<usize>) -> TcpStream {
    if source || generation.is_none_or(|g| g == 0) {
        return super::super::control(source);
    }
    let path = file("peer", generation.unwrap());
    let deadline = Instant::now() + Duration::from_secs(20);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "new source process was not announced"
        );
        thread::sleep(Duration::from_millis(10));
    }
    let stream = TcpStream::connect_timeout(
        &std::fs::read_to_string(path).unwrap().parse().unwrap(),
        Duration::from_secs(5),
    )
    .unwrap();
    stream.set_read_timeout(Some(TIMEOUT)).unwrap();
    stream.set_write_timeout(Some(TIMEOUT)).unwrap();
    stream
}

pub(super) fn remember(peer: &crate::RemoteSegment, generation: usize) {
    let region = peer.regions().first().unwrap();
    assert_eq!(peer.regions().len(), 1);
    std::fs::write(
        file("peer-state", generation),
        format!(
            "{} {} {} {} {}\n",
            peer.endpoint(),
            peer.source_epoch(),
            region.id(),
            region.base_address(),
            region.len()
        ),
    )
    .unwrap();
}

pub(super) fn peer_exited(stream: &mut TcpStream) {
    let mut byte = [0];
    match stream.read(&mut byte) {
        Ok(0) => {}
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::UnexpectedEof
            ) => {}
        other => panic!("expected peer process exit, not a timeout: {other:?}"),
    }
}

pub(super) fn terminate(source: bool, generation: usize, phase: &str, stats: &RdmaStats) -> ! {
    std::fs::write(
        env::var("ORBITTE_SHARED_MAPS").unwrap(),
        std::fs::read("/proc/self/maps").unwrap(),
    )
    .unwrap();
    let state = std::fs::read_to_string(file("state", generation)).unwrap();
    let fields = state.split_whitespace().collect::<Vec<_>>();
    assert_eq!(fields.len(), 6);
    assert_eq!(fields[0].parse::<u32>().unwrap(), std::process::id());
    let marker = file("request", generation);
    let temporary = marker.with_extension("tmp");
    std::fs::write(&temporary, format!("{} {generation}\n", state.trim())).unwrap();
    println!(
        "ORBITTE_RESTART_KILL_READY {{\"source\":{source},\"generation\":{generation},\"phase\":\"{phase}\",\"pid\":{},\"epoch\":\"{}\",\"endpoint\":\"{}\",\"region_id\":{},\"active_grants\":{},\"rq_debt\":{},\"sq_debt\":{}}}",
        std::process::id(),
        fields[2],
        fields[1],
        fields[3],
        stats.active_grants,
        stats.receive_work_requests,
        stats.work_requests
    );
    std::io::stdout().flush().unwrap();
    std::fs::rename(temporary, marker).unwrap();
    loop {
        thread::park_timeout(Duration::from_secs(1));
    }
}

pub(super) fn stale(
    stream: &mut TcpStream,
    engine: &TransferEngine,
    memory: &crate::RegisteredMemory,
    peer: Option<&crate::RemoteSegment>,
    shape: Shape,
    operation: TransferOp,
    original: &[u8],
) {
    let generation = generation(&env::var("ORBITTE_RAIL_MODE").unwrap()).unwrap();
    if let Some(peer) = peer {
        let before = engine.stats().unwrap().rdma.unwrap().opened_connections;
        let mut epochs = std::collections::HashSet::new();
        for previous in 0..generation {
            let text = std::fs::read_to_string(file("peer-state", previous)).unwrap();
            let fields = text.split_whitespace().collect::<Vec<_>>();
            assert_eq!(fields.len(), 5);
            let endpoint = fields[0].parse().unwrap();
            let epoch = fields[1].parse().unwrap();
            let address = fields[3].parse().unwrap();
            assert_eq!(
                peer.endpoint(),
                endpoint,
                "replacement changed the RPC address"
            );
            assert_eq!(peer.regions()[0].id(), fields[2].parse::<u64>().unwrap());
            assert_ne!(
                peer.source_epoch(),
                epoch,
                "a new process reused source authority"
            );
            assert!(epochs.insert(epoch), "old processes reused an epoch");
            assert!(matches!(
                engine.open_segment(endpoint, epoch, TIMEOUT),
                Err(Error::EpochMismatch)
            ));
            let stale = crate::RemoteSegment {
                endpoint,
                epoch,
                regions: vec![crate::RemoteRegion {
                    epoch,
                    id: fields[2].parse().unwrap(),
                    address,
                    length: fields[4].parse().unwrap(),
                    permission: Permission::RemoteReadWrite,
                }],
            };
            let report = transfer(
                shape,
                engine,
                &stale,
                memory.base_address(),
                address,
                operation,
                0,
            );
            assert!(report.locally_drained && !report.is_complete());
            let failure = report.failure.unwrap();
            assert!(matches!(failure.error, Error::EpochMismatch));
            assert_eq!(failure.remote_effect, RemoteEffect::None);
            assert_eq!(
                before,
                engine.stats().unwrap().rdma.unwrap().opened_connections
            );
            println!(
                "ORBITTE_RESTART_STALE {{\"generation\":{generation},\"previous_generation\":{previous},\"old_epoch\":\"{epoch}\",\"new_epoch\":\"{}\",\"same_endpoint\":true,\"same_region_id\":true,\"opened_qps\":0,\"remote_effect_none\":true}}",
                peer.source_epoch()
            );
        }
        let mut actual = vec![0; shape.length];
        memory.read(0, &mut actual).unwrap();
        assert_eq!(actual, original);
        stream.write_all(&[0x60]).unwrap();
        token(stream, 0x61);
    } else {
        token(stream, 0x60);
        assert_eq!(engine.stats().unwrap().rdma.unwrap().opened_connections, 0);
        let mut actual = vec![0; shape.length];
        memory.read(0, &mut actual).unwrap();
        assert_eq!(
            actual, original,
            "old authority modified the replacement allocation"
        );
        stream.write_all(&[0x61]).unwrap();
    }
}
