//! Actual source-process death, lease-preserving restart and old-epoch rejection.
use super::*;
use std::net::SocketAddr;
use std::path::PathBuf;

fn file(name: &str) -> PathBuf {
    PathBuf::from(env::var("ORBITTE_RESTART_ROOT").unwrap()).join(name)
}

pub(super) fn generation(mode: &str) -> usize {
    if mode != "large-peer-exit" {
        return 2;
    }
    let generation = env::var("ORBITTE_RESTART_GENERATION")
        .unwrap()
        .parse()
        .unwrap();
    assert!(generation <= 1);
    generation
}

pub(super) fn bind(source: bool, generation: usize) -> SocketAddr {
    if source && generation == 1 {
        let text = std::fs::read_to_string(file("restart-request.txt")).unwrap();
        return text.split_whitespace().nth(1).unwrap().parse().unwrap();
    }
    env::var("ORBITTE_SHARED_BIND").unwrap().parse().unwrap()
}

pub(super) fn control(source: bool, generation: usize) -> TcpStream {
    if source || generation != 1 {
        return super::super::control(source);
    }
    let path = file("restart-peer.txt");
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

pub(super) fn remember(peer: &crate::RemoteSegment) {
    let region = peer.regions().first().unwrap();
    assert_eq!(peer.regions().len(), 1);
    std::fs::write(
        file("restart-peer-state.txt"),
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

pub(super) fn terminate(engine: &TransferEngine, memory: &crate::RegisteredMemory) -> ! {
    std::fs::write(
        env::var("ORBITTE_SHARED_MAPS").unwrap(),
        std::fs::read("/proc/self/maps").unwrap(),
    )
    .unwrap();
    let marker = file("restart-request.txt");
    let temporary = marker.with_extension("tmp");
    std::fs::write(
        &temporary,
        format!(
            "{} {} {} {}\n",
            std::process::id(),
            engine.endpoint(),
            engine.source_epoch(),
            memory.base_address()
        ),
    )
    .unwrap();
    println!(
        "ORBITTE_RESTART_KILL_READY {{\"pid\":{},\"epoch\":\"{}\",\"endpoint\":\"{}\",\"active_grants\":{},\"rq_debt\":{}}}",
        std::process::id(),
        engine.source_epoch(),
        engine.endpoint(),
        engine.stats().unwrap().rdma.unwrap().active_grants,
        engine.stats().unwrap().rdma.unwrap().receive_work_requests
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
    if let Some(peer) = peer {
        let text = std::fs::read_to_string(file("restart-peer-state.txt")).unwrap();
        let fields = text.split_whitespace().collect::<Vec<_>>();
        assert_eq!(fields.len(), 5);
        let endpoint = fields[0].parse().unwrap();
        let epoch = fields[1].parse().unwrap();
        let address = fields[3].parse().unwrap();
        assert_eq!(
            peer.endpoint(),
            endpoint,
            "restart must rebind the same public RPC endpoint"
        );
        assert_eq!(
            peer.regions()[0].id(),
            fields[2].parse::<u64>().unwrap(),
            "the replacement must exercise the old numeric registration ID"
        );
        assert_ne!(
            peer.source_epoch(),
            epoch,
            "a new process reused source authority"
        );
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
        let before = engine.stats().unwrap().rdma.unwrap().opened_connections;
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
        let mut actual = vec![0; shape.length];
        memory.read(0, &mut actual).unwrap();
        assert_eq!(actual, original);
        println!(
            "ORBITTE_RESTART_STALE {{\"old_epoch\":\"{epoch}\",\"new_epoch\":\"{}\",\"same_endpoint\":true,\"opened_qps\":0,\"remote_effect_none\":true}}",
            peer.source_epoch()
        );
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
