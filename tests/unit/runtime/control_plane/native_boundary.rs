use super::*;
use std::cell::RefCell;
use std::process::{Command, Stdio};

thread_local! {
    // Only the next engine on this test thread observes handler completion.
    static OBSERVER: RefCell<Option<mpsc::SyncSender<Result<()>>>> = const { RefCell::new(None) };
}

pub(crate) fn take_observer() -> Option<mpsc::SyncSender<Result<()>>> {
    OBSERVER.with(|observer| observer.take())
}

fn observed_engine() -> (TransferEngine, mpsc::Receiver<Result<()>>) {
    let (sender, receiver) = mpsc::sync_channel(4);
    OBSERVER.with(|observer| assert!(observer.replace(Some(sender)).is_none()));
    (
        TransferEngine::new(EngineConfig::default()).unwrap(),
        receiver,
    )
}

fn assert_rejected(source: &TransferEngine, results: &mpsc::Receiver<Result<()>>) {
    assert!(matches!(
        results.recv_timeout(common::EVENTUAL_PROGRESS).unwrap(),
        Err(Error::Protocol("OrbiTE control magic mismatch"))
    ));
    let stats = source.stats().unwrap();
    assert_eq!(stats.inflight_batches, 0);
    assert_eq!(stats.inflight_bytes, 0);
    assert_eq!(stats.active_peer_operations, 0);
    assert_eq!(stats.active_source_operations, 0);
    assert_eq!(stats.retained_notifications, 0);
}

#[test]
fn frozen_rpc_prefix_is_rejected_before_a_native_header_or_payload_is_read() {
    let (source, results) = observed_engine();
    let memory = source
        .register_memory(vec![0xa5; 32].into(), Permission::RemoteReadWrite)
        .unwrap();
    let corpus: serde_json::Value =
        serde_json::from_str(include_str!("../../../reference/wire/rpc.json")).unwrap();
    for name in ["GetSegmentDesc", "SendData", "RecvData"] {
        let header = corpus[name]["request"]["header_hex"].as_str().unwrap();
        let prefix: Vec<u8> = (0..16)
            .step_by(2)
            .map(|i| u8::from_str_radix(&header[i..i + 2], 16).unwrap())
            .collect();
        let mut channel = ControlChannel::connect(
            source.endpoint(),
            deadline(common::EVENTUAL_PROGRESS).unwrap(),
        )
        .unwrap();
        channel.write_all(&prefix).unwrap();
        // No native-sized header, EOF, or payload is needed to reject the peer.
        assert_rejected(&source, &results);
        assert!(matches!(channel.read_exact(&mut [0]), Err(Error::Io(e))
            if e.kind() == io::ErrorKind::UnexpectedEof));
    }
    let mut bytes = [0; 32];
    memory.read(0, &mut bytes).unwrap();
    assert_eq!(bytes, [0xa5; 32]);
    // Rejecting a foreign protocol does not stop the native acceptor.
    let requester = TransferEngine::new(EngineConfig::default()).unwrap();
    let peer = requester
        .open_segment(
            source.endpoint(),
            source.source_epoch(),
            common::EVENTUAL_PROGRESS,
        )
        .unwrap();
    assert_eq!(peer.regions()[0].id(), memory.id());
}

#[test]
#[ignore = "run by the frozen CPU oracle with ORBITE_TEST_FROZEN_CLIENT"]
fn frozen_tent_client_is_rejected_without_admission_or_payload() {
    let binary = std::env::var_os("ORBITE_TEST_FROZEN_CLIENT").expect("frozen client required");
    let (source, results) = observed_engine();
    let memory = source
        .register_memory(vec![0xa5; 32].into(), Permission::RemoteReadWrite)
        .unwrap();
    // Invoke actual frozen ControlClient descriptor, WRITE and READ calls.
    for operation in [1, 3, 4] {
        let mut child = Command::new(&binary)
            .args([
                "client",
                &operation.to_string(),
                &source.endpoint().to_string(),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let until = Instant::now() + common::EVENTUAL_PROGRESS;
        while child.try_wait().unwrap().is_none() {
            if Instant::now() >= until {
                child.kill().unwrap();
                let output = child.wait_with_output().unwrap();
                panic!("frozen client did not observe rejection: {output:?}");
            }
            thread::sleep(Duration::from_millis(10));
        }
        let output = child.wait_with_output().unwrap();
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        assert_rejected(&source, &results);
        let mut bytes = [0; 32];
        memory.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, [0xa5; 32]);
    }
}
