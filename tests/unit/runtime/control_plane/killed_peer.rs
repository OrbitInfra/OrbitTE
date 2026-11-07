//! A real requester process killed after a partial payload, before its TCP socket closes.
use crate::test_support::EVENTUAL_PROGRESS;
use std::fmt::Write as _;
use std::{
    fs,
    io::{Read, Write},
    net::TcpStream,
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
    time::Instant,
};

pub(crate) struct KilledPeer {
    child: Child,
    marker: PathBuf,
}
impl KilledPeer {
    pub(crate) fn start(endpoint: std::net::SocketAddr, header: &[u8], reply_bytes: usize) -> Self {
        let marker = std::env::temp_dir().join(format!(
            "orbite-p10-{}",
            crate::SourceEpoch::generate().unwrap()
        ));
        let mut encoded = String::with_capacity(header.len() * 2);
        for byte in header {
            write!(&mut encoded, "{byte:02x}").unwrap();
        }
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runtime::control_plane::tests::killed_peer::payload_child",
                "--ignored",
            ])
            .env("ORBITE_P10_PEER", endpoint.to_string())
            .env("ORBITE_P10_HEADER", encoded)
            .env("ORBITE_P10_REPLY", reply_bytes.to_string())
            .env("ORBITE_P10_MARKER", &marker)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let mut peer = Self { child, marker };
        let end = Instant::now() + EVENTUAL_PROGRESS;
        while !peer.marker.exists() {
            assert!(
                peer.child.try_wait().unwrap().is_none(),
                "partial requester exited early"
            );
            assert!(
                Instant::now() < end,
                "partial requester did not reach payload boundary"
            );
            thread::sleep(std::time::Duration::from_millis(1));
        }
        peer
    }
    pub(crate) fn kill(&mut self) {
        self.child.kill().unwrap();
        assert!(!self.child.wait().unwrap().success());
    }
}
impl Drop for KilledPeer {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        let _ = fs::remove_file(&self.marker);
    }
}
#[test]
#[ignore = "spawned by P10 peer-kill controls"]
fn payload_child() {
    let mut stream = TcpStream::connect_timeout(
        &std::env::var("ORBITE_P10_PEER").unwrap().parse().unwrap(),
        EVENTUAL_PROGRESS,
    )
    .unwrap();
    stream.set_read_timeout(Some(EVENTUAL_PROGRESS)).unwrap();
    stream.set_write_timeout(Some(EVENTUAL_PROGRESS)).unwrap();
    let encoded = std::env::var("ORBITE_P10_HEADER").unwrap();
    let header: Vec<_> = (0..encoded.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&encoded[i..i + 2], 16).unwrap())
        .collect();
    stream.write_all(&header).unwrap();
    let n: usize = std::env::var("ORBITE_P10_REPLY").unwrap().parse().unwrap();
    stream.read_exact(&mut vec![0; n]).unwrap();
    stream.write_all(b"part").unwrap();
    fs::write(std::env::var_os("ORBITE_P10_MARKER").unwrap(), b"posted").unwrap();
    // The parent kills us while the native socket still owns a partial WRITE.
    let mut end = [0];
    let _ = std::io::stdin().read(&mut end);
}
