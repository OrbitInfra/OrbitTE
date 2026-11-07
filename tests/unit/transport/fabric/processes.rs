use super::native::cfg;
use super::shim::{self, Faults};
use crate::test_support::EVENTUAL_PROGRESS as WAIT;
use crate::*;
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, atomic::Ordering::SeqCst},
    thread,
    time::{Duration, Instant},
};
const CHILD: &str = "transport::fabric::tests::processes::fabric_child";
const LEN: usize = 131075;
struct Process {
    child: Child,
    dir: PathBuf,
}
fn wait(f: impl Fn() -> bool) {
    let end = Instant::now() + WAIT;
    while !f() {
        assert!(Instant::now() < end);
        thread::sleep(Duration::from_millis(1));
    }
}
fn put(path: &Path, bytes: impl AsRef<[u8]>) {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, bytes).unwrap();
    fs::rename(tmp, path).unwrap();
}
impl Process {
    fn start() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "orbite-fabric-{}-{}",
            std::process::id(),
            SourceEpoch::generate().unwrap()
        ));
        fs::create_dir(&dir).unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", CHILD, "--ignored", "--nocapture"])
            .env("ORBITE_FABRIC_CHILD", &dir)
            .stdin(Stdio::null())
            .spawn()
            .unwrap();
        let p = Self { child, dir };
        wait(|| p.dir.join("ready").exists());
        p
    }
    fn peer(&self, e: &TransferEngine) -> RemoteSegment {
        let line = fs::read_to_string(self.dir.join("ready")).unwrap();
        let p: Vec<_> = line.split_whitespace().collect();
        e.open_segment(p[0].parse().unwrap(), p[1].parse().unwrap(), WAIT)
            .unwrap()
    }
    fn finish(&mut self) {
        put(&self.dir.join("finish"), []);
        let end = Instant::now() + WAIT;
        loop {
            if let Some(s) = self.child.try_wait().unwrap() {
                assert!(s.success());
                break;
            }
            assert!(Instant::now() < end);
            thread::sleep(Duration::from_millis(1));
        }
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        let _ = fs::remove_dir_all(&self.dir);
    }
}
#[test]
#[ignore = "spawned by real libfabric two-process controls"]
fn fabric_child() {
    let dir = PathBuf::from(std::env::var_os("ORBITE_FABRIC_CHILD").unwrap());
    let e = TransferEngine::new(cfg()).unwrap();
    let mem = e
        .register_memory(vec![0x42; LEN + 2].into(), Permission::RemoteReadWrite)
        .unwrap();
    put(
        &dir.join("ready"),
        format!("{} {}", e.endpoint(), e.source_epoch()),
    );
    wait(|| dir.join("finish").exists());
    e.close_source(WAIT).unwrap();
    let mut got = vec![0; LEN + 2];
    mem.read(0, &mut got).unwrap();
    assert_eq!(got[0], 0x42);
    assert_eq!(got[LEN + 1], 0x42);
    assert!(got[1..=LEN].iter().all(|&b| b == 0xa5));
    e.close(WAIT).unwrap();
}
#[test]
fn two_process_provider_read_write_with_guards() {
    let mut p = Process::start();
    let e = TransferEngine::new(cfg()).unwrap();
    let peer = p.peer(&e);
    let remote = &peer.regions()[0];
    let local = e
        .register_memory(vec![0xee; LEN + 2].into(), Permission::LocalOnly)
        .unwrap();
    local.write(1, &vec![0xa5; LEN]).unwrap();
    let request = TransferRequest::new(
        Transport::Fabric,
        local.base_address() + 1,
        remote.base_address() + 1,
        LEN,
    );
    for op in [TransferOp::Write, TransferOp::Read] {
        if op == TransferOp::Read {
            local.write(1, &vec![0; LEN]).unwrap();
        }
        let r = e
            .submit_and_wait(op, &peer, std::slice::from_ref(&request), WAIT)
            .unwrap();
        assert!(r.is_complete(), "{r:?}");
        assert_eq!(r.transferred_bytes, LEN);
    }
    let mut got = vec![0; LEN + 2];
    local.read(0, &mut got).unwrap();
    assert_eq!(got[0], 0xee);
    assert_eq!(got[LEN + 1], 0xee);
    assert!(got[1..=LEN].iter().all(|&b| b == 0xa5));
    p.finish();
    e.close(WAIT).unwrap();
}
#[test]
fn peer_exit_after_post_is_not_a_release_receipt() {
    let mut p = Process::start();
    let f = Arc::new(Faults::default());
    f.hold.store(true, SeqCst);
    let e = Arc::new(shim::configured(f.clone(), || TransferEngine::new(cfg())).unwrap());
    let peer = p.peer(&e);
    let local = e
        .register_memory(vec![0xee; 10].into(), Permission::LocalOnly)
        .unwrap();
    let request = TransferRequest::new(
        Transport::Fabric,
        local.base_address() + 1,
        peer.regions()[0].base_address() + 1,
        8,
    );
    let b = e
        .submit_async(TransferOp::Read, &peer, &[request], None, WAIT)
        .unwrap();
    wait(|| f.intercepted.load(SeqCst) == 1);
    p.child.kill().unwrap();
    assert!(!p.child.wait().unwrap().success());
    f.hold.store(false, SeqCst);
    assert_eq!(b.wait(WAIT).unwrap().state, BatchState::Failed);
    assert!(!b.task_status(0).unwrap().locally_drained);
    assert!(local.read(1, &mut [0; 1]).is_err());
    let mut guard = [0; 1];
    local.read(0, &mut guard).unwrap();
    assert_eq!(guard, [0xee]);
    local.read(9, &mut guard).unwrap();
    assert_eq!(guard, [0xee]);
    assert!(e.stats().unwrap().fabric.unwrap().memory_regions > 0);
}
