use super::*;
use std::process::{Child, Command, Stdio};
const CHILD: &str = "transport::shm::tests::processes::shm_child";
struct Process(Child);
impl Process {
    fn start(role: &str, dir: &Path, mount: &Path) -> Self {
        let c = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", CHILD, "--ignored", "--nocapture"])
            .env("ORBITTE_SHM_ROLE", role)
            .env("ORBITTE_SHM_CONTROL", dir)
            .env("ORBITTE_SHM_MOUNT", mount)
            .stdin(Stdio::null())
            .spawn()
            .unwrap();
        Self(c)
    }
    fn at(&mut self, marker: &Path) {
        let end = Instant::now() + WAIT;
        while !marker.exists() {
            assert!(
                self.0.try_wait().unwrap().is_none(),
                "child exited before {marker:?}"
            );
            assert!(Instant::now() < end, "child did not reach {marker:?}");
            thread::sleep(Duration::from_millis(1));
        }
    }
    fn finish(&mut self) {
        let end = Instant::now() + WAIT;
        loop {
            if let Some(s) = self.0.try_wait().unwrap() {
                assert!(s.success(), "{s}");
                return;
            }
            assert!(Instant::now() < end);
            thread::sleep(Duration::from_millis(1));
        }
    }
    fn kill(&mut self) {
        self.0.kill().unwrap();
        assert!(!self.0.wait().unwrap().success());
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}
fn wait(p: &Path) {
    let end = Instant::now() + WAIT;
    while !p.exists() {
        assert!(Instant::now() < end, "missing {p:?}");
        thread::sleep(Duration::from_millis(1));
    }
}
fn put(p: &Path, data: impl AsRef<[u8]>) {
    let tmp = p.with_extension("tmp");
    fs::write(&tmp, data).unwrap();
    fs::rename(tmp, p).unwrap();
}
fn config_path(cxl: bool, d: &Dir) -> &Path {
    if cxl { &d.0 } else { Path::new("") }
}
fn paused(fd: &fs::File, start: i64, len: i64, write: bool) -> io::Result<()> {
    (NATIVE.lock)(fd, start, len, write)?;
    if start < GATES {
        let dir = PathBuf::from(std::env::var_os("ORBITTE_SHM_CONTROL").unwrap());
        put(&dir.join("inside"), b"");
        wait(&dir.join("resume"));
    }
    Ok(())
}
static PAUSED: Sys = Sys {
    lock: paused,
    ..NATIVE
};

#[test]
#[ignore = "spawned by the cross-process SHM lifetime controls"]
fn shm_child() -> Result<()> {
    let role = std::env::var("ORBITTE_SHM_ROLE").unwrap();
    let dir = PathBuf::from(std::env::var_os("ORBITTE_SHM_CONTROL").unwrap());
    let mount = PathBuf::from(std::env::var_os("ORBITTE_SHM_MOUNT").unwrap());
    let mut cfg = config(&mount);
    cfg.tcp = false;
    if role == "source" {
        cfg.bind_addr = fs::read_to_string(dir.join("bind"))?.parse().unwrap();
        let src = engine(cfg, &NATIVE)?;
        let host = src.allocate_shm_memory(32)?;
        host.write(0, &[0x5a; 32])?;
        let region = grant(&src, &host, 3, 22, Permission::RemoteReadWrite);
        let name = host.shared().unwrap().mapping.name.basename();
        put(
            &dir.join("peer"),
            format!(
                "{} {} {} {}",
                src.endpoint(),
                src.source_epoch(),
                region.base_address(),
                name
            ),
        );
        wait(&dir.join("stop"));
        return Ok(());
    }
    let dst = engine(cfg, &PAUSED)?;
    let line = fs::read_to_string(dir.join("peer"))?;
    let p: Vec<_> = line.split_whitespace().collect();
    let peer = dst.open_segment(p[0].parse().unwrap(), p[1].parse()?, WAIT)?;
    let local = dst.register_memory(vec![0xee; 16].into(), Permission::LocalOnly)?;
    let req = TransferRequest::new(
        Transport::Shm,
        local.base_address() + 2,
        p[2].parse::<u64>().unwrap() + 5,
        8,
    );
    let op = if role == "read" {
        TransferOp::Read
    } else {
        TransferOp::Write
    };
    let report = dst.submit_and_wait(op, &peer, std::slice::from_ref(&req), WAIT)?;
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(report.transferred_bytes, 8);
    let mut want = [0xee; 16];
    if op == TransferOp::Read {
        want[2..10].fill(0x5a);
    }
    assert_eq!(bytes(&local), want);
    // A cached mapping cannot grant another request after source death or unregister.
    assert!(matches!(
        dst.submit_and_wait(op, &peer, &[req], WAIT),
        Err(Error::RegionNotFound)
    ));
    dst.close(WAIT)?;
    Ok(())
}

#[test]
fn unregister_unlink_and_peer_exit_hold_exact_inflight_grants() -> Result<()> {
    for cxl in [false, true] {
        for kill in [false, true] {
            let dir = Dir::new();
            let mount_dir = Dir::new();
            let mount = config_path(cxl, &mount_dir);
            let mut cfg = config(mount);
            cfg.tcp = false;
            let src = engine(cfg, &NATIVE)?;
            let host = src.allocate_shm_memory(32)?;
            host.write(0, &[0x5a; 32])?;
            let region = grant(&src, &host, 3, 22, Permission::RemoteReadWrite);
            let name = host.shared().unwrap().mapping.name.basename();
            let backing = if cxl {
                mount.to_owned()
            } else {
                PathBuf::from("/dev/shm")
            }
            .join(&name);
            put(
                &dir.0.join("peer"),
                format!(
                    "{} {} {} {name}",
                    src.endpoint(),
                    src.source_epoch(),
                    region.base_address()
                ),
            );
            let mut child = Process::start("write", &dir.0, mount);
            child.at(&dir.0.join("inside"));
            src.unregister_memory(&region)?;
            drop(region);
            assert_eq!(src.stats()?.retained_bytes, 22);
            assert!(matches!(
                src.close_source(Duration::from_millis(8)),
                Err(Error::Timeout)
            ));
            let mut blocked = [0; 1];
            assert!(matches!(
                host.read(8, &mut blocked),
                Err(Error::Unavailable(_))
            ));
            fs::remove_file(&backing)?;
            assert!(!backing.exists());
            if kill {
                child.kill();
            } else {
                put(&dir.0.join("resume"), b"");
                child.finish();
            }
            src.close_source(WAIT)?;
            assert_eq!(src.stats()?.retained_bytes, 0);
            let mut got = [0; 32];
            host.read(0, &mut got)?;
            let mut want = [0x5a; 32];
            if !kill {
                want[8..16].fill(0xee);
            }
            assert_eq!(got, want);
            src.free_shm_memory(host)?;
            assert!(!backing.exists());
        }
    }
    Ok(())
}

#[test]
fn killed_source_cannot_regrant_cached_pages_and_restart_reaps_its_name() -> Result<()> {
    for cxl in [false, true] {
        let control = Dir::new();
        let directory = Dir::new();
        let mount = config_path(cxl, &directory);
        let (listener, _guard) = crate::test_ports::guarded_port();
        put(&control.0.join("bind"), listener.local_addr()?.to_string());
        drop(listener);
        let mut source = Process::start("source", &control.0, mount);
        source.at(&control.0.join("peer"));
        let first = fs::read_to_string(control.0.join("peer"))?;
        let fields: Vec<_> = first.split_whitespace().collect();
        let backing = if cxl {
            mount.to_owned()
        } else {
            PathBuf::from("/dev/shm")
        }
        .join(fields[3]);
        let mut requester = Process::start("read", &control.0, mount);
        requester.at(&control.0.join("inside"));
        assert!(backing.exists());
        source.kill();
        // The private directory contains only this creator. The shared namespace
        // has a bounded scan, and the final mapped peer can collect its exact inode.
        let survivor = engine(config(mount), &NATIVE)?;
        if cxl {
            assert!(!backing.exists());
        }
        put(&control.0.join("resume"), b"");
        requester.finish();
        assert!(!backing.exists());
        let old = fields[1].parse::<SourceEpoch>()?;
        let mut replacement = config(mount);
        replacement.bind_addr = fields[0].parse().unwrap();
        let replace = engine(replacement, &NATIVE)?;
        assert_ne!(replace.source_epoch(), old);
        assert!(matches!(
            survivor.open_segment(replace.endpoint(), old, WAIT),
            Err(Error::EpochMismatch)
        ));
    }
    Ok(())
}
