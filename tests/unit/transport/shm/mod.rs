use super::{
    mapping::Local,
    sys::{NATIVE, Namespace, Sys},
    wire::{Descriptor, GATES, Manifest, Name},
};
use crate::test_support::EVENTUAL_PROGRESS as WAIT;
use crate::{
    EngineConfig, Error, Permission, RegisteredMemory, RemoteEffect, Result, ShmConfig,
    SourceEpoch, TransferEngine, TransferOp, TransferRequest, Transport,
};
use std::{
    cell::Cell,
    fs, io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

mod processes;
thread_local! { static TABLE:Cell<&'static Sys>=const{Cell::new(&NATIVE)}; }
pub(super) fn sys() -> &'static Sys {
    TABLE.with(Cell::get)
}
fn engine(cfg: EngineConfig, table: &'static Sys) -> Result<TransferEngine> {
    let old = TABLE.with(|t| t.replace(table));
    let e = TransferEngine::new(cfg);
    TABLE.with(|t| t.set(old));
    e
}
fn config(path: &Path) -> EngineConfig {
    EngineConfig {
        shm: ShmConfig {
            enable: true,
            cxl_mount_path: path.to_owned(),
        },
        ..Default::default()
    }
}
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let path = PathBuf::from("/dev/shm")
            .join(format!("orbite-test-{}", SourceEpoch::generate().unwrap()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn bytes(m: &RegisteredMemory) -> Vec<u8> {
    let mut b = vec![0; m.len()];
    m.read(0, &mut b).unwrap();
    b
}
fn grant(
    eng: &TransferEngine,
    host: &Arc<crate::HostMemory>,
    off: usize,
    size: usize,
    permission: Permission,
) -> RegisteredMemory {
    eng.register_host_memory(host.clone(), off, size, permission)
        .unwrap()
}
fn req(
    transport: Transport,
    l: &RegisteredMemory,
    r: &RegisteredMemory,
    offset: usize,
    len: usize,
) -> TransferRequest {
    TransferRequest::new(
        transport,
        l.base_address() + offset as u64,
        r.base_address() + offset as u64,
        len,
    )
}
fn path(dir: &Path, local: &Local) -> PathBuf {
    dir.join(local.mapping.name.basename())
}

#[test]
fn wire_fixture_rejects_malformed_ranges_and_grants() {
    let d = Descriptor {
        id: 7,
        offset: 3,
        size: 64,
        gate: 2,
        dev: 9,
        ino: 11,
        name: Name {
            ns: 5,
            pid: 21,
            nonce: SourceEpoch([0x5a; 16]),
        },
    };
    let table = Manifest {
        cxl: false,
        entries: vec![d],
    };
    let fixture = include_bytes!("../../../reference/shm/describe.bin");
    let mut encoded = vec![0; table.wire_len()];
    table.encode(&mut encoded);
    assert_eq!(&encoded[..], fixture);
    let (decoded, end) = Manifest::decode(fixture, 1).unwrap();
    assert_eq!(end, encoded.len());
    assert_eq!(decoded.get(7), Some(&d));
    for pos in [4, 7, 8, 16, 32, 68] {
        let mut bad = encoded.clone();
        bad[pos] = 255;
        if pos == 8 {
            bad[8..16].fill(0);
        }
        assert!(Manifest::decode(&bad, 1).is_err(), "{pos}");
    }
    assert!(Manifest::decode(&encoded[0..79], 1).is_err());
    assert!(Manifest::decode(&encoded, 0).is_err());
}

#[test]
fn shared_relocation_is_unique_across_threads_and_close_unmaps_it() -> Result<()> {
    static MAPS: AtomicUsize = AtomicUsize::new(0);
    static UNMAPS: AtomicUsize = AtomicUsize::new(0);
    fn map(f: &fs::File, s: usize, w: bool) -> io::Result<std::ptr::NonNull<u8>> {
        MAPS.fetch_add(1, Ordering::Relaxed);
        assert!(!w);
        (NATIVE.map)(f, s, w)
    }
    fn unmap(a: std::ptr::NonNull<u8>, s: usize) -> io::Result<()> {
        UNMAPS.fetch_add(1, Ordering::Relaxed);
        (NATIVE.unmap)(a, s)
    }
    static TABLE: Sys = Sys {
        map,
        unmap,
        ..NATIVE
    };
    let src = engine(config(Path::new("")), &NATIVE)?;
    let dst = engine(config(Path::new("")), &TABLE)?;
    let host = src.allocate_shm_memory(64)?;
    let remote = grant(&src, &host, 3, 32, Permission::RemoteRead);
    host.write(0, &[0xa5; 64])?;
    remote.write(0, b"source bytes")?;
    let peer = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    let barrier = std::sync::Barrier::new(8);
    thread::scope(|scope| {
        let mut hs = Vec::new();
        for _ in 0..8 {
            hs.push(scope.spawn(|| {
                let local = dst
                    .register_memory(vec![0xee; 16].into(), Permission::LocalOnly)
                    .unwrap();
                let r = TransferRequest::new(
                    Transport::Unspec,
                    local.base_address() + 2,
                    remote.base_address(),
                    12,
                );
                barrier.wait();
                let report = dst
                    .submit_and_wait(TransferOp::Read, &peer, &[r], WAIT)
                    .unwrap();
                assert!(report.is_complete(), "{report:?}");
                assert_eq!(bytes(&local), b"\xee\xeesource bytes\xee\xee");
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
    });
    assert_eq!(MAPS.load(Ordering::Relaxed), 1);
    dst.close_requester(WAIT)?;
    assert_eq!(UNMAPS.load(Ordering::Relaxed), 1);
    let mut want = [0xa5; 64];
    want[3..15].copy_from_slice(b"source bytes");
    let mut got = [0; 64];
    host.read(0, &mut got)?;
    assert_eq!(got, want);
    Ok(())
}

#[test]
fn missing_short_and_exclusive_create_never_materialize_or_truncate_a_peer_file() -> Result<()> {
    let dir = Dir::new();
    let mut cfg = config(&dir.0);
    cfg.tcp = false;
    let src = engine(cfg.clone(), &NATIVE)?;
    let dst = engine(cfg, &NATIVE)?;
    let host = src.allocate_shm_memory(64)?;
    let local = host.shared().unwrap();
    let backing = path(&dir.0, &local);
    let r = grant(&src, &host, 0, 64, Permission::RemoteReadWrite);
    let peer = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    let dest = dst.register_memory(vec![7; 10].into(), Permission::LocalOnly)?;
    let q = req(Transport::Shm, &dest, &r, 1, 8);
    let ns = Namespace::new(&dir.0)?;
    assert_eq!(
        (NATIVE.open)(&ns, local.mapping.name, true, true)
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    assert_eq!(fs::metadata(&backing)?.len(), 64);
    fs::OpenOptions::new()
        .write(true)
        .open(&backing)?
        .set_len(4)?;
    assert!(matches!(
        dst.submit_and_wait(TransferOp::Read, &peer, std::slice::from_ref(&q), WAIT),
        Err(Error::RegionNotFound)
    ));
    fs::remove_file(&backing)?;
    assert!(matches!(
        dst.submit_and_wait(TransferOp::Read, &peer, &[q], WAIT),
        Err(Error::RegionNotFound)
    ));
    assert!(!backing.exists());
    assert_eq!(bytes(&dest), [7; 10]);
    Ok(())
}

#[test]
fn allocator_retries_a_real_name_collision_without_truncating_or_unlinking_it() -> Result<()> {
    static CREATIONS: AtomicUsize = AtomicUsize::new(0);
    fn collide(ns: &Namespace, name: Name, create: bool, write: bool) -> io::Result<fs::File> {
        use std::io::Write;
        if create && CREATIONS.fetch_add(1, Ordering::SeqCst) == 0 {
            let mut existing = (NATIVE.open)(ns, name, true, true)?;
            existing.write_all(b"retained bytes")?;
        }
        (NATIVE.open)(ns, name, create, write)
    }
    static COLLISION: Sys = Sys {
        open: collide,
        ..NATIVE
    };
    let dir = Dir::new();
    let src = engine(config(&dir.0), &COLLISION)?;
    let allocation = src.allocate_shm_memory(32)?;
    assert_eq!(CREATIONS.load(Ordering::SeqCst), 2);
    assert_eq!(fs::read_dir(&dir.0)?.count(), 2);
    src.free_shm_memory(allocation)?;
    let remaining = fs::read_dir(&dir.0)?.collect::<io::Result<Vec<_>>>()?;
    assert_eq!(remaining.len(), 1);
    assert_eq!(fs::read(remaining[0].path())?, b"retained bytes");
    Ok(())
}

#[test]
fn rejected_late_relocation_leaves_the_whole_batch_untouched_and_keeps_cached_pages() -> Result<()>
{
    let dir = Dir::new();
    let mut cfg = config(&dir.0);
    cfg.tcp = false;
    let src = engine(cfg.clone(), &NATIVE)?;
    let dst = engine(cfg, &NATIVE)?;
    let a = src.allocate_shm_memory(32)?;
    let b = src.allocate_shm_memory(32)?;
    let ra = grant(&src, &a, 4, 12, Permission::RemoteReadWrite);
    let rb = grant(&src, &b, 2, 12, Permission::RemoteReadWrite);
    a.write(0, &[0xa5; 32])?;
    b.write(0, &[0xb5; 32])?;
    let peer = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    let l = dst.register_memory(vec![0xcc; 16].into(), Permission::LocalOnly)?;
    let qa = req(Transport::Shm, &l, &ra, 1, 8);
    let qb = req(Transport::Shm, &l, &rb, 1, 8);
    assert!(
        dst.submit_and_wait(TransferOp::Write, &peer, std::slice::from_ref(&qa), WAIT)?
            .is_complete()
    );
    ra.write(0, &[0xa5; 12])?;
    fs::remove_file(path(&dir.0, &a.shared().unwrap()))?;
    fs::remove_file(path(&dir.0, &b.shared().unwrap()))?;
    assert!(matches!(
        dst.submit_and_wait(TransferOp::Write, &peer, &[qa.clone(), qb], WAIT),
        Err(Error::RegionNotFound)
    ));
    assert_eq!(bytes(&ra), [0xa5; 12]);
    assert_eq!(bytes(&rb), [0xb5; 12]);
    assert!(
        dst.submit_and_wait(TransferOp::Write, &peer, &[qa], WAIT)?
            .is_complete()
    );
    let mut want = [0xa5; 32];
    want[5..13].fill(0xcc);
    let mut got = [0; 32];
    a.read(0, &mut got)?;
    assert_eq!(got, want);
    b.read(0, &mut got)?;
    assert_eq!(got, [0xb5; 32]);
    Ok(())
}

#[test]
fn source_grants_and_read_only_page_protection_follow_each_registration() -> Result<()> {
    static READS: AtomicUsize = AtomicUsize::new(0);
    fn readonly(fd: &fs::File, n: usize, write: bool) -> io::Result<std::ptr::NonNull<u8>> {
        assert!(!write, "read-only export opened a write mapping");
        let p = (NATIVE.map)(fd, n, write)?;
        let address = p.as_ptr() as usize;
        let maps = fs::read_to_string("/proc/self/maps")?;
        assert!(maps.lines().any(|l| {
            let mut fields = l.split_whitespace();
            let range = fields.next().unwrap();
            let (a, b) = range.split_once('-').unwrap();
            usize::from_str_radix(a, 16).unwrap() <= address
                && address < usize::from_str_radix(b, 16).unwrap()
                && fields.next() == Some("r--s")
        }));
        READS.fetch_add(1, Ordering::Relaxed);
        Ok(p)
    }
    static TABLE: Sys = Sys {
        map: readonly,
        ..NATIVE
    };
    let mut cfg = config(Path::new(""));
    cfg.tcp = false;
    let src = engine(cfg.clone(), &NATIVE)?;
    let dst = engine(cfg, &TABLE)?;
    let host = src.allocate_shm_memory(48)?;
    host.write(0, &[0x61; 48])?;
    let ro = grant(&src, &host, 2, 12, Permission::RemoteRead);
    let duplicate = grant(&src, &host, 2, 12, Permission::RemoteReadWrite);
    assert_eq!(ro.id(), duplicate.id());
    assert_eq!(duplicate.permission(), Permission::RemoteRead);
    let rw = grant(&src, &host, 24, 12, Permission::RemoteReadWrite);
    let private = grant(&src, &host, 40, 4, Permission::LocalOnly);
    let live = path(Path::new("/dev/shm"), &host.shared().unwrap());
    let mut peer = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    assert!(matches!(
        peer.resolve_address(private.base_address(), 1),
        Err(Error::RegionNotFound)
    ));
    let dest = dst.register_memory(vec![0xee; 12].into(), Permission::LocalOnly)?;
    let r = req(Transport::Shm, &dest, &ro, 1, 8);
    src.unregister_memory(&ro)?;
    let rep = dst.submit_and_wait(TransferOp::Read, &peer, std::slice::from_ref(&r), WAIT)?;
    assert!(rep.is_complete());
    assert_eq!(READS.load(Ordering::Relaxed), 1);
    assert_eq!(
        bytes(&dest),
        [
            0xee, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0x61, 0xee, 0xee, 0xee
        ]
    );
    assert!(matches!(
        dst.submit_and_wait(TransferOp::Write, &peer, std::slice::from_ref(&r), WAIT),
        Err(Error::PermissionDenied)
    ));
    peer.regions
        .iter_mut()
        .find(|v| v.id() == ro.id())
        .unwrap()
        .permission = Permission::RemoteReadWrite;
    // Even a corrupted descriptor cannot obtain a writable mapping for this source grant.
    assert!(matches!(
        dst.submit_and_wait(TransferOp::Write, &peer, std::slice::from_ref(&r), WAIT),
        Err(Error::PermissionDenied)
    ));
    src.unregister_memory(&duplicate)?;
    let after = dst.submit_and_wait(TransferOp::Read, &peer, &[r], WAIT);
    assert!(matches!(after, Err(Error::RegionNotFound)), "{after:?}");
    assert!(live.exists());
    assert_eq!(bytes(&ro), [0x61; 12]);
    src.unregister_memory(&rw)?;
    src.unregister_memory(&private)?;
    drop((ro, duplicate, rw, private));
    src.free_shm_memory(host)?;
    assert!(!live.exists());
    Ok(())
}

#[test]
fn unregister_revokes_even_an_inherited_grant_fd_and_preserves_uncertain_owners() -> Result<()> {
    static DUP: std::sync::Mutex<Option<fs::File>> = std::sync::Mutex::new(None);
    fn duplicate(fd: &fs::File, write: bool) -> io::Result<fs::File> {
        let grant = (NATIVE.reopen)(fd, write)?;
        if write {
            *DUP.lock().unwrap() = Some(grant.try_clone()?);
        }
        Ok(grant)
    }
    static TABLE: Sys = Sys {
        reopen: duplicate,
        ..NATIVE
    };
    let dir = Dir::new();
    let mut c = config(&dir.0);
    c.tcp = false;
    let src = engine(c.clone(), &TABLE)?;
    let dst = engine(c.clone(), &NATIVE)?;
    let host = src.allocate_shm_memory(12)?;
    let remote = grant(&src, &host, 0, 12, Permission::RemoteRead);
    let peer = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    let local = dst.register_memory(vec![0xf3; 12].into(), Permission::LocalOnly)?;
    let q = req(Transport::Shm, &local, &remote, 2, 8);
    let inherited = DUP.lock().unwrap().take().unwrap();
    src.unregister_memory(&remote)?;
    let after = dst.submit_and_wait(TransferOp::Read, &peer, &[q], WAIT);
    assert!(matches!(after, Err(Error::RegionNotFound)), "{after:?}");
    assert_eq!(bytes(&remote), [0; 12]);
    assert_eq!(bytes(&local), [0xf3; 12]);
    drop(inherited);
    drop(remote);
    src.free_shm_memory(host)?;

    static CALLS: AtomicUsize = AtomicUsize::new(0);
    fn failed(_: &fs::File, _: i64, _: i64) -> io::Result<()> {
        CALLS.fetch_add(1, Ordering::SeqCst);
        Err(io::Error::from_raw_os_error(libc::EIO))
    }
    static FAIL: Sys = Sys {
        unlock: failed,
        ..NATIVE
    };
    let src = engine(c, &FAIL)?;
    let host = src.allocate_shm_memory(12)?;
    let backing = path(&dir.0, &host.shared().unwrap());
    let r = grant(&src, &host, 0, 12, Permission::RemoteRead);
    for result in [
        src.unregister_memory(&r),
        src.close_source(WAIT),
        src.close_source(WAIT),
    ] {
        assert!(matches!(
            result,
            Err(Error::Unquiesced {
                operation: "SHM revoke",
                code: libc::EIO
            })
        ));
    }
    drop((r, host, src));
    assert!(backing.exists());
    assert_eq!(CALLS.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn inherited_payload_fd_does_not_extend_a_completed_copy() -> Result<()> {
    static INHERITED: std::sync::Mutex<Option<fs::File>> = std::sync::Mutex::new(None);
    fn duplicate(fd: &fs::File, write: bool) -> io::Result<fs::File> {
        let f = (NATIVE.reopen)(fd, write)?;
        if write {
            *INHERITED.lock().unwrap() = Some(f.try_clone()?);
        }
        Ok(f)
    }
    static DUP: Sys = Sys {
        reopen: duplicate,
        ..NATIVE
    };
    let dir = Dir::new();
    let c = config(&dir.0);
    let src = engine(c.clone(), &NATIVE)?;
    let dst = engine(c.clone(), &DUP)?;
    let host = src.allocate_shm_memory(12)?;
    let remote = grant(&src, &host, 0, 12, Permission::RemoteReadWrite);
    let local = dst.register_memory(vec![0xaa; 12].into(), Permission::LocalOnly)?;
    let peer = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    let q = req(Transport::Shm, &local, &remote, 2, 8);
    assert!(
        dst.submit_and_wait(TransferOp::Write, &peer, &[q], WAIT)?
            .is_complete()
    );
    let inherited = INHERITED.lock().unwrap().take().unwrap();
    let mut want = [0; 12];
    want[2..10].fill(0xaa);
    assert_eq!(bytes(&remote), want);
    drop(inherited);

    Ok(())
}

#[test]
fn physical_aliases_use_memmove_and_keep_dependent_public_order() -> Result<()> {
    let src = engine(config(Path::new("")), &NATIVE)?;
    let dst = Arc::new(engine(config(Path::new("")), &NATIVE)?);
    let host = src.allocate_shm_memory(32)?;
    let initial: Vec<_> = (0..32u8).collect();
    let remote = grant(&src, &host, 4, 24, Permission::RemoteReadWrite);
    let local = grant(&dst, &host, 2, 24, Permission::LocalOnly);
    let peer = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    for op in [TransferOp::Read, TransferOp::Write] {
        host.write(0, &initial)?;
        let requests = [0, 4].map(|offset| req(Transport::Unspec, &local, &remote, offset, 4));
        let batch = dst.submit_async(op, &peer, &requests, None, WAIT)?;
        assert_eq!(batch.wait(WAIT)?.completed_tasks, 2);
        let statuses = batch.task_statuses()?;
        assert_eq!(statuses[0].transport, Transport::Shm);
        assert_ne!(statuses[0].physical_owner, statuses[1].physical_owner);
        let mut want = initial.clone();
        for off in [0, 4] {
            if op == TransferOp::Write {
                want.copy_within(2 + off..6 + off, 4 + off);
            } else {
                want.copy_within(4 + off..8 + off, 2 + off);
            }
        }
        let mut got = vec![0; 32];
        host.read(0, &mut got)?;
        assert_eq!(got, want);
    }
    // Disjoint ranges on the same inode can share one physical owner.
    host.write(0, &initial)?;
    let requests = [0, 4].map(|offset| {
        TransferRequest::new(
            Transport::Shm,
            local.base_address() + offset,
            remote.base_address() + 12 + offset,
            4,
        )
    });
    let batch = dst.submit_async(TransferOp::Write, &peer, &requests, None, WAIT)?;
    assert_eq!(batch.wait(WAIT)?.completed_tasks, 2);
    assert_eq!(
        batch.task_status(0)?.physical_owner,
        batch.task_status(1)?.physical_owner
    );
    let mut want = initial.clone();
    want.copy_within(2..10, 16);
    let mut got = [0; 32];
    host.read(0, &mut got)?;
    assert_eq!(got.as_slice(), want);
    // Different allocations retain ordinary physical-owner merging.
    let other = dst.register_memory(vec![0x6d; 12].into(), Permission::LocalOnly)?;
    let batch = dst.submit_async(
        TransferOp::Write,
        &peer,
        &[
            req(Transport::Shm, &other, &remote, 0, 4),
            req(Transport::Shm, &other, &remote, 4, 4),
        ],
        None,
        WAIT,
    )?;
    assert_eq!(batch.wait(WAIT)?.completed_tasks, 2);
    assert_eq!(
        batch.task_status(0)?.physical_owner,
        batch.task_status(1)?.physical_owner
    );
    assert_eq!(&bytes(&remote)[..8], &[0x6d; 8]);
    Ok(())
}

#[test]
fn describe_with_hp_tcp_chooses_shm_only_for_shared_regions() -> Result<()> {
    let mut cfg = config(Path::new(""));
    cfg.tcp = false;
    cfg.hp_tcp.enable = true;
    let src = engine(cfg.clone(), &NATIVE)?;
    let dst = Arc::new(engine(cfg, &NATIVE)?);
    let host = src.allocate_shm_memory(12)?;
    host.write(0, &[0x37; 12])?;
    let shared = grant(&src, &host, 0, 12, Permission::RemoteRead);
    let ordinary = src.register_memory(vec![0x42; 12].into(), Permission::RemoteRead)?;
    let local = dst.register_memory(vec![0xcc; 12].into(), Permission::LocalOnly)?;
    let peer = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    assert!(peer.metadata.shm.is_some() && peer.metadata.hp.is_some());
    for (region, owner, byte) in [
        (&shared, Transport::Shm, 0x37),
        (&ordinary, Transport::HpTcp, 0x42),
    ] {
        let q = req(Transport::Unspec, &local, region, 2, 8);
        let b = dst.submit_async(TransferOp::Read, &peer, &[q], None, WAIT)?;
        assert_eq!(b.wait(WAIT)?.completed_tasks, 1);
        assert_eq!(b.task_status(0)?.transport, owner);
        let mut want = [0xcc; 12];
        want[2..10].fill(byte);
        assert_eq!(bytes(&local), want);
        assert_eq!(bytes(region), [byte; 12]);
    }
    Ok(())
}

#[cfg(feature = "rdma")]
#[test]
fn rdma_precedes_shm_unless_a_shared_memory_policy_selects_it() -> Result<()> {
    use crate::engine::tests::rdma_fixture_with_config;
    use crate::transport::rdma::tests::runtime;
    let mut c = config(Path::new(""));
    c.selector = Some(crate::TransportSelectorConfig::from_json(
        r#"{"policy":[{"name":"shared","segment_type":"memory","intent_type":"migration","transports":["shm","rdma"]},{"name":"default","segment_type":"memory"}]}"#,
    )?);
    let src = rdma_fixture_with_config(runtime("copy"), c.clone());
    let dst = Arc::new(rdma_fixture_with_config(runtime("copy"), c));
    let host = src.allocate_shm_memory(12)?;
    host.write(0, &[0x76; 12])?;
    let remote = grant(&src, &host, 0, 12, Permission::RemoteRead);
    let local = dst.register_memory(vec![0xee; 12].into(), Permission::LocalOnly)?;
    let peer = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    let q = req(Transport::Unspec, &local, &remote, 2, 8);
    for (request, owner) in [
        (q.clone(), Transport::Rdma),
        (
            q.with_policy_name("shared")?
                .with_intent(crate::IntentType::Migration),
            Transport::Shm,
        ),
    ] {
        local.write(0, &[0xee; 12])?;
        let b = dst.submit_async(TransferOp::Read, &peer, &[request], None, WAIT)?;
        assert_eq!(b.wait(WAIT)?.completed_tasks, 1);
        assert_eq!(b.task_status(0)?.transport, owner);
        let mut want = [0xee; 12];
        want[2..10].fill(0x76);
        assert_eq!(bytes(&local), want);
        assert_eq!(bytes(&remote), [0x76; 12]);
    }
    Ok(())
}

#[test]
fn clean_shm_rejection_falls_through_but_a_lost_tcp_write_ack_never_replays() -> Result<()> {
    use crate::runtime::control_plane::{REQUEST_SIZE, Reply};
    use std::io::{Read, Write};
    let mut cfg = config(Path::new(""));
    cfg.selector = Some(crate::TransportSelectorConfig::from_json(
        r#"{"policy":[{"name":"local","segment_type":"memory","transports":["shm","tcp"]},{"name":"network","segment_type":"memory","transports":["tcp","shm"]}]}"#,
    )?);
    let src = engine(cfg.clone(), &NATIVE)?;
    let dst = Arc::new(engine(cfg, &NATIVE)?);
    let host = src.allocate_shm_memory(32)?;
    host.write(0, &[0xaa; 32])?;
    let remote = grant(&src, &host, 2, 22, Permission::RemoteReadWrite);
    let local = dst.register_memory(vec![0xcc; 16].into(), Permission::LocalOnly)?;
    let mut peer = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    let q = req(Transport::Unspec, &local, &remote, 3, 8);
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0))?;
    let mut fault = peer.clone();
    fault.endpoint = listener.local_addr()?;
    dst.bind_test_peer(&mut fault)?;
    let epoch = peer.epoch;
    let target = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_read_timeout(Some(WAIT)).unwrap();
        stream.read_exact(&mut [0; REQUEST_SIZE]).unwrap();
        stream
            .write_all(
                &Reply {
                    epoch,
                    status: 9,
                    length: 0,
                }
                .encode(),
            )
            .unwrap();
        let mut body = [0; 8];
        stream.read_exact(&mut body).unwrap();
        assert_eq!(body, [0xcc; 8]);
    });
    let result = dst.submit_and_wait(
        TransferOp::Write,
        &fault,
        &[q.clone().with_policy_name("network")?],
        WAIT,
    )?;
    assert_eq!(result.failure.unwrap().remote_effect, RemoteEffect::Unknown);
    target.join().unwrap();
    assert_eq!(bytes(&remote), [0xaa; 22]);
    fs::remove_file(path(Path::new("/dev/shm"), &host.shared().unwrap()))?;
    let batch = dst.submit_async(
        TransferOp::Write,
        &peer,
        &[q.clone().with_policy_name("local")?],
        None,
        WAIT,
    )?;
    assert_eq!(batch.wait(WAIT)?.completed_tasks, 1);
    let s = batch.task_status(0)?;
    assert_eq!((s.transport, s.failover_count), (Transport::Tcp, 1));
    let mut want = [0xaa; 32];
    want[5..13].fill(0xcc);
    let mut got = [0; 32];
    host.read(0, &mut got)?;
    assert_eq!(got, want);
    peer.metadata.boot = Some(SourceEpoch([0xab; 16]));
    assert!(matches!(
        dst.submit_and_wait(
            TransferOp::Read,
            &peer,
            &[req(Transport::Shm, &local, &remote, 1, 4)],
            WAIT
        ),
        Err(Error::TransportPolicy { .. }) | Err(Error::Unavailable(_))
    ));
    assert_eq!(bytes(&local), [0xcc; 16]);
    Ok(())
}

#[test]
fn failed_unmap_quarantines_real_mapping_fd_and_budget_without_retry() -> Result<()> {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    static ADDRESS: AtomicUsize = AtomicUsize::new(0);
    fn fail(p: std::ptr::NonNull<u8>, _: usize) -> io::Result<()> {
        ADDRESS.store(p.as_ptr() as usize, Ordering::SeqCst);
        CALLS.fetch_add(1, Ordering::SeqCst);
        Err(io::Error::from_raw_os_error(libc::EIO))
    }
    static TABLE: Sys = Sys {
        unmap: fail,
        ..NATIVE
    };
    let dir = Dir::new();
    let cfg = config(&dir.0);
    let src = engine(cfg.clone(), &NATIVE)?;
    let dst = engine(cfg, &TABLE)?;
    let host = src.allocate_shm_memory(16)?;
    let r = grant(&src, &host, 0, 16, Permission::RemoteReadWrite);
    host.write(0, &[9; 16])?;
    let peer = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    let l = dst.register_memory(vec![0xee; 16].into(), Permission::LocalOnly)?;
    assert!(
        dst.submit_and_wait(
            TransferOp::Read,
            &peer,
            &[req(Transport::Shm, &l, &r, 2, 8)],
            WAIT
        )?
        .is_complete()
    );
    for _ in 0..2 {
        assert!(matches!(
            dst.close_requester(WAIT),
            Err(Error::Unquiesced {
                operation: "SHM munmap",
                code: libc::EIO
            })
        ));
    }
    assert_eq!(CALLS.load(Ordering::SeqCst), 1);
    let addr = ADDRESS.load(Ordering::SeqCst);
    let name = host.shared().unwrap().mapping.name.basename();
    assert!(
        fs::read_to_string("/proc/self/maps")?
            .lines()
            .any(|l| l.starts_with(&format!("{addr:x}-")) && l.ends_with(&name))
    );
    assert_eq!(
        bytes(&l),
        [
            0xee, 0xee, 9, 9, 9, 9, 9, 9, 9, 9, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee
        ]
    );
    Ok(())
}

#[test]
fn mapping_budgets_and_failed_create_roll_back_before_copy() -> Result<()> {
    static ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
    fn resize(fd: &fs::File, size: usize) -> io::Result<()> {
        if ATTEMPTS.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(io::Error::from_raw_os_error(libc::ENOSPC));
        }
        (NATIVE.resize)(fd, size)
    }
    static TABLE: Sys = Sys { resize, ..NATIVE };
    let dir = Dir::new();
    let mut cfg = config(&dir.0);
    cfg.tcp = false;
    let src = engine(cfg.clone(), &TABLE)?;
    assert!(
        matches!(src.allocate_shm_memory(16),Err(Error::Io(e)) if e.raw_os_error()==Some(libc::ENOSPC))
    );
    assert_eq!(fs::read_dir(&dir.0)?.count(), 0);
    let a = src.allocate_shm_memory(16)?;
    let b = src.allocate_shm_memory(16)?;
    let r1 = grant(&src, &a, 0, 16, Permission::RemoteReadWrite);
    let r2 = grant(&src, &b, 0, 16, Permission::RemoteReadWrite);
    cfg.max_registered_regions = 1;
    cfg.max_registered_bytes = 16;
    let dst = engine(cfg, &NATIVE)?;
    let l = dst.register_memory(vec![0xbd; 16].into(), Permission::LocalOnly)?;
    let peer = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    let q = req(Transport::Shm, &l, &r2, 1, 8);
    assert!(matches!(
        dst.submit_and_wait(
            TransferOp::Write,
            &peer,
            &[req(Transport::Shm, &l, &r1, 1, 8), q.clone()],
            WAIT
        ),
        Err(Error::ResourceExhausted)
    ));
    assert_eq!(bytes(&r1), [0; 16]);
    assert_eq!(bytes(&r2), [0; 16]);
    assert!(
        dst.submit_and_wait(TransferOp::Write, &peer, &[q], WAIT)?
            .is_complete()
    );
    let mut want = [0; 16];
    want[1..9].fill(0xbd);
    assert_eq!(bytes(&r2), want);
    fn full(_: &fs::File, _: usize) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(libc::ENOSPC))
    }
    fn uncertain(_: &Namespace, _: Name) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(libc::EIO))
    }
    static UNCONFIRMED: Sys = Sys {
        resize: full,
        unlink: uncertain,
        ..NATIVE
    };
    let separate = Dir::new();
    let mut cfg = config(&separate.0);
    cfg.max_registered_regions = 1;
    let failed = engine(cfg, &UNCONFIRMED)?;
    assert!(matches!(
        failed.allocate_shm_memory(16),
        Err(Error::Unquiesced {
            operation: "SHM allocation rollback",
            code: libc::EIO
        })
    ));
    assert_eq!(fs::read_dir(&separate.0)?.count(), 1);
    assert!(matches!(
        failed.allocate_shm_memory(1),
        Err(Error::ResourceExhausted)
    ));
    Ok(())
}

#[test]
fn owner_retirement_never_unlinks_a_replacement_inode_and_failed_unmap_retains_its_charge()
-> Result<()> {
    let dir = Dir::new();
    let mut cfg = config(&dir.0);
    cfg.max_registered_regions = 1;
    let src = engine(cfg.clone(), &NATIVE)?;
    let host = src.allocate_shm_memory(8)?;
    let old = path(&dir.0, &host.shared().unwrap());
    fs::remove_file(&old)?;
    fs::write(&old, b"replacement")?;
    src.free_shm_memory(host)?;
    assert_eq!(fs::read(&old)?, b"replacement");
    static FAILS: AtomicUsize = AtomicUsize::new(0);
    fn fail(_: std::ptr::NonNull<u8>, _: usize) -> io::Result<()> {
        FAILS.fetch_add(1, Ordering::Relaxed);
        Err(io::Error::from_raw_os_error(libc::EIO))
    }
    static TABLE: Sys = Sys {
        unmap: fail,
        ..NATIVE
    };
    let owner = engine(cfg, &TABLE)?;
    let allocation = owner.allocate_shm_memory(8)?;
    let name = path(&dir.0, &allocation.shared().unwrap());
    assert!(matches!(
        owner.free_shm_memory(allocation),
        Err(Error::Unquiesced {
            operation: "SHM munmap",
            ..
        })
    ));
    assert_eq!(FAILS.load(Ordering::Relaxed), 1);
    assert!(name.exists());
    assert!(matches!(
        owner.allocate_shm_memory(8),
        Err(Error::ResourceExhausted)
    ));
    Ok(())
}

#[cfg(feature = "metrics")]
#[test]
fn shm_counts_bytes_on_its_own_transport_label() -> Result<()> {
    let mut cfg = config(Path::new(""));
    cfg.metrics.enabled = true;
    cfg.metrics.report_interval_seconds = 0;
    let src = engine(cfg.clone(), &NATIVE)?;
    let dst = engine(cfg, &NATIVE)?;
    let host = src.allocate_shm_memory(10)?;
    let remote = grant(&src, &host, 0, 10, Permission::RemoteReadWrite);
    let l = dst.register_memory(vec![3; 10].into(), Permission::LocalOnly)?;
    let peer = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    assert!(
        dst.submit_and_wait(
            TransferOp::Write,
            &peer,
            &[req(Transport::Unspec, &l, &remote, 1, 8)],
            WAIT
        )?
        .is_complete()
    );
    assert!(
        dst.metrics()
            .unwrap()
            .prometheus()
            .contains("tent_write_bytes_total{transport=\"shm\"} 8")
    );
    assert_eq!(bytes(&remote), [0, 3, 3, 3, 3, 3, 3, 3, 3, 0]);
    Ok(())
}

mod peer_retirement;
