use super::*;
use crate::cuda::fabric::Platform;
use crate::test_support::EVENTUAL_PROGRESS as WAIT;
use crate::{Error, MnnvlConfig, MnnvlHandleType, RegisteredMemory, RemoteEffect, SourceEpoch};
mod authority;
mod cache;
mod external;
mod shim;
use shim::Rig;
pub(in crate::cuda) fn install(
    c: &MnnvlConfig,
    boot: SourceEpoch,
    n: usize,
    b: usize,
) -> Option<Result<Arc<Platform>>> {
    shim::install(c, boot, n, b)
}
fn config() -> EngineConfig {
    EngineConfig {
        mnnvl: Some(MnnvlConfig {
            fabric_domain: Some(SourceEpoch([0x4e; 16])),
            ..Default::default()
        }),
        handler_timeout: WAIT,
        ..Default::default()
    }
}
fn gpu(e: &TransferEngine, n: usize, p: Permission) -> Result<(Arc<CudaMemory>, RegisteredMemory)> {
    let m = e.allocate_mnnvl(0, n)?;
    let r = e
        .register_cuda_memory_batch(&[(m.clone(), 0, n)], p)?
        .remove(0);
    Ok((m, r))
}
fn drained(e: &TransferEngine) -> Result<()> {
    let end = std::time::Instant::now() + WAIT;
    while e.stats()?.active_source_operations != 0 {
        assert!(std::time::Instant::now() < end);
        std::thread::yield_now();
    }
    Ok(())
}
fn read(m: &RegisteredMemory) -> Vec<u8> {
    let mut got = vec![0; m.len()];
    m.read(0, &mut got).unwrap();
    got
}
fn q(
    t: Transport,
    l: &RegisteredMemory,
    r: &RegisteredMemory,
    lo: usize,
    ro: usize,
    n: usize,
) -> TransferRequest {
    TransferRequest::new(
        t,
        l.base_address() + lo as u64,
        r.base_address() + ro as u64,
        n,
    )
}

#[test]
fn both_sizes_use_stream_event_and_shared_handle_views_keep_exact_authority() -> Result<()> {
    use super::readiness::Producer;
    let mut c = config();
    c.mnnvl.as_mut().unwrap().async_memcpy_threshold = 1;
    c.merge_requests = false;
    let sr = Rig::new();
    let dr = Rig::new().single();
    let src = sr.engine(c.clone())?;
    let dst = Arc::new(dr.engine(c)?);
    let total = 132 * 1024 + 37;
    let remote = src.allocate_mnnvl(1, total)?;
    let regs = src.register_cuda_memory_batch(
        &[(remote.clone(), 9, 48), (remote.clone(), 64, total - 79)],
        Permission::RemoteReadWrite,
    )?;
    assert_eq!(sr.calls("export"), 1);
    assert!(Arc::ptr_eq(
        regs[0].0.fabric.get().unwrap().share.as_ref().unwrap(),
        regs[1].0.fabric.get().unwrap().share.as_ref().unwrap()
    ));
    let (owner, local) = gpu(&dst, total, Permission::LocalOnly)?;
    let mut peer = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    peer.metadata.boot = Some(SourceEpoch([0x1b; 16])); // Different boots with the same published domain.
    let mut source: Vec<_> = (0..total).map(|i| (i % 239) as u8).collect();
    remote.write(0, &source)?;
    let mut wanted = vec![0xcd; total];
    local.write(0, &wanted)?;
    let producer = Producer::new(&remote, 0x51, false);
    producer.enqueue(remote.base_address() + 10, 16);
    producer.record(&remote)?;
    source[10..26].fill(0x51);
    for (j, (r, start, n)) in [(&regs[0], 9, 16), (&regs[1], 64, total - 100)]
        .into_iter()
        .enumerate()
    {
        let req = q(Transport::Unspec, &local, r, 3, 1, n);
        let b = dst.submit_async(TransferOp::Read, &peer, &[req], None, WAIT)?;
        assert_eq!(b.wait(WAIT)?.completed_tasks, 1);
        assert_eq!(b.task_status(0)?.transport, Transport::Mnnvl);
        wanted[3..3 + n].copy_from_slice(&source[start + 1..start + 1 + n]);
        assert_eq!(read(&local), wanted);
        assert!(dr.gpus[0].event_query_successes.load(Ordering::Acquire) > j);
    }
    let producer = Producer::new(&owner, 0x37, false);
    producer.enqueue(owner.base_address() + 5, 16);
    producer.record(&owner)?;
    let requests = [
        q(Transport::Mnnvl, &local, &regs[0], 5, 2, 8),
        q(Transport::Mnnvl, &local, &regs[0], 13, 22, 8),
    ];
    let report = dst.submit_and_wait(TransferOp::Write, &peer, &requests, WAIT)?;
    assert!(report.is_complete(), "{report:?}");
    source[11..19].fill(0x37);
    source[31..39].fill(0x37);
    let mut got = vec![0; total];
    remote.read(0, &mut got)?;
    assert_eq!(got, source);
    assert!(dr.gpus[0].events.lock().unwrap().contains(&"copy-batch"));
    assert_eq!(dr.calls("import"), 3);
    assert!(dr.hooks.access.lock().unwrap().iter().all(|entries| {
        entries.len() == 1
            && entries
                .iter()
                .all(|(t, _)| *t == sys::CUmemLocationType::CU_MEM_LOCATION_TYPE_DEVICE as i32)
    }));
    peer.metadata.mnnvl.as_mut().unwrap().domain = SourceEpoch([0x91; 16]);
    assert!(
        dst.submit_and_wait(
            TransferOp::Read,
            &peer,
            &[q(Transport::Mnnvl, &local, &regs[0], 0, 0, 1)],
            WAIT
        )
        .is_err()
    );
    assert_eq!(dr.calls("import"), 3);
    dst.close(WAIT)?;
    src.close(WAIT)?;
    Ok(())
}

#[test]
fn egm_directions_are_owned_by_the_local_numa_gpu_and_plain_host_is_requester_only() -> Result<()> {
    let mut c = config();
    let base = Rig::new();
    let off = base.engine(c.clone())?;
    assert!(matches!(
        off.allocate_egm(0, 32),
        Err(Error::Unavailable(_))
    ));
    c.mnnvl.as_mut().unwrap().egm = true;
    let sr = Rig::new();
    let dr = Rig::new();
    let src = sr.engine(c.clone())?;
    let dst = dr.engine(c.clone())?;
    assert!(matches!(
        src.allocate_egm(7, 32),
        Err(Error::Unavailable(_))
    ));
    let host = src.allocate_egm(1, 40)?;
    let remote = src.register_host_memory(host.clone(), 4, 32, Permission::RemoteReadWrite)?;
    let (_, dev) = gpu(&src, 36, Permission::RemoteReadWrite)?;
    let ordinary = src.register_memory(vec![5; 32].into(), Permission::RemoteReadWrite)?;
    let local_host = dst.allocate_egm(1, 40)?;
    let egm = dst.register_host_memory(local_host.clone(), 3, 34, Permission::LocalOnly)?;
    let (_, gpu) = gpu(&dst, 34, Permission::LocalOnly)?;
    let plain = dst.register_memory(vec![0; 34].into(), Permission::LocalOnly)?;
    let peer = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    let off_peer = off.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    let off_local = off.register_memory(vec![0xee; 32].into(), Permission::LocalOnly)?;
    assert!(
        off.submit_and_wait(
            TransferOp::Read,
            &off_peer,
            &[q(Transport::Mnnvl, &off_local, &remote, 0, 0, 8)],
            WAIT
        )
        .is_err()
    );
    assert!(
        dst.submit_and_wait(
            TransferOp::Read,
            &peer,
            &[q(Transport::Mnnvl, &gpu, &ordinary, 0, 0, 8)],
            WAIT
        )
        .is_err()
    );
    for r in [&remote, &dev] {
        for l in [&egm, &gpu, &plain] {
            r.write(0, &vec![0x67; r.len()])?;
            l.write(0, &vec![0xb9; l.len()])?;
            let req = q(Transport::Mnnvl, l, r, 1, 2, 21);
            assert!(
                dst.submit_and_wait(TransferOp::Read, &peer, std::slice::from_ref(&req), WAIT)?
                    .is_complete()
            );
            let mut want = vec![0xb9; l.len()];
            want[1..22].fill(0x67);
            assert_eq!(read(l), want);
            l.write(1, &[0x83; 21])?;
            assert!(
                dst.submit_and_wait(TransferOp::Write, &peer, &[req], WAIT)?
                    .is_complete()
            );
            let mut want = vec![0x67; r.len()];
            want[2..23].fill(0x83);
            assert_eq!(read(r), want);
        }
    }
    assert!(dr.gpus[1].events.lock().unwrap().contains(&"DtoD"));
    assert!(sr.hooks.access.lock().unwrap().iter().any(|x| x.last()
        == Some(&(
            sys::CUmemLocationType::CU_MEM_LOCATION_TYPE_HOST_NUMA as i32,
            1
        ))));
    assert_eq!(dr.gpus[0].host_registrations.lock().unwrap().len(), 1);
    dst.unregister_memory(&plain)?;
    drop(plain);
    assert!(dr.gpus[0].host_registrations.lock().unwrap().is_empty());
    let mut all = [0; 40];
    local_host.read(0, &mut all)?;
    assert_eq!(&all[..3], &[0; 3]);
    assert_eq!(&all[37..], &[0; 3]);
    src.unregister_memory(&remote)?;
    drop(remote);
    drained(&src)?;
    src.free_egm(host)?;
    dst.unregister_memory(&egm)?;
    drop(egm);
    dst.free_egm(local_host)?;
    c.mnnvl.as_mut().unwrap().host_register = false;
    let r = Rig::new();
    let e = r.engine(c)?;
    let l = e.register_memory(vec![0xee; 16].into(), Permission::LocalOnly)?;
    let p = e.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    assert!(
        e.submit_and_wait(
            TransferOp::Read,
            &p,
            &[q(Transport::Mnnvl, &l, &dev, 2, 2, 9)],
            WAIT
        )?
        .is_complete()
    );
    assert!(r.gpus[0].host_registrations.lock().unwrap().is_empty());
    let mut want = [0xee; 16];
    want[2..11].fill(0x83);
    assert_eq!(read(&l), want);
    Ok(())
}

#[test]
fn missing_symbols_unsupported_gpu_and_bad_config_never_disable_tcp() -> Result<()> {
    let mut rig = Rig::new();
    rig.missing = true;
    let e = rig.engine(config())?;
    assert!(matches!(
        e.allocate_mnnvl(0, 32),
        Err(Error::Unavailable(_))
    ));
    let peer = e.register_memory(vec![0x76; 12].into(), Permission::RemoteRead)?;
    let dst = TransferEngine::new(EngineConfig::default())?;
    let l = dst.register_memory(vec![0xfc; 12].into(), Permission::LocalOnly)?;
    let p = dst.open_segment(e.endpoint(), e.source_epoch(), WAIT)?;
    assert!(p.metadata.mnnvl.is_none());
    assert!(
        dst.submit_and_wait(
            TransferOp::Read,
            &p,
            &[q(Transport::Unspec, &l, &peer, 2, 1, 8)],
            WAIT
        )?
        .is_complete()
    );
    let mut want = [0xfc; 12];
    want[2..10].fill(0x76);
    assert_eq!(read(&l), want);
    rig.missing = false;
    rig.fail("fabric-capability");
    assert!(rig.engine(config())?.allocate_mnnvl(0, 1).is_err());
    let mut c = config();
    c.nvlink = true;
    assert!(matches!(rig.engine(c), Err(Error::InvalidConfig(_))));
    let mut c = config();
    c.mnnvl.as_mut().unwrap().async_memcpy_threshold = usize::MAX;
    assert!(matches!(rig.engine(c), Err(Error::InvalidConfig(_))));
    Ok(())
}

#[test]
fn partial_setup_rolls_back_known_stages_and_never_retries_an_uncertain_destroy() -> Result<()> {
    for fault in [
        "create",
        "reserve",
        "map",
        "set-access",
        "partially-mapped",
        "release",
    ] {
        let r = Rig::new();
        let mut c = config();
        c.max_registered_regions = 1;
        let e = r.engine(c)?;
        r.fail(fault);
        let error = e.allocate_mnnvl(0, 37).unwrap_err();
        if ["partially-mapped", "release"].contains(&fault) {
            assert!(
                matches!(error, Error::Unquiesced { .. }),
                "{fault}: {error:?}"
            );
            assert!(matches!(
                e.allocate_mnnvl(0, 37),
                Err(Error::ResourceExhausted)
            ));
        } else {
            assert!(matches!(error, Error::Backend { .. }));
            let next = e.allocate_mnnvl(0, 37)?;
            e.free_mnnvl(next)?;
        }
        if fault == "set-access" {
            let calls = r.hooks.calls.lock().unwrap();
            assert!(
                calls
                    .windows(3)
                    .any(|w| w == ["unmap", "release", "free-va"])
            );
        }
    }
    for fault in ["unmap", "release", "free-va"] {
        let r = Rig::new();
        let mut c = config();
        c.max_registered_regions = 1;
        let e = r.engine(c)?;
        let m = e.allocate_mnnvl(0, 37)?;
        r.fail(fault);
        let before = r.calls(fault);
        assert!(matches!(e.free_mnnvl(m), Err(Error::Unquiesced { .. })));
        assert_eq!(r.calls(fault), before + 1);
        assert!(matches!(
            e.allocate_mnnvl(0, 37),
            Err(Error::ResourceExhausted)
        ));
    }
    Ok(())
}

#[test]
fn import_rejection_can_fall_back_but_a_posted_write_never_replays() -> Result<()> {
    for fault in ["import", "reserve", "map", "set-access", "copy"] {
        let sr = Rig::new();
        let dr = Rig::new();
        let src = sr.engine(config())?;
        let dst = Arc::new(dr.engine(config())?);
        let (_, r) = gpu(&src, 32, Permission::RemoteReadWrite)?;
        let l = dst.register_memory(vec![0xb9; 24].into(), Permission::LocalOnly)?;
        r.write(0, &[0x51; 32])?;
        let p = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
        if fault == "copy" {
            dr.gpus[0].copy_error.store(1, Ordering::Release)
        } else {
            dr.fail(fault)
        }
        let b = dst.submit_async(
            TransferOp::Write,
            &p,
            &[q(Transport::Unspec, &l, &r, 4, 7, 11)],
            None,
            WAIT,
        )?;
        let status = b.wait(WAIT)?;
        let t = b.task_status(0)?;
        let mut want = [0x51; 32];
        if fault == "copy" {
            assert_eq!(t.transport, Transport::Mnnvl);
            assert_eq!(t.failover_count, 0);
            assert_eq!(
                t.failure.as_ref().unwrap().remote_effect,
                RemoteEffect::Unknown
            );
            assert_eq!(status.completed_tasks, 0);
            want[7] = 0xb9;
        } else {
            assert_eq!((t.transport, t.failover_count), (Transport::Tcp, 1));
            assert_eq!(status.completed_tasks, 1);
            want[7..18].fill(0xb9);
        }
        assert_eq!(read(&r), want, "{fault}");
        assert_eq!(read(&l), [0xb9; 24]);
    }
    Ok(())
}

#[test]
fn live_cache_entry_is_not_evicted_and_cancel_waits_for_its_event() -> Result<()> {
    let mut c = config();
    c.nvlink_cache_entries = 1;
    c.max_failover_attempts = 0;
    let sr = Rig::new();
    let dr = Rig::new();
    let src = sr.engine(c.clone())?;
    let dst = Arc::new(dr.engine(c)?);
    let (_, a) = gpu(&src, 32, Permission::RemoteReadWrite)?;
    let (_, b) = gpu(&src, 32, Permission::RemoteReadWrite)?;
    a.write(0, &[0x54; 32])?;
    b.write(0, &[0x26; 32])?;
    let (_, l) = gpu(&dst, 32, Permission::LocalOnly)?;
    let (_, l2) = gpu(&dst, 32, Permission::LocalOnly)?;
    l.write(0, &[0xee; 32])?;
    l2.write(0, &[0xbf; 32])?;
    let p = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    dr.gpus[0]
        .query_hold_address
        .store((l.base_address() + 2) as usize, Ordering::Release);
    let active = dst.submit_async(
        TransferOp::Read,
        &p,
        &[
            q(Transport::Mnnvl, &l, &a, 2, 3, 11),
            q(Transport::Mnnvl, &l2, &b, 1, 2, 13),
        ],
        None,
        WAIT,
    )?;
    let end = std::time::Instant::now() + WAIT;
    while dr.gpus[0].event_not_ready.load(Ordering::Acquire) == 0 {
        assert!(std::time::Instant::now() < end);
        std::thread::yield_now()
    }
    let rejected = dst.submit_and_wait(
        TransferOp::Read,
        &p,
        &[q(Transport::Mnnvl, &l2, &b, 1, 2, 13)],
        WAIT,
    )?;
    assert!(matches!(
        rejected.failure.unwrap().error,
        Error::ResourceExhausted
    ));
    assert_eq!(read(&l2), [0xbf; 32]);
    assert_eq!(dr.calls("unmap"), 0);
    assert!(matches!(active.cancel(0), Err(Error::Unavailable(_))));
    assert!(active.cancel(1)?);
    assert_eq!(dst.stats()?.nvlink.unwrap().imports, 1);
    dr.gpus[0].query_hold_address.store(0, Ordering::Release);
    let _ = active.wait(WAIT)?;
    assert_eq!(active.task_status(1)?.state, crate::TaskState::Cancelled);
    let mut want = [0xee; 32];
    want[2..13].fill(0x54);
    assert_eq!(read(&l), want);
    assert!(
        dst.submit_and_wait(
            TransferOp::Read,
            &p,
            &[q(Transport::Mnnvl, &l2, &b, 1, 2, 13)],
            WAIT
        )?
        .is_complete()
    );
    let mut want = [0xbf; 32];
    want[1..14].fill(0x26);
    assert_eq!(read(&l2), want);
    dst.flush_nvlink_cache(WAIT)?;
    assert_eq!(dr.calls("unmap"), 2);
    assert_eq!(src.stats()?.nvlink.unwrap().exports, 0);
    Ok(())
}

#[test]
fn failed_import_retirement_keeps_mapping_budget_export_and_registration() -> Result<()> {
    for fault in ["unmap", "release"] {
        let mut c = config();
        c.nvlink_cache_entries = 1;
        let sr = Rig::new();
        let dr = Rig::new();
        let src = sr.engine(c.clone())?;
        let dst = dr.engine(c)?;
        let (_, r) = gpu(&src, 32, Permission::RemoteRead)?;
        r.write(0, &[0x52; 32])?;
        let l = dst.register_memory(vec![0xeb; 20].into(), Permission::LocalOnly)?;
        let p = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
        assert!(
            dst.submit_and_wait(
                TransferOp::Read,
                &p,
                &[q(Transport::Mnnvl, &l, &r, 2, 3, 11)],
                WAIT
            )?
            .is_complete()
        );
        let mut want = [0xeb; 20];
        want[2..13].fill(0x52);
        assert_eq!(read(&l), want);
        dr.fail(fault);
        let prev = dr.calls(fault);
        for _ in 0..2 {
            assert!(matches!(
                dst.flush_nvlink_cache(WAIT),
                Err(Error::Unquiesced { .. })
            ))
        }
        assert_eq!(dr.calls(fault), prev + 1);
        assert_eq!(dst.stats()?.nvlink.unwrap().retained_imports, 1);
        src.unregister_memory(&r)?;
        drop(r);
        assert_eq!(src.stats()?.retained_bytes, 32);
        assert_eq!(src.stats()?.nvlink.unwrap().exports, 1);
        assert!(matches!(
            src.close_source(Duration::ZERO),
            Err(Error::Timeout)
        ));
    }
    Ok(())
}

#[test]
fn posix_fd_value_imports_a_live_child_and_rejects_departed_or_replaced_exporter() -> Result<()> {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    let mut c = config();
    c.mnnvl.as_mut().unwrap().handle_type = MnnvlHandleType::PosixFd;
    let rig = Rig::new();
    let engine = rig.engine(c)?;
    let m = engine.allocate_mnnvl(0, 16)?;
    let ctx = m.allocation.lock().unwrap().context.clone();
    let exporter = "import os, sys
fd = os.memfd_create('mnnvl-child')
os.write(fd, bytes(i % 251 for i in range(4096)))
print(fd, flush=True)
sys.stdin.buffer.read(1)";
    let mut child = Command::new("python3")
        .args(["-c", exporter])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let pid = child.id() as i32;
    let mut line = String::new();
    BufReader::new(child.stdout.take().unwrap()).read_line(&mut line)?;
    let fd = line.trim().parse::<i32>().unwrap();
    let mut handle = [0; 64];
    handle[..4].copy_from_slice(&pid.to_be_bytes());
    handle[4..8].copy_from_slice(&fd.to_be_bytes());
    handle[8..24].copy_from_slice(&crate::cuda::fabric::pid_identity(pid)?);
    let platform = engine.mnnvl_platform()?;
    let mut mapped = platform.import(&ctx, 4096, handle)?;
    // SAFETY: the active shim owns the live, 4096-byte read/write mapping of the child's memfd.
    let bytes = unsafe { std::slice::from_raw_parts(mapped.address as usize as *const u8, 4096) };
    assert!(bytes.iter().enumerate().all(|(i, b)| *b == (i % 251) as u8));
    mapped.close()?;
    let mut stale = handle;
    stale[23] ^= 1;
    assert!(platform.import(&ctx, 4096, stale).is_err());
    drop(child.stdin.take());
    assert!(child.wait()?.success());
    assert!(platform.import(&ctx, 4096, handle).is_err());
    assert!(crate::cuda::fabric::duplicate_fd(pid, fd, handle[8..24].try_into().unwrap()).is_err());
    Ok(())
}
