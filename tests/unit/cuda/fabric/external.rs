use super::*;

#[test]
fn external_vmm_requires_the_whole_extent_and_retains_the_supplied_owner() -> Result<()> {
    let mut c = config();
    c.mnnvl.as_mut().unwrap().egm = true;
    let rig = Rig::new();
    let e = rig.engine(c.clone())?;
    let requester = Rig::new().engine(c)?;
    let dst = requester.register_memory(vec![0xe7; 16].into(), Permission::LocalOnly)?;
    let original = e.allocate_mnnvl(0, 64)?;
    original.write(0, &[0x56; 64])?;
    let id = original.base_address();
    rig.fail("range-mismatch");
    // SAFETY: the owner retains initialized, offset-zero storage of the supplied exact extent.
    let wrong = unsafe { e.wrap_mnnvl(0, id, 32, 4096, original.clone()) };
    assert!(matches!(wrong, Err(Error::InvalidRange)));
    let weak = Arc::downgrade(&original);
    // SAFETY: this fixture granted all visible GPUs access to the initialized page-rounded extent.
    let wrapped = unsafe { e.wrap_mnnvl(0, id, 64, 4096, original) }?;
    let r = e
        .register_cuda_memory_batch(&[(wrapped.clone(), 3, 40)], Permission::RemoteRead)?
        .remove(0);
    let p = requester.open_segment(e.endpoint(), e.source_epoch(), WAIT)?;
    assert!(
        requester
            .submit_and_wait(
                TransferOp::Read,
                &p,
                &[q(Transport::Mnnvl, &dst, &r, 3, 5, 8)],
                WAIT
            )?
            .is_complete()
    );
    let mut want = [0xe7; 16];
    want[3..11].fill(0x56);
    assert_eq!(read(&dst), want);
    assert!(weak.upgrade().is_some());
    e.unregister_memory(&r)?;
    drop(r);
    drained(&e)?;
    e.free_mnnvl(wrapped)?;
    assert!(weak.upgrade().is_none());

    let original = e.allocate_egm(1, 64)?;
    original.write(0, &[0x9a; 64])?;
    let id = original.base_address();
    rig.fail("numa-mismatch");
    // SAFETY: the caller supplies the actual full HOST_NUMA mapping. The shim reports a conflicting property.
    let wrong = unsafe { e.wrap_egm(1, id, 64, 4096, original.clone()) };
    assert!(matches!(wrong, Err(Error::Unavailable(_))));
    let weak = Arc::downgrade(&original);
    // SAFETY: this wrapper exclusively represents the initialized full mapping on its actual socket.
    let host = unsafe { e.wrap_egm(1, id, 64, 4096, original) }?;
    let r = e.register_host_memory(host.clone(), 3, 40, Permission::RemoteRead)?;
    let p = requester.open_segment(e.endpoint(), e.source_epoch(), WAIT)?;
    assert!(
        requester
            .submit_and_wait(
                TransferOp::Read,
                &p,
                &[q(Transport::Mnnvl, &dst, &r, 3, 5, 8)],
                WAIT
            )?
            .is_complete()
    );
    want[3..11].fill(0x9a);
    assert_eq!(read(&dst), want);
    e.unregister_memory(&r)?;
    drop(r);
    e.close_source(WAIT)?;
    e.free_egm(host)?;
    assert!(weak.upgrade().is_none());
    Ok(())
}

#[test]
fn physical_aliases_merge_only_within_the_same_execution_context() -> Result<()> {
    let c = config();
    let sr = Rig::new();
    let dr = Rig::new();
    let src = sr.engine(c.clone())?;
    let dst = Arc::new(dr.engine(c)?);
    let (_, r) = gpu(&src, 48, Permission::RemoteRead)?;
    r.write(0, &[0x79; 48])?;
    let (_, l) = gpu(&dst, 24, Permission::LocalOnly)?;
    let second = dst.allocate_mnnvl(1, 24)?;
    let other = dst
        .register_cuda_memory_batch(&[(second, 0, 24)], Permission::LocalOnly)?
        .remove(0);
    l.write(0, &[0xee; 24])?;
    other.write(0, &[0xdd; 24])?;
    let p = dst.open_segment(src.endpoint(), src.source_epoch(), WAIT)?;
    let b = dst.submit_async(
        TransferOp::Read,
        &p,
        &[
            q(Transport::Mnnvl, &l, &r, 2, 2, 4),
            q(Transport::Mnnvl, &l, &r, 6, 6, 4),
            q(Transport::Mnnvl, &other, &r, 3, 14, 7),
        ],
        None,
        WAIT,
    )?;
    assert_eq!(b.wait(WAIT)?.completed_tasks, 3);
    assert_eq!(
        b.task_status(0)?.physical_owner,
        b.task_status(1)?.physical_owner
    );
    assert_ne!(
        b.task_status(0)?.physical_owner,
        b.task_status(2)?.physical_owner
    );
    assert_eq!(dr.calls("import"), 2);
    let mut a = [0xee; 24];
    a[2..10].fill(0x79);
    assert_eq!(read(&l), a);
    let mut c = [0xdd; 24];
    c[3..10].fill(0x79);
    assert_eq!(read(&other), c);
    assert_eq!(read(&r), [0x79; 48]);
    Ok(())
}

#[test]
fn loader_switch_uses_presence_and_allows_typed_disable() {
    const TEST: &str =
        "cuda::tests::fabric::external::loader_switch_uses_presence_and_allows_typed_disable";
    if let Ok(expected) = std::env::var("ORBITTE_TEST_MNNVL_SWITCH") {
        assert_eq!(
            EngineConfig::default().mnnvl.is_some(),
            expected == "present"
        );
        let c = EngineConfig {
            mnnvl: None,
            ..Default::default()
        };
        assert!(c.mnnvl.is_none());
        return;
    }
    for enabled in [false, true] {
        let mut child = std::process::Command::new(std::env::current_exe().unwrap());
        child.args(["--exact", TEST]).env_remove("MC_ENABLE_MNNVL");
        if enabled {
            child.env("MC_ENABLE_MNNVL", "0");
        }
        let out = child
            .env(
                "ORBITTE_TEST_MNNVL_SWITCH",
                if enabled { "present" } else { "absent" },
            )
            .output()
            .unwrap();
        assert!(out.status.success(), "{:?}", out);
    }
}
