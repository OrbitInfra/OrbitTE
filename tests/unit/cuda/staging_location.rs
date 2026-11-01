use super::*;
use crate::StagingLocation;
use crate::platform::numa::{Numa, page_round};
use crate::runtime::proxy_manager::ProxyManager;
use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::sync::atomic::AtomicI32;
use std::time::Instant;

pub(super) static SERIAL: Mutex<()> = Mutex::new(());
static MAPPINGS: OnceLock<Mutex<BTreeMap<usize, (usize, u32)>>> = OnceLock::new();
static NEAR: AtomicI32 = AtomicI32::new(-1);
static ALLOW_NEAR: AtomicBool = AtomicBool::new(true);
static FAIL_BIND: AtomicBool = AtomicBool::new(false);
static FAIL_UNMAP: AtomicBool = AtomicBool::new(false);
static UNMAPS: AtomicUsize = AtomicUsize::new(0);
static NUMA: Numa = Numa {
    nodes: || {
        Ok(if ALLOW_NEAR.load(Ordering::Acquire) {
            vec![130, 2]
        } else {
            vec![2]
        })
    },
    gpu_node: |pci| {
        let node = NEAR.load(Ordering::Acquire);
        if node >= 0 {
            Some(node as u32)
        } else {
            u32::from_str_radix(&pci[5..7], 16).ok()
        }
    },
    bind: |pointer, length, node| {
        if FAIL_BIND.load(Ordering::Acquire) {
            return Err(Error::Unavailable("test mbind"));
        }
        assert!(
            MAPPINGS
                .get_or_init(Mutex::default)
                .lock()
                .unwrap()
                .insert(pointer.as_ptr() as usize, (length, node))
                .is_none()
        );
        Ok(())
    },
    unmap: |pointer, length| {
        UNMAPS.fetch_add(1, Ordering::AcqRel);
        if FAIL_UNMAP.load(Ordering::Acquire) {
            return Err(Error::Io(std::io::Error::from_raw_os_error(libc::EIO)));
        }
        MAPPINGS
            .get_or_init(Mutex::default)
            .lock()
            .unwrap()
            .remove(&(pointer.as_ptr() as usize));
        assert_eq!(unsafe { libc::munmap(pointer.as_ptr().cast(), length) }, 0);
        Ok(())
    },
};
pub(super) fn reset() {
    NEAR.store(-1, Ordering::Release);
    ALLOW_NEAR.store(true, Ordering::Release);
    FAIL_BIND.store(false, Ordering::Release);
    FAIL_UNMAP.store(false, Ordering::Release);
    UNMAPS.store(0, Ordering::Release);
}
pub(super) fn pool(chunk: usize, location: StagingLocation, count: usize) -> Arc<ProxyManager> {
    let mut pool = ProxyManager::new(chunk, count, location).unwrap();
    Arc::get_mut(&mut pool).unwrap().numa = &NUMA;
    pool
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

#[test]
fn location_selection_reuses_only_matching_context_and_node_under_one_physical_budget() {
    let _serial = SERIAL.lock().unwrap();
    reset();
    let owner = allocation(&[0x57; 16]);
    owner.ordinal.store(130, Ordering::Release);
    let gpu = memory(&owner);
    let pool = pool(7, StagingLocation::Auto, 1);
    let physical = page_round(7).unwrap();
    for node in [130, 130, 2] {
        ALLOW_NEAR.store(node == 130, Ordering::Release);
        let mut bytes = [0; 16];
        gpu.copy_staged(0, &mut bytes, None, &pool, deadline())
            .unwrap();
        assert_eq!(bytes, [0x57; 16]);
        let map = MAPPINGS.get().unwrap().lock().unwrap().clone();
        let registered = owner.host_registrations.lock().unwrap().clone();
        assert_eq!(registered.len(), 1);
        let (&pointer, &length) = registered.first_key_value().unwrap();
        assert_eq!(map[&pointer], (physical, node as u32));
        assert_eq!(length, physical);
        assert_eq!(pool.stats().unwrap().reserved_bytes, physical);
    }
    assert_eq!(owner.pci_device.load(Ordering::Acquire), 130);
    assert_eq!(
        owner
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| **e == "pci-bus-id")
            .count(),
        1
    );
    assert_eq!(
        (
            pool.stats().unwrap().allocations,
            pool.stats().unwrap().reuses
        ),
        (2, 1)
    );
    assert_eq!(UNMAPS.load(Ordering::Acquire), 1);
    // Busy slots are not a per-location budget: another location cannot exceed it.
    let context = gpu.allocation.lock().unwrap().context.clone();
    let active = pool
        .pin_stage_buffer(context.raw.as_ptr() as usize, Some(2), |_| unreachable!())
        .unwrap();
    ALLOW_NEAR.store(true, Ordering::Release);
    assert!(matches!(
        gpu.copy_staged(0, &mut [0; 16], None, &pool, deadline()),
        Err(Error::ResourceExhausted)
    ));
    drop(active);
    pool.close(deadline()).unwrap();
    assert_eq!(UNMAPS.load(Ordering::Acquire), 2);
    assert!(owner.host_registrations.lock().unwrap().is_empty());
    assert!(owner.pins.lock().unwrap().is_empty());
    assert!(ProxyManager::new((1 << 30) / 4096 - 1, 4096, StagingLocation::Auto).is_ok());
    assert!(ProxyManager::new((1 << 30) / 4096 + 1, 4096, StagingLocation::Auto).is_err());
}

#[test]
fn unknown_gpu_uses_first_eligible_node_but_explicit_denied_and_binding_failure_never_fallback() {
    let _serial = SERIAL.lock().unwrap();
    reset();
    let owner = allocation(&[0x19; 16]);
    let gpu = memory(&owner);
    for (location, bind_failure, expected) in [
        (StagingLocation::Auto, false, Some(2)),
        (StagingLocation::Cpu(130), false, Some(130)),
        (StagingLocation::Cpu(3), false, None),
        (StagingLocation::Cpu(2), true, None),
    ] {
        FAIL_BIND.store(bind_failure, Ordering::Release);
        let pool = pool(7, location, 1);
        let result = gpu.copy_staged(0, &mut [0; 16], None, &pool, deadline());
        assert_eq!(result.is_ok(), expected.is_some());
        if let Some(node) = expected {
            let registrations = owner.host_registrations.lock().unwrap();
            let (&pointer, _) = registrations.first_key_value().unwrap();
            assert_eq!(MAPPINGS.get().unwrap().lock().unwrap()[&pointer].1, node);
        } else {
            assert_eq!(pool.stats().unwrap().reserved_bytes, 0);
        }
        pool.close(deadline()).unwrap();
        assert!(owner.pins.lock().unwrap().is_empty());
    }
    reset();
}

#[test]
fn mapped_pin_failures_retain_backing_context_and_charge_without_repeating_native_cleanup() {
    let _serial = SERIAL.lock().unwrap();
    for case in [
        "register",
        "register-pop",
        "unregister",
        "unregister-pop",
        "munmap",
    ] {
        reset();
        let owner = allocation(&[0x57; 16]);
        let weak = Arc::downgrade(&owner);
        let gpu = memory(&owner);
        let manager = pool(7, StagingLocation::Cpu(2), 1);
        if case == "register" {
            owner.pin_error.store(1, Ordering::Release);
        }
        if case == "register-pop" {
            owner.pop_after_pin.store(true, Ordering::Release);
        }
        let result = gpu.copy_staged(0, &mut [0; 16], None, &manager, deadline());
        if case.starts_with("register") {
            assert!(
                matches!(result, Err(Error::Unquiesced { .. })),
                "{case}: {result:?}"
            );
        } else {
            result.unwrap();
        }
        if case == "unregister" {
            owner.free_error.store(1, Ordering::Release);
        }
        if case == "unregister-pop" {
            owner.pop_after_unpin.store(true, Ordering::Release);
        }
        FAIL_UNMAP.store(case == "munmap", Ordering::Release);
        for _ in 0..2 {
            assert!(matches!(
                manager.close(deadline()),
                Err(Error::Unquiesced { .. })
            ));
        }
        assert_eq!(manager.stats().unwrap().retained_chunks, 1);
        assert_eq!(
            manager.stats().unwrap().reserved_bytes,
            page_round(7).unwrap()
        );
        assert_eq!(
            owner
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| **e == "host-unregister")
                .count(),
            usize::from(!case.starts_with("register"))
        );
        assert_eq!(
            UNMAPS.load(Ordering::Acquire),
            usize::from(case == "munmap")
        );
        assert_eq!(
            owner.host_registrations.lock().unwrap().len(),
            usize::from(!matches!(case, "unregister-pop" | "munmap"))
        );
        assert!(owner.pins.lock().unwrap().is_empty());
        drop(gpu);
        drop(manager);
        drop(owner);
        assert!(
            weak.upgrade().is_some(),
            "{case} lost its real CUDA context owner"
        );
    }
    reset();
}

fn engine(transport: Transport) -> TransferEngine {
    let proxy = pool(7, StagingLocation::Auto, 3);
    let config = EngineConfig {
        remote_staging: true,
        staging_chunk_size: 7,
        staging_chunk_count: 3,
        staging_pipeline_depth: 3,
        staging_location: StagingLocation::Auto,
        ..EngineConfig::default()
    };
    #[cfg(feature = "rdma")]
    let rdma = if transport == Transport::Rdma {
        let mut runtime = crate::transport::rdma::tests::runtime("direct:copy");
        runtime.proxy = proxy.clone();
        Some(runtime)
    } else {
        None
    };
    let _ = transport;
    crate::engine::tests::staging_fixture(
        config,
        proxy,
        #[cfg(feature = "rdma")]
        rdma,
    )
}

#[test]
fn both_endpoints_select_their_own_gpu_location_for_public_tcp_and_rdma_transfers() {
    let _serial = SERIAL.lock().unwrap();
    reset();
    for transport in [
        Transport::Tcp,
        #[cfg(feature = "rdma")]
        Transport::Rdma,
    ] {
        for operation in [TransferOp::Read, TransferOp::Write] {
            let source = engine(transport);
            let requester = engine(transport);
            let source_bytes: [u8; 64] = std::array::from_fn(|i| (i * 37 + 11) as u8);
            let local_bytes: [u8; 64] = std::array::from_fn(|i| (i * 19 + 53) as u8);
            let a = allocation(&source_bytes);
            a.ordinal.store(2, Ordering::Release);
            let b = allocation(&local_bytes);
            b.ordinal.store(130, Ordering::Release);
            let source_gpu = memory(&a);
            let local_gpu = memory(&b);
            let remote = source
                .register_cuda_memory_batch(
                    &[(source_gpu.clone(), 0, 64)],
                    Permission::RemoteReadWrite,
                )
                .unwrap()
                .remove(0);
            let local = requester
                .register_cuda_memory_batch(&[(local_gpu.clone(), 0, 64)], Permission::LocalOnly)
                .unwrap()
                .remove(0);
            let peer = requester
                .open_segment(
                    source.endpoint(),
                    source.source_epoch(),
                    Duration::from_secs(3),
                )
                .unwrap();
            let request = TransferRequest {
                priority: Default::default(),
                remote_staging: true,
                transport,
                local_address: local.base_address() + 2,
                remote_address: remote.base_address() + 1,
                length: 41,
            };
            // A repeat consumes the same idle mapped slots, preserving both real locations.
            for _ in 0..2 {
                let report = requester
                    .submit_and_wait(
                        operation,
                        &peer,
                        std::slice::from_ref(&request),
                        Duration::from_secs(5),
                    )
                    .unwrap();
                assert!(
                    report.is_complete() && report.locally_drained,
                    "{transport:?} {operation:?}: {report:?}"
                );
                for (owner, node, engine) in [(&a, 2, &source), (&b, 130, &requester)] {
                    assert_eq!(owner.pci_device.load(Ordering::Acquire), node);
                    assert_eq!(owner.host_registrations.lock().unwrap().len(), 3);
                    for (&pointer, &length) in owner.host_registrations.lock().unwrap().iter() {
                        assert_eq!(
                            MAPPINGS.get().unwrap().lock().unwrap()[&pointer],
                            (length, node as u32)
                        );
                    }
                    assert_eq!(
                        engine.stats().unwrap().staging.reserved_bytes,
                        3 * page_round(7).unwrap()
                    );
                    #[cfg(feature = "rdma")]
                    if let Some(stats) = engine.stats().unwrap().rdma {
                        assert_eq!(
                            stats.persistent_mr_bytes, 0,
                            "private MR must retire before mapped backing"
                        );
                    }
                }
            }
            let mut expected_source = source_bytes;
            let mut expected_local = local_bytes;
            if operation == TransferOp::Read {
                expected_local[2..43].copy_from_slice(&source_bytes[1..42]);
            } else {
                expected_source[1..42].copy_from_slice(&local_bytes[2..43]);
            }
            let mut bytes = [0; 64];
            source_gpu.read(0, &mut bytes).unwrap();
            assert_eq!(bytes, expected_source);
            local_gpu.read(0, &mut bytes).unwrap();
            assert_eq!(bytes, expected_local);
            requester.close(Duration::from_secs(3)).unwrap();
            source.close(Duration::from_secs(3)).unwrap();
            for owner in [&a, &b] {
                assert!(owner.host_registrations.lock().unwrap().is_empty());
            }
        }
    }
}

#[test]
fn unmap_failure_preserves_its_errno_and_mapping_after_successful_unregister() {
    let _serial = SERIAL.lock().unwrap();
    reset();
    let owner = allocation(&[0x57; 16]);
    let weak = Arc::downgrade(&owner);
    let gpu = memory(&owner);
    let context = gpu.allocation.lock().unwrap().context.clone();
    let buffer = PinnedAllocation::new_at(&context, 7, Some(2), &NUMA).unwrap();
    let address = buffer.address;
    let buffer = Arc::new(buffer);
    let host = HostMemory::from_cuda_pinned(Arc::clone(&buffer));
    assert_eq!(
        host.location(1, 3).unwrap().location,
        crate::MemoryLocation::Cpu(Some(2))
    );
    drop(host);
    let buffer = Arc::try_unwrap(buffer).ok().unwrap();
    FAIL_UNMAP.store(true, Ordering::Release);
    assert!(matches!(
        buffer.close(),
        Err(Error::Unquiesced {
            operation: "staging host munmap",
            code: libc::EIO
        })
    ));
    assert_eq!(UNMAPS.load(Ordering::Acquire), 1);
    assert!(owner.host_registrations.lock().unwrap().is_empty());
    assert!(
        MAPPINGS
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .contains_key(&(address.as_ptr() as usize))
    );
    let mut resident = 0u8;
    assert_eq!(
        unsafe {
            libc::mincore(
                address.as_ptr().cast(),
                page_round(7).unwrap(),
                &mut resident,
            )
        },
        0
    );
    drop(context);
    drop(gpu);
    drop(owner);
    assert!(weak.upgrade().is_some());
    reset();
}
