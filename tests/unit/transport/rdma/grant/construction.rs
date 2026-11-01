use std::time::Duration;

use super::*;
use crate::engine::tests::{rdma_fixture, rdma_fixture_with_runtime, source_runtime};
use crate::memory::MemoryRegistry;
use crate::transport::rdma::{
    buffers::tests::publication::HeldPublication, endpoint::EndpointKey,
    grant::admission::GrantCredits,
};
use crate::{Permission, TransferOp, TransferRequest, Transport};

#[test]
fn pending_grant_abort_returns_unused_owners_and_preserves_reused_qp_close_failure() {
    for (reuse, close_fails, timeout, shared) in [
        (false, false, false, false),
        (false, false, true, false),
        (true, false, false, false),
        (true, false, true, false),
        (true, true, false, false),
        (true, true, false, true),
    ] {
        let source = rdma_fixture("direct:copy");
        let mut requester_runtime = crate::transport::rdma::tests::runtime(if close_fails {
            "direct:copy_close0"
        } else {
            "direct:copy"
        });
        requester_runtime.config.max_direct_bytes = 12;
        let requester = rdma_fixture_with_runtime(requester_runtime);
        let deadline = Instant::now() + Duration::from_secs(5);
        let runtime = source_runtime(&requester);
        let device = runtime.device().unwrap();
        let mut slot = None;
        let mut sibling = None;
        let connection = if reuse {
            let local = requester
                .register_memory(vec![0; 8].into_boxed_slice(), Permission::LocalOnly)
                .unwrap();
            let remote = source
                .register_memory(vec![1; 8].into_boxed_slice(), Permission::RemoteReadWrite)
                .unwrap();
            let peer = requester
                .open_segment(
                    source.endpoint(),
                    source.source_epoch(),
                    Duration::from_secs(3),
                )
                .unwrap();
            assert!(
                requester
                    .submit_and_wait(
                        TransferOp::Read,
                        &peer,
                        &[TransferRequest {
                            remote_staging: false,
                            transport: Transport::Rdma,
                            local_address: local.base_address(),
                            remote_address: remote.base_address(),
                            length: 8,
                        }],
                        Duration::from_secs(3)
                    )
                    .unwrap()
                    .is_complete()
            );
            if shared {
                let claim = runtime
                    .claim(Arc::clone(&device), |peer| !peer.source, deadline)
                    .unwrap()
                    .unwrap();
                sibling = Some(
                    runtime
                        .endpoint(&local, 0, 8, 0, deadline, Some(claim))
                        .unwrap(),
                );
            }
            Connection::Reuse(Box::new(
                runtime
                    .claim(Arc::clone(&device), |peer| !peer.source, deadline)
                    .unwrap()
                    .unwrap(),
            ))
        } else {
            let connection = runtime
                .connection(
                    Arc::clone(&device),
                    EndpointKey {
                        route: Some(source.endpoint()),
                        epoch: source.source_epoch(),
                        domain: [1; 16],
                        operation: TransferOp::Read,
                        source: false,
                        source_nic_id: 0,
                    },
                    1,
                    None,
                    deadline,
                )
                .unwrap();
            slot = Some(
                runtime.context_set.lock().unwrap().slots[0]
                    .endpoints
                    .snapshot()
                    .unwrap()
                    .remove(0),
            );
            connection
        };
        let fixture = crate::test_support::external_memory(&[2; 16]);
        let registry = MemoryRegistry::new(SourceEpoch([32; 16]), 1, 16);
        let memory = registry
            .register_host(Arc::clone(&fixture.memory), 0, 16, Permission::LocalOnly)
            .unwrap();
        let mut gate = HeldPublication::arm(runtime, 0);
        let credits =
            GrantCredits::acquire(Arc::clone(&runtime.usage), runtime.config.max_grants, 1)
                .unwrap();
        let lease = runtime
            .reserve_grant(
                runtime
                    .allocate(4, crate::MemoryLocation::Cpu(None), u64::MAX)
                    .unwrap(),
                &memory,
                &credits,
            )
            .unwrap();
        drop(credits);
        let mut construction =
            GrantConstruction::new(runtime, lease, &memory, 4..8, 0, deadline, connection).unwrap();
        for _ in 0..32 {
            assert!(construction.poll(runtime, deadline).unwrap().is_none());
        }
        gate.assert_held();
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.active_grants,
                stats.grant_credits,
                stats.direct_bytes,
                stats.active_endpoints
            ),
            (
                1 + usize::from(shared),
                1 + usize::from(shared),
                4 + 8 * usize::from(shared),
                1
            )
        );
        assert_eq!(stats.opened_connections, u64::from(reuse));
        assert!(memory.write(4, &[9; 4]).is_err());
        let reason = if timeout {
            match construction.poll(runtime, Instant::now()) {
                Err(Error::Timeout) => Error::Timeout,
                _ => panic!("original deadline was not checked"),
            }
        } else {
            Error::Cancelled
        };
        let error = construction.abort(reason);
        assert!(
            match (close_fails && !shared, timeout) {
                (true, _) => matches!(error, Error::Unquiesced { .. }),
                (false, true) => matches!(error, Error::Timeout),
                _ => matches!(error, Error::Cancelled),
            },
            "{error:?}"
        );
        gate.assert_held();
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (stats.active_grants, stats.grant_credits, stats.direct_bytes),
            (
                usize::from(shared),
                usize::from(shared),
                8 * usize::from(shared)
            )
        );
        assert_eq!(stats.active_endpoints, usize::from(close_fails));
        assert_eq!(
            stats.retained_endpoints,
            usize::from(close_fails && !shared)
        );
        if let Some(sibling) = &sibling {
            sibling.record.connection.require_live().unwrap();
        }
        memory.write(4, &[9; 4]).unwrap();
        if let Some(slot) = slot {
            assert!(slot.reserve(None).unwrap().is_some());
        }
        drop(construction);
        registry.unregister(&memory).unwrap();
        runtime.registrations.retire(&memory).unwrap();
        gate.release();
        drop(memory);
        drop(fixture.memory);
        assert!(
            fixture.owner.upgrade().is_none(),
            "unused waiter backing cannot be retained with an older QP"
        );
        if let Some(sibling) = &mut sibling {
            assert!(matches!(sibling.close(), Err(Error::Unquiesced { .. })));
        }
        source.close(Duration::from_secs(3)).unwrap();
        if !close_fails {
            requester.close(Duration::from_secs(3)).unwrap();
        }
    }
}

static CLOSE_BEFORE_ATTACH: std::sync::OnceLock<
    std::sync::Mutex<std::collections::BTreeSet<usize>>,
> = std::sync::OnceLock::new();

pub(super) fn before_attach(runtime: &RdmaTransport, endpoint: &RdmaGrant) {
    let close = CLOSE_BEFORE_ATTACH
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .remove(&(runtime as *const _ as usize));
    if close {
        assert!(
            endpoint
                .record
                .lock()
                .unwrap()
                .resources
                .as_ref()
                .unwrap()
                .grant
                .is_none()
        );
        endpoint
            .progress
            .close(Instant::now() + Duration::from_secs(2))
            .unwrap();
    }
}

#[test]
fn lane_close_before_grant_attachment_cannot_reopen_qp_or_hide_mr_release_failure() {
    for retain in [false, true] {
        let runtime = crate::transport::rdma::tests::runtime(if retain {
            "direct:mr_close"
        } else {
            "direct:copy"
        });
        let fixture = crate::test_support::external_memory(&[3; 16]);
        let registry = MemoryRegistry::new(SourceEpoch([33; 16]), 1, 16);
        let memory = registry
            .register_host(Arc::clone(&fixture.memory), 0, 16, Permission::LocalOnly)
            .unwrap();
        // Already resolved work may outlive unregister; its MR must retire on release.
        registry.unregister(&memory).unwrap();
        let credits =
            GrantCredits::acquire(Arc::clone(&runtime.usage), runtime.config.max_grants, 1)
                .unwrap();
        let lease = runtime
            .reserve_grant(
                runtime
                    .allocate(4, crate::MemoryLocation::Cpu(None), u64::MAX)
                    .unwrap(),
                &memory,
                &credits,
            )
            .unwrap();
        drop(credits);
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut construction = GrantConstruction::new(
            &runtime,
            lease,
            &memory,
            4..8,
            0,
            deadline,
            Connection::Fresh(None),
        )
        .unwrap();
        assert!(
            CLOSE_BEFORE_ATTACH
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .insert(&runtime as *const _ as usize)
        );
        let error = match construction.poll(&runtime, deadline) {
            Err(error) => construction.abort(error),
            _ => panic!("stopped lane reopened native QP"),
        };
        assert_eq!(
            matches!(error, Error::Unquiesced { .. }),
            retain,
            "{error:?}"
        );
        drop(construction);
        let stats = runtime.stats().unwrap();
        assert_eq!(
            (
                stats.opened_connections,
                stats.active_endpoints,
                stats.active_grants,
                stats.grant_credits,
                stats.direct_bytes
            ),
            (0, 0, 0, 0, 0)
        );
        assert_eq!(
            (stats.persistent_mrs, stats.retained_persistent_mrs),
            (usize::from(retain), usize::from(retain))
        );
        let events = crate::transport::rdma::tests::events(&runtime);
        assert_eq!(
            events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| **e == "deregister_local")
                .count(),
            1
        );
        drop(memory);
        drop(fixture.memory);
        assert_eq!(fixture.owner.upgrade().is_some(), retain);
        if !retain {
            runtime.close(deadline).unwrap();
        }
        drop(runtime);
        assert_eq!(
            events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| **e == "deregister_local")
                .count(),
            1,
            "uncertain native cleanup must not be retried by Drop"
        );
    }
}

#[test]
fn provider_return_after_deadline_does_not_start_native_qp_construction() {
    let runtime = Arc::new(crate::transport::rdma::tests::runtime(
        "direct:register_pause",
    ));
    let registry = MemoryRegistry::new(SourceEpoch([34; 16]), 1, 16);
    let memory = registry
        .register(vec![4; 16].into_boxed_slice(), Permission::LocalOnly)
        .unwrap();
    let gate = crate::transport::rdma::tests::pause(&runtime);
    let deadline = Instant::now() + Duration::from_millis(100);
    let worker_runtime = Arc::clone(&runtime);
    let worker_memory = memory.clone();
    let worker = std::thread::spawn(move || {
        let credits = GrantCredits::acquire(
            Arc::clone(&worker_runtime.usage),
            worker_runtime.config.max_grants,
            1,
        )
        .unwrap();
        let lease = worker_runtime
            .reserve_grant(
                worker_runtime
                    .allocate(4, crate::MemoryLocation::Cpu(None), u64::MAX)
                    .unwrap(),
                &worker_memory,
                &credits,
            )
            .unwrap();
        drop(credits);
        let mut construction = GrantConstruction::new(
            &worker_runtime,
            lease,
            &worker_memory,
            4..8,
            0,
            deadline,
            Connection::Fresh(None),
        )
        .unwrap();
        match construction.poll(&worker_runtime, deadline) {
            Err(error) => construction.abort(error),
            _ => panic!("expired provider call started a native QP"),
        }
    });
    let (state, timeout) = gate
        .1
        .wait_timeout_while(gate.0.lock().unwrap(), Duration::from_secs(2), |state| {
            !state.0
        })
        .unwrap();
    assert!(!timeout.timed_out());
    drop(state);
    std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
    gate.0.lock().unwrap().1 = true;
    gate.1.notify_all();
    assert!(matches!(worker.join().unwrap(), Error::Timeout));
    let stats = runtime.stats().unwrap();
    assert_eq!(stats.opened_persistent_mrs, 1);
    assert_eq!(
        (
            stats.opened_connections,
            stats.active_endpoints,
            stats.active_grants,
            stats.grant_credits,
            stats.direct_bytes
        ),
        (0, 0, 0, 0, 0)
    );
    registry.unregister(&memory).unwrap();
    runtime.registrations.retire(&memory).unwrap();
    assert_eq!(runtime.stats().unwrap().persistent_mrs, 0);
    runtime
        .close(Instant::now() + Duration::from_secs(2))
        .unwrap();
}
