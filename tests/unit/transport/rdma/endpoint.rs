use super::*;
use crate::transport::rdma::tests::runtime;
use std::time::{Duration, Instant};

#[test]
fn peer_qp_arrays_keep_lane_owners_and_retire_only_after_native_drain() {
    let mut runtime = runtime("copy");
    runtime.config.num_lanes = 3;
    runtime.config.max_endpoints = 8;
    runtime.config.max_staging_bytes = 128;
    let mut grants: Vec<_> = (0..6)
        .map(|_| runtime.staging_endpoint(8, 0).unwrap())
        .collect();
    let domain = [0x23; 16];
    let mut store = super::super::endpoint_store::EndpointStore::default();
    let peers: Vec<_> = grants
        .iter()
        .enumerate()
        .map(|(index, grant)| Peer {
            route: Some("127.0.0.1:1234".parse().unwrap()),
            source_nic_id: 0,
            epoch: SourceEpoch([7; 16]),
            local: grant.incarnation().unwrap(),
            connection: ConnectionId {
                domain,
                generation: index as u64 + 1,
            },
            operation: TransferOp::Read,
            source: false,
            generation: 1,
        })
        .collect();
    for (grant, peer) in grants.iter_mut().zip(&peers) {
        let endpoint = store.get_or_insert((*peer).into(), 6).unwrap();
        let slot = endpoint.reserve(None).unwrap().unwrap();
        slot.attach(grant).unwrap();
        endpoint.adopt(grant, *peer).unwrap();
    }
    let endpoints = store.snapshot().unwrap();
    assert_eq!(endpoints.len(), 1);
    let endpoint = &endpoints[0];
    let qps = endpoint.qp_list.lock().unwrap();
    assert_eq!(qps.len(), 6);
    let mut lanes = [0; 3];
    for qp in qps.iter().flatten() {
        lanes[*qp.lane.upgrade().unwrap().worker_id.get().unwrap()] += 1;
    }
    assert_eq!(lanes, [2, 2, 2]);
    drop(qps);
    let mut wrong = peers[0];
    wrong.connection.domain[0] ^= 1;
    assert!(endpoint.adopt(&grants[0], wrong).is_err());
    wrong = peers[0];
    wrong.connection.generation += 100;
    assert!(endpoint.adopt(&grants[0], wrong).is_err());
    let mut newer = peers[0];
    newer.generation = 7;
    endpoint.adopt(&grants[0], newer).unwrap();
    newer.generation = 3;
    endpoint.adopt(&grants[0], newer).unwrap();
    assert_eq!(
        endpoint
            .candidates(&|peer| peer.local == peers[0].local)
            .unwrap()[0]
            .2
            .generation,
        7,
        "out-of-order grants cannot roll back the connection's observed lower bound"
    );
    // Lookup detachment alone cannot erase a physical QP owner.
    grants[0]
        .record
        .terminal
        .store(true, std::sync::atomic::Ordering::Release);
    assert!(!endpoint.prune().unwrap());
    assert_eq!(endpoint.qp_list.lock().unwrap().len(), 6);
    for grant in &mut grants {
        grant.close().unwrap();
    }
    assert!(endpoint.prune().unwrap());
    drop(grants);
    drop(endpoints);
    assert!(store.snapshot().unwrap().is_empty());
    runtime
        .close(Instant::now() + Duration::from_secs(3))
        .unwrap();
}

impl RdmaEndpoint {
    pub(in crate::transport::rdma) fn native_qps(&self) -> Vec<(Arc<Record>, usize, Peer)> {
        self.qp_list
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .filter(|qp| qp.peer.is_some())
            .map(|qp| {
                let lane = qp.lane.upgrade().unwrap();
                (
                    Arc::clone(qp.record.as_ref().unwrap()),
                    *lane.worker_id.get().unwrap(),
                    qp.peer.unwrap(),
                )
            })
            .collect()
    }
}

fn key() -> EndpointKey {
    EndpointKey {
        route: Some("127.0.0.1:1234".parse().unwrap()),
        epoch: SourceEpoch([7; 16]),
        domain: [0x23; 16],
        source: false,
        source_nic_id: 0,
        operation: TransferOp::Read,
    }
}

#[test]
fn cold_reservations_are_atomic_and_unbound_cancellation_returns_only_its_slot() {
    for width in [1, 3, 6] {
        let endpoint = Arc::new(RdmaEndpoint::new(key(), width).unwrap());
        let admitted = std::sync::atomic::AtomicUsize::new(0);
        let barrier = std::sync::Barrier::new(32);
        std::thread::scope(|scope| {
            for _ in 0..32 {
                let endpoint = &endpoint;
                let barrier = &barrier;
                let admitted = &admitted;
                scope.spawn(move || {
                    let reservation = endpoint.reserve(None).unwrap();
                    if reservation.is_some() {
                        admitted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    barrier.wait();
                    assert_eq!(admitted.load(std::sync::atomic::Ordering::Relaxed), width);
                    drop(reservation);
                });
            }
        });
        assert!(endpoint.prune().unwrap());
        let remote = ConnectionId {
            domain: key().domain,
            generation: 1,
        };
        let slot = endpoint.reserve(Some(remote)).unwrap().unwrap();
        assert!(
            endpoint.reserve(Some(remote)).unwrap().is_none(),
            "duplicate bootstrap allocated a second QP"
        );
        drop(slot);
        assert!(endpoint.reserve(Some(remote)).unwrap().is_some());
    }
}

#[test]
fn cancelled_construction_counts_until_native_drain_and_cannot_release_a_replacement() {
    for mode in ["copy", "close0"] {
        let runtime = runtime(mode);
        let endpoint = Arc::new(RdmaEndpoint::new(key(), 1).unwrap());
        let slot = endpoint.reserve(None).unwrap().unwrap();
        let mut grant = runtime.staging_endpoint(8, 0).unwrap();
        slot.attach(&mut grant).unwrap();
        assert!(endpoint.reserve(None).unwrap().is_none());
        let result = grant.close();
        if mode == "close0" {
            assert!(matches!(result, Err(Error::Unquiesced { .. })));
            drop(slot);
            assert!(
                endpoint.reserve(None).unwrap().is_none(),
                "retained native QP lost its width charge"
            );
            assert!(
                runtime
                    .close(Instant::now() + Duration::from_secs(1))
                    .is_err()
            );
        } else {
            result.unwrap();
            let replacement = endpoint.reserve(None).unwrap().unwrap();
            drop(slot);
            assert!(
                endpoint.reserve(None).unwrap().is_none(),
                "old construction released the next slot generation"
            );
            drop(replacement);
            assert!(endpoint.prune().unwrap());
            drop(grant);
            runtime
                .close(Instant::now() + Duration::from_secs(1))
                .unwrap();
        }
    }
}

#[test]
fn cold_waiter_deadline_cannot_cancel_the_builder_and_retry_reuses_the_slot() {
    use crate::transport::rdma::rdma_transport::connections::Connection;
    let runtime = runtime("copy");
    let device = runtime.device().unwrap();
    let claim = runtime
        .connection(
            Arc::clone(&device),
            key(),
            1,
            None,
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
    assert!(matches!(claim, Connection::Fresh(Some(_))));
    assert!(matches!(
        runtime.connection(
            Arc::clone(&device),
            key(),
            1,
            None,
            Instant::now() + Duration::from_millis(20)
        ),
        Err(Error::Timeout)
    ));
    assert_eq!(runtime.stats().unwrap().opened_connections, 0);
    drop(claim);
    let retry = runtime
        .connection(
            device,
            key(),
            1,
            None,
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
    assert!(matches!(retry, Connection::Fresh(Some(_))));
    drop(retry);
    runtime
        .close(Instant::now() + Duration::from_secs(1))
        .unwrap();
}
