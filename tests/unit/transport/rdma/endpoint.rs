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
    for (grant, peer) in grants.iter().zip(&peers) {
        store.adopt(grant, *peer).unwrap();
    }
    let endpoints = store.snapshot().unwrap();
    assert_eq!(endpoints.len(), 1);
    let endpoint = &endpoints[0];
    let qps = endpoint.qp_list.lock().unwrap();
    assert_eq!(qps.len(), 6);
    let mut lanes = [0; 3];
    for qp in qps.iter() {
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
    assert!(store.snapshot().unwrap().is_empty());
    drop(grants);
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
            .map(|qp| {
                let lane = qp.lane.upgrade().unwrap();
                (
                    Arc::clone(&qp.record),
                    *lane.worker_id.get().unwrap(),
                    qp.peer,
                )
            })
            .collect()
    }
}
