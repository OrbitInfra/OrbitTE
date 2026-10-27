//! Query-failure injection on real verbs; recovery receives no PORT_ACTIVE event.
use super::*;
use crate::transport::rdma::discovery::RdmaAddress;

pub(super) unsafe fn query_address(
    handles: *const sys::DeviceHandles,
    address: &RdmaAddress,
    gid_index: Option<u8>,
) -> crate::Result<RdmaAddress> {
    let observed = unsafe { (sys::REAL.query_address)(handles, address, gid_index) }?;
    let mut trace = TRACE.get().unwrap().lock().unwrap();
    if trace.mode == "monitor-recovery" && trace.target && address.device == trace.rail {
        trace.native_queries += 1;
        if trace.armed {
            trace.injections += 1;
            return Err(Error::Backend {
                operation: "test-only port query failure",
                code: libc::ENETDOWN,
            });
        }
    }
    Ok(observed)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run(
    stream: &mut TcpStream,
    engine: &TransferEngine,
    memory: &crate::RegisteredMemory,
    peer: Option<&crate::RemoteSegment>,
    remote: u64,
    source: bool,
    target: bool,
    rail: usize,
    operation: TransferOp,
) {
    round(
        stream, engine, memory, peer, remote, source, 0, false, false, operation,
    );
    let original = snapshot(engine);
    let original_domains = domains(engine);
    let before = engine.stats().unwrap().rdma.unwrap();
    assert!(!original.is_empty());
    if target {
        let runtime = source_runtime(engine);
        let (owner, sibling) = {
            let contexts = runtime.context_set.lock().unwrap();
            (
                Arc::clone(contexts.slots[rail].owner.as_ref().unwrap()),
                Arc::clone(contexts.slots[1 - rail].owner.as_ref().unwrap()),
            )
        };
        let old_address = owner.address().unwrap();
        TRACE.get().unwrap().lock().unwrap().armed = true;
        // No path lookup, submission or explicit recover_progress call here.
        wait(|| {
            let contexts = runtime.context_set.lock().unwrap();
            owner.asynchronous.paused() && contexts.slots[rail].cq_list.iter().all(Option::is_none)
        });
        {
            let mut contexts = runtime.context_set.lock().unwrap();
            assert!(
                contexts.slots[rail]
                    .endpoints
                    .snapshot()
                    .unwrap()
                    .is_empty()
            );
            assert!(contexts.slots[1 - rail].cq_list.iter().any(Option::is_some));
        }
        assert!(sibling.asynchronous.check().is_ok());
        let paused = engine.stats().unwrap().rdma.unwrap();
        assert!(paused.progress_recoveries > before.progress_recoveries);
        assert_eq!(paused.opened_connections, before.opened_connections);
        assert_eq!(paused.route_recoveries, before.route_recoveries);
        assert!(paused.active_cqs < before.active_cqs);
        assert_eq!(owner.address().unwrap(), old_address);
        let native_queries = TRACE.get().unwrap().lock().unwrap().native_queries;
        TRACE.get().unwrap().lock().unwrap().armed = false;
        wait(|| engine.stats().unwrap().rdma.unwrap().route_recoveries > before.route_recoveries);
        assert!(owner.asynchronous.check().is_ok());
        assert_eq!(owner.address().unwrap(), old_address);
        assert!(TRACE.get().unwrap().lock().unwrap().native_queries > native_queries);
        assert_eq!(
            engine.stats().unwrap().rdma.unwrap().opened_connections,
            before.opened_connections
        );
    }
    super::super::super::connected::barrier(stream, 0x31);
    let mut previous = None;
    for sequence in 1..=2 {
        round(
            stream, engine, memory, peer, remote, source, sequence, false, false, operation,
        );
        let rows = snapshot(engine);
        if let Some(previous) = previous {
            assert_eq!(rows, previous, "reconnected QPs were not reused");
        } else {
            let domain = original_domains[rail];
            for row in &original {
                if row.1[..16] == domain {
                    assert!(
                        !rows.iter().any(|next| next.1 == row.1),
                        "stale connection survived recovery"
                    );
                } else {
                    assert!(rows.contains(row), "healthy rail connection was replaced");
                }
            }
            assert!(rows.iter().any(|row| row.1[..16] == domain));
        }
        previous = Some(rows);
        assert_eq!(domains(engine), original_domains);
        assert_eq!(
            engine.stats().unwrap().rdma.unwrap().opened_persistent_mrs,
            before.opened_persistent_mrs
        );
    }
    let stats = engine.stats().unwrap().rdma.unwrap();
    let trace = TRACE.get().unwrap().lock().unwrap();
    if target {
        assert!(trace.native_queries > trace.injections && trace.injections > 0);
        assert_eq!(stats.route_recoveries, before.route_recoveries + 1);
    }
    println!(
        "ORBITTE_MONITOR_RECOVERY {{\"source\":{source},\"target\":{target},\"rail\":{rail},\"native_queries\":{},\"injections\":{},\"monitor_ticks\":{},\"route_recoveries\":{},\"same_pd\":true,\"same_mrs\":true,\"healthy_rail_reused\":true,\"idle_recovery\":true,\"recovery_event_injected\":false,\"physical_port_fault\":false,\"reconnected_rounds\":2}}",
        trace.native_queries, trace.injections, stats.monitor_ticks, stats.route_recoveries
    );
}
