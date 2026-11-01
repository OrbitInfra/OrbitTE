//! Stable NIC slots, shared device/PD owners and per-context CQ-lane lifecycles.

use std::sync::Arc;

use super::context::AddressRefresh;
use super::{RdmaContext, cq_lane, quota};

#[derive(Default)]
pub(super) struct ContextSet {
    pub(super) closed: bool,
    pub(super) topology: Option<crate::Topology>,
    pub(super) slots: Vec<ContextSlot>,
    pub(super) selector: Option<Arc<quota::DeviceSelector>>,
    pub(super) failure: Option<(&'static str, i32)>,
}

#[derive(Default)]
pub(super) struct ContextSlot {
    pub(super) owner: Option<Arc<RdmaContext>>,
    pub(super) cq_list: Vec<Option<Arc<cq_lane::CqLane>>>,
    pub(super) endpoints: super::endpoint_store::EndpointStore,
}

impl ContextSet {
    pub(super) fn reclaim(&mut self, usage: &std::sync::Mutex<super::RdmaStats>) {
        if self.closed || self.failure.is_some() {
            return;
        }
        let mut recovered_lanes = 0;
        let mut queries = 0;
        let mut recovered_routes = 0;
        for (id, slot) in self.slots.iter_mut().enumerate() {
            let Some(owner) = &slot.owner else {
                continue;
            };
            for entry in &mut slot.cq_list {
                if entry
                    .as_ref()
                    .is_some_and(|lane| lane.recoverable().unwrap_or(false))
                {
                    // recoverable includes native QP/CQ drain and worker detachment.
                    *entry = None;
                    recovered_lanes += 1;
                }
            }
            if slot.endpoints.prune().is_err() {
                continue;
            }
            let drained = slot.cq_list.iter().all(Option::is_none)
                && slot.endpoints.quiescent().unwrap_or(false);
            let refreshed = owner.refresh_address(drained);
            queries += u64::from(!matches!(refreshed, AddressRefresh::Deferred));
            let address = match refreshed {
                AddressRefresh::Active(address) => Some(address),
                AddressRefresh::Resumed(address) => {
                    recovered_routes += 1;
                    Some(address)
                }
                AddressRefresh::Deferred | AddressRefresh::Paused => None,
            };
            let mut available = owner.asynchronous.check().is_ok();
            if let (Some(address), Some(topology)) = (address, &mut self.topology) {
                if let Some(selector) = &self.selector {
                    selector.set_bandwidth(id, address.link_speed_mbps);
                }
                if address.publish(topology).is_err() {
                    owner.asynchronous.fence_route();
                    available = false;
                }
            }
            if let Some(selector) = &self.selector {
                selector.set_available(id, available);
            }
        }
        let mut stats = usage.lock().unwrap_or_else(|p| p.into_inner());
        stats.monitor_ticks += 1;
        stats.progress_recoveries += recovered_lanes;
        stats.route_queries += queries;
        stats.route_recoveries += recovered_routes;
    }
}
