/// A retained endpoint is charged and blocks further RDMA admission. It can
/// still own pinned pages or RNIC resources after native teardown failed.
#[derive(Clone, Copy, Debug, Default)]
pub struct RdmaStats {
    /// Shared context/PD owners, including partially constructed retained ones.
    pub active_devices: usize,
    pub retained_devices: usize,
    /// Successful context/PD constructions during this runtime lifetime.
    pub opened_devices: u64,
    pub active_windows: usize,
    pub opened_windows: u64,
    /// Historical native MW indices charged for the lifetime of their PD.
    pub window_key_indices: usize,
    /// Full rkeys burned before an endpoint can submit BIND, including failures.
    pub burned_window_keys: u64,
    pub window_key_exhaustions: u64,
    pub retained_windows: usize,
    pub completed_window_binds: u64,
    pub completed_window_invalidations: u64,
    /// Source completion credits: parent reservations and unreaped message MRs.
    pub receive_credits: usize,
    pub peak_receive_credits: usize,
    pub receive_admission_rejections: u64,
    pub receive_work_requests: usize,
    pub retained_receive_work_requests: usize,
    pub peak_receive_work_requests: usize,
    pub receive_post_calls: u64,
    pub completed_receives: u64,
    pub completed_sends: u64,
    pub active_message_mrs: usize,
    pub retained_message_mrs: usize,
    pub message_bytes: usize,
    pub retained_message_bytes: usize,
    pub active_completion_channels: usize,
    pub opened_completion_channels: u64,
    pub retained_completion_channels: usize,
    /// Live completion-channel registrations in the transport monitor.
    pub completion_event_sources: usize,
    /// One transport-wide thread consumes all device and CQ-channel events.
    pub monitor_workers: usize,
    pub opened_monitor_workers: u64,
    /// Completed periodic context/endpoint scans, independent of new requests.
    pub monitor_ticks: u64,
    pub route_queries: u64,
    pub route_recoveries: u64,
    /// Successful native gets, each acknowledged before notifying progress.
    pub completion_events: u64,
    pub cq_notification_arms: u64,
    pub cq_event_waits: u64,
    pub active_cqs: usize,
    pub retained_cqs: usize,
    pub opened_cqs: u64,
    /// Live transport-level data threads, including idle workers.
    pub progress_workers: usize,
    /// Successful data-thread starts; context recovery does not start threads.
    pub opened_progress_workers: u64,
    /// CQ lanes still assigned to data workers, summed across all contexts.
    pub progress_lanes: usize,
    pub async_event_sources: usize,
    pub acknowledged_async_events: u64,
    /// Cumulative shared-progress faults, including permanently retained owners.
    pub progress_failures: u64,
    /// Faulted progress owners retired after complete native drain and worker detachment.
    pub progress_recoveries: u64,
    /// Highest pending data-QP count observed by any single CQ lane progress pass.
    pub peak_pending_qps: usize,
    pub active_endpoints: usize,
    pub idle_endpoints: usize,
    pub opened_connections: u64,
    pub closed_connections: u64,
    pub reused_connections: u64,
    pub evicted_connections: u64,
    pub completed_reusable_grants: u64,
    /// Per-request memory owners, including outputs after physical QP close.
    pub active_grants: usize,
    /// Whole-parent grant reservations, held through every child cleanup.
    pub grant_credits: usize,
    pub retained_grants: usize,
    pub staging_bytes: usize,
    /// Direct host bytes charged through endpoint release, including retention.
    pub direct_bytes: usize,
    /// Successfully constructed direct host endpoints during this runtime.
    pub opened_direct_endpoints: u64,
    pub retained_endpoints: usize,
    pub retained_bytes: usize,
    /// All cached physical MRs, including creation, closing and retained failures.
    pub persistent_mrs: usize,
    pub persistent_mr_bytes: usize,
    pub retained_persistent_mrs: usize,
    pub opened_persistent_mrs: u64,
    pub reused_persistent_mrs: u64,
    pub evicted_persistent_mrs: u64,
    /// MW-bind parent subset of persistent_mrs; never remote-enabled MRs.
    pub persistent_source_mrs: usize,
    pub opened_source_mrs: u64,
    pub reused_source_mrs: u64,
    /// Source parents retained by failed deregistration. Failed MW cleanup keeps
    /// its parent borrow charged in persistent_source_mrs without a dereg attempt.
    pub retained_source_mrs: usize,
    pub completed_reads: u64,
    pub completed_writes: u64,
    pub completed_read_bytes: u64,
    pub completed_write_bytes: u64,
    /// Reserved or possibly posted WR slots pending CQ validation or teardown.
    pub work_requests: usize,
    pub retained_work_requests: usize,
    pub peak_work_requests: usize,
    pub posted_work_requests: u64,
    pub post_calls: u64,
    pub partial_posts: u64,
    pub uncertain_posts: u64,
    pub completion_errors: u64,
}
