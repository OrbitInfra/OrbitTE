#!/usr/bin/env bash
set -euo pipefail

binary=$1
iterations=50
export RUST_TEST_THREADS=${RUST_TEST_THREADS:-$(nproc)}
controls=(
  engine::failover::tests::foreground_and_worker_describe_a_same_port_restart_only_with_its_new_epoch
  transport::gds::tests::a_drained_file_read_can_switch_but_a_posted_write_and_a_short_read_cannot
  engine::failover::tests::missing_descriptor_refreshes_once_in_the_original_epoch_before_admission
  engine::failover::tests::a_refresh_cannot_rebind_an_admitted_write_to_a_replaced_registration
  engine::failover::tests::native::submit_rejection_walks_the_pinned_policy_once_per_physical_owner
  engine::failover::tests::native::polling_progress_wait_and_alias_cancel_decide_one_drained_attempt
  engine::failover::tests::native::partial_rdma_read_replays_only_after_the_real_close_receipt
  engine::failover::tests::native::ambiguous_write_and_unquiesced_read_never_start_a_fallback
  engine::failover::tests::native::tcp_faults_keep_a_successful_sibling_and_never_replay_a_lost_write_ack
  runtime::control_plane::tests::describe_retries_only_read_only_io_on_a_fresh_connection_with_the_same_epoch
  cuda::tests::remote_staging::rejected_staging_retries_the_same_policy_without_staging_and_keeps_gpu_guards
  transport::gds::tests::clean_file_submit_rejection_retries_each_owner_without_reissuing_its_neighbours
  batch::tests::one_progress_turn_dispatches_or_reaps_one_group_and_status_only_observes
  batch::tests::independent_batches_progress_while_a_sibling_progress_lock_is_held
  runtime::progress_worker::tests::worker_wakes_are_bounded_coalesced_and_stop_joins
  runtime::progress_worker::tests::fallback_tick_runs_only_while_the_queue_owns_work
  runtime::control_plane::tests::deferred_free_races_progress_and_allocation_until_real_tcp_drain
  runtime::control_plane::tests::free_fences_hook_retries_without_joining_a_peer_that_has_not_replied
  runtime::control_plane::tests::notify_ack_requires_a_registered_accepting_callback_and_well_formed_body
  runtime::qos_contract::tests::actions::public_qos_delay_preserves_batch_heads_and_actual_owner_release
  runtime::control_plane::tests::background_progress_publishes_a_lost_write_receipt_without_replay
  cuda::tests::staging::concurrent_first_stage_leases_charge_exactly_the_pool_capacity
  cuda::tests::staging_pipeline::abandoned_remote_delegate_releases_only_after_a_confirmed_event
  cuda::tests::staging_pipeline::source_destruction_joins_an_abandoned_delegate_before_freeing_the_pin
  transport::gds::tests::deferred_sweep_skips_permanent_destroy_and_reclaims_independent_work
  notification::tests::hook_self_delivers_while_the_remote_peer_withholds_its_receipt
  notification::tests::attached_targets_retry_only_missing_peers_and_keep_self_deliverable
  notification::tests::notification_routes_try_only_unposted_unavailable_channels
  notification::tests::notification_receive_keeps_draining_after_a_channel_fails
  runtime::receiver_credit::issuer::tests::lost_open_and_close_replies_preserve_identity_and_sync_cancel_is_retryable
  engine::tests::cancelling_a_running_rdma_alias_drains_posts_and_suppresses_the_remaining_slices
  engine::tests::direct_running_cancel_detach_and_close_keep_dma_lease_until_actual_worker_exit
  runtime::control_plane::tests::one_tcp_transfer_carries_shuffled_aliases_and_owns_exact_qos_and_credit
  runtime::control_plane::tests::shuffled_owners_keep_the_public_failure_prefix_and_do_not_replay_a_write
  runtime::request_merge::tests::shuffled_aliases_keep_absolute_public_ids_lengths_and_notification_intervals
  runtime::request_merge::tests::cancelling_a_queued_alias_uncharges_one_owner_without_touching_either_range
  runtime::admission_queue::tests::physical::physical_windows_charge_only_live_wire_bytes_and_use_each_transports_current_rate
  runtime::admission_queue::tests::physical::promotion_partitions_critical_future_slack_stably_across_admits
  runtime::admission_queue::tests::physical::same_submit_can_fill_the_window_and_an_exact_threshold_drop_frees_capacity
  transport::rdma::rdma_transport::tests::rails::admission_uses_the_selected_rail_rate_and_drops_aliases_before_any_native_post
  transport::rdma::rdma_transport::tests::rails::an_infeasible_merged_write_does_not_hide_a_later_feasible_tcp_owner
  cuda::tests::staging_pipeline::only_a_validated_proxy_session_can_take_internal_owner_and_byte_reserves
  cuda::tests::staging_credit::automatic_staging_credit_uses_negotiated_depth_and_only_committed_owners_finish_after_fence
  transport::rdma::rail_monitor::tests::concurrent_workers_share_exactly_one_trial_until_its_own_receipt
  transport::rdma::rdma_transport::tests::cold_connections::simultaneous_first_parents_wait_for_fixed_width_then_share_without_reopening
  cuda::tests::staging_cross::second_cross_finishes_on_shared_qp_while_first_payload_completion_is_held
  cuda::tests::staging_shards::eight_active_sessions_defer_pin_and_expired_ninth_does_not_block_on_native_work
  transport::rdma::rdma_transport::tests::rails::a_masked_pair_fault_keeps_the_other_nic_live_and_requires_one_recovery_receipt
  transport::rdma::rdma_transport::tests::rails::periodic_source_recovery_retires_every_stale_array_entry_before_single_or_sliced_payload
  transport::rdma::rdma_transport::tests::rails::initially_paused_rail_keeps_registration_without_blocking_its_healthy_sibling
  transport::rdma::rdma_transport::tests::rails::one_unquiesced_rail_retains_its_exact_loan_and_other_rail_cleanup_cannot_report_parent_drain
  transport::rdma::endpoint_store::tests::bounded_store_fences_an_external_handle_and_never_revives_its_slots
  transport::rdma::rail_monitor::tests::an_open_pair_admits_one_probe_and_only_its_receipt_can_reopen_it
  transport::rdma::rail_monitor::tests::half_open_failures_cap_at_five_minutes_and_prepost_abandonment_reopens_the_trial
)
if [[ ${2:-} == metrics ]]; then
  controls=(
    runtime::control_plane::tests::one_tcp_transfer_carries_shuffled_aliases_and_owns_exact_qos_and_credit
    metrics::tests::recording::concurrent_cells_keep_exact_transport_counts_and_inclusive_buckets
    metrics::tests::recording::real_tcp_merged_owner_exposes_one_transfer_and_scrapes_both_formats
    metrics::tests::recording::http_keeps_zero_histograms_for_tcp_and_unspec_in_both_formats
    metrics::tests::recording::busy_port_degrades_to_log_only_and_drop_joins_bounded_http_workers
    metrics::tests::recording::stalled_client_leaves_the_single_http_worker_and_long_interval_reporter_joinable
    engine::failover::tests::native::metrics_attribute_each_retry_and_one_merged_outcome_to_the_real_transport
    transport::io_uring::tests::truncation_reports_short_and_eof_per_cqe_with_exact_copyback
    transport::io_uring::tests::real_files_move_full_bytes_on_ext4_and_tmpfs
    transport::gds::tests::open_mode_selection_and_synchronous_file_bytes
    transport::gds::tests::deferred_sweep_skips_permanent_destroy_and_reclaims_independent_work
    cuda::tests::ipc_cache::cached_ipc_reuses_import_but_releases_completed_local_owners
    transport::rdma::rdma_transport::tests::rails::admission_uses_the_selected_rail_rate_and_drops_aliases_before_any_native_post
    transport::rdma::rdma_transport::tests::rails::a_masked_pair_fault_keeps_the_other_nic_live_and_requires_one_recovery_receipt
  )
fi
if [[ ${2:-} == quotas ]]; then
  controls=(
    transport::rdma::workers::tests::priority::event_worker_wakes_for_promotion_and_shared_slots_without_a_cqe
    transport::rdma::quota::allocation::tests::baseline_parents_rotate_first_tier_and_concurrent_reservations_balance
    transport::rdma::shared_quota::tests::independent_process_attach_and_killed_initializer_recovery
    transport::rdma::shared_quota::tests::killed_publisher_releases_the_shared_lock_without_stealing_a_live_nic_charge
    transport::rdma::shared_quota::tests::shared_slot_publication_is_visible_and_never_extends_a_stale_tier
    transport::rdma::shared_quota::tests::slot_attachment_propagates_native_failures_and_releases_initialization_lock
    transport::rdma::rdma_transport::tests::rails::weighted_parent_read_write_uses_exact_tail_bytes_and_compatible_remote_rails
    runtime::receiver_credit::tests::concurrent_manual_reservations_never_exceed_cumulative_grants
    runtime::receiver_credit::tests::credit_metrics_scrape_the_real_sender_and_receiver_owners
    runtime::control_plane::tests::background_progress_publishes_a_lost_write_receipt_without_replay
    engine::failover::tests::native::metrics_attribute_each_retry_and_one_merged_outcome_to_the_real_transport
  )
fi
if [[ ${2:-} == rail-recovery ]]; then
  iterations=200
  controls=(
    transport::rdma::rdma_transport::tests::rails::a_masked_pair_fault_keeps_the_other_nic_live_and_requires_one_recovery_receipt
    transport::rdma::cq_lane::tests::retirement::poll_retirement_keeps_native_owners_until_close_or_retention_receipt
    engine::failover::tests::native::partial_rdma_read_replays_only_after_the_real_close_receipt
    engine::failover::tests::native::ambiguous_write_and_unquiesced_read_never_start_a_fallback
  )
fi
if [[ ${2:-} == batch-release ]]; then
  iterations=200
  controls=(
    runtime::control_plane::tests::deferred_free_races_progress_and_allocation_until_real_tcp_drain
    batch::tests::a_progress_lookup_keeps_its_own_reservation_after_public_free
    runtime::progress_worker::tests::worker_wakes_are_bounded_coalesced_and_stop_joins
    batch::tests::independent_batches_progress_while_a_sibling_progress_lock_is_held
  )
fi
if [[ ${2:-} == metrics-port ]]; then
  iterations=200
  controls=(
    metrics::tests::recording::busy_port_degrades_to_log_only_and_drop_joins_bounded_http_workers
    metrics::tests::recording::stalled_client_leaves_the_single_http_worker_and_long_interval_reporter_joinable
    metrics::tests::recording::real_tcp_merged_owner_exposes_one_transfer_and_scrapes_both_formats
    metrics::tests::recording::http_keeps_zero_histograms_for_tcp_and_unspec_in_both_formats
  )
fi
for ((iteration=1; iteration<=iterations; iteration++)); do
  echo "Iteration $iteration/$iterations with RUST_TEST_THREADS=$RUST_TEST_THREADS"
  output=$("$binary" --exact "${controls[@]}") || { echo "$output"; exit 1; }
  echo "$output"
  grep -Fq "test result: ok. ${#controls[@]} passed; 0 failed;" <<<"$output"
done
echo "All ${#controls[@]} controls passed $iterations/$iterations on Linux with RUST_TEST_THREADS=$RUST_TEST_THREADS."
