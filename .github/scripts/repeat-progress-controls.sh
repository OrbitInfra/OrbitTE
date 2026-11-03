#!/usr/bin/env bash
set -euo pipefail

binary=$1
export RUST_TEST_THREADS=${RUST_TEST_THREADS:-$(nproc)}
controls=(
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
)
for iteration in {1..50}; do
  echo "Iteration $iteration/50 with RUST_TEST_THREADS=$RUST_TEST_THREADS"
  output=$("$binary" --exact "${controls[@]}") || { echo "$output"; exit 1; }
  echo "$output"
  grep -Fq "test result: ok. ${#controls[@]} passed; 0 failed;" <<<"$output"
done
echo "All ${#controls[@]} controls passed 50/50 on Linux with RUST_TEST_THREADS=$RUST_TEST_THREADS."
