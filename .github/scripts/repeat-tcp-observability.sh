#!/usr/bin/env bash
set -euo pipefail
export RUST_TEST_THREADS
RUST_TEST_THREADS=$(nproc)
test_binary() {
    cargo test --locked --lib --no-run --features "$1" --message-format=json |
        jq -ers 'map(select(.reason == "compiler-artifact" and .target.kind == ["lib"] and .profile.test and .executable != null)) | last.executable'
}
export ORBITE_TEST_CPU_BINARY
ORBITE_TEST_CPU_BINARY=$(test_binary '')
binary=$(test_binary "$ORBITE_FEATURES")
controls=(
    transport::hp_tcp::tests::rails::least_loaded_lane_bypasses_a_delayed_ack_and_releases_after_completion
    transport::hp_tcp::tests::rails::static_ipv4_pairing_balances_uneven_reads_rotates_small_requests_and_never_stripes_writes
    engine::tests::rejected_hp_submissions_leave_no_lane_load
    cuda::tests::peer_gpu_tcp_stages_both_directions_for_a_host_requester
    cuda::tests::requester_staging::requester_reuses_bounded_slots_at_negotiated_chunk_size_including_tails
    metrics::tests::accounting::exporter_health_survives_pause_and_failed_serialization
    metrics::tests::accounting::rejected_preflight_records_no_start_and_a_failed_prefix_settles_its_suffix
    metrics::tests::accounting::pairs_follow_the_start_and_only_emit_bounded_outcomes
    metrics::tests::recording::real_tcp_merged_owner_exposes_one_transfer_and_scrapes_both_formats
    metrics::tests::recording::busy_port_degrades_to_log_only_and_drop_joins_bounded_http_workers
    engine::failover::tests::native::metrics_attribute_each_retry_and_one_merged_outcome_to_the_real_transport
    runtime::admission_queue::tests::queued_cancellation_timeout_and_oversize_have_no_io_and_partial_cancel_keeps_prefix
)
for i in $(seq 1 50); do
    echo "Iteration $i/50 with $RUST_TEST_THREADS Linux threads"
    "$binary" --exact "${controls[@]}" > "$1" || { cat "$1"; exit 1; }
    grep -F "test result: ok. ${#controls[@]} passed; 0 failed;" "$1"
done
