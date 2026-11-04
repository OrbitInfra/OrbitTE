#!/usr/bin/env bash
set -euo pipefail
binary=$1
output=$2
mkdir -p "$output"
export RUST_TEST_THREADS
RUST_TEST_THREADS=$(nproc)
filters=(r1c_ pipeline_failed_query_is_sticky_but_every_other_slot_is_drained
  abandoned_remote_delegate_releases_only_after_a_confirmed_event
  failed_second_cross_drains_first_or_retains_every_uncertain_owner
  failed_new_publication_preserves_published_mrs_lookup_and_reference_counts_on_every_context
  capability_fanout_rolls_back_as_one_publication_and_failed_cleanup_keeps_unpublished_backing
  final_unregister_retirement_respects_duplicate_references_and_already_resolved_work
  worker_wakes_are_bounded_coalesced_and_stop_joins fallback_tick_runs_only_while_the_queue_owns_work)

count=$("$binary" "${filters[@]}" --list | awk '/: test$/ {n++} END {print n+0}')
test "$count" -ge 19
for i in $(seq 1 50); do
  "$binary" "${filters[@]}" > "$output/controls-$i.log" || { cat "$output/controls-$i.log"; exit 1; }
  grep -F "test result: ok. $count passed; 0 failed;" "$output/controls-$i.log"
done

# Each process owns a distinct tmpfile and io_uring. Keep the actual destruction
# window in the test and overlap at most ten sleeping cases.
for group in 0 1 2 3 4; do
  pids=()
  for slot in $(seq 1 10); do
    i=$((group * 10 + slot))
    ( "$binary" a_missing_cqe_at_the_drain_deadline_quarantines_the_submitted_span > "$output/window-$i.log" &&
      grep -F 'test result: ok. 1 passed; 0 failed;' "$output/window-$i.log" ) &
    pids+=("$!")
  done
  for pid in "${pids[@]}"; do wait "$pid"; done
done
