#!/usr/bin/env bash
set -euo pipefail
out=$1
mkdir -p "$out"
for i in $(seq 1 50); do
  python -I tests/python/test_installed.py \
    Contract.test_gil_release_during_blocking_wait \
    Contract.test_timeout_cancel_and_early_drop_retain_exporter_until_drain \
    Contract.test_memory_guard_early_drop_preserves_pending_destination \
    Contract.test_queued_cancel_status_and_destination_guards \
    > "$out/$i.log" 2>&1 || { cat "$out/$i.log"; exit 1; }
done
echo '4 installed-wheel timing controls x 50 iterations passed'
