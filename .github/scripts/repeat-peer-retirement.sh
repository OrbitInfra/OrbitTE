#!/usr/bin/env bash
set -euo pipefail
binary=$1
out=$2
mkdir -p "$out"
count=$("$binary" peer_retirement --list | awk '/: test$/ {n++} END {print n+0}')
test "$count" -ge 17
for i in $(seq 1 50); do
  RUST_TEST_THREADS=$(nproc) "$binary" peer_retirement > "$out/$i.log" 2>&1 || { cat "$out/$i.log"; exit 1; }
  grep -F "test result: ok. $count passed; 0 failed;" "$out/$i.log"
done
