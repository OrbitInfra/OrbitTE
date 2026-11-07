#!/usr/bin/env bash
set -euo pipefail
binary=$1
out=$2
mkdir -p "$out"
export FI_PROVIDER=tcp ORBITE_REQUIRE_FABRIC=1
count=$("$binary" transport::fabric:: --list | awk '/: test$/ {n++} END {print n+0}')
test "$count" -ge 27
for i in $(seq 1 50); do
  RUST_TEST_THREADS=$(nproc) "$binary" transport::fabric:: > "$out/$i.log" 2>&1 || { cat "$out/$i.log"; exit 1; }
  grep -F "test result: ok. $((count-1)) passed; 0 failed; 1 ignored;" "$out/$i.log"
done
