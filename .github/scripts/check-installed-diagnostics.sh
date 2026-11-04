#!/usr/bin/env bash
set -euo pipefail
out=$1
mkdir -p "$out/crate"
target=$(cargo metadata --locked --format-version 1 --no-deps | jq -r .target_directory)
version=$(cargo metadata --locked --format-version 1 --no-deps | jq -r '.packages[0].version')
cargo package --locked --no-verify
tar -xf "$target/package/orbit-te-$version.crate" -C "$out/crate"
crate="$out/crate/orbit-te-$version"
cargo install --locked --offline --path "$crate" --root "$out/install" --bin tent
strace -f -qq -e trace=network,clone,clone3 -o "$out/syscalls" "$out/install/bin/tent" diagnostics version --json > "$out/version.json"
test ! -s "$out/syscalls"
commit=$(git rev-parse HEAD)
jq -e --arg commit "$commit" --arg version "$version" '.command=="version" and .build.commit==$commit and .build.version==$version and .build.features.cuda==false and .checks[0].outcome=="pass"' "$out/version.json"
# A C caller and a C++ caller can consume the same packaged diagnostics header.
cargo rustc --locked --offline --manifest-path "$crate/Cargo.toml" --lib --release --crate-type staticlib --target-dir "$out/target"
for compiler in cc c++; do
  "$compiler" -I"$crate/include" "$crate/tests/reference/diagnostics.c" "$out/target/release/liborbit_te.a" -ldl -lm -lpthread -o "$out/client"
  "$out/client" > "$out/client.json"
  jq -e --slurp '.[0] == .[1]' "$out/version.json" "$out/client.json"
done
