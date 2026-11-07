#!/usr/bin/env bash
set -euo pipefail
out=$1
mkdir -p "$out"
# Cargo normalizes archive mtimes. Give same-version, different-commit packages
# fresh sources and artifacts even when a developer exports CARGO_TARGET_DIR.
mkdir "$out/crate" "$out/target"
target=$(cargo metadata --locked --format-version 1 --no-deps | jq -r .target_directory)
version=$(cargo metadata --locked --format-version 1 --no-deps | jq -r '.packages[0].version')
cargo package --locked --no-verify
tar -xf "$target/package/orbite-$version.crate" -C "$out/crate"
crate="$out/crate/orbite-$version"
cargo install --locked --offline --path "$crate" --target-dir "$out/target" --root "$out/install" --bin orbite
strace -f -qq -e trace=network,clone,clone3 -o "$out/syscalls" "$out/install/bin/orbite" diagnostics version --json > "$out/version.json"
test ! -s "$out/syscalls"
commit=$(git rev-parse HEAD)
jq -e --arg commit "$commit" --arg version "$version" '.command=="version" and .build.commit==$commit and .build.version==$version and .build.features.cuda==false and .checks[0].outcome=="pass"' "$out/version.json"
