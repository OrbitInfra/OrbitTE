#!/usr/bin/env bash
set -euo pipefail
prefix=$1
# Cargo versions use different temporary paths for publish --dry-run. Export
# the verified sources with cargo package before testing an external install.
cargo package --locked --no-verify -p orbit-te --features "$ORBITTE_FEATURES"
target=$(cargo metadata --locked --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
version=$(cargo metadata --locked --no-deps --format-version 1 | python3 -c 'import json,sys; print(next(p["version"] for p in json.load(sys.stdin)["packages"] if p["name"]=="orbit-te-c"))')
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
ffi="$work/orbit-te-c-$version"
tar -xzf "$target/package/orbit-te-$version.crate" -C "$work"
core="$work/orbit-te-$version"
test -f "$core/Cargo.toml"
patch=$(python3 -c 'import json,sys; print("patch.crates-io.orbit-te.path="+json.dumps(sys.argv[1]))' "$PWD")
cargo publish --dry-run --locked -p orbit-te-c --features "$ORBITTE_FEATURES" --config "$patch"
cargo package --locked --no-verify -p orbit-te-c --features "$ORBITTE_FEATURES" --config "$patch"
tar -xzf "$target/package/orbit-te-c-$version.crate" -C "$work"
python3 scripts/install-c-api.py --debug --toolchain "$RUSTUP_TOOLCHAIN" --features "$ORBITTE_FEATURES" \
  --manifest-path "$ffi/Cargo.toml" --engine-source "$core" --prefix "$prefix"
python3 tests/c-api/run.py --prefix "$prefix" --features "$ORBITTE_FEATURES"
