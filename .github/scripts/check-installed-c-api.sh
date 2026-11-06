#!/usr/bin/env bash
set -euo pipefail
prefix=$1
# Cargo versions use different temporary paths for publish --dry-run. Export
# the verified sources with cargo package before testing an external install.
cargo package --locked --no-verify -p orbite --features "$ORBITE_FEATURES"
target=$(cargo metadata --locked --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
version=$(cargo metadata --locked --no-deps --format-version 1 | python3 -c 'import json,sys; print(next(p["version"] for p in json.load(sys.stdin)["packages"] if p["name"]=="orbite-c"))')
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
ffi="$work/orbite-c-$version"
tar -xzf "$target/package/orbite-$version.crate" -C "$work"
core="$work/orbite-$version"
test -f "$core/Cargo.toml"
patch=$(python3 -c 'import json,sys; print("patch.crates-io.orbite.path="+json.dumps(sys.argv[1]))' "$PWD")
cargo publish --dry-run --locked -p orbite-c --features "$ORBITE_FEATURES" --config "$patch"
cargo package --locked --no-verify -p orbite-c --features "$ORBITE_FEATURES" --config "$patch"
tar -xzf "$target/package/orbite-c-$version.crate" -C "$work"
python3 scripts/install-c-api.py --debug --cpp --toolchain "$RUSTUP_TOOLCHAIN" --features "$ORBITE_FEATURES" \
  --manifest-path "$ffi/Cargo.toml" --engine-source "$core" --prefix "$prefix"
python3 tests/c-api/run.py --prefix "$prefix" --features "$ORBITE_FEATURES"
python3 tests/cpp-api/run.py --prefix "$prefix"
if [[ ",$ORBITE_FEATURES," == *,rdma,* && ",$ORBITE_FEATURES," == *,cuda,* ]]; then
  cc -std=c11 -Wall -Wextra -Werror -I"$prefix/include" tests/hardware/c_abi.c \
    -L"$prefix/lib" -ltent -o "$work/c-abi-native"
fi
