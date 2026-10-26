# Installed host RDMA qualification

This gate consumes a packaged `orbit-te` crate as a normal Rust dependency.
It runs the public `TransferEngine` API with the `rdma` feature only. Private
libtests, injected verbs tables and CUDA are absent from the installed binary.
The published alpha on crates.io remains a name reservation; this workflow
packages the selected source without publishing it.

## Build the artifact

Use the prerequisites in [the build guide](build.md), including the native
compiler/libclang environment. From a clean committed checkout, select an
artifact directory outside the repository:

```sh
python3 tests/hardware/build_installed.py \
  --toolchain stable --output /var/tmp/orbitte-installed/stable
python3 tests/hardware/build_installed.py \
  --toolchain 1.85.0 --output /var/tmp/orbitte-installed/msrv
```

The script creates a `.crate`, extracts it into the artifact directory and
copies its packaged `rdma_host` fixture into an independent consumer package.
Cargo resolves OrbitTE from that extracted package, preserves its locked
dependency versions, checks formatting/Clippy and installs a release executable
under `installed/bin/orbitte-rdma-host`. The manifest pins the source commit,
archive, fixture, consumer lockfile, binary, features and local native libraries.
The consumer is an acceptance fixture, not a new production CLI/API.

CI performs this build on stable and Rust 1.85.0 and retains the artifacts.
Running `--protocol` proves the binary starts; it is not hardware qualification.
`--allow-dirty` is for local build diagnosis only. The hardware controller
rejects a dirty or mismatched installation manifest.

## Run the two-host profile

Use the existing identity-pinned SSH profile and supervisor described in
[the native RDMA guide](rdma-cuda.md). Select `runner: "orbitte-installed-host"`,
two explicit `rails`, a `gid_index`, distinct source/requester nodes, and one
absolute resource lock per RNIC on each node. No GPU is requested. Each case
has these explicit fields:

```json
{
  "source": "a", "requester": "b",
  "source_kind": "cpu", "requester_kind": "cpu", "transport": "rdma",
  "mode": "installed-host", "operation": "read", "rdma_events": false,
  "parents": 16, "payload": 6291457, "block": 1048576,
  "num_lanes": 3, "mask": 3
}
```

```sh
python3 tests/hardware/run.py --profile /var/tmp/host-profile.json \
  --installation /var/tmp/orbitte-installed/stable/manifest.json \
  --binary /var/tmp/orbitte-installed/stable/installed/bin/orbitte-rdma-host \
  --output /var/tmp/orbitte-installed/native-stable
```

The fixture bounds each peer to 256 MiB of application backing, 32 parents,
32 MiB per parent and 1–3 QPs per peer/context/operation. Each of three rounds
starts concurrent public parent requests, varies every parent's input, checks
the complete allocation and guards, and waits for source-handler/grant drain.
Both contexts publish persistent MRs even with a one-rail dispatch mask.
Native completion-byte counters, bounded QP creation and increasing reuse
must agree with the workload; persistent MRs must not be recreated. Both peers
close, unregister and release all counted owners before reporting success.

TCP carries only rendezvous and test barriers. Payload requests explicitly
select `Transport::Rdma`; native byte counters and mapped providers must prove
that path. The controller independently checks the complete round/result
ledger, provider mappings, native-child exit and released resource locks.
Missing, duplicate, mismatched or partially drained results fail the run.

## TENT correspondence and limits

The fixture crosses TENT's engine/transport boundary: registration reaches
context-owned MRs, requests reach endpoint QP arrays and slices, and Workers
drive completion. It calls those owners through the public Rust engine; no
test-only route bypasses the production handshake or native resource lifecycle.
Frozen TENT CPU/geometry comparisons remain separate compatibility evidence.

The installed gate currently checks healthy host traffic, concurrency, reuse
and orderly close. It does not inject physical port/device faults, prove
per-rail bandwidth or fairness, or qualify InfiniBand/GPU transfers. Native
fault libtests remain necessary for internal ownership evidence. See
[qualification](qualification.md) for executed artifact/profile results and
[P4's final acceptance queue](completion-plan.md#p4-remaining-delivery) for the
remaining hardware and independent-review gates.
