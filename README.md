# OrbitTE

A Rust-native transfer engine for AI infrastructure.

OrbitTE is an independent OrbitInfra project using Mooncake TENT as its
capability baseline. It is intended to provide byte movement for OrbitKV and
other systems while leaving cache identity, recovery planning and replica
ownership with those consumers.

## Status

**Early development.** This initial prerelease establishes the repository and
Rust package. No transfer API or transport backend is implemented yet. The
package is not ready for application use; TENT API/wire compatibility and
performance have not been established.

## Implementation plan

The target is functional parity with the frozen latest Mooncake TENT baseline
for CPU DRAM and NVIDIA GPUs. Network and memory movement form the core;
local file/SSD movement through io_uring and cuFile/GDS is optional. Other
accelerator platforms are outside the implementation scope.

- [Completion plan](docs/completion-plan.md): delivery order, dependencies and
  acceptance gates; the project's only execution queue.
- [TENT parity contract](docs/tent-parity.md): reference revision, API inventory,
  runtime behavior, transport matrix and compatibility boundaries.

## Package identity

- Project: **OrbitTE**
- Repository: <https://github.com/OrbitInfra/OrbitTE>
- Cargo package: `orbit-te`
- Rust crate: `orbit_te`

## Development

```sh
cargo fmt --check
cargo check --locked
cargo publish --dry-run --locked
```

## License and provenance

Licensed under Apache-2.0. See [LICENSE](LICENSE).

[Mooncake TENT](https://github.com/OrbitInfra/Mooncake/tree/e2d6e8e0f2df113d84f9e39379f98c4b67b199ab/mooncake-transfer-engine/tent)
is the capability baseline. This bootstrap contains no Mooncake source code.
