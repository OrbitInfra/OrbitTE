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

[Mooncake TENT](https://github.com/OrbitInfra/Mooncake/tree/main/mooncake-transfer-engine/tent)
is the capability baseline. This bootstrap contains no Mooncake source code.
