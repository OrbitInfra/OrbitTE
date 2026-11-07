# Offline diagnostics

Use the packaged `orbite` command to inspect the build that produced a binary.
It reads compile-time metadata and works on a host with no CUDA driver, RNIC
or network access.

```bash
cargo install --locked --path /path/to/unpacked/orbite
orbite diagnostics version
orbite diagnostics version --json
```

Both formats report the crate version, source commit, build profile, Rust
compiler and compiled features. The commit comes from the checkout or Cargo’s
packaged VCS metadata. Source archives without either report `unknown`.
When comparing two unreleased packages with the same crate version, use a
fresh Cargo target directory for each extraction. Cargo can reuse binaries
from the earlier package because archive file timestamps are normalized.
Feature flags describe what the binary contains. Installed devices and network
reachability require separate qualification.

## Library interfaces

Rust consumers can call `orbite::diagnostics::version_snapshot()`. A
`Snapshot` renders the same compile-time text and JSON as the command. It
contains build facts and the version check only; it never collects addresses,
credentials or live engine state. External-document rendering and redaction
have no CLI or Python consumer and are removed, along with the C/C++ header.

| Exit status | Meaning |
| --- | --- |
| 0 | All required checks passed |
| 1 | A check failed, or a warning was elevated by the library caller |
| 2 | Invalid command, option |
| 3 | A required collector failed |
| 4 | An internal check or output operation failed |

The version command runs one offline check. Collector and strict-warning
statuses are available to library callers supplying their own checks.
