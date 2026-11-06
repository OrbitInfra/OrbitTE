# Offline diagnostics

Use the packaged `tent` command to inspect the build that produced a binary.
It reads compile-time metadata and works on a host with no CUDA driver, RNIC
or network access.

```bash
cargo install --locked --path /path/to/unpacked/orbite
tent diagnostics version
tent diagnostics version --json
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
`Snapshot` renders the same text and JSON as the command. Checks carry an
outcome, evidence and remediation. Unavailable evidence has
`"available": false` and omits the observed value. The snapshot redactor
removes credentials and raw addresses in every mode. Strict mode also replaces
known deployment identifiers and network addresses with correlated pseudonyms
within one snapshot.

The packaged `include/orbite/diagnostics.h` works with C and C++. Build the
diagnostics symbols with `cargo rustc --locked --lib --release --crate-type
staticlib`. The version, rendering and redaction functions lend UTF-8 bytes
to a synchronous caller callback. Document input is limited to 1 MiB.
The callback returns zero when it has consumed the output.

| Exit status | Meaning |
| --- | --- |
| 0 | All required checks passed |
| 1 | A check failed, or a warning was elevated by the library caller |
| 2 | Invalid command, option or native argument |
| 3 | A required collector failed |
| 4 | An internal check or output operation failed |

The version command runs one offline check. Collector and strict-warning
statuses are available to library callers supplying their own checks.
