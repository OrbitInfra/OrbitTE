# Frozen TENT acceptance inventory

S17–S23 use the tables in this directory as their contract. The pin and native
build options come from [contract.json](../contract.json). Paths in the
`source` column start at `mooncake-transfer-engine/tent/` in that revision.
OrbitTE paths start at the crate root. Each row defines a check. Qualification results are recorded separately. In particular, inventorying a provider does not run it.

The files are UTF-8, tab-separated, with a header and literal field text.
Identify a case by table and primary key, for example `config:rpc_server_port`
or `rpc:BootstrapRdma`. Join its `case` column to [cases.tsv](cases.tsv) for
the Rust owner, downstream step, trigger and evidence to retain. Reference test
rows carry those fields directly and give the GTest filter in their source.
Future steps should add their executable evidence to the existing row.

C declarations and semantics resolve to `include/tent/transfer_engine.h` and
`src/transfer_engine_c.cpp`. RPC numbers, dispatcher and attachment semantics
resolve to `include/tent/rpc/rpc.h`, `src/rpc/rpc.cpp` and
`src/runtime/control_plane.cpp`. Stage-specific mapped outcomes are test
targets even where the corresponding compatibility adapter is still planned.

| Surface | Contract | Acceptance |
| --- | --- | --- |
| C ABI | [33 declarations and call contracts](c-api.tsv), [Linux LP64 layouts](abi.lp64) | S19 compares input validation, allocation/free, results and guards. The C probe measures size, alignment, member offset/width and the public constants. |
| C++, Python and device plugins | [Binding cases](bindings.tsv) | S20–S22 use the same storage, submission and retirement contracts. The plugin table layout comes from the independent C probe. |
| Configuration | [Keys and effective absent values](config.tsv), [environment mappings](environment.tsv), [precedence vectors](precedence.json) | S17 compares the effective immutable snapshot, including nested policy/QoS/pool fields. The shipped example JSON supplies explicit values. The absent column comes from the consuming code. |
| Capabilities | [Transport values, directions, notification and cancellation](capabilities.tsv) | Test the six local-to-target direction flags after a successful native install. GDR requires the provider's positive GPU Direct check. EGM requires its explicit setting. Unspec authorizes selection and has no provider of its own. BufIo has no loader entry or public transport value. |
| Observability | [Metric families](metrics.tsv), [other observations](observations.tsv) | Compare type, labels, finite histogram boundaries and structured observations. Bind and runtime-enable gauges report actual state. Extensions with the `orbitte_` prefix are outside the frozen family set. |
| Metadata and wire | [Metadata fields and ownership](metadata.tsv), [logical RPC IDs and attachments](rpc.tsv), [native fixtures](../wire/) | S18 and S23 check required/optional fields, bytes and errors. S24/S25 can use the same provider bootstrap fixtures. |
| Test sources | [88 recursive C++ test source mappings](tests.tsv) | Includes nested vendor directories. A shim exercises ownership and serialization. The hardware trigger is a separate acceptance condition. Primitive-specific and excluded SDK cases have an explicit disposition. |

## Configuration order

TENT first reads an existing file named by `MC_TENT_CONF`, otherwise it parses
the value as inline JSON. A successful load replaces the root. Legacy variable
mappings are then applied in source order. `MC_FORCE_TCP` acts on presence.
`NCCL_IB_GID_INDEX` follows `MC_GID_INDEX`; string-list and boolean aliases
use dedicated parsers. The explicit-config constructor captures selected
identity and placement fields before loading, then restores them. A true
explicit `transports/force_tcp` reapplies its transport overrides. The
`constructor_precedence` column names the exceptions.

The environment table includes the shared and TE-specific client I/O thread
counts read by the common RPC pool. SDK-private and Store-only variables
belong to their respective consumers.

The metrics loader applies environment values to its defaults, then applies
each present metrics config leaf. A runtime-candidate lifecycle classification
does not imply a live-update handler. Nested QoS defaults inherit in
global, tenant and intent order. The absent column describes the upstream
parser, including fields OrbitTE rejects when it has no executor.

## Reproduce the checks

```sh
python3 tests/reference/run.py --fetch-reference --output "$NATIVE"
python3 tests/reference/inventory/check.py --source "$NATIVE/reference"
python3 tests/reference/wire/check.py --source "$NATIVE/reference" \
  --build "$NATIVE/tent-build" --output "$WIRE"
```

Use fresh external output directories and the Linux CPU oracle prerequisites.
The inventory checker compares the pinned public header, transport enums,
configuration reads, lifecycle paths, metrics, RPC IDs and GTest suites with
the tables. The wire checker uses the isolated reference build's compile
flags and YLT dependency. It emits observed JSON outside the worktree and
compares it with the checked-in bytes. A reference change requires reviewing
those differences before replacing fixtures.

The RPC corpus contains the raw header, serialized body and attachment for
both halves of a fresh connection. YLT carries a route key for
`CoroRpcAgent::process` in its 20-byte request header. The struct_pack body
contains TENT's logical function ID. The 16-byte reply header carries the
sequence, RPC error and separate body/attachment lengths. The captured Linux
profile uses little-endian integer fields. HP-TCP has its own network-order
framing. Numeric map keys in segment JSON are arrays of key/value pairs, and
the topology and several provider attributes are themselves encoded strings.

The reference peer uses actual TENT clients, codecs and dispatcher with
deterministic application replies. It replays the committed frames into the
native dispatcher. A second, registered TENT source accepts real TCP READ and
WRITE, checks both guards and rejects crossing the registered range. Only the
fixture's address field is relocated to that process's live allocation. Stage
addresses, GPU handles, RDMA QPNs and optional provider tokens are inert test
values. Mixed Rust/TENT peer execution belongs to S23.
