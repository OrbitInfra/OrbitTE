# C API

OrbiTE provides the 33 C functions in Mooncake v0.3.14's
`<tent/transfer_engine.h>`. The `orbite-c` crate builds `libtent.so` and
`libtent.a`. Linux LP64 clients can use either library. The CPU/TCP
qualification uses separate source and requester processes on loopback.

For the native TENT C++ and classic TE source interfaces, see the [C++ API](cpp-api.md).

## Install and link

```bash
python3 scripts/install-c-api.py --prefix "$HOME/.local" --toolchain stable
export PKG_CONFIG_PATH="$HOME/.local/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
cc client.c $(pkg-config --cflags --libs tent) -Wl,-rpath,"$HOME/.local/lib" -o client
```

Pass `--features rdma,cuda` or the required Rust feature set at install time.
For static linkage, pass the installed `libtent.a` followed by the private
system libraries from `pkg-config --static --libs tent`. RDMA builds include
the verbs link dependency. CUDA and cuFile use the Rust providers' runtime
loading rules. Applications must build the library with panic unwinding enabled.

## Construct an engine

```c
#include <tent/transfer_engine.h>

tent_set_config("rpc_server_hostname", "127.0.0.1");
tent_set_config("transports/force_tcp", "true");
tent_engine_t engine = tent_create_engine();
if (!tent_available(engine)) {
    tent_destroy_engine(engine);
    return 1;
}
```

The two setters belong to the calling thread and persist across constructions.
`tent_set_config` accepts slash paths and textual scalar values. Repeating a
key replaces its value. The frozen `tent_load_config_from_file` implementation
stores its argument and parses it as **JSON text** at construction. Passing a
filesystem path here logs a parse warning and defaults the constructor
document. Set **`MC_TENT_CONF`** to read an actual configuration file or an
inline document. The C constructor applies the S17 replacement, legacy alias,
metrics and explicit-identity precedence rules. A failed construction can
return an opaque handle with `tent_available(engine) == 0`. Destroy it normally.
See [loading TENT configuration](rust-api.md#loading-tent-configuration).

Discovery and external metadata stores belong to the consumer. Non-P2P
`metadata_type` and etcd/Redis/HTTP(S) server URIs fail construction before
engine startup. `tent_available` returns zero and integer operations on the
unavailable handle return `-6` (TENT's `InvalidMetadataType`, with the fork's
negative-code convention). The log facade emits migration guidance to publish
endpoint and source epoch in the consumer's registry without logging the URI.
The classic adapter returns `ERR_METADATA`; native C++ status messages carry
the same guidance. These APIs install no logger. See the
[discovery boundary](rust-api.md#discovery-boundary).

## Register and transfer

The source registers a live allocation with `tent_register_memory` and passes
its P2P segment name and the permitted address range to its requester.
`tent_segment_name` returns the bound endpoint in P2P mode.
`tent_segment_name` and `tent_rpc_server_addr_port` follow `strncpy` semantics.
Supply enough space for the terminating NUL.

```c
tent_segment_id_t peer;
if (tent_open_segment(engine, &peer, source_name) != 0) return 1;
tent_segment_info_t info = {0};
if (tent_get_segment_info(engine, peer, &info) != 0) return 1;
/* Select the buffer that the source assigned to this transfer. */
if (!info.num_buffers || info.buffers[0].length < 64) return 1;
uint64_t source_address = info.buffers[0].base;
tent_free_segment_info(&info);

unsigned char destination[64] = {0};
if (tent_register_memory_with_perm(engine, destination, sizeof destination,
                                   PERM_LOCAL_READ_WRITE) != 0) return 1;
tent_batch_id_t batch = tent_allocate_batch(engine, 1);
if (!batch) return 1;
tent_request_t request = {0};
request.opcode = OPCODE_READ;
request.source = destination;
request.target_id = peer;
request.target_offset = source_address;
request.length = sizeof destination;
request.transport_hint = TRANSPORT_TCP;
if (tent_submit(engine, batch, &request, 1) != 0) return 1;
```

Poll `tent_task_status` or `tent_overall_status` with a bounded application
deadline and backoff. Check both the call's result and the returned transfer
state. Status-list and NIC-list calls copy at most the incoming `*count` and
return the total available count. Both require non-null output and count
pointers, including for a zero-capacity query. Match successful allocations
and registrations with their free and unregister calls.

A memory request uses an absolute remote address. A `file://` segment uses a
zero-based file offset. `LOCAL_SEGMENT_ID` is this engine. Remote P2P names
use `host:port` on a trusted network. Opening a name is local. Its first
metadata lookup pins the responding source's epoch, and every descriptor
refresh and payload request checks that identity. An epoch mismatch requires
explicit close and reopen. The 33-function API has no argument for an
out-of-band expected epoch. The Rust API accepts a source-issued epoch.
Closing a memory segment currently removes its handle only; it is not an
admission fence or drain receipt for the peer's native owners.

## Ownership and results

- `tent_allocate_memory[_ex]` owns storage. Registration is a separate call.
  `tent_free_memory` succeeds only after the allocation can retire. Borrowed C
  storage must stay allocated, initialized and accessible until every final
  unregister succeeds. Synchronize external CPU and CUDA producers and keep
  their contexts alive. A CPU-only build requires borrowed host-accessible
  storage. Concurrent raw access to an in-flight range is unsafe.
- Use zero-initialized options and requests. Permission 0 is local read/write,
  1 permits remote READ, and 2 permits both remote READ and WRITE. Local-only
  buffers remain visible in local information. Internal buffers are hidden from
  public segment information. Exact duplicate registrations count references.
  Overlapping borrowed ranges need a common retained backing.
- An explicit transport must be installed. A scoped registration constrains route
  selection. Known excluded and deferred transport constants return `-200`.
  Engine-owned SHM allocations use generated names. A supplied registration
  name or offset must match the actual mapping. Foreign path strings cannot
  create SHM authority. `cpu:N` requires a backing with verified NUMA placement.
  Use `*` for borrowed host storage whose node the owner cannot verify.
- A batch can accept successive submissions up to capacity. Each call validates
  the entire input, including mixed targets and opcodes, before publishing its
  first task. A notified submission delivers once per distinct memory target
  after all of its groups succeed. File targets reject attached notifications.
- A busy free or unregister can return `-2`. Poll and retry retirement.
  Closing admission, accepting a cancel or reaching an application deadline
  cannot release a live borrower. Uncertain native cleanup retains the owners.
  Explicitly unregister borrowed memory before destroying the engine to obtain
  a release receipt. The void destructor logs cleanup failures.
- `tent_get_segment_info` returns a C-allocated buffer array or NULL for an
  empty result. Call `tent_free_segment_info` once. That function preserves the
  pointer value in the caller's record. `tent_recv_notifs` returns C-allocated
  records with a zero handle and terminated name/message strings.
  `tent_free_notifs` releases and resets the pointer and count, so a second free
  is safe. Free a previous list before reusing its output struct.

### Observing terminal evidence

The OrbitInfra fork and OrbiTE add `tent_terminal_result(engine, batch, task,
&code, &known)` outside frozen v0.3.14. Pass `TENT_ALL_TASKS` for the whole
batch. The query observes cached task outcomes. It neither advances progress
nor certifies physical drain or permission to replay. A pending owner returns
`-2`. On a query error the outputs default to 135 and zero. A terminal query
returns zero and writes a positive native code (zero on success) and an effect
certainty bit. Cancellation, timeout and missing evidence yield 135 and zero.
Aliases observe their physical owner. Batch certainty requires every task to
be terminal and certain. Code 135 takes precedence over the first other error.
A successful sibling may already have changed its destination.

### Compatibility with native error codes

Integer operations return zero on success and the negative native status code
on error. This follows the OrbitInfra fork and the proposed upstream change.
Frozen v0.3.14 collapses native operation failures to `-1`. Existing callers
checking `rc != 0` work with both conventions. Callers checking only `rc == -1`
must change that test. [Mooncake #5255](https://github.com/kvcache-ai/Mooncake/pull/5255)

| Return | Meaning |
| --- | --- |
| `-1` | Invalid argument, handle, range, or permission |
| `-2` | Request, registration or byte budget exhausted, or an owner still busy |
| `-3` | Address or registration missing |
| `-5`, `-6`, `-7` | Invalid entry, metadata backend, or stale source epoch |
| `-100`, `-101` | Native verbs or CUDA failure |
| `-132`, `-133` | RPC/protocol failure or malformed configuration JSON |
| `-134` | Connection or delivery failed before dispatch could take effect |
| `-135` | Effect or physical retirement unknown |
| `-199`, `-200` | Internal failure (including a caught panic), or unavailable operation/provider |

`tent_available` is boolean. Batch allocation returns zero on exhaustion and
the native unsigned `-1` sentinel for a null engine. A successful status query
can report `STATUS_FAILED`. Inspect prior submissions separately before any
retry. An unknown WRITE effect must never be replayed.
