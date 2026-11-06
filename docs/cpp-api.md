# C++ API

OrbitTE installs source adapters for `mooncake::tent::TransferEngine` and
the TENT-backed classic `mooncake::TransferEngine`. Both use the same Rust
engine, registrations, workers and batch receipts as the [C API](c-api.md).
Recompile clients against the installed headers. Classic and native TENT
have different request, status and error types.

## Install

On Linux LP64, install a C++20 compiler, `ar` and the nlohmann JSON headers
(`nlohmann-json3-dev` on Ubuntu). The JSON header supports the public C++
`Config` type and topology values. The `--cpp` option builds the separate
`libtent_cpp.a` source adapter. The C library can still be installed on its
own.

```bash
python3 scripts/install-c-api.py --cpp --prefix "$PREFIX" --toolchain stable
export PKG_CONFIG_PATH="$PREFIX/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
c++ client.cpp $(pkg-config --cflags --libs tent-cpp) -pthread -Wl,-rpath,"$PREFIX/lib" -o client
```

Use `#include <tent/transfer_engine.h>` for the native API and
`#include <transfer_engine.h>` for classic TE. The adapters link to
`libtent`. The original Mooncake C++ engine and the classic legacy backend
are absent from the link.

## Native transfers

The default constructor takes the TENT environment snapshot. The path
constructor reads an actual JSON file and yields an unavailable engine if the
file cannot be loaded. The `std::shared_ptr<Config>` overload snapshots the
supplied values. All three use S17 precedence and leave the C API's
thread-local constructor settings alone.

```cpp
#include <tent/transfer_engine.h>
#include <tent/common/config.h>
#include <chrono>

namespace t = mooncake::tent;

t::Status read_block(t::TransferEngine& engine, t::SegmentID peer,
                     uint64_t address, void* registered_destination, size_t length) {
    t::Request r{};
    r.opcode = t::Request::READ;
    r.source = registered_destination;
    r.target_id = peer;
    r.target_offset = address;
    r.length = length;
    r.intent_type = t::IntentType::BACKGROUND_PREFETCH;
    r.deadline_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        (std::chrono::steady_clock::now() + std::chrono::seconds(2))
            .time_since_epoch()).count();
    return engine.transferSync({r});
}
```

A request can bind a compiled selector rule by setting `r.policy_name`.
The adapter passes the name, all seven intent values, and the absolute
monotonic deadline across the versioned `orbitte_submit_v1` boundary. The
frozen C request layout is unchanged. A submit validates all records before
publication. `progressBatch` advances one bounded owner step. Scalar, list
and overall `getTransferStatus` overloads retain their ordinary poll
semantics. `waitTransferCompletion` takes over the batch and frees it after
physical drain. `transferSync` allocates, submits, waits and retires the batch.
Both helpers use capped backoff and are also available on
`mooncake::tent::TransferEngineImpl` through
`<tent/runtime/transfer_engine_impl.h>`. Inspect terminal evidence separately when
deciding what to do with a failed logical operation.

Both peers must run the same OrbitTE build. The first metadata lookup binds a
P2P handle to a source epoch. `probePeerAliveByID` checks the live peer at
that epoch. Topology queries snapshot the registered RDMA runtime and its
ranked NICs. Before verbs initialization they report verified host NUMA nodes
and, in CUDA builds, enumerate visible devices without creating a context.
Editing a returned topology snapshot affects only that copy. Unknown placement stays unknown. C++ notifications use length-delimited UTF-8
and preserve embedded NULs and strings larger than the C output arrays.
Native segment export and import return `NotImplemented`, as in v0.3.14.

## Classic TE

Construct `mooncake::TransferEngine`, call `init(P2PHANDSHAKE, name)` and
check its integer result. `isUsingTent()` is always true. `getTentEngine()`
returns a shared native adapter that remains usable while retained. Use the
classic integer `ERR_*` codes for registration, notification and segment
operations. Batch operations return `mooncake::Status`.
`installTransport` supplies the frozen compatibility handle. Use the engine
for payloads. Direct calls on that handle, multi-protocol registration and
submission, and classic shared-memory allocation retain their unsupported
results. An application can explicitly enable the bounded SIGINT/SIGTERM
watcher with `enableGracefulShutdown`.

`submitScatter` accepts ordered fragment offsets and lengths.
`waitFor` reports a clock error on expiry and leaves the operation live.
Retain the operation and its buffers until a later wait finishes. Moving the
operation or closing its classic facade preserves the executing owner.
`transferScatter` waits synchronously. A valid zero-length fragment completes
in process. A nonempty fragment requires a direct HP-TCP or RDMA route with
post-submit cancellation. The route is pinned, skips runtime-queue dispatch
and retains the normal credit, QoS and provider budgets. Ordinary TCP and
staged GPU routes reject before posting. A direct GPU source advertises its
registration policy under Describe. Each callback runs once after its fragment
has a physical receipt. Uncertain cleanup keeps the batch and callback-owned
storage charged and returns a deferred-cleanup error.

The [qualification ledger](qualification.md#c-source-adapters) separates
installed Linux loopback evidence from the queued RDMA and GPU Direct gate.
