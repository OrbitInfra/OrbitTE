# Python API

The `orbite` distribution exposes the Mooncake v0.3.14 TENT Python API through
OrbiTE's Rust owners. It targets CPython 3.10–3.14 on Linux x86_64 and aarch64.
The API is in early development. Wheels are built for review in CI; publishing
to PyPI is the maintainer's action.

## Build and install

```bash
python -m pip install 'maturin==1.10.2' 'patchelf==0.17.2.4'
python -m maturin build --release --locked --out /tmp/orbite-wheels
python -m pip install /tmp/orbite-wheels/*.whl
```

The default wheel contains the CPU transports. To build with native features,
pass `--features python,pyo3/extension-module,rdma,cuda,io-uring,gds,metrics,rdma-adaptive`
to maturin, after installing the [native prerequisites](build.md).
A compiled provider still needs its runtime library and hardware. Building a
combined wheel does not qualify any GPU or RDMA profile.

The wheel imports as `orbite`. Frozen Mooncake's wheel stages the extension at
`mooncake.tent`; its standalone build/tests also use top-level `tent`. OrbiTE
installs neither alias, so the distributions can coexist. A TENT application
can explicitly use `import orbite as tent`.

## Transfers

Construct with no arguments to apply `MC_TENT_CONF` and the legacy environment,
or pass a configuration file path. Both use the immutable
[TENT configuration loader](rust-api.md#loading-tent-configuration). No environment
variable is changed to construct an engine. External metadata types and
etcd/Redis/HTTP URLs raise `TentException` with the same endpoint/epoch migration
guidance as Rust and C; server URIs are not included in the diagnostic.

```python
import orbite as tent

source = tent.TransferEngine("source.json")
requester = tent.TransferEngine("requester.json")
source_bytes = bytearray(b"source bytes")
destination = bytearray(len(source_bytes))
source_address = source.register_buffer(source_bytes)
local_address = requester.register_buffer(destination)
peer = requester.open_segment(source.get_segment_name())

with requester.allocate_batch_guard(1) as batch:
    request = tent.Request(
        tent.READ, local_address, peer, source_address, len(destination),
        transport_hint=tent.TCP,
    )
    requester.submit_transfer(batch, [request])
    result = requester.wait(batch, timeout=5.0)
    assert result.state == tent.COMPLETED
    assert destination == source_bytes

requester.unregister_local_memory(local_address)
source.unregister_local_memory(source_address)
requester.close()
source.close()
```

For loopback, each file can contain:

```json
{
  "metadata_type": "p2p",
  "rpc_server_hostname": "127.0.0.1",
  "rpc_server_port": 0,
  "transports": {
    "tcp": {"enable": true},
    "hp_tcp": {"enable": false},
    "rdma": {"enable": false},
    "shm": {"enable": false}
  }
}
```

The frozen `open_segment(name)` signature pins the first observed epoch for
that handle, like the C/C++ adapters. Only trust a source endpoint supplied by
the consumer's authority. `close_segment(handle)` invalidates that handle;
it is not membership retirement or a drain receipt. See the
[discovery boundary](rust-api.md#discovery-boundary) for the Rust API that accepts
an authoritative epoch and retires an endpoint/epoch identity.

## Surface and defaults

All six enums and their values are exported both through their enum classes
and at module scope, alongside `LOCAL_SEGMENT_ID`, `kWildcardLocation` and the
three `PRIO_*` constants. The [binding inventory](https://github.com/OrbitInfra/OrbiTE/blob/main/tests/reference/inventory/python.tsv)
maps 128 surface rows, covering 132 declarations including overloads, to the
installed-wheel controls. Its source checker compares the frozen binding.

| Surface | Contract |
| --- | --- |
| `Notification` | Mutable `name`, `msg`; empty constructor or `(name, msg)` |
| `Request` | Mutable opcode, source, target ID/offset, length, priority, hint, policy name, deadline and intent. Priority defaults to high, hint/intent to unspecified, policy to `None`, deadline to zero. |
| `TransferStatus` | Mutable `state`, `bytes`; each task keeps its own result |
| `SegmentInfoBuffer`, `SegmentInfo` | Mutable base/length/location and type/buffer list |
| `MemoryOptions` | Location `"*"`, global read/write, unspecified transport, empty SHM path, zero SHM offset, `internal=False` |
| `MemoryGuard` | `address()`, `size()`, context entry returns address; exit requests a safe release |
| `BatchGuard` | `id()`, idempotent `release()`, context entry returns batch ID; exit delegates pending owners to deferred retirement |
| `TransferEngine` | Both constructors; introspection, segment, basic/advanced/bulk memory, batches, both notification submit overloads, cancellation, notifications and all status queries |

Data records also accept keyword field initializers. Policy, intent, priority,
transport hint and absolute monotonic `deadline_ns` reach the same compiled
selector and physical owners as Rust/C++. `export_local_segment` and
`import_remote_segment` raise `NotImplementedError`, as the frozen engine does.
Excluded transports retain their enum values and report unavailable.

Errors follow TENT's exception hierarchy under `TentException`, including
`InvalidArgumentError`, `AddressNotRegisteredError`, `DeviceNotFoundError`,
`InvalidEntryError`, `RdmaError`, `CudaError`, `MetadataError`, `RpcServiceError`,
`InternalError` and `NotImplementedError`. Resource exhaustion, malformed config,
and unknown effect use `InternalError`, matching TENT's default mapping. Messages
include the native status code. Rust panic barriers return `InternalError`.

## Buffer and guard lifetimes

An integer address from `allocate_local_memory` or a memory guard has an engine
owner. For foreign host storage, use `register_buffer(buffer)` or retain an
exporter explicitly:

```python
engine.register_local_memory(address, size, owner=writable_buffer)
engine.register_local_memory_batch(addresses, sizes, owners=buffers)
```

The `owner` argument is also available on `register_local_memory_ex`, and
`owners` on its batch counterpart. The exporter must be writable, C-contiguous
and nonempty, and must contain the entire requested range. A retained host
memoryview prevents resizing and follows the actual backing through aliases and
quarantine. Bare foreign addresses and read-only exporters are rejected. Unlike
frozen TENT's unchecked integer-pointer registration, dropping the caller's last
reference cannot free submitted storage. Do not read or mutate it concurrently
with native I/O.

### CUDA exporters

A CUDA-feature wheel accepts retained CUDA Array Interface v2/v3 or DLPack
exporters, including a PyTorch GPU KV-cache tensor:

```python
engine.register_local_memory(t.data_ptr(), t.nbytes, owner=t)
engine.register_local_memory_batch(
    [a.data_ptr(), b.data_ptr()], [a.nbytes, b.nbytes], owners=[a, b],
)
# Submit and drain using the same Request/BatchGuard API above.
engine.unregister_local_memory(t.data_ptr())
```

This is the migration from raw `tensor.data_ptr()` registration. Retainable
owners are mandatory for foreign addresses; engine-owned allocation addresses
keep their existing API. `register_buffer(exporter)` also accepts these CUDA
protocols and returns the checked base address. A CPU wheel raises a clear
`NotImplementedError` before requesting a GPU capsule.

The engine checks shape, byte extent, contiguity, writable access and the
requested subrange, then uses CUDA pointer attributes and allocation bounds to
verify the full export and actual device. A claimed device must match the driver.
The [CUDA Array Interface](https://nvidia.github.io/numba-cuda/user/cuda_array_interface.html)
read-only flag is honored. Masked, strided, empty and overflowing exports are
rejected. Its producer stream is captured as an event in the allocation's context;
the caller's exact context is restored. With v2 or `stream=None`, the producer
must already have made the exported bytes ready, as that protocol requires.

[DLPack](https://dmlc.github.io/dlpack/latest/python_spec.html) negotiates a
zero-copy export with `max_version=(1, 0)` and the device's legacy default stream;
older producers that lack that keyword use the legacy capsule protocol. The
engine consumes each capsule once and retains its managed tensor, deleter and
Python owner. Versioned read-only or copied exports, unsupported versions and
sub-byte dtypes are rejected. The managed tensor's deleter runs once under the
GIL only after its native owners release it. A producer must provide a valid
interchange capsule and keep the exported allocation and context stable.

Each retained export lease, including duplicate registrations, counts against
`max_registered_regions`; aliases cannot create an unbounded capsule list. Idle
staging buffers and idle private copy/producer events are released before
unregister acknowledges a foreign export, while its registration charge remains
held. Their context cache cannot indefinitely pin a retired tensor. Active copies
and uncertain cleanup still keep the exporter and charges. Unregister can report
resource exhaustion while another alias or admitted operation is draining; this
is not permission to reuse the memory. CPU objects that also implement DLPack
still use the writable buffer protocol. CPU tensor-only protocols are not added.

CUDA export controls use a CPU driver shim through the installed wheel. Python
GPU is **Implemented; installed profile unqualified** until the
{ref}`packaged GPU gate <queue-python-cuda>` runs.

Registrations and allocation charges outlive a busy memory guard. After
unregister obtains physical drain, deferred guard frees are reaped; engine close
also drains them. A pending batch guard delegates to the existing batch owner.
Neither a guard exit, cancellation, timeout nor terminal status is permission to
free externally managed memory. Failed native cleanup retains the ABI slot,
exporter, registrations and charges, and never retries an uncertain destroy.
Repeated explicit close returns the saved cleanup error.
At interpreter finalization, a slot that cannot safely enter Python is retained.

Every native call releases the GIL before acquiring engine locks, waiting or
performing I/O. Python argument conversion and buffer export happen while the
GIL is held. One batch's wait does not hold its lock while sleeping, so another
thread can query or cancel it.

Additive helpers are `progress_batch(batch)`, `wait(batch, timeout)`,
`terminal_result(batch, task_id=None)` and `close()`. `wait` requires both terminal
tasks and physical drain, without sealing the batch; a timeout retains ownership.
`terminal_result` returns `(native_status, effect_known)` and is observational
only, never a drain receipt or replay permission. Cancellation of already-posted
TCP work reports unavailable; queued cancellation prevents payload submission.

## Packaging and evidence

PyO3 `0.27.2` is a semver-compatible minimum in the optional `python` feature of
`orbite-c`; Cargo.lock fixes the tested dependency graph. The same Rust adapter
owners back the C, C++ and Python boundaries. The extension is installed as
`orbite._native`; the ordinary C build still installs `libtent` and does not
compile or load Python. Maturin uses a compatible `>=1.10.2,<2` build requirement;
CI fixes the build tool at 1.10.2.

Version-specific wheels preserve the buffer API on CPython 3.10. PyO3's
[stable ABI buffer support starts at 3.11](https://pyo3.rs/v0.27.2/building-and-distribution.html#missing-features),
so a single `abi3-py310` wheel would not provide the required ownership API.
The wheel matrix uses regular CPython builds, not free-threaded interpreters.

[Installed-wheel CI](qualification.md#python-wheels) covers both CPU and combined
feature wheels on both Rust toolchains and both Linux architectures. Tests run
with isolated imports outside the source package, move guarded bytes between
processes, hold real TCP replies for GIL/lifetime controls and rebuild a wheel
from the source distribution. These checks qualify only the declared CPU profile.
