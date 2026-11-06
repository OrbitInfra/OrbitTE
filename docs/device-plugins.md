# Device plugins

The optional Linux `device-plugin` feature in `orbite-c` loads native allocation
providers through TENT's frozen `device_plugin_t` table. It supports CPU and
NVIDIA CUDA. ROCm/HIP and the other excluded platforms return unavailable.
Native libraries are trusted code; the loader checks compatibility, not sandbox
safety. No plugin is loaded unless the caller explicitly requests it.

## Build and load

```bash
python3 scripts/install-c-api.py --prefix "$PREFIX" --features device-plugin,cuda
```

`libtent.so` built with both features also exports the CUDA plugin registration
entry point. It uses OrbiTE's fallible CUDA Driver API table, with no CUDA SDK,
C++ runtime or link-time CUDA dependency. To use another provider, pass its
shared library or a directory containing up to eight `.so` files:

```c
#include <tent/orbite_device_plugin.h>

tent_engine_t engine = tent_create_engine();
if (!tent_available(engine)) return 1;
/* Call before any allocation or registration on this engine. */
if (orbite_load_device_plugins(engine, "/opt/orbite/lib/libtent.so")) return 1;
void *gpu = NULL;
if (tent_allocate_memory(engine, &gpu, 4096, "cuda:0")) return 1;
if (tent_register_memory(engine, gpu, 4096)) return 1;
/* Submit through the ordinary batch API, then wait for physical drain. */
if (tent_unregister_memory(engine, gpu, 4096)) return 1;
if (tent_free_memory(engine, gpu)) return 1;
if (orbite_close_device_plugins(engine)) return 1;
tent_destroy_engine(engine);
```

Frozen v0.3.14's `MemoryProber::loadPlugins(path)` is called only by its CUDA
plugin context test. No production loader call, config key or environment
mapping selects it. `orbite_load_device_plugins` is therefore an explicit
OrbiTE extension; it does not reinterpret `MC_TENT_CONF` or invent a legacy
environment alias. The typed Rust constructor is unchanged. Libraries in a
directory load in sorted path order, with one provider per class. CPU plugin
allocations override the C allocator only after this explicit opt-in.

## Compatibility and failures

The installed `tent/device_plugin.h` is byte-identical to the frozen header:
`location_t`, `LOCATION_LEN`, `device_plugin_t`, `TENT_DEVICE_PLUGIN_ERROR`,
and `tent_register_device_plugin_fn` retain their declarations and layout.
The public transfer engine's 33 declarations are unchanged.

The frozen table contains no size or version handshake. OrbiTE requires this
additional data symbol **before** calling `tent_register_device_plugin`:

```c
#include <tent/orbite_device_plugin.h>
const orbite_device_plugin_descriptor_v1 orbite_device_plugin_abi_v1 =
    ORBITE_DEVICE_PLUGIN_DESCRIPTOR_V1;
```

The descriptor checks its revision, pointer width, record sizes, alignments
and every field offset. A legacy plugin without it is rejected. All function
slots must be present. Registration, context creation and bounded device/PCI
queries must succeed before allocation. Missing files or symbols, incompatible
layouts, partial tables and unsupported platforms return unavailable (`-200`).
This deliberately replaces TENT's skip-and-continue behavior with a checked
failure. Plugins must obey their C signatures, buffer bounds and synchronous
callback contract; they must not unwind across the ABI.

Each engine bounds its loaded libraries, allocation count and bytes. Registered
plugin allocations use the same backing and admission owners as other memory.
Aliases in other engines also retain them. Free returns busy (`-2`) until all
registrations and native work have drained. A failed allocation with uncertain
ownership, handled copy error, failed free or failed destroy returns
`Unquiesced` (`-135`). The memory, library and charges remain retained and the
failed cleanup callback is never retried. Destroying the engine does not turn
that error into a release receipt.

`orbite_plugin_memcpy_sync` copies between checked engine allocations before
registration or after unregister/drain. It refuses overlap and live aliases.
A handled plugin error cannot fall through to host `memcpy`; host fallback is
allowed only for two verified CPU allocations. CUDA copies restore the caller's
exact context, including an absent context. The CUDA table can query external CUDA metadata,
but device copies require allocations retained by its context; a foreign device
address returns the handled error before copying, so TENT cannot fall through
to a host copy. This is stricter than the frozen CUDA plugin. Raw table callers
must use `create_plugin` and honor the C buffer-lifetime contract.

## Evidence

`tests/device-plugin/run.py` compiles real CPU shared libraries and checks the
installed C ABI, guarded loopback TCP, aliases, paused transfer drain,
registration/ABI errors and sticky cleanup. `tests/device-plugin/cuda.py` drives
the installed CUDA plugin through a CPU Driver API shim. These are CPU controls.
The [installed CUDA plugin gate](qualification.md#queue-device-plugin) is written
but unrun: **Implemented; installed profile unqualified**.
