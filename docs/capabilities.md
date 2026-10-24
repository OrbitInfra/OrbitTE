# Capabilities

The matrix describes the current source checkout. The published
`0.1.0-alpha.1` crate remains the original package reservation. Qualification
applies to the specific code, binaries, devices and profiles in the maintained
contracts.

| Path or capability | Current source | Qualification boundary |
| --- | --- | --- |
| CPU DRAM over TCP | Implemented | Loopback, independent-process, byte, restart and lifetime controls |
| Owned/external host registration | Implemented | Duplicate references, aliases, batch admission and retained allocation owners |
| Async batches and notifications | Implemented | CPU ownership, cancellation, detached handle and receipt controls |
| Host DRAM over RDMA | Implemented | Scoped two-host IPv6 RoCEv2 native profiles |
| Persistent local/source MRs and eager multi-context publication | Implemented | Atomic visibility/rollback, context/capability isolation and retirement controls; native profiles are declared separately |
| Connected QP reuse and shared progress | Implemented | Fixed role/operation, one active grant, exact completion proof |
| Device/port/GID discovery | Implemented | CPU and scoped native evidence; complete independent native repeat remains open |
| Multiple contexts and request-level rail selection | Implemented | CPU dispatch controls and single-RNIC native regression; two-physical-RNIC dispatch remains unqualified |
| NVIDIA device/pinned-host allocation and synchronized staging | Implemented | Declared CUDA and host-staging profiles only |
| Single-request cross-rail slicing | Implemented | CPU READ/WRITE, concurrent workers, one source handler and failed-drain controls; physical multi-RNIC striping remains unqualified |
| Direct GPU registration / GPUDirect RDMA | Planned | Requires actual GPU/RNIC path and drain qualification |
| CUDA IPC / NVLink transfer | Planned | Separate from NVIDIA allocation support |
| Local SSD through io_uring / cuFile GDS | Planned, optional | Independent storage transports and native direct-path gates |
| TENT C/Python/C++ API and mixed-peer wire compatibility | Open | Separate API/ABI/wire gates; current protocol is OrbitTE-only |

## Provider selection

CPU/TCP is the default. `rdma` and `cuda` are independent Cargo features.
Enabling a build feature makes that provider's code available; runtime use
also requires its actual device, driver and resources.

`RdmaConfig::new("mlx5_0")` is an explicit device/port 1/GID 0 pin.
`RdmaConfig::default()` enables automatic discovery, with whitelist/blacklist
filters and strict optional pins. `device_mask` restricts dispatch using stable
topology NIC IDs. No mask match fails before creating memory authority.

## Scope of support

The implementation target is CPU DRAM and NVIDIA GPUs. Local file/SSD movement
is optional. Other accelerator platforms are outside the selected scope.

Implemented, qualified and independently accepted are different states.
See [qualification](qualification.md) for the latest declared evidence and
[the completion plan](completion-plan.md) for outstanding acceptance gates.
