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
| Host DRAM over RDMA | Implemented | Scoped two-host IPv4/IPv6 RoCEv2 native profiles |
| Persistent local/source MRs and eager multi-context publication | Implemented | Atomic visibility/rollback and retirement controls, including actual two-RNIC publication and rollback in the declared host Direct profile |
| Transport-level RDMA Workers pool | Implemented | Scoped 1/2/3-worker two-RNIC host Direct tests verify CQ/thread routing, retained pool lifetime and fault recovery without new threads; 1/2/3/6 CQ lanes and load-based dispatch pass the 180-case native profile; broader TENT scheduling parity remains open |
| Peer endpoint QP arrays | Implemented | Demand-grown physical QPs; one active grant per QP. Fixed-width concurrent multiplexing remains open |
| Worker NUMA binding and unified event monitor | Implemented | Declared CPU DRAM/RoCEv2 profile verifies actual CPU masks and native get/ACK ownership; periodic port recovery remains open |
| Connected QP reuse and shared progress | Implemented | Fixed role/operation, one active grant, exact completion proof |
| Concurrent multirail parents and aggregate quotas | Implemented | Scoped eight-parent MiB/folded/exact-quarter-tail profiles and retained two-/four-parent cases; shared SQ/RQ pressure, 32/40 active slice grants and connected reuse and two-WR fault/drain. Wider concurrency/QoS remains open |
| Whole-parent source RQ admission | Implemented | Wire v12; residual-capacity rejection before QPs, caller retry, uncertain-post ownership and native receive-credit drain in declared four-/eight-parent profiles |
| RDMA endpoint destruction | Implemented | One independent shared 30 s parent destruction window; data failure is preserved and actual unproven teardown retains owners |
| Native fault and restart profile | Scoped tests | Actual QP ERR, dropped successful SQ/RQ CQEs, source/requester death and two/three fresh process generations over two RNICs; all earlier source epochs rejected before QPs. Physical port/device loss remains unqualified |
| Native CQ/PD cleanup failure | Scoped tests | Actual dependent-QP/MW EBUSY on either peer/rail, post-Drop owner retention, and drained-backing release for PD-only failures; spontaneous QP/MR/MW, completion-channel and device-close cleanup errors remain unqualified |
| Live-process RDMA recovery | Scoped tests | Two QP ERR/reconnect cycles preserve engine/PD/MR identity; failed MR/MW owners stay quarantined while same-process fresh engines use new epochs/PDs and reject old authority |
| MR/MW command failures | Scoped tests | Thread-local uverbs syscall injection produces actual EIO from libibverbs; backing/MW loans survive repeated close and observer-free Drop; spontaneous hardware failures and in-place healing remain unqualified |
| Device/port/GID discovery | Implemented | CPU and scoped native evidence; complete independent native repeat remains open |
| Multiple contexts and request-level rail selection | Implemented | CPU dispatch controls and scoped two-physical-RNIC native masks/routing; broader topology and fabric qualification remains open |
| NVIDIA device/pinned-host allocation and synchronized staging | Implemented | Declared CUDA and host-staging profiles only |
| Single-request cross-rail slicing | Implemented | CPU controls and scoped two-RNIC host Direct READ/WRITE, reuse, either-rail CQ/partial-post faults and drain; broader physical faults remain open |
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


See [the P4 exit queue](completion-plan.md#p4-remaining-delivery) for the two
remaining host-runtime gaps and final acceptance boundary. P4 acceptance alone
does not complete the full TENT core.
