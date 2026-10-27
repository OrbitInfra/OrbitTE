# Capabilities

This matrix describes the current source. The published `0.1.0-alpha.1` crate
remains the original package reservation. **Implemented**, **qualified** and
**independently accepted** are separate states; see [Qualification](qualification.md)
for the exact artifacts and profiles behind each result.

| Path or capability | Current source | Qualification boundary |
| --- | --- | --- |
| CPU DRAM over TCP | Implemented | Loopback and independent-process byte, restart and lifetime controls |
| Owned/external host registration | Implemented | Transactional batches, duplicate references, allocation aliases and retained backing |
| Async batches and notifications | Implemented | CPU cancellation, detached handles, bounded admission and receipt controls |
| Host DRAM over RDMA | Implemented | Scoped two-host IPv4/IPv6 RoCEv2 READ/WRITE profiles; InfiniBand remains unqualified |
| Persistent host MRs and multi-context publication | Implemented | Scoped two-RNIC atomic publication, rollback and retirement; paused-context MR preparation and recovery pass the focused native PD/MR/QP gate |
| Reusable QP arrays and concurrent grants | Implemented | Fixed-width cold construction, public v14 multiplexing, exact WR/receipt dispatch and shared physical SQ/RQ limits |
| Multirail slices and shared quotas | Implemented | Selected folded-tail and concurrent-parent profiles verify bytes, whole-parent admission and native drain. The installed two-physical-rail profile joins every payload WR/CQE and verifies shared-QP contention/accounting; bandwidth/fairness policy remains P6 |
| Transport workers, CQ lanes and NUMA binding | Implemented | Scoped native worker/lane routing, CPU-mask and polling/event profiles; broader scheduling policy remains later work |
| Discovery and periodic recovery | Implemented | Scoped process-local query-failure recovery; initially paused contexts and automatic/strict GID recovery pass the focused one-RNIC native gate. Independent discovery, physical loss and acceptance remain open |
| Native failure retention and live reconnect | Implemented | Scoped QP ERR, lost CQEs, QP/MR/MW syscall faults and CQ/channel/PD EBUSY. Failed owners remain retained; device-close errors have CPU ABI coverage only |
| Packaged public host consumer | Scoped tests | Qualified profiles cover healthy traffic, restarts, lost CQEs, actual in-flight DMA interruption, QP/MR/MW/CQ/channel/PD cleanup, engine replacement and physical rail accounting. Physical link/device loss and InfiniBand remain open |
| NVIDIA allocation, pinned host memory and synchronized staging | Implemented | Declared CUDA/host-staging profiles only; device pointers use host staging |
| Direct GPU registration / GPUDirect RDMA | Planned | Requires actual GPU/RNIC transfer and drain qualification |
| CUDA IPC / NVLink | Planned | Separate from NVIDIA allocation support |
| Local SSD through io_uring / cuFile GDS | Planned, optional | Separate storage transports and direct-path qualification |
| TENT C/Python/C++ API and mixed-peer wire compatibility | Open | Separate API/ABI/wire gates; the current protocol is OrbitTE-only |

## Provider selection

CPU/TCP is the default. `rdma` and `cuda` are independent Cargo features.
Enabling a feature makes its provider code available; runtime use requires the
actual device, driver and resources.

`RdmaConfig::new("mlx5_0")` strictly pins that device, port 1 and GID index 0.
`RdmaConfig::default()` enables automatic discovery with optional strict pins
and whitelist/blacklist filters. `device_mask` limits dispatch by stable NIC
IDs. See [RDMA configuration](rdma-cuda.md#device-port-gid-and-host-topology-discovery)
for selection and resource ownership.

## Scope of support

The target is CPU DRAM and NVIDIA GPUs; local file/SSD movement is optional.
Other accelerators are outside the selected scope. Architectural correspondence
with frozen TENT does not establish full behavior or wire compatibility.

The [P4 exit queue](completion-plan.md#p4-remaining-delivery) contains the remaining
host-RDMA hardware and independent-review gates. Complete that boundary before
renewing OrbitKV integration; the other parity rows remain in their own stages.
