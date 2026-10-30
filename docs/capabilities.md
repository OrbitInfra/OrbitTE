# Capabilities

This matrix describes the current source. The published `0.1.0-alpha.1` crate
remains the original package reservation. **Implemented**, **qualified** and
**independently accepted** are separate states; see [Qualification](qualification.md)
for the exact artifacts and profiles behind each result.

| Path or capability | Current source | Qualification boundary |
| --- | --- | --- |
| CPU DRAM over TCP | Implemented | Loopback and independent-process byte, restart and lifetime controls. Shared TcpTask owns partial framing and payload snapshots; ordinary active-batch cancellation remains unchanged |
| Owned/external host registration | Implemented | Transactional batches, duplicate references, allocation aliases and retained backing |
| Async batches and notifications | Implemented | CPU cancellation, detached handles, bounded admission and receipt controls |
| Host DRAM over RDMA | Implemented | Scoped two-host IPv4/IPv6 RoCEv2 READ/WRITE profiles; InfiniBand remains unqualified |
| Persistent host MRs and multi-context publication | Implemented | Scoped two-RNIC atomic publication, rollback and retirement; paused-context MR preparation and recovery pass the focused native PD/MR/QP gate |
| Reusable QP arrays and concurrent grants | Implemented | Fixed-width cold construction, public v20 multiplexing, exact WR/receipt dispatch and shared physical SQ/RQ limits |
| Multirail slices and shared quotas | Implemented | Selected folded-tail and concurrent-parent profiles verify bytes, whole-parent admission and native drain. The installed two-physical-rail profile joins every payload WR/CQE and verifies shared-QP contention/accounting; bandwidth/fairness policy remains P6 |
| Transport workers, CQ lanes and NUMA binding | Implemented | Scoped native worker/lane routing, CPU-mask and polling/event profiles; broader scheduling policy remains later work |
| Discovery and periodic recovery | Implemented | Scoped process-local query-failure recovery; initially paused contexts and automatic/strict GID recovery pass the focused one-RNIC native gate. Installed two-host automatic discovery matches a separate sysfs inventory, including strict-pin rejection and NUMA ranks. Independent review includes four fresh live RoCE repeats; physical loss remains open |
| Native failure retention and live reconnect | Implemented | Scoped QP ERR, lost CQEs, QP/MR/MW syscall faults and CQ/channel/PD EBUSY. Failed owners remain retained; device-close errors have CPU ABI coverage only |
| Packaged public host consumer | Scoped tests | Qualified profiles cover healthy traffic, restarts, lost CQEs, actual in-flight DMA interruption, QP/MR/MW/CQ/channel/PD cleanup, engine replacement, physical rail accounting and automatic discovery. Physical link/device loss and InfiniBand remain open |
| NVIDIA allocation, pinned host memory and copy streams/events | Implemented | Serialized stream/event reuse passes the bounded two-host/four-H20 local-copy profile on stable and Rust 1.85. Failure retention has CPU controls. Staging is the default device policy |
| Direct GPU registration / GPUDirect RDMA | Implemented; bounded H20/RoCE profile accepted with limits | Both installed toolchains pass host/GPU/GPU READ/WRITE, tiny and folded two-rail slices, real GPU MR/WR witnesses and owned-context drain. B host-wide context non-growth remains unqualified. Explicit Direct requires scatter-to-CQE disabled; GPU source MRs/QPs are per-grant |
| Cooperative CUDA IPC / NVLink | Bounded GPU/GPU, subbatch and pinned host profiles accepted | Owned GPU/GPU and local owned pinned host/GPU; whole-allocation opt-in and quota, event completion and explicit import-close receipts. The initial path passes 48 installed H20/NV18 cases plus four independent repeats on stable/MSRV. Same-context subbatch reuse passes 28 additional installed cases and four independent repeats. Pinned host and mixed groups pass 30 additional installed cases plus four independent repeats; exporter-crash safety and broader TENT IPC parity remain open |
| Cross-submission CUDA IPC import cache | Bounded fixed-working-set profile accepted | Explicit bounded cache, fresh source authorization per submission, single-flight imports and separate exporter lifetime. Fixed working set until requester shutdown; no eviction or forced exporter-crash safety |
| Local CUDA staging chunks | Bounded healthy profile accepted; P5.5a | Shared TCP/RDMA chunk owner; 24 installed H20 cases and four independent repeats verify bounded pins, copies and frees. CPU controls cover uncertain completion/cleanup retention. Opt-in serial remote sessions are implemented separately; see [staging](staging.md) |
| Host staging location | Implemented; source validation in progress | Opt-in Auto / explicit CPU NUMA location, mapped CUDA-registered backing and context/location pool reuse. Page padding counts toward the global bound. Hardware residency, DMA and performance remain unqualified; see [location contract](staging.md#host-staging-location) |
| Remote CUDA staging | Healthy serial and bounded pipeline profiles accepted | Serial Pin/Delegate/Unpin passes 16 installed cases plus four independent repeats. The up-to-16-slot pipeline passes 24 fresh installed H20 TCP/RDMA cases plus four independent repeats. Native stream/event lifetimes, per-slot reuse and private MR cleanup are checked. Requester chunk-size negotiation and GPU range loans pass independent source review. Independent requester PRE/POST rings and failure ownership pass source review at `c87c403`. Bounded CROSS queues separate local CUDA progress from network/Delegate waits. Up to eight shared staging shards reuse threads, bound queued sessions and cancel them before Pin. Requester chunk progress queries CUDA events and refills slots independently; RDMA CROSS admission stays ordered on one connection while payload completion can reorder. Separate CROSS quotas, scoped FinishCross and aggregate drain preserve slot ownership. TCP payload and scoped staging control receipts now have bounded nonblocking progress. Connect owns one pending socket; worker stop reaches Pin and native handshake socket waits, with complete Pin validation before chunk publication and native drain before retirement. Requester RDMA finish now polls SEND and reusable-grant cleanup and retains partial ACK/READY frames while querying other submitted CROSS owners. Requester RDMA setup retains partial path/admission/OFFER/GRANT frames, observes other submitted CROSS payloads and validates every parent grant before payload submission. Pin, cold connection acquisition, native resource creation, nonreusable QP retirement, copyback and source handlers remain synchronous. Independent source review accepts TCP/scoped-control progress at `736a16a` and cancellable Connect/Pin plus native socket waits at `a2d1b9e`. Independent source review accepts the v20 CROSS boundary at `adc803f`. The v21 source adds scoped Delegate completion queries, including depth one, and defers remote slot reuse/Unpin while copies remain pending; independent source review accepts this boundary at `3767dc5`. Hardware qualification remains pending. Full ProxyManager parity remains open |
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
host-RDMA physical/fabric gates. Source and the bounded RoCE profile are
independently accepted with fresh live repeats; P5 proceeds with available hardware.
Complete the host-RDMA acceptance boundary before
renewing OrbitKV integration; the other parity rows remain in their own stages.

Optional `nvlink_cache_entries` adds transport-owned cross-submission CUDA IPC
reuse. Source/CPU controls, 34 installed H20 cases and four independent repeats
pass for the declared fixed-working-set profile.
This fixed working-set profile keeps the whole source allocation loan until
requester shutdown. See [CUDA IPC](cuda-ipc.md) for lifecycle and open limits.
