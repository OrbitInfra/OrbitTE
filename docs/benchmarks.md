# Benchmarks and fault fixtures

The S26 tools measure verified CPU transfers and prepare the P10 lifetime
matrix. They change no transfer policy. Results describe the recorded host and
artifact; they are not hardware qualification or a general performance claim.

## CPU sweeps

[benches/run.py](https://github.com/OrbitInfra/OrbiTE/blob/main/benches/run.py)
runs the public Rust API and a test-only C++ caller of frozen Mooncake v0.3.14.
The C++ runtime is confined to the reference executable. CPU memcpy, ordinary
TCP and io_uring have matched rows; HP-TCP and SHM have OrbiTE-only rows. HP-TCP
uses `127.0.0.1` and `127.0.0.2` as two loopback rails.

The default workload is READ and WRITE at 4 KiB, 64 KiB, 1 MiB and 64 MiB, plus
batches of 8 and 64 four-KiB blocks. Contiguous memory-target blocks exercise
physical-owner merging. Gapped blocks use a stride of twice the block size and
must remain separate owners. File batches preserve one owner per public request
for independent CQE outcomes, including contiguous blocks; the runner checks
that mapping too. There is one caller, two configured workers per engine,
three warmup batches, 100 samples and three independent process trials. Trial
order alternates the two implementations. Both disable engine failover,
metrics, progress workers and runtime queues, and enable merging. TENT's
transport-local TCP retry defaults are pinned separately: three retries,
100 ms initial delay and a 2,000 ms cap. OrbiTE has no corresponding inner
TCP retry loop. These are healthy-path measurements, not retry benchmarks.
The manifest records
these settings, actual process thread counts, compiler versions, affinity,
source and binary hashes, filesystems and every command. Internal worker
arrangements differ: these are matched caller/worker settings, not identical
engine implementations. TENT's common and TE RPC client pools are each pinned
to the same worker count through their two explicit environment variables.

Each iteration changes the source pattern and resets the destination. Every
payload byte, gap and guard is checked after **every** transfer, including
warmup; corruption aborts without a result. The runner also corrupts a guard
in each implementation and checks rejection. It rejects missing checks,
wrong byte counts and incomplete/nonpositive samples. There is no unchecked
mode.

Timing covers batch allocation, submission, the same polling schedule (128
hot polls, then exponential sleeps capped at 200 microseconds), and batch free.
OrbiTE additionally checks local drain. Setup, registration, reset and full
verification are outside the interval. CPU time uses the whole process clock,
including both engines' threads during that interval. Throughput divides
verified payload bytes by the sum of measured batch times. p50 and nearest-rank
p99 are empirical per-batch latency, not per-block latency; 100 samples give
limited tail resolution. The CPU memcpy row is only a memory-copy control,
not an engine submission. Raw samples are retained so results can be recomputed.

File offsets, lengths and buffer addresses are four-KiB aligned. The runner
requires actual ext4 and tmpfs mounts and checks OrbiTE's descriptor route.
Both file profiles leave TCP installed for host registration (TENT does not
include IOURING in its memory-registration transport list), but requests carry
an explicit io_uring hint. TENT attempts O_DIRECT on both; on tmpfs the flag may be accepted without a
direct path. Each iteration initializes the file through buffered I/O before
the measured operation. These are warmed, page-cache/coherence-sensitive
measurements, **not cold SSD throughput**. Completion does not include fsync
and makes no durability claim. Maximum payload per batch is 64 MiB; the
allocation and guard extent is bounded separately.

On a Linux runner with the native dependencies from the
[CPU oracle](cpu-oracle.md), also install `liburing-dev` **before** building
TENT. Enable io_uring in the kernel and choose writable ext4 and tmpfs paths:

```bash
python3 tests/reference/run.py --fetch-reference --output "$OUT/reference"
python3 benches/run.py --reference "$OUT/reference" --output "$OUT/sweep" \
  --ext4 "$EXT4_DIR" --tmpfs /dev/shm --workers 2 --warmup 3 \
  --samples 100 --trials 3
```

Both output directories must be fresh and outside the checkout. The
`Frozen TENT CPU comparison` CI workflow runs this on stable and Rust 1.85 and
uploads `cpu-bench-*` artifacts. GPU, RDMA, GDS and EFA are emitted as
**not run (hardware)**, without estimated numbers. The scoped summary lives in
[qualification](qualification.md#cpu-benchmark-evidence).

## CPU fault matrix

[tests/faults/matrix.tsv](https://github.com/OrbitInfra/OrbiTE/blob/main/tests/faults/matrix.tsv)
is the executable fixture list: backend, fault, exact production-owner control
and expected charge/release state. It reuses the existing socket, file, mapping
and fallible-table controls. New TCP and HP-TCP cases kill a real requester
process after a partial WRITE. TCP leaves the destination untouched; HP-TCP
keeps the received prefix. Both retain the registration during unregister and
close, then verify all bytes and guards after physical socket drain.
The restart fixture keeps its reserved endpoint and bounds only setup-time
`AddrInUse` retries: concurrent process creation can inherit a listener until
exec closes it. Transfer deadlines, epoch rejection and drain assertions stay
unchanged.

The matrix covers peer death, timeout, cancellation, partial submission,
unregister/close during work, and restart authority. It distinguishes native
provider failures from injected completion faults. Fabric uses the real
`tcp;ofi_rxm` provider; its held/error CQ controls wrap that provider. io_uring
uses real files/rings. A missing kernel/provider fails, never silently skips.
SHM's synchronous memcpy has no native cancel or partial-post queue; queued
cancellation belongs to the common batch owner. Local files have no peer epoch:
path replacement is tested against the admitted inode. Those dispositions are
explicit, not counted as passes.

```bash
cargo test --locked --features io-uring,fabric --lib --no-run --message-format=json > "$OUT/build.json"
# BINARY is the compiler-artifact executable in build.json.
ORBITE_EXT4_DIR="$EXT4_DIR" ORBITE_TMPFS_DIR=/dev/shm \
  python3 tests/faults/run.py --binary "$BINARY" --output "$OUT/faults" --kill-repeats 50
```

CI executes the matrix on both toolchains with the full feature set. It repeats
each new process-kill control 50 times. Existing native fault/stability gates
remain in the matrix. This prepares P10; it does not close its installed
hardware acceptance matrix.

## Installed fault fixtures

The [verification queue](qualification.md#verification-queue) owns the exact
commands and hardware leases. Reuse the installed RDMA fault runner's
`installed-inflight-dma`, `installed-lost-completion`,
`installed-cleanup-failure` and restart modes: accepted posts remain charged
until matching retirement receipts, and failed cleanup retains the native
owner. The NVLink grouped requester-exit and graceful-close modes check
cooperative CUDA import/event lifetime and guarded bytes. Exporter crash and
physical device loss remain explicit limits, not simulated passes.

The additional `gds-p10` packaged consumer runs one real native READ while a
small, test-only cuFile observer withholds status receipts. `drain` checks a
wait timeout and concurrent unregister/file close, then releases real CQEs and
verifies the full payload and guards. `quarantine` withholds receipts past the
submission deadline plus destruction window and checks `Unquiesced`, retained
byte charges and GPU backing after every caller handle is dropped. It never
fabricates a completion or retries a
WRITE. The driver requires compatibility disabled, resource locks and matching
nvidia-fs READ-counter evidence. Both modes are written and compiled in CI,
**not run**. These are controlled receipt faults, not spontaneous device faults.
