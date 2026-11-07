"""Matched, verified CPU measurements. Evidence MUST be outside the checkout."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import statistics
import subprocess

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = json.loads((ROOT / "tests/reference/contract.json").read_text())


def summarize(row, count, payload):
    if row.get("verified") is not True or row.get("payload_bytes") != payload:
        raise ValueError("unverified or wrong-payload result")
    for key in ("samples_ns", "cpu_ns"):
        values = row[key]
        if len(values) != count or any(type(n) is not int or n <= 0 for n in values):
            raise ValueError("incomplete or invalid measurements")
    latency = sorted(row["samples_ns"])
    return {"GiB_s": payload * count / sum(latency) * 1e9 / 2**30,
            "p50_us": statistics.median(latency) / 1000,
            "p99_us": latency[math.ceil(count * .99) - 1] / 1000,
            "cpu_us_per_batch": statistics.mean(row["cpu_ns"]) / 1000}


def controls():
    valid = {"verified": True, "payload_bytes": 4096, "samples_ns": [10, 20], "cpu_ns": [10, 20]}
    summarize(valid, 2, 4096)
    for mutation in ({"verified": False}, {"payload_bytes": 1}, {"samples_ns": [10]},
                     {"cpu_ns": [0, 1]}, {"samples_ns": [float("nan"), 1]}):
        try:
            summarize(valid | mutation, 2, 4096)
        except ValueError:
            continue
        raise AssertionError("reporter accepted a broken measurement")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--reference", type=Path, required=True, help="successful tests/reference/run.py output, built with liburing-dev installed")
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--ext4", type=Path, required=True)
    p.add_argument("--tmpfs", type=Path, default=Path("/dev/shm"))
    p.add_argument("--samples", type=int, default=100)
    p.add_argument("--warmup", type=int, default=3)
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--sizes", type=int, nargs="+", default=[4096, 65536, 1048576, 67108864])
    p.add_argument("--batches", type=int, nargs="+", default=[1, 8, 64])
    p.add_argument("--trials", type=int, default=3)
    a = p.parse_args()
    out, ref = a.output.resolve(), a.reference.resolve()
    if out.is_relative_to(ROOT) or out.is_relative_to(ref) or platform.system() != "Linux":
        p.error("Linux only; output must be outside both source trees")
    if not (1 <= a.samples <= 10000 and 1 <= a.warmup <= 100 and 1 <= a.workers <= 16 and 1 <= a.trials <= 9):
        p.error("invalid sample/worker bounds")
    if any(n < 4096 or n > 64 << 20 or n % 4096 for n in a.sizes) or any(n < 1 or n > 64 for n in a.batches):
        p.error("invalid sweep bounds")
    out.mkdir(parents=True, exist_ok=False)
    env = {k: v for k, v in os.environ.items() if not k.startswith(("MC_", "TENT_", "MOONCAKE_")) and k not in ("LD_PRELOAD", "LD_LIBRARY_PATH", "ORBITE_BENCH_CORRUPT_BYTE")}
    commands = []
    tent_env = {"MC_RPC_CLIENT_IO_THREADS": str(a.workers), "MC_TE_RPC_CLIENT_IO_THREADS": str(a.workers)}

    def run(name, cmd, timeout=1200, overrides=None, expect_failure=False):
        cmd = list(map(str, cmd))
        commands.append({"name": name, "command": cmd, "env_overrides": overrides or {}})
        (out / "commands.json").write_text(json.dumps(commands, indent=2)+"\n")
        with (out / (name+".stderr")).open("w") as err:
            r = subprocess.run(cmd, cwd=ROOT, env=env | (overrides or {}), text=True,
                               stdout=subprocess.PIPE, stderr=err, timeout=timeout)
        (out / (name+".stdout")).write_text(r.stdout)
        if (r.returncode == 0) == expect_failure:
            raise RuntimeError(f"unexpected exit {r.returncode}: {name}; see {out}")
        return r.stdout

    controls()
    source = ref / "reference"
    if run("tent-revision", ["git", "-C", source, "rev-parse", "HEAD"]).strip() != CONTRACT["tent_revision"]:
        raise ValueError("not the frozen TENT reference")
    oracle = json.loads((ref / "manifest.json").read_text())
    if oracle["status"] != "passed-cpu-trace" or not oracle["native_executed"]:
        raise ValueError("reference CPU oracle must pass first")
    for path, expected in [(a.ext4, "ext4"), (a.tmpfs, "tmpfs")]:
        if run(expected+"-fs", ["findmnt", "-n", "-o", "FSTYPE", "-T", path]).strip() != expected:
            raise ValueError(f"{path} is not {expected}")
    built = run("rust-build", ["cargo", "build", "--release", "--locked", "--features", "io-uring", "--bench", "transfer", "--message-format=json"])
    rust = next(Path(r["executable"]) for line in built.splitlines() if (r := json.loads(line)).get("executable") and r.get("target", {}).get("name") == "transfer")
    library = ref / "tent-build/mooncake-transfer-engine/tent/src/libtent_shared.so"
    tent = out / "tent-bench"
    run("tent-build", ["c++", "-std=c++20", "-O3", "-DNDEBUG", "-Wall", "-Wextra", "-Werror", "-I", source / "mooncake-transfer-engine/tent/include", ROOT / "benches/tent.cpp", library, "-Wl,-rpath,"+str(library.parent), "-o", tent])
    manifest = {"orbite_revision": run("orbite-revision", ["git", "rev-parse", "HEAD"]).strip(),
                "dirty": bool(run("orbite-status", ["git", "status", "--porcelain"]).strip()),
                "tent_revision": CONTRACT["tent_revision"], "host": platform.node(), "kernel": platform.platform(),
                "cpu": run("cpu", ["lscpu"]), "affinity": sorted(os.sched_getaffinity(0)),
                "rustc": run("rustc", ["rustc", "-Vv"]), "cxx": run("cxx", ["c++", "--version"]),
                "parameters": {k: str(v) if isinstance(v, Path) else v for k, v in vars(a).items()},
                "caller_threads": 1, "workers_per_engine": a.workers, "tent_rpc_pools": tent_env, "merge": True,
                "engine_max_failover_attempts": 0,
                "tent_tcp_local_retry": {"max_retry_count": 3, "base_delay_ms": 100, "max_delay_ms": 2000},
                "poll": "128 hot then 1,2,4,...,128,200 us capped", "metrics": False,
                "timing": "allocate batch + submit + poll + free; per-batch wall and whole-process CPU",
                "checks": "every warmup and sample; changed pattern, reset destination, full bytes and guards; excluded from time",
                "file_state": "buffered reset before each sample; warmed ext4 O_DIRECT / tmpfs; no fsync, no durability or SSD performance claim",
                "builds": {"rust": "release, io-uring", "tent": oracle["contract"]["cmake_options"]},
                "harness_sha256": {f.name: hashlib.sha256(f.read_bytes()).hexdigest() for f in (ROOT / "benches").iterdir() if f.is_file()},
                "binary_sha256": {k: hashlib.sha256(v.read_bytes()).hexdigest() for k, v in {"orbite": rust, "tent": tent}.items()},
                "status": "running"}
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2)+"\n")
    for name, exe in [("orbite", rust), ("tent", tent)]:
        text = run(name+"-bad-byte", [exe, "tcp", "read", 4096, 1, "contiguous", 1, 1, a.workers, "-"], overrides=({**(tent_env if name == "tent" else {}), "ORBITE_BENCH_CORRUPT_BYTE": "1"}), expect_failure=True)
        if '"verified":true' in text:
            raise ValueError("corrupted guard produced a report")
    shapes = [(size, 1, "contiguous") for size in a.sizes]
    shapes += [(4096, n, shape) for n in a.batches if n != 1 for shape in ("contiguous", "gapped")]
    rows = []
    for trial in range(a.trials):
        for profile in ("cpu", "tcp", "hp_tcp", "shm", "io_uring-ext4", "io_uring-tmpfs"):
            backend = "io_uring" if profile.startswith("io_uring") else profile
            implementations = [("orbite", rust), ("tent", tent)] if backend in ("cpu", "tcp", "io_uring") else [("orbite", rust)]
            if trial % 2: implementations.reverse()
            for op in ("read", "write"):
                for block, batch, layout in shapes:
                    for implementation, exe in implementations:
                        name = f"{trial}-{implementation}-{profile}-{op}-{block}-{batch}-{layout}"
                        directory = a.ext4 if profile.endswith("ext4") else a.tmpfs
                        path = directory.resolve() / ("orbite-bench-"+name)
                        if path.exists(): raise FileExistsError(path)
                        try:
                            text = run(name, [exe, backend, op, block, batch, layout, a.warmup, a.samples, a.workers, path], overrides=tent_env if implementation == "tent" else None)
                            row = json.loads(text)
                            summary = summarize(row, a.samples, block*batch)
                            if implementation == "orbite" and backend == "io_uring" and row["direct_io"] != profile.endswith("ext4"):
                                raise ValueError("wrong file route")
                            expected_owners = 1 if backend != "io_uring" and layout == "contiguous" else batch
                            if implementation == "orbite" and backend != "cpu" and row["physical_owners"] != expected_owners:
                                raise ValueError("merge/coalescing path did not match layout")
                            record = dict(trial=trial, implementation=implementation, profile=profile, op=op, block=block, batch=batch, layout=layout, **row, **summary)
                            rows.append(record)
                            with (out / "results.jsonl").open("a") as f: f.write(json.dumps(record)+"\n")
                            print(json.dumps({k: v for k, v in record.items() if k not in ("samples_ns", "cpu_ns")}), flush=True)
                        finally:
                            path.unlink(missing_ok=True)
    for backend in ("GPU", "RDMA", "GDS", "EFA"):
        with (out / "results.jsonl").open("a") as f: f.write(json.dumps({"profile": backend, "status": "not run (hardware)"})+"\n")
    manifest.update(status="passed", verified_rows=len(rows), reporter_negative_controls=5, guard_negative_controls=2)
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2)+"\n")


if __name__ == "__main__":
    main()
