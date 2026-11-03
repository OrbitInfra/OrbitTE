"""Measure CPU admission/completion and a guarded loopback READ on one idle runner."""
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import tempfile

def cargo(*args):
    return subprocess.check_output(["cargo", *args], text=True)

target = Path(json.loads(cargo("metadata", "--format-version", "1", "--no-deps"))["target_directory"])
cpu = platform.machine()
if Path("/proc/cpuinfo").is_file():
    cpu = next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
                if line.startswith("model name")), cpu)
print(json.dumps({"kernel": platform.platform(), "cpu": cpu,
                  "cpus": len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else os.cpu_count(),
                  "rust": subprocess.check_output(["rustc", "-V"], text=True).strip()}), flush=True)
results = {m: [] for m in ("off", "disabled", "enabled")}
with tempfile.TemporaryDirectory() as build:
    binaries = {}
    for name, features in [("off", ""), ("on", "metrics")]:
        cargo("build", "--release", "--locked", "--no-default-features", "--features", features, "--example", "metrics_overhead")
        binaries[name] = Path(shutil.copy(target / "release/examples/metrics_overhead", Path(build) / name))
    modes = list(results)
    for i in range(9):
        for mode in modes[i % 3:] + modes[:i % 3]:
            row = json.loads(subprocess.check_output([binaries["off" if mode == "off" else "on"], mode], text=True))
            assert row.pop("mode") == mode
            results[mode].append(row)
            print(json.dumps({"trial": i+1, "mode": mode, **row}), flush=True)
summary = {}
for mode, rows in results.items():
    summary[mode] = {key: {"median": statistics.median(vals), "min": min(vals), "max": max(vals)}
                     for key in rows[0] if (vals := [r[key] for r in rows])}
print("ORBITTE_METRICS_BENCH " + json.dumps(summary), flush=True)
