#!/usr/bin/env python3
"""Repeat the submit-rejection control under bounded process and CPU load."""
import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import json
import os
from pathlib import Path
import subprocess
import sys
import time

CONTROLS = [
    "engine::failover::tests::native::submit_rejection_walks_the_pinned_policy_once_per_physical_owner",
    "engine::failover::tests::native::source_receive_retirement_keeps_owners_and_allows_unposted_policy_fallback",
    "transport::rdma::cq_lane::tests::retirement::receive_debt_retirement_rejects_new_grants_until_the_native_receipt",
]


def bounded(low, high):
    def parse(value):
        result = int(value)
        if not low <= result <= high:
            raise argparse.ArgumentTypeError(f"must be between {low} and {high}")
        return result
    return parse


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--iterations", type=bounded(1, 100000), default=2500)
    parser.add_argument("--jobs", type=bounded(1, 32), default=8)
    parser.add_argument("--cpu-load", type=bounded(0, 32), default=0)
    parser.add_argument("--test-threads", type=bounded(1, 256), default=64)
    args = parser.parse_args()
    if sys.platform != "linux":
        parser.error("the stress control requires Linux")
    binary = str(args.binary.resolve())
    listed = subprocess.check_output([binary, "--list"], text=True, timeout=30)
    if any(listed.splitlines().count(control + ": test") != 1 for control in CONTROLS):
        parser.error("binary must contain each submit-rejection and retirement control once")
    args.output.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    loads, failures = [], []
    completed = 0

    def run(index):
        try:
            result = subprocess.run(
                [binary, "--exact", *CONTROLS, f"--test-threads={args.test_threads}", "--nocapture"],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=90,
            )
            passed = result.returncode == 0 and f"test result: ok. {len(CONTROLS)} passed; 0 failed;".encode() in result.stdout
            output = result.stdout
        except subprocess.TimeoutExpired as error:
            passed, output = False, (error.stdout or b"") + b"\nstress process timeout\n"
        if not passed:
            (args.output / f"failure-{index}.log").write_bytes(output)
        return index, passed

    try:
        for _ in range(args.cpu_load):
            loads.append(subprocess.Popen([sys.executable, "-c", "while True: pass"]))
        jobs = iter(range(args.iterations))
        with ThreadPoolExecutor(max_workers=args.jobs) as executor:
            pending = {executor.submit(run, next(jobs)) for _ in range(min(args.jobs, args.iterations))}
            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    index, passed = future.result()
                    completed += 1
                    if not passed:
                        failures.append(index)
                        print(f"Failure at iteration {index}:\n" + (args.output / f"failure-{index}.log").read_text(errors="replace"), flush=True)
                    # Finish running controls, but do not keep generating work
                    # after a failure. Its full diagnostics stay in the output.
                    if not failures:
                        index = next(jobs, None)
                        if index is not None:
                            pending.add(executor.submit(run, index))
                    if completed % 100 == 0:
                        print(f"completed {completed}/{args.iterations}; failures {len(failures)}", flush=True)
    finally:
        for process in loads:
            process.terminate()
        for process in loads:
            process.wait(timeout=10)
        report = dict(
            binary=binary, controls=CONTROLS, requested=args.iterations, completed=completed,
            failures=failures, jobs=args.jobs, cpu_load=args.cpu_load,
            test_threads=args.test_threads, host_cpus=os.cpu_count(),
            elapsed_seconds=round(time.monotonic() - started, 3),
        )
        (args.output / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report), flush=True)
    return int(bool(failures) or completed != args.iterations)


if __name__ == "__main__":
    sys.exit(main())
