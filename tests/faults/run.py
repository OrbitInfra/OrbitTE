"""Execute the P10 CPU matrix through existing production-owner controls; never hardware."""
import argparse
import csv
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True, help="cargo test --features io-uring,fabric --lib --no-run executable")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--kill-repeats", type=int, default=50)
    args = parser.parse_args()
    out = args.output.resolve()
    if out.is_relative_to(ROOT) or not 1 <= args.kill_repeats <= 2000:
        parser.error("external evidence directory and bounded repetitions required")
    out.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, ORBITE_REQUIRE_IO_URING="1", ORBITE_REQUIRE_FABRIC="1", FI_PROVIDER="tcp")
    if not all(k in env for k in ("ORBITE_EXT4_DIR", "ORBITE_TMPFS_DIR")):
        parser.error("required ext4/tmpfs fixture directories missing")
    tests = subprocess.check_output([args.binary, "--list"], text=True).splitlines()
    tests = [s.removesuffix(": test") for s in tests if s.endswith(": test")]
    with (ROOT / "tests/faults/matrix.tsv").open() as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    results = []
    for i, row in enumerate(rows):
        if row["control"] == "N/A":
            results.append(row | {"status": "not applicable"})
            continue
        matches = [t for t in tests if t.endswith("::"+row["control"])]
        if len(matches) != 1:
            raise ValueError(f"missing/ambiguous control: {row}")
        count = args.kill_repeats if row["control"].startswith("p10_killed_") else 1
        for n in range(count):
            cmd = [str(args.binary), "--exact", matches[0], "--test-threads", str(os.cpu_count())]
            with (out / f"{i}-{n}.log").open("w") as f:
                subprocess.run(cmd, env=env, stdout=f, stderr=subprocess.STDOUT, check=True, timeout=120)
            if "test result: ok. 1 passed; 0 failed;" not in (out / f"{i}-{n}.log").read_text():
                raise ValueError("control did not execute")
        results.append(row | {"status": "passed", "repetitions": count, "test": matches[0]})
        (out / "matrix.json").write_text(json.dumps(results, indent=2)+"\n")
    for backend in {r["backend"] for r in rows}:
        axes = {a for r in rows if r["backend"] == backend for a in r["faults"].split(",")}
        if axes != {"peer-kill", "timeout", "cancel", "partial-submit", "close", "restart"}:
            raise ValueError("incomplete fault matrix: "+backend)
    print(f"{len(results)} fixture dispositions checked; {args.kill_repeats} real-process kill repetitions per socket backend")


if __name__ == "__main__":
    main()
