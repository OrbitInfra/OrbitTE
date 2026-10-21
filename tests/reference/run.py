"""Run the shared CPU trace against public OrbitTE APIs and frozen native TENT."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import os
import platform
import re
import shlex
import signal
import subprocess
import sys
import tarfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
CONTRACT = json.loads((REPO / "tests/reference/contract.json").read_text())


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def compare(expected: list[str], actual: list[str]) -> None:
    if expected != actual:
        difference = "\n".join(
            difflib.unified_diff(
                expected, actual, fromfile="expected", tofile="actual", lineterm=""
            )
        )
        raise ValueError("CPU trace mismatch:\n" + difference)


def negative_controls(expected: list[str]) -> list[str]:
    controls = []
    for name, kind, field, value in [
        ("corrupt-byte", "BYTES", 3, "00"),
        ("wrong-byte-count", "RESULT", 5, "999"),
        ("nonterminal-status", "RESULT", 3, "pending"),
        ("duplicate-message", "TAKE", 3, "999"),
    ]:
        changed = expected.copy()
        index = next(i for i, line in enumerate(changed) if line.split("\t")[1] == kind)
        fields = changed[index].split("\t")
        fields[field] = value
        if name == "corrupt-byte":
            original = expected[index].split("\t")[field]
            fields[field] = f"{int(original[:2], 16) ^ 1:02x}" + original[2:]
        elif name == "duplicate-message":
            fields[3] = str(int(expected[index].split("\t")[3]) + 1)
            fields[4] += "," + fields[4]
        changed[index] = "\t".join(fields)
        try:
            compare(expected, changed)
        except ValueError:
            controls.append(name)
        else:
            raise RuntimeError("comparator accepted " + name)
    for name, changed in [
        ("missing-cleanup", expected[:-1]),
        ("extra-observation", expected + [expected[-1]]),
    ]:
        try:
            compare(expected, changed)
        except ValueError:
            controls.append(name)
        else:
            raise RuntimeError("comparator accepted " + name)
    return controls


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--rust-only",
        action="store_true",
        help="fixture gate only; never claims native comparison",
    )
    mode.add_argument(
        "--fetch-reference",
        action="store_true",
        help="fetch the frozen public reference and YLT archive into the artifact directory",
    )
    mode.add_argument(
        "--tent-source",
        type=Path,
        help="clean external checkout at the frozen commit, including pybind11",
    )
    parser.add_argument(
        "--ylt-archive",
        type=Path,
        help="the archive named in frozen FindYLT.cmake; SHA-256 is checked",
    )
    parser.add_argument(
        "--output", type=Path, required=True, help="new external artifact directory"
    )
    parser.add_argument("--jobs", type=int, default=4, choices=range(1, 17))
    args = parser.parse_args()
    output = args.output.resolve()
    if output.is_relative_to(REPO) or (
        args.tent_source and output.is_relative_to(args.tent_source.resolve())
    ):
        parser.error("output must be outside both source checkouts")
    if args.tent_source and not args.ylt_archive:
        parser.error("--tent-source requires --ylt-archive")
    if args.rust_only and args.ylt_archive:
        parser.error("--rust-only does not consume a YLT archive")
    output.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": 1,
        "status": "running",
        "qualification": "P0.1 scoped CPU trace; independent acceptance pending",
        "profile": CONTRACT["profile"],
        "native_executed": False,
        "performance_claim": False,
        "commands": [],
        "platform": platform.platform(),
        "contract": CONTRACT,
    }
    manifest_path = output / "manifest.json"

    def save() -> None:
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    # Do not inherit a consumer's native loader paths or Mooncake configuration.
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("MC_", "TENT_", "MOONCAKE_"))
    }
    for key in ("LD_LIBRARY_PATH", "LD_PRELOAD"):
        environment.pop(key, None)

    def run(name: str, command: list[str], cwd: Path = REPO, timeout: int = 120) -> str:
        record = {"name": name, "command": command, "cwd": str(cwd)}
        manifest["commands"].append(record)
        started = time.monotonic()
        stdout = output / (name + ".stdout")
        stderr = output / (name + ".stderr")
        record.update(stdout=str(stdout), stderr=str(stderr))
        save()
        with stdout.open("w") as out, stderr.open("w") as err:
            process = subprocess.Popen(
                command,
                cwd=cwd,
                env=environment,
                stdout=out,
                stderr=err,
                start_new_session=True,
            )
            try:
                result = process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                record["timed_out"] = True
                result = process.returncode
        record.update(
            exit_code=result, elapsed_seconds=round(time.monotonic() - started, 3)
        )
        save()
        if result != 0:
            raise RuntimeError(f"{name} failed ({result}); see {stderr}")
        return stdout.read_text()

    try:
        save()
        packaged_vcs = REPO / ".cargo_vcs_info.json"
        if packaged_vcs.is_file():
            manifest["source"] = {
                "revision": json.loads(packaged_vcs.read_text())["git"]["sha1"],
                "origin": "cargo-package",
                "dirty": None,
            }
        else:
            revision = run("source-revision", ["git", "rev-parse", "HEAD"]).strip()
            dirty = bool(run("source-status", ["git", "status", "--porcelain"]).strip())
            manifest["source"] = {"revision": revision, "origin": "git", "dirty": dirty}
        manifest["rustc"] = run("rustc-version", ["rustc", "-Vv"]).strip()
        inputs = [
            Path(CONTRACT["trace"]),
            Path(CONTRACT["expected"]),
            Path("tests/reference/contract.json"),
            Path("tests/reference/run.py"),
            Path("tests/reference/rust_driver.rs"),
            Path("tests/reference/tent_driver.cpp"),
        ]
        manifest["input_sha256"] = {str(path): digest(REPO / path) for path in inputs}
        expected = (REPO / CONTRACT["expected"]).read_text().splitlines()
        manifest["negative_controls"] = negative_controls(expected)
        trace = REPO / CONTRACT["trace"]
        run(
            "rust-build",
            [
                "cargo",
                "build",
                "--locked",
                "--example",
                "cpu_trace",
                "--target-dir",
                str(output / "cargo"),
            ],
        )
        rust_binary = output / "cargo/debug/examples/cpu_trace"
        rust_output = run("rust-trace", [str(rust_binary), str(trace)], timeout=30)
        rust_records = [
            line for line in rust_output.splitlines() if line.startswith("OBS\t")
        ]
        compare(expected, rust_records)
        rust_linkage = run("rust-linkage", ["ldd", str(rust_binary)])
        if re.search(r"lib(?:tent|mooncake|asio)", rust_linkage):
            raise RuntimeError("Rust runner unexpectedly depends on a Mooncake runtime")
        manifest["rust_binary"] = {
            "path": str(rust_binary),
            "sha256": digest(rust_binary),
        }
        manifest["observations"] = len(expected)
        if not args.rust_only:
            if args.fetch_reference:
                source = output / "reference"
                run("reference-init", ["git", "init", str(source)], cwd=output)
                run(
                    "reference-fetch",
                    [
                        "git",
                        "fetch",
                        "--depth=1",
                        CONTRACT["tent_repository"],
                        CONTRACT["tent_revision"],
                    ],
                    cwd=source,
                    timeout=180,
                )
                run(
                    "reference-checkout",
                    ["git", "checkout", "--detach", "FETCH_HEAD"],
                    cwd=source,
                )
                run(
                    "reference-submodules",
                    [
                        "git",
                        "submodule",
                        "update",
                        "--init",
                        "--depth=1",
                        "extern/pybind11",
                    ],
                    cwd=source,
                    timeout=180,
                )
                archive = output / "yalantinglibs.tar.gz"
                url = (
                    "https://github.com/alibaba/yalantinglibs/archive/"
                    + CONTRACT["ylt_revision"]
                    + ".tar.gz"
                )
                run(
                    "ylt-download",
                    [
                        "curl",
                        "--fail",
                        "--location",
                        "--max-time",
                        "120",
                        "--output",
                        str(archive),
                        url,
                    ],
                    cwd=output,
                    timeout=130,
                )
            else:
                source = args.tent_source.resolve()
                archive = args.ylt_archive.resolve()
            native_revision = run(
                "tent-revision", ["git", "rev-parse", "HEAD"], cwd=source
            ).strip()
            if native_revision != CONTRACT["tent_revision"]:
                raise ValueError("native source is not the frozen TENT revision")
            if run(
                "tent-status",
                ["git", "status", "--porcelain", "--untracked-files=all"],
                cwd=source,
            ).strip():
                raise ValueError("native source checkout is modified")
            submodule_revision = run(
                "pybind11-revision",
                ["git", "rev-parse", "HEAD"],
                cwd=source / "extern/pybind11",
            ).strip()
            if submodule_revision != CONTRACT["pybind11_revision"]:
                raise ValueError("pybind11 revision differs from the frozen gitlink")
            if digest(archive) != CONTRACT["ylt_archive_sha256"]:
                raise ValueError("YLT archive SHA-256 mismatch")
            with tarfile.open(archive) as package:
                package.extractall(output / "deps", filter="data")
            ylt = output / ("deps/yalantinglibs-" + CONTRACT["ylt_revision"])
            build = output / "tent-build"
            flags = [
                f"-D{key}={value}" for key, value in CONTRACT["cmake_options"].items()
            ]
            flags.append("-DFETCHCONTENT_SOURCE_DIR_YALANTINGLIBS=" + str(ylt))
            run(
                "tent-configure",
                [
                    "cmake",
                    "-S",
                    str(source / "mooncake-transfer-engine"),
                    "-B",
                    str(build),
                    *flags,
                ],
            )
            run(
                "tent-build",
                [
                    "cmake",
                    "--build",
                    str(build),
                    "--target",
                    "tent_shared",
                    "--parallel",
                    str(args.jobs),
                ],
                timeout=900,
            )
            library = build / "tent/src/libtent_shared.so"
            binary = output / "tent-driver"
            compiler = shlex.split(os.environ.get("CXX", "c++"))
            manifest["cxx"] = run("cxx-version", [*compiler, "--version"]).strip()
            run(
                "tent-driver-build",
                [
                    *compiler,
                    "-std=c++20",
                    "-O2",
                    "-Wall",
                    "-Wextra",
                    "-Werror",
                    "-I",
                    str(source / "mooncake-transfer-engine/tent/include"),
                    str(REPO / "tests/reference/tent_driver.cpp"),
                    str(library),
                    "-Wl,-rpath," + str(library.parent),
                    "-o",
                    str(binary),
                ],
            )
            linkage = run("tent-linkage", ["ldd", str(binary)])
            dependencies = {}
            for name, path in re.findall(
                r"^\s*(\S+)\s+=>\s+(\S+)", linkage, re.MULTILINE
            ):
                resolved = Path(path).resolve()
                if path == "not":
                    raise RuntimeError("missing native dependency: " + name)
                if name.startswith(
                    ("libtent", "libmooncake", "libasio")
                ) and not resolved.is_relative_to(build):
                    raise RuntimeError(
                        "native dependency escaped isolated build: " + str(resolved)
                    )
                dependencies[name] = {"path": str(resolved), "sha256": digest(resolved)}
            manifest["native_dependencies"] = dependencies
            manifest["native_executed"] = True
            native_output = run("tent-trace", [str(binary), str(trace)], timeout=30)
            native_records = [
                line for line in native_output.splitlines() if line.startswith("OBS\t")
            ]
            compare(expected, native_records)
            compare(rust_records, native_records)
            if run(
                "tent-final-status",
                ["git", "status", "--porcelain", "--untracked-files=all"],
                cwd=source,
            ).strip():
                raise ValueError("native build modified the frozen source checkout")
            manifest["native_binary"] = {
                "path": str(binary),
                "sha256": digest(binary),
                "library_sha256": digest(library),
            }
        manifest["status"] = (
            "passed-cpu-trace"
            if manifest["native_executed"]
            else "passed-rust-fixture-only"
        )
        save()
        print(f"{manifest['status']}: {len(expected)} observations; {manifest_path}")
        return 0
    except Exception as error:
        manifest.update(status="failed", error=str(error))
        save()
        print(str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
