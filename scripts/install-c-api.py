#!/usr/bin/env python3
"""Build libtent and install its C header and both link modes into a prefix."""

import argparse
import json
from pathlib import Path
import shutil
import os
import tempfile
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
HEADER = Path("include/tent/transfer_engine.h")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--prefix", required=True, type=Path)
    p.add_argument("--features", default="")
    p.add_argument("--toolchain", default="stable")
    p.add_argument(
        "--cpp", action="store_true", help="also build the Linux C++20 adapter archive"
    )
    p.add_argument("--debug", action="store_true", help="install a debug build")
    p.add_argument(
        "--manifest-path", type=Path, default=ROOT / "crates/orbite-c/Cargo.toml"
    )
    p.add_argument(
        "--engine-source", type=Path, help="use the matching unpublished engine package"
    )
    args = p.parse_args()
    manifest = args.manifest_path.resolve()
    cmd = [
        "cargo",
        "+" + args.toolchain,
        "build",
        "--locked",
        "-p",
        "orbite-c",
        "--features",
        args.features,
        "--message-format=json-render-diagnostics",
    ]
    cmd.extend(["--manifest-path", str(manifest)])
    if args.engine_source:
        cmd.extend(
            [
                "--config",
                "patch.crates-io.orbite.path="
                + json.dumps(str(args.engine_source.resolve())),
            ]
        )
    if not args.debug:
        cmd.append("--release")
    build = subprocess.run(cmd, cwd=ROOT, check=True, text=True, stdout=subprocess.PIPE)
    files = [
        Path(f)
        for line in build.stdout.splitlines()
        if (row := json.loads(line))["reason"] == "compiler-artifact"
        and row["target"]["name"] == "tent"
        for f in row["filenames"]
    ]
    if not any(p.suffix == ".a" for p in files) or not any(
        p.suffix in {".so", ".dylib"} for p in files
    ):
        raise SystemExit("expected a shared and static TENT library")
    lib = args.prefix / "lib"
    lib.mkdir(parents=True, exist_ok=True)
    for f in files:
        if f.suffix in {".a", ".so", ".dylib"}:
            shutil.copy2(f, lib / f.name)
    source = manifest.parent
    shutil.copytree(source / "include", args.prefix / "include", dirs_exist_ok=True)
    if args.cpp:
        if not sys.platform.startswith("linux"):
            raise SystemExit("the installed C++ profile requires Linux")
        with tempfile.TemporaryDirectory() as tmp:
            objects = []
            for unit in sorted((source / "cpp").glob("*.cpp")):
                obj = Path(tmp) / unit.with_suffix(".o").name
                subprocess.run(
                    [
                        os.environ.get("CXX", "c++"),
                        "-std=c++20",
                        "-fPIC",
                        "-Wall",
                        "-Wextra",
                        "-Werror",
                        "-O0" if args.debug else "-O2",
                        "-I",
                        source / "include",
                        "-c",
                        unit,
                        "-o",
                        obj,
                    ],
                    check=True,
                )
                objects.append(obj)
            archive = Path(tmp) / "libtent_cpp.a"
            subprocess.run(
                [os.environ.get("AR", "ar"), "rcs", archive, *objects], check=True
            )
            shutil.copy2(archive, lib / archive.name)

    if sys.platform.startswith("linux"):
        features = set(args.features.split(","))
        private = "-ldl -lm -lrt -lutil -pthread"
        if features.intersection({"rdma", "rdma-adaptive"}):
            private = "-libverbs " + private
        package = json.loads(
            subprocess.check_output(
                [
                    "cargo",
                    "+" + args.toolchain,
                    "metadata",
                    "--no-deps",
                    "--format-version",
                    "1",
                    "--manifest-path",
                    str(manifest),
                ],
                cwd=manifest.parent,
            )
        )
        version = next(
            x["version"] for x in package["packages"] if x["name"] == "orbite-c"
        )
        pc = lib / "pkgconfig/tent.pc"
        pc.parent.mkdir(exist_ok=True)
        pc.write_text(f"""prefix=${{pcfiledir}}/../..
libdir=${{prefix}}/lib
includedir=${{prefix}}/include

Name: tent
Description: OrbiTE TENT C transfer-engine ABI
Version: {version}
Libs: -L"${{libdir}}" -ltent
Libs.private: {private}
Cflags: -I"${{includedir}}"
""")

        if args.cpp:
            (pc.parent / "tent-cpp.pc").write_text(f"""prefix=${{pcfiledir}}/../..
libdir=${{prefix}}/lib
Name: tent-cpp
Description: OrbiTE native TENT and classic TE source adapters
Version: {version}
Requires: tent
Libs: -L"${{libdir}}" -ltent_cpp
Cflags: -std=c++20
""")


if __name__ == "__main__":
    main()
