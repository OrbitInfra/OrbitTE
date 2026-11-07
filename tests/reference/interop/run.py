#!/usr/bin/env python3
"""Directional READ/WRITE against the isolated frozen TENT build; no hardware claims."""
import argparse
import csv
import importlib.util
import json
import os
from pathlib import Path
import select
import shlex
import socket
import struct
import subprocess
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
SPEC = importlib.util.spec_from_file_location("wire", HERE.parent / "wire/check.py")
wire = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(wire)
CONTRACT = json.loads((HERE.parent / "contract.json").read_text())
CORPUS = json.loads((HERE.parent / "wire/rpc.json").read_text())
TIMEOUT = 30


def environment():
    return {k: v for k, v in os.environ.items() if not k.startswith(("MC_", "TENT_", "MOONCAKE_")) and k not in ("LD_PRELOAD", "LD_LIBRARY_PATH")}


def request(endpoint, name, payload, corrupt=None):
    h = bytearray.fromhex(CORPUS[name]["request"]["header_hex"])
    struct.pack_into("<I", h, 16, len(payload))
    if corrupt is not None:
        h[corrupt] ^= 1
    body = bytes.fromhex(CORPUS[name]["request"]["body_hex"])
    host, port = endpoint.rsplit(":", 1)
    with socket.create_connection((host, int(port)), TIMEOUT) as s:
        s.settimeout(TIMEOUT)
        try:
            s.sendall(h + body + payload)
            if corrupt is not None:
                data = s.recv(1)
                if data:
                    raise ValueError("bad RPC magic/version accepted")
                return
            header, returned, data = wire.frame(s, wire.RESP)
            if header[:4] != bytes.fromhex("15000000") or returned != bytes.fromhex(CORPUS[name]["response"]["body_hex"]):
                raise ValueError("response framing/serialization drift")
            return data
        except (ConnectionResetError, BrokenPipeError):
            if corrupt is None:
                raise


def negative_controls(endpoint, base, n):
    # Mismatched request headers must close before touching the guarded source.
    for byte in (0, 1):
        request(endpoint, "SendData", struct.pack("<QQ", base + 1, 3) + b"bad", byte)
    initial = request(endpoint, "RecvData", struct.pack("<QQ", base, min(n + 2, 32)))
    expected = bytes([0xa1]) + bytes((i * 17 + 23) % 251 for i in range(min(n, 31)))
    if n + 2 <= 32:
        expected = bytes([0xa1]) + bytes((i * 17 + 23) % 251 for i in range(n)) + bytes([0xa1])
    if initial != expected:
        raise ValueError("rejected frame changed source bytes/guard")
    # Correct framing, invalid range: a service rejection, not success bytes.
    invalid = request(endpoint, "RecvData", struct.pack("<QQ", (1 << 64) - 1, 3))
    if len(invalid) == 3:
        raise ValueError("invalid READ looked successful")


def pair(source_binary, requester_binary, n, output, tag, records):
    with (output / (tag + "-source.stderr")).open("wb") as err:
        server = subprocess.Popen([str(source_binary), "source", str(n)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=err, env=environment())
        try:
            if not select.select([server.stdout], [], [], TIMEOUT)[0]:
                raise TimeoutError("source startup")
            line = server.stdout.readline().decode().strip().split()
            if len(line) != 5 or line[0] != "READY":
                raise ValueError("bad source announcement: " + repr(line))
            endpoint, base, readonly, size = line[1:]
            negative_controls(endpoint, int(base), n)
            client = subprocess.run([str(requester_binary), "requester", endpoint, base, readonly, size], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=TIMEOUT, env=environment())
            (output / (tag + "-requester.stdout")).write_bytes(client.stdout)
            (output / (tag + "-requester.stderr")).write_bytes(client.stderr)
            if client.returncode or b"PASS requester" not in client.stdout:
                raise ValueError(f"{tag}: requester failed ({client.returncode}); see stderr")
            stdout, _ = server.communicate(b"verify\n", timeout=TIMEOUT)
            (output / (tag + "-source.stdout")).write_bytes(stdout)
            if server.returncode or b"PASS source" not in stdout:
                raise ValueError(f"{tag}: source verification failed ({server.returncode})")
            records.append(dict(direction=tag, bytes=n, read=True, write=True, guards=True, permission_rejection=True, bad_magic=True, bad_version=True, invalid_range=True))
        finally:
            if server.poll() is None:
                server.kill()
            server.wait(timeout=TIMEOUT)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--build", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--iterations", type=int, default=1, choices=range(1, 101))
    a = p.parse_args()
    output = a.output.resolve()
    if output.is_relative_to(ROOT):
        p.error("evidence must stay outside the repository")
    coverage = list(csv.DictReader((HERE / "coverage.tsv").open(), delimiter="\t"))
    for inventory, key in [("rpc.tsv", "name"), ("metadata.tsv", "item")]:
        expected = {r[key] for r in csv.DictReader((HERE.parent / "inventory" / inventory).open(), delimiter="\t")}
        mapped = [r["row"] for r in coverage if r["inventory"] == inventory]
        if len(mapped) != len(set(mapped)) or set(mapped) != expected:
            p.error("incomplete or duplicate S16 coverage: " + inventory)
    output.mkdir(parents=True, exist_ok=False)
    source, build = a.source.resolve(), a.build.resolve()
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip()
    if revision != CONTRACT["tent_revision"]:
        p.error("native source must be the frozen revision")
    lib = build / "mooncake-transfer-engine/tent/src/libtent_shared.so"
    native = output / "native-peer"
    rust_target = output / "cargo"
    rust = rust_target / "debug/examples/tent_interop"
    report = dict(reference=revision, toolchain=os.environ.get("RUSTUP_TOOLCHAIN", "default"), status="running", inventory_rows=len(coverage), checks=[])
    started = time.monotonic()
    try:
        subprocess.run([*shlex.split(os.environ.get("CXX", "c++")), "-std=c++20", "-O2", "-Wall", "-Wextra", "-Werror", "-I", str(source / "mooncake-transfer-engine/tent/include"), str(HERE / "native.cpp"), str(lib), "-Wl,-rpath," + str(lib.parent), "-o", str(native)], check=True, timeout=240, env=environment())
        subprocess.run(["cargo", "build", "--locked", "--example", "tent_interop", "--target-dir", str(rust_target)], cwd=ROOT, env=environment(), check=True, timeout=240)
        linked = subprocess.check_output(["ldd", str(rust)], text=True)
        if any(name in linked for name in ("libtent", "libmooncake", "libasio")):
            raise ValueError("OrbiTE linked the native oracle runtime")
        (output / "rust-linkage.txt").write_text(linked)
        for i in range(a.iterations):
            for n in ([3, 65539] if i == 0 else [4097]):
                pair(native, rust, n, output, f"{i}-{n}-rust-requester", report["checks"])
                pair(rust, native, n, output, f"{i}-{n}-cpp-requester", report["checks"])
        report["status"] = "passed"
    finally:
        report["elapsed_seconds"] = round(time.monotonic() - started, 3)
        (output / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(f"passed: {len(report['checks'])} directional guarded READ/WRITE pairs")


if __name__ == "__main__":
    main()
