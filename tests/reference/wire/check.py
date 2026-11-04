"""Compare frozen TENT's native codecs and loopback RPC bytes with the committed corpus."""

import argparse
from contextlib import contextmanager
import csv
import difflib
import json
import os
from pathlib import Path
import select
import shlex
import socket
import struct
import subprocess

HERE = Path(__file__).resolve().parent
REF = HERE.parent
CASES = REF / "inventory"
TIMEOUT = 30
# The Linux little-endian YLT header has four byte fields followed by uint32s.
REQ = struct.Struct("<BBBBIIII")
RESP = struct.Struct("<BBBBIII")


def build(source, build_dir, output):
    unit = next(
        u
        for u in json.loads((build_dir / "compile_commands.json").read_text())
        if u["file"].endswith("/tent/src/rpc/rpc.cpp")
    )
    args = shlex.split(unit["command"]) if "command" in unit else unit["arguments"].copy()
    del args[args.index("-o") : args.index("-o") + 2]
    args.remove("-c")
    args.remove(unit["file"])
    tent = source / "mooncake-transfer-engine/tent"
    lib = build_dir / "mooncake-transfer-engine/tent/src/libtent_shared.so"
    binary = output / "wire-peer"
    # The two optional provider codecs are SDK-independent upstream translation units.
    subprocess.run(
        [
            *args,
            str(HERE / "native.cpp"),
            str(tent / "src/transport/fabric/fabric_attrs.cpp"),
            str(tent / "src/transport/mpcomm/mpcomm_peer_registry.cpp"),
            str(lib),
            str(build_dir / "mooncake-common/libasio.so"),
            str(lib.parent / "metrics/libtent_metrics.so"),
            "-Wl,-rpath,"
            + ":".join(map(str, (lib.parent, lib.parent / "metrics", build_dir / "mooncake-common"))),
            "-lglog",
            "-o",
            str(binary),
        ],
        cwd=unit["directory"],
        check=True,
        timeout=240,
    )
    return binary


def exact(sock, length):
    out = bytearray()
    while len(out) < length:
        block = sock.recv(length - len(out))
        if not block:
            raise ValueError("truncated RPC frame")
        out.extend(block)
    return bytes(out)


def frame(sock, layout):
    header = exact(sock, layout.size)
    size, attachment = layout.unpack(header)[-2:]
    if size + attachment > 65536:
        raise ValueError("fixture frame exceeds capture limit")
    return header, exact(sock, size), exact(sock, attachment)


def describe(parts):
    head, body, attachment = parts
    result = {"header_hex": head.hex(), "body_hex": body.hex()}
    if all(32 <= c < 127 for c in attachment):
        result["attachment"] = attachment.decode("ascii")
    else:
        result["attachment_hex"] = attachment.hex()
    return result


@contextmanager
def process(args, **kwargs):
    with subprocess.Popen(args, stdin=subprocess.PIPE, env=clean_environment(), **kwargs) as p:
        try:
            yield p
            p.communicate(b"\n", timeout=TIMEOUT)
            if p.returncode:
                raise RuntimeError(f"{args} exited {p.returncode}")
        finally:
            if p.poll() is None:
                p.kill()


def encoded(record):
    attachment = (
        record["attachment"].encode("ascii")
        if "attachment" in record
        else bytes.fromhex(record["attachment_hex"])
    )
    return bytes.fromhex(record["header_hex"]) + bytes.fromhex(record["body_hex"]) + attachment


def capture(peer, ids, frozen):
    records = {}
    with process([peer, "server"], stdout=subprocess.PIPE) as server:
        if not select.select([server.stdout], [], [], TIMEOUT)[0]:
            raise TimeoutError("native RPC server startup")
        address = ("127.0.0.1", int(server.stdout.readline()))
        with socket.create_server(("127.0.0.1", 0)) as relay:
            relay.settimeout(TIMEOUT)
            for name, number in ids:
                with process([peer, "client", str(number), f"127.0.0.1:{relay.getsockname()[1]}"]):
                    client, _ = relay.accept()
                    with client, socket.create_connection(address, TIMEOUT) as remote:
                        client.settimeout(TIMEOUT)
                        req = frame(client, REQ)
                        remote.sendall(b"".join(req))
                        resp = frame(remote, RESP)
                        client.sendall(b"".join(resp))
                records[name] = {"request": describe(req), "response": describe(resp)}
            # Reverse direction: replay the checked-in bytes over a fresh connection
            # into the native RPC decoder, independent of the native client above.
            for name, record in frozen.items():
                with socket.create_connection(address, TIMEOUT) as remote:
                    remote.sendall(encoded(record["request"]))
                    compare("replay " + name, describe(frame(remote, RESP)), record["response"])
    return records


def compare(name, actual, expected):
    if actual != expected:
        old = json.dumps(expected, indent=2, sort_keys=True).splitlines()
        new = json.dumps(actual, indent=2, sort_keys=True).splitlines()
        raise ValueError("\n".join(difflib.unified_diff(old, new, name, "native output")))


def replay_registered(peer, frozen):
    # The synthetic address in SendData/RecvData is the sole relocation. All
    # other bytes are exactly the checked-in frame received by the native source.
    with process([peer, "registered"], stdout=subprocess.PIPE) as server:
        if not select.select([server.stdout], [], [], TIMEOUT)[0]:
            raise TimeoutError("registered TCP source startup")
        port, address = map(int, server.stdout.readline().split())

        def exchange(name, offset=0):
            req = frozen[name]["request"]
            data = encoded(req)
            if name in ("SendData", "RecvData"):
                start = REQ.size + len(bytes.fromhex(req["body_hex"]))
                data = data[:start] + struct.pack("<Q", address + offset) + data[start + 8 :]
            with socket.create_connection(("127.0.0.1", port), TIMEOUT) as remote:
                remote.sendall(data)
                return frame(remote, RESP)

        if exchange("RecvData")[2] != b"old":
            raise ValueError("initial TCP range differed")
        for name in ("Probe", "Notify", "SendData", "RecvData"):
            compare("registered " + name, describe(exchange(name)), frozen[name]["response"])
        for name in ("SendData", "RecvData"):
            if b"not in registered buffer" not in exchange(name, offset=1)[2]:
                raise ValueError("registered range guard accepted " + name)


def clean_environment():
    with (CASES / "environment.tsv").open() as f:
        names = {r["variable"] for r in csv.DictReader(f, delimiter="\t")}
    return {k: v for k, v in os.environ.items() if k not in names}


def check_precedence(peer, output):
    vectors = json.loads((CASES / "precedence.json").read_text())
    clean = clean_environment()
    for vector in vectors:
        env = clean | vector.get("env", {})
        if "file" in vector:
            file = output / (vector["id"] + ".json")
            file.write_text(json.dumps(vector["file"]))
            env["MC_TENT_CONF"] = str(file)
        elif "inline" in vector:
            env["MC_TENT_CONF"] = json.dumps(vector["inline"])
        elif "raw" in vector:
            env["MC_TENT_CONF"] = vector["raw"]
        result = subprocess.check_output(
            [peer, "config"], input=json.dumps(vector), text=True, env=env, timeout=TIMEOUT
        )
        compare(
            vector["id"], json.loads(result), {"ok": vector.get("ok", True), "values": vector["expect"]}
        )
    print(f"{len(vectors)} native configuration precedence cases matched")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source, build_dir, output = (p.resolve() for p in (args.source, args.build, args.output))
    if output.is_relative_to(REF.parents[1]) or output.is_relative_to(source):
        parser.error("evidence belongs outside both checkouts")
    output.mkdir(parents=True, exist_ok=False)
    revision = json.loads((REF / "contract.json").read_text())["tent_revision"]
    if (
        subprocess.check_output(
            ["git", "-c", "safe.directory=" + str(source), "-C", source, "rev-parse", "HEAD"], text=True
        ).strip()
        != revision
    ):
        raise ValueError("wrong TENT checkout")
    peer = build(source, build_dir, output)
    check_precedence(peer, output)
    formats = json.loads(subprocess.check_output([peer, "formats"], timeout=TIMEOUT))
    for name, encoded in formats.pop("hp_tcp").items():
        if bytes.fromhex(encoded) != (REF / "hp_tcp" / (name + ".bin")).read_bytes():
            raise ValueError(f"HP-TCP fixture mismatch: {name}")
    with (CASES / "rpc.tsv").open() as f:
        ids = [(r["name"], int(r["id"])) for r in csv.DictReader(f, delimiter="\t")]
    frozen = json.loads((HERE / "rpc.json").read_text())
    records = capture(peer, [*ids, ("Unknown", 99)], frozen)
    for name, obj in [("formats", formats), ("rpc", records)]:
        (output / (name + ".json")).write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")
    for name, obj in [("formats", formats), ("rpc", records)]:
        compare(name, obj, json.loads((HERE / (name + ".json")).read_text()))
    replay_registered(peer, frozen)
    print(
        f"{revision}: {len(records)} native loopback exchanges, memory/file metadata, "
        "three HP-TCP frames and provider attributes matched"
    )


if __name__ == "__main__":
    main()
