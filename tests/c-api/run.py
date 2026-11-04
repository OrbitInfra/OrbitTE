"""Compile and run a downstream C program using only an installed libtent prefix."""

import argparse
import csv
from pathlib import Path
import os
import re
import socket
import shlex
import struct
import subprocess
import tempfile
from threading import Thread

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
INVENTORY = ROOT / "tests/reference/inventory"


def exact(stream, count):
    out = bytearray()
    while len(out) != count:
        chunk = stream.recv(count - len(out))
        if not chunk:
            raise EOFError("truncated OrbitTE control frame")
        out.extend(chunk)
    return out


def relay(listener, peer, effects, gate=None, truncate=False):
    # Test-only relay. It forwards the actual Describe(s), ready reply and body.
    # Seeing the source's final OK proves one enqueue before dropping that reply.
    try:
        while True:
            client, _ = listener.accept()
            with client, socket.create_connection(peer, timeout=15) as server:
                client.settimeout(15)
                header = exact(client, 52)
                server.sendall(header)
                reply = exact(server, 36)
                if header[10] == 1 and gate:
                    assert reply[10] == 0
                    body = exact(server, int.from_bytes(reply[28:36], "big"))
                    os.write(gate[0], b"!")
                    assert os.read(gate[1], 1) == b"!"
                    client.sendall(reply + (body[:2] if truncate else body))
                    effects.append("read")
                    return
                client.sendall(reply)
                if header[10] == 0:
                    client.sendall(exact(server, int.from_bytes(reply[28:36], "big")))
                    continue
                if header[10] == 2:
                    n = int.from_bytes(header[44:52], "big")
                    assert n == 2 and reply[10] == 9
                    server.sendall(exact(client, n))
                    assert exact(server, 36)[10] == 0
                    effects.append("write committed")
                    continue  # close the requester socket without forwarding the ACK
                assert header[10] in (3, 22) and reply[10] == 9
                name, msg = struct.unpack(">QQ", header[36:52])
                assert name + msg <= 65536
                server.sendall(
                    exact(client, name + msg + 1)
                )  # includes the body commit byte
                assert exact(server, 36)[10] == 0
                effects.append("committed")
                return
    except Exception as e:
        effects.append(str(e))


def cases(exe, tmp, env, features):
    constructor = tmp / "constructor.json"
    constructor.write_text('{"rpc_server_hostname":"127.0.0.3"}')
    file = tmp / "test.bin"
    file.write_bytes(b"!123456789!")
    subprocess.run(
        [exe, "local", constructor, file.as_uri()], check=True, timeout=60, env=env
    )
    shm_env = dict(
        env,
        MC_TENT_CONF='{"transports/shm/enable":true,"transports/rdma/enable":false,"transports/nvlink/enable":false,"metrics/enabled":false}',
    )
    shm_env.pop("MC_FORCE_TCP", None)
    subprocess.run([exe, "shm"], check=True, timeout=30, env=shm_env)
    if "io-uring" in features:
        subprocess.run([exe, "file", file.as_uri()], check=True, timeout=30, env=env)
        assert file.read_bytes() == b"!1FILE6789!"
    with subprocess.Popen(
        [exe, "source"],
        text=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        env=env,
    ) as source:
        try:
            name = source.stdout.readline().strip()
            host, port = name.rsplit(":", 1)
            for truncate in (False, True):
                with socket.socket() as paused:
                    paused.bind(("127.0.0.1", 0))
                    paused.listen(1)
                    paused.settimeout(30)
                    entered, signal = os.pipe()
                    resume, release = os.pipe()
                    try:
                        observed = []
                        thread = Thread(
                            target=relay,
                            args=(
                                paused,
                                (host, int(port)),
                                observed,
                                (signal, resume),
                                truncate,
                            ),
                            daemon=True,
                        )
                        thread.start()
                        subprocess.run(
                            [
                                exe,
                                "cut-read" if truncate else "held",
                                f"127.0.0.1:{paused.getsockname()[1]}",
                                str(entered),
                                str(release),
                            ],
                            pass_fds=(entered, release),
                            check=True,
                            timeout=45,
                            env=env,
                        )
                        thread.join(30)
                        assert observed == ["read"], observed
                    finally:
                        for fd in (entered, signal, resume, release):
                            os.close(fd)
            with socket.socket() as listener, socket.socket() as refused:
                listener.bind(("127.0.0.1", 0))
                listener.listen(2)
                listener.settimeout(30)
                refused.bind(("127.0.0.1", 0))  # held, never listened on
                effects = []
                reader = Thread(
                    target=relay,
                    args=(listener, (host, int(port)), effects),
                    daemon=True,
                )
                reader.start()
                subprocess.run(
                    [
                        exe,
                        "transfer",
                        name,
                        f"127.0.0.1:{listener.getsockname()[1]}",
                        f"127.0.0.1:{refused.getsockname()[1]}",
                    ],
                    check=True,
                    timeout=60,
                    env=env,
                )
                reader.join(30)
                assert effects == ["write committed", "committed"], effects
            source.communicate("v\n", timeout=30)
            assert source.returncode == 0
        finally:
            if source.poll() is None:
                source.kill()


def restart(exe, env):
    def start(child_env):
        return subprocess.Popen(
            [exe, "source"],
            env=child_env,
            text=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )

    a = start(env)
    probe = None
    b = None
    try:
        name = a.stdout.readline().strip()
        probe = subprocess.Popen(
            [exe, "epoch", name],
            env=env,
            text=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        assert probe.stdout.readline().strip() == "pinned"
        a.communicate("q\n", timeout=30)
        assert a.returncode == 0
        port = int(name.rsplit(":", 1)[1])
        new_env = dict(
            env, MC_TENT_CONF='{"metrics/enabled":false,"rpc_server_port":%d}' % port
        )
        b = start(new_env)
        assert b.stdout.readline().strip() == name
        probe.communicate("r\n", timeout=30)
        assert probe.returncode == 0
        b.communicate("q\n", timeout=30)
        assert b.returncode == 0
    finally:
        for child in (a, b, probe):
            if child is not None and child.poll() is None:
                child.kill()
                child.wait(timeout=10)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--prefix", type=Path, required=True)
    p.add_argument("--features", default="")
    p.add_argument("--repeat", type=int, default=1)
    args = p.parse_args()
    prefix = args.prefix.resolve()
    lib = prefix / "lib"
    cc = os.environ.get("CC", "cc")
    flags = [
        cc,
        "-std=c11",
        "-Wall",
        "-Wextra",
        "-Werror",
        "-I",
        str(prefix / "include"),
    ]
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        with (INVENTORY / "c-api.tsv").open() as f:
            symbols = {r["symbol"] for r in csv.DictReader(f, delimiter="\t")}
        assert len(symbols) == 33
        symbols.add("tent_terminal_result")  # fork extension outside the frozen inventory
        exercised = set(
            re.findall(r"\b(tent_\w+)\s*\(", (HERE / "consumer.c").read_text())
        )
        assert exercised == symbols, (symbols - exercised, exercised - symbols)
        nm = subprocess.check_output(
            ["nm", "-D", "--defined-only", lib / "libtent.so"], text=True
        )
        assert {
            s.split()[-1] for s in nm.splitlines() if s.split()[-1].startswith("tent_")
        } == symbols
        subprocess.run(
            [*flags, "-DORBITTE_C_API_ONLY", INVENTORY / "abi.c", "-o", tmp / "layout"],
            check=True,
        )
        observed = subprocess.check_output([tmp / "layout"], text=True).splitlines()
        expected = [
            s
            for s in (INVENTORY / "abi.lp64").read_text().splitlines()
            if s.split("\t")[0] not in {"location_t", "device_plugin_t", "plugin"}
        ]
        assert observed == expected, (observed, expected)
        env = dict(os.environ)
        env.update(
            MC_TENT_CONF='{"metrics":{"enabled":false},"rpc_server_port":0}',
            MC_FORCE_TCP="0",
            MOONCAKE_LOCAL_HOSTNAME="127.0.0.2",
        )
        assert 1 <= args.repeat <= 200
        pkg_env = dict(os.environ, PKG_CONFIG_PATH=str(lib / "pkgconfig"))
        priv = shlex.split(
            subprocess.check_output(
                ["pkg-config", "--static", "--libs-only-other", "tent"],
                env=pkg_env,
                text=True,
            )
        )
        libs = shlex.split(
            subprocess.check_output(
                ["pkg-config", "--static", "--libs-only-l", "tent"],
                env=pkg_env,
                text=True,
            )
        )
        for mode in ["shared", "static"]:
            exe = tmp / ("consumer-" + mode)
            link = (
                ["-L", str(lib), "-Wl,-rpath," + str(lib), "-ltent"]
                if mode == "shared"
                else [lib / "libtent.a", *[v for v in libs if v != "-ltent"], *priv]
            )
            subprocess.run(
                [*flags, HERE / "consumer.c", "-o", exe, *link, "-pthread"], check=True
            )
            fixture_env = env.copy()
            if mode == "static":
                file = tmp / "engine.json"
                file.write_text(env["MC_TENT_CONF"])
                fixture_env["MC_TENT_CONF"] = str(file)
            for _ in range(args.repeat):
                cases(exe, tmp, fixture_env, args.features.split(","))
                restart(exe, fixture_env)
    print(
        "33 installed C exports, LP64 layout, both link modes, two-process TCP and lost-ack controls passed"
    )


if __name__ == "__main__":
    main()
