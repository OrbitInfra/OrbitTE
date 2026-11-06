"""Hold a real HP-TCP READ response after its source has produced all the bytes."""

import json
import select
import signal
import socket
import struct
import subprocess
import threading


def line(process):
    if not select.select([process.stdout], [], [], 30)[0]:
        raise TimeoutError("client did not reach its checkpoint")
    return process.stdout.readline().strip()


def read(sock, count):
    out = bytearray()
    while len(out) < count:
        piece = sock.recv(count - len(out))
        if not piece:
            raise EOFError("truncated frame")
        out.extend(piece)
    return out


def scatter(binary, settings, env, hp):
    cfg = dict(settings)
    cfg.update(
        {
            "transports/hp_tcp/enable": hp,
            "transports/tcp/enable": not hp,
            "transports/hp_tcp/connections_per_peer": 1,
        }
    )
    if hp:
        cfg.update(
            {
                "enable_runtime_queue": True,
                "runtime_queue/max_outstanding_bytes": 4,
                "runtime_queue/max_dispatch_bytes": 4,
            }
        )
    env = dict(env, MC_TENT_CONF=json.dumps(cfg))
    with subprocess.Popen(
        [binary, "source", "classic"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        env=env,
    ) as source:
        try:
            name = line(source)
            if not hp:
                subprocess.run(
                    [binary, "no-scatter", name], env=env, check=True, timeout=45
                )
                source.communicate("untouched\n", timeout=15)
                assert source.returncode == 0
                return
            host, port = name.rsplit(":", 1)
            with socket.socket() as control, socket.socket() as lane:
                for s in (control, lane):
                    s.bind(("127.0.0.1", 0))
                    s.listen(4)
                    s.settimeout(0.25)
                stop = threading.Event()
                held = threading.Event()
                release = threading.Event()
                endpoint = []
                errors = []

                def describe():
                    while not stop.is_set():
                        try:
                            c, _ = control.accept()
                        except TimeoutError:
                            continue
                        with (
                            c,
                            socket.create_connection(
                                (host, int(port)), timeout=15
                            ) as peer,
                        ):
                            c.settimeout(15)
                            header = read(c, 52)
                            assert header[10] == 0
                            peer.sendall(header)
                            reply = read(peer, 36)
                            c.sendall(reply)
                            n = int.from_bytes(reply[-8:], "big")
                            if n:
                                data = read(peer, n)
                                assert data[-14:-12] == bytes([1, 0])
                                endpoint[:] = [
                                    (
                                        socket.inet_ntoa(data[-4:]),
                                        int.from_bytes(data[-16:-14], "big"),
                                    )
                                ]
                                data[-16:-14] = struct.pack(">H", lane.getsockname()[1])
                                c.sendall(data)

                def payload():
                    first = True
                    while not stop.is_set():
                        try:
                            c, _ = lane.accept()
                        except TimeoutError:
                            continue
                        with (
                            c,
                            socket.create_connection(endpoint[0], timeout=15) as peer,
                        ):
                            c.settimeout(15)
                            if first:
                                header = read(c, 48)
                                assert header[:4] == b"MCTP"
                                peer.sendall(header)
                                response = read(peer, 32 + 6)
                                assert response[-6:] == b"source"
                                held.set()
                                assert release.wait(30)
                                c.sendall(response)
                                first = False
                            while not stop.is_set():
                                sockets = select.select([c, peer], [], [], 0.25)[0]
                                closed = False
                                for origin in sockets:
                                    chunk = origin.recv(65536)
                                    if not chunk:
                                        closed = True
                                        break
                                    (peer if origin is c else c).sendall(chunk)
                                if closed:
                                    break

                def guarded(fn):
                    try:
                        fn()
                    except Exception as ex:
                        errors.append(repr(ex))
                        stop.set()
                        held.set()

                threads = [
                    threading.Thread(target=guarded, args=(fn,), daemon=True)
                    for fn in (describe, payload)
                ]
                for t in threads:
                    t.start()
                try:
                    with subprocess.Popen(
                        [binary, "scatter", f"127.0.0.1:{control.getsockname()[1]}"],
                        stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        text=True,
                        env=env,
                    ) as requester:
                        try:
                            assert line(requester) == "submitted"
                            assert held.wait(30) and not errors, errors
                            requester.stdin.write("pending\n")
                            requester.stdin.flush()
                            assert line(requester) == "held"
                            release.set()
                            assert requester.wait(timeout=45) == 0
                        finally:
                            if requester.poll() is None:
                                requester.kill()
                finally:
                    stop.set()
                    release.set()
                    for t in threads:
                        t.join(15)
                assert not errors and not any(t.is_alive() for t in threads), errors
            source.communicate("scatter\n", timeout=15)
            assert source.returncode == 0
        finally:
            if source.poll() is None:
                source.kill()


def shutdown(binary, env):
    with subprocess.Popen(
        [binary, "signal"], stdout=subprocess.PIPE, text=True, env=env
    ) as child:
        try:
            assert line(child) == "ready"
            child.send_signal(signal.SIGTERM)
            assert child.wait(timeout=30) == 128 + signal.SIGTERM
        finally:
            if child.poll() is None:
                child.kill()
