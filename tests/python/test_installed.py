"""Installed-wheel contract, byte and lifetime controls (no source-tree import)."""
from dataclasses import asdict
import ctypes
import gc
import inspect
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest
import weakref

import orbite as te

BOUND = 30


def exact(sock, size):
    data = bytearray()
    while len(data) < size:
        piece = sock.recv(size - len(data))
        if not piece:
            raise EOFError("truncated relay frame")
        data.extend(piece)
    return data


class ReadRelay:
    """Hold a real TCP reply, after source bytes exist, before requester drain."""
    def __init__(self, endpoint):
        host, port = endpoint.rsplit(":", 1)
        self.target = host, int(port)
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(8)
        self.listener.settimeout(.1)
        self.endpoint = "127.0.0.1:%d" % self.listener.getsockname()[1]
        self.held = threading.Event()
        self.release = threading.Event()
        self.stop = threading.Event()
        self.errors = []
        self.workers = []
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def run(self):
        try:
            while not self.stop.is_set():
                try:
                    client, _ = self.listener.accept()
                except TimeoutError:
                    continue
                assert len(self.workers) < 16, "fixture connection bound"
                worker = threading.Thread(target=self.serve, args=(client,))
                self.workers.append(worker)
                worker.start()
        except Exception as exc:
            self.errors.append(exc)
            self.held.set()

    def serve(self, client):
        try:
            with client, socket.create_connection(self.target, timeout=BOUND) as peer:
                client.settimeout(BOUND)
                request = exact(client, 52)
                peer.sendall(request)
                response = exact(peer, 36)
                n = int.from_bytes(response[-8:], "big")
                payload = exact(peer, n) if n else b""
                if request[10] == 1:
                    self.held.set()
                    if not self.release.wait(BOUND):
                        raise TimeoutError("release never reached")
                client.sendall(response + payload)
        except Exception as exc:
            self.errors.append(exc)
            self.held.set()

    def close(self):
        self.release.set()
        self.stop.set()
        self.thread.join(BOUND)
        self.listener.close()
        for worker in self.workers:
            worker.join(BOUND)
            assert not worker.is_alive()
        assert not self.thread.is_alive(), "relay owner did not stop"
        assert not self.errors, self.errors


class Exporter(bytearray):
    pass


class Contract(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.engines = []
        self.relays = []

    def tearDown(self):
        for r in self.relays:
            r.close()
        for e in reversed(self.engines):
            e.close()
        self.temp.cleanup()

    def engine(self, **updates):
        config = {
            "metadata_type": "p2p", "rpc_server_hostname": "127.0.0.1", "rpc_server_port": 0,
            "transports": {"tcp": {"enable": True}, "hp_tcp": {"enable": False},
                           "rdma": {"enable": False}, "shm": {"enable": False}},
        }
        config.update(updates)
        path = Path(self.temp.name) / f"engine-{len(self.engines)}.json"
        path.write_text(json.dumps(config))
        engine = te.TransferEngine(str(path))
        self.engines.append(engine)
        return engine

    def fixture(self, held=False):
        source, client = self.engine(), self.engine()
        payload = bytes(range(64))
        src = Exporter(b"L" * 8 + payload + b"R" * 8)
        dst = Exporter(b"x" * 80)
        src_addr = source.register_buffer(src)
        dst_addr = client.register_buffer(dst)
        endpoint = source.get_segment_name()
        if held:
            relay = ReadRelay(endpoint)
            self.relays.append(relay)
            endpoint = relay.endpoint
        peer = client.open_segment(endpoint)
        batch = client.allocate_transfer_batch(2)
        req = te.Request(te.READ, dst_addr + 8, peer, src_addr + 8, 64, transport_hint=te.TCP)
        return source, client, src, dst, src_addr, dst_addr, peer, batch, req

    def test_installed_distribution_and_surface(self):
        self.assertNotIn("/python/orbite/", str(Path(te.__file__).resolve()))
        import importlib.metadata
        self.assertEqual(importlib.metadata.version("orbite"), "0.1.0a1")
        for name in ("Notification", "Request", "TransferStatus", "SegmentInfoBuffer", "SegmentInfo", "MemoryOptions", "MemoryGuard", "BatchGuard", "TransferEngine"):
            self.assertTrue(hasattr(te, name), name)
        enum_values = {
            "OpCode": {"READ": 0, "WRITE": 1},
            "TransferStatusEnum": dict(zip(["INITIAL", "PENDING", "INVALID", "CANCELED", "COMPLETED", "TIMEOUT", "FAILED"], range(7))),
            "Permission": {"LocalReadWrite": 0, "GlobalReadOnly": 1, "GlobalReadWrite": 2},
            "TransportType": dict(zip(["UNSPEC", "RDMA", "MNNVL", "SHM", "NVLINK", "GDS", "IOURING", "TCP", "AscendDirect", "SUNRISE_LINK", "TPU", "UB", "MPCOMM", "HP_TCP", "XPU", "HYLINK", "FABRIC", "MLU", "MEMFABRIC"], range(19))),
            "IntentType": dict(zip(["INTENT_UNSPEC", "FOREGROUND_GET", "BACKGROUND_PREFETCH", "MIGRATION", "CHECKPOINT", "WEIGHT_LOADING", "STAGING_INTERNAL"], range(7))),
            "SegmentInfoType": {"Memory": 0, "File": 1},
        }
        for name, rows in enum_values.items():
            cls = getattr(te, name)
            self.assertEqual({k: int(v) for k, v in cls.__members__.items()}, rows)
            for key in rows:
                self.assertIs(getattr(te, key), getattr(cls, key))
        self.assertEqual((te.LOCAL_SEGMENT_ID, te.kWildcardLocation, te.PRIO_HIGH, te.PRIO_MEDIUM, te.PRIO_LOW), (0, "*", 0, 1, 2))
        request = te.Request()
        self.assertEqual(asdict(request), dict(opcode=te.READ, source=0, target_id=0, target_offset=0, length=0, priority=0, transport_hint=te.UNSPEC, policy_name=None, deadline_ns=0, intent_type=te.INTENT_UNSPEC))
        self.assertEqual(asdict(te.MemoryOptions()), dict(location="*", perm=te.GlobalReadWrite, type=te.UNSPEC, shm_path="", shm_offset=0, internal=False))
        # Every frozen property is readable and writable; independent defaults.
        for value in (request, te.MemoryOptions(), te.Notification(), te.TransferStatus(), te.SegmentInfoBuffer(), te.SegmentInfo()):
            for key, old in ((name, getattr(value, name)) for name in value._fields):
                setattr(value, key, old)
                self.assertEqual(getattr(value, key), old)
        with self.assertRaises(OverflowError):
            request.length = -1
        with self.assertRaises(TypeError):
            request.policy_name = 42
        with self.assertRaises(AttributeError):
            request.typo = 1
        a, b = te.SegmentInfo(), te.SegmentInfo()
        a.buffers.append(te.SegmentInfoBuffer(1, 2, "cpu:0"))
        self.assertEqual(b.buffers, [])
        self.assertEqual(inspect.signature(te.TransferEngine.register_local_memory).parameters["permission"].default, te.GlobalReadWrite)

    def test_inventory_rows(self):
        import csv
        path = Path(__file__).resolve().parents[1] / "reference/inventory/python.tsv"
        with path.open() as f:
            rows = list(csv.DictReader(f, delimiter="\t"))
        self.assertEqual((len(rows), sum(int(r["overloads"]) for r in rows)), (128, 132))
        for row in rows:
            cls = te if row["surface"] == "module" else getattr(te, row["surface"])
            value = cls() if row["kind"] == "property" else cls
            self.assertTrue(hasattr(value, row["member"]), row)
            test = row["verification"].rsplit("::", 1)[-1]
            self.assertTrue(hasattr(Contract, test), row)

    def test_policy_intent_deadline_and_permissions_reach_owner(self):
        e = self.engine(policy=[
            {"name": "background", "segment_type": "memory", "intent_type": "background_prefetch", "transports": ["tcp"]},
            {"name": "no_route", "segment_type": "memory", "transports": ["nvlink"]},
        ])
        data = bytearray(b"!" * 32)
        data[2:6] = b"data"
        address = e.register_buffer(data)
        batch = e.allocate_transfer_batch(4)
        request = te.Request(te.READ, address + 16, 0, address + 2, 4)
        for name, intent in (("unknown", te.INTENT_UNSPEC), (None, te.INTENT_UNSPEC), ("no_route", te.BACKGROUND_PREFETCH)):
            request.policy_name, request.intent_type = name, intent
            with self.assertRaises(te.TentException):
                e.submit_transfer(batch, [request])
        self.assertEqual(e.get_transfer_status_list(batch), [])
        request.policy_name, request.intent_type, request.deadline_ns = "background", te.BACKGROUND_PREFETCH, 1
        try:
            e.submit_transfer(batch, [request])
        except te.TentException:
            pass
        else:
            self.assertIn(e.wait(batch, BOUND).state, (te.TIMEOUT, te.CANCELED))
        self.assertEqual(data[16:], b"!" * 16)
        e.free_transfer_batch(batch)
        batch = e.allocate_transfer_batch(1)
        request.deadline_ns = time.monotonic_ns() + 10_000_000_000
        e.submit_transfer(batch, [request])
        self.assertEqual(e.wait(batch, BOUND).state, te.COMPLETED)
        self.assertEqual(data, b"!!data" + b"!" * 10 + b"data" + b"!" * 12)
        e.free_transfer_batch(batch)
        e.unregister_local_memory(address)
        e.register_local_memory(address, len(data), te.GlobalReadOnly, owner=data)
        batch = e.allocate_transfer_batch(1)
        request.opcode = te.WRITE
        with self.assertRaises(te.InvalidArgumentError):
            e.submit_transfer(batch, [request])
        self.assertEqual(data[2:6], b"data")
        e.free_transfer_batch(batch)

    def test_two_process_tcp_installed_wheel(self):
        import subprocess, sys
        source_code = r"""
import json, sys, orbite as te
engine = te.TransferEngine(sys.argv[1])
memory = bytearray(b"LLsourceRR")
address = engine.register_buffer(memory)
print(json.dumps([engine.get_segment_name(), address]), flush=True)
assert sys.stdin.readline().strip() == "done"
assert memory == b"LLtargetRR", memory
engine.unregister_local_memory(address)
engine.close()
"""
        requester = self.engine()
        cfg = str(Path(self.temp.name) / "engine-0.json")
        with subprocess.Popen([sys.executable, "-I", "-c", source_code, cfg], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as child:
            try:
                import select
                self.assertTrue(select.select([child.stdout], [], [], BOUND)[0])
                endpoint, remote = json.loads(child.stdout.readline())
                memory = bytearray(b"xx------yy")
                local = requester.register_buffer(memory)
                peer = requester.open_segment(endpoint)
                with requester.allocate_batch_guard(1) as batch:
                    requester.submit_transfer(batch, [te.Request(te.READ, local + 2, peer, remote + 2, 6, transport_hint=te.TCP)])
                    requester.wait(batch, BOUND)
                self.assertEqual(memory, b"xxsourceyy")
                memory[2:8] = b"target"
                with requester.allocate_batch_guard(1) as batch:
                    requester.submit_transfer(batch, [te.Request(te.WRITE, local + 2, peer, remote + 2, 6, transport_hint=te.TCP)])
                    requester.wait(batch, BOUND)
                out, err = child.communicate("done\n", timeout=BOUND)
                self.assertEqual(child.returncode, 0, (out, err))
                self.assertEqual(memory, b"xxtargetyy")
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait()

    def test_real_tcp_read_write_guards_status_and_notifications(self):
        source, client, src, dst, sa, da, peer, batch, req = self.fixture()
        self.assertTrue(client.available())
        self.assertEqual(client.get_segment_name(), f"{client.get_rpc_server_address()}:{client.get_rpc_server_port()}")
        self.assertIsInstance(json.loads(client.get_local_topology()), dict)
        info = client.get_segment_info(peer)
        self.assertEqual(info.type, te.Memory)
        self.assertIn(sa, [b.base for b in info.buffers])
        client.submit_transfer(batch, [req], te.Notification("ready", "read"))
        self.assertEqual(client.wait(batch, BOUND), te.TransferStatus(te.COMPLETED, 64))
        self.assertEqual(dst, b"x" * 8 + bytes(range(64)) + b"x" * 8)
        self.assertEqual(src, b"L" * 8 + bytes(range(64)) + b"R" * 8)
        self.assertEqual(client.get_transfer_status(batch, 0).state, te.COMPLETED)
        self.assertEqual(client.get_transfer_status_list(batch), [te.TransferStatus(te.COMPLETED, 64)])
        self.assertEqual(client.get_transfer_status_overall(batch).state, te.COMPLETED)
        self.assertEqual(client.progress_batch(batch).state, te.COMPLETED)
        self.assertEqual(client.terminal_result(batch), (0, True))
        self.assertEqual(source.recv_notifi(), [te.Notification("ready", "read")])
        client.free_transfer_batch(batch)
        # The source/destination roles reverse for WRITE; both byte guards survive.
        dst[8:72] = bytes(reversed(range(64)))
        req.opcode = te.WRITE
        batch = client.allocate_transfer_batch(1)
        client.submit_transfer_notif(batch, [req], "ready", "write")
        self.assertEqual(client.wait(batch, BOUND).bytes, 64)
        self.assertEqual(src, b"L" * 8 + bytes(reversed(range(64))) + b"R" * 8)
        self.assertEqual(source.recv_notifi(), [te.Notification("ready", "write")])
        client.send_notifi(peer, te.Notification("direct", "\0unicode λ"))
        self.assertEqual(source.recv_notifi(), [te.Notification("direct", "\0unicode λ")])
        client.send_notifi(te.LOCAL_SEGMENT_ID, te.Notification("self", "ok"))
        self.assertEqual(client.recv_notifi(), [te.Notification("self", "ok")])
        client.free_transfer_batch(batch)
        client.close_segment(peer)
        with self.assertRaises(te.InvalidArgumentError):
            client.get_segment_info(peer)
        self.assertNotEqual(client.open_segment(source.get_segment_name()), peer)
        client.unregister_local_memory(da)
        source.unregister_local_memory(sa, len(src))

    def test_registration_options_lists_exceptions_and_limits(self):
        e = self.engine()
        a, b = e.allocate_local_memory(64), e.allocate_local_memory_ex(64, te.MemoryOptions())
        e.register_local_memory_batch([a, b], [64, 64], te.GlobalReadOnly)
        self.assertEqual(len(e.get_segment_info(0).buffers), 2)
        e.unregister_local_memory_batch([a, b])
        e.register_local_memory_batch([], [])
        e.unregister_local_memory_batch([])
        e.register_local_memory_batch_ex([a, b], [64, 64], te.MemoryOptions(perm=te.GlobalReadWrite))
        e.unregister_local_memory_batch([a, b], [64, 64])
        e.free_local_memory(a)
        e.free_local_memory(b)
        with self.assertRaises(te.InvalidArgumentError):
            e.allocate_local_memory(0)
        with self.assertRaises(te.AddressNotRegisteredError):
            e.register_local_memory(1, 8)
        with self.assertRaises(te.InvalidArgumentError):
            e.register_local_memory_batch([1], [])
        for method, args in ((e.export_local_segment, ()), (e.import_remote_segment, ("opaque",))):
            with self.assertRaises(te.NotImplementedError):
                method(*args)
        for transport in (te.UB, te.AscendDirect, te.XPU, te.MLU, te.MEMFABRIC):
            with self.assertRaises(te.NotImplementedError):
                e.allocate_local_memory_ex(8, te.MemoryOptions(type=transport))
        for name in ("InvalidArgumentError", "AddressNotRegisteredError", "DeviceNotFoundError", "InvalidEntryError", "RdmaError", "CudaError", "MetadataError", "RpcServiceError", "InternalError", "NotImplementedError"):
            self.assertTrue(issubclass(getattr(te, name), te.TentException))
        with self.assertRaises((te.InvalidArgumentError, BufferError, TypeError)):
            e.register_buffer(b"immutable")
        view = bytearray(32)
        addr = ctypes.addressof(ctypes.c_char.from_buffer(view))
        e.register_local_memory(addr + 4, 8, owner=view)
        with self.assertRaises(BufferError):
            view.extend(b"resize")
        with self.assertRaises(te.InvalidArgumentError):
            e.register_local_memory(addr + 30, 8, owner=view)
        e.unregister_local_memory(addr + 4)
        view.extend(b"resize")

    def test_frozen_context_manager_exits_and_engine_keepalive(self):
        e = self.engine()
        for allocate in (lambda: e.allocate_memory_guard(4096), lambda: e.allocate_memory_guard_ex(4096, te.MemoryOptions()), lambda: e.allocate_batch_guard(1)):
            with allocate() as value:
                self.assertGreater(value, 0)
            expected = ValueError("exception from with body")
            with self.assertRaises(ValueError) as raised:
                with allocate():
                    raise expected
            self.assertIs(raised.exception, expected)
        guard = e.allocate_memory_guard(16)
        self.assertEqual(guard.size(), 16)
        self.assertGreater(guard.address(), 0)
        g = e.allocate_batch_guard(1)
        g.release()
        g.release()
        with self.assertRaises(te.InvalidArgumentError):
            e.get_transfer_status_overall(g.id())
        ref = weakref.ref(e)
        self.engines.remove(e)
        del e
        gc.collect()
        self.assertIsNotNone(ref())
        del guard, g
        gc.collect()
        self.assertIsNone(ref())

    def test_p2p_config_migration_file_and_environment(self):
        for cfg in ({"metadata_type": "etcd"}, {"metadata_servers": "https://secret-token.example"}):
            with self.assertRaises(te.TentException) as error:
                self.engine(**cfg)
            message = str(error.exception)
            self.assertIn("publish endpoint() and source_epoch()", message)
            self.assertNotIn("secret-token", message)
        # Separate interpreter: never mutate the engine's process environment.
        import subprocess, sys
        env = dict(os.environ, MC_TENT_CONF='{"metadata_type":"redis"}')
        out = subprocess.run([sys.executable, "-I", "-c", "import orbite; orbite.TransferEngine()"], env=env, text=True, capture_output=True, timeout=BOUND)
        self.assertNotEqual(out.returncode, 0)
        self.assertIn("publish endpoint() and source_epoch()", out.stderr)
        config = {"rpc_server_hostname": "127.0.0.1", "rpc_server_port": 0, "transports/rdma/enable": False}
        env = dict(os.environ, MC_TENT_CONF=json.dumps(config))
        subprocess.run([sys.executable, "-I", "-c", "import orbite; e=orbite.TransferEngine(); assert e.available(); e.close(); assert not e.available(); e.close()"], env=env, check=True, timeout=BOUND)

    def test_gil_release_during_blocking_wait(self):
        _, client, _, dst, _, _, _, batch, req = self.fixture(held=True)
        client.submit_transfer(batch, [req])
        relay = self.relays[-1]
        self.assertTrue(relay.held.wait(BOUND))
        entered, finished = threading.Event(), threading.Event()
        outcomes = []
        def wait():
            entered.set()
            try:
                outcomes.append(client.wait(batch, 5))
            except Exception as error:
                outcomes.append(error)
            finally:
                finished.set()
        thread = threading.Thread(target=wait)
        thread.start()
        self.assertTrue(entered.wait(BOUND))
        time.sleep(.05)
        # A Python loop and cancellation/status calls run while native wait blocks.
        self.assertEqual(sum(range(10000)), 49995000)
        self.assertFalse(finished.is_set(), outcomes)
        self.assertEqual(client.get_transfer_status(batch, 0).state, te.PENDING)
        relay.release.set()
        thread.join(BOUND)
        self.assertFalse(thread.is_alive())
        self.assertEqual(outcomes, [te.TransferStatus(te.COMPLETED, 64)])
        self.assertEqual(dst, b"x" * 8 + bytes(range(64)) + b"x" * 8)
        client.free_transfer_batch(batch)

    def test_timeout_cancel_and_early_drop_retain_exporter_until_drain(self):
        _, client, _, dst, sa, da, peer, batch, req = self.fixture(held=True)
        ref = weakref.ref(dst)
        client.submit_transfer(batch, [req])
        relay = self.relays[-1]
        self.assertTrue(relay.held.wait(BOUND))
        with self.assertRaises(te.RpcServiceError):
            client.wait(batch, .001)
        with self.assertRaises(te.NotImplementedError):
            client.cancel_transfer(batch, 0)  # TCP cannot cancel posted I/O
        client.submit_transfer(batch, [te.Request(te.READ, da, peer, sa, 8, transport_hint=te.TCP)])
        client.cancel_transfer(batch, 1)  # queued work has no payload owner yet
        with self.assertRaises(te.InternalError):
            client.unregister_local_memory(da)
        del dst
        gc.collect()
        self.assertIsNotNone(ref())
        guard = te.BatchGuard(client, batch)
        del guard
        gc.collect()
        self.assertIsNotNone(ref())
        relay.release.set()
        until = time.monotonic() + BOUND
        while True:
            try:
                client.unregister_local_memory(da)
                break
            except te.InternalError:
                if time.monotonic() > until:
                    raise
                time.sleep(.001)
        gc.collect()
        self.assertIsNone(ref())

    def test_queued_cancel_status_and_destination_guards(self):
        _, client, _, dst, sa, da, peer, batch, req = self.fixture(held=True)
        client.submit_transfer(batch, [req])
        relay = self.relays[-1]
        self.assertTrue(relay.held.wait(BOUND))
        client.submit_transfer(batch, [te.Request(te.READ, da, peer, sa, 8, transport_hint=te.TCP)])
        client.cancel_transfer(batch, 1)
        relay.release.set()
        client.wait(batch, BOUND)
        self.assertEqual([s.state for s in client.get_transfer_status_list(batch)], [te.COMPLETED, te.CANCELED])
        self.assertEqual(dst, b"x" * 8 + bytes(range(64)) + b"x" * 8)
        client.free_transfer_batch(batch)

    def test_memory_guard_early_drop_preserves_pending_destination(self):
        _, client, _, _, sa, _, peer, _, _ = self.fixture(held=True)
        guard = client.allocate_memory_guard(80)
        addr = guard.address()
        ctypes.memset(addr, 0x71, 80)
        client.register_local_memory(addr, 80)
        batch = client.allocate_transfer_batch(1)
        client.submit_transfer(batch, [te.Request(te.READ, addr + 8, peer, sa + 8, 64, transport_hint=te.TCP)])
        relay = self.relays[-1]
        self.assertTrue(relay.held.wait(BOUND))
        del guard
        gc.collect()
        self.assertEqual(ctypes.string_at(addr, 80), b"q" * 80)
        relay.release.set()
        client.wait(batch, BOUND)
        self.assertEqual(ctypes.string_at(addr, 80), b"q" * 8 + bytes(range(64)) + b"q" * 8)
        client.free_transfer_batch(batch)
        client.unregister_local_memory(addr)
        with self.assertRaises(te.InvalidArgumentError):
            client.free_local_memory(addr)  # deferred guard free already drained


if __name__ == "__main__":
    unittest.main()
