"""Run installed same-host SHM consumers and verify the actual backing file."""
import argparse
import ctypes
import json
import os
from pathlib import Path
import selectors
import struct
import subprocess


def mount(path):
    out = subprocess.check_output(["findmnt", "-J", "-T", str(path), "-o",
                                   "TARGET,SOURCE,FSTYPE,OPTIONS,MAJ:MIN"], text=True, timeout=15)
    return json.loads(out)["filesystems"][0]


def resource(directory):
    return [int((directory / name).read_text().strip(), 0) for name in ("resource", "size")]


def cxl_evidence(file, info, region_name):
    region = Path("/sys/bus/cxl/devices") / region_name
    if not region_name.startswith("region") or not region_name[6:].isdecimal():
        raise ValueError("supply a CXL region from /sys/bus/cxl/devices")
    # Linux statx publishes the inode DAX attribute and its support mask.
    buf = ctypes.create_string_buffer(256)
    api = ctypes.CDLL(None, use_errno=True).statx
    api.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_uint, ctypes.c_void_p]
    if api(-100, os.fsencode(file), 0x100, 0x7ff, buf) != 0:
        raise OSError(ctypes.get_errno(), "statx")
    attr = struct.unpack_from("=Q", buf.raw, 8)[0]
    mask = struct.unpack_from("=Q", buf.raw, 56)[0]
    if not attr & mask & 0x200000:
        raise RuntimeError("the mapped inode is not DAX")
    block = (Path("/sys/dev/block") / info["maj:min"]).resolve(strict=True)
    if (block / "partition").exists():
        block = block.parent
    if not block.name.startswith("pmem"):
        raise RuntimeError("this gate requires a directly mounted fsdax pmem block device")
    namespace = (block / "device").resolve(strict=True)
    start, size = resource(namespace)
    cxl_start, cxl_size = resource(region)
    if min(size, cxl_size) <= 0 or not cxl_start <= start < start + size <= cxl_start + cxl_size:
        raise RuntimeError("the mounted pmem extent is not inside the selected CXL region")
    return {"dax": True, "block": str(block), "pmem_resource": [start, size],
            "cxl_region": str(region), "cxl_resource": [cxl_start, cxl_size]}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binary", required=True)
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--posix", action="store_true")
    mode.add_argument("--tmpfs", type=Path, help="ordinary tmpfs directory for file-backed CI")
    mode.add_argument("--cxl", type=Path, help="physical fsdax directory")
    p.add_argument("--region", help="physical CXL region name")
    args = p.parse_args()
    if bool(args.cxl) != bool(args.region):
        p.error("--cxl and --region are required together")
    root = (args.cxl or args.tmpfs or Path("/dev/shm")).resolve(strict=True)
    backing = "posix" if args.posix else str(root)
    source = subprocess.Popen([args.binary, "source", backing], text=True, stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE)
    try:
        with selectors.DefaultSelector() as poll:
            poll.register(source.stdout, selectors.EVENT_READ)
            if not poll.select(45):
                raise TimeoutError("source SHM registration")
        ready = source.stdout.readline().rstrip("\n").split(" ", 5)
        if len(ready) != 6 or ready[0] != "READY":
            raise RuntimeError(f"source bootstrap failed: {ready}")
        file = Path(ready[5]).resolve(strict=True)
        if file.parent != root or not file.name.startswith("orbitte-shm-"):
            raise RuntimeError("source mapping does not belong to the requested mount")
        info = mount(file)
        physical = {}
        if args.cxl:
            physical = cxl_evidence(file, info, args.region)
        elif info["fstype"] != "tmpfs":
            raise RuntimeError("the CI profile needs real tmpfs")
        run = subprocess.run([args.binary, "requester", backing, *ready[1:5]], text=True,
                             capture_output=True, timeout=45, check=True)
        if "PASS requester 1048583" not in run.stdout:
            raise RuntimeError(run.stdout)
        receipt, _ = source.communicate("verify\n", timeout=45)
        if source.returncode or "PASS source 1048583" not in receipt or file.exists():
            raise RuntimeError(f"SHM retirement receipt failed: {receipt}")
        print(json.dumps({"profile": "CXL" if args.cxl else "POSIX" if args.posix else "tmpfs file",
                          "bytes_each_direction": 1048583, "guards": "passed", "name_unlinked": True,
                          "mount": info, **physical}))
    finally:
        if source.poll() is None:
            source.kill()
            source.wait(timeout=10)


if __name__ == "__main__":
    main()
