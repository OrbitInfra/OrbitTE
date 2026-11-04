"""Run the packaged source-control gate on one identified GPU."""
import argparse
import os
import subprocess
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--gpu", required=True, help="physical GPU UUID")
    parser.add_argument("--rdma", nargs=2, metavar=("DEVICE", "GID_INDEX"),
                        help="run the RDMA loopback variant on this RNIC and GID")
    args = parser.parse_args()
    identity = subprocess.check_output(
        ["nvidia-smi", "-i", args.gpu, "--query-gpu=uuid,pci.bus_id,name,driver_version",
         "--format=csv,noheader"], text=True, timeout=15).strip()
    if identity.split(",", 1)[0] != args.gpu:
        raise ValueError("GPU UUID does not match the requested device")
    print(identity, flush=True)
    route = ["--rdma", *args.rdma] if args.rdma else []
    subprocess.run([args.binary.resolve(), *route], check=True, timeout=100,
                   env={**os.environ, "CUDA_VISIBLE_DEVICES": args.gpu})


if __name__ == "__main__":
    main()
