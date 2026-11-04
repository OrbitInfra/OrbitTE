"""Run the packaged CUDA source with a CPU-only requester on the same Linux host."""
import argparse
import os
import select
import subprocess
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--source", type=Path, required=True)
parser.add_argument("--requester", type=Path, required=True)
parser.add_argument("--gpu", required=True, help="GPU UUID for the source process")
args = parser.parse_args()
source = subprocess.Popen([args.source], env={**os.environ, "CUDA_VISIBLE_DEVICES": args.gpu},
                          stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
try:
    if not select.select([source.stdout], [], [], 30)[0]:
        raise TimeoutError("waiting for the CUDA source")
    endpoint, epoch = source.stdout.readline().split()
    subprocess.run([args.requester, endpoint, epoch], check=True, timeout=65,
                   env={**os.environ, "CUDA_VISIBLE_DEVICES": ""})
    output, _ = source.communicate(input="done\n", timeout=35)
    if source.returncode or output.strip() != "gpu guards and WRITE verified":
        raise RuntimeError((source.returncode, output))
    print(f"CPU requester, CUDA source {args.gpu}, TCP READ/WRITE and guards passed")
finally:
    if source.poll() is None:
        source.kill()
    source.wait(timeout=5)
