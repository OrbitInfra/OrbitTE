#!/usr/bin/env bash
set -euo pipefail
out=$1
mkdir -p "$out"
cc -std=c11 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Werror -fPIC -shared -pthread \
  tests/python/cuda_shim.c -o "$out/libcuda.so.1"
export ORBITE_PYTHON_CUDA_SHIM="$out/libcuda.so.1"
export LD_LIBRARY_PATH="$out${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
python -I tests/python/test_cuda_exporters.py -v
for i in $(seq 2 "${2:-1}"); do
  python -I tests/python/test_cuda_exporters.py > "$out/$i.log" 2>&1 || { cat "$out/$i.log"; exit 1; }
done
echo "7 installed CUDA-export controls x ${2:-1} iterations passed (CPU shim, no GPU qualification)"
