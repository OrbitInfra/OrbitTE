#!/usr/bin/env bash
set -euo pipefail
out=$1
features=${2:-}
mkdir -p "$out"
python -m maturin build --locked --release --interpreter "$(command -v python)" \
  --features "pyo3/extension-module${features:+,$features}" --out "$out"
python -m pip install --force-reinstall "$out"/*.whl
# -I ignores cwd and PYTHONPATH. The test additionally checks site-packages.
python -I tests/python/test_installed.py -v
