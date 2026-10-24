#!/usr/bin/env bash
set -euo pipefail

# Use one signed Ubuntu 24.04 source set for update and install. The hosted
# runner's Azure HTTP mirror can keep a slow package download alive past the
# workflow deadline despite Acquire timeouts.
sources="${RUNNER_TEMP:?RUNNER_TEMP must name the CI scratch directory}/orbitte-ubuntu.sources"
sourceparts="$RUNNER_TEMP/orbitte-apt-sourceparts"
mkdir -p "$sourceparts"
cat > "$sources" <<'SOURCES'
Types: deb
URIs: https://archive.ubuntu.com/ubuntu
Suites: noble noble-updates
Components: main universe
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg

Types: deb
URIs: https://security.ubuntu.com/ubuntu
Suites: noble-security
Components: main universe
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg
SOURCES
options=(
    -o "Dir::Etc::sourcelist=$sources"
    -o "Dir::Etc::sourceparts=$sourceparts"
    -o 'Acquire::Retries=3'
    -o 'Acquire::http::Timeout=30'
    -o 'Acquire::https::Timeout=30'
)
sudo apt-get "${options[@]}" update
sudo apt-get "${options[@]}" install -y --no-install-recommends "$@"
