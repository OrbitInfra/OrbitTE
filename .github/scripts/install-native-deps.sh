#!/usr/bin/env bash
set -euo pipefail

# Use one signed Ubuntu 24.04 source set for update and install. The hosted
# runner's Azure HTTP mirror can keep a slow package download alive past the
# workflow deadline despite Acquire timeouts.
sources="${RUNNER_TEMP:?RUNNER_TEMP must name the CI scratch directory}/orbite-ubuntu.sources"
sourceparts="$RUNNER_TEMP/orbite-apt-sourceparts"
mkdir -p "$sourceparts"
case "$(dpkg --print-architecture)" in
    amd64)
        archive_uri=https://archive.ubuntu.com/ubuntu
        security_uri=https://security.ubuntu.com/ubuntu
        ;;
    arm64)
        archive_uri=https://ports.ubuntu.com/ubuntu-ports
        security_uri=$archive_uri
        ;;
    *) echo 'unsupported CI package architecture' >&2; exit 1 ;;
esac
cat > "$sources" <<SOURCES
Types: deb
URIs: $archive_uri
Suites: noble noble-updates
Components: main universe
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg

Types: deb
URIs: $security_uri
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
