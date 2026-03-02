#!/usr/bin/env bash
set -euo pipefail

# Deploy scooper-v2 to a remote host running Amazon Linux 2023 (glibc 2.34).
#
# Why cargo-zigbuild?
#   Building on NixOS with plain `cargo build` produces binaries that reference
#   the Nix store's dynamic linker (/nix/store/.../ld-linux-x86-64.so.2).
#   These won't run on non-NixOS systems. cargo-zigbuild cross-compiles against
#   a specific glibc version, producing a portable binary with the standard
#   /lib64/ld-linux-x86-64.so.2 interpreter.
#
# Prerequisites (provided via nix-shell):
#   - cargo-zigbuild
#   - zig
#
# Usage:
#   ./deploy.sh [host]
#
# Examples:
#   ./deploy.sh                                    # deploy to preview (default)
#   ./deploy.sh ec2-user@preprod-sundae-scooper    # deploy to preprod

HOST="${1:-ec2-user@preview-sundae-scooper}"
REMOTE_DIR="~/scooper-v2"
TARGET="x86_64-unknown-linux-gnu.2.34"
BINARY="target/x86_64-unknown-linux-gnu/release/scooper-v2"

echo "==> Building release binary for $TARGET"
nix-shell -p cargo-zigbuild zig --run "cargo zigbuild --release --target $TARGET"

echo "==> Copying binary to $HOST"
scp "$BINARY" "$HOST:$REMOTE_DIR/scooper-v2.new"

echo "==> Deploying on $HOST"
ssh "$HOST" bash -s <<'REMOTE'
set -euo pipefail
cd ~/scooper-v2
chmod +x scooper-v2.new
sudo systemctl stop scooper-v2
cp scooper-v2 scooper-v2.old
mv scooper-v2.new scooper-v2
sudo systemctl start scooper-v2
sleep 2
sudo systemctl status scooper-v2 --no-pager
REMOTE

echo "==> Done"
