#!/usr/bin/env bash
set -euo pipefail

# Deploy scooper-v2 to a remote host running Amazon Linux 2023 (glibc 2.34).
#
# Pushes:
#   - Cross-compiled release binary (cargo-zigbuild, glibc 2.34)
#   - config/preview-v4.json → ~/scooper-v2/config.json on remote
#     (node-address `preview-sundae-scooper:3002` is rewritten to
#     `localhost:3002` so the remote talks to its co-located node).
#   - deploy/scooper-v2.service → /etc/systemd/system/scooper-v2.service
#     (systemd-reloaded if it changed).
#
# Then stops the service, swaps the binary, optionally wipes the DB,
# starts the service, and tails status.
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
#   ./deploy.sh [--wipe-db] [host]
#
# Flags:
#   --wipe-db    Delete the remote scooper-v2.db before starting. Use after
#                a contract cutover or when the protocol config gained new
#                module ref-utxos that need to be fetched at bootstrap.
#
# Examples:
#   ./deploy.sh                                          # preview, keep DB
#   ./deploy.sh --wipe-db                                # preview, fresh DB
#   ./deploy.sh ec2-user@preprod-sundae-scooper          # preprod
#   ./deploy.sh --wipe-db ec2-user@preprod-sundae-scooper

WIPE_DB=0
HOST="ec2-user@preview-sundae-scooper"
for arg in "$@"; do
  case "$arg" in
    --wipe-db) WIPE_DB=1 ;;
    *) HOST="$arg" ;;
  esac
done

REMOTE_DIR="/home/ec2-user/scooper-v2"
TARGET="x86_64-unknown-linux-gnu.2.34"
BINARY="target/x86_64-unknown-linux-gnu/release/scooper-v2"
LOCAL_CONFIG="config/preview-v4.json"
LOCAL_UNIT="deploy/scooper-v2.service"

echo "==> Building release binary for $TARGET"
nix-shell -p cargo-zigbuild zig --run "cargo zigbuild --release --target $TARGET"

echo "==> Staging config (rewriting node-address for remote)"
TMP_CONFIG="$(mktemp)"
trap 'rm -f "$TMP_CONFIG"' EXIT
# When run locally during dev, the node address points at the remote box.
# On the remote, the node lives on localhost. Rewrite at deploy time so
# we keep one canonical config file.
sed 's#preview-sundae-scooper:3002#localhost:3002#g' "$LOCAL_CONFIG" > "$TMP_CONFIG"

echo "==> Copying binary, config, and unit file to $HOST"
scp -q "$BINARY"        "$HOST:$REMOTE_DIR/scooper-v2.new"
scp -q "$TMP_CONFIG"    "$HOST:$REMOTE_DIR/config.json.new"
scp -q "$LOCAL_UNIT"    "$HOST:/tmp/scooper-v2.service.new"

echo "==> Deploying on $HOST (wipe_db=$WIPE_DB)"
ssh "$HOST" WIPE_DB="$WIPE_DB" bash -s <<'REMOTE'
set -euo pipefail
cd /home/ec2-user/scooper-v2

# Update systemd unit if it changed; reload daemon only when needed.
UNIT_PATH=/etc/systemd/system/scooper-v2.service
if ! sudo cmp -s /tmp/scooper-v2.service.new "$UNIT_PATH" 2>/dev/null; then
  echo "    systemd unit changed — installing"
  sudo install -m 0644 /tmp/scooper-v2.service.new "$UNIT_PATH"
  sudo systemctl daemon-reload
fi
rm -f /tmp/scooper-v2.service.new

# Stop service before swapping binary/config/db.
sudo systemctl stop scooper-v2

# Binary: archive old, install new.
chmod +x scooper-v2.new
[ -f scooper-v2 ] && cp scooper-v2 scooper-v2.old
mv scooper-v2.new scooper-v2

# Config: archive old, install new.
[ -f config.json ] && cp config.json config.json.old
mv config.json.new config.json

# Optional: wipe DB so bootstrap re-runs (needed after contract cutover or
# when new module ref-utxos are added to the config).
if [ "${WIPE_DB:-0}" = "1" ]; then
  echo "    wiping scooper-v2.db (fresh bootstrap)"
  rm -f scooper-v2.db scooper-v2.db-journal
fi

# Start and confirm.
sudo systemctl enable scooper-v2 >/dev/null 2>&1 || true
sudo systemctl start scooper-v2
sleep 3
sudo systemctl status scooper-v2 --no-pager | head -15
REMOTE

echo "==> Done"
echo
echo "Tail logs with:  ssh $HOST 'sudo journalctl -u scooper-v2 -f'"
