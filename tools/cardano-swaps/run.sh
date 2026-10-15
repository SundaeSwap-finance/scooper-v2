#!/usr/bin/env bash
# Runs a tool with the shared test wallet (the "sugar hub" mnemonic, as in
# sundae-v4's run-deploy.sh) and the Blockfrost project id taken from the
# network's config file, so neither appears on a command line.
#   ./run.sh wallet.ts
#   ./run.sh publish-ref.ts prepare|submit <file>
#   ./run.sh create-order.ts <beacon-ref> <lovelace> <policy> <name-hex> <num>/<den>
# NETWORK=preprod switches networks; that config carries no Blockfrost id, so
# pass BLOCKFROST_PROJECT_ID in the environment there.
set -euo pipefail
cd "$(dirname "$0")"
words=(); for _ in $(seq 23); do words+=(sugar); done; words+=(hub)
export SEED_PHRASE="${words[*]}"
export NETWORK="${NETWORK:-preview}"
if [ -z "${BLOCKFROST_PROJECT_ID:-}" ]; then
  BLOCKFROST_PROJECT_ID="$(python3 - "$NETWORK" <<'EOF'
import json, re, sys
url = json.load(open(f"../../config/{sys.argv[1]}-v4.json"))["protocol"]["v4"]["execution"]["submit-url"]
m = re.search(r"project_id=(\w+)", url)
print(m.group(1) if m else "")
EOF
)"
  export BLOCKFROST_PROJECT_ID
fi
exec bun run "$@"
