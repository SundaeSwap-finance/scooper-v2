#!/usr/bin/env bash
# Runs a tool with the shared test wallet (the "sugar hub" mnemonic, as in
# sundae-v4's run-deploy.sh) and the preview Blockfrost project id taken from
# config/preview-v4.json, so neither appears on a command line.
#   ./run.sh wallet.ts
#   ./run.sh publish-ref.ts
#   ./run.sh create-order.ts <beacon-ref> <lovelace> <policy> <name-hex> <num>/<den>
set -euo pipefail
cd "$(dirname "$0")"
words=(); for _ in $(seq 23); do words+=(sugar); done; words+=(hub)
export SEED_PHRASE="${words[*]}"
export BLOCKFROST_PROJECT_ID="$(python3 -c '
import json,re;u=json.load(open("../../config/preview-v4.json"))["protocol"]["v4"]["execution"]["submit-url"]
print(re.search(r"project_id=(\w+)",u).group(1))')"
exec bun run "$@"
