#!/usr/bin/env python3
"""Derive the public observer's config.json from the signing scooper's.

Run this ON the signer box, which is the only place the authoritative
mainnet config lives. `deploy.sh` ships mainnet with --keep-config, so the
copy in this repo is a distributable example and not what production runs;
deriving from the live file is what keeps the two instances in step on the
things they must agree about, such as script hashes and the starting point.

    python3 mk-observer-config.py --tailnet-ip 100.x.y.z > observer-config.json

The observer differs from the signer in six ways and no others:

  role            observer, so the binary refuses to start with a key
  no key          both key fields removed
  no mempool      there is no local cardano-node on that box
  peers           public Cardano relays, not localhost
  bootstrap       seeds from Blockfrost instead of replaying from the v4
                  starting point. This also matters on a rebuild: a sync
                  that starts behind the database's own position has every
                  block discarded and never advances, so a box being rebuilt
                  wants an empty database, not a restart in place.
  listeners       operational surface on loopback and the tailnet only,
                  intents public, and gossip pointed at the signer
"""
import argparse
import json
import sys

# The signer's tailnet address. Intents accepted publicly are forwarded here,
# over the tailnet, to the instance that can actually execute them.
SIGNER_TAILNET = "100.100.132.62"

# Public Cardano relays. The observer follows the tip from these rather than
# running a cardano-node of its own, which is what keeps it a small instance.
#
# NOT the dolos relays on the sundae-sync-v2 boxes, which was the original
# plan. Tried on 2026-09-29: acropolis connects to 30031 over TCP and is
# dropped again immediately, logging "disconnected from pre-configured peer"
# against both, and no block ever arrives. Its node-to-node relay is not
# compatible with this chain-sync client. Public relays worked first time,
# which is unsurprising: it is the same protocol the signer already speaks to
# its own local node.
RELAYS = [
    "backbone.cardano.iog.io:3001",
    "backbone.mainnet.cardanofoundation.org:3001",
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="/home/ec2-user/scooper-v2/config.json")
    ap.add_argument(
        "--tailnet-ip",
        required=True,
        help="the OBSERVER's tailnet address, from `tailscale ip -4` on that box",
    )
    args = ap.parse_args()

    d = json.load(open(args.source))
    d["role"] = "observer"

    ex = d["protocol"]["v4"]["execution"]
    # The binary refuses to start with either of these set. Removing them
    # here is what makes that refusal the guarantee rather than a formality.
    for k in ("scooper-secret-key", "scooper-secret-key-file", "scooper-stake-keyhash"):
        ex.pop(k, None)

    d["protocol"]["v4"].pop("mempool", None)
    d["acropolis"]["module"]["peer-network-interface"]["node-addresses"] = list(RELAYS)

    # The signer disables bootstrap by renaming the key, and the block sits
    # under `protocol`, not at the root. The observer wants it on. Reuse
    # whichever spelling is present so the URL and project id come from the
    # live config rather than being retyped.
    proto = d["protocol"]
    bs = proto.pop("bootstrap-disabled", None) or proto.get("bootstrap") or {}
    if not bs.get("project-id"):
        print("source config has no bootstrap project-id", file=sys.stderr)
        return 2
    bs["source"] = "blockfrost"
    proto["bootstrap"] = bs

    d["server"] = {
        "address": ["127.0.0.1:9999", f"{args.tailnet_ip}:9999"],
        "public_address": "0.0.0.0:9998",
    }

    # Kebab-case, and ScooperExecution does not deny unknown fields, so the
    # snake_case spelling parses cleanly and gossips nothing. Startup warns
    # when this ends up empty.
    ex["strategy-peers"] = [f"http://{SIGNER_TAILNET}:9998"]

    d["persistence"]["sqlite"]["filename"] = (
        "/home/ec2-user/scooper-v2/scooper-v2-mainnet-observer.db"
    )

    print(json.dumps(d, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
