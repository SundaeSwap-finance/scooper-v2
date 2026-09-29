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
  peers           our own dolos relays, not localhost
  bootstrap       seeds from Blockfrost instead of replaying from the v4
                  starting point, which is older than the window the relays
                  retain
  listeners       operational surface on loopback and the tailnet only,
                  intents public, and gossip pointed at the signer
"""
import argparse
import json
import sys

# The signer's tailnet address. Intents accepted publicly are forwarded here,
# over the tailnet, to the instance that can actually execute them.
SIGNER_TAILNET = "100.100.132.62"

# Dolos relays on the two sundae-sync-v2 boxes. The observer follows the tip
# from these rather than running a cardano-node of its own, which is what
# keeps it a small instance.
DOLOS_RELAYS = ["10.0.101.82:30031", "10.0.107.92:30031"]


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
    d["acropolis"]["module"]["peer-network-interface"]["node-addresses"] = list(DOLOS_RELAYS)

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
