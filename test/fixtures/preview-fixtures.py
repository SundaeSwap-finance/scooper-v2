"""Build the scoop-test fixture from an unparameterized sundae-v4 blueprint.

    python3 -I test/fixtures/preview-fixtures.py <plutus.json>

Input: sundae-v4 115f19d, the audited build (PR #238), `aiken build`.

Each validator gets the preview parameters through `aiken blueprint apply`:
  pool_policy      b8a18e251b3e5078c04203f1a9af52408064c5d3a4256cfcfe3f67dd
  settings_policy  7b576da254d745bcbbabc91f34605deaa08f4e68d216f2a67d72ee12
  base_order_hash  the applied order validator's hash
  pool_mint_policy pool_policy

Output, next to this script:
  preview-audited-blueprint.json  the whole set, in the deployment-blueprint
      form `TestEnv::from_blueprint_file` reads.

The script asserts the applied hashes of the boot-fixed scripts that preview
runs, and of the CL and oracle modules, so a wrong input or a wrong parameter
fails here and not in a test. settings_mint needs the preview boot UTxO,
which the scooper repo does not hold, so the fixture names its policy with
no code: a scoop never runs it.
"""
import hashlib, json, os, shutil, subprocess, sys, tempfile

POOL_POLICY = "b8a18e251b3e5078c04203f1a9af52408064c5d3a4256cfcfe3f67dd"
SETTINGS_POLICY = "7b576da254d745bcbbabc91f34605deaa08f4e68d216f2a67d72ee12"
HERE = os.path.dirname(os.path.abspath(__file__))


def bytes_param(h):
    return "581c" + h


def apply_all(src, work, names):
    """Apply every parameter of each `module.validator` in `names`. Returns
    {name: (hash, compiledCode)} for the applied validators."""
    cur = os.path.join(work, "cur.json")
    shutil.copy(src, cur)

    def run(module, validator, cbor):
        out = os.path.join(work, "next.json")
        r = subprocess.run(
            ["aiken", "blueprint", "apply", "-i", cur, "-m", module, "-v", validator, cbor, "-o", out],
            capture_output=True, text=True, cwd=work)
        if r.returncode != 0:
            raise SystemExit(f"apply {module}.{validator}: {r.stdout}{r.stderr}")
        shutil.move(out, cur)

    def find(name):
        for v in json.load(open(cur))["validators"]:
            if v["title"].startswith(name + "."):
                return v
        raise SystemExit(f"{src}: no validator {name}")

    run("order", "order_validator", bytes_param(SETTINGS_POLICY))
    order_hash = find("order.order_validator")["hash"]
    params = {
        "pool_policy": bytes_param(POOL_POLICY),
        "settings_policy": bytes_param(SETTINGS_POLICY),
        "base_order_hash": bytes_param(order_hash),
        "pool_mint_policy": bytes_param(POOL_POLICY),
    }
    out = {}
    for name in names:
        if name != "order.order_validator":
            module, validator = name.split(".")
            for p in find(name).get("parameters", []):
                run(module, validator, params[p["title"]])
        v = find(name)
        assert "parameters" not in v, name
        out[name] = (v["hash"], v["compiledCode"])
    return out


def ref(i):
    return {"hash": hashlib.sha256(f"scooper fixture ref {i}".encode()).hexdigest(), "index": 0}


def main():
    src = sys.argv[1]
    # (fixture title, validator). `to_v4_module_scripts` matches titles by
    # substring and takes the first hit, so the order matters.
    rows = [
        ("pool", "pool.pool_validator"),
        ("poolMint", "pool_mint.pool_mint"),
        ("order", "order.order_validator"),
        ("basicOrder", "constraints/basic_order.basic_order_constraint"),
        ("swapOrder", "constraints/swap_order.swap_order_constraint"),
        ("settings", "settings.settings_validator"),
        ("constantSum", "modules/constant_sum.constant_sum_module"),
        ("constantProduct", "modules/constant_product.constant_product_module"),
        ("feeSplit", "modules/fee_split.fee_split_module"),
        ("fairness", "modules/fairness.fairness_module"),
        ("routeOrder", "constraints/route.route_constraint"),
        ("fairnessOrder", "constraints/fairness_order.fairness_order_constraint"),
        ("strategyOrder", "constraints/strategy_order.strategy_order_constraint"),
        ("stableswap", "modules/stableswap.stableswap_module"),
        ("bandedConcentratedLiquidity", "modules/concentrated_liquidity.concentrated_liquidity_module"),
        ("oracle", "modules/oracle.oracle_module"),
    ]
    expect = {
        "pool": "f577d24cfa393efdb85482554acbdd5082f99acba9c17eab916cf87a",
        "poolMint": POOL_POLICY,
        "order": "8a7ecdafb3605ddf761751b391c20482b7ef42ca01575d135d484c2b",
        "settings": "43f2ee25b54dcd8150df9e9908ae890638844cd3a748ef05d8a09d09",
        "bandedConcentratedLiquidity": "1ecfe94638a3c110140b2f2d71bb69077c7ebc832ca5e78dc69185f9",
        "oracle": "e0819f6e35de6ce91a2a8a6e1e156ddaee5289b910aa20f71dddd287",
    }
    with tempfile.TemporaryDirectory() as work:
        got = apply_all(src, work, [n for _, n in rows])
    vals, refs = [], []
    for title, name in rows:
        h, code = got[name]
        if title in expect and expect[title] != h:
            raise SystemExit(f"{title}: applied hash {h}, expected {expect[title]}")
        vals.append({"title": title, "hash": h, "compiledCode": code})
        refs.append({"key": title, "txIn": ref(f"{title}/{h}")})
    # After settings. Hash only: see the docstring.
    vals.insert(6, {"title": "settingsMint", "hash": SETTINGS_POLICY})
    bp = {
        "source": {"sundae-v4": "115f19d",
                   "plutus.json sha256": hashlib.sha256(open(src, "rb").read()).hexdigest(),
                   "pool_policy": POOL_POLICY, "settings_policy": SETTINGS_POLICY},
        "validators": vals, "references": refs,
    }
    name = "preview-audited-blueprint.json"
    with open(os.path.join(HERE, name), "w") as f:
        json.dump(bp, f, indent=1)
        f.write("\n")
    print("wrote", name)
    for t, n in rows:
        print(f"  {t:30} {got[n][0]}")


main()
