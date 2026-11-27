"""Build the scoop-test fixtures from two unparameterized sundae-v4 blueprints.

    python3 -I test/fixtures/preview-fixtures.py <launch-candidate plutus.json> <revision-1 plutus.json> <audited plutus.json>

Inputs:
  launch candidate: sundae-v4 25d60ae (main + PRs #234, #226-#233), `aiken build`.
  revision 1:       sundae-v4 e7d9c14, `aiken build`.
  audited:          sundae-v4 115f19d (PR #238), `aiken build`.

Each validator gets the preview parameters through `aiken blueprint apply`:
  pool_policy      b8a18e251b3e5078c04203f1a9af52408064c5d3a4256cfcfe3f67dd
  settings_policy  7b576da254d745bcbbabc91f34605deaa08f4e68d216f2a67d72ee12
  base_order_hash  the applied order validator's hash
  pool_mint_policy pool_policy

Outputs, next to this script:
  preview-launch-candidate-blueprint.json  the whole launch-candidate set, in
      the deployment-blueprint form `TestEnv::from_blueprint_file` reads. CL
      and oracle carry "revision": 2.
  preview-revision-1-modules.json  the revision-1 CL and oracle, and the
      e7d9c14 constant_sum, titled by config role key, for
      `TestEnv::with_modules_file`.
  preview-revision-3-modules.json  the audited (revision-3) CL and oracle,
      in the same form.

The script asserts the applied hashes that preview runs, so a wrong input or
a wrong parameter fails here and not in a test. settings_mint needs the
preview boot UTxO, which the scooper repo does not hold, so the fixture
names its policy with no code: a scoop never runs it.
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
    launch_src, r1_src, r3_src = sys.argv[1], sys.argv[2], sys.argv[3]
    # (fixture title, validator, revision). The order is the one the
    # October fixture used: `to_v4_module_scripts` matches titles by
    # substring and takes the first hit.
    launch = [
        ("pool", "pool.pool_validator", None),
        ("poolMint", "pool_mint.pool_mint", None),
        ("order", "order.order_validator", None),
        ("basicOrder", "constraints/basic_order.basic_order_constraint", None),
        ("swapOrder", "constraints/swap_order.swap_order_constraint", None),
        ("settings", "settings.settings_validator", None),
        ("constantSum", "modules/constant_sum.constant_sum_module", None),
        ("constantProduct", "modules/constant_product.constant_product_module", None),
        ("feeSplit", "modules/fee_split.fee_split_module", None),
        ("fairness", "modules/fairness.fairness_module", None),
        ("routeOrder", "constraints/route.route_constraint", None),
        ("fairnessOrder", "constraints/fairness_order.fairness_order_constraint", None),
        ("strategyOrder", "constraints/strategy_order.strategy_order_constraint", None),
        ("stableswap", "modules/stableswap.stableswap_module", None),
        ("bandedConcentratedLiquidity", "modules/concentrated_liquidity.concentrated_liquidity_module", 2),
        ("oracle", "modules/oracle.oracle_module", 2),
    ]
    revision_1 = [
        ("banded-concentrated-liquidity", "modules/banded_concentrated_liquidity.banded_concentrated_liquidity_module", 1),
        ("oracle", "modules/oracle.oracle_module", 1),
        ("constant-sum", "modules/constant_sum.constant_sum_module", 1),
    ]
    revision_3 = [
        ("banded-concentrated-liquidity", "modules/concentrated_liquidity.concentrated_liquidity_module", 3),
        ("oracle", "modules/oracle.oracle_module", 3),
    ]
    expect = {
        "pool": "f577d24cfa393efdb85482554acbdd5082f99acba9c17eab916cf87a",
        "poolMint": POOL_POLICY,
        "order": "8a7ecdafb3605ddf761751b391c20482b7ef42ca01575d135d484c2b",
        "settings": "43f2ee25b54dcd8150df9e9908ae890638844cd3a748ef05d8a09d09",
        "bandedConcentratedLiquidity": "e025f97689e2550fe68426650322878451112984729843250c25576e",
        "oracle": "a5c8dd38f6545c82f45fe4096ec9fa068d4efb06f6ac5f145f045dfa",
    }
    expect_r1 = {
        "banded-concentrated-liquidity": "33485bba2689d4b82b412fb683f07474b9c6c042778d3b9d2b691f18",
        "oracle": "31e23dfda0b2c2120667234dfa92a66909fd8765e80166de734c6927",
    }
    expect_r3 = {
        "banded-concentrated-liquidity": "1ecfe94638a3c110140b2f2d71bb69077c7ebc832ca5e78dc69185f9",
        "oracle": "e0819f6e35de6ce91a2a8a6e1e156ddaee5289b910aa20f71dddd287",
    }
    sha = lambda p: hashlib.sha256(open(p, "rb").read()).hexdigest()
    with tempfile.TemporaryDirectory() as work:
        got = apply_all(launch_src, work, [n for _, n, _ in launch])
    with tempfile.TemporaryDirectory() as work:
        got_r1 = apply_all(r1_src, work, ["order.order_validator"] + [n for _, n, _ in revision_1])
    with tempfile.TemporaryDirectory() as work:
        got_r3 = apply_all(r3_src, work, ["order.order_validator"] + [n for _, n, _ in revision_3])

    def entries(rows, got, expect):
        vals, refs = [], []
        for i, (title, name, rev) in enumerate(rows):
            h, code = got[name]
            if title in expect and expect[title] != h:
                raise SystemExit(f"{title}: applied hash {h}, preview runs {expect[title]}")
            v = {"title": title, "hash": h, "compiledCode": code}
            if rev is not None:
                v["revision"] = rev
            vals.append(v)
            refs.append({"key": title, "txIn": ref(f"{title}/{h}")})
        return vals, refs

    vals, refs = entries(launch, got, expect)
    # After settings, as in the October fixture. Hash only: see the docstring.
    vals.insert(6, {"title": "settingsMint", "hash": SETTINGS_POLICY})
    main_bp = {
        "source": {"sundae-v4": "25d60ae", "plutus.json sha256": sha(launch_src),
                   "pool_policy": POOL_POLICY, "settings_policy": SETTINGS_POLICY},
        "validators": vals, "references": refs,
    }
    vals1, refs1 = entries(revision_1, got_r1, expect_r1)
    r1_bp = {
        "source": {"sundae-v4": "e7d9c14", "plutus.json sha256": sha(r1_src),
                   "pool_policy": POOL_POLICY, "settings_policy": SETTINGS_POLICY},
        "validators": vals1, "references": refs1,
    }
    vals3, refs3 = entries(revision_3, got_r3, expect_r3)
    r3_bp = {
        "source": {"sundae-v4": "115f19d", "plutus.json sha256": sha(r3_src),
                   "pool_policy": POOL_POLICY, "settings_policy": SETTINGS_POLICY},
        "validators": vals3, "references": refs3,
    }
    for name, bp in [("preview-launch-candidate-blueprint.json", main_bp),
                     ("preview-revision-1-modules.json", r1_bp),
                     ("preview-revision-3-modules.json", r3_bp)]:
        with open(os.path.join(HERE, name), "w") as f:
            json.dump(bp, f, indent=1)
            f.write("\n")
        print("wrote", name)
    for t, n, _ in launch:
        print(f"  {t:30} {got[n][0]}")
    for t, n, _ in revision_1:
        print(f"  r1 {t:27} {got_r1[n][0]}")
    for t, n, _ in revision_3:
        print(f"  r3 {t:27} {got_r3[n][0]}")


main()
