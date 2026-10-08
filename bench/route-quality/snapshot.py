"""Snapshot of the Cardano DEX market used by the route-quality benchmark.

    python3 snapshot.py [--refresh]      -> data/market-mainnet-selected.json

Four steps, one section each below:
  1. SOURCE   api.float.sundae.fi/v1/markets: every pool of the 13 Cardano venues,
              reserves read at a single block, liquidity already valued in ADA.
  2. RETAIN   keep a pool iff (a) both of its assets are in TOKENS, (b) its float
              liquidity is >= MIN_LIQUIDITY_ADA, (c) its venue's curve is known.
  3. FEES     float returns `fees: null`, so each retained pool's LP fee is read from its
              venue's own public API, joined on the pool id (FEE_SOURCES); venues with no
              public source get a documented constant.
  4. MODEL    every retained pool is then treated as a sundae-v4 pool: constant product for
              regular AMM pools, constant sum for stable pools, with its reserves and fee.

Raw API answers are cached in data/cache/ (re-downloaded with --refresh); everything after
step 1 is a pure function of those files."""
import json, subprocess, sys, time
from collections import Counter
from pathlib import Path

CACHE = Path(__file__).parent / "data" / "cache"
OUT = Path(__file__).parent / "data" / "market-mainnet-selected.json"
REFRESH = "--refresh" in sys.argv[1:]

ADA = "ada.lovelace"
# The token set: ADA + the most traded tokens of the snapshot (float 30d volume, all venues:
# USDCx, USDM, NIGHT, USDA, SNEK, DJED are the top 6), plus iUSD and MIN which add
# non-ADA links (stable cluster, MIN/USDM, MIN/NIGHT) and hence multi-path routes.
TOKENS = {
    ADA: "ADA",
    "0691b2fecca1ac4f53cb6dfb00b7013e561d1f34403b957cbb5af1fa.4e49474854": "NIGHT",
    "1f3aec8bfe7ea4fe14c5f121e2a92e301afe414147860d557cac7e34.5553444378": "USDCx",
    "c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad.0014df105553444d": "USDM",
    "8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61.446a65644d6963726f555344": "DJED",
    "fe7c786ab321f41c654ef6c1af7b3250a613c24e4213e0425a7ae456.55534441": "USDA",
    "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f.534e454b": "SNEK",
    "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880.69555344": "iUSD",
    "29d222ce763455e3d7a09a665ce554f00ac89d2e99a1a83d267170c6.4d494e": "MIN",
}
MIN_LIQUIDITY_ADA = 10_000
# Venue -> curve of its pools. "pool" = regular and stable pools coexist in the venue and
# the venue's own API tells which is which. Venues absent from this table are dropped.
VENUE_CURVE = {
    "minswap-v1": "constant_product", "minswap-v2": "constant_product",
    "sundae-v1": "constant_product", "sundae-v3": "constant_product",
    "cswap": "constant_product", "vyfi": "constant_product",
    "minswap-stable": "constant_sum", "sundae-stable": "constant_sum",
    # "sundae-v4": dropped, float does not expose the pool's module (curve unknown).
    "wingriders-v1": "pool", "wingriders-v2": "pool", "splash": "pool",
    # "danogo": dropped, its reserve ratios are not CP prices (20-100 % off every other
    #           venue, e.g. 111k USDC against 234k USDCx) and float does not expose the curve.
    # "snek-fun": dropped, launchpad bonding curves.
}


def curl(*args, attempts=4):
    # Public APIs drop connections now and then (e.g. curl exit 56 on Minswap's paging):
    # retry with a growing pause before giving up.
    for attempt in range(1, attempts + 1):
        try:
            out = subprocess.run(["curl", "-sf", "-m", "60", *args], check=True, capture_output=True)
            return json.loads(out.stdout)
        except subprocess.CalledProcessError as e:
            if attempt == attempts:
                raise
            print(f"  curl exit {e.returncode}, retry {attempt}/{attempts - 1} ...", file=sys.stderr)
            time.sleep(2 ** attempt)


def cached(name, fetch):
    path = CACHE / name
    if REFRESH or not path.exists():
        CACHE.mkdir(parents=True, exist_ok=True)
        print(f"fetching {name} ...", file=sys.stderr)
        json.dump(fetch(), open(path, "w"))
    return json.load(open(path))


# ---------------------------------------------------------------- 1. SOURCE (float)

def fetch_float():
    # Sorted by liquidity; markets whose liquidity is unknown (no price for one side) come
    # last and are cut: they can't pass the liquidity criterion anyway.
    url, markets, meta = "https://api.float.sundae.fi/v1/markets?sort=liquidity&limit=100", [], None
    while url:
        res = curl(url)
        meta = meta or res["meta"]
        page = res.get("markets") or []
        ranked = [m for m in page if m.get("liquidity") not in (None, "0")]
        markets += ranked
        if len(ranked) < len(page):
            break
        url = res.get("links", {}).get("next")
        time.sleep(0.2)
    return dict(meta=meta, markets=markets)


# ---------------------------------------------------------------- 3. FEES (per venue)
# Each fetcher downloads the venue's pool list (it is given the venue's retained markets,
# which only CSwap needs); each lookup returns (fee, curve, source)
# for one float market. The pool is identified by float's `market_key` wherever the venue
# exposes that id; the join is always done inside the market's own venue (market keys are
# not unique across venues: WingRiders v1 and Minswap v2 derive them from the same pair hash).

def fetch_sundae(_markets):
    fields = "id version bidFee askFee assets { id }"
    pools = {}
    for asset in TOKENS:
        if asset == ADA:
            continue
        q = f'query q($a: ID!) {{ pools {{ byAsset(asset: $a) {{ {fields} }} }} }}'
        res = curl("https://api.sundae.fi/graphql", "-H", "content-type: application/json",
                   "-d", json.dumps({"query": q, "variables": {"a": asset}}))
        for p in res["data"]["pools"]["byAsset"]:
            pools[p["id"]] = p
        time.sleep(0.2)
    return list(pools.values())


def fetch_minswap(_markets):
    # Sorted by liquidity_currency (pool value in ADA, within ~1 % of float's), descending:
    # stop once a page ends below half the retention threshold (the safety margin covers
    # the two valuations). That is ~4 pages instead of all ~135 (13k+ pools).
    stop_below = MIN_LIQUIDITY_ADA / 2
    pools, after = [], None
    while True:
        body = {"term": "", "limit": 100, "sort_direction": "desc", "sort_field": "liquidity"}
        if after:
            body["search_after"] = after
        res = curl("-X", "POST", "https://api-mainnet-prod.minswap.org/v1/pools/metrics",
                   "-H", "content-type: application/json", "-d", json.dumps(body))
        page = res.get("pool_metrics") or []
        pools += page
        after = res.get("search_after")
        if not page or not after or float(page[-1].get("liquidity_currency") or 0) < stop_below:
            return pools
        time.sleep(0.2)


def fetch_wingriders(_markets):
    common = "version poolType issuedShareToken { policyId assetName } " \
             "tokenA { policyId assetName quantity } " \
             "tokenB { policyId assetName quantity } treasuryA treasuryB"
    q = (f"{{ liquidityPools(input: {{}}) {{ ... on LiquidityPoolV1 {{ {common} }} "
         f"... on LiquidityPoolV2 {{ {common} swapFeeInBasis protocolFeeInBasis "
         f"projectFeeInBasis reserveFeeInBasis feeBasis }} }} }}")
    return curl("https://api.mainnet.wingriders.com/graphql", "-H",
                "content-type: application/json", "-d", json.dumps({"query": q}))["data"]["liquidityPools"]


def fetch_splash(_markets):
    return curl("https://api.splash.trade/platform-api/v1/pools/overview")


def fetch_vyfi(_markets):
    return curl("https://api.vyfi.io/lp?networkId=1&v2=true")


def fetch_cswap(markets):
    # No CSwap API: read each pool's UTxO on chain (Koios). float's key = the policy that
    # mints the pool NFT (asset name "c") and the LP token.
    body = {"_asset_list": [[m["key"], "63"] for m in markets], "_extended": True}
    return curl("-X", "POST", "https://api.koios.rest/api/v1/asset_utxos",
                "-H", "content-type: application/json", "-d", json.dumps(body))


def asset_id(policy, name):
    return ADA if not policy else f"{policy}.{name}"


def lookup_sundae(m, pools):
    p = next((p for p in pools if p["id"] == m["key"]), None)
    if p is None:
        return None
    fee = max(f[0] / f[1] for f in (p["bidFee"], p["askFee"]))  # equal on every pool fetched so far
    return fee, None, "api.sundae.fi pool bidFee/askFee"


def lookup_minswap(m, pools):
    if m["venue"] == "minswap-stable":  # float's key isn't exposed by the API: join on the pair
        cands = [p for p in pools if p["type"] == "MinswapStable" and {m["a"], m["b"]} == {
            asset_id(p[s]["currency_symbol"], p[s]["token_name"]) for s in ("asset_a", "asset_b")}]
        how = "pair"
    else:  # v1/v2: float's key = LP token name
        cands = [p for p in pools if p["type"] in ("Minswap", "MinswapV2")
                 and p["lp_asset"]["token_name"] == m["key"]]
        how = "LP token name"
    if len(cands) != 1:
        return None
    return cands[0]["trading_fee_tier"][0] / 100, None, f"minswap API trading_fee_tier (by {how})"


def lookup_wingriders(m, pools):
    # float's key = the pool's share (LP) token name.
    version = m["venue"].split("-")[1].upper()
    p = next((p for p in pools if p["version"] == version
              and p["issuedShareToken"]["assetName"] == m["key"]), None)
    if p is None:
        return None
    stable = p["poolType"] == "STABLESWAP"
    curve = "constant_sum" if stable else "constant_product"
    if version == "V1":
        # V1 pools carry no fee, neither in the API nor in their datum (it is fixed in the
        # validator, and the docs give no figure). ASSUMED equal to the V2 default of the
        # same curve: swap 30 + protocol 5 bps (97 of 111 V2 CP pools), stableswap 5 + 1
        # bps (6 of 7 V2 stable pools).
        return (0.0006 if stable else 0.0035), curve, \
            "ASSUMED = WingRiders V2 default for the curve (V1 exposes no fee)"
    fee = sum(p[k] or 0 for k in ("swapFeeInBasis", "protocolFeeInBasis",
                                  "projectFeeInBasis", "reserveFeeInBasis")) / p["feeBasis"]
    return fee, curve, "WingRiders API *FeeInBasis sum (by share token name)"


def lookup_splash(m, pools):
    p = next((e["pool"] for e in pools if e["pool"]["id"] == m["key"]), None)
    if p is None or p["poolType"] not in ("cfmm", "weighted", "stable"):
        return None
    num = p["poolFeeNumX"]  # fraction of the input kept, over 1000 (v1) or 100000 (later)
    fee = 1 - num / (100_000 if num > 1000 else 1000)
    curve = "constant_sum" if p["poolType"] == "stable" else "constant_product"
    return fee, curve, "splash API poolFeeNumX"


def lookup_vyfi(m, pools):
    # float's key = the pool's main NFT policy; fee = bar fee + LP fee, in basis points
    # (processFee is a flat per-order batcher fee, not part of the curve).
    for p in pools:
        j = json.loads(p["json"])
        if j["mainNFT"]["currencySymbol"] == m["key"]:
            f = j["feesSettings"]
            return (f["barFee"] + f["liqFee"]) / 10_000, None, "api.vyfi.io feesSettings barFee+liqFee"
    return None


def lookup_cswap(m, utxos):
    # Pool datum = [LP supply, fee in basis points, policy A, name A, policy B, name B,
    # LP policy, LP name]. Unit checked against two mainnet swaps (2026-09-25): ADA/NIGHT
    # datum 15 -> implied 0.149 %, ADA/SNEK datum 85 -> implied 0.844 %.
    for u in utxos:
        if any(a["policy_id"] == m["key"] and a["asset_name"] == "63" for a in u["asset_list"]):
            return (u["inline_datum"]["value"]["fields"][1]["int"] / 10_000, None,
                    "on-chain pool datum field 1 (bps), via Koios")
    return None


FEE_SOURCES = {  # venue prefix -> (raw file, fetcher, lookup)
    "sundae": ("sundae-pools.json", fetch_sundae, lookup_sundae),
    "minswap": ("minswap-pools.json", fetch_minswap, lookup_minswap),
    "wingriders": ("wingriders-pools.json", fetch_wingriders, lookup_wingriders),
    "splash": ("splash-pools.json", fetch_splash, lookup_splash),
    "vyfi": ("vyfi-pools.json", fetch_vyfi, lookup_vyfi),
    "cswap": ("cswap-pools.json", fetch_cswap, lookup_cswap),
}


# ---------------------------------------------------------------- run

snap = cached("float-markets.json", fetch_float)
meta = snap["meta"]
print(f"1. SOURCE  api.float.sundae.fi/v1/markets  block {meta['as_of']['height']} "
      f"({meta['as_of_time']}): {len(snap['markets'])} markets with a known liquidity")

# 2. RETAIN
retained, dropped = [], Counter()
for m in snap["markets"]:
    a, b, liq = m["quote"]["asset"], m["base"]["asset"], int(m["liquidity"]) / 1e6
    if a not in TOKENS or b not in TOKENS:
        continue
    if liq < MIN_LIQUIDITY_ADA:
        dropped[f"liquidity < {MIN_LIQUIDITY_ADA} ADA"] += 1
        continue
    if m["venue"] not in VENUE_CURVE:
        dropped[f"venue {m['venue']}"] += 1
        continue
    ra, rb = int(m["quote"]["reserve"]), int(m["base"]["reserve"])
    if b == ADA:  # ADA first
        a, b, ra, rb = b, a, rb, ra
    retained.append(dict(id=m["id"], venue=m["venue"], key=m["id"].split(":", 1)[1],
                         a=a, b=b, ra=ra, rb=rb, tvl_ada=liq))
print(f"2. RETAIN  both assets in {{{', '.join(TOKENS.values())}}}: "
      f"{len(retained) + sum(dropped.values())} pools -> {len(retained)} kept, dropped {dict(dropped)}")

# 3. FEES + 4. MODEL
raw = {}
for p in retained:
    prefix = p["venue"].split("-")[0]
    file, fetch, lookup = FEE_SOURCES[prefix]
    if prefix not in raw:
        raw[prefix] = cached(file, lambda: fetch([q for q in retained if q["venue"].startswith(prefix)]))
    got = lookup(p, raw[prefix])
    if got is None:
        raise SystemExit(f"no fee found for {p['id']} in data/cache/{file} (stale cache? try --refresh)")
    p["fee"], curve, p["fee_source"] = got
    p["curve"] = curve or VENUE_CURVE[p["venue"]]
    assert p["curve"] in ("constant_product", "constant_sum"), p

# Reference price of each token, in lovelace per raw unit: liquidity-weighted median of the
# implied prices of the retained constant-product ADA/X pools. Used to value orders and
# measure losses; pools are NOT snapped to it (mainnet arbitrage stays in the snapshot).
price = {ADA: 1.0}
for t in TOKENS:
    qs = sorted((p["ra"] / p["rb"], p["tvl_ada"]) for p in retained
                if p["a"] == ADA and p["b"] == t and p["curve"] == "constant_product")
    acc = 0.0
    for px, w in qs:
        acc += w
        if acc >= sum(w for _, w in qs) / 2:
            price[t] = px
            break
missing = [TOKENS[t] for t in TOKENS if t not in price]
assert not missing, f"no ADA pool to price {missing}"

# Decimals of each token (10^decimals raw units per token), from the Cardano token registry
# (tokens.cardano.org, CIP-26; subject = policy id + asset name). ADA is not a registry
# entry: its 6 decimals (lovelace) are the protocol's.
def fetch_registry():
    subjects = [t.replace(".", "") for t in TOKENS if t != ADA]
    return curl("-X", "POST", "https://tokens.cardano.org/metadata/query",
                "-H", "content-type: application/json",
                "-d", json.dumps({"subjects": subjects, "properties": ["decimals"]}))["subjects"]


registry = {e["subject"]: e["decimals"]["value"]
            for e in cached("token-registry.json", fetch_registry) if "decimals" in e}
decimals = {ADA: 6} | {t: registry.get(t.replace(".", "")) for t in TOKENS if t != ADA}
missing = [TOKENS[t] for t, n in decimals.items() if n is None]
assert not missing, f"no decimals in the token registry for {missing}"
usdm = next(t for t, n in TOKENS.items() if n == "USDM")
ada_usd = 1 / price[usdm]  # both 6 decimals

print("3. FEES / 4. MODEL (every pool = a sundae-v4 pool)")
for s, n in Counter(p["fee_source"] for p in retained).most_common():
    print(f"   {n:3} pools  fee from {s}")
print(f"\n   {'pair':13} {'venue':15} {'TVL (ADA)':>11} {'fee':>7}  curve")
for p in sorted(retained, key=lambda p: (TOKENS[p["a"]], TOKENS[p["b"]], -p["tvl_ada"])):
    print(f"   {TOKENS[p['a']] + '/' + TOKENS[p['b']]:13} {p['venue']:15} {p['tvl_ada']:>11,.0f} "
          f"{p['fee'] * 100:6.2f}%  {p['curve']}")
tvl = sum(p["tvl_ada"] for p in retained)
print(f"\n   {len(retained)} pools, TVL {tvl / 1e6:.1f}M ADA ≈ {tvl * ada_usd / 1e6:.1f}M $ "
      f"(ADA ≈ {ada_usd:.4f} $, from the ADA/USDM reference price)")

json.dump(dict(
    source=dict(api="https://api.float.sundae.fi/v1/markets?sort=liquidity", **meta),
    criteria=dict(tokens=list(TOKENS.values()), min_liquidity_ada=MIN_LIQUIDITY_ADA,
                  venues=sorted(VENUE_CURVE)),
    model="every pool is a sundae-v4 pool: constant_product or constant_sum, observed reserves and fee",
    ada_usd=ada_usd, price=price, ticker=TOKENS,
    decimals=decimals,
    pools=[{k: v for k, v in p.items() if k != "key"} for p in retained],
), open(OUT, "w"), indent=1)
print(f"-> {OUT}")
