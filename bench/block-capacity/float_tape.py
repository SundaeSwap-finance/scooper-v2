"""Cardano DEX trade flow over the last 30 days, the dynamic input of the block-capacity bench.

    python3 float_tape.py [--refresh]   -> data/tape.csv

Source: api.float.sundae.fi/v1/trades (public, no auth): every swap of every venue, newest
first, 250 per page. One row is the net swap of ONE pool in ONE tx (not one order: a batch
filling two orders on the same pool is one row, a routed order through three pools is
three rows sharing a tx_hash).

The tape is paged backwards from the newest trade at the first run (the anchor, saved in
bench/cache/float-tape/anchor.json) until it is 30 days older than the anchor. Pages are
cached one file each, so an interrupted run resumes where it stopped; --refresh drops
the cache and takes a new anchor. Everything after the download is a pure function of
bench/cache/ (shared by the benches)."""
import csv, json, shutil, subprocess, sys, time
from pathlib import Path

HERE = Path(__file__).parent
CACHE = HERE.parent / "cache"
TAPE = CACHE / "float-tape"
API = "https://api.float.sundae.fi/v1"
PAGE_SIZE = 250

REFRESH = "--refresh" in sys.argv[1:]
DAYS = 30


def curl(url, attempts=5):
    # Same retry policy as route-quality/snapshot.py: public APIs drop connections now and then.
    for attempt in range(1, attempts + 1):
        try:
            out = subprocess.run(["curl", "-sf", "-m", "60", url], check=True, capture_output=True)
            return json.loads(out.stdout)
        except subprocess.CalledProcessError as e:
            if attempt == attempts:
                raise
            # Leading newline: do not overwrite the in-place progress line.
            print(f"\n  curl exit {e.returncode}, retry {attempt}/{attempts - 1} ...", file=sys.stderr)
            time.sleep(2 ** attempt)


# ---------------------------------------------------------------- TAPE

def page_path(i):
    return TAPE / f"page-{i:05d}.json"


def fetch_tape():
    if REFRESH and TAPE.exists():
        shutil.rmtree(TAPE)
    TAPE.mkdir(parents=True, exist_ok=True)
    anchor_path = TAPE / "anchor.json"
    if not anchor_path.exists():
        print("taking a new anchor: the newest trade ...", file=sys.stderr)
        first = curl(f"{API}/trades?limit={PAGE_SIZE}")
        json.dump(first, open(page_path(0), "w"))
        json.dump(dict(meta=first["meta"], time=first["trades"][0]["time"]), open(anchor_path, "w"))
    anchor = json.load(open(anchor_path))
    oldest = anchor["time"] - DAYS * 86400

    def progress(page):
        # How far back in time the pages reach, out of the DAYS to cover. Redrawn in place.
        age = (anchor["time"] - page["trades"][-1]["time"]) / 86400
        print(f"\r  downloading trades: {min(100, 100 * age / DAYS):3.0f} % ({i + 1} pages)",
              end="", file=sys.stderr, flush=True)

    i = 0
    while page_path(i + 1).exists():
        i += 1
    page = json.load(open(page_path(i)))
    print(f"downloading {DAYS} days of trades from {API}/trades "
          f"(~2-3 s per page of {PAGE_SIZE}; cached, resumable)", file=sys.stderr)
    progress(page)
    while page["trades"] and page["trades"][-1]["time"] > oldest and page["links"].get("next"):
        i += 1
        page = curl(page["links"]["next"])
        json.dump(page, open(page_path(i), "w"))
        progress(page)
        time.sleep(0.2)
    print(file=sys.stderr)
    return anchor, oldest, i + 1


COLUMNS = ["slot", "time", "height", "tx_hash", "event_index", "venue", "market", "direction",
           "base_asset", "base_amount", "quote_asset", "quote_amount", "value_lovelace"]


def write_tape(anchor, oldest, pages):
    # Pages are disjoint by cursor, but the same trade could straddle a page boundary if the
    # API ever re-orders within a slot: key on (tx, event, market) to be safe.
    seen, rows = set(), []
    for i in range(pages):
        for t in json.load(open(page_path(i)))["trades"]:
            key = (t["tx_hash"], t["event_index"], t["market"])
            if t["time"] <= oldest or key in seen:
                continue
            seen.add(key)
            rows.append([t["point"]["slot"], t["time"], t["point"]["height"], t["tx_hash"],
                         t["event_index"], t["venue"], t["market"], t["direction"],
                         t["base"]["asset"], t["base"]["amount"], t["quote"]["asset"],
                         t["quote"]["amount"], t.get("value") or ""])
    rows.sort(key=lambda r: (r[0], r[3], r[4]))
    out = HERE / "data" / "tape.csv"
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(COLUMNS)
        w.writerows(rows)
    print(f"{out.relative_to(HERE)}: {len(rows)} trades, {len({r[3] for r in rows})} txs, "
          f"anchor block {anchor['meta']['as_of']['height']} ({anchor['meta']['as_of_time']})",
          file=sys.stderr)


write_tape(*fetch_tape())
