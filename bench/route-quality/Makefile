# Route-quality benchmark: trade execution vs max tx complexity. Fetches the raw API data,
# builds the markets, runs the bench. See README.md.
#
#   make bench
#       run on data/market-v4-extrapolated.json, default trades and sizes
#   make bench PAIRS=ADA-USDM SIZES=1000000,5000000 KS=4,15 SHOW=1
#   make bench MAINNET_LIMIT=… RAISED_LIMIT=16384,1000000000,500000000000
#       tx limits as bytes,mem,steps (defaults: see the bench's --help)
#   make data/market-v4-extrapolated.json
#       rebuild it if data/market-mainnet-selected.json or cl_market.py changed
#       (CL_ARGS="…" for cl_market.py's options)
#   make refresh
#       new mainnet snapshot: re-download data/cache/, rewrite
#       data/market-mainnet-selected.json (the v4 market follows on the next make)
#
# data/market-mainnet-selected.json is never rebuilt implicitly: rebuilding it means
# downloading a new snapshot (data/cache/ is not committed), so it only happens with
# `make refresh`, or when the file is missing.

# Every path below is relative to this folder: refuse to run from anywhere else
# (e.g. `make -f path/to/Makefile`) rather than fail later on missing files.
MAKEFILE_DIR := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
ifneq ($(CURDIR),$(MAKEFILE_DIR))
$(error run from $(MAKEFILE_DIR) (cd there, or `make -C $(MAKEFILE_DIR) …`), not from $(CURDIR))
endif

PYTHON ?= python3
# Optional bench arguments (empty = the binary's defaults, see --help).
PAIRS ?=
SIZES ?=
KS ?=
SHOW ?=
MAINNET_LIMIT ?=
RAISED_LIMIT ?=
# Extra arguments for cl_market.py, e.g. CL_ARGS="--volatile-width 0.005". Changing them
# alone does not trigger a rebuild: use `make -B data/market-v4-extrapolated.json CL_ARGS=…`.
CL_ARGS ?=

SNAPSHOT := data/market-mainnet-selected.json
CL := data/market-v4-extrapolated.json
BLUEPRINT := data/devnet-blueprint-without-traces.json
RESULTS := results

# cargo runs against the repo's manifest; the bench gets absolute paths.
ROUTE_BENCH := cargo run --release --manifest-path $(CURDIR)/../../Cargo.toml \
	--features route-bench --example route-bench --
BENCH_ARGS = --blueprint $(CURDIR)/$(BLUEPRINT) \
	$(if $(PAIRS),--pairs $(PAIRS)) $(if $(SIZES),--sizes $(SIZES)) \
	$(if $(KS),--ks $(KS)) $(if $(SHOW),--show) \
	$(if $(MAINNET_LIMIT),--mainnet-limit $(MAINNET_LIMIT)) \
	$(if $(RAISED_LIMIT),--raised-limit $(RAISED_LIMIT))

.PHONY: help bench refresh clean

help:
	@# The header comment block above, up to the first line not starting with '#'.
	@sed -n '/^#/!q; s/^# \{0,1\}//p' Makefile

# The mainnet snapshot: no prerequisite, so it is only built when missing. snapshot.py
# downloads the missing files of data/cache/ (git-ignored API cache) first.
$(SNAPSHOT):
	$(PYTHON) snapshot.py

# The CL market is a pure function of the snapshot and the generator: rebuilt whenever
# either is newer (after a git checkout it may just be regenerated identically).
$(CL): $(SNAPSHOT) cl_market.py
	$(PYTHON) cl_market.py $(CL_ARGS)

$(RESULTS):
	mkdir -p $@

bench: $(CL) | $(RESULTS)
	$(ROUTE_BENCH) --market $(CURDIR)/$(CL) $(BENCH_ARGS) --csv $(CURDIR)/$(RESULTS)/market-v4-extrapolated.csv

# New mainnet snapshot: re-download all of data/cache/ and rewrite data/market-mainnet-selected.json.
refresh:
	$(PYTHON) snapshot.py --refresh

clean:
	rm -rf $(RESULTS)
