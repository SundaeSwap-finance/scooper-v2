# Block-capacity benchmark: orders per ranking block (RB) vs per endorser block (EB), on
# an order queue whose parameters are measured on the real Cardano DEX flow (float API).
# See README.md.
#
#   make
#       data/dataset-params.json: pools_per_order, pool_reuse_distance and bands_per_leg,
#       measured on the last 30 days of trades (plus a float reserves snapshot)
#   make bench
#       run the bench (block-bench example) on those parameters, one CSV row per scoop in
#       results/scoops.csv and one per block size (RB, each EB) in results/blocks.csv
#   make bench ORDERS=2000 SEED=2 MAX_ORDERS=30
#       queue length, generator seed, max orders per scoop (defaults: see the bench's --help)
#   make bench TX_LIMIT=… RB_LIMIT=… EB_LIMITS="6000000,3500000000,1000000000000 12000000,7000000000,2000000000000"
#       tx and block limits as bytes,mem,steps; EB_LIMITS takes several EB sizes, packed
#       from the same scoops (defaults: see the bench's --help)
#   make refresh
#       new download: drop the cached tape, take a new anchor, recompute
#
# The tape (data/tape.csv) is never rebuilt implicitly: rebuilding it means downloading
# trades again (bench/cache/ is not committed), so it only happens with `make refresh`,
# or when the file is missing and the parameters must be recomputed. Only the parameters
# are committed: without a local tape, the bench runs on them as they are.

# Every path below is relative to this folder: refuse to run from anywhere else
# (e.g. `make -f path/to/Makefile`) rather than fail later on missing files.
MAKEFILE_DIR := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
ifneq ($(CURDIR),$(MAKEFILE_DIR))
$(error run from $(MAKEFILE_DIR) (cd there, or `make -C $(MAKEFILE_DIR) …`), not from $(CURDIR))
endif

PYTHON ?= python3
# Optional bench arguments (empty = the binary's defaults, see --help).
ORDERS ?=
SEED ?=
MAX_ORDERS ?=
TX_LIMIT ?=
RB_LIMIT ?=
EB_LIMITS ?=

TAPE := data/tape.csv
PARAMS := data/dataset-params.json
RESULTS := results
# The scripts are shared by the benches.
BLUEPRINT := ../common/devnet-blueprint-without-traces.json

# cargo runs against the repo's manifest; the bench gets absolute paths.
EB_BENCH := cargo run --release --manifest-path $(CURDIR)/../../Cargo.toml \
	--features bench --example block-bench --
BENCH_ARGS = --params $(CURDIR)/$(PARAMS) \
	--blueprint $(abspath $(BLUEPRINT)) \
	$(if $(ORDERS),--orders $(ORDERS)) $(if $(SEED),--seed $(SEED)) \
	$(if $(MAX_ORDERS),--max-orders $(MAX_ORDERS)) \
	$(if $(TX_LIMIT),--tx-limit $(TX_LIMIT)) \
	$(if $(RB_LIMIT),--rb-limit $(RB_LIMIT)) \
	$(foreach eb_limit,$(EB_LIMITS),--eb-limit $(eb_limit))

.PHONY: all help bench refresh clean

all: $(PARAMS)

help:
	@# The header comment block above, up to the first line not starting with '#'.
	@sed -n '/^#/!q; s/^# \{0,1\}//p' Makefile

# The trade tape: no prerequisite, so it is only built when missing. float_tape.py
# downloads the missing pages of bench/cache/ (git-ignored API cache) first.
$(TAPE):
	$(PYTHON) float_tape.py

# The parameters are a pure function of the tape and the script. They are committed and
# the tape is not: with no local tape, keep them rather than download a new one.
$(PARAMS): $(if $(wildcard $(TAPE)),$(TAPE) dataset_params.py)
	$(MAKE) $(TAPE)
	$(PYTHON) dataset_params.py

$(RESULTS):
	mkdir -p $@

bench: $(PARAMS) | $(RESULTS)
	$(EB_BENCH) $(BENCH_ARGS) --csv $(CURDIR)/$(RESULTS)/scoops.csv \
		--blocks-csv $(CURDIR)/$(RESULTS)/blocks.csv

# New download: drop the cached tape, take a new anchor, then recompute the parameters.
refresh:
	$(PYTHON) float_tape.py --refresh
	$(PYTHON) dataset_params.py

clean:
	rm -rf $(RESULTS)
