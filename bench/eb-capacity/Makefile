# Block-capacity benchmark: the order-queue parameters, measured on the real Cardano DEX
# flow (float API). See the docstrings of float_tape.py and dataset_params.py.
#
#   make
#       data/dataset-params.json: pools_per_order and pool_reuse_distance, measured on
#       the last 30 days of trades
#   make refresh
#       new download: drop the cached tape, take a new anchor, recompute
#
# The tape (data/tape.csv) is never rebuilt implicitly: rebuilding it means downloading
# trades again (bench/cache/ is not committed), so it only happens with `make refresh`,
# or when the file is missing.

# Every path below is relative to this folder: refuse to run from anywhere else
# (e.g. `make -f path/to/Makefile`) rather than fail later on missing files.
MAKEFILE_DIR := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
ifneq ($(CURDIR),$(MAKEFILE_DIR))
$(error run from $(MAKEFILE_DIR) (cd there, or `make -C $(MAKEFILE_DIR) …`), not from $(CURDIR))
endif

PYTHON ?= python3

TAPE := data/tape.csv
PARAMS := data/dataset-params.json

.PHONY: all help refresh

all: $(PARAMS)

help:
	@# The header comment block above, up to the first line not starting with '#'.
	@sed -n '/^#/!q; s/^# \{0,1\}//p' Makefile

# The trade tape: no prerequisite, so it is only built when missing. float_tape.py
# downloads the missing pages of bench/cache/ (git-ignored API cache) first.
$(TAPE):
	$(PYTHON) float_tape.py

# The parameters are a pure function of the tape and the script.
$(PARAMS): $(TAPE) dataset_params.py
	$(PYTHON) dataset_params.py

# New download: drop the cached tape, take a new anchor, then recompute the parameters.
refresh:
	$(PYTHON) float_tape.py --refresh
	$(PYTHON) dataset_params.py
