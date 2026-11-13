#!/usr/bin/env bash
# Live-node soak (PLAN T3.2, gate G3.1): the `verify` example for N hours in
# its own memory-capped systemd user unit, so it outlives the shell that
# started it. Read-only: one feed subscriber plus pinned eth_* reads.
#
#   scripts/soak.sh [hours=24] [feed=127.0.0.1:8585] [rpc=http://127.0.0.1:8545]
#
# Log: target/soak/soak-<UTC start>.log — one line per block, a SUMMARY line
# every hour, and the final report (a markdown table). Stop early with
# `systemctl --user stop silt-soak`; the report is still written.
set -euo pipefail
cd "$(dirname "$0")/.."
hours=${1:-24}
feed=${2:-127.0.0.1:8585}
rpc=${3:-http://127.0.0.1:8545}
CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2} cargo build --release -p silt-cache --example verify
mkdir -p target/soak
log="$PWD/target/soak/soak-$(date -u +%Y%m%dT%H%M%SZ).log"
systemd-run --user --unit=silt-soak --collect \
    -p MemoryMax=1G -p MemorySwapMax=0 --working-directory="$PWD" \
    -- bash -c "exec '$PWD/target/release/examples/verify' '$feed' '$rpc' --hours '$hours' >'$log' 2>&1"
echo "silt-soak running for ${hours} h; log: $log"
echo "status: systemctl --user status silt-soak; stop: systemctl --user stop silt-soak"
