#!/usr/bin/env bash
# silt CI: fmt, clippy -D warnings, tests, dependency-boundary checks
# (PLAN T0.1, G0.3). Default run covers the reth-free members; FULL=1 adds
# the wrapper (compiles the reth tree — minutes, not seconds).
set -euo pipefail
cd "$(dirname "$0")/.."

cargo fmt --all -- --check
cargo clippy -p silt-wire -p silt-cache --all-targets -- -D warnings
cargo clippy -p silt-cache --features yevm --all-targets -- -D warnings
cargo test -p silt-wire -p silt-cache
cargo test -p silt-cache --features yevm

# D4 dependency boundaries:
# - wire: no reth, no yevm, ever.
if cargo tree -p silt-wire --prefix none | awk '{print $1}' | grep -qiE '^(reth|yevm)'; then
  echo "FAIL: silt-wire grew a reth/yevm dependency"; exit 1
fi
# - cache (default features): no reth, no yevm.
if cargo tree -p silt-cache --prefix none | awk '{print $1}' | grep -qiE '^(reth|yevm)'; then
  echo "FAIL: silt-cache default features grew a reth/yevm dependency"; exit 1
fi
# - cache (+yevm): yevm allowed, reth still forbidden.
if cargo tree -p silt-cache --features yevm --prefix none | awk '{print $1}' | grep -qiE '^reth'; then
  echo "FAIL: silt-cache (+yevm) grew a reth dependency"; exit 1
fi

if [ "${FULL:-0}" = "1" ]; then
  cargo clippy -p silt-exex -- -D warnings
  cargo test -p silt-exex
fi

echo "CI OK"
