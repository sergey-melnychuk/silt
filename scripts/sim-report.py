#!/usr/bin/env python3
"""Simulation-latency report (PLAN M6, T6.5) from the JSON lines the tools write.

  sim-report.py b1 <sim-bench.jsonl> [--warm-step 2]
  sim-report.py b2 <sim-a.jsonl> <sim-node.jsonl> [--warm-seq 64]

b1: the pairs `sim-bench` (yevm-reth) wrote, silt and reth side by side.
b2: the silt example `sim` (side A, at the head) joined with the node's
    `YEVM_SIM_OUT` file (side B2) on (block, tx index).

Only pairs with equal outcomes count (G6.1); a pair where either side
failed is listed but left out of the latency figures. Percentiles are
nearest-rank. Prints markdown.
"""

import json
import math
import sys


def pct(xs, p):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    k = max(0, min(len(xs) - 1, math.ceil(p / 100 * len(xs)) - 1))
    return xs[k]


def row(name, xs, unit="µs"):
    return (
        f"| {name} | {len(xs)} | {pct(xs, 50):,.0f} | {pct(xs, 99):,.0f} | "
        f"{pct(xs, 99.9):,.0f} | {max(xs) if xs else float('nan'):,.0f} |"
    )


HEAD = "| | n | p50 | p99 | p99.9 | max |\n|---|---|---|---|---|---|"


def load(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def summary(title, pairs, bundles):
    """pairs: (a_us, b_us, a_reads_us, b_reads_us, hits, misses); bundles: (a_us, b_us)."""
    a = [p[0] for p in pairs]
    b = [p[1] for p in pairs]
    d = [p[0] - p[1] for p in pairs]
    print(f"\n### {title}\n")
    print("Per transaction, µs (each alone on a fresh overlay):\n")
    print(HEAD)
    print(row("silt (A)", a))
    print(row("reth (B)", b))
    print(row("A − B, same tx", d))
    print(row("time in reads, A", [p[2] for p in pairs]))
    print(row("time in reads, B", [p[3] for p in pairs]))
    hits = sum(p[4] for p in pairs if p[4] is not None)
    misses = sum(p[5] for p in pairs if p[5] is not None)
    if hits + misses:
        print(
            f"\nsilt store: {hits:,} hits, {misses:,} misses "
            f"({100 * hits / (hits + misses):.1f}% hit rate, "
            f"{misses / max(1, len(pairs)):.2f} misses per tx)."
        )
    faster = sum(1 for x in d if x <= 0)
    print(f"A no slower than B on {faster:,} of {len(d):,} transactions.")
    if bundles:
        print("\nPer bundle (whole block in order), ms:\n")
        print(HEAD)
        print(row("silt (A)", [x[0] / 1e3 for x in bundles]))
        print(row("reth (B)", [x[1] / 1e3 for x in bundles]))
        print(row("A − B, same block", [(x[0] - x[1]) / 1e3 for x in bundles]))
    gate = (
        pct(a, 50) <= pct(b, 50)
        and pct(a, 99) <= pct(b, 99)
        and (
            not bundles
            or (
                pct([x[0] for x in bundles], 50) <= pct([x[1] for x in bundles], 50)
                and pct([x[0] for x in bundles], 99) <= pct([x[1] for x in bundles], 99)
            )
        )
    )
    print(
        f"\nG6.2 ({len(pairs):,} paired txs, need ≥ 2,000): "
        f"{'met' if gate and len(pairs) >= 2000 else 'not met'}"
    )


def b1(path, warm_step):
    rows = load(path)
    txs = [r for r in rows if r["kind"] == "tx"]
    differ = [r for r in txs if not r["same"]]
    failed = [r for r in txs if r["same"] and r["failed"]]
    print(f"## A vs B1 (read-only datadir), {path}\n")
    print(
        f"{len(txs):,} transactions over {len({r['block'] for r in txs})} blocks; "
        f"{len(differ)} with different outcomes (G6.1), {len(failed)} failed on both sides."
    )
    for r in differ[:20]:
        print(f"- DIFFER block {r['block']} tx {r['i']}: silt {r['silt']['outcome']} | reth {r['reth']['outcome']}")
    for label, keep in (("warm", lambda r: r["step"] >= warm_step), ("cold (first steps)", lambda r: r["step"] < warm_step)):
        sel = [r for r in txs if r["same"] and not r["failed"] and keep(r)]
        if not sel:
            continue
        pairs = [
            (r["silt"]["us"], r["reth"]["us"], r["silt"]["reads"]["us"], r["reth"]["reads"]["us"], r["hits"], r["misses"])
            for r in sel
        ]
        bundles = [
            (r["silt"]["us"], r["reth"]["us"])
            for r in rows
            if r["kind"] == "bundle" and r["same"] and keep(r)
        ]
        summary(f"{label}: steps {sorted({r['step'] for r in sel})[0]}+", pairs, bundles)


def b2(path_a, path_b, warm_seq):
    a_rows = load(path_a)
    b_rows = load(path_b)
    a_tx = {(r["block"], r["i"]): r for r in a_rows if r["kind"] == "tx"}
    b_tx = {(r["block"], r["i"]): r for r in b_rows if r["kind"] == "tx"}
    a_bun = {r["block"]: r for r in a_rows if r["kind"] == "bundle"}
    b_bun = {r["block"]: r for r in b_rows if r["kind"] == "bundle"}
    keys = sorted(set(a_tx) & set(b_tx))
    print(f"## A vs B2 (inside the node), {path_a} + {path_b}\n")
    print(
        f"A simulated {len(a_tx):,} txs, B2 {len(b_tx):,}; {len(keys):,} on both "
        f"({len({k[0] for k in keys})} blocks)."
    )
    bad_hash = [k for k in keys if a_tx[k]["hash"].lower() != b_tx[k]["hash"].lower()]
    differ = [k for k in keys if k not in bad_hash and a_tx[k]["outcome"] != b_tx[k]["outcome"]]
    print(f"{len(bad_hash)} index mismatches, {len(differ)} with different outcomes (G6.1).")
    for k in differ[:20]:
        print(f"- DIFFER block {k[0]} tx {k[1]}: silt {a_tx[k]['outcome']} | node {b_tx[k]['outcome']}")
    good = [k for k in keys if k not in bad_hash and k not in differ and not a_tx[k]["failed"]]
    for label, keep in (("warm", lambda s: s > warm_seq), ("cold (first blocks)", lambda s: s <= warm_seq)):
        sel = [k for k in good if keep(a_tx[k]["seq"])]
        if not sel:
            continue
        pairs = [
            (a_tx[k]["us"], b_tx[k]["us"], a_tx[k]["reads"]["us"], b_tx[k]["reads"]["us"], a_tx[k]["hits"], a_tx[k]["misses"])
            for k in sel
        ]
        blocks = {k[0] for k in sel}
        bundles = [
            (a_bun[n]["us"], b_bun[n]["us"])
            for n in sorted(blocks)
            if n in a_bun and n in b_bun and a_bun[n]["digest"] == b_bun[n]["digest"]
        ]
        summary(label, pairs, bundles)


def main(argv):
    if len(argv) >= 2 and argv[0] == "b1":
        warm = int(argv[argv.index("--warm-step") + 1]) if "--warm-step" in argv else 2
        b1(argv[1], warm)
    elif len(argv) >= 3 and argv[0] == "b2":
        warm = int(argv[argv.index("--warm-seq") + 1]) if "--warm-seq" in argv else 64
        b2(argv[1], argv[2], warm)
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main(sys.argv[1:])
