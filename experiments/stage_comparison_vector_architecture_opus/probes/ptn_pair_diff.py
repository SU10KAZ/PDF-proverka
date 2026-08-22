"""Per-motif instance-count diff across the 10 Track A pairs, for each signature.

Repro: python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_pair_diff
Reads  artifacts/ptn/signatures_<pair>_<side>.json (produced by ptn_run_signatures)
Writes artifacts/ptn_pair_diff.json + a Markdown table on stdout.
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
PTN = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts/ptn"
PAIRS_FILE = ROOT / "experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json"
SIGS = ("S0", "S1", "S2", "S3", "S4", "S5", "S5c")
MIN_COUNT = 2  # a "motif" must repeat at least twice on at least one side


def counts(data: dict, name: str) -> dict[str, int]:
    node = data["S0"] if name == "S0" else data["signatures"][name]
    return {k: v for k, v in node["all_counts"].items() if v >= 1}


def main() -> None:
    pairs = json.load(open(PAIRS_FILE, encoding="utf-8"))["pairs"]
    out = {}
    print("| pair | sig | motif groups L/R | shared | count-changed | appeared | disappeared | max |Δ| |")
    print("|---|---|---|---|---|---|---|---|")
    for pair in pairs:
        pid = pair["pair_id"]
        try:
            left = json.load(open(PTN / f"signatures_{pid}_left.json", encoding="utf-8"))
            right = json.load(open(PTN / f"signatures_{pid}_right.json", encoding="utf-8"))
        except FileNotFoundError:
            continue
        out[pid] = {"human_expected": pair["human_expected"], "sigs": {}}
        for name in SIGS:
            cl, cr = counts(left, name), counts(right, name)
            keys = {k for k in set(cl) | set(cr) if max(cl.get(k, 0), cr.get(k, 0)) >= MIN_COUNT}
            shared = [k for k in keys if k in cl and k in cr]
            changed = [(k, cl[k], cr[k]) for k in shared if cl[k] != cr[k]]
            appeared = [(k, cr[k]) for k in keys if k not in cl]
            disappeared = [(k, cl[k]) for k in keys if k not in cr]
            maxd = max((abs(a - b) for _k, a, b in changed), default=0)
            out[pid]["sigs"][name] = {
                "groups_left": sum(1 for k in keys if k in cl),
                "groups_right": sum(1 for k in keys if k in cr),
                "shared": len(shared),
                "changed": sorted(changed, key=lambda t: -abs(t[1] - t[2]))[:15],
                "changed_n": len(changed),
                "appeared_n": len(appeared),
                "disappeared_n": len(disappeared),
                "appeared_top": sorted(appeared, key=lambda t: -t[1])[:8],
                "disappeared_top": sorted(disappeared, key=lambda t: -t[1])[:8],
                "max_abs_delta": maxd,
            }
            r = out[pid]["sigs"][name]
            print(f"| {pid} | {name} | {r['groups_left']}/{r['groups_right']} | {r['shared']} | "
                  f"{r['changed_n']} | {r['appeared_n']} | {r['disappeared_n']} | {maxd} |")
    with open(PTN.parent / "ptn_pair_diff.json", "w", encoding="utf-8") as handle:
        json.dump(out, handle, ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
