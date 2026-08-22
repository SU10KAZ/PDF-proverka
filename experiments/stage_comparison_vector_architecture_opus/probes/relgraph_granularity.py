#!/usr/bin/env python3
"""relgraph_granularity -- Track-B probe 1b: which part of the relation multiset
is signal and which is threshold noise?

Recomputes pair similarity at 4 token granularities:
  G0 rel_type only                       (9 numbers)
  G1 rel_type + shape class              (rect/round/seg/poly_*)
  G2 rel_type + shape + size bucket      (log2 diag)
  G3 rel_type + full class               (shape|size|aspect)  == relgraph_core default

Also dumps the raw relation counters per pair/side for later reuse.

Run from repo root:
    python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_granularity.py
"""
from __future__ import annotations

import collections
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(Path(__file__).resolve().parent))
import relgraph_core as R  # noqa: E402

A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts"
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"
CHANGED = {"ss_scheme_text_changed", "eom_singleline_changed", "vk_nodes"}


def coarse(cls: str, level: int) -> str:
    if level >= 3:
        return cls
    parts = cls.split("|")
    if parts[0] == "txt":
        return "txt" if level <= 1 else "|".join(parts[:2])
    if level <= 1:
        return parts[0]
    return "|".join(parts[:2])  # shape + size bucket


def project(counter: collections.Counter, level: int) -> collections.Counter:
    out: collections.Counter = collections.Counter()
    for (rel, a, b), n in counter.items():
        if level == 0:
            out[(rel,)] += n
        elif rel == "member_of_group":
            out[(rel, coarse(a, level), "-")] += n
        elif rel == "repeats_along":
            out[(rel, a, coarse(b, level))] += n
        else:
            key = tuple(sorted((coarse(a, level), coarse(b, level))))
            out[(rel, key[0], key[1])] += n
    return out


def main() -> None:
    pairs = json.loads((A / "block_pairs.json").read_text())["pairs"]
    rows = []
    dump: dict[str, dict] = {}
    for p in pairs:
        pid = p["pair_id"]
        g = {}
        for side in ("left", "right"):
            desc = json.loads((A / "descriptions" / pid / side / "vector_block.json").read_text())
            g[side] = R.build_relation_graph(desc)
        rl, rr = g["left"]["relations"], g["right"]["relations"]
        el, er = g["left"]["entities"], g["right"]["entities"]
        row = {"pair_id": pid, "changed": pid in CHANGED, "human": p["human_expected"],
               "n_clusters_left": g["left"]["stats"]["n_clusters"],
               "n_clusters_right": g["right"]["stats"]["n_clusters"],
               "n_rel_instances_left": g["left"]["stats"]["n_relation_instances"],
               "n_rel_instances_right": g["right"]["stats"]["n_relation_instances"],
               "n_rel_tokens_left": g["left"]["stats"]["n_relation_tokens"],
               "n_rel_tokens_right": g["right"]["stats"]["n_relation_tokens"]}
        for lvl in (0, 1, 2, 3):
            pl, pr = project(rl, lvl), project(rr, lvl)
            row[f"G{lvl}_jaccard"] = round(R.weighted_jaccard(pl, pr), 6)
            row[f"G{lvl}_cosine"] = round(R.cosine(pl, pr), 6)
        # entity inventory at coarse level
        cel, cer = collections.Counter(), collections.Counter()
        for k, v in el.items():
            cel[coarse(k, 1)] += v
        for k, v in er.items():
            cer[coarse(k, 1)] += v
        row["entity_G1_jaccard"] = round(R.weighted_jaccard(cel, cer), 6)
        rows.append(row)
        dump[pid] = {
            "left": {"|".join(map(str, k)): v for k, v in rl.items()},
            "right": {"|".join(map(str, k)): v for k, v in rr.items()},
            "entities_left": dict(el), "entities_right": dict(er),
            "groups_left": g["left"]["groups"][:40], "groups_right": g["right"]["groups"][:40],
        }
        print(f"{pid:24s} G0={row['G0_jaccard']:.4f} G1={row['G1_jaccard']:.4f} "
              f"G2={row['G2_jaccard']:.4f} G3={row['G3_jaccard']:.4f} "
              f"clusters {row['n_clusters_left']}->{row['n_clusters_right']}")

    print("\n--- separation margin per granularity ---")
    summary = {}
    for key in ("G0_jaccard", "G1_jaccard", "G2_jaccard", "G3_jaccard",
                "G0_cosine", "G1_cosine", "G2_cosine", "G3_cosine", "entity_G1_jaccard"):
        ch = [r[key] for r in rows if r["changed"]]
        un = [r[key] for r in rows if not r["changed"]]
        worst = min((r for r in rows if not r["changed"]), key=lambda r: r[key])["pair_id"]
        best = max((r for r in rows if r["changed"]), key=lambda r: r[key])["pair_id"]
        summary[key] = {"max_changed": max(ch), "max_changed_pair": best,
                        "min_unchanged": min(un), "min_unchanged_pair": worst,
                        "margin": round(min(un) - max(ch), 6), "separated": min(un) > max(ch)}
        print(f"{key:20s} margin={summary[key]['margin']:+.4f} "
              f"max_changed={max(ch):.4f}({best})  min_unchanged={min(un):.4f}({worst})")

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "relgraph_granularity.json").write_text(
        json.dumps({"research_only": True, "rows": rows, "separation": summary},
                   ensure_ascii=False, indent=1))
    (OUT / "relgraph_counters.json").write_text(json.dumps(dump, ensure_ascii=False))
    print("\nwrote", OUT / "relgraph_granularity.json")


if __name__ == "__main__":
    main()
