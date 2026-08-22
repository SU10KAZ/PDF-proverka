#!/usr/bin/env python3
"""relgraph_stability -- Track-B probe 1: does a RELATION multiset separate
truly-changed from unchanged pairs better than v0.1 segment coverage / topology?

Run from repo root:
    python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_stability.py
Writes: artifacts/relgraph_stability.json
"""
from __future__ import annotations

import collections
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(Path(__file__).resolve().parent))
import relgraph_core as R  # noqa: E402

A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts"
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"

# human verdicts from artifacts/human_validation.md
CHANGED = {"ss_scheme_text_changed", "eom_singleline_changed", "vk_nodes"}


def sub(counter: collections.Counter, prefix: str) -> collections.Counter:
    return collections.Counter({k: v for k, v in counter.items() if k[0] == prefix})


def main() -> None:
    pairs = json.loads((A / "block_pairs.json").read_text())["pairs"]
    rows = []
    for p in pairs:
        pid = p["pair_id"]
        t0 = time.time()
        graphs = {}
        for side in ("left", "right"):
            desc = json.loads((A / "descriptions" / pid / side / "vector_block.json").read_text())
            graphs[side] = R.build_relation_graph(desc)
        gl, gr = graphs["left"], graphs["right"]
        rl, rr = gl["relations"], gr["relations"]
        el, er = gl["entities"], gr["entities"]
        comb_l = collections.Counter(rl)
        comb_l.update({("entity", k, "-"): v for k, v in el.items()})
        comb_r = collections.Counter(rr)
        comb_r.update({("entity", k, "-"): v for k, v in er.items()})

        cmp_path = A / "comparisons" / pid / "comparison.json"
        cmpj = json.loads(cmp_path.read_text())

        row = {
            "pair_id": pid,
            "human": p["human_expected"],
            "changed": pid in CHANGED,
            "v01_geometry_similarity": cmpj["geometry"]["similarity"],
            "v01_topology_similarity": cmpj["topology"]["similarity"],
            "v01_text_similarity": cmpj["text"].get("effective_similarity"),
            "v01_status": cmpj["status"],
            "rel_jaccard": round(R.weighted_jaccard(rl, rr), 6),
            "rel_cosine": round(R.cosine(rl, rr), 6),
            "entity_jaccard": round(R.weighted_jaccard(el, er), 6),
            "combined_jaccard": round(R.weighted_jaccard(comb_l, comb_r), 6),
            "per_relation_jaccard": {
                k: round(R.weighted_jaccard(sub(rl, k), sub(rr, k)), 6)
                for k in ("contains", "adjacent", "connected", "crosses", "parallel",
                          "labelled_by", "member_of_group", "repeats_along", "between")
            },
            "left_stats": gl["stats"],
            "right_stats": gr["stats"],
            "seconds": round(time.time() - t0, 1),
        }
        rows.append(row)
        print(f"{pid:24s} rel_J={row['rel_jaccard']:.4f} ent_J={row['entity_jaccard']:.4f} "
              f"comb_J={row['combined_jaccard']:.4f} geom={row['v01_geometry_similarity']:.4f} "
              f"topo={row['v01_topology_similarity']:.4f}  ({row['seconds']}s)")

    def margin(key: str) -> dict:
        ch = [r[key] for r in rows if r["changed"]]
        un = [r[key] for r in rows if not r["changed"]]
        return {
            "max_changed": max(ch),
            "min_unchanged": min(un),
            "margin": round(min(un) - max(ch), 6),
            "separated": min(un) > max(ch),
            "changed_values": {r["pair_id"]: r[key] for r in rows if r["changed"]},
            "min_unchanged_pair": min((r for r in rows if not r["changed"]), key=lambda r: r[key])["pair_id"],
        }

    metrics = ["v01_geometry_similarity", "v01_topology_similarity", "v01_text_similarity",
               "rel_jaccard", "rel_cosine", "entity_jaccard", "combined_jaccard"]
    summary = {m: margin(m) for m in metrics}
    for k in ("contains", "adjacent", "connected", "crosses", "parallel",
              "labelled_by", "member_of_group", "repeats_along", "between"):
        ch = [r["per_relation_jaccard"][k] for r in rows if r["changed"]]
        un = [r["per_relation_jaccard"][k] for r in rows if not r["changed"]]
        summary[f"rel::{k}"] = {
            "max_changed": max(ch), "min_unchanged": min(un),
            "margin": round(min(un) - max(ch), 6), "separated": min(un) > max(ch),
        }

    print("\n--- separation margin (min over unchanged - max over changed; >0 = clean split) ---")
    for m, v in summary.items():
        print(f"{m:32s} margin={v['margin']:+.4f}  max_changed={v['max_changed']:.4f} "
              f"min_unchanged={v['min_unchanged']:.4f} separated={v['separated']}")

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "relgraph_stability.json").write_text(
        json.dumps({"research_only": True, "changed_set": sorted(CHANGED),
                    "rows": rows, "separation": summary}, ensure_ascii=False, indent=1))
    print("\nwrote", OUT / "relgraph_stability.json")


if __name__ == "__main__":
    main()
