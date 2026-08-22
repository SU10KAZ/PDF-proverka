#!/usr/bin/env python3
"""relgraph_size -- Track-B probe 3: payload size of the relation graph vs
VectorBlockDescription Level 1 / Level 2 / Level 3 for the same 20 blocks.

Token estimate uses the same rule as extractor._size_metrics (ceil(chars/4)).

Run from repo root:
    python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_size.py
"""
from __future__ import annotations

import collections
import json
import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(Path(__file__).resolve().parent))
import relgraph_core as R  # noqa: E402
from relgraph_granularity import project, coarse  # noqa: E402

A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts"
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"


def metrics(value):
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return {"bytes": len(payload.encode("utf-8")), "estimated_tokens": math.ceil(len(payload) / 4)}


def main() -> None:
    pairs = json.loads((A / "block_pairs.json").read_text())["pairs"]
    rows = []
    for p in pairs:
        for side in ("left", "right"):
            pid = p["pair_id"]
            desc = json.loads((A / "descriptions" / pid / side / "vector_block.json").read_text())
            g = R.build_relation_graph(desc)
            g3 = R.relation_json(g)
            ent1 = collections.Counter()
            for k, v in g["entities"].items():
                ent1[coarse(k, 1)] += v
            g1 = {
                "entities": dict(ent1.most_common()),
                "relations": [{"rel": k[0], "a": k[1], "b": k[2], "n": v}
                              for k, v in sorted(project(g["relations"], 1).items(),
                                                 key=lambda kv: -kv[1])],
                "groups": [{"cls": coarse(x["cls"], 1), "count": x["count"]}
                           for x in g["groups"][:60]],
            }
            g1_with_text = dict(g1)
            g1_with_text["texts"] = [t["text"] for t in g["texts"]]
            sm = desc["size_metrics"]
            row = {
                "pair_id": pid, "side": side,
                "L0_raw": sm["level_0_raw_vector"]["estimated_tokens"],
                "L1_normalized_primitives": sm["level_1_normalized_primitives"]["estimated_tokens"],
                "L2_groups_topology": sm["level_2_groups_topology"]["estimated_tokens"],
                "L3_compact": sm["level_3_compact_description"]["estimated_tokens"],
                "RG3_full_class": metrics(g3)["estimated_tokens"],
                "RG1_shape_class": metrics(g1)["estimated_tokens"],
                "RG1_plus_texts": metrics(g1_with_text)["estimated_tokens"],
                "RG3_bytes": metrics(g3)["bytes"],
                "RG1_bytes": metrics(g1)["bytes"],
                "n_relation_tokens_G3": len(g["relations"]),
                "n_relation_tokens_G1": len(project(g["relations"], 1)),
                "n_relation_instances": sum(g["relations"].values()),
                "n_clusters": g["stats"]["n_clusters"],
                "n_segments": g["stats"]["n_segments"],
            }
            rows.append(row)
            print(f"{pid:24s} {side:5s} L1={row['L1_normalized_primitives']:7d} "
                  f"L2={row['L2_groups_topology']:7d} L3={row['L3_compact']:5d} "
                  f"RG3={row['RG3_full_class']:6d} RG1={row['RG1_shape_class']:5d} "
                  f"RG1+txt={row['RG1_plus_texts']:6d} relToks={row['n_relation_tokens_G3']:5d}")

    tot = {k: sum(r[k] for r in rows) for k in
           ("L0_raw", "L1_normalized_primitives", "L2_groups_topology", "L3_compact",
            "RG3_full_class", "RG1_shape_class", "RG1_plus_texts")}
    print("\ntotals over 20 blocks (estimated tokens):")
    for k, v in tot.items():
        print(f"  {k:26s} {v:9d}   x{v/tot['RG1_shape_class']:.2f} of RG1")
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "relgraph_size.json").write_text(json.dumps(
        {"research_only": True, "rows": rows, "totals": tot}, ensure_ascii=False, indent=1))
    print("wrote", OUT / "relgraph_size.json")


if __name__ == "__main__":
    main()
