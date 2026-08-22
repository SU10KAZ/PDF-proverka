#!/usr/bin/env python3
"""relgraph_wl -- Track-B probe: does the GRAPH (not the multiset) buy anything?

Weisfeiler-Lehman refinement over the relation graph. Node label at depth 0 is
the coarse shape class; depth k folds in the sorted multiset of
(relation_type, neighbour_label_{k-1}). A repeated *motif* (e.g. a device that
always sits on a feeder, is labelled by text and connects to a switch symbol)
becomes one WL label whose multiplicity is the object count.

Reports for every pair: WL-label multiset similarity at depth 0/1/2, and for the
eom pair the labels whose multiplicity changed (the "12 -> 14" channel).

Run from repo root:
    python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_wl.py
"""
from __future__ import annotations

import collections
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(Path(__file__).resolve().parent))
import relgraph_core as R  # noqa: E402
from relgraph_granularity import coarse  # noqa: E402

A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts"
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"
CHANGED = {"ss_scheme_text_changed", "eom_singleline_changed", "vk_nodes"}
GEOM_CHANGED = {"eom_singleline_changed"}


def wl_labels(graph, depth=2, level=1):
    """Returns list of label-dicts, one per WL depth (0..depth)."""
    n = len(graph["clusters"])
    lab = [coarse(c["cls"], level) for c in graph["clusters"]]
    adj = collections.defaultdict(list)
    raw = graph["raw"]
    for rel in ("contains", "adjacent", "connected", "crosses"):
        for a, b in raw.get(rel, []):
            adj[a].append((rel, b))
            adj[b].append((rel + "_inv" if rel == "contains" else rel, a))
    # text attachment as a node-local attribute
    txt_deg = collections.Counter()
    for cid, tid in raw.get("labelled_by", []):
        txt_deg[cid] += 1
    out = [collections.Counter(lab)]
    cur = lab
    for _ in range(depth):
        nxt = []
        for i in range(n):
            nb = sorted((rel, cur[j]) for rel, j in adj.get(i, []))
            key = repr((cur[i], min(txt_deg.get(i, 0), 4), nb))
            nxt.append(hashlib.sha1(key.encode()).hexdigest()[:12])
        cur = nxt
        out.append(collections.Counter(cur))
    return out, cur


def main() -> None:
    pairs = json.loads((A / "block_pairs.json").read_text())["pairs"]
    rows = []
    eom_detail = None
    for p in pairs:
        pid = p["pair_id"]
        gs = {}
        for side in ("left", "right"):
            desc = json.loads((A / "descriptions" / pid / side / "vector_block.json").read_text())
            gs[side] = R.build_relation_graph(desc)
        wl_l, _ = wl_labels(gs["left"])
        wl_r, _ = wl_labels(gs["right"])
        row = {"pair_id": pid, "changed": pid in CHANGED, "human": p["human_expected"]}
        for d in range(len(wl_l)):
            row[f"wl{d}_jaccard"] = round(R.weighted_jaccard(wl_l[d], wl_r[d]), 6)
        rows.append(row)
        print(f"{pid:24s} wl0={row['wl0_jaccard']:.4f} wl1={row['wl1_jaccard']:.4f} "
              f"wl2={row['wl2_jaccard']:.4f}")
        if pid == "eom_singleline_changed":
            # which WL-1 labels changed multiplicity, and what are they made of?
            l1, r1 = wl_l[1], wl_r[1]
            keys = set(l1) | set(r1)
            chg = sorted(((r1.get(k, 0) - l1.get(k, 0), k, l1.get(k, 0), r1.get(k, 0))
                          for k in keys), key=lambda x: -abs(x[0]))
            eom_detail = {
                "wl1_changes": [{"label": k, "left": a, "right": b, "delta": d}
                                for d, k, a, b in chg[:40]],
                "wl1_multiplicity_2_to_4": [k for k in keys
                                            if l1.get(k, 0) == 2 and r1.get(k, 0) == 4],
                "wl1_labels_left": len(l1), "wl1_labels_right": len(r1),
                "wl1_count_2_left": sum(1 for k, v in l1.items() if v == 2),
                "wl1_count_4_right": sum(1 for k, v in r1.items() if v == 4),
            }

    def margin(key, changed_set):
        ch = [r[key] for r in rows if r["pair_id"] in changed_set]
        un = [r[key] for r in rows if r["pair_id"] not in changed_set]
        return {"max_changed": max(ch), "min_unchanged": min(un),
                "margin": round(min(un) - max(ch), 6), "separated": min(un) > max(ch),
                "min_unchanged_pair": min((r for r in rows if r["pair_id"] not in changed_set),
                                          key=lambda r: r[key])["pair_id"]}

    summary = {}
    for key in ("wl0_jaccard", "wl1_jaccard", "wl2_jaccard"):
        summary[key + "::all_changed"] = margin(key, CHANGED)
        summary[key + "::geometry_changed_only"] = margin(key, GEOM_CHANGED)
    print("\n--- WL separation margins ---")
    for k, v in summary.items():
        print(f"{k:42s} margin={v['margin']:+.4f} max_changed={v['max_changed']:.4f} "
              f"min_unchanged={v['min_unchanged']:.4f}({v['min_unchanged_pair']})")

    if eom_detail:
        print("\n--- eom WL-1 detail ---")
        print(json.dumps({k: v for k, v in eom_detail.items() if k != "wl1_changes"},
                         ensure_ascii=False, indent=1))
        for c in eom_detail["wl1_changes"][:12]:
            print(f"  {c['delta']:+4d}  {c['label']}  {c['left']} -> {c['right']}")

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "relgraph_wl.json").write_text(json.dumps(
        {"research_only": True, "rows": rows, "separation": summary, "eom": eom_detail},
        ensure_ascii=False, indent=1))
    print("\nwrote", OUT / "relgraph_wl.json")


if __name__ == "__main__":
    main()
