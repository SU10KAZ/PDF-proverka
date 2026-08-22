#!/usr/bin/env python3
"""Rank the relation types and measure the honest "unbound" fallback.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.txgeo_rank
"""
from __future__ import annotations

import collections
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
REL = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts/txgeo_relations/line"
ART = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"

TYPES = ["dimension_interval", "dimension_line_only", "leader", "symbol_cluster", "enclosure_tight",
         "contour_caption", "repeated_label", "grid_cell", "between_extension_lines",
         "band_association", "along_line"]
SELF_VERIFIED = {"ar_plan", "ar_wall_sections", "fresh_ar_lintels", "fresh_kj_sections", "ss_plan_dense", "vk_nodes"}


def main() -> None:
    hits = collections.Counter()
    uniq = collections.Counter()
    units = 0
    leader_total = leader_resolved = leader_single = 0
    safe_bound = 0
    any_bound = 0
    per_block = {}
    for path in sorted(REL.glob("*/left.json")):
        res = json.loads(path.read_text(encoding="utf-8"))
        block = path.parent.name
        b_units = len(res["units"])
        b_safe = 0
        units += b_units
        for u in res["units"]:
            for t in TYPES:
                r = u["relations"].get(t)
                if r and r.get("hit"):
                    hits[t] += 1
                    if r.get("unique"):
                        uniq[t] += 1
            lr = u["relations"].get("leader")
            if lr and lr.get("hit"):
                leader_total += 1
                if lr.get("resolved"):
                    leader_resolved += 1
                if lr.get("leaders") == 1:
                    leader_single += 1
            # "safely bound" = at least one UNIQUE hit of a type whose corpus uniqueness|hit >= 0.5
            safe_types = ("dimension_line_only", "symbol_cluster", "contour_caption",
                          "repeated_label", "enclosure_tight")
            if any(u["relations"].get(t, {}).get("hit") and u["relations"][t].get("unique") for t in safe_types):
                safe_bound += 1
                b_safe += 1
            if u["primary"] != "unbound":
                any_bound += 1
        per_block[block] = {"units": b_units, "safely_bound": b_safe,
                            "safely_bound_share": round(b_safe / b_units, 3) if b_units else None}

    table = []
    for t in TYPES:
        table.append({
            "relation": t,
            "hits": hits[t],
            "hit_rate": round(hits[t] / units, 3),
            "unique_hits": uniq[t],
            "uniqueness_given_hit": round(uniq[t] / hits[t], 3) if hits[t] else None,
        })
    table.sort(key=lambda r: -(r["uniqueness_given_hit"] or 0))
    out = {
        "left_sides_only": True,
        "units": units,
        "ranking_by_uniqueness": table,
        "leader": {
            "hits": leader_total,
            "tip_referent_resolved": leader_resolved,
            "resolved_share": round(leader_resolved / leader_total, 3) if leader_total else None,
            "single_leader_chain": leader_single,
            "single_leader_share": round(leader_single / leader_total, 3) if leader_total else None,
        },
        "coverage": {
            "any_relation_primary_not_unbound": any_bound,
            "any_share": round(any_bound / units, 3),
            "safely_bound": safe_bound,
            "safely_bound_share": round(safe_bound / units, 3),
            "honest_unbound_share": round(1 - safe_bound / units, 3),
        },
        "per_block_safely_bound": per_block,
    }
    (ART / "txgeo_ranking.json").write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"units (left sides only) = {units}")
    print(f"{'relation':26s} {'hits':>6s} {'hit_rate':>9s} {'unique':>7s} {'uniq|hit':>9s}")
    for r in table:
        print(f"{r['relation']:26s} {r['hits']:6d} {r['hit_rate']:9.3f} {r['unique_hits']:7d} {str(r['uniqueness_given_hit']):>9s}")
    print()
    print("leader:", json.dumps(out["leader"], ensure_ascii=False))
    print("coverage:", json.dumps(out["coverage"], ensure_ascii=False))
    print()
    for b, v in sorted(per_block.items()):
        print(f"  {b:24s} units={v['units']:5d} safely_bound={v['safely_bound']:5d} ({v['safely_bound_share']})")


if __name__ == "__main__":
    main()
