# -*- coding: utf-8 -*-
"""scope · SC2 — cross-arm bookkeeping: which records of the forced 1:1 comparison survive
when the scope is right, and which real changes the forced 1:1 comparison never saw.

Records of every arm live in the SAME page display coordinates, so they can be matched by
bbox overlap.  Only records >= 20 pt of ink are counted (the working threshold loc L12
measured); the record lists are the top 25 per arm, which covers every big record in all
but a few very noisy tasks (`truncated` flags them).

Writes artifacts/scope_sc2_cross.json
"""
from __future__ import annotations
import json, statistics
from pathlib import Path
HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
BIG = 20.0


def ov(a, b, pad=2.0):
    ix = min(a[2], b[2]) - max(a[0], b[0]) + 2 * pad
    iy = min(a[3], b[3]) - max(a[1], b[1]) + 2 * pad
    return ix > 0 and iy > 0


def inside(bb, r, pad=1.0):
    return bb[0] >= r[0] - pad and bb[1] >= r[1] - pad and bb[2] <= r[2] + pad and bb[3] <= r[3] + pad


def big_of(arm):
    return [r for r in arm.get("rec_top", []) if r[2] >= BIG]


def main():
    rows = [json.load(open(p, encoding="utf-8")) for p in sorted((ART / "scope_runs").glob("T*.json"))]
    out = []
    for r in rows:
        a1, a2 = r["arm1_forced_1to1"], r["arm2_scope_union"]
        if "n_records" not in a1 or "n_records" not in a2:
            out.append({"task_id": r["task_id"], "tags": r["tags"], "kind": r["kind"],
                        "error": a1.get("error") or a2.get("error")})
            continue
        roi = [max(a1["clip_a"][0], a1["clip_b"][0]), max(a1["clip_a"][1], a1["clip_b"][1]),
               min(a1["clip_a"][2], a1["clip_b"][2]), min(a1["clip_a"][3], a1["clip_b"][3])]
        B1, B2 = big_of(a1), big_of(a2)
        surv = [x for x in B1 if any(ov(x[1], y[1]) for y in B2)]
        gone = [x for x in B1 if not any(ov(x[1], y[1]) for y in B2)]
        new = [x for x in B2 if not any(ov(y[1], x[1]) for y in B1)]
        out.append({
            "task_id": r["task_id"], "kind": r["kind"], "tags": r["tags"],
            "disc": r["discipline"], "doc": r["doc_id"], "n_a": r["n_a"], "n_b": r["n_b"],
            "arm1_big": len(B1), "arm2_big": len(B2),
            "arm1_big_added_removed": sum(1 for x in B1 if x[0] in ("ADDED_OBJECT", "REMOVED_OBJECT")),
            "arm2_big_added_removed": sum(1 for x in B2 if x[0] in ("ADDED_OBJECT", "REMOVED_OBJECT")),
            "arm1_big_outside_shared_region": sum(1 for x in B1 if not inside(x[1], roi)),
            "arm1_big_ink_outside_shared_region": round(sum(x[2] for x in B1 if not inside(x[1], roi)), 1),
            "arm1_big_at_boundary": sum(1 for x in B1 if x[3]),
            "arm2_big_at_boundary": sum(1 for x in B2 if x[3]),
            "false_added_removed": sum(1 for x in gone if x[0] in ("ADDED_OBJECT", "REMOVED_OBJECT")),
            "false_added_removed_ink": round(sum(x[2] for x in gone
                                                 if x[0] in ("ADDED_OBJECT", "REMOVED_OBJECT")), 1),
            "page_rect_equal": r.get("page_rect_equal"),
            "survives_right_scope": len(surv),
            "vanishes_with_right_scope": len(gone),
            "vanishes_ink": round(sum(x[2] for x in gone), 1),
            "invisible_to_forced_1to1": len(new),
            "invisible_ink": round(sum(x[2] for x in new), 1),
            "sim1": a1["similarity"], "sim2": a2["similarity"],
            "truncated": bool(a1["n_records"] > 25 or a2["n_records"] > 25),
            "dropped_ink": r["dropped_blocks_ink_len"], "dropped_seg": r["dropped_blocks_n_seg"],
        })
    agg = {}
    for tag in ("multi", "nested", "control_aligned", "m14",
                "multi|same_page_size", "nested|same_page_size",
                "control_aligned|same_page_size", "m14|same_page_size"):
        base = tag.split("|")[0]
        sub = [x for x in out if any(t.split(":")[0] == base for t in x.get("tags", [])) and "error" not in x]
        if tag.endswith("same_page_size"):
            sub = [x for x in sub if x.get("page_rect_equal")]
        if not sub:
            continue
        agg[tag] = {
            "n": len(sub),
            "arm1_big_median": round(statistics.median([x["arm1_big"] for x in sub]), 2),
            "arm2_big_median": round(statistics.median([x["arm2_big"] for x in sub]), 2),
            "vanishes_total": sum(x["vanishes_with_right_scope"] for x in sub),
            "vanishes_median": round(statistics.median([x["vanishes_with_right_scope"] for x in sub]), 2),
            "tasks_with_at_least_one_vanishing": sum(1 for x in sub if x["vanishes_with_right_scope"]),
            "vanishing_share_of_arm1_records": round(
                sum(x["vanishes_with_right_scope"] for x in sub) / max(1, sum(x["arm1_big"] for x in sub)), 4),
            "vanishing_ink_share": round(
                sum(x["vanishes_ink"] for x in sub) /
                max(1e-9, sum(x["vanishes_ink"] + x["invisible_ink"] for x in sub) or 1), 4),
            "invisible_total": sum(x["invisible_to_forced_1to1"] for x in sub),
            "tasks_with_invisible_change": sum(1 for x in sub if x["invisible_to_forced_1to1"]),
            "arm1_outside_shared_total": sum(x["arm1_big_outside_shared_region"] for x in sub),
            "dropped_ink_median": round(statistics.median([x["dropped_ink"] for x in sub]), 1),
            "dropped_seg_median": round(statistics.median([x["dropped_seg"] for x in sub]), 1),
            "truncated_tasks": sum(1 for x in sub if x["truncated"]),
            "false_added_removed_total": sum(x["false_added_removed"] for x in sub),
            "false_added_removed_median": round(statistics.median([x["false_added_removed"] for x in sub]), 2),
            "tasks_with_false_added_removed": sum(1 for x in sub if x["false_added_removed"]),
            "false_added_removed_ink_total": round(sum(x["false_added_removed_ink"] for x in sub), 1),
            "arm1_big_at_boundary_total": sum(x["arm1_big_at_boundary"] for x in sub),
            "arm2_big_at_boundary_total": sum(x["arm2_big_at_boundary"] for x in sub),
        }
    json.dump({"schema_version": "scope_sc2_cross/1", "threshold_pt": BIG,
               "aggregate": agg, "per_task": out},
              open(ART / "scope_sc2_cross.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(json.dumps(agg, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
