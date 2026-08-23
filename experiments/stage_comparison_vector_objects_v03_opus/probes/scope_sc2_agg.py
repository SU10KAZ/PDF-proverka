# -*- coding: utf-8 -*-
"""scope · SC2/SC3 aggregation over artifacts/scope_runs/*.json."""
from __future__ import annotations
import json, statistics, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"


def med(v):
    return round(statistics.median(v), 3) if v else None


def main():
    rows = [json.load(open(p, encoding="utf-8")) for p in sorted((ART / "scope_runs").glob("T*.json"))]
    groups = {}
    for r in rows:
        for t in r["tags"]:
            g = t.split(":")[0]
            groups.setdefault(g, []).append(r)
            groups.setdefault(g + "|same_page_size", []).append(r) if r.get("page_rect_equal") else \
                groups.setdefault(g + "|page_size_changed", []).append(r)
    out = {"schema_version": "scope_sc2/1", "n_tasks": len(rows), "groups": {}}
    for g, rs in groups.items():
        ok = [r for r in rs if "n_records" in r["arm1_forced_1to1"] and "n_records" in r["arm2_scope_union"]]
        okp = [r for r in ok if "n_records" in r["arm3_page"]]
        if not ok:
            out["groups"][g] = {"n": len(rs), "n_usable": 0}
            continue
        a1 = [r["arm1_forced_1to1"] for r in ok]
        a2 = [r["arm2_scope_union"] for r in ok]
        a3 = [r["arm3_page"] for r in okp]
        d = {
            "n": len(rs), "n_usable": len(ok), "n_usable_page": len(okp),
            "arm1_forced_1to1": {
                "records_median": med([x["n_records"] for x in a1]),
                "records_big_median": med([x["n_records_big"] for x in a1]),
                "added_removed_big_median": med([x["added_removed_big"] for x in a1]),
                "added_removed_big_total": sum(x["added_removed_big"] for x in a1),
                "similarity_median": med([x["similarity"] for x in a1]),
                "changed_len_median": med([x["changed_len_total"] for x in a1]),
                "big_outside_roi_total": sum(x.get("n_big_outside_roi", 0) for x in a1),
                "len_outside_roi_share_median": med([
                    (x.get("len_outside_roi", 0) / x["changed_len_total"]) if x["changed_len_total"] else 0
                    for x in a1]),
                "t_sec_median": med([x["t_sec"] for x in a1]),
            },
            "arm2_scope_union": {
                "records_median": med([x["n_records"] for x in a2]),
                "records_big_median": med([x["n_records_big"] for x in a2]),
                "added_removed_big_median": med([x["added_removed_big"] for x in a2]),
                "added_removed_big_total": sum(x["added_removed_big"] for x in a2),
                "similarity_median": med([x["similarity"] for x in a2]),
                "changed_len_median": med([x["changed_len_total"] for x in a2]),
                "t_sec_median": med([x["t_sec"] for x in a2]),
                "n_seg_median": med([x["n_seg_a"] + x["n_seg_b"] for x in a2]),
            },
            "arm3_page": {
                "records_median": med([x["n_records"] for x in a3]),
                "records_big_median": med([x["n_records_big"] for x in a3]),
                "similarity_median": med([x["similarity"] for x in a3]),
                "changed_len_median": med([x["changed_len_total"] for x in a3]),
                "big_outside_component_median": med([x.get("n_big_outside_roi", 0) for x in a3]),
                "share_records_outside_component_median": med([
                    (x.get("n_records_outside_roi", 0) / x["n_records"]) if x["n_records"] else 0
                    for x in a3]),
                "t_sec_median": med([x["t_sec"] for x in a3]),
                "n_seg_median": med([x["n_seg_a"] + x["n_seg_b"] for x in a3]),
            },
            "delta_big_records_arm1_minus_arm2": med([
                r["arm1_forced_1to1"]["n_records_big"] - r["arm2_scope_union"]["n_records_big"] for r in ok]),
            "pairs_where_arm1_has_more_big": sum(
                1 for r in ok if r["arm1_forced_1to1"]["n_records_big"] > r["arm2_scope_union"]["n_records_big"]),
            "pairs_where_arm2_has_more_big": sum(
                1 for r in ok if r["arm2_scope_union"]["n_records_big"] > r["arm1_forced_1to1"]["n_records_big"]),
            "dropped_ink_len_median": med([r["dropped_blocks_ink_len"] for r in ok]),
            "dropped_ink_share_median": med([
                r["dropped_blocks_ink_len"] / max(1e-9, r["dropped_blocks_ink_len"]
                                                  + r["arm1_forced_1to1"]["ink_len_a"]
                                                  + r["arm1_forced_1to1"]["ink_len_b"]) for r in ok]),
            "page_rect_equal": sum(1 for r in rs if r.get("page_rect_equal")),
        }
        out["groups"][g] = d
    # per-task table
    out["per_task"] = [{
        "task_id": r["task_id"], "kind": r["kind"], "tags": r["tags"], "disc": r["discipline"],
        "doc": r["doc_id"], "page_a": r["page_a"], "page_b": r["page_b"], "n_a": r["n_a"], "n_b": r["n_b"],
        "arm1": {k: r["arm1_forced_1to1"].get(k) for k in
                 ("n_records", "n_records_big", "added_removed_big", "similarity",
                  "changed_len_total", "n_big_outside_roi", "len_outside_roi", "error")},
        "arm2": {k: r["arm2_scope_union"].get(k) for k in
                 ("n_records", "n_records_big", "added_removed_big", "similarity",
                  "changed_len_total", "error")},
        "arm3": {k: r["arm3_page"].get(k) for k in
                 ("n_records", "n_records_big", "similarity", "changed_len_total",
                  "n_records_outside_roi", "n_big_outside_roi", "error")},
        "dropped_ink": r["dropped_blocks_ink_len"], "dropped_seg": r["dropped_blocks_n_seg"],
        "page_rect_equal": r.get("page_rect_equal"),
    } for r in rows]
    json.dump(out, open(ART / "scope_sc2_summary.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    for g, d in out["groups"].items():
        print(g, json.dumps(d, ensure_ascii=False)[:600])


if __name__ == "__main__":
    main()
