# -*- coding: utf-8 -*-
"""scope · SC1 aggregation — the shape of the block-to-block relation, one file."""
from __future__ import annotations
import json, statistics
from pathlib import Path
from collections import Counter
HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"


def area(r):
    return max(0.0, r[2] - r[0]) * max(0.0, r[3] - r[1])


def inter(a, b):
    return area((max(a[0], b[0]), max(a[1], b[1]), min(a[2], b[2]), min(a[3], b[3])))


def iou(a, b):
    i = inter(a, b)
    u = area(a) + area(b) - i
    return i / u if u > 0 else 0.0


def main():
    rows = [json.loads(l) for l in open(ART / "scope_components.jsonl", encoding="utf-8")]
    cens = json.load(open(ART / "scope_relation_census.json", encoding="utf-8"))
    kinds = Counter(r["kind"] for r in rows)
    matched = [r for r in rows if "orphan" not in r["kind"]]
    multi = [r for r in rows if r["kind"] in ("1:N", "N:1", "N:M")]

    # structure of the multi-block components
    struct = Counter()
    cover = []
    for r in rows:
        if r["kind"] not in ("1:N", "N:1"):
            continue
        if r["kind"] == "N:1":
            kids = [b["norm"] for b in r["blocks_a"]]; par = r["blocks_b"][0]["norm"]
        else:
            kids = [b["norm"] for b in r["blocks_b"]]; par = r["blocks_a"][0]["norm"]
        ins = [inter(k, par) / area(k) if area(k) else 0 for k in kids]
        mut = max((inter(kids[i], kids[j]) / min(area(kids[i]), area(kids[j]))
                   for i in range(len(kids)) for j in range(i + 1, len(kids))), default=0)
        if min(ins) >= 0.9 and mut <= 0.10:
            struct[(r["kind"], "tiling_split_or_merge")] += 1
            cover.append(sum(inter(k, par) for k in kids) / area(par) if area(par) else 0)
        elif min(ins) >= 0.9:
            struct[(r["kind"], "nested_but_children_overlap")] += 1
        elif max(ins) >= 0.9:
            struct[(r["kind"], "one_child_nested_rest_partial")] += 1
        else:
            struct[(r["kind"], "loose_overlap_chain")] += 1

    # what a greedy 1:1 matcher throws away inside a multi component
    drop_share, dropped_blocks, tot_blocks = [], 0, 0
    for r in multi:
        A = [b["norm"] for b in r["blocks_a"]]; B = [b["norm"] for b in r["blocks_b"]]
        cand = sorted(((iou(A[i], B[j]), i, j) for i in range(len(A)) for j in range(len(B))), reverse=True)
        ua, ub = set(), set()
        for v, i, j in cand:
            if v <= 0.05 or i in ua or j in ub:
                continue
            ua.add(i); ub.add(j)
        dr = [A[i] for i in range(len(A)) if i not in ua] + [B[j] for j in range(len(B)) if j not in ub]
        tot = sum(area(x) for x in A) + sum(area(x) for x in B)
        drop_share.append(sum(area(x) for x in dr) / tot if tot else 0)
        dropped_blocks += len(dr); tot_blocks += len(A) + len(B)

    blocks = {"total": sum(r["n_a"] + r["n_b"] for r in rows)}
    for k in kinds:
        blocks[k] = sum(r["n_a"] + r["n_b"] for r in rows if r["kind"] == k)

    per_disc = {}
    for r in rows:
        d = per_disc.setdefault(r["discipline"], Counter())
        d[r["kind"]] += 1
    per_disc_out = {}
    for d, c in per_disc.items():
        m = sum(v for k, v in c.items() if "orphan" not in k)
        nn = c["1:N"] + c["N:1"] + c["N:M"]
        per_disc_out[d] = {"matched_components": m, "non_1to1": nn,
                           "share_non_1to1": round(nn / m, 4) if m else None,
                           "nested_1to1": c["1:1_nested"], "partial_1to1": c["1:1_partial"],
                           "orphans": c["1:0_orphan_a"] + c["0:1_orphan_b"]}

    out = {
        "schema_version": "scope_sc1/1",
        "population": {
            "version_pairs": cens["summary"]["version_pairs"],
            "matched_sheets": cens["summary"]["pages"],
            "prepared_graphic_blocks_a": cens["summary"]["blocks_a"],
            "prepared_graphic_blocks_b": cens["summary"]["blocks_b"],
            "components_total": len(rows),
            "components_matched": len(matched),
        },
        "kinds": dict(kinds),
        "shares_of_matched_components": {k: round(v / len(matched), 4)
                                         for k, v in kinds.items() if "orphan" not in k},
        "non_1to1": {
            "components": len(multi),
            "share_of_matched_components": round(len(multi) / len(matched), 4),
            "plus_geometric_nesting_1to1": kinds["1:1_nested"],
            "plus_partial_overlap_1to1": kinds["1:1_partial"],
            "share_of_matched_components_not_a_clean_1to1": round(
                (len(multi) + kinds["1:1_nested"] + kinds["1:1_partial"]) / len(matched), 4),
        },
        "orphans": {"a_side": kinds["1:0_orphan_a"], "b_side": kinds["0:1_orphan_b"],
                    "share_of_all_components": round(
                        (kinds["1:0_orphan_a"] + kinds["0:1_orphan_b"]) / len(rows), 4)},
        "blocks": blocks,
        "block_shares": {k: round(v / blocks["total"], 4) for k, v in blocks.items() if k != "total"},
        "multi_structure": {f"{k[0]}|{k[1]}": v for k, v in sorted(struct.items())},
        "tiling_coverage_of_the_single_block": {
            "median": round(statistics.median(cover), 3) if cover else None,
            "p10": round(sorted(cover)[len(cover) // 10], 3) if cover else None, "n": len(cover)},
        "greedy_1to1_inside_multi_components": {
            "blocks_dropped": dropped_blocks, "blocks_total": tot_blocks,
            "share_blocks_dropped": round(dropped_blocks / tot_blocks, 4),
            "share_of_component_area_dropped_median": round(statistics.median(drop_share), 4),
        },
        "category_mix": {
            "multi_components": dict(Counter(b["cat"] for r in multi for b in r["blocks_a"] + r["blocks_b"])),
            "aligned_1to1": dict(Counter(b["cat"] for r in rows if r["kind"] == "1:1_aligned"
                                         for b in r["blocks_a"] + r["blocks_b"])),
        },
        "per_discipline": per_disc_out,
        "edge_threshold_sensitivity": cens["sensitivity_edge_threshold"],
        "rotation": {"sheets_with_different_rotate": cens["summary"]["rot_mismatch_pages"],
                     "blocks_on_them": cens["summary"]["rot_mismatch_blocks"]},
    }
    json.dump(out, open(ART / "scope_sc1_summary.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(json.dumps({k: out[k] for k in ("population", "kinds", "non_1to1", "orphans",
                                          "block_shares", "multi_structure",
                                          "greedy_1to1_inside_multi_components")},
                     ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
