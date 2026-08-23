# -*- coding: utf-8 -*-
"""scope · SC2/SC3 — the same component compared in THREE frames.

    arm1 "forced_1to1"  : the greedy best-IoU block pair, each side clipped to its OWN
                          prepared block — what a contract without left_scope/right_scope
                          is forced to do;
    arm2 "scope_union"  : left_scope x right_scope — the union of the component's blocks
                          on both sides, the SAME physical rectangle read from both pages;
    arm3 "page"         : the whole sheet, no block boundaries at all.

Everything is read through v03_foundation; the comparator is loc_common.ledger, unchanged.

usage: scope_sc2_run.py <task_index>          (one component per process)
"""
from __future__ import annotations
import json, math, sys, time
from pathlib import Path
HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
ROOT = HERE.parents[2]
sys.path.insert(0, str(HERE))
import v03_foundation as F   # noqa
import grp_match as M        # noqa
import loc_common as L       # noqa
import fitz                  # noqa

MAX_SEG = 220000
BIG_THR = 20.0     # points of ink: the "real record" threshold loc measured (L12)


def px_rect_from_pt(rect_pt, page_rect, page_px):
    sx = page_px[0] / page_rect.width
    sy = page_px[1] / page_rect.height
    return [rect_pt[0] * sx, rect_pt[1] * sy, rect_pt[2] * sx, rect_pt[3] * sy]


def clip_pt(pdf, page_index, coords_px, page_px):
    fr = F.block_frame(pdf, page_index, coords_px, page_px[0], page_px[1])
    c = fr.clip_display
    return [c.x0, c.y0, c.x1, c.y1]


def union(rs):
    return [min(r[0] for r in rs), min(r[1] for r in rs),
            max(r[2] for r in rs), max(r[3] for r in rs)]


def inter_rect(a, b):
    return [max(a[0], b[0]), max(a[1], b[1]), min(a[2], b[2]), min(a[3], b[3])]


def area(r):
    return max(0.0, r[2] - r[0]) * max(0.0, r[3] - r[1])


def inside(bb, r, pad=1.0):
    return (bb[0] >= r[0] - pad and bb[1] >= r[1] - pad
            and bb[2] <= r[2] + pad and bb[3] <= r[3] + pad)


def summarise(led, region_of_interest=None):
    recs = led["records"]
    by = {}
    for r in recs:
        by[r["type"]] = by.get(r["type"], 0) + 1
    big = [r for r in recs if r["change_len"] >= BIG_THR]
    out = {
        "n_records": led["n_records"],
        "n_records_interior": led["n_records_interior"],
        "n_records_big": len(big),
        "by_type": by,
        "by_type_big": {t: sum(1 for r in big if r["type"] == t) for t in set(r["type"] for r in big)},
        "changed_len_total": led["changed_len_total"],
        "changed_len_big": round(sum(r["change_len"] for r in big), 2),
        "added_removed_len": round(sum(r["change_len"] for r in recs
                                       if r["type"] in ("ADDED_OBJECT", "REMOVED_OBJECT")), 2),
        "added_removed_big": sum(1 for r in big if r["type"] in ("ADDED_OBJECT", "REMOVED_OBJECT")),
        "similarity": led["scalar"]["ink_similarity"],
        "n_seg_a": led["scalar"]["n_seg_a"], "n_seg_b": led["scalar"]["n_seg_b"],
        "ink_len_a": led["scalar"]["ink_len_a"], "ink_len_b": led["scalar"]["ink_len_b"],
        "n_obj_a": led["counts"]["n_obj_a"], "n_obj_b": led["counts"]["n_obj_b"],
        "S": led["S"],
        "rec_top": [[r["type"], [round(v, 1) for v in r["bbox_pt"]], round(r["change_len"], 1),
                     1 if r["at_boundary"] else 0] for r in recs[:25]],
    }
    if region_of_interest is not None:
        out["n_records_outside_roi"] = sum(1 for r in recs if not inside(r["bbox_pt"], region_of_interest))
        out["len_outside_roi"] = round(sum(r["change_len"] for r in recs
                                           if not inside(r["bbox_pt"], region_of_interest)), 2)
        out["n_big_outside_roi"] = sum(1 for r in big if not inside(r["bbox_pt"], region_of_interest))
    return out


def run_arm(pdf_a, pi_a, rpx_a, ppx_a, pdf_b, pi_b, rpx_b, ppx_b, roi=None):
    t0 = time.time()
    exA = F.extract_block(pdf_a, pi_a, rpx_a, ppx_a[0], ppx_a[1])
    exB = F.extract_block(pdf_b, pi_b, rpx_b, ppx_b[0], ppx_b[1])
    if not exA.segments or not exB.segments:
        return {"error": "no vector geometry on one side",
                "n_seg_a": len(exA.segments), "n_seg_b": len(exB.segments)}
    if len(exA.segments) > MAX_SEG or len(exB.segments) > MAX_SEG:
        return {"error": "too dense", "n_seg_a": len(exA.segments), "n_seg_b": len(exB.segments)}
    ca, cb = exA.frame["clip_display"], exB.frame["clip_display"]
    seeds = {(0.0, 0.0), (ca[0] - cb[0], ca[1] - cb[1])}
    dx, dy, score = M.register(exA.segments, exB.segments, seeds)
    LA, LB, meta = L.layers(exA, exB)
    led = L.ledger(exA, exB, off=(dx, dy), LA=LA, LB=LB, meta=meta)
    s = summarise(led, roi)
    s["reg_offset"] = [round(dx, 2), round(dy, 2)]
    s["reg_score"] = round(score, 4)
    s["clip_a"] = [round(v, 1) for v in ca]
    s["clip_b"] = [round(v, 1) for v in cb]
    s["t_sec"] = round(time.time() - t0, 1)
    return s


def main():
    i = int(sys.argv[1])
    T = json.load(open(ART / "scope_tasks.json", encoding="utf-8"))["tasks"]
    t = T[i]
    out_dir = ART / "scope_runs"
    out_dir.mkdir(exist_ok=True)
    dst = out_dir / f"{t['task_id']}.json"
    if dst.exists():
        print("skip", t["task_id"])
        return
    row = {k: t[k] for k in ("task_id", "kind", "tags", "doc_id", "discipline", "ver_a", "ver_b",
                             "page_a", "page_b", "n_a", "n_b")}
    pdf_a = str(ROOT / t["pdf_a"]); pdf_b = str(ROOT / t["pdf_b"])
    pi_a = t["blocks_a"][0]["page_index"]; pi_b = t["blocks_b"][0]["page_index"]
    ppx_a = t["blocks_a"][0]["page_px"]; ppx_b = t["blocks_b"][0]["page_px"]
    pra = F.open_doc(pdf_a)[pi_a].rect
    prb = F.open_doc(pdf_b)[pi_b].rect
    row["page_rect_a"] = [round(pra.width, 2), round(pra.height, 2)]
    row["page_rect_b"] = [round(prb.width, 2), round(prb.height, 2)]
    row["page_rect_equal"] = bool(abs(pra.width - prb.width) < 1.0 and abs(pra.height - prb.height) < 1.0)

    A_clips = [clip_pt(pdf_a, b["page_index"], b["coords_px"], b["page_px"]) for b in t["blocks_a"]]
    B_clips = [clip_pt(pdf_b, b["page_index"], b["coords_px"], b["page_px"]) for b in t["blocks_b"]]
    uA, uB = union(A_clips), union(B_clips)
    U = union([uA, uB])
    row["union_a_pt"] = [round(v, 1) for v in uA]
    row["union_b_pt"] = [round(v, 1) for v in uB]
    row["union_pt"] = [round(v, 1) for v in U]

    # ---- arm 1: forced 1:1 (greedy best IoU inside the component, or the mine pair)
    fp = t.get("forced_pair")
    if fp:
        ia = [k for k, b in enumerate(t["blocks_a"]) if b["id"] == fp[0]][0]
        ib = [k for k, b in enumerate(t["blocks_b"]) if b["id"] == fp[1]][0]
    else:
        best = (-1, 0, 0)
        for k, ra in enumerate(A_clips):
            for m, rb in enumerate(B_clips):
                it = area(inter_rect(ra, rb))
                u = area(ra) + area(rb) - it
                v = it / u if u > 0 else 0
                if v > best[0]:
                    best = (v, k, m)
        ia, ib = best[1], best[2]
    row["forced_pair"] = [t["blocks_a"][ia]["id"], t["blocks_b"][ib]["id"]]
    row["forced_dropped_a"] = [b["id"] for k, b in enumerate(t["blocks_a"]) if k != ia]
    row["forced_dropped_b"] = [b["id"] for k, b in enumerate(t["blocks_b"]) if k != ib]
    roi1 = inter_rect(A_clips[ia], B_clips[ib])
    try:
        row["arm1_forced_1to1"] = run_arm(pdf_a, pi_a, t["blocks_a"][ia]["coords_px"], ppx_a,
                                          pdf_b, pi_b, t["blocks_b"][ib]["coords_px"], ppx_b,
                                          roi=roi1)
    except Exception as e:
        row["arm1_forced_1to1"] = {"error": repr(e)}

    # ink of the blocks a forced 1:1 comparison never looks at
    lost = 0.0
    nlost = 0
    try:
        for k, b in enumerate(t["blocks_a"]):
            if k == ia:
                continue
            ex = F.extract_block(pdf_a, pi_a, b["coords_px"], ppx_a[0], ppx_a[1])
            lost += sum(s["len"] for s in ex.segments); nlost += len(ex.segments)
        for m, b in enumerate(t["blocks_b"]):
            if m == ib:
                continue
            ex = F.extract_block(pdf_b, pi_b, b["coords_px"], ppx_b[0], ppx_b[1])
            lost += sum(s["len"] for s in ex.segments); nlost += len(ex.segments)
    except Exception as e:
        row["dropped_ink_error"] = repr(e)
    row["dropped_blocks_ink_len"] = round(lost, 1)
    row["dropped_blocks_n_seg"] = nlost

    # ---- arm 2: explicit scope (union of both sides), same physical rectangle
    try:
        ra = px_rect_from_pt(U, pra, ppx_a)
        rb = px_rect_from_pt(U, prb, ppx_b)
        row["arm2_scope_union"] = run_arm(pdf_a, pi_a, ra, ppx_a, pdf_b, pi_b, rb, ppx_b, roi=U)
    except Exception as e:
        row["arm2_scope_union"] = {"error": repr(e)}

    # ---- arm 3: whole page
    try:
        ra = [0, 0, ppx_a[0], ppx_a[1]]
        rb = [0, 0, ppx_b[0], ppx_b[1]]
        row["arm3_page"] = run_arm(pdf_a, pi_a, ra, ppx_a, pdf_b, pi_b, rb, ppx_b, roi=U)
    except Exception as e:
        row["arm3_page"] = {"error": repr(e)}

    json.dump(row, open(dst, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(t["task_id"], t["kind"], t["tags"],
          "arm1", row["arm1_forced_1to1"].get("n_records", row["arm1_forced_1to1"].get("error")),
          "arm2", row["arm2_scope_union"].get("n_records", row["arm2_scope_union"].get("error")),
          "arm3", row["arm3_page"].get("n_records", row["arm3_page"].get("error")), flush=True)


if __name__ == "__main__":
    main()
