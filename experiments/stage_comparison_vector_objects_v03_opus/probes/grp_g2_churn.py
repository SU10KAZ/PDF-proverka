# -*- coding: utf-8 -*-
"""G2 — OBJECT BOUNDARY CHURN on REAL cross-revision pairs [REAL].

For every object on side A: how many objects on side B overlap its ink, and what
share of its ink lands in the largest of them.  Reported as a distribution
(1:1 / split / merge / mixed / lost), never as an average.

Correspondence between the two sides is geometric, in PDF points at EQUAL PHYSICAL
SCALE (mine M4: fitting each block to its own bbox inflates the residual 4.8x).
Translation is estimated by a two-stage grid search seeded with the FFT registration
shift the `mine` probe already measured.

Objects touching the block border are reported separately (mine M5: 36.5 % of pairs
with a non-zero residual have ALL big difference components on the crop border).
Usage:  grp_g2_churn.py
"""
from __future__ import annotations
import json, math, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import grp_match as M

TOL_PT = 0.8            # perpendicular tolerance, PDF points
SAMPLE_POINTS = 3


def mark_border(layer, clip, pad_frac=0.02):
    w, h = clip[2] - clip[0], clip[3] - clip[1]
    px, py = max(2.0, pad_frac * w), max(2.0, pad_frac * h)
    for o in layer.objects:
        b = o["bbox"]
        o["border"] = (b[0] <= clip[0] + px or b[1] <= clip[1] + py or
                       b[2] >= clip[2] - px or b[3] >= clip[3] - py)


def side_layer(side):
    ex = G.F.extract_block(str(G.ROOT / side["pdf"]), side["page_index"], side["coords_px"],
                           side["page_px"][0], side["page_px"][1])
    L = G.layer_of(ex.segments, ex.texts)
    clip = ex.frame["clip_display"]
    mark_border(L, clip)
    return ex, L, clip


def main():
    pairs = json.load(open(G.ART / "mine_pairs.json", encoding="utf-8"))["pairs"]
    out = []
    for p in pairs:
        t0 = time.time()
        row = {"pair_id": p["pair_id"], "discipline": p["discipline"],
               "classes": p["classes"], "expected": p["expected_verdict"],
               "label_confidence": p["label_confidence"]}
        try:
            exA, LA, clipA = side_layer(p["side_a"])
            exB, LB, clipB = side_layer(p["side_b"])
        except Exception as e:
            row["error"] = repr(e)
            out.append(row)
            continue
        if not exA.segments or not exB.segments:
            row["error"] = "no vector geometry on one side"
            out.append(row)
            continue
        # Both revisions of the same sheet live in the same page coordinate system,
        # so the correspondence is a pure translation in PDF points.  We seed the search
        # with 0, with the crop-origin difference and with mine's FFT shift, and measure.
        base = (clipA[0] - clipB[0], clipA[1] - clipB[1])
        seed = p["screen_signals"].get("registration_shift_pt") or [0.0, 0.0]
        seeds = {(0.0, 0.0), (base[0], base[1]),
                 (float(seed[0]), float(seed[1])),
                 (base[0] + float(seed[0]), base[1] + float(seed[1]))}
        dx, dy, score = M.register(exA.segments, exB.segments, seeds)
        off = (dx, dy)
        rowsAB = M.churn_rows(LA, exA.segments, LB, exB.segments, off)
        rowsBA = M.churn_rows(LB, exB.segments, LA, exA.segments, (-off[0], -off[1]))
        row.update({
            "n_seg_a": len(exA.segments), "n_seg_b": len(exB.segments),
            "n_obj_a": len(LA.objects), "n_obj_b": len(LB.objects),
            "d_obj": len(LB.objects) - len(LA.objects),
            "S_a": round(LA.S, 2), "S_b": round(LB.S, 2),
            "scale_src_a": LA.scale_source, "scale_src_b": LB.scale_source,
            "reg_offset": [round(off[0], 3), round(off[1], 3)], "reg_score": round(score, 4),
            "unmatched_ink_share_a": round(sum(r["len"] * r["unmatched_share"] for r in rowsAB) / max(sum(r["len"] for r in rowsAB), 1e-9), 5),
            "churn_ab": {k: round(v, 5) for k, v in M.classify(rowsAB).items()},
            "churn_ba": {k: round(v, 5) for k, v in M.classify(rowsBA).items()},
            "churn_ab_interior": {k: round(v, 5) for k, v in
                                  M.classify([r for r in rowsAB if not r["border"]]).items()},
            "churn_by_cls": {c: {k: round(v, 5) for k, v in
                                 M.classify([r for r in rowsAB if r["cls"] == c]).items()}
                             for c in sorted({r["cls"] for r in rowsAB})},
            "border_obj_share": round(sum(1 for o in LA.objects if o["border"]) /
                                      max(1, len(LA.objects)), 4),
            "t_sec": round(time.time() - t0, 1),
        })
        print(row["pair_id"], row["churn_ab"], row["reg_score"], flush=True)
        out.append(row)
    json.dump({"tol_pt": TOL_PT, "pairs": out},
              open(G.ART / "grp_boundary_churn_real.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
