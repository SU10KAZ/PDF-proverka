# -*- coding: utf-8 -*-
"""[REAL] Is 'touches the crop border' the right attribution, or is 'thin strip along the
border' better?

mine M5: in 36.5 % of real pairs with a residual, every large component of the difference
touches the block border — the sheet frame line that fell inside one version's bbox and
not the other's.  Dropping every record that touches the border kills those false
positives, but it also kills real additions that happen to sit near the edge (measured
here).  The candidate refinement: a record is a border ARTEFACT only when it is a thin
strip hugging the border; anything with real thickness is a change.

    python probes/loc_border_rule.py
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))
import grp_common as G  # noqa: E402


def clip_of(side):
    fr = G.F.block_frame(str(G.ROOT / side["pdf"]), side["page_index"],
                         list(side["coords_px"]), side["page_px"][0], side["page_px"][1])
    return list(fr.clip_display)


def strip(rec_bbox, clip, thin_pt, along_frac):
    x0, y0, x1, y1 = rec_bbox
    w, h = x1 - x0, y1 - y0
    W, H = clip[2] - clip[0], clip[3] - clip[1]
    return ((h <= thin_pt and w >= along_frac * W) or
            (w <= thin_pt and h >= along_frac * H))


def main():
    P = json.load(open(ART / "loc_real_pairs.json", encoding="utf-8"))["pairs"]
    bench = {p["pair_id"]: p for p in
             json.load(open(ART / "mine_pairs.json", encoding="utf-8"))["pairs"]}
    # the run that would have stored every record was killed by the machine running out
    # of memory; the stored top-12 records are the LARGEST ones, so every threshold
    # T >= 20 pt is computed exactly from them and only smaller T would undercount.
    for p in P:
        if "error" in p:
            continue
        if "clip_a" not in p:
            p["clip_a"] = clip_of(bench[p["pair_id"]]["side_a"])
        if "rec_all" not in p:
            p["rec_all"] = [[r["type"], r["bbox_pt"], r["change_len"],
                             1 if r["at_boundary"] else 0,
                             r.get("len_lost", 0), r.get("len_new", 0)]
                            for r in p["records_top"]]
    grids = [(thin, frac, T) for thin in (2.0, 5.0, 10.0, 20.0)
             for frac in (0.2, 0.5, 0.8) for T in (20.0, 60.0, 200.0)]
    out = []
    for thin, frac, T in grids:
        tp = fp = 0
        pos = neg = 0
        fp_rec = 0
        for p in P:
            if "error" in p or "rec_all" not in p:
                continue
            exp = p["expected"]
            if exp not in ("GRAPHIC_CHANGE", "NO_GRAPHIC_CHANGE"):
                continue
            clip = p["clip_a"]
            keep = [r for r in p["rec_all"]
                    if r[2] >= T and not (r[3] and strip(r[1], clip, thin, frac))]
            if exp == "GRAPHIC_CHANGE":
                pos += 1
                tp += 1 if keep else 0
            else:
                neg += 1
                fp += 1 if keep else 0
                fp_rec += len(keep)
        out.append({"thin_pt": thin, "along_frac": frac, "T_pt": T,
                    "recall_pairs": round(tp / max(pos, 1), 4),
                    "false_alarm_pairs": round(fp / max(neg, 1), 4),
                    "false_records_per_quiet_pair": round(fp_rec / max(neg, 1), 3),
                    "n_pos": pos, "n_neg": neg})
    # per-pair detail at one setting for the report
    detail = []
    for p in P:
        if "error" in p or "rec_all" not in p:
            continue
        clip = p["clip_a"]
        keep = [r for r in p["rec_all"]
                if r[2] >= 20.0 and not (r[3] and strip(r[1], clip, 5.0, 0.5))]
        detail.append({"pair_id": p["pair_id"], "expected": p["expected"],
                       "classes": p["classes"],
                       "n_records": len(p["rec_all"]),
                       "n_after_rule": len(keep),
                       "kept_top": keep[:4],
                       "human": p["human"][:150]})
    json.dump({"note": "[REAL] border attribution sweep; ground truth = mine benchmark",
               "grid": out, "detail_thin5_frac0.5_T20": detail},
              open(ART / "loc_border_rule.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    best = sorted(out, key=lambda r: (-(r["recall_pairs"] - r["false_alarm_pairs"]),
                                      r["false_records_per_quiet_pair"]))[:8]
    for b in best:
        print(b)


if __name__ == "__main__":
    main()
