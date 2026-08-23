# -*- coding: utf-8 -*-
"""visprep V1(a) — REAL candidates from the deterministic layer's OWN uncertainties.

Source of a candidate is never invented: it is a record the object-level ledger
(`loc_common.ledger`, probe `loc`) actually published on a real cross-revision pair
of prepared graphic blocks, i.e. a place where the vector layer says "here is ink I
could not pair".  Two families:

  R1  record on a pair a human labelled GRAPHIC_CHANGE     (probable answer: different)
  R2  record on a pair a human labelled NO_GRAPHIC_CHANGE  (probable answer: same)

"Probable" only — every candidate is verified by eye before it becomes a case.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import vis_common as V   # noqa: E402

PAD_FRAC = 0.18
PAD_MIN = 6.0
MIN_SIDE = 26.0
MAX_SIDE = 900.0
TARGET = 700


def window(bbox_pt):
    x0, y0, x1, y1 = bbox_pt
    m = max(x1 - x0, y1 - y0)
    pad = max(PAD_MIN, PAD_FRAC * m)
    r = V.pad_rect(bbox_pt, pad, MIN_SIDE)
    w, h = r[2] - r[0], r[3] - r[1]
    # keep the aspect sane: a hairline record must not become a 200:1 strip
    if w > 3 * h:
        cy = (r[1] + r[3]) / 2.0
        r[1], r[3] = cy - w / 6.0, cy + w / 6.0
    if h > 3 * w:
        cx = (r[0] + r[2]) / 2.0
        r[0], r[2] = cx - h / 6.0, cx + h / 6.0
    return r


def main():
    mp, lp = V.mine_pairs(), V.loc_pairs()
    V.CAND_DIR.mkdir(parents=True, exist_ok=True)
    out = []
    for pid, L in lp.items():
        if pid not in mp:
            continue
        exp = L["expected"]
        if exp not in ("NO_GRAPHIC_CHANGE", "GRAPHIC_CHANGE"):
            continue
        recs = [r for r in (L.get("records_top") or []) if not r.get("at_boundary")]
        recs.sort(key=lambda r: -float(r.get("change_len") or 0))
        recs = recs[:3]
        off = L.get("reg_offset") or [0.0, 0.0]
        P = mp[pid]
        for k, r in enumerate(recs):
            big = max(r["bbox_pt"][2] - r["bbox_pt"][0], r["bbox_pt"][3] - r["bbox_pt"][1])
            if big > MAX_SIDE:
                continue
            wA = window(r["bbox_pt"])
            wB = [wA[0] - off[0], wA[1] - off[1], wA[2] - off[0], wA[3] - off[1]]
            cid = f"R_{pid}_{k}"
            try:
                szL = V.render_region(P["side_a"], wA, V.CAND_DIR / f"{cid}_L.png", TARGET)
                szR = V.render_region(P["side_b"], wB, V.CAND_DIR / f"{cid}_R.png", TARGET)
                V.montage(V.CAND_DIR / f"{cid}_L.png", V.CAND_DIR / f"{cid}_R.png",
                          V.CAND_DIR / f"{cid}_M.png")
            except Exception as e:                       # noqa: BLE001
                print("FAIL", cid, e)
                continue
            out.append({
                "cand_id": cid, "pair_id": pid, "family": "R1" if exp == "GRAPHIC_CHANGE" else "R2",
                "discipline": L["discipline"], "pair_expected": exp,
                "pair_label_confidence": L.get("label_confidence"),
                "pair_classes": L.get("classes"), "pair_human": (mp[pid].get("human_expected_ru") or "")[:400],
                "rec_type": r["type"], "change_len": r.get("change_len"),
                "len_lost": r.get("len_lost"), "len_new": r.get("len_new"),
                "n_seg_lost": r.get("n_seg_lost"), "n_seg_new": r.get("n_seg_new"),
                "objects_a": [(o.get("object_id"), o.get("cls"), o.get("label")) for o in (r.get("objects_a") or [])][:6],
                "rect_a_pt": wA, "rect_b_pt": wB, "reg_offset": off,
                "px": [szL, szR],
            })
            print(cid, exp, r["type"], round(float(r.get("change_len") or 0), 1), szL)
    with open(V.ART / "vis_real_candidates.json", "w", encoding="utf-8") as fh:
        json.dump({"n": len(out), "candidates": out}, fh, ensure_ascii=False, indent=1)
    print("candidates:", len(out))


if __name__ == "__main__":
    main()
