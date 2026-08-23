# -*- coding: utf-8 -*-
"""visprep V1(b) — REAL candidates of the "same picture, different vector decomposition"
kind (role A) and whole-block views of the raster pairs (roles B/C).

For a pair whose two sides carry the SAME picture with a different number of vector
primitives (probe `mine`, M11), a window where the two sides disagree on primitive
count by >=1.5x while carrying the same amount of ink is, by construction, a place
where the deterministic layer cannot decide "one object or another decomposition".
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import vis_common as V           # noqa: E402
import v03_foundation as F       # noqa: E402

TARGET = 700


def ex_side(side):
    return F.extract_block(V.abspath(side["pdf"]), side["page_index"], side["coords_px"],
                           side["page_px"][0], side["page_px"][1])


def in_win(s, w):
    x0, y0, x1, y1 = w
    for p in (s["p0"], s["p1"]):
        if x0 <= p[0] <= x1 and y0 <= p[1] <= y1:
            return True
    return False


def scan(pair_id, win_pt=70.0, step=35.0, top=8):
    mp, lp = V.mine_pairs(), V.loc_pairs()
    P, L = mp[pair_id], lp[pair_id]
    off = L.get("reg_offset") or [0.0, 0.0]
    A, B = ex_side(P["side_a"]), ex_side(P["side_b"])
    fa = A.frame["clip_display"]
    x0, y0, x1, y1 = fa
    rows = []
    yy = y0
    while yy + win_pt <= y1:
        xx = x0
        while xx + win_pt <= x1:
            w = (xx, yy, xx + win_pt, yy + win_pt)
            wb = (xx - off[0], yy - off[1], xx + win_pt - off[0], yy + win_pt - off[1])
            sa = [s for s in A.segments if in_win(s, w)]
            sb = [s for s in B.segments if in_win(s, wb)]
            la = sum(s["len"] for s in sa)
            lb = sum(s["len"] for s in sb)
            if len(sa) >= 25 and len(sb) >= 25 and la > 60 and lb > 60:
                r_n = max(len(sa), len(sb)) / max(1, min(len(sa), len(sb)))
                r_l = max(la, lb) / max(1e-6, min(la, lb))
                rows.append({"win": w, "n_a": len(sa), "n_b": len(sb), "len_a": round(la, 1),
                             "len_b": round(lb, 1), "seg_ratio": round(r_n, 3),
                             "len_ratio": round(r_l, 4)})
            xx += step
        yy += step
    rows = [r for r in rows if r["len_ratio"] <= 1.03]
    rows.sort(key=lambda r: -r["seg_ratio"])
    picked, out = [], []
    for r in rows:
        w = r["win"]
        if any(not (w[2] <= q[0] or q[2] <= w[0] or w[3] <= q[1] or q[3] <= w[1]) for q in picked):
            continue
        picked.append(w)
        out.append(r)
        if len(out) >= top:
            break
    V.CAND_DIR.mkdir(parents=True, exist_ok=True)
    res = []
    for k, r in enumerate(out):
        w = list(r["win"])
        wb = [w[0] - off[0], w[1] - off[1], w[2] - off[0], w[3] - off[1]]
        cid = f"P_{pair_id}_{k}"
        szL = V.render_region(P["side_a"], w, V.CAND_DIR / f"{cid}_L.png", TARGET)
        szR = V.render_region(P["side_b"], wb, V.CAND_DIR / f"{cid}_R.png", TARGET)
        V.montage(V.CAND_DIR / f"{cid}_L.png", V.CAND_DIR / f"{cid}_R.png", V.CAND_DIR / f"{cid}_M.png")
        res.append({"cand_id": cid, "pair_id": pair_id, "family": "R3",
                    "discipline": L["discipline"], "pair_expected": L["expected"],
                    "rect_a_pt": w, "rect_b_pt": wb, "reg_offset": off, "px": [szL, szR], **r})
        print(cid, r["n_a"], r["n_b"], r["seg_ratio"], r["len_ratio"])
    return res


def whole(pair_id, target=900):
    mp = V.mine_pairs()
    P = mp[pair_id]
    V.CAND_DIR.mkdir(parents=True, exist_ok=True)
    a, b = P["side_a"], P["side_b"]
    for tag, side in (("L", a), ("R", b)):
        sx, sy = V.page_scale(V.abspath(side["pdf"]), side["page_index"],
                              side["page_px"][0], side["page_px"][1])
        F.render_block(V.abspath(side["pdf"]), side["page_index"], side["coords_px"],
                       side["page_px"][0], side["page_px"][1], target_px=target,
                       out_png=str(V.CAND_DIR / f"W_{pair_id}_{tag}.png"))
    V.montage(V.CAND_DIR / f"W_{pair_id}_L.png", V.CAND_DIR / f"W_{pair_id}_R.png",
              V.CAND_DIR / f"W_{pair_id}_M.png")
    print("whole", pair_id)


if __name__ == "__main__":
    what = sys.argv[1]
    if what == "scan":
        rows = []
        for pid in sys.argv[2].split(","):
            rows += scan(pid, win_pt=float(sys.argv[3]) if len(sys.argv) > 3 else 70.0)
        with open(V.ART / "vis_real_scan.json", "w", encoding="utf-8") as fh:
            json.dump({"n": len(rows), "candidates": rows}, fh, ensure_ascii=False, indent=1)
    else:
        for pid in sys.argv[2].split(","):
            whole(pid)
