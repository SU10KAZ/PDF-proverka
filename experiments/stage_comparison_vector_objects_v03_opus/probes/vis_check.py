# -*- coding: utf-8 -*-
"""visprep — independent pixel arbiter for the eye labels.

The eye is one observer and it makes mistakes (it made one here: a REAL case labelled
"different" turned out to be two byte-identical crops).  This arbiter answers a narrow
question: is there STRUCTURAL ink difference between the two crops, i.e. ink on one side
that no ink within 1 px explains on the other?  Antialiasing and hairline phase noise die
under the dilation; a removed or added element does not.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import fitz                     # noqa: E402
import numpy as np              # noqa: E402
import vis_common as V          # noqa: E402


def mask(png, thr=200):
    p = fitz.Pixmap(str(png))
    a = np.frombuffer(p.samples, dtype=np.uint8)
    a = a.reshape(p.height, p.stride)[:, : p.width * p.n].reshape(p.height, p.width, p.n)
    return (a[:, :, :3].mean(axis=2) < thr), p.width, p.height


def dilate(m, r=1):
    out = m.copy()
    for dy in range(-r, r + 1):
        for dx in range(-r, r + 1):
            out |= np.roll(np.roll(m, dy, axis=0), dx, axis=1)
    return out


def compare(l, r, dil=1):
    A, wa, ha = mask(l)
    B, wb, hb = mask(r)
    if (wa, ha) != (wb, hb):
        h, w = min(ha, hb), min(wa, wb)
        A, B = A[:h, :w], B[:h, :w]
    dA = dilate(A, dil)
    dB = dilate(B, dil)
    a_un = float((A & ~dB).sum()) / max(1, A.size)
    b_un = float((B & ~dA).sum()) / max(1, B.size)
    return {"raw_diff": round(float((A != B).mean()), 6),
            "ink_a": round(float(A.mean()), 6), "ink_b": round(float(B.mean()), 6),
            "unexplained_a": round(a_un, 6), "unexplained_b": round(b_un, 6),
            "structural": round(max(a_un, b_un), 6),
            "size_mismatch": (wa, ha) != (wb, hb)}


def main():
    which = sys.argv[1] if len(sys.argv) > 1 else "cases"
    rows = []
    if which == "cases":
        truth = json.load(open(V.ART / "vis_truth.json", encoding="utf-8"))["truth"]
        for t in truth:
            c = compare(V.CASES_DIR / f"{t['case_id']}_L.png", V.CASES_DIR / f"{t['case_id']}_R.png")
            flag = ""
            if t["truth"] == "DIFFERENT" and c["structural"] < 1e-5:
                flag = "CONFLICT: truth=DIFFERENT but no structural ink difference"
            if t["truth"] == "SAME" and c["structural"] > 0.004:
                flag = "REVIEW: truth=SAME with sizeable structural difference"
            rows.append({**{k: t[k] for k in ("case_id", "cand_id", "truth", "source_kind")},
                         **c, "flag": flag})
            print(t["case_id"], t["source_kind"], t["truth"], t["cand_id"],
                  "struct", c["structural"], flag)
    else:
        for cid in sys.argv[2:]:
            c = compare(V.CAND_DIR / f"{cid}_L.png", V.CAND_DIR / f"{cid}_R.png")
            rows.append({"cand_id": cid, **c})
            print(cid, json.dumps(c, ensure_ascii=False))
    with open(V.ART / f"vis_pixel_arbiter_{which}.json", "w", encoding="utf-8") as fh:
        json.dump(rows, fh, ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
