# -*- coding: utf-8 -*-
"""`hyb` probe — three-arm A/B at the object level (§20).

Arm A  = deterministic vector object comparator (loc_common.ledger + border/threshold rule)
Arm B  = Vision on the whole block, one pair of images, one general question (the control
         arm the brief forbids as a product but requires as a baseline)
Arm C  = vector object diff + pointed Vision, called only through the gate of vis_FINDINGS

Nothing here extracts geometry on its own: blocks go through v03_foundation, objects
through v03_objects, counterfactuals through v03_counterfactual, the ledger through
loc_common, image pricing / region rendering through vis_common.
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
EXP = HERE.parent
ART = EXP / "artifacts"
ROOT = EXP.parents[1]
sys.path.insert(0, str(HERE))

import fitz                        # noqa: E402
import v03_foundation as F         # noqa: E402
import vis_common as V             # noqa: E402

CF_DIR = ART / "hyb_cf"            # materialised counterfactual PDFs
VIEW_DIR = ART / "hyb_view"        # anonymised whole-block images (arm B)
WIN_DIR = ART / "hyb_win"          # anonymised window images (arm C)

# arm A reporting rule, both numbers taken from loc (L5/L6), not re-tuned here
T_RECORD_PT = 60.0                 # interior record longer than this -> GRAPHIC_CHANGE
PREFILTER_EPS = 0.001              # vis gate step 0
WHOLE_TARGET_PX = 1500             # production render size (F.render_block default)
WINDOW_TARGET_PX = 700             # vis gate window size


def image_tokens(w, h):
    return V.image_tokens(int(w), int(h))


def rect(b):
    return fitz.Rect(min(b[0], b[2]), min(b[1], b[3]), max(b[0], b[2]), max(b[1], b[3]))


def isect(a, b):
    A, B = rect(a), rect(b)
    return not (A.x1 < B.x0 or B.x1 < A.x0 or A.y1 < B.y0 or B.y1 < A.y0)


def seg_bbox(s):
    return [min(s["p0"][0], s["p1"][0]), min(s["p0"][1], s["p1"][1]),
            max(s["p0"][0], s["p1"][0]), max(s["p0"][1], s["p1"][1])]


def _dilate(mask):
    import numpy as np
    out = mask.copy()
    out[1:, :] |= mask[:-1, :]
    out[:-1, :] |= mask[1:, :]
    out[:, 1:] |= mask[:, :-1]
    out[:, :-1] |= mask[:, 1:]
    return out


def structural_diff(png_a: Path, png_b: Path, thr: int = 200) -> float:
    import numpy as np
    pa, pb = fitz.Pixmap(str(png_a)), fitz.Pixmap(str(png_b))
    if pa.width != pb.width or pa.height != pb.height:
        return 1.0

    def m(p):
        a = np.frombuffer(p.samples, dtype=np.uint8)
        a = a.reshape(p.height, p.stride)[:, : p.width * p.n].reshape(p.height, p.width, p.n)
        v = a[:, :, :3].mean(axis=2) if p.n >= 3 else a[:, :, 0]
        return v < thr

    A, B = m(pa), m(pb)
    Ad, Bd = _dilate(A), _dilate(B)
    ua = int((A & ~Bd).sum())
    ub = int((B & ~Ad).sum())
    den = max(int(A.sum()) + int(B.sum()), 1)
    return (ua + ub) / den


def load(name):
    with open(ART / name, encoding="utf-8") as fh:
        return json.load(fh)


def dump(obj, name):
    with open(ART / name, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=1)
