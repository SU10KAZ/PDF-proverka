# -*- coding: utf-8 -*-
"""Render one record of a REAL pair, both sides, zoomed.  python probes/ldg_eye_real.py <pair_id> [rec_index]"""
from __future__ import annotations
import json, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))
import grp_common as G          # noqa: E402
import grp_match as M           # noqa: E402
import loc_common as L          # noqa: E402
import ldg_ledger as LDG        # noqa: E402
import v03_counterfactual as C  # noqa: E402


def side(p):
    return G.F.extract_block(str(G.ROOT / p["pdf"]), p["page_index"], p["coords_px"],
                             p["page_px"][0], p["page_px"][1])


pid = sys.argv[1]
idx = int(sys.argv[2]) if len(sys.argv) > 2 else 0
p = {q["pair_id"]: q for q in json.load(open(ART / "mine_pairs.json", encoding="utf-8"))["pairs"]}[pid]
exA, exB = side(p["side_a"]), side(p["side_b"])
clipA, clipB = exA.frame["clip_display"], exB.frame["clip_display"]
base = (clipA[0] - clipB[0], clipA[1] - clipB[1])
sd = p["screen_signals"].get("registration_shift_pt") or [0.0, 0.0]
dx, dy, sc = M.register(exA.segments, exB.segments,
                        {(0.0, 0.0), base, (float(sd[0]), float(sd[1])),
                         (base[0] + float(sd[0]), base[1] + float(sd[1]))})
ldg = LDG.build(exA, exB, off=(dx, dy))
recs = ldg["_full"]
print(pid, "records", len(recs), "offset", round(dx, 2), round(dy, 2))
for i, c in enumerate(recs[:6]):
    print(i, c["type"], c["welded"], round(c["_len"], 1), c["_bbox"],
          (c["object_before"] or {}).get("label"), (c["object_after"] or {}).get("label"))
bb = recs[idx]["_bbox"]
out = ART / "ldg_eye"
for name, pad in (("zoom", 15.0), ("wide", 120.0)):
    fr = (bb[0] - pad, bb[1] - pad, bb[2] + pad, bb[3] + pad)
    C.render_extract(exA, frame=fr, target_px=560, out_png=str(out / f"REAL_{pid}_{idx}_{name}_A.png"))
    frB = (fr[0] - dx, fr[1] - dy, fr[2] - dx, fr[3] - dy)
    C.render_extract(exB, frame=frB, target_px=560, out_png=str(out / f"REAL_{pid}_{idx}_{name}_B.png"))
print("rendered")
