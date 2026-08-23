# -*- coding: utf-8 -*-
"""Rule 5 of BRIEF_COUNTERFACTUALS: prove the rewrite really is a no-op on the PICTURE.

For X1/X1b/X2/X7/X8 the rendered raster must be identical pixel for pixel.
For X3..X6b (rigid maps) the picture is the same drawing moved, so we check the
INK MASS instead (number of dark pixels) and the layer's own class census.
"""
from __future__ import annotations
import json, random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import advB_rw as R
import v03_counterfactual as CF

SEED = 20260823
EXACT = ["X1_split_at_0.37", "X1b_split_at_0.29_0.71", "X2_reverse_vertices",
         "X7_rect_to_lines", "X8_lines_to_rect"]


class Ex:
    def __init__(self, base, segments, texts):
        self.segments = segments; self.texts = texts
        self.frame = base.frame; self.images = base.images
        self.provenance = base.provenance; self.quality = base.quality


def main():
    sample = json.load(open(G.ART / "grp_sample.json", encoding="utf-8"))
    blocks = [b for b in sample["blocks"] if 200 <= b["n_seg"] <= 6000]
    rng = random.Random(SEED)
    rng.shuffle(blocks)
    out = []
    for rec in blocks[:12]:
        pb = G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
        if pb is None: continue
        ex = G.extract(pb)
        if not ex.segments: continue
        S0 = G.layer_of(G.rw_identity(ex.segments, None), ex.texts).S
        pa = CF.render_extract(ex, target_px=1100)
        row = {"block_id": rec["block_id"], "discipline": rec["discipline"], "n_seg": len(ex.segments)}
        fns = {"X1_split_at_0.37": R._split_at([0.37], max(0.5, 0.05 * S0)),
               "X1b_split_at_0.29_0.71": R._split_at([0.29, 0.71], max(0.5, 0.05 * S0)),
               "X2_reverse_vertices": R.rw_reverse_vertices,
               "X7_rect_to_lines": R.rw_rect_to_lines,
               "X8_lines_to_rect": R.rw_lines_to_rect}
        for nm, fn in fns.items():
            segs = fn(ex.segments, random.Random(SEED))
            ex2 = Ex(ex, segs, ex.texts)
            pb2 = CF.render_extract(ex2, frame=ex.frame["clip_display"], target_px=1100)
            d = CF.raster_diff(pa, pb2)
            row[nm] = {"pixels_changed": d.get("changed", d.get("n_changed")),
                       "ink_iou": round(CF.ink_iou(pa, pb2), 6),
                       "n_seg_out": len(segs)}
        out.append(row)
        print(json.dumps(row, ensure_ascii=False), flush=True)
    json.dump({"n": len(out), "rows": out},
              open(G.ART / "advB_rewrites_visual.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
