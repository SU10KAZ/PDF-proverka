# -*- coding: utf-8 -*-
"""visprep helper — render a fractional sub-rectangle of one side of a benchmark pair.

Used to cut the roles B/C cases (raster / vector+raster blocks), where the "object" is
a whole drawing inside the prepared block and there is no vector layer to point at it.
Rendering still goes through v03_foundation.render_block.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import vis_common as V           # noqa: E402
import v03_foundation as F       # noqa: E402


def frame_pt(side):
    fr = F.block_frame(V.abspath(side["pdf"]), side["page_index"], side["coords_px"],
                       side["page_px"][0], side["page_px"][1])
    r = fr.clip_display
    return [r.x0, r.y0, r.x1, r.y1]


def rect_of(side, frac):
    x0, y0, x1, y1 = frame_pt(side)
    w, h = x1 - x0, y1 - y0
    return [x0 + frac[0] * w, y0 + frac[1] * h, x0 + frac[2] * w, y0 + frac[3] * h]


def main():
    pair_id, which, fr, tag = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    target = int(sys.argv[5]) if len(sys.argv) > 5 else 700
    P = V.mine_pairs()[pair_id]
    side = P["side_a"] if which == "a" else P["side_b"]
    frac = [float(v) for v in fr.split(",")]
    rect = rect_of(side, frac)
    out = V.CAND_DIR / f"{tag}.png"
    sz = V.render_region(side, rect, out, target)
    print(json.dumps({"pair_id": pair_id, "side": which, "frac": frac,
                      "rect_pt": [round(v, 2) for v in rect], "px": sz, "png": str(out)},
                     ensure_ascii=False))


if __name__ == "__main__":
    main()
