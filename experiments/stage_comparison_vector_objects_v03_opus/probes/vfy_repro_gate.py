# -*- coding: utf-8 -*-
"""MINIMAL REPRODUCTION of the degenerate-rect path gate defect in v03_foundation.

Run:
  python experiments/stage_comparison_vector_objects_v03_opus/probes/vfy_repro_gate.py
"""
import sys
from pathlib import Path
import fitz

ROOT = Path("/home/coder/projects/PDF-proverka")
sys.path.insert(0, str(ROOT))
from experiments.stage_comparison_vector_objects_v03_opus.probes import v03_foundation as F

# ---- 1. the library fact -----------------------------------------------------------
horiz = fitz.Rect(10, 50, 200, 50)          # a horizontal line path: rect height == 0
clip = fitz.Rect(0, 0, 300, 300)            # a block clip that fully contains it
print("rect of a horizontal line :", horiz, "is_empty =", horiz.is_empty)
print("clip.intersects(line rect):", clip.intersects(horiz), "  <- v03_foundation gates on this")
print("truth: the line is inside the clip")

# v03_foundation.extract_block:
#     if r is not None and not fitz.Rect(r).intersects(read_rect):
#         paths_outside_clip += 1
#         continue
# => every purely horizontal / vertical single-line path is discarded as 'outside_clip'.

# ---- 2. on a real prepared block ----------------------------------------------------
PDF = ROOT / ("projects_v2/objects/213_Mosfilmovskaya_31A_KingSons/disciplines/OV/documents/"
              "133-23-ГК-ОВ1.1/versions/v001/02_work/document.pdf")
if len(sys.argv) > 3:
    PDF, PAGE, COORDS, PPX = Path(sys.argv[1]), int(sys.argv[2]), eval(sys.argv[3]), eval(sys.argv[4])
else:
    import json
    idx = ROOT / "experiments/stage_comparison_vector_objects_v03_opus/artifacts/vfy_f6_gate.json"
    rows = json.load(open(idx))["rows"] if idx.exists() else []
    rows = [r for r in rows if r.get("n_overlap", 0) > 0 and r.get("n_intersects", 1) == 0]
    if not rows:
        print("\n(run vfy_f6_gate.py first for a corpus-mined example)")
        raise SystemExit
    r = rows[0]
    PDF, PAGE, COORDS, PPX = Path(r["pdf"]), r["pi"], r["coords"], (r["pw"], r["ph"])
    print("\nexample block:", r["bid"], r["disc"], r["doc"])

ex = F.extract_block(str(PDF), PAGE, COORDS, PPX[0], PPX[1])
print("v03_foundation segments:", ex.inked_segments_count,
      " paths_outside_clip:", ex.paths_outside_clip, "of", ex.paths_total)

doc = fitz.open(str(PDF)); page = doc[PAGE]
sx = page.rect.width / PPX[0]; sy = page.rect.height / PPX[1]
cd = fitz.Rect(COORDS[0]*sx, COORDS[1]*sy, COORDS[2]*sx, COORDS[3]*sy)
cp = fitz.Rect(cd) * page.derotation_matrix; cp.normalize()
n_ok = n_lost = 0
for g in page.get_drawings():
    rr = fitz.Rect(g["rect"])
    overlaps = (rr.x0 <= cp.x1 and cp.x0 <= rr.x1 and rr.y0 <= cp.y1 and cp.y0 <= rr.y1)
    if overlaps and not rr.intersects(cp):
        n_lost += sum(len(g.get("items") or []) for _ in (0,))
    elif overlaps:
        n_ok += 1
print("paths that overlap the clip but fitz says 'no intersection':", n_lost)
