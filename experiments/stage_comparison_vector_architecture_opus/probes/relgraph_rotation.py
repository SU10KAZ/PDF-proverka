#!/usr/bin/env python3
"""relgraph_rotation -- Track-B probe: on rotated PDF pages the v0.1 extractor
clips geometry and text in two DIFFERENT coordinate spaces.

PyMuPDF returns `page.get_text(...)` bboxes in the rotated page space
(= page.rect) but `page.get_drawings()` coordinates in the *unrotated*
cropbox space (width/height transposed for /Rotate 90|270).
`extractor.extract_block` builds one block_rect from `page.rect` and clips both
layers with it, so on a rotated page the geometry layer and the text layer
describe different physical regions of the sheet.

Run from repo root:
    python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_rotation.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
import fitz  # noqa: E402

A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts"
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"


def drawing_space_extent(page):
    """Empirical bounding box of get_drawings() output."""
    x0 = y0 = 1e9
    x1 = y1 = -1e9
    for d in page.get_drawings():
        r = d["rect"]
        x0, y0 = min(x0, r.x0), min(y0, r.y0)
        x1, y1 = max(x1, r.x1), max(y1, r.y1)
    return [round(x0, 1), round(y0, 1), round(x1, 1), round(y1, 1)]


def text_space_extent(page):
    x0 = y0 = 1e9
    x1 = y1 = -1e9
    td = page.get_text("dict")
    for b in td["blocks"]:
        for l in b.get("lines", []):
            for s in l["spans"]:
                bb = s["bbox"]
                x0, y0 = min(x0, bb[0]), min(y0, bb[1])
                x1, y1 = max(x1, bb[2]), max(y1, bb[3])
    return [round(x0, 1), round(y0, 1), round(x1, 1), round(y1, 1)]


def main() -> None:
    pairs = json.loads((A / "block_pairs.json").read_text())["pairs"]
    rows = []
    for p in pairs:
        for side_name in ("left", "right"):
            side = p[side_name]
            doc = fitz.open(ROOT / side["pdf"])
            page = doc[int(side["page_index"])]
            rot = page.rotation
            pr = page.rect
            bn = side["bbox_norm"]
            block_rect = [bn[0] * pr.width, bn[1] * pr.height, bn[2] * pr.width, bn[3] * pr.height]
            row = {
                "pair_id": p["pair_id"], "side": side_name, "page_rotation": rot,
                "page_rect": [round(pr.width, 1), round(pr.height, 1)],
                "drawings_extent": drawing_space_extent(page),
                "texts_extent": text_space_extent(page),
                "block_rect": [round(v, 1) for v in block_rect],
            }
            if rot in (90, 270):
                # map the geometry rect (unrotated space) into the text/page space
                m = page.rotation_matrix
                r = fitz.Rect(*block_rect) * m
                geo_in_page = [round(v, 1) for v in (r.x0, r.y0, r.x1, r.y1)]
                inter = fitz.Rect(*block_rect) & fitz.Rect(*geo_in_page)
                a_txt = (block_rect[2] - block_rect[0]) * (block_rect[3] - block_rect[1])
                a_int = max(inter.width, 0) * max(inter.height, 0)
                row["geometry_rect_mapped_into_page_space"] = geo_in_page
                row["overlap_fraction_of_block"] = round(a_int / a_txt, 4) if a_txt else None
            else:
                row["geometry_rect_mapped_into_page_space"] = row["block_rect"]
                row["overlap_fraction_of_block"] = 1.0
            rows.append(row)
            doc.close()
            print(f"{p['pair_id']:24s} {side_name:5s} rot={rot:4d} page={row['page_rect']} "
                  f"draw_extent={row['drawings_extent']} txt_extent={row['texts_extent']} "
                  f"overlap={row['overlap_fraction_of_block']}")

    n_rot = sum(1 for r in rows if r["page_rotation"] in (90, 270))
    mixed = sorted({r["pair_id"] for r in rows
                    if any(o["pair_id"] == r["pair_id"] and
                           (o["page_rotation"] in (90, 270)) != (r["page_rotation"] in (90, 270))
                           for o in rows)})
    print(f"\nblocks on rotated pages: {n_rot}/{len(rows)}")
    print(f"pairs where the two sides disagree about rotation: {mixed}")

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "relgraph_rotation.json").write_text(json.dumps(
        {"research_only": True, "rows": rows, "rotated_blocks": n_rot,
         "total_blocks": len(rows), "mixed_rotation_pairs": mixed},
        ensure_ascii=False, indent=1))
    print("wrote", OUT / "relgraph_rotation.json")


if __name__ == "__main__":
    main()


def geometry_in_page_space(pdf, page_index, block_rect):
    """Fraction of extracted segment midpoints that, mapped into page (text)
    space, actually land inside the block rect the text was clipped with."""
    import sys as _s
    _s.path.insert(0, str(ROOT))
    from experiments.stage_comparison_vector_blocks import extractor as E
    desc = E.extract_block(pdf, page_index=page_index,
                           bbox_norm=[0, 0, 1, 1], block_id="probe")
    doc = fitz.open(pdf)
    page = doc[page_index]
    m = page.rotation_matrix
    br = fitz.Rect(*block_rect)
    inside = total = 0
    for prim in desc["geometry"]["primitives"]:
        for s, e in prim["raw"]["segments"]:
            mx, my = (s[0] + e[0]) / 2, (s[1] + e[1]) / 2
            total += 1
            if br.contains(fitz.Point(mx, my)):
                inside += 1
    doc.close()
    return inside, total
