#!/usr/bin/env python3
"""VVD — how much of the described region is actually visible in the crop shown to the verifier.

On a page with /Rotate 90|270 PyMuPDF returns get_drawings()/get_text() coordinates in the
UNROTATED mediabox frame, while page.get_pixmap(clip=R) interprets R in the ROTATED display
frame.  Track A built the block rect from page.rect (display) and used it for both.  This
probe measures, per block, the share of the DESCRIPTION's own texts and segments that fall
inside the part of the sheet the crop actually shows.
"""
from __future__ import annotations

import json

import fitz

from experiments.stage_comparison_vector_architecture_opus.probes import vv_harness as vv


def main() -> None:
    manifest = json.loads(vv.CASES_JSON.read_text(encoding="utf-8"))
    out = {}
    for key, b in sorted(manifest["blocks"].items()):
        doc = fitz.open(b["pdf"])
        page = doc[b["page_index"]]
        rot = page.rotation
        bb = b["bbox_norm_on_page"]
        # the rect the extractor used, numerically (display-frame numbers)
        rect = fitz.Rect(bb[0] * page.rect.width, bb[1] * page.rect.height,
                         bb[2] * page.rect.width, bb[3] * page.rect.height)
        # region the PIXMAP shows, expressed in the mediabox frame the data lives in
        inv = ~page.rotation_matrix          # display -> mediabox
        shown = rect * inv
        shown.normalize()
        inter = fitz.Rect(rect) & shown
        area_desc = rect.get_area()
        area_int = inter.get_area() if not inter.is_empty else 0.0

        desc = vv.load_description(vv.ROOT / b["description_path"])
        n_tx = len(desc["texts"])
        tx_in = sum(1 for t in desc["texts"]
                    if fitz.Rect(t["bbox"]).intersects(shown))
        segs = 0
        seg_in = 0
        for p in desc["geometry"]["primitives"]:
            for s in p["raw"]["segments"]:
                segs += 1
                mx = (s[0][0] + s[1][0]) / 2.0
                my = (s[0][1] + s[1][1]) / 2.0
                if shown.x0 <= mx <= shown.x1 and shown.y0 <= my <= shown.y1:
                    seg_in += 1
        out[key] = {
            "page_rotation": rot,
            "extractor_rect_mediabox": [round(v, 1) for v in rect],
            "region_the_crop_shows_in_mediabox": [round(v, 1) for v in shown],
            "area_overlap_share": round(area_int / area_desc, 4) if area_desc else None,
            "texts_total": n_tx,
            "texts_visible_in_crop": tx_in,
            "texts_visible_share": round(tx_in / n_tx, 4) if n_tx else None,
            "segments_total": segs,
            "segments_visible_in_crop": seg_in,
            "segments_visible_share": round(seg_in / segs, 4) if segs else None,
        }
        doc.close()
    dest = vv.ARTIFACTS / "vvd_rotation_confound.json"
    dest.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    hdr = f"{'block':30s} {'rot':>4s} {'area':>7s} {'texts':>7s} {'segs':>7s}"
    print(hdr)
    for k, v in out.items():
        print(f"{k:30s} {v['page_rotation']:4d} {v['area_overlap_share']:7.3f} "
              f"{v['texts_visible_share']:7.3f} {v['segments_visible_share']:7.3f}")
    print("written:", dest)


if __name__ == "__main__":
    main()
