#!/usr/bin/env python3
"""VVD — crop rendering for ARM 1 side-arms.

Two things this makes:

* ``rotfix``  — the crop of the region the description ACTUALLY describes on a page with
  /Rotate 90|270.  PyMuPDF returns ``get_drawings``/``get_text`` coordinates in the
  UNROTATED mediabox frame, while ``page.get_pixmap(clip=...)`` interprets the clip in the
  ROTATED display frame.  Track A's extractor built its block rect from ``page.rect``
  (display) and clipped the vector data with it, so on a rotated page the description and
  the diagnostics PNG are two different regions of the sheet (orchestrator finding O13).
  ``rotfix`` renders the same numeric rect after ``page.set_rotation(0)``.

* ``zoom``    — the same region at a different render zoom, to test whether more pixels
  change what the verifier can check on a dense block.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.vvd_crops rotfix
    python -m ...vvd_crops zoom --blocks ar_wall_sections:left vk_nodes:left --zooms 0.6 2.7
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import fitz

from experiments.stage_comparison_vector_architecture_opus.probes import vv_harness as vv

CROPS = vv.ARTIFACTS / "vvd_crops"


def render(pdf: str, page_index: int, bbox_norm, out_png: Path, zoom: float,
           derotate: bool) -> dict:
    doc = fitz.open(pdf)
    page = doc[page_index]
    rotation = page.rotation
    # the rect the extractor used: bbox_norm * page.rect (DISPLAY frame numbers)
    rect = fitz.Rect(bbox_norm[0] * page.rect.width, bbox_norm[1] * page.rect.height,
                     bbox_norm[2] * page.rect.width, bbox_norm[3] * page.rect.height)
    if derotate:
        page.set_rotation(0)
    pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), clip=rect, alpha=False)
    out_png.parent.mkdir(parents=True, exist_ok=True)
    pix.save(out_png)
    doc.close()
    return {"png": str(out_png.relative_to(vv.ROOT)), "pixels": [pix.width, pix.height],
            "bytes": out_png.stat().st_size, "zoom": zoom, "derotate": derotate,
            "page_rotation": rotation, "clip_rect": [round(v, 2) for v in rect]}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["rotfix", "zoom"])
    ap.add_argument("--blocks", nargs="*", default=None)
    ap.add_argument("--zooms", nargs="*", type=float, default=[2.7])
    ap.add_argument("--derotate-if-rotated", action="store_true", default=True)
    args = ap.parse_args()

    manifest = json.loads(vv.CASES_JSON.read_text(encoding="utf-8"))
    blocks = manifest["blocks"]
    out = {}
    if args.mode == "rotfix":
        for key, b in blocks.items():
            doc = fitz.open(b["pdf"]); rot = doc[b["page_index"]].rotation; doc.close()
            if rot == 0:
                continue
            png = CROPS / f"rotfix_{key.replace(':', '_')}.png"
            out[key] = render(b["pdf"], b["page_index"], b["bbox_norm_on_page"], png,
                              zoom=1.35, derotate=True)
        dest = vv.ARTIFACTS / "vvd_crops_rotfix.json"
    else:
        keys = args.blocks or list(blocks)
        for key in keys:
            b = blocks[key]
            doc = fitz.open(b["pdf"]); rot = doc[b["page_index"]].rotation; doc.close()
            for z in args.zooms:
                png = CROPS / f"zoom{z:g}_{key.replace(':', '_')}.png"
                out[f"{key}@{z:g}"] = render(b["pdf"], b["page_index"], b["bbox_norm_on_page"],
                                             png, zoom=z, derotate=(rot != 0))
        dest = vv.ARTIFACTS / "vvd_crops_zoom.json"
    dest.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False, indent=2))
    print("written:", dest)


if __name__ == "__main__":
    main()
