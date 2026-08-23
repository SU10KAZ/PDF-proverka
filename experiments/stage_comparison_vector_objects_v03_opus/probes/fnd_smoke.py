# -*- coding: utf-8 -*-
"""Smoke test of the foundation contract.  `python -m ... .probes.fnd_smoke`"""
from __future__ import annotations

import json, os, sys
from pathlib import Path

sys.path.insert(0, os.path.abspath("."))
from experiments.stage_comparison_vector_objects_v03_opus.probes import v03_foundation as F

ART = Path("experiments/stage_comparison_vector_objects_v03_opus/artifacts")

REQUIRED_BLOCK = ["block_id", "page_number", "page_index", "coords_px", "coords_norm",
                  "page_px_w", "page_px_h", "rotation", "shape_type", "category_code",
                  "ocr_text", "crop_url", "pdf_path", "doc_id", "version", "discipline"]
REQUIRED_FRAME = ["clip_display", "clip_page", "to_page", "to_display", "rotation", "page_rect"]
REQUIRED_EXTRACT = ["segments", "segments_raw_count", "inked_segments_count", "invisible_dropped",
                    "texts", "images", "curves_flattened_count", "clipped_at_border_flags",
                    "quality", "char_scale", "provenance"]


def main():
    b = None
    with open(ART / "fnd_blocks.jsonl", encoding="utf-8") as fh:
        for line in fh:
            x = json.loads(line)
            if x["rotation"] in (90, 270) and x["coords_px"][2] - x["coords_px"][0] > 200:
                b = x
                break
    assert b, "no rotated block in the index"
    blocks = F.iter_prepared_blocks(b["result_json"], graphic_only=True)
    pb = [p for p in blocks if p.block_id == b["block_id"]][0]
    for f in REQUIRED_BLOCK:
        assert hasattr(pb, f), f
    fr = F.block_frame(pb.pdf_path, pb.page_index, pb.coords_px, pb.page_px_w, pb.page_px_h)
    for f in REQUIRED_FRAME:
        assert hasattr(fr, f), f
    assert fr.rotation in (0, 90, 180, 270)
    # clip_page must be the derotated twin of clip_display
    back = (fr.clip_page * fr.to_display)
    back.normalize()
    assert abs(back.x0 - fr.clip_display.x0) < 1e-6 and abs(back.y1 - fr.clip_display.y1) < 1e-6
    ex = F.extract_block(pb.pdf_path, pb.page_index, pb.coords_px, pb.page_px_w, pb.page_px_h, frame=fr)
    for f in REQUIRED_EXTRACT:
        assert hasattr(ex, f), f
    assert isinstance(ex.S, float)
    assert ex.provenance["pdf_sha256"] and len(ex.provenance["pdf_sha256"]) == 64
    n_iso = len(F.normalize(ex.segments, fr, "isotropic"))
    n_ani = len(F.normalize(ex.segments, fr, "anisotropic"))
    n_pts = len(F.normalize(ex.segments, fr, "points"))
    assert n_iso == n_ani == n_pts == len(ex.segments)
    pix = F.render_block(pb.pdf_path, pb.page_index, pb.coords_px, pb.page_px_w, pb.page_px_h,
                         dpi=100, min_long_side=800)
    assert pix.width > 0 and pix.height > 0
    print(json.dumps({
        "block": pb.block_id, "rotation": fr.rotation, "page_number": pb.page_number,
        "page_index_used": pb.page_index, "page_index_field": pb.page_index_field,
        "page_index_conflict": pb.page_index_conflict, "page_aspect_ok": pb.page_aspect_ok,
        "segments": ex.inked_segments_count, "dropped_invisible": ex.invisible_dropped,
        "texts": len(ex.texts), "images": len(ex.images), "S": ex.S,
        "render": [pix.width, pix.height], "OK": True}, ensure_ascii=False))


if __name__ == "__main__":
    main()
