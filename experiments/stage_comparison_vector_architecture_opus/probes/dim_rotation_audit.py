#!/usr/bin/env python3
"""dim_* probe (side finding): measure the /Rotate coordinate-space mismatch.

`page.rect` is rotation-aware; `page.get_drawings()` and `page.get_text(clip=...)`
return MEDIABOX (unrotated) coordinates; `page.get_pixmap(clip=...)` uses the
rotated space.  A block rect derived from `bbox_norm * page.rect` therefore
selects a *different region* than the diagnostic PNG rendered from the same rect.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.dim_rotation_audit
"""
from __future__ import annotations

import json
from pathlib import Path

import fitz

PAIRS = "experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json"
OUT = "experiments/stage_comparison_vector_architecture_opus/artifacts/dim_rotation_audit.json"


def main() -> None:
    data = json.loads(Path(PAIRS).read_text(encoding="utf-8"))
    rows = []
    for pair in data["pairs"]:
        for side in ("left", "right"):
            spec = pair[side]
            doc = fitz.open(spec["pdf"])
            page = doc[spec["page_index"]]
            bn = spec["bbox_norm"]
            r = page.rect
            visual = fitz.Rect(bn[0] * r.width, bn[1] * r.height, bn[2] * r.width, bn[3] * r.height)
            # what the extractor actually reads: the same numbers, mediabox space
            read = fitz.Rect(visual)
            read_in_visual = read * page.rotation_matrix          # back to visual space
            inter = fitz.Rect(read_in_visual) & visual
            area_v = visual.get_area()
            overlap = inter.get_area() / area_v if area_v else 0.0
            texts_visual = len(page.get_text("words", clip=read * page.derotation_matrix))
            texts_read = len(page.get_text("words", clip=read))
            rows.append({
                "pair": pair["pair_id"], "side": side, "rotation": page.rotation,
                "page_rect": [r.width, r.height],
                "mediabox": [page.mediabox.width, page.mediabox.height],
                "intended_region_recovered_fraction": round(overlap, 4),
                "words_in_intended_region": texts_visual,
                "words_in_region_actually_read": texts_read,
            })
            doc.close()
    Path(OUT).write_text(json.dumps(rows, ensure_ascii=False, indent=1), encoding="utf-8")
    for row in rows:
        print(f"{row['pair']:>24} {row['side']:>5} rot={row['rotation']:>3} "
              f"overlap={row['intended_region_recovered_fraction']:>7.2%} "
              f"words intended={row['words_in_intended_region']:>4} read={row['words_in_region_actually_read']:>4}")


if __name__ == "__main__":
    main()
