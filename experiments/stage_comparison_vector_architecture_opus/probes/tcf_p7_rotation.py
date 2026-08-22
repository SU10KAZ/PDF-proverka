#!/usr/bin/env python3
"""TCF probe 7 (bonus) — coordinate-frame audit of the extracted block window.

`extractor.extract_block` builds the block rectangle from `page.rect`, which is the
*display* (rotation-applied) rectangle, and then clips `page.get_drawings()` output,
which PyMuPDF returns in *unrotated* page coordinates.  On a page with /Rotate 90 or
270 the two frames are transposed.  This probe measures, per benchmark block:

  * page rotation, display rect, unrotated rect;
  * the block rect as interpreted by the extractor (unrotated frame) vs the block
    rect the caller meant (display frame), and their IoU after de-rotation;
  * whether left and right sides of a pair have the same rotation.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p7_rotation
"""
from __future__ import annotations

import json
import pathlib

import fitz

ROOT = pathlib.Path("experiments/stage_comparison_vector_blocks/artifacts/descriptions")
OUT = pathlib.Path("experiments/stage_comparison_vector_architecture_opus/artifacts/tcf_p7_rotation.json")


def iou(a, b) -> float:
    x0, y0 = max(a[0], b[0]), max(a[1], b[1])
    x1, y1 = min(a[2], b[2]), min(a[3], b[3])
    inter = max(0.0, x1 - x0) * max(0.0, y1 - y0)
    area_a = (a[2] - a[0]) * (a[3] - a[1])
    area_b = (b[2] - b[0]) * (b[3] - b[1])
    return round(inter / (area_a + area_b - inter), 4) if area_a + area_b - inter else 0.0


def main() -> None:
    rows = []
    for pair_dir in sorted(ROOT.iterdir()):
        for side in ("left", "right"):
            path = pair_dir / side / "vector_block.json"
            if not path.exists():
                continue
            d = json.loads(path.read_text(encoding="utf-8"))
            doc = fitz.open(d["source"]["pdf"])
            page = doc[d["page_index"]]
            block = d["bbox"]
            # what the caller meant: `block` read in the display frame, mapped back
            # into the unrotated frame where the geometry actually lives
            r = fitz.Rect(*block) * page.derotation_matrix
            meant = [min(r.x0, r.x1), min(r.y0, r.y1), max(r.x0, r.x1), max(r.y0, r.y1)]
            rows.append(
                {
                    "pair": pair_dir.name,
                    "side": side,
                    "rotation": page.rotation,
                    "display_rect": [round(page.rect.width, 2), round(page.rect.height, 2)],
                    "unrotated_rect": [round(page.mediabox.width, 2), round(page.mediabox.height, 2)],
                    "block_rect_used": [round(v, 2) for v in block],
                    "block_rect_meant": [round(v, 2) for v in meant],
                    "iou_used_vs_meant": iou(block, meant),
                    "block_outside_unrotated_page": bool(
                        block[2] > page.mediabox.width or block[3] > page.mediabox.height
                    ),
                }
            )
            doc.close()
    OUT.write_text(json.dumps(rows, ensure_ascii=False, indent=1), encoding="utf-8")
    hdr = ["pair", "side", "rotation", "display_rect", "unrotated_rect", "iou_used_vs_meant",
           "block_outside_unrotated_page"]
    print("\t".join(hdr))
    for r in rows:
        print("\t".join(str(r[k]) for k in hdr))
    rotated = [r for r in rows if r["rotation"]]
    print(f"\nrotated blocks: {len(rotated)}/{len(rows)}")
    if rotated:
        print("mean IoU(used, meant) on rotated pages:",
              round(sum(r["iou_used_vs_meant"] for r in rotated) / len(rotated), 4))


if __name__ == "__main__":
    main()
