"""Track A clips the block rect in DISPLAY space while get_drawings()/get_text() return
UNROTATED page space. On a /Rotate 90|270 page those are different rectangles.

This probe re-extracts a side with the block rect mapped through page.derotation_matrix,
so the vectors finally cover the region the diagnostic PNG shows.

Repro: python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_derotate_fix <pair> <side>
Writes artifacts/ptn/derot/<pair>/<side>/vector_block.json  (+ a check PNG)
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import fitz

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
from experiments.stage_comparison_vector_blocks import extractor  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts/ptn/derot"


def main() -> None:
    pair_id, side = sys.argv[1], sys.argv[2]
    pairs = json.load(open(ROOT / "experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json",
                           encoding="utf-8"))["pairs"]
    spec = next(p for p in pairs if p["pair_id"] == pair_id)[side]
    doc = fitz.open(ROOT / spec["pdf"])
    page = doc[spec["page_index"]]
    b = spec["bbox_norm"]
    display = fitz.Rect(b[0] * page.rect.width, b[1] * page.rect.height,
                        b[2] * page.rect.width, b[3] * page.rect.height)
    derot = (display * page.derotation_matrix).normalize()
    fixed_norm = [derot.x0 / page.rect.width, derot.y0 / page.rect.height,
                  derot.x1 / page.rect.width, derot.y1 / page.rect.height]
    print("rotation", page.rotation, "display", [round(v, 1) for v in display],
          "derotated", [round(v, 1) for v in derot])
    desc = extractor.extract_block(ROOT / spec["pdf"], page_index=spec["page_index"],
                                   bbox_norm=fixed_norm, block_id=spec["block_id"],
                                   storage_cap=200_000, topology_cap=200_000)
    target = OUT / pair_id / side
    target.mkdir(parents=True, exist_ok=True)
    with open(target / "vector_block.json", "w", encoding="utf-8") as handle:
        json.dump(desc, handle, ensure_ascii=False)
    print("prims", len(desc["geometry"]["primitives"]), "texts", len(desc["texts"]),
          "device labels", [t["text"] for t in desc["texts"] if t["text"][:2] in ("Wh", "QD", "QF")])
    print(target / "vector_block.json")


if __name__ == "__main__":
    main()
