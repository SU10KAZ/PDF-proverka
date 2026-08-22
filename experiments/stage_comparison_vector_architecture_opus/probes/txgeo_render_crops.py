#!/usr/bin/env python3
"""Render raster crops of blocks for human checking (never read by the detectors)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts/txgeo_crops"


def render(description_path: Path, name: str, dpi: int = 170) -> None:
    d = json.loads(description_path.read_text(encoding="utf-8"))
    doc = fitz.open(d["source"]["pdf"])
    page = doc[d["page_index"]]
    pix = page.get_pixmap(clip=fitz.Rect(*d["bbox"]), dpi=dpi)
    OUT.mkdir(parents=True, exist_ok=True)
    pix.save(str(OUT / f"{name}.png"))
    print(name, pix.width, pix.height, d["bbox"])
    doc.close()


if __name__ == "__main__":
    targets = sys.argv[1:]
    base_fresh = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts/txgeo_fresh_descriptions"
    base_a = ROOT / "experiments/stage_comparison_vector_blocks/artifacts/descriptions"
    for t in targets:
        block, side = t.split(":")
        p = (base_fresh if block.startswith("fresh_") else base_a) / block / side / "vector_block.json"
        render(p, f"{block}_{side}")
