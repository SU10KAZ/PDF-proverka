#!/usr/bin/env python3
"""TCF probe 7b — visual proof of the rotated-page coordinate-frame defect.

Renders, for two blocks that sit on rotated pages, the region the block bbox denotes
in the display frame ("meant") next to the region the extractor actually described
(the same numbers read in the unrotated frame, mapped back for viewing: "used"),
and a crop proving that a text bbox returned by `page.get_text` is likewise in the
unrotated frame (so texts and drawings agree with each other, and only the block
window is wrong).

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p7b_render_regions
"""
from __future__ import annotations

import json
import pathlib

import fitz
from PIL import Image, ImageDraw

ROOT = pathlib.Path("experiments/stage_comparison_vector_blocks/artifacts/descriptions")
CROPS = pathlib.Path("experiments/stage_comparison_vector_architecture_opus/artifacts/tcf_crops")
PAIRS = ("vk_node_plan", "eom_singleline_changed")


def main() -> None:
    CROPS.mkdir(parents=True, exist_ok=True)
    tiles = []
    for pair in PAIRS:
        d = json.loads((ROOT / pair / "left" / "vector_block.json").read_text(encoding="utf-8"))
        doc = fitz.open(d["source"]["pdf"])
        page = doc[d["page_index"]]
        b = d["bbox"]
        meant = fitz.Rect(*b)
        used = fitz.Rect(*b) * page.rotation_matrix
        used = fitz.Rect(min(used.x0, used.x1), min(used.y0, used.y1),
                         max(used.x0, used.x1), max(used.y0, used.y1))
        for name, rect in (("meant", meant), ("used", used)):
            rect = rect & page.rect
            zoom = min(520 / max(1.0, rect.width), 520 / max(1.0, rect.height))
            path = CROPS / f"tcf_rot_{pair}_{name}.png"
            page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), clip=rect, alpha=False).save(path)
            tiles.append((f"{pair} {name}", path))
        doc.close()
    cell = 540
    sheet = Image.new("RGB", (cell * 2, cell * 2), (255, 255, 255))
    draw = ImageDraw.Draw(sheet)
    for index, (label, path) in enumerate(tiles):
        image = Image.open(path).convert("RGB")
        image.thumbnail((cell - 20, cell - 40))
        x, y = (index % 2) * cell, (index // 2) * cell
        sheet.paste(image, (x + 10, y + 30))
        draw.text((x + 10, y + 8), label, fill=(0, 0, 160))
        draw.rectangle([x, y, x + cell - 1, y + cell - 1], outline=(180, 180, 180))
    sheet.save(CROPS / "tcf_rot_regions.png")

    # a text bbox from get_text, mapped through rotation_matrix, lands on its glyphs
    d = json.loads((ROOT / "eom_singleline_changed" / "left" / "vector_block.json").read_text(encoding="utf-8"))
    doc = fitz.open(d["source"]["pdf"])
    page = doc[d["page_index"]]
    span = d["texts"][0]
    rect = fitz.Rect(*span["bbox"]) * page.rotation_matrix
    page.get_pixmap(matrix=fitz.Matrix(6, 6), clip=rect + (-10, -10, 10, 10), alpha=False).save(
        CROPS / "tcf_rot_text_display.png"
    )
    page.get_pixmap(matrix=fitz.Matrix(6, 6), clip=fitz.Rect(*span["bbox"]) + (-10, -10, 10, 10),
                    alpha=False).save(CROPS / "tcf_rot_text_asis.png")
    doc.close()
    print("first text span:", span["text"], "-> tcf_rot_text_display.png shows its glyphs, "
          "tcf_rot_text_asis.png does not")


if __name__ == "__main__":
    main()
