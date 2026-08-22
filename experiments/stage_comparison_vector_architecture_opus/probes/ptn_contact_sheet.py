"""Render a contact sheet of motif groups so a human can judge symbol-vs-hatch.

Repro: python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_contact_sheet \
          <pair> <side> <SIG> [top_n]
Writes artifacts/ptn/sheet_<pair>_<side>_<SIG>.png
"""
from __future__ import annotations

import os
import sys
from collections import defaultdict
from pathlib import Path

from PIL import Image, ImageDraw

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
from experiments.stage_comparison_vector_architecture_opus.probes import ptn_motifs as M  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
TRACK_A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts/descriptions"
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts/ptn"

CELL = 150
COLS = 6


def main() -> None:
    pair, side, signame = sys.argv[1], sys.argv[2], sys.argv[3]
    top_n = int(sys.argv[4]) if len(sys.argv) > 4 else 24
    desc = M.load_description(TRACK_A / pair / side / "vector_block.json")
    bundle = M.build_motifs(desc, unit=os.environ.get("PTN_UNIT", "cc_split"))
    M.enrich_bundle(bundle, desc)
    texts = desc["texts"]
    motifs = bundle["motifs"]
    segments = bundle["segments"]
    sigs = [M.signatures_for(m, bundle, texts, float(os.environ.get("PTN_Q", "0.05"))) for m in motifs]
    groups: dict[str, list[int]] = defaultdict(list)
    for i, s in enumerate(sigs):
        groups[s[signame]].append(i)
    rows = sorted(groups.items(), key=lambda kv: -len(kv[1]))[:top_n]

    cols = COLS
    rows_n = (len(rows) + cols - 1) // cols
    img = Image.new("RGB", (cols * CELL, rows_n * CELL), "white")
    draw = ImageDraw.Draw(img)
    for idx, (key, members) in enumerate(rows):
        cx = (idx % cols) * CELL
        cy = (idx // cols) * CELL
        draw.rectangle([cx, cy, cx + CELL - 1, cy + CELL - 1], outline=(200, 200, 200))
        m = motifs[members[0]]
        x0, y0, x1, y1 = m["bbox"]
        w, h = max(x1 - x0, 1e-9), max(y1 - y0, 1e-9)
        scale = min((CELL - 42) / w, (CELL - 42) / h)
        ox = cx + (CELL - w * scale) / 2
        oy = cy + 24 + (CELL - 46 - h * scale) / 2
        for i in m["seg_indexes"]:
            s = segments[i]
            draw.line(
                [ox + (s["p0"][0] - x0) * scale, oy + (s["p0"][1] - y0) * scale,
                 ox + (s["p1"][0] - x0) * scale, oy + (s["p1"][1] - y0) * scale],
                fill=(0, 0, 0), width=1,
            )
        lab = M.text_context(m, texts)
        draw.text((cx + 4, cy + 3), f"#{idx} n={len(members)} s={m['nseg']}", fill=(180, 0, 0))
        draw.text((cx + 4, cy + 13), f"{round(m['diag'],1)}pt txt={lab[1][:9]}", fill=(0, 0, 160))
    path = OUT / f"sheet_{pair}_{side}_{signame}.png"
    img.save(path)
    print(path, len(rows), "groups")


if __name__ == "__main__":
    main()
