"""Draw the instances of one motif group onto the Track A diagnostic PNG for eye checking.

Repro: python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_overlay <pair> <side> <SIG> <rank> [crop x0 y0 x1 y1]
Writes artifacts/ptn/overlay_<pair>_<side>_<SIG><rank>.png
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
TRACK_A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts"
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts/ptn"


def main() -> None:
    pair, side, signame, rank = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
    crop = [int(v) for v in sys.argv[5:9]] if len(sys.argv) >= 9 else None
    zoom = float(sys.argv[9]) if len(sys.argv) >= 10 else 1.0
    desc = M.load_description(TRACK_A / "descriptions" / pair / side / "vector_block.json")
    bundle = M.build_motifs(desc, unit="cc_split")
    M.enrich_bundle(bundle, desc)
    sigs = [M.signatures_for(m, bundle, desc["texts"]) for m in bundle["motifs"]]
    groups = defaultdict(list)
    for i, s in enumerate(sigs):
        groups[s[signame]].append(i)
    ranked = sorted(groups.items(), key=lambda kv: -len(kv[1]))
    key, members = ranked[rank]

    img = Image.open(TRACK_A / "diagnostics" / pair / f"{side}.png").convert("RGB")
    bx0, by0, bx1, by1 = desc["bbox"]
    sx = img.width / (bx1 - bx0)
    sy = img.height / (by1 - by0)
    draw = ImageDraw.Draw(img)
    for i in members:
        x0, y0, x1, y1 = bundle["motifs"][i]["bbox"]
        draw.rectangle([(x0 - bx0) * sx - 5, (y0 - by0) * sy - 5, (x1 - bx0) * sx + 5, (y1 - by0) * sy + 5],
                       outline=(255, 0, 0), width=2)
    if crop:
        img = img.crop(tuple(crop))
    if zoom != 1.0:
        img = img.resize((int(img.width * zoom), int(img.height * zoom)), Image.LANCZOS)
    path = OUT / f"overlay_{pair}_{side}_{signame}{rank}.png"
    img.save(path)
    print(path, "instances", len(members))


if __name__ == "__main__":
    main()
