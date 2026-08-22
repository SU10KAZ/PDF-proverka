"""Literal hatch-region vs symbol-region test inside ONE block (ar_plan left).

Regions were picked by eye on the Track A diagnostic PNG and are given in PNG pixels;
they are converted to PDF points with the block bbox. Crops of both regions are written
so the choice can be re-checked.

Repro: python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_region_test
Writes artifacts/ptn_region_test.json + artifacts/ptn/region_{hatch,symbol}.png
"""
from __future__ import annotations

import json
import os
import sys
from collections import Counter
from pathlib import Path

from PIL import Image

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
from experiments.stage_comparison_vector_architecture_opus.probes import ptn_motifs as M  # noqa: E402
from experiments.stage_comparison_vector_architecture_opus.probes.ptn_group_features import closure, straightness, text_inside  # noqa: E402

ROOT = Path(__file__).resolve().parents[3]
A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts"
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"

PAIR, SIDE = "ar_plan", "left"
REGIONS_PX = {
    "hatch": (1340, 440, 1460, 720),     # 45-degree wall hatch band, right-hand outer wall
    "symbol": (1520, 110, 1720, 1460),   # axis-bubble column (circle + text inside)
}


def main() -> None:
    desc = M.load_description(A / "descriptions" / PAIR / SIDE / "vector_block.json")
    bundle = M.build_motifs(desc, unit="cc_split")
    M.enrich_bundle(bundle, desc)
    texts = desc["texts"]
    motifs, segments = bundle["motifs"], bundle["segments"]
    sigs = [M.signatures_for(m, bundle, texts) for m in motifs]

    png = Image.open(A / "diagnostics" / PAIR / f"{SIDE}.png")
    bx0, by0, bx1, by1 = desc["bbox"]
    sx, sy = png.width / (bx1 - bx0), png.height / (by1 - by0)

    out = {"pair": PAIR, "side": SIDE, "regions_px": REGIONS_PX, "regions": {}}
    for name, (px0, py0, px1, py1) in REGIONS_PX.items():
        png.crop((px0, py0, px1, py1)).save(OUT / "ptn" / f"region_{name}.png")
        rx0, ry0 = bx0 + px0 / sx, by0 + py0 / sy
        rx1, ry1 = bx0 + px1 / sx, by0 + py1 / sy
        inside = [i for i, m in enumerate(motifs)
                  if rx0 <= m["center"][0] <= rx1 and ry0 <= m["center"][1] <= ry1]
        seg_in = sum(1 for s in segments if rx0 <= s["p0"][0] <= rx1 and ry0 <= s["p0"][1] <= ry1)
        row = {"rect_pt": [round(rx0, 1), round(ry0, 1), round(rx1, 1), round(ry1, 1)],
               "segments_in_region": seg_in, "motifs_in_region": len(inside)}
        for sig in ("S1", "S2", "S3", "S4", "S5", "S5c"):
            c = Counter(sigs[i][sig] for i in inside)
            row[f"{sig}_groups"] = len(c)
            row[f"{sig}_groups_repeated"] = sum(1 for v in c.values() if v >= 2)
            row[f"{sig}_largest"] = max(c.values(), default=0)
        if inside:
            row["mean_nseg"] = round(sum(motifs[i]["nseg"] for i in inside) / len(inside), 2)
            row["mean_straightness"] = round(sum(straightness(motifs[i], segments) for i in inside) / len(inside), 4)
            row["mean_closure"] = round(sum(closure(motifs[i])[0] for i in inside) / len(inside), 3)
            row["frac_closed_2d"] = round(sum(1 for i in inside
                                              if closure(motifs[i])[0] >= 0.9 and straightness(motifs[i], segments) > 0.10)
                                          / len(inside), 3)
            row["frac_text_inside"] = round(sum(1 for i in inside if text_inside(motifs[i], texts)) / len(inside), 3)
            row["frac_labeled"] = round(sum(1 for i in inside if M.text_context(motifs[i], texts)[0]) / len(inside), 3)
        out["regions"][name] = row
        print(name, json.dumps(row, ensure_ascii=False))
    with open(OUT / "ptn_region_test.json", "w", encoding="utf-8") as handle:
        json.dump(out, handle, ensure_ascii=False, indent=1)
    print(OUT / "ptn_region_test.json")


if __name__ == "__main__":
    main()
