#!/usr/bin/env python3
"""TCF probe 3 — how trustworthy is `x_crossings_unconnected`?

Two parts.

A. Deterministic audit of every recorded crossing in the 10 left-side blocks:
   * distance from the crossing point to the nearest endpoint of either segment,
     compared with the node-merge tolerance (a crossing that sits within the
     tolerance of an endpoint is a junction candidate, not a crossing);
   * whether the two segments are already in the same connected component;
   * whether the T-junction pass already glued exactly these two segments;
   * how often the hard 5000-crossing truncation fires.

B. Visual sample: 15 crossings, stratified over blocks, rendered as a tight and a
   context crop straight from the source PDF, plus a 3x5 montage per zoom level,
   so a human (or the auditing model) can decide whether the crossing is a real
   connection, a genuine unconnected crossing, or an artefact.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.tcf_p3_crossings
"""
from __future__ import annotations

import json
import pathlib
import random

import fitz
from PIL import Image, ImageDraw

from experiments.stage_comparison_vector_architecture_opus.probes import tcf_topo

ROOT = pathlib.Path("experiments/stage_comparison_vector_blocks/artifacts/descriptions")
ART = pathlib.Path("experiments/stage_comparison_vector_architecture_opus/artifacts")
CROPS = ART / "tcf_crops"
OUT = ART / "tcf_p3_crossings.json"
TOLERANCE = 0.0025
SAMPLE = 15
SEED = 20260822


def render(pdf: str, page_index: int, cx: float, cy: float, half: float, zoom: float, path: pathlib.Path) -> None:
    doc = fitz.open(pdf)
    page = doc[page_index]
    # page.get_drawings() coordinates are UNROTATED; get_pixmap clips in display space
    point = fitz.Point(cx, cy) * page.rotation_matrix
    cx, cy = point.x, point.y
    clip = fitz.Rect(cx - half, cy - half, cx + half, cy + half)
    pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), clip=clip, alpha=False)
    pix.save(path)
    doc.close()
    image = Image.open(path).convert("RGB")
    draw = ImageDraw.Draw(image)
    w, h = image.size
    r = max(6, int(min(w, h) * 0.06))
    draw.ellipse([w / 2 - r, h / 2 - r, w / 2 + r, h / 2 + r], outline=(255, 0, 0), width=2)
    image.save(path)


def montage(paths: list[pathlib.Path], out: pathlib.Path, cols: int = 5, cell: int = 260) -> None:
    rows = (len(paths) + cols - 1) // cols
    sheet = Image.new("RGB", (cols * cell, rows * cell), (255, 255, 255))
    draw = ImageDraw.Draw(sheet)
    for index, path in enumerate(paths):
        image = Image.open(path).convert("RGB")
        image.thumbnail((cell - 24, cell - 24))
        x, y = (index % cols) * cell, (index // cols) * cell
        sheet.paste(image, (x + 12, y + 20))
        draw.text((x + 6, y + 4), f"#{index + 1}", fill=(0, 0, 160))
        draw.rectangle([x, y, x + cell - 1, y + cell - 1], outline=(200, 200, 200))
    sheet.save(out)


def main() -> None:
    CROPS.mkdir(parents=True, exist_ok=True)
    stats = {}
    pool = []
    for pair_dir in sorted(ROOT.iterdir()):
        pair = pair_dir.name
        description = json.loads((pair_dir / "left" / "vector_block.json").read_text(encoding="utf-8"))
        topo = tcf_topo.topology(
            description["geometry"]["primitives"], TOLERANCE, 8_000, keep_crossings=True
        )
        records = topo["_crossings"]
        total = len(records)
        stats[pair] = {
            "crossings": total,
            "truncated_at_5000": topo["crossings_truncated"],
            "near_endpoint_within_tolerance": sum(1 for r in records if r["endpoint_within_tolerance"]),
            "same_component": sum(1 for r in records if r["same_component"]),
            "already_joined_by_t_junction": sum(1 for r in records if r["joined_by_t_junction"]),
            "min_param_lt_0.02": sum(1 for r in records if r["min_param"] < 0.02),
            "share_near_endpoint": round(
                sum(1 for r in records if r["endpoint_within_tolerance"]) / total, 4
            ) if total else None,
            "share_same_component": round(
                sum(1 for r in records if r["same_component"]) / total, 4
            ) if total else None,
            "share_joined_by_t_junction": round(
                sum(1 for r in records if r["joined_by_t_junction"]) / total, 4
            ) if total else None,
        }
        bbox = description["bbox"]
        for record in records:
            pool.append(
                {
                    "pair": pair,
                    "pdf": description["source"]["pdf"],
                    "page_index": description["page_index"],
                    "x_page": bbox[0] + record["point"][0] * (bbox[2] - bbox[0]),
                    "y_page": bbox[1] + record["point"][1] * (bbox[3] - bbox[1]),
                    **record,
                }
            )
        print(pair, stats[pair], flush=True)

    random.seed(SEED)
    by_pair: dict[str, list] = {}
    for item in pool:
        by_pair.setdefault(item["pair"], []).append(item)
    picked = []
    pairs = sorted(by_pair)
    index = 0
    while len(picked) < SAMPLE and any(by_pair.values()):
        bucket = by_pair[pairs[index % len(pairs)]]
        if bucket:
            picked.append(bucket.pop(random.randrange(len(bucket))))
        index += 1
    samples = []
    tight_paths, wide_paths = [], []
    for number, item in enumerate(picked, 1):
        tight = CROPS / f"tcf_x{number:02d}_tight.png"
        wide = CROPS / f"tcf_x{number:02d}_wide.png"
        render(item["pdf"], item["page_index"], item["x_page"], item["y_page"], 6.0, 22.0, tight)
        render(item["pdf"], item["page_index"], item["x_page"], item["y_page"], 34.0, 6.0, wide)
        tight_paths.append(tight)
        wide_paths.append(wide)
        samples.append(
            {
                "n": number,
                "pair": item["pair"],
                "point_norm": item["point"],
                "min_param": item["min_param"],
                "nearest_endpoint_distance": item["nearest_endpoint_distance"],
                "endpoint_within_tolerance": item["endpoint_within_tolerance"],
                "same_component": item["same_component"],
                "joined_by_t_junction": item["joined_by_t_junction"],
                "tight_png": str(tight),
                "wide_png": str(wide),
            }
        )
    montage(tight_paths, CROPS / "tcf_x_montage_tight.png")
    montage(wide_paths, CROPS / "tcf_x_montage_wide.png")
    OUT.write_text(
        json.dumps({"probe": "tcf_p3_crossings", "tolerance": TOLERANCE, "per_block": stats, "samples": samples},
                   ensure_ascii=False, indent=1),
        encoding="utf-8",
    )
    print("\nsamples:")
    for s in samples:
        print(s["n"], s["pair"], s["point_norm"], "min_param", s["min_param"],
              "near_end", s["nearest_endpoint_distance"], "same_comp", s["same_component"],
              "t_joined", s["joined_by_t_junction"])


if __name__ == "__main__":
    main()
