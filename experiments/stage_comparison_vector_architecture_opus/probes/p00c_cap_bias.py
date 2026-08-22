#!/usr/bin/env python3
"""Orchestrator probe O11: what does the longest-first cap actually throw away?

Track A caps storage at 20 000 primitives and segment coverage at 12 000 segments, keeping the
LONGEST segments.  Long segments are frames, borders, buses and walls — precisely the geometry that
does not change between two versions of the same sheet.  This probe measures how much of a block
the comparator really sees, and whether the discarded short geometry agrees as well as the kept
long geometry.

Run from the repository root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.p00c_cap_bias
"""
from __future__ import annotations

import collections
import json
import math
import random
from pathlib import Path
from typing import Any, Sequence

import fitz

from experiments.stage_comparison_vector_architecture_opus.poc.objects_v02 import strokes

BENCHMARK = Path("experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json")
CAP = 12_000
TOLERANCE = 0.005
SEED = 20260823


def _angle_distance(left: float, right: float) -> float:
    difference = abs(left - right) % 180
    return min(difference, 180 - difference)


def coverage(source: Sequence[dict[str, Any]], target: Sequence[dict[str, Any]], tolerance: float) -> float:
    cell = max(tolerance * 2.0, 0.001)
    buckets: dict[tuple[int, int], list[dict[str, Any]]] = collections.defaultdict(list)
    for feature in target:
        buckets[(round(feature["cx"] / cell), round(feature["cy"] / cell))].append(feature)
    matched = 0
    for first in source:
        gx, gy = round(first["cx"] / cell), round(first["cy"] / cell)
        found = False
        for x in range(gx - 2, gx + 3):
            for y in range(gy - 2, gy + 3):
                for second in buckets.get((x, y), ()):
                    if math.hypot(first["cx"] - second["cx"], first["cy"] - second["cy"]) > tolerance * 3.0:
                        continue
                    if abs(first["length"] - second["length"]) > tolerance * 8.0:
                        continue
                    if _angle_distance(first["angle"], second["angle"]) > max(1.0, tolerance * 500):
                        continue
                    found = True
                    break
                if found:
                    break
            if found:
                break
        matched += 1 if found else 0
    return matched / max(len(source), 1)


def features(page: fitz.Page, rect: Sequence[float]) -> list[dict[str, Any]]:
    rows = []
    for stroke in strokes(page, rect):
        rows.append(
            {
                "cx": (stroke["p1"][0] + stroke["p2"][0]) / 2,
                "cy": (stroke["p1"][1] + stroke["p2"][1]) / 2,
                "length": stroke["length"],
                "angle": stroke["angle"],
            }
        )
    return rows


def main() -> None:
    random.seed(SEED)
    manifest = json.loads(BENCHMARK.read_text(encoding="utf-8"))
    output: dict[str, Any] = {}
    for pair in manifest["pairs"]:
        sides = {}
        for side in ("left", "right"):
            spec = pair[side]
            document = fitz.open(spec["pdf"])
            page = document[spec["page_index"]]
            box = spec["bbox_norm"]
            rect = [
                box[0] * page.rect.width,
                box[1] * page.rect.height,
                box[2] * page.rect.width,
                box[3] * page.rect.height,
            ]
            sides[side] = features(page, rect)
            document.close()

        left, right = sides["left"], sides["right"]
        total = max(len(left), len(right))
        kept_left = sorted(left, key=lambda item: -item["length"])[:CAP]
        kept_right = sorted(right, key=lambda item: -item["length"])[:CAP]
        rest_left = sorted(left, key=lambda item: -item["length"])[CAP:]
        rest_right = sorted(right, key=lambda item: -item["length"])[CAP:]
        sample_left = random.sample(rest_left, min(CAP, len(rest_left))) if rest_left else []
        sample_right = random.sample(rest_right, min(CAP, len(rest_right))) if rest_right else []

        record: dict[str, Any] = {
            "left_segments": len(left),
            "right_segments": len(right),
            "fraction_seen_by_comparator": round(min(1.0, CAP / max(total, 1)), 4),
            "coverage_kept_longest": round(
                (coverage(kept_left, kept_right, TOLERANCE) + coverage(kept_right, kept_left, TOLERANCE)) / 2,
                4,
            ),
        }
        if sample_left and sample_right:
            record["coverage_discarded_short"] = round(
                (
                    coverage(sample_left, sample_right, TOLERANCE)
                    + coverage(sample_right, sample_left, TOLERANCE)
                )
                / 2,
                4,
            )
            record["discarded_sampled"] = len(sample_left)
        else:
            record["coverage_discarded_short"] = None
            record["discarded_sampled"] = 0
        output[pair["pair_id"]] = record
        print(pair["pair_id"], json.dumps(record, ensure_ascii=False))

    destination = Path(
        "experiments/stage_comparison_vector_architecture_opus/artifacts/p00c_cap_bias.json"
    )
    destination.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print("written", destination)


if __name__ == "__main__":
    main()
