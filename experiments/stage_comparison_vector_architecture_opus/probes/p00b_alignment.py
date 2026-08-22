#!/usr/bin/env python3
"""Orchestrator probe O9: is the tolerance ladder absorbing a systematic crop offset?

Track A's comparator raises its matching tolerance until coverage clears 0.985.  On vk_nodes,
coverage moves from 0.095 at 0.10 % to 0.991 at 1.00 %.  Random PDF coordinate noise would raise
coverage gradually; a step like that is the signature of a systematic offset/scale mismatch
introduced by normalising two *different* crops onto the same unit square.

This probe estimates a translation + uniform scale from the segment sets themselves (a coarse
grid search over midpoints, no image features, no ORB, no affine warp of content), applies it, and
re-measures directional coverage at every tolerance.

Run from the repository root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.p00b_alignment
"""
from __future__ import annotations

import collections
import json
import math
from pathlib import Path
from typing import Any, Sequence

DESCRIPTIONS = Path("experiments/stage_comparison_vector_blocks/artifacts/descriptions")
TOLERANCES = (0.001, 0.0025, 0.005, 0.01)
CAP = 12_000


def load(pair: str, side: str) -> dict[str, Any]:
    return json.loads((DESCRIPTIONS / pair / side / "vector_block.json").read_text(encoding="utf-8"))


def segments(description: dict[str, Any]) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    for primitive in description["geometry"]["primitives"]:
        for start, end in primitive["normalized"]["segments"]:
            dx, dy = end[0] - start[0], end[1] - start[1]
            length = math.hypot(dx, dy)
            if length <= 1e-8:
                continue
            rows.append(
                {
                    "cx": (start[0] + end[0]) / 2,
                    "cy": (start[1] + end[1]) / 2,
                    "length": length,
                    "angle": math.degrees(math.atan2(dy, dx)) % 180,
                }
            )
    if len(rows) > CAP:
        rows = sorted(rows, key=lambda item: item["length"], reverse=True)[:CAP]
    return rows


def _angle_distance(left: float, right: float) -> float:
    difference = abs(left - right) % 180
    return min(difference, 180 - difference)


def coverage(
    source: Sequence[dict[str, float]],
    target: Sequence[dict[str, float]],
    tolerance: float,
    shift: tuple[float, float] = (0.0, 0.0),
    scale: tuple[float, float] = (1.0, 1.0),
) -> float:
    cell = max(tolerance * 2.0, 0.001)
    buckets: dict[tuple[int, int], list[dict[str, float]]] = collections.defaultdict(list)
    for feature in target:
        buckets[(round(feature["cx"] / cell), round(feature["cy"] / cell))].append(feature)
    matched = 0
    for first in source:
        cx = first["cx"] * scale[0] + shift[0]
        cy = first["cy"] * scale[1] + shift[1]
        length = first["length"] * (scale[0] + scale[1]) / 2
        gx, gy = round(cx / cell), round(cy / cell)
        found = False
        for x in range(gx - 2, gx + 3):
            for y in range(gy - 2, gy + 3):
                for second in buckets.get((x, y), []):
                    if math.hypot(cx - second["cx"], cy - second["cy"]) > tolerance * 3.0:
                        continue
                    if abs(length - second["length"]) > tolerance * 8.0:
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


def estimate(
    left: Sequence[dict[str, float]], right: Sequence[dict[str, float]]
) -> tuple[tuple[float, float], tuple[float, float], float]:
    """Coarse-to-fine search for the translation + per-axis scale that maximises coverage."""
    best = ((0.0, 0.0), (1.0, 1.0), coverage(left, right, 0.0025))
    sample = sorted(left, key=lambda item: item["length"], reverse=True)[:1500]
    target = sorted(right, key=lambda item: item["length"], reverse=True)[:1500]
    for scale_x in (0.98, 0.99, 1.0, 1.01, 1.02):
        for scale_y in (0.98, 0.99, 1.0, 1.01, 1.02):
            for shift_x in [i * 0.002 for i in range(-6, 7)]:
                for shift_y in [i * 0.002 for i in range(-6, 7)]:
                    score = coverage(
                        sample, target, 0.0025, shift=(shift_x, shift_y), scale=(scale_x, scale_y)
                    )
                    if score > best[2]:
                        best = ((shift_x, shift_y), (scale_x, scale_y), score)
    return best


def main() -> None:
    pairs = ["vk_nodes", "vk_node_plan", "vk_plan", "ss_table_graphic", "ss_plan_dense", "ar_wall_sections"]
    output: dict[str, Any] = {}
    for pair in pairs:
        left = segments(load(pair, "left"))
        right = segments(load(pair, "right"))
        shift, scale, _ = estimate(left, right)
        before = {
            f"{tolerance:.4f}": round(
                (coverage(left, right, tolerance) + coverage(right, left, tolerance)) / 2, 4
            )
            for tolerance in TOLERANCES
        }
        inverse_shift = (-shift[0] / scale[0], -shift[1] / scale[1])
        inverse_scale = (1 / scale[0], 1 / scale[1])
        after = {
            f"{tolerance:.4f}": round(
                (
                    coverage(left, right, tolerance, shift=shift, scale=scale)
                    + coverage(right, left, tolerance, shift=inverse_shift, scale=inverse_scale)
                )
                / 2,
                4,
            )
            for tolerance in TOLERANCES
        }
        output[pair] = {
            "estimated_shift_norm": [round(value, 4) for value in shift],
            "estimated_scale": [round(value, 4) for value in scale],
            "coverage_before": before,
            "coverage_after": after,
        }
        print(pair, json.dumps(output[pair], ensure_ascii=False))

    destination = Path(
        "experiments/stage_comparison_vector_architecture_opus/artifacts/p00b_alignment.json"
    )
    destination.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print("written", destination)


if __name__ == "__main__":
    main()
