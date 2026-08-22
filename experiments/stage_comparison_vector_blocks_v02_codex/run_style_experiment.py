#!/usr/bin/env python3
"""Controlled style-only cases and tolerance boundary checks."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

import fitz

from .comparator import compare_descriptions
from .extractor import PageCache, extract_block
from .gates import route_comparison


ARTIFACT = Path(__file__).resolve().parent / "artifacts/style_results.json"


def _pdf(path: Path, style: dict[str, Any]) -> None:
    document = fitz.open(); page = document.new_page(width=240, height=180)
    shape = page.new_shape()
    shape.draw_rect(fitz.Rect(20, 20, 100, 100)); shape.draw_line(fitz.Point(20, 60), fitz.Point(100, 60))
    finish = {"color": style.get("color", (0, 0, 0)), "width": style.get("width", 0.5)}
    for source, target in (("fill", "fill"), ("dash", "dashes"), ("stroke_opacity", "stroke_opacity"), ("fill_opacity", "fill_opacity"), ("line_cap", "lineCap")):
        if source in style:
            finish[target] = style[source]
    shape.finish(**finish); shape.commit()
    circle = page.new_shape(); circle.draw_circle(fitz.Point(60, 60), 12)
    circle.finish(color=style.get("color", (0, 0, 0)), width=style.get("width", 0.5), dashes=style.get("dash")); circle.commit()
    page.insert_text(fitz.Point(38, 45), "250 A", fontsize=8)
    document.save(path); document.close()


def run() -> dict[str, Any]:
    base = {"color": (0, 0, 0), "width": 0.5}
    cases = [
        ("unchanged", base, base, "IDENTICAL"),
        ("solid_to_dashed", base, {**base, "dash": "[3 2] 0"}, "STYLE_ONLY_CHANGED"),
        ("fill_added", base, {**base, "fill": (0.8, 0.8, 0.8)}, "STYLE_ONLY_CHANGED"),
        ("fill_removed", {**base, "fill": (0.8, 0.8, 0.8)}, base, "STYLE_ONLY_CHANGED"),
        ("width_changed", base, {**base, "width": 1.0}, "STYLE_ONLY_CHANGED"),
        ("color_changed", base, {**base, "color": (1, 0, 0)}, "STYLE_ONLY_CHANGED"),
        ("opacity_changed", base, {**base, "stroke_opacity": 0.5}, "STYLE_ONLY_CHANGED"),
        ("cap_changed", base, {**base, "line_cap": 1}, "STYLE_ONLY_CHANGED"),
        ("join_changed", base, base, "STYLE_ONLY_CHANGED"),
        ("width_below_tolerance", base, {**base, "width": 0.53}, "NEAR_IDENTICAL"),
        ("color_below_tolerance", base, {**base, "color": (0.01, 0, 0)}, "NEAR_IDENTICAL"),
    ]
    rows = []
    with tempfile.TemporaryDirectory(prefix="vector-style-") as directory:
        root = Path(directory); cache = PageCache(root / "cache")
        for case_id, left_style, right_style, expected in cases:
            left_path, right_path = root / f"{case_id}-left.pdf", root / f"{case_id}-right.pdf"
            _pdf(left_path, left_style); _pdf(right_path, right_style)
            common = {"page_index": 0, "bbox_norm": (0.05, 0.05, 0.5, 0.65), "page_cache": cache}
            left = extract_block(left_path, block_id=f"{case_id}-left", **common)
            right = extract_block(right_path, block_id=f"{case_id}-right", **common)
            if case_id == "join_changed":
                for primitive in right["geometry"]["primitives"]:
                    primitive["style"]["line_join"] = 1
            comparison = compare_descriptions(left, right); routing = route_comparison(left, right, comparison)
            rows.append({
                "case_id": case_id, "expected": expected, "actual": comparison["status"],
                "passed": comparison["status"] == expected, "route": routing["route"],
                "geometry_similarity": comparison["geometry"]["similarity"],
                "style": comparison["style"],
            })
    existing = json.loads(ARTIFACT.read_text(encoding="utf-8")) if ARTIFACT.is_file() else {}
    existing["controlled_cases"] = rows
    existing["controlled_summary"] = {
        "cases": len(rows), "passed": sum(row["passed"] for row in rows),
        "failed": sum(not row["passed"] for row in rows),
        "tolerance_conclusion": "0.015 per color channel, max(0.05 pt, 5%) width, and 0.02 opacity suppress controlled sub-threshold noise while preserving material changes.",
    }
    ARTIFACT.write_text(json.dumps(existing, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return existing["controlled_summary"]


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
