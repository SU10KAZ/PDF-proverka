#!/usr/bin/env python3
"""Reproduce the isolated vector-block benchmark and its machine artifacts."""
from __future__ import annotations

import argparse
import collections
import json
import time
from pathlib import Path
from typing import Any

from .comparator import compare_descriptions, save_comparison
from .extractor import extract_block, save_description


EXPERIMENT_DIR = Path(__file__).resolve().parent
REPOSITORY_ROOT = EXPERIMENT_DIR.parents[1]
DEFAULT_MANIFEST = EXPERIMENT_DIR / "artifacts" / "block_pairs.json"


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_pdf(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = REPOSITORY_ROOT / path
    if not path.is_file():
        raise FileNotFoundError(path)
    return path


def _run_side(
    pair_id: str, side_name: str, side: dict[str, Any], *, reuse_descriptions: bool = False
) -> tuple[dict[str, Any], float]:
    output_dir = EXPERIMENT_DIR / "artifacts" / "descriptions" / pair_id / side_name
    existing = output_dir / "vector_block.json"
    if reuse_descriptions and existing.is_file():
        description = _load(existing)
        if description["block_id"] != side["block_id"]:
            raise ValueError(f"Cached block id does not match manifest: {existing}")
        return description, 0.0
    started = time.perf_counter()
    description = extract_block(
        _resolve_pdf(side["pdf"]),
        page_index=int(side["page_index"]),
        bbox_norm=side["bbox_norm"],
        polygon_norm=side.get("polygon_norm"),
        block_id=side["block_id"],
    )
    save_description(description, output_dir, diagnostic_png=True)
    diagnostic_dir = EXPERIMENT_DIR / "artifacts" / "diagnostics" / pair_id
    diagnostic_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "diagnostic_crop.png").replace(diagnostic_dir / f"{side_name}.png")
    return description, time.perf_counter() - started


def _size_row(description: dict[str, Any]) -> dict[str, Any]:
    sizes = description["size_metrics"]
    return {
        "block_id": description["block_id"],
        "vector_quality": description["vector_quality"],
        "primitive_count": description["primitive_summary"]["primitive_count"],
        "text_items": description["primitive_summary"]["text_items"],
        "raw": sizes["level_0_raw_vector"],
        "normalized": sizes["level_1_normalized_primitives"],
        "grouped": sizes["level_2_groups_topology"],
        "compact": sizes["level_3_compact_description"],
    }


def _markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Reproducible benchmark result",
        "",
        f"- Pairs: {summary['pair_count']}",
        f"- Blocks: {summary['block_count']}",
        f"- Total extraction time: {summary['timing_seconds']['extraction_total']:.3f} s",
        f"- Total comparison time: {summary['timing_seconds']['comparison_total']:.3f} s",
        "",
        "## Comparator statuses",
        "",
    ]
    lines.extend(f"- {key}: {value}" for key, value in summary["status_counts"].items())
    lines.extend(
        [
            "",
            "## Pairs",
            "",
            "| Pair | Discipline | Type | Status | Geometry | Text | Topology |",
            "|---|---|---|---|---:|---:|---:|",
        ]
    )
    for pair in summary["pairs"]:
        lines.append(
            f"| {pair['pair_id']} | {pair['discipline']} | {pair['type']} | {pair['status']} | "
            f"{pair['geometry_similarity']:.3f} | {pair['text_similarity']:.3f} | "
            f"{pair['topology_similarity']:.3f} |"
        )
    return "\n".join(lines) + "\n"


def run(
    manifest_path: Path = DEFAULT_MANIFEST, *, reuse_descriptions: bool = False
) -> dict[str, Any]:
    manifest = _load(manifest_path)
    pair_rows: list[dict[str, Any]] = []
    size_rows: list[dict[str, Any]] = []
    extraction_total = 0.0
    comparison_total = 0.0
    for pair in manifest["pairs"]:
        pair_id = pair["pair_id"]
        left, left_time = _run_side(
            pair_id, "left", pair["left"], reuse_descriptions=reuse_descriptions
        )
        right, right_time = _run_side(
            pair_id, "right", pair["right"], reuse_descriptions=reuse_descriptions
        )
        extraction_total += left_time + right_time
        started = time.perf_counter()
        comparison = compare_descriptions(left, right)
        comparison_total += time.perf_counter() - started
        comparison_dir = EXPERIMENT_DIR / "artifacts" / "comparisons" / pair_id
        save_comparison(comparison, left, right, comparison_dir)
        pair_rows.append(
            {
                "pair_id": pair_id,
                "discipline": pair["discipline"],
                "type": pair["type"],
                "human_expected": pair["human_expected"],
                "status": comparison["status"],
                "selected_tolerance": comparison["geometry"]["selected_tolerance"],
                "geometry_similarity": comparison["geometry"]["similarity"],
                "text_similarity": comparison["text"]["effective_similarity"],
                "text_reliable": comparison["text"]["reliable"],
                "topology_similarity": comparison["topology"]["similarity"],
                "left_quality": left["vector_quality"],
                "right_quality": right["vector_quality"],
                "left_extraction_seconds": round(left_time, 6),
                "right_extraction_seconds": round(right_time, 6),
            }
        )
        size_rows.extend((_size_row(left), _size_row(right)))
    status_counts = collections.Counter(row["status"] for row in pair_rows)
    summary = {
        "schema_version": "vector-block-benchmark-results-v0.1",
        "research_only": True,
        "manifest": str(manifest_path.relative_to(EXPERIMENT_DIR)),
        "pair_count": len(pair_rows),
        "block_count": len(size_rows),
        "status_counts": dict(sorted(status_counts.items())),
        "timing_seconds": {
            "extraction_total": round(extraction_total, 6),
            "comparison_total": round(comparison_total, 6),
        },
        "pairs": pair_rows,
        "sizes": size_rows,
    }
    artifact_dir = EXPERIMENT_DIR / "artifacts"
    (artifact_dir / "benchmark_results.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (artifact_dir / "benchmark_results.md").write_text(_markdown(summary), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument(
        "--reuse-descriptions",
        action="store_true",
        help="Re-run only comparison/report generation from existing description JSON files.",
    )
    args = parser.parse_args()
    summary = run(args.manifest.resolve(), reuse_descriptions=args.reuse_descriptions)
    print(json.dumps({key: summary[key] for key in ("pair_count", "block_count", "status_counts", "timing_seconds")}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
