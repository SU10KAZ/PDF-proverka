#!/usr/bin/env python3
"""signoise probe 8 — the minimal sub-contract the Track A comparator actually reads.

Builds a stripped description that keeps ONLY the keys `comparator.compare_descriptions` touches,
re-runs all 10 pairs, and asserts that status, every score and every emitted difference line are
byte-identical to the full-payload run. Reports how many bytes that removes.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_08_minimal_contract
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from experiments.stage_comparison_vector_blocks import comparator as C

ROOT = Path(__file__).resolve().parents[3]
DESC = ROOT / "experiments/stage_comparison_vector_blocks/artifacts/descriptions"
OUT = Path(__file__).resolve().parents[1] / "artifacts"

PAIRS = ["ss_scheme_text_changed", "ss_plan_dense", "ss_simple_node", "ss_table_graphic",
         "ar_plan", "ar_wall_sections", "vk_plan", "vk_nodes", "vk_node_plan",
         "eom_singleline_changed"]

TOPOLOGY_KEYS = ("node_count", "edge_count", "connected_components", "endpoints", "branch_points",
                 "t_junctions", "x_crossings_unconnected", "closed_contours", "nested_contours")


def compact(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def minimal(description: dict[str, Any]) -> dict[str, Any]:
    return {
        "block_id": description["block_id"],
        "vector_quality": description["vector_quality"],
        "primitive_summary": {"primitive_count": description["primitive_summary"]["primitive_count"]},
        "geometry": {
            "extraction": {"source_item_counts": description["geometry"]["extraction"]["source_item_counts"]},
            "primitives": [
                {
                    "id": p["id"],
                    "type": p["type"],
                    "closed": p["closed"],
                    "segment_count": p["segment_count"],
                    "length_norm": p["length_norm"],
                    "angle_degrees": p["angle_degrees"],
                    "normalized": {"bbox": p["normalized"]["bbox"], "segments": p["normalized"]["segments"]},
                }
                for p in description["geometry"]["primitives"]
            ],
        },
        "texts": [
            {"text": t["text"], "category": t["category"], "x_norm": t["x_norm"], "y_norm": t["y_norm"]}
            for t in description["texts"]
        ],
        "topology": {k: description["topology"][k] for k in TOPOLOGY_KEYS},
        "repeated_elements": [
            {"pattern_id": r["pattern_id"], "count": r["count"]} for r in description["repeated_elements"]
        ],
        "structural_signature": {
            k: description["structural_signature"][k]
            for k in ("level_1_exact_vector", "level_2_normalized_geometry", "level_3_structural_topology")
        },
    }


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    rows, full_bytes, minimal_bytes, mismatches = [], 0, 0, []
    for pair in PAIRS:
        left = json.loads((DESC / pair / "left" / "vector_block.json").read_text(encoding="utf-8"))
        right = json.loads((DESC / pair / "right" / "vector_block.json").read_text(encoding="utf-8"))
        reference = C.compare_descriptions(left, right)
        small_left, small_right = minimal(left), minimal(right)
        pair_full = compact(left) + compact(right)
        pair_min = compact(small_left) + compact(small_right)
        del left, right
        stripped = C.compare_descriptions(small_left, small_right)
        identical = (
            reference["status"] == stripped["status"]
            and reference["differences"] == stripped["differences"]
            and reference["geometry"]["similarity"] == stripped["geometry"]["similarity"]
            and reference["text"] == stripped["text"]
            and reference["topology"]["similarity"] == stripped["topology"]["similarity"]
            and reference["repeated_patterns"] == stripped["repeated_patterns"]
            and reference["exact_vector_signature_equal"] == stripped["exact_vector_signature_equal"]
        )
        if not identical:
            mismatches.append(pair)
        full_bytes += pair_full
        minimal_bytes += pair_min
        rows.append({
            "pair": pair,
            "status_full": reference["status"],
            "status_minimal": stripped["status"],
            "output_identical": identical,
            "full_bytes": pair_full,
            "minimal_bytes": pair_min,
            "reduction_percent": round(100 * (1 - pair_min / pair_full), 3),
        })
        print(f"{pair:24s} {reference['status']:30s} identical={identical} "
              f"{pair_full:,} -> {pair_min:,} B ({100*(1-pair_min/pair_full):.1f} % smaller)")

    payload = {
        "probe": "signoise_08_minimal_contract",
        "research_only": True,
        "pairs_with_output_mismatch": mismatches,
        "corpus_full_bytes": full_bytes,
        "corpus_minimal_bytes": minimal_bytes,
        "corpus_reduction_percent": round(100 * (1 - minimal_bytes / full_bytes), 3),
        "kept_keys": {
            "top_level": ["block_id", "vector_quality", "primitive_summary.primitive_count",
                          "geometry.extraction.source_item_counts", "geometry.primitives[]",
                          "texts[]", "topology (9 count keys)", "repeated_elements[].pattern_id/count",
                          "structural_signature.level_1/2/3"],
            "primitive": ["id", "type", "closed", "segment_count", "length_norm", "angle_degrees",
                          "normalized.bbox", "normalized.segments"],
            "text": ["text", "category", "x_norm", "y_norm"],
        },
        "per_pair": rows,
    }
    (OUT / "signoise_08_minimal_contract.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")

    lines = [
        "# signoise probe 8 — minimal sub-contract that reproduces every Track A verdict",
        "",
        "Reproduce: `python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_08_minimal_contract`",
        "",
        f"Pairs where the stripped payload changed ANY part of the output: **{len(mismatches)}/10** "
        f"{mismatches}",
        "",
        f"Corpus: {full_bytes:,} B full -> {minimal_bytes:,} B minimal "
        f"(**{100*(1-minimal_bytes/full_bytes):.1f} %** of VectorBlockDescription v0.1 is unread by the comparator).",
        "",
        "| pair | status (full) | status (minimal) | output identical | full B | minimal B | reduction |",
        "|---|---|---|:--:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            f"| {row['pair']} | {row['status_full']} | {row['status_minimal']} | "
            f"{'YES' if row['output_identical'] else 'NO'} | {row['full_bytes']:,} | "
            f"{row['minimal_bytes']:,} | {row['reduction_percent']:.1f} % |"
        )
    (OUT / "signoise_08_minimal_contract.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("written:", OUT / "signoise_08_minimal_contract.json")


if __name__ == "__main__":
    main()
