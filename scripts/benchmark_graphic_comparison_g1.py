#!/usr/bin/env python3
"""Run the production G1 router on the research 56-pair regression corpus.

Heavy images are not copied.  The fixture contains existing PDF paths and
prepared block bboxes; output is a compact metrics JSON.  ``--assert-baseline``
turns the research safety properties into a non-zero exit status.
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

ARTIFACTS = REPOSITORY_ROOT / "experiments" / "local_graphic_diff_mode1_opus" / "artifacts"
STRONG_GRSH = REPOSITORY_ROOT / "experiments" / "strong_stage_graph_comparison_grsh"


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _record(pair: dict[str, Any], side: str) -> dict[str, Any]:
    return {
        "block_id": str(pair[f"block_{side}"]),
        "page_index": int(pair[f"page_index_{side}"]),
        "page_label": int(pair[f"page_index_{side}"]) + 1,
        "block_type": "image",
        "coords_norm": list(pair[f"bbox_{side}"]),
        "source": "mode1_56_pair_regression_fixture",
    }


def _run_pair(pair: dict[str, Any]) -> dict[str, Any]:
    from backend.app.services.stage_comparison.graphic_comparison import compare_prepared_blocks

    start = time.perf_counter()
    ledger = compare_prepared_blocks(
        left_pdf_path=pair["pdf_left"],
        right_pdf_path=pair["pdf_right"],
        left_records=[_record(pair, "left")],
        right_records=[_record(pair, "right")],
        left_source_artifact="benchmark_pairs.json",
        right_source_artifact="benchmark_pairs.json",
    )
    elapsed = time.perf_counter() - start
    registration = (ledger.get("quality") or {}).get("registration") or {}
    diff = (ledger.get("quality") or {}).get("diff") or {}
    extraction = (ledger.get("quality") or {}).get("extraction") or {}
    return {
        "pair_id": pair["pair_id"],
        "bucket": pair["bucket"],
        "route": ledger["route"],
        "reason_code": ledger["diagnostics"]["routing"]["reason_code"],
        "changes": len(ledger["changes"]),
        "types": dict(sorted(Counter(change["type"] for change in ledger["changes"]).items())),
        "change_bboxes_right": [
            change["evidence"][0]["right_comparison_bbox_visual_pt"]
            for change in ledger["changes"]
        ],
        "symmetric_coverage": (registration.get("coverage") or {}).get("sym_cov"),
        "changed_ink_fraction": diff.get("changed_ink_fraction"),
        "regions": diff.get("n_regions_published"),
        "extraction_flags": extraction.get("flags") or {},
        "extraction": {
            "left_precision": (extraction.get("left") or {}).get("precision"),
            "left_recall": (extraction.get("left") or {}).get("recall"),
            "right_precision": (extraction.get("right") or {}).get("precision"),
            "right_recall": (extraction.get("right") or {}).get("recall"),
        },
        "latency_s": round(elapsed, 3),
    }


def _touches(left: list[float], right: list[float], pad_pt: float = 6.0) -> bool:
    """Research evaluator's physical-point localization criterion."""
    return not (
        left[2] + pad_pt < right[0]
        or left[0] - pad_pt > right[2]
        or left[3] + pad_pt < right[1]
        or left[1] - pad_pt > right[3]
    )


def _strong_grsh_result() -> dict[str, Any]:
    from backend.app.services.stage_comparison.graphic_comparison import compare_prepared_blocks

    resolved = []
    for side in ("left", "right"):
        metadata = _load(STRONG_GRSH / f"{side}_structural_description.json")["input"]
        pdf_path = REPOSITORY_ROOT / metadata["source_pdf"]
        blocks_path = REPOSITORY_ROOT / metadata["blocks_json"]
        blocks = _load(blocks_path)["blocks"]
        record = next(item for item in blocks if item.get("block_id") == metadata["block_id"])
        resolved.append((pdf_path, record))
    started = time.perf_counter()
    ledger = compare_prepared_blocks(
        left_pdf_path=resolved[0][0], right_pdf_path=resolved[1][0],
        left_records=[resolved[0][1]], right_records=[resolved[1][1]],
        left_source_artifact="blocks.json", right_source_artifact="blocks.json",
    )
    return {
        "route": ledger["route"],
        "reason_code": ledger["diagnostics"]["routing"]["reason_code"],
        "changes_published": len(ledger["changes"]),
        "symmetric_coverage": (
            ((ledger.get("quality") or {}).get("registration") or {}).get("coverage") or {}
        ).get("sym_cov"),
        "latency_s": round(time.perf_counter() - started, 3),
    }


def _metrics(results: list[dict[str, Any]], include_strong_grsh: bool) -> dict[str, Any]:
    ground_truth = {
        item["pair_id"]: item
        for item in _load(ARTIFACTS / "human_ground_truth.json")["pairs"]
    }
    routing = {
        item["pair_id"]: item
        for item in _load(ARTIFACTS / "routing_results.json")["pairs"]
    }
    confusion = Counter()
    mode1_results = [item for item in results if item["route"] == "MODE_1_APPLICABLE"]
    for result in mode1_results:
        actual = bool(ground_truth[result["pair_id"]]["graphic_change"])
        predicted = result["changes"] > 0
        confusion[(predicted, actual)] += 1
    gt_graphic_regions = 0
    hit_graphic_regions = 0
    gt_strict_regions = 0
    hit_strict_regions = 0
    for result in mode1_results:
        gt_regions = [
            region for region in ground_truth[result["pair_id"]].get("gt_regions") or []
            if (region.get("text_share") or 0.0) < 0.5
        ]
        strict_regions = [
            region for region in ground_truth[result["pair_id"]].get("gt_regions") or []
            if (region.get("text_share") or 0.0) < 0.3
        ]
        gt_graphic_regions += len(gt_regions)
        gt_strict_regions += len(strict_regions)
        for region in gt_regions:
            if any(_touches(region["bbox_pt"], predicted) for predicted in result["change_bboxes_right"]):
                hit_graphic_regions += 1
        for region in strict_regions:
            if any(_touches(region["bbox_pt"], predicted) for predicted in result["change_bboxes_right"]):
                hit_strict_regions += 1
    non_degenerate = [item for item in results if not routing[item["pair_id"]]["degenerate_input"]]
    routing_matches = sum(
        item["route"] == routing[item["pair_id"]]["desired_route"]
        for item in non_degenerate
    )
    major_redesigns_in_mode1 = [
        item["pair_id"] for item in results
        if routing[item["pair_id"]]["desired_route"] == "MODE_2_REQUIRED"
        and item["route"] == "MODE_1_APPLICABLE"
    ]
    repack = [item for item in results if item["bucket"] == "repack"]
    no_change_false_positive_pairs = [
        item["pair_id"] for item in mode1_results
        if not ground_truth[item["pair_id"]]["graphic_change"] and item["changes"]
    ]
    latencies = sorted(item["latency_s"] for item in results)
    precisions = [
        float(value)
        for item in results
        for key, value in item["extraction"].items()
        if key.endswith("_precision") and value is not None
    ]
    recalls = [
        float(value)
        for item in results
        for key, value in item["extraction"].items()
        if key.endswith("_recall") and value is not None
    ]
    percentile_90 = latencies[min(len(latencies) - 1, int(0.9 * (len(latencies) - 1)))]
    summary: dict[str, Any] = {
        "schema_version": "graphic-comparison-g1-regression-v1",
        "fixture": "b37e9f20 benchmark_pairs.json",
        "pairs": len(results),
        "routes": dict(sorted(Counter(item["route"] for item in results).items())),
        "mode1_pair_level": {
            "TP": confusion[(True, True)],
            "FP": confusion[(True, False)],
            "FN": confusion[(False, True)],
            "TN": confusion[(False, False)],
        },
        "mode1_region_recall": {
            "graphic_gt_regions": gt_graphic_regions,
            "hit": hit_graphic_regions,
            "recall": round(hit_graphic_regions / gt_graphic_regions, 4) if gt_graphic_regions else None,
            "strict_text_cutoff_0_3_gt_regions": gt_strict_regions,
            "strict_hit": hit_strict_regions,
            "strict_recall": round(hit_strict_regions / gt_strict_regions, 4) if gt_strict_regions else None,
        },
        "routing": {
            "correct_excluding_degenerate": routing_matches,
            "total_excluding_degenerate": len(non_degenerate),
            "major_redesigns_kept_in_mode1": major_redesigns_in_mode1,
        },
        "negative_controls": {
            "false_positive_pairs": no_change_false_positive_pairs,
            "repack_pairs": len(repack),
            "repack_pairs_with_changes": [item["pair_id"] for item in repack if item["changes"]],
        },
        "latency_s": {
            "median": round(statistics.median(latencies), 3),
            "p90": round(percentile_90, 3),
            "max": round(max(latencies), 3),
        },
        "extraction": {
            "precision_min": min(precisions) if precisions else None,
            "precision_median": round(statistics.median(precisions), 4) if precisions else None,
            "recall_min": min(recalls) if recalls else None,
            "recall_median": round(statistics.median(recalls), 4) if recalls else None,
        },
    }
    if include_strong_grsh:
        summary["strong_grsh"] = _strong_grsh_result()
    summary["baseline_checks"] = {
        "no_major_redesign_in_mode1": not major_redesigns_in_mode1,
        "no_mode1_false_positive_pair": not no_change_false_positive_pairs,
        "mode1_local_change_recall_preserved": confusion[(False, True)] == 0,
        "mode1_region_recall_preserved": (
            hit_graphic_regions / max(1, gt_graphic_regions) >= 40 / 42
        ),
        "mode1_strict_region_recall_preserved": (
            hit_strict_regions == gt_strict_regions
        ),
        "strong_grsh_routes_mode2": (
            not include_strong_grsh or summary["strong_grsh"]["route"] == "MODE_2_REQUIRED"
        ),
        "mode2_publishes_no_changes": all(
            item["changes"] == 0 for item in results if item["route"] == "MODE_2_REQUIRED"
        ),
    }
    return {"summary": summary, "pairs": sorted(results, key=lambda item: item["pair_id"])}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=min(8, os.cpu_count() or 1))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--include-strong-grsh", action="store_true")
    parser.add_argument("--assert-baseline", action="store_true")
    arguments = parser.parse_args()
    pairs = _load(ARTIFACTS / "benchmark_pairs.json")["pairs"]
    missing = [
        pair["pair_id"] for pair in pairs
        if not Path(pair["pdf_left"]).is_file() or not Path(pair["pdf_right"]).is_file()
    ]
    if missing:
        raise SystemExit(f"Regression source PDFs are missing for: {', '.join(missing[:10])}")
    results = []
    with ProcessPoolExecutor(max_workers=max(1, arguments.workers)) as executor:
        futures = {executor.submit(_run_pair, pair): pair["pair_id"] for pair in pairs}
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            print(
                f"{len(results):02d}/{len(pairs)} {result['pair_id']}: "
                f"{result['route']} changes={result['changes']} {result['latency_s']:.3f}s",
                file=sys.stderr,
            )
    payload = _metrics(results, arguments.include_strong_grsh)
    encoded = json.dumps(payload, ensure_ascii=False, indent=2)
    if arguments.output:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(encoded + "\n", encoding="utf-8")
    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))
    if arguments.assert_baseline and not all(payload["summary"]["baseline_checks"].values()):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
