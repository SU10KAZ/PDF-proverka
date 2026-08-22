#!/usr/bin/env python3
"""Run the 39-pair research benchmark and write compact artifacts."""
from __future__ import annotations

import collections
import json
import statistics
import tempfile
import time
from pathlib import Path
from typing import Any

from experiments.stage_comparison_vector_blocks import extractor as baseline_extractor

from .benchmark_data import REPOSITORY_ROOT, benchmark_manifest, ground_truth_artifact
from .comparator import compare_descriptions
from .extractor import PageCache, extract_block, extract_block_from_payload
from .gates import route_comparison
from .l3_change_only import build_l3_change_only, payload_metrics


EXPERIMENT_DIR = Path(__file__).resolve().parent
ARTIFACTS = EXPERIMENT_DIR / "artifacts"
CACHE_DIR = EXPERIMENT_DIR / ".page_cache"


def _dump(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _resolve(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else REPOSITORY_ROOT / path


def _old_l3(left: dict[str, Any], right: dict[str, Any], comparison: dict[str, Any]) -> dict[str, Any]:
    return {
        "left_level_3": left["size_metrics"]["compact_payload"],
        "right_level_3": right["size_metrics"]["compact_payload"],
        "deterministic_diff": comparison,
    }


def _result(expected_route: str, actual_route: str, expected_verdict: str, actual_verdict: str) -> str:
    route_ok, verdict_ok = expected_route == actual_route, expected_verdict == actual_verdict
    if route_ok and verdict_ok:
        return "CORRECT"
    if route_ok or verdict_ok:
        return "PARTIAL"
    return "WRONG"


def _cache_profile(full_cache: PageCache, full_times: list[float]) -> dict[str, Any]:
    doc = Path("projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13AB-РД-СОТ-К7 V1")
    pdf = REPOSITORY_ROOT / doc / "versions/v002/02_work/document.pdf"
    blocks_data = json.loads((REPOSITORY_ROOT / doc / "versions/v002/02_work/blocks.json").read_text(encoding="utf-8"))["blocks"]
    blocks = [row for row in blocks_data if row["page_index"] == 4][:5]
    with tempfile.TemporaryDirectory(prefix="vector-v02-cache-") as directory:
        disk_dir = Path(directory)
        cold_cache = PageCache(disk_dir)
        started = time.perf_counter()
        payload = cold_cache.get(pdf, 4)
        cold_payload = time.perf_counter() - started
        started = time.perf_counter()
        for row in blocks:
            extract_block_from_payload(payload, bbox_norm=row["coords_norm"], block_id=row["block_id"])
        shared_block_clip = time.perf_counter() - started

        disk_cache = PageCache(disk_dir)
        started = time.perf_counter(); warm_payload = disk_cache.get(pdf, 4); disk_payload = time.perf_counter() - started
        started = time.perf_counter()
        for row in blocks:
            extract_block_from_payload(warm_payload, bbox_norm=row["coords_norm"], block_id=row["block_id"])
        warm_blocks = time.perf_counter() - started
        started = time.perf_counter(); disk_cache.get(pdf, 4); memory_payload = time.perf_counter() - started

        started = time.perf_counter()
        for row in blocks:
            baseline_extractor.extract_block(pdf, page_index=4, bbox_norm=row["coords_norm"], block_id=row["block_id"])
        repeated_uncached = time.perf_counter() - started
        cached_total = cold_payload + shared_block_clip
        warm_total = disk_payload + warm_blocks
        dense_pdf = REPOSITORY_ROOT / "projects_v2/objects/256_Primavera_K14_Spartak/disciplines/AR/documents/СТ26_01-14-АР0-АС-1-РД_V1/versions/v001/02_work/document.pdf"
        dense_dir = disk_dir / "dense"
        dense_cold_cache = PageCache(dense_dir)
        started = time.perf_counter(); dense_cold_cache.get(dense_pdf, 14); dense_cold = time.perf_counter() - started
        dense_disk_cache = PageCache(dense_dir)
        started = time.perf_counter(); dense_disk_cache.get(dense_pdf, 14); dense_disk = time.perf_counter() - started
        started = time.perf_counter(); dense_disk_cache.get(dense_pdf, 14); dense_memory = time.perf_counter() - started
        started = time.perf_counter()
        for _ in range(5):
            document = __import__("fitz").open(dense_pdf)
            page = document[14]
            page.get_drawings(); page.get_text("dict")
            document.close()
        dense_repeated_raw = time.perf_counter() - started
        return {
            "schema_version": "vector-cache-profile-v0.2-codex",
            "real_pdf": str(pdf.relative_to(REPOSITORY_ROOT)),
            "page_index": 4,
            "blocks_on_same_page": len(blocks),
            "cold": {"page_payload_seconds": round(cold_payload, 6), "end_to_end_seconds": round(cached_total, 6)},
            "warm_disk": {"page_payload_seconds": round(disk_payload, 6), "end_to_end_seconds": round(warm_total, 6)},
            "warm_memory": {"page_payload_seconds": round(memory_payload, 6)},
            "repeated_uncached_baseline_seconds": round(repeated_uncached, 6),
            "speedup": {
                "page_payload_cold_to_disk": round(cold_payload / max(disk_payload, 1e-9), 3),
                "page_payload_cold_to_memory": round(cold_payload / max(memory_payload, 1e-9), 3),
                "five_blocks_uncached_vs_shared_page": round(repeated_uncached / max(cached_total, 1e-9), 3),
                "five_blocks_uncached_vs_warm": round(repeated_uncached / max(warm_total, 1e-9), 3),
            },
            "temporary_cache_bytes": sum(path.stat().st_size for path in disk_dir.glob("*.pickle.gz")),
            "get_drawings_calls_for_five_cached_blocks": cold_cache.stats["get_drawings_calls"],
            "dense_page_payload_profile": {
                "real_pdf": str(dense_pdf.relative_to(REPOSITORY_ROOT)), "page_index": 14,
                "drawings": 15352, "simulated_blocks": 5,
                "cold_cache_build_seconds": round(dense_cold, 6),
                "warm_disk_seconds": round(dense_disk, 6),
                "warm_memory_seconds": round(dense_memory, 6),
                "five_repeated_raw_page_reads_seconds": round(dense_repeated_raw, 6),
                "cache_bytes": dense_cold_cache.disk_size_bytes(),
                "speedup_repeated_vs_cold_shared": round(dense_repeated_raw / max(dense_cold, 1e-9), 3),
                "speedup_repeated_vs_warm_disk": round(dense_repeated_raw / max(dense_disk, 1e-9), 3),
                "speedup_repeated_vs_memory": round(dense_repeated_raw / max(dense_memory, 1e-9), 3),
            },
            "full_benchmark": {
                "block_extractions": len(full_times),
                "median_block_seconds": round(statistics.median(full_times), 6),
                "cache_bytes": full_cache.disk_size_bytes(),
                "cache_stats": full_cache.stats,
            },
        }


def _human_markdown(rows: list[dict[str, Any]], summary: dict[str, Any]) -> str:
    lines = [
        "# Human validation — Vector v02 Codex", "",
        "Ground truth was fixed by side-by-side visual inspection of every real crop pair. Model outputs were not used as the judge.", "",
        "| Pair | Discipline | Human route | Actual route | Vector verdict | Human verdict | Result |",
        "|---|---|---|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            f"| {row['pair_id']} | {row['discipline']} | {row['human_route']} | {row['actual_route']} | "
            f"{row['vector_verdict']} | {row['human_verdict']} | {row['result']} |"
        )
    lines.extend([
        "", "## Counts", "",
        f"- Pairs: {summary['pair_count']}",
        f"- Correct: {summary['result_counts'].get('CORRECT', 0)}",
        f"- Partial: {summary['result_counts'].get('PARTIAL', 0)}",
        f"- Wrong: {summary['result_counts'].get('WRONG', 0)}",
        f"- Routing errors: {summary['routing_errors']}",
        f"- False VECTOR_OK: {summary['false_vector_ok']}",
        "", "KJ/KM/GP were not substituted with artificial pairs: the active corpus did not contain usable two-version block manifests for those disciplines.",
    ])
    return "\n".join(lines) + "\n"


def run() -> dict[str, Any]:
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    manifest = benchmark_manifest()
    ground_truth = ground_truth_artifact()
    _dump(ARTIFACTS / "benchmark_pairs.json", manifest)
    _dump(ARTIFACTS / "ground_truth.json", ground_truth)
    cache = PageCache(CACHE_DIR)
    rows = []
    extraction_times: list[float] = []
    comparison_times: list[float] = []
    prompt_rows = []
    style_rows = []
    started_all = time.perf_counter()
    for index, pair in enumerate(manifest["pairs"], 1):
        sides = []
        for side_name in ("left", "right"):
            side = pair[side_name]
            started = time.perf_counter()
            description = extract_block(
                _resolve(side["pdf"]), page_index=side["page_index"], bbox_norm=side["bbox_norm"],
                polygon_norm=side.get("polygon_norm"), block_id=side["block_id"], page_cache=cache,
            )
            extraction_times.append(time.perf_counter() - started)
            sides.append(description)
        started = time.perf_counter(); comparison = compare_descriptions(*sides); comparison_times.append(time.perf_counter() - started)
        routing = route_comparison(*sides, comparison)
        change_only = build_l3_change_only(comparison, routing)
        old = _old_l3(*sides, comparison)
        gt = pair["ground_truth"]
        row = {
            "pair_id": pair["pair_id"], "discipline": pair["discipline"], "type": pair["type"],
            "human_route": gt["expected_route"], "actual_route": routing["route"],
            "vector_verdict": comparison["status"], "human_verdict": gt["expected_verdict"],
            "result": _result(gt["expected_route"], routing["route"], gt["expected_verdict"], comparison["status"]),
            "routing": routing, "evidence": comparison["evidence"], "l3_change_only": change_only,
            "important_factual_changes": gt["important_factual_changes"],
            "quality": {"left_vector": sides[0]["vector_quality"], "right_vector": sides[1]["vector_quality"], "left_text": sides[0]["text_quality"], "right_text": sides[1]["text_quality"], "left_caps": sides[0]["cap_flags"], "right_caps": sides[1]["cap_flags"]},
        }
        rows.append(row)
        old_metrics, new_metrics = payload_metrics(old), payload_metrics(change_only)
        prompt_rows.append({
            "pair_id": pair["pair_id"], "old_l3": old_metrics, "l3_change_only": new_metrics,
            "estimated_reduction_percent": round((1 - new_metrics["estimated_tokens"] / max(old_metrics["estimated_tokens"], 1)) * 100, 3),
            "real_model_input_tokens": None,
        })
        style_rows.append({"pair_id": pair["pair_id"], "status": comparison["status"], **comparison["style"]})
        print(f"[{index:02d}/{len(manifest['pairs'])}] {pair['pair_id']}: {routing['route']} / {comparison['status']}", flush=True)

    route_confusion: dict[str, collections.Counter[str]] = collections.defaultdict(collections.Counter)
    for row in rows:
        route_confusion[row["human_route"]][row["actual_route"]] += 1
    result_counts = collections.Counter(row["result"] for row in rows)
    route_counts = collections.Counter(row["actual_route"] for row in rows)
    verdict_counts = collections.Counter(row["vector_verdict"] for row in rows)
    false_vector_ok = sum(row["actual_route"] == "VECTOR_OK" and row["human_route"] != "VECTOR_OK" for row in rows)
    summary = {
        "schema_version": "vector-routing-results-v0.2-codex", "research_only": True,
        "pair_count": len(rows), "route_counts": dict(sorted(route_counts.items())),
        "verdict_counts": dict(sorted(verdict_counts.items())), "result_counts": dict(sorted(result_counts.items())),
        "routing_confusion_matrix": {key: dict(sorted(value.items())) for key, value in sorted(route_confusion.items())},
        "routing_errors": sum(row["human_route"] != row["actual_route"] for row in rows),
        "false_vector_ok": false_vector_ok,
        "vision_usage_rate": round(sum(row["actual_route"] != "VECTOR_OK" for row in rows) / len(rows), 6),
        "timing_seconds": {
            "total": round(time.perf_counter() - started_all, 6),
            "extraction_total": round(sum(extraction_times), 6), "comparison_total": round(sum(comparison_times), 6),
            "extraction_median": round(statistics.median(extraction_times), 6), "comparison_median": round(statistics.median(comparison_times), 6),
        },
        "pairs": rows,
    }
    _dump(ARTIFACTS / "routing_results.json", summary)
    _dump(ARTIFACTS / "style_results.json", {"schema_version": "vector-style-results-v0.2-codex", "real_pairs": style_rows, "controlled_cases": []})
    prompt_summary = {
        "schema_version": "vector-prompt-size-v0.2-codex", "pairs": prompt_rows,
        "aggregate": {
            "old_l3_median_estimated_tokens": statistics.median(row["old_l3"]["estimated_tokens"] for row in prompt_rows),
            "change_only_median_estimated_tokens": statistics.median(row["l3_change_only"]["estimated_tokens"] for row in prompt_rows),
            "median_reduction_percent": round(statistics.median(row["estimated_reduction_percent"] for row in prompt_rows), 3),
            "real_model_tokens_pending_hybrid_run": True,
        },
    }
    _dump(ARTIFACTS / "prompt_size_results.json", prompt_summary)
    cache_profile = _cache_profile(cache, extraction_times)
    _dump(ARTIFACTS / "cache_profile.json", cache_profile)
    (ARTIFACTS / "human_validation.md").write_text(_human_markdown(rows, summary), encoding="utf-8")
    return summary


if __name__ == "__main__":
    result = run()
    print(json.dumps({key: result[key] for key in ("pair_count", "route_counts", "result_counts", "routing_errors", "false_vector_ok", "timing_seconds")}, ensure_ascii=False, indent=2))
