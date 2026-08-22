#!/usr/bin/env python3
"""Build all real v0.3 artifacts from existing prepared block references."""
from __future__ import annotations

import json
import gzip
import hashlib
import statistics
import time
from pathlib import Path
from typing import Any

from .benchmark_data import TABLE_ONLY, TEXT_ONLY, benchmark_manifest, ground_truth_artifact
from .comparator import compare_graphic_scopes
from .controlled_falsifiers import run_controlled_falsifiers
from .input_contract import resolve_prepared_block
from .objects import build_graphic_block_description
from .page_cache import PageDrawingCache


EXPERIMENT_DIR = Path(__file__).resolve().parent
ARTIFACTS = EXPERIMENT_DIR / "artifacts"


def _write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_retained(path: Path, payload: dict[str, Any], *, kind: str) -> None:
    """Keep a readable index plus a lossless deterministic compressed payload."""
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    full_path = path.with_name(path.stem + ".full.json.gz")
    full_path.parent.mkdir(parents=True, exist_ok=True)
    full_path.write_bytes(gzip.compress(canonical + b"\n", compresslevel=9, mtime=0))
    digest = hashlib.sha256(canonical).hexdigest()
    def compact_evidence(row: dict[str, Any]) -> dict[str, Any]:
        evidence = row.get("evidence")
        serialized = json.dumps(evidence, ensure_ascii=False, separators=(",", ":"))
        if len(serialized) <= 1_000:
            return row
        return {**row, "evidence": {"sample_truncated": True, "uncompressed_json_chars": len(serialized), "preview": serialized[:500]}}
    if kind == "description":
        index = {key: payload.get(key) for key in ("schema_version", "research_only", "input", "quality", "visible_geometry_summary", "uncertainties")}
        prepared = (index.get("input") or {}).get("prepared_text_metadata", [])
        if prepared:
            index["input"] = {**index["input"], "prepared_text_metadata": prepared[:10], "prepared_text_metadata_count": len(prepared)}
        object_sample = []
        for row in payload.get("objects", [])[:10]:
            object_sample.append({**row, "relation_ids": row.get("relation_ids", [])[:20], "provenance": row.get("provenance", [])[:5]})
        index.update({"objects": object_sample, "object_families": payload.get("object_families", [])[:10], "relations": payload.get("relations", [])[:20]})
        counts = {"objects": len(payload.get("objects", [])), "object_families": len(payload.get("object_families", [])), "relations": len(payload.get("relations", []))}
    else:
        index = {key: payload.get(key) for key in ("schema_version", "research_only", "block_pair", "scope", "applicability", "alignment", "validator", "decision")}
        if index.get("alignment"):
            index["alignment"] = {**index["alignment"], "evidence": index["alignment"].get("evidence", [])[:20]}
        index.update({"object_matches": [compact_evidence(row) for row in payload.get("object_matches", [])[:10]], "object_statuses": [compact_evidence(row) for row in payload.get("object_statuses", [])[:20]], "changes": [compact_evidence(row) for row in payload.get("changes", [])[:20]]})
        counts = {"object_matches": len(payload.get("object_matches", [])), "object_statuses": len(payload.get("object_statuses", [])), "changes": len(payload.get("changes", []))}
    index["artifact_retention"] = {"index_is_sampled": True, "full_lossless_artifact": full_path.name, "full_uncompressed_sha256": digest, "full_uncompressed_bytes": len(canonical), "full_counts": counts}
    _write(path, index)


def _hybrid_packet(ledger: dict[str, Any]) -> dict[str, Any]:
    changes = []
    for row in ledger.get("changes", [])[:3]:
        changes.append({key: row.get(key) for key in ("type", "left_object", "right_object", "confidence")})
    uncertainty = ledger["decision"]["route_reasons"]
    return {
        "pair_id": ledger["block_pair"], "vector_found": changes,
        "target_uncertainty": uncertainty,
        "question": "Determine whether these candidates are real graphical-object changes or crop/export/primitive-packaging differences. Ignore text and table content.",
    }


def _metrics(rows: list[dict[str, Any]], timings: list[float], cache: PageDrawingCache, falsifiers: dict[str, Any]) -> dict[str, Any]:
    scored = [row for row in rows if row["ground_truth"] != "UNSURE"]
    correct = [row for row in scored if (row["ground_truth"] == "GRAPHIC_CHANGE") == row["detected_change"]]
    false_positives = [row for row in scored if row["ground_truth"] == "NO_GRAPHIC_CHANGE" and row["detected_change"]]
    false_negatives = [row for row in scored if row["ground_truth"] == "GRAPHIC_CHANGE" and not row["detected_change"]]
    unchanged = [row for row in rows if row["ground_truth"] == "NO_GRAPHIC_CHANGE" and row["applicability"] == "GRAPHIC_APPLICABLE"]
    routes = {route: sum(row["route"] == route for row in rows) for route in ("GRAPHIC_VECTOR_OK", "GRAPHIC_HYBRID", "GRAPHIC_VISION_ONLY")}
    safe = [row for row in scored if row["route"] == "GRAPHIC_VECTOR_OK"]
    unmatched_predicted = [row for row in rows if row["has_added_or_removed"]]
    actual_unmatched = [row for row in rows if row["ground_truth"] == "GRAPHIC_CHANGE"]
    added_predicted = [row for row in scored if "ADDED" in row["change_status_counts"]]
    removed_predicted = [row for row in scored if "REMOVED" in row["change_status_counts"]]
    actual_added = [row for row in scored if "ADDED_OBJECT" in row.get("expected_event_types", [])]
    actual_removed = [row for row in scored if "REMOVED_OBJECT" in row.get("expected_event_types", [])]
    connection_predicted = [row for row in rows if row["has_connection_change"]]
    return {
        "real_pair_count": len(rows), "scored_pair_count": len(scored),
        "graphic_pair_accuracy": {"correct": len(correct), "total": len(scored), "rate": round(len(correct) / max(len(scored), 1), 6), "false_positive_pairs": [row["pair_id"] for row in false_positives], "false_negative_pairs": [row["pair_id"] for row in false_negatives]},
        "object_detection_stability_on_no_change": {"pair_count": len(unchanged), "median_object_count_ratio": round(statistics.median(row["object_count_ratio"] for row in unchanged), 6) if unchanged else None, "median_matched_fraction": round(statistics.median(row["matched_fraction"] for row in unchanged), 6) if unchanged else None, "exact_object_count_pairs": sum(row["object_count_ratio"] == 1 for row in unchanged)},
        "object_matching_precision_recall": {"value": None, "reason": "No exhaustive human per-object correspondence annotation exists for the 38 real pairs; matched fraction is stability evidence, not precision/recall.", "controlled_mechanism_checks": f"{falsifiers['passed']}/{falsifiers['total']}"},
        "added_object_pair_level": {"predicted_pairs": len(added_predicted), "true_positive_pairs": sum(row in actual_added for row in added_predicted), "actual_positive_pairs": len(actual_added), "precision": round(sum(row in actual_added for row in added_predicted) / max(len(added_predicted), 1), 6), "recall": round(sum(row in added_predicted for row in actual_added) / max(len(actual_added), 1), 6), "scope": "pair-level proxy; exhaustive per-object annotation unavailable"},
        "removed_object_pair_level": {"predicted_pairs": len(removed_predicted), "true_positive_pairs": sum(row in actual_removed for row in removed_predicted), "actual_positive_pairs": len(actual_removed), "precision": round(sum(row in actual_removed for row in removed_predicted) / max(len(removed_predicted), 1), 6), "recall": round(sum(row in removed_predicted for row in actual_removed) / max(len(actual_removed), 1), 6), "scope": "pair-level proxy; exhaustive per-object annotation unavailable"},
        "added_removed_object_candidates": {"candidate_pairs": len(unmatched_predicted), "candidate_pair_ids": [row["pair_id"] for row in unmatched_predicted], "confirmed_positive_pairs": len(actual_unmatched), "confirmed_positive_recalled": sum(row["has_added_or_removed"] for row in actual_unmatched)},
        "connection_change_precision_recall": {"value": None, "predicted_pairs": len(connection_predicted), "reason": "The eligible real corpus has no manually confirmed connection-only positive; controlled connection falsifier is reported separately."},
        "local_change_recall": {"confirmed_pairs_recalled": sum(row["detected_change"] for row in actual_unmatched), "confirmed_pairs": len(actual_unmatched), "rate": round(sum(row["detected_change"] for row in actual_unmatched) / max(len(actual_unmatched), 1), 6)},
        "text_only_false_graphic_change": {"false_changes": sum(row["detected_change"] for row in rows if row["pair_id"] in TEXT_ONLY), "controls": len(TEXT_ONLY), "rate": round(sum(row["detected_change"] for row in rows if row["pair_id"] in TEXT_ONLY) / len(TEXT_ONLY), 6)},
        "table_only_false_graphic_change": {"false_changes": sum(row["detected_change"] for row in rows if row["pair_id"] in TABLE_ONLY), "controls": len(TABLE_ONLY), "rate": round(sum(row["detected_change"] for row in rows if row["pair_id"] in TABLE_ONLY) / len(TABLE_ONLY), 6), "note": "Both real table controls are upstream block_type=text and are GRAPHIC_NOT_APPLICABLE."},
        "graphic_vector_ok_false_safe": {"false_safe": sum(not ((row["ground_truth"] == "GRAPHIC_CHANGE") == row["detected_change"]) for row in safe), "vector_ok_pairs": len(safe), "rate": round(sum(not ((row["ground_truth"] == "GRAPHIC_CHANGE") == row["detected_change"]) for row in safe) / max(len(safe), 1), 6)},
        "routing": {**routes, "vision_usage_count": routes["GRAPHIC_HYBRID"] + routes["GRAPHIC_VISION_ONLY"], "vision_usage_rate": round((routes["GRAPHIC_HYBRID"] + routes["GRAPHIC_VISION_ONLY"]) / len(rows), 6)},
        "latency_seconds_per_pair": {"median": round(statistics.median(timings), 6), "total": round(sum(timings), 6), "max": round(max(timings), 6)},
        "cache": {**cache.stats, "disk_size_bytes": cache.disk_size_bytes()},
        "tokens": {"deterministic_vector": 0, "hybrid": "measured in artifacts/hybrid_results.json for a single fused call; no verify-all arm"},
    }


def _human_validation(rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Human validation — graphic-only scope", "",
        "Ground truth was fixed by manual side-by-side raster inspection; model output was not used as truth. Text and table-content differences were deliberately removed from graphic GT.", "",
        "| Pair | Human graphic GT | Deterministic | Route | Note |", "|---|---|---|---|---|",
    ]
    for row in rows:
        note = row["scope_note"].replace("|", "/")
        lines.append(f"| {row['pair_id']} | {row['ground_truth']} | {'GRAPHIC_CHANGE' if row['detected_change'] else 'NO_GRAPHIC_CHANGE'} | {row['route']} | {note} |")
    lines.extend(["", "The corpus limitation is material: only three pairs (`vk_plan`, `vk_nodes`, `ov_plan_floor07`) are manually confirmed graphical-change positives. `ss_crop_mismatch_page07` is UNSURE because existing prepared blocks cover different semantic extents.", ""])
    return "\n".join(lines)


def run() -> dict[str, Any]:
    manifest = benchmark_manifest(); truth = ground_truth_artifact(); truth_by_id = {row["pair_id"]: row for row in truth["pairs"]}
    _write(ARTIFACTS / "benchmark_pairs.json", manifest); _write(ARTIFACTS / "ground_truth.json", truth)
    cache = PageDrawingCache(EXPERIMENT_DIR / ".page_cache"); rows = []; timings = []
    for index, pair in enumerate(manifest["pairs"], 1):
        started = time.perf_counter(); descriptions: dict[str, list[dict[str, Any]]] = {}
        for scope_key, side_name in (("left_blocks", "left"), ("right_blocks", "right")):
            descriptions[side_name] = []
            for block_index, reference in enumerate(pair["scope"][scope_key], 1):
                description = build_graphic_block_description(resolve_prepared_block(reference), cache)
                descriptions[side_name].append(description)
                _write_retained(ARTIFACTS / "object_descriptions" / pair["pair_id"] / f"{side_name}-{block_index}.json", description, kind="description")
        ledger = compare_graphic_scopes(pair["pair_id"], descriptions["left"], descriptions["right"], block_group_id=pair["scope"]["block_group_id"])
        _write_retained(ARTIFACTS / "object_comparisons" / pair["pair_id"] / "graphic_change_ledger.json", ledger, kind="ledger")
        elapsed = time.perf_counter() - started; timings.append(elapsed)
        gt = truth_by_id[pair["pair_id"]]
        decision = ledger["decision"]; left_count = decision.get("left_object_count", 0); right_count = decision.get("right_object_count", 0)
        status_set = {row["status"] for row in ledger.get("changes", [])}
        packet = _hybrid_packet(ledger) if decision["route"] == "GRAPHIC_HYBRID" else None
        rows.append({
            "pair_id": pair["pair_id"], "ground_truth": gt["expected_graphic_verdict"], "expected_event_types": sorted({event["type"] for event in gt["important_graphic_events"]}), "scope_note": gt["graphic_scope_note"],
            "applicability": ledger["applicability"], "detected_change": bool(ledger["changes"]), "route": decision["route"], "route_reasons": decision["route_reasons"],
            "change_count": len(ledger["changes"]), "change_status_counts": dict(sorted(__import__("collections").Counter(row["status"] for row in ledger["changes"]).items())),
            "has_added_or_removed": bool(status_set & {"ADDED", "REMOVED"}), "has_connection_change": "CONNECTION_CHANGED" in status_set,
            "matched_fraction": decision.get("matched_fraction", 1.0), "left_object_count": left_count, "right_object_count": right_count,
            "object_count_ratio": min(left_count, right_count) / max(left_count, right_count, 1) if max(left_count, right_count, 1) else 1,
            "alignment": ledger.get("alignment"), "hybrid_packet": packet,
            "latency_seconds": round(elapsed, 6),
        })
        print(f"[{index:02d}/{len(manifest['pairs'])}] {pair['pair_id']} {decision['route']} changes={len(ledger['changes'])} {elapsed:.2f}s", flush=True)
    falsifiers = run_controlled_falsifiers(); _write(ARTIFACTS / "controlled_falsifiers.json", falsifiers)
    result = {"schema_version": "graphic-object-routing-results-v0.3-codex", "research_only": True, "pairs": rows}
    result["metrics"] = _metrics(rows, timings, cache, falsifiers)
    _write(ARTIFACTS / "routing_results.json", result)
    (ARTIFACTS / "human_validation.md").write_text(_human_validation(rows), encoding="utf-8")
    return result


if __name__ == "__main__":
    run()
