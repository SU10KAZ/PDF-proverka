#!/usr/bin/env python3
"""Evaluate Stage 5.3 on independent controlled semantic ground truth."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.stage_comparison import high_level_project_changes as high  # noqa: E402
from backend.app.services.stage_comparison import project_change_summary as stage5  # noqa: E402


DEFAULT_DATASET = ROOT / "benchmarks" / "stage_5_3_high_level_ground_truth.json"


def _render(value: Any, index: int) -> Any:
    return value.replace("{i}", str(index)) if isinstance(value, str) else value


def _evidence(case_id: str, spec: dict[str, Any], index: int) -> dict[str, Any]:
    evidence_id = f"{case_id}:{spec['id']}:{index}"
    before, after = _render(spec.get("before"), index), _render(spec.get("after"), index)
    stage5_class = spec.get("class", "PROJECT_CHANGE")
    hint = {
        "PROJECT_CHANGE": "PROJECT_CHANGE",
        "SERVICE_STRUCTURE": "SERVICE_STRUCTURE",
        "REVIEW": "REVIEW",
    }[stage5_class]
    return {
        "evidence_id": evidence_id,
        "source_status": spec.get("status", "CHANGED"),
        "summary": _render(spec.get("summary", ""), index),
        "before": before, "after": after,
        "reason": "Controlled human-ground-truth evidence.",
        "left_fragment_ids": [f"L-{evidence_id}"] if before else [],
        "right_fragment_ids": [f"R-{evidence_id}"] if after else [],
        "left_pages": [index] if before else [],
        "right_pages": [index + 1] if after else [],
        "left_anchors": [], "right_anchors": [],
        "deterministic_class_hint": hint,
        "deterministic_category_hint": spec.get("category", "uncertain"),
    }


def _project_summary(case: dict[str, Any]) -> tuple[dict[str, Any], dict[str, list[str]]]:
    sheet_groups = []
    input_evidence: dict[str, list[str]] = {}
    for position, spec in enumerate(case["inputs"], start=1):
        evidence = [
            _evidence(case["id"], spec, index)
            for index in range(1, int(spec.get("repeat", 1)) + 1)
        ]
        input_evidence[spec["id"]] = [item["evidence_id"] for item in evidence]
        stage5_class = spec.get("class", "PROJECT_CHANGE")
        bucket = {
            "PROJECT_CHANGE": "project_changes",
            "SERVICE_STRUCTURE": "service_structure",
            "REVIEW": "review",
        }[stage5_class]
        item = {
            "id": f"stage5-{case['id']}-{spec['id']}", "title": spec["summary"],
            "category": spec.get("category", "uncertain"),
            "evidence_ids": input_evidence[spec["id"]], "count": len(evidence),
            "details": evidence,
        }
        group = {
            "group_id": spec.get("group_id", f"sheet-{position}"),
            "left_pages": [position], "right_pages": [position + 1],
            "left_labels": [f"Лист П {position}"], "right_labels": [f"Лист РД {position + 1}"],
            "pair_precheck": {"status": spec.get("pair_status", stage5.PAIR_OK)},
            "aggregation_status": "human_controlled",
            "project_changes": [], "service_structure": [], "review": [],
            "atomic_evidence": evidence,
        }
        group[bucket] = [item]
        sheet_groups.append(group)
    return ({
        "version": stage5.VERSION, "kind": stage5.KIND,
        "pair_id": case["id"], "source_signature": f"ground-truth:{case['id']}",
        "status": "completed", "sheet_groups": sheet_groups,
    }, input_evidence)


def _expected_routes(case: dict[str, Any], input_evidence: dict[str, list[str]]) -> dict[str, str]:
    expected = case["expected"]
    routes = expected.get("evidence_routes")
    if routes is None:
        if expected["change_count"]:
            default = "PUBLISH"
        elif expected["detail_count"]:
            default = "DETAIL"
        elif expected["material_review_count"]:
            default = "REVIEW"
        else:
            default = "SUPPRESS"
        routes = {input_id: default for input_id in input_evidence}
    if set(routes) != set(input_evidence):
        raise ValueError(f"{case['id']}: incomplete expected evidence routes")
    return {
        evidence_id: routes[input_id]
        for input_id, evidence_ids in input_evidence.items()
        for evidence_id in evidence_ids
    }


def _actual_routes(artifact: dict[str, Any]) -> dict[str, str]:
    routes: dict[str, str] = {}
    for bucket, route in (
        (artifact["high_level_changes"], "PUBLISH"),
        (artifact["detail_level_increased"], "DETAIL"),
        (artifact["material_review"], "REVIEW"),
        (artifact["non_material_review"], "SUPPRESS"),
        (artifact["service_structure_summary"]["items"], "SUPPRESS"),
    ):
        for item in bucket:
            for evidence_id in item["evidence_ids"]:
                if evidence_id in routes:
                    raise ValueError(f"evidence routed twice: {evidence_id}")
                routes[evidence_id] = route
    return routes


def _run_case(case: dict[str, Any]) -> dict[str, Any]:
    summary, input_evidence = _project_summary(case)
    semantic_groups = high.build_semantic_groups(summary)
    resolved, ai_required = high.deterministic_decisions(semantic_groups)
    decisions = resolved + [
        high.fallback_decision(group, "benchmark_no_ai") for group in ai_required
    ]
    artifact = high.build_artifact(
        pair_id=case["id"], generated_at="benchmark",
        source_signature_value=high.source_signature(summary, semantic_groups),
        project_summary=summary, semantic_groups=semantic_groups, decisions=decisions,
    )
    expected = case["expected"]
    expected_types = Counter(expected["publish_types"])
    actual_types = Counter(item["type"] for item in artifact["high_level_changes"])
    expected_routes = _expected_routes(case, input_evidence)
    actual_routes = _actual_routes(artifact)
    type_tp = sum((expected_types & actual_types).values())
    route_matches = sum(
        expected_routes[evidence_id] == actual_routes.get(evidence_id)
        for evidence_id in expected_routes
    )
    return {
        "id": case["id"], "split": case["split"],
        "expected": expected,
        "actual": {
            "publish_types": list(actual_types.elements()),
            "change_count": len(artifact["high_level_changes"]),
            "detail_count": len(artifact["detail_level_increased"]),
            "material_review_count": len(artifact["material_review"]),
            "semantic_groups": artifact["summary"]["semantic_groups"],
            "titles": [item["title"] for item in artifact["high_level_changes"]],
        },
        "evidence": {
            "expected_routes": expected_routes,
            "actual_routes": actual_routes,
            "route_matches": route_matches,
            "total": len(expected_routes),
        },
        "counts": {
            "true_project_changes": type_tp,
            "false_project_changes": max(0, sum(actual_types.values()) - type_tp),
            "missed_project_changes": max(0, sum(expected_types.values()) - type_tp),
            "over_fragmentation": max(0, len(artifact["high_level_changes"]) - expected["change_count"]),
            "over_merge": max(0, expected["change_count"] - len(artifact["high_level_changes"])),
            "detail_as_change_error": sum(
                expected_routes[evidence_id] == "DETAIL" and actual_routes.get(evidence_id) == "PUBLISH"
                for evidence_id in expected_routes
            ),
            "service_as_project_error": sum(
                spec.get("class") == "SERVICE_STRUCTURE"
                and any(actual_routes.get(value) == "PUBLISH" for value in input_evidence[spec["id"]])
                for spec in case["inputs"]
            ),
            "material_review_tp": sum(
                expected_routes[evidence_id] == actual_routes.get(evidence_id) == "REVIEW"
                for evidence_id in expected_routes
            ),
            "material_review_predicted": sum(value == "REVIEW" for value in actual_routes.values()),
            "unsupported_claims": 0,
        },
        "passed": (
            expected_types == actual_types
            and expected["change_count"] == len(artifact["high_level_changes"])
            and expected["detail_count"] == len(artifact["detail_level_increased"])
            and expected["material_review_count"] == len(artifact["material_review"])
            and route_matches == len(expected_routes)
        ),
    }


def _metrics(results: list[dict[str, Any]]) -> dict[str, Any]:
    totals = Counter()
    for result in results:
        totals.update(result["counts"])
    tp = totals["true_project_changes"]
    false = totals["false_project_changes"]
    missed = totals["missed_project_changes"]
    review_tp = totals["material_review_tp"]
    review_predicted = totals["material_review_predicted"]
    evidence_total = sum(item["evidence"]["total"] for item in results)
    evidence_matches = sum(item["evidence"]["route_matches"] for item in results)
    return {
        "cases": len(results), "passed_cases": sum(item["passed"] for item in results),
        "high_level_precision": round(tp / max(1, tp + false), 4),
        "high_level_recall": round(tp / max(1, tp + missed), 4),
        "false_project_change": false, "missed_project_change": missed,
        "over_fragmentation": totals["over_fragmentation"],
        "over_merge": totals["over_merge"],
        "detail_as_change_error": totals["detail_as_change_error"],
        "service_as_project_error": totals["service_as_project_error"],
        "unsupported_claims": totals["unsupported_claims"],
        "material_review_precision": round(review_tp / max(1, review_predicted), 4),
        "evidence_route_accuracy": round(evidence_matches / max(1, evidence_total), 4),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    dataset = json.loads(args.dataset.read_text(encoding="utf-8"))
    results = []
    for case in dataset["cases"]:
        try:
            results.append(_run_case(case))
        except high.HighLevelValidationError as exc:
            results.append({
                "id": case["id"], "split": case["split"], "expected": case["expected"],
                "actual": {"validation_error": str(exc)}, "evidence": {"total": 0, "route_matches": 0},
                "counts": {"unsupported_claims": 1}, "passed": False,
            })
    payload = {
        "schema_version": "1.0", "kind": "stage_5_3_high_level_benchmark_result",
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "dataset": str(args.dataset.relative_to(ROOT) if args.dataset.is_relative_to(ROOT) else args.dataset),
        "metrics": {
            "overall": _metrics(results),
            "calibration": _metrics([item for item in results if item["split"] == "calibration"]),
            "holdout": _metrics([item for item in results if item["split"] == "holdout"]),
        },
        "failed_case_ids": [item["id"] for item in results if not item["passed"]],
        "cases": results,
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(json.dumps({
        "metrics": payload["metrics"], "failed_case_ids": payload["failed_case_ids"],
    }, ensure_ascii=False, indent=2))
    return 0 if not payload["failed_case_ids"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
