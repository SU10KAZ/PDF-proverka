"""Three-run product stability gate for bounded selector artifacts."""
from __future__ import annotations

import itertools
from pathlib import Path
from typing import Any, Mapping, Sequence
import json

from ..production_artifacts import content_signature


def _load(directory: Path, name: str) -> dict[str, Any]:
    path = directory / f"{name}.json"
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path}: root is not an object")
    return value


def _products(materialization: Mapping[str, Any]) -> set[str]:
    return {
        content_signature({
            "task_id": value.get("task_id"),
            "selected_candidate_id": value.get("selected_candidate_id"),
            "product_fingerprint": value.get("product_fingerprint"),
        })
        for value in materialization.get("outcomes") or ()
        if isinstance(value, Mapping) and value.get("product_fingerprint")
    }


def _selection_products(materialization: Mapping[str, Any]) -> list[dict[str, Any]]:
    return sorted([
        {
            "task_id": value.get("task_id"),
            "selected_candidate_id": value.get("selected_candidate_id"),
            "product_fingerprint": value.get("product_fingerprint"),
            "outcome": value.get("outcome"),
        }
        for value in materialization.get("outcomes") or ()
        if isinstance(value, Mapping) and value.get("product_fingerprint")
    ], key=lambda value: (str(value["task_id"]), str(value["selected_candidate_id"])))


def _jaccard(left: set[str], right: set[str]) -> dict[str, Any]:
    union = left | right
    overlap = left & right
    return {
        "intersection": len(overlap),
        "union": len(union),
        "ratio": round(len(overlap) / len(union), 6) if union else 1.0,
    }


def evaluate(run_dirs: Sequence[Path]) -> dict[str, Any]:
    if len(run_dirs) != 3:
        raise ValueError("exactly three cold run directories are required")
    rows = []
    systemic_problems: list[str] = []
    stability_problems: list[str] = []
    for directory in run_dirs:
        factory = _load(directory, "candidate_factory")
        run = _load(directory, "run")
        materialization = _load(directory, "materialization")
        audit = _load(directory, "manual_audit")
        cache = (run.get("diagnostics") or {}).get("cache") or {}
        if cache.get("enabled") or cache.get("hits"):
            systemic_problems.append(f"{directory}: cache was not disabled")
        unsupported = int(
            (materialization.get("diagnostics") or {}).get("unsupported_materialized") or 0
        )
        if unsupported:
            systemic_problems.append(f"{directory}: unsupported materialized={unsupported}")
        if audit.get("status") != "COMPLETE":
            systemic_problems.append(f"{directory}: manual audit is incomplete")
        rows.append({
            "directory": str(directory),
            "fast_input_signature": run.get("fast_input_signature"),
            "candidate_set_signature": factory.get("candidate_set_signature"),
            "prompt_signature": content_signature([
                {
                    "batch_id": value.get("batch_id"),
                    "pass_identity": value.get("pass_identity"),
                    "prompt_signature": value.get("prompt_signature"),
                    "schema_signature": value.get("schema_signature"),
                    "task_ids": value.get("task_ids"),
                }
                for value in run.get("prompt_manifest") or ()
            ]),
            "hro_count": int(
                ((materialization.get("human_review_plan") or {}).get("summary") or {}).get(
                    "mandatory_human_interactions"
                ) or 0
            ),
            "unsupported": unsupported,
            "model_calls": int((run.get("diagnostics") or {}).get("model_calls") or 0),
            "duration_ms": int((run.get("diagnostics") or {}).get("duration_ms") or 0),
            "products": _products(materialization),
            "product_decisions": _selection_products(materialization),
        })
    if len({row["fast_input_signature"] for row in rows}) != 1:
        systemic_problems.append("frozen FAST signature differs")
    if len({row["candidate_set_signature"] for row in rows}) != 1:
        systemic_problems.append("candidate factory output differs")
    if len({row["prompt_signature"] for row in rows}) != 1:
        systemic_problems.append("serialized selector prompts differ")
    if len({row["hro_count"] for row in rows}) != 1:
        stability_problems.append("HRO count differs")

    overlap = {}
    for (left_index, left), (right_index, right) in itertools.combinations(enumerate(rows, 1), 2):
        overlap[f"run{left_index}_run{right_index}"] = _jaccard(
            left["products"], right["products"]
        )
    stable_core = set.intersection(*(row["products"] for row in rows))
    lookup = {
        content_signature({
            "task_id": value.get("task_id"),
            "selected_candidate_id": value.get("selected_candidate_id"),
            "product_fingerprint": value.get("product_fingerprint"),
        }): value
        for value in rows[0]["product_decisions"]
    }
    minimum_overlap = min((value["ratio"] for value in overlap.values()), default=1.0)
    if systemic_problems:
        verdict = "C"
    elif stability_problems or minimum_overlap < 0.90:
        verdict = "B"
    else:
        verdict = "A"
    return {
        "kind": "stage_comparison_ai_v3_reproducibility_gate",
        "schema_version": "stage-comparison-ai-v3-reproducibility.v1",
        "verdict": verdict,
        "recommend_rollout": verdict == "A",
        "problems": systemic_problems + stability_problems,
        "systemic_problems": systemic_problems,
        "stability_problems": stability_problems,
        "runs": [
            {key: value for key, value in row.items() if key not in {"products"}}
            for row in rows
        ],
        "candidate_factory_identical": len({row["candidate_set_signature"] for row in rows}) == 1,
        "hro_count_stable": len({row["hro_count"] for row in rows}) == 1,
        "pairwise_product_overlap": overlap,
        "minimum_pairwise_product_overlap": minimum_overlap,
        "all_run_stable_core": [lookup[value] for value in sorted(stable_core) if value in lookup],
        "unsupported_all_runs": [row["unsupported"] for row in rows],
        "total_model_calls": sum(row["model_calls"] for row in rows),
        "total_runtime_ms": sum(row["duration_ms"] for row in rows),
    }


def compare_modes(single: Mapping[str, Any], unanimity: Mapping[str, Any]) -> dict[str, Any]:
    def row(value: Mapping[str, Any]) -> dict[str, Any]:
        run = value.get("run") or {}
        materialization = value.get("materialization") or {}
        diagnostics = run.get("diagnostics") or {}
        return {
            "stable_selections": sum(
                item.get("status") == "VERIFIED_SELECTION"
                for item in run.get("stable_selections") or ()
            ),
            "verified": int(diagnostics.get("verified_selections") or 0),
            "materialized": len(materialization.get("product_fingerprints") or ()),
            "hro_reduction": int(
                (materialization.get("diagnostics") or {}).get("human_interactions_saved") or 0
            ),
            "unsupported": int(
                (materialization.get("diagnostics") or {}).get("unsupported_materialized") or 0
            ),
            "latency_ms": int(diagnostics.get("duration_ms") or 0),
            "model_calls": int(diagnostics.get("model_calls") or 0),
            "selector_disagreements": int(diagnostics.get("selector_disagreements") or 0),
        }
    return {
        "kind": "stage_comparison_ai_v3_single_vs_unanimity",
        "single": row(single),
        "unanimity": row(unanimity),
        "selected_mode": "unanimity",
        "selection_rule": "fail-closed unanimity for every AI-selected product decision",
    }


__all__ = ["compare_modes", "evaluate"]
