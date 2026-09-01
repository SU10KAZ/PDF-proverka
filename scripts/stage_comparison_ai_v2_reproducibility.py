#!/usr/bin/env python3
"""Evaluate three cache-disabled AI Analyst v2 runs over one frozen input."""
from __future__ import annotations

import argparse
import json
import sys
from itertools import combinations
from pathlib import Path
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.stage_comparison.ai import cache as cache_module  # noqa: E402
from backend.app.services.stage_comparison.production_artifacts import (  # noqa: E402
    content_signature,
)


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path}: root is not an object")
    return value


def _jaccard(left: set[str], right: set[str]) -> dict[str, Any]:
    union = left | right
    intersection = left & right
    return {
        "left_count": len(left),
        "right_count": len(right),
        "intersection": len(intersection),
        "union": len(union),
        "ratio": round(len(intersection) / len(union), 6) if union else 1.0,
        "only_left": sorted(left - right),
        "only_right": sorted(right - left),
    }


def _prompt_capture_problems(
    run_dir: Path,
    run: Mapping[str, Any],
    effort: str,
) -> list[str]:
    problems: list[str] = []
    manifest = list(run.get("prompt_manifest") or ())
    if len(manifest) != 3:
        problems.append(f"expected 3 prompt records, found {len(manifest)}")
    prompt_schema = str((manifest[0] if manifest else {}).get(
        "schema_version"
    ) or "")
    expected_prompt_signature = content_signature({
        "schema": prompt_schema,
        "sessions": manifest,
    })
    if run.get("prompt_signature") != expected_prompt_signature:
        problems.append("prompt signature does not cover the session manifest")
    routed_ids: list[str] = []
    for record in manifest:
        if not isinstance(record, Mapping):
            problems.append("prompt manifest contains a non-object")
            continue
        sequence = int(record.get("sequence") or 0)
        role = str(record.get("role") or "")
        payload_path = (
            run_dir / effort / "prompt_payloads"
            / str(record.get("payload_file") or "")
        )
        try:
            payload = _load(payload_path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            problems.append(f"session {sequence}/{role}: missing payload: {exc}")
            continue
        prompt = str(payload.get("prompt") or "")
        system_prompt = str(payload.get("system_prompt") or "")
        schema = payload.get("response_schema") or {}
        checks = {
            "payload_signature": content_signature(payload),
            "prompt_digest": cache_module.digest_prompt(prompt, system_prompt),
            "schema_digest": cache_module.digest_schema(schema),
            "prompt_bytes": len(prompt.encode("utf-8"))
            + len(system_prompt.encode("utf-8")),
        }
        for key, actual in checks.items():
            if record.get(key) != actual:
                problems.append(
                    f"session {sequence}/{role}: {key} does not match capture"
                )
        expected_key = cache_module.cache_key(
            evidence_digest=str(record.get("evidence_digest") or ""),
            model=str(record.get("model") or ""),
            reasoning_level=str(record.get("reasoning_effort") or ""),
            prompt_version=str(record.get("prompt_version") or ""),
            schema_version=str(record.get("contract_schema_version") or ""),
            role=role,
            prompt_digest=str(record.get("prompt_digest") or ""),
            schema_digest=str(record.get("schema_digest") or ""),
        )
        if record.get("cache_key") != expected_key:
            problems.append(f"session {sequence}/{role}: incomplete cache key")
        routed_ids.extend(str(value) for value in record.get("task_ids") or ())
    routed = int((run.get("diagnostics") or {}).get("routed") or 0)
    if len(routed_ids) != routed or len(set(routed_ids)) != routed:
        problems.append(
            f"prompt routing is not one-to-one: {len(routed_ids)} rows, "
            f"{len(set(routed_ids))} unique, diagnostics routed={routed}"
        )
    expected_run_signature = content_signature({
        "schema": run.get("schema_version"),
        "context": run.get("context_signature"),
        "model_context": run.get("model_context_signature"),
        "inventory": run.get("inventory_signature"),
        "fast_input": run.get("fast_input_signature"),
        "prompt": run.get("prompt_signature"),
        "effort": run.get("reasoning_effort"),
        "model": run.get("model"),
    })
    if run.get("input_signature") != expected_run_signature:
        problems.append("run input signature omits a reproducibility axis")
    return problems


def _metrics(
    run: Mapping[str, Any],
    materialization: Mapping[str, Any],
) -> dict[str, Any]:
    diagnostics = run.get("diagnostics") or {}
    material = materialization.get("diagnostics") or {}
    plan = (materialization.get("human_review_plan") or {}).get("summary") or {}
    report = (
        (materialization.get("preliminary_report") or {}).get("summary") or {}
    ).get("counts") or {}
    before = int(material.get("stage7_before") or 0)
    after = int(material.get("stage7_after") or 0)
    unsupported = max(
        int(diagnostics.get("unsupported_published") or 0),
        int(material.get("unsupported_materialized") or 0),
    )
    return {
        "ai_verifier_pass": int(diagnostics.get("ai_resolved_verified") or 0),
        "ai_verifier_reject": int(diagnostics.get("verifier_rejected") or 0),
        "materialized_findings": int(material.get("materialized_findings") or 0),
        "stage7_before": before,
        "stage7_after": after,
        "stage7_removed": max(0, before - after),
        "ai_verified_in_report": int(report.get("ai_verified") or 0),
        "human_interactions": int(plan.get("mandatory_human_interactions") or 0),
        "unsupported": unsupported,
        "duration_ms": int(diagnostics.get("duration_ms") or 0),
        "model_calls": int(diagnostics.get("model_calls") or 0),
        "sessions": int(diagnostics.get("sessions") or 0),
        "cache": dict(diagnostics.get("cache") or {}),
    }


def evaluate(run_dirs: list[Path], *, effort: str = "low") -> dict[str, Any]:
    if len(run_dirs) != 3:
        raise ValueError("reproducibility gate requires exactly three runs")
    records = []
    problems: list[str] = []
    for index, run_dir in enumerate(run_dirs, 1):
        manifest = _load(run_dir / "fast_input_manifest.json")
        run = _load(run_dir / effort / "run.json")
        materialization = _load(run_dir / effort / "materialization.json")
        label = f"Run {index}"
        cache = (run.get("diagnostics") or {}).get("cache") or {}
        if cache.get("enabled") is not False:
            problems.append(f"{label}: cache was not disabled")
        if any(int(cache.get(key) or 0) for key in ("hits", "writes")):
            problems.append(f"{label}: cache hit/write detected")
        if int((run.get("diagnostics") or {}).get("model_calls") or 0) != 3:
            problems.append(f"{label}: not a real three-call cold run")
        if manifest.get("input_signature") != run.get("fast_input_signature"):
            problems.append(f"{label}: FAST manifest/run signature mismatch")
        problems.extend(
            f"{label}: {value}"
            for value in _prompt_capture_problems(run_dir, run, effort)
        )
        resolutions = {
            str(value.get("task_id") or "")
            for value in run.get("resolutions") or ()
            if isinstance(value, Mapping)
            and value.get("status") == "AI_RESOLVED_VERIFIED"
        }
        supported_materialized = {
            str(value.get("task_id") or "")
            for value in materialization.get("outcomes") or ()
            if isinstance(value, Mapping)
            and value.get("outcome") in {"MATERIALIZED_FINDING", "NO_CHANGE"}
        }
        materialized_products = {
            content_signature({
                "task_id": value.get("task_id"),
                "outcome": value.get("outcome"),
                "finding_ids": value.get("materialized_finding_ids") or [],
                "removed_targets": value.get("removed_review_target_ids") or [],
            })
            for value in materialization.get("outcomes") or ()
            if isinstance(value, Mapping)
            and value.get("outcome") in {"MATERIALIZED_FINDING", "NO_CHANGE"}
        }
        records.append({
            "label": label,
            "dir": str(run_dir),
            "manifest": manifest,
            "run": run,
            "materialization": materialization,
            "metrics": _metrics(run, materialization),
            "resolutions": resolutions,
            "supported_materialized": supported_materialized,
            "materialized_products": materialized_products,
        })

    equality_fields = (
        "fast_input_signature", "context_signature", "model_context_signature",
        "inventory_signature", "prompt_signature", "model", "reasoning_effort",
        "schema_version", "settings",
    )
    for field in equality_fields:
        values = [content_signature(value["run"].get(field)) for value in records]
        if len(set(values)) != 1:
            problems.append(f"runs disagree on {field}")
    manifest_signatures = {
        str(value["manifest"].get("input_signature") or "") for value in records
    }
    if len(manifest_signatures) != 1:
        problems.append("runs disagree on frozen FAST input")

    resolution_overlap = {}
    supported_overlap = {}
    product_overlap = {}
    for left, right in combinations(records, 2):
        pair = f"{left['label']}↔{right['label']}"
        resolution_overlap[pair] = _jaccard(
            left["resolutions"], right["resolutions"]
        )
        supported_overlap[pair] = _jaccard(
            left["supported_materialized"], right["supported_materialized"]
        )
        product_overlap[pair] = _jaccard(
            left["materialized_products"], right["materialized_products"]
        )

    unsupported = [value["metrics"]["unsupported"] for value in records]
    interactions = [value["metrics"]["human_interactions"] for value in records]
    material_stable = all(
        value["ratio"] == 1.0 for value in product_overlap.values()
    )
    if problems or any(unsupported):
        verdict = "C"
        recommendation = False
    elif len(set(interactions)) == 1 and material_stable:
        verdict = "A"
        recommendation = True
    else:
        verdict = "B"
        recommendation = False
    return {
        "kind": "stage_comparison_ai_v2_reproducibility_gate",
        "schema_version": "stage-comparison-ai-v2-reproducibility-gate.v1",
        "effort": effort,
        "frozen_fast_input_signature": next(iter(manifest_signatures), ""),
        "runs": {
            value["label"]: value["metrics"] for value in records
        },
        "resolution_overlap": resolution_overlap,
        "supported_materialized_overlap": supported_overlap,
        "materialized_product_overlap": product_overlap,
        "problems": problems,
        "verdict": verdict,
        "recommend_rollout": recommendation,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dirs", nargs=3, type=Path)
    parser.add_argument("--effort", default="low")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = evaluate(
        [value.resolve() for value in args.run_dirs], effort=args.effort
    )
    rendered = json.dumps(result, ensure_ascii=False, indent=1) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if result["verdict"] != "C" else 2


if __name__ == "__main__":
    raise SystemExit(main())
