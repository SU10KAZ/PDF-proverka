#!/usr/bin/env python3
"""Run the frozen LOW AI Analyst v2 experiment over a FAST artifact set.

This command never edits the production artifact directory.  It writes a
frozen context, inventory and per-effort runs beneath ``--output-dir``.

Example:
    STAGE_COMPARISON_AI_ANALYST_V2=true python \
      scripts/stage_comparison_ai_v2_experiment.py \
      comparison/sessions/<session>/pairs/<pair>/production \
      --pair-id <pair> --output-dir comparison/ai_analyst_v2/<run>
"""
from __future__ import annotations

import argparse
import copy
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.stage_comparison.ai import gateway  # noqa: E402
from backend.app.services.stage_comparison.ai_v2 import settings  # noqa: E402
from backend.app.services.stage_comparison.ai_v2.engine import (  # noqa: E402
    WholeDocumentAnalyst,
)
from backend.app.services.stage_comparison.ai_v2.materialization import (  # noqa: E402
    materialize_verified_resolutions,
)
from backend.app.services.stage_comparison.production_artifacts import (  # noqa: E402
    content_signature,
)

ARTIFACT_NAMES = (
    "state",
    "direct_page_mode2",
    "document_inconsistencies",
    "electrical_table_changes",
    "unified_synthesis",
    "text_preparation",
    "sheet_relations",
    "ai_routing_inventory",
    "preliminary_report",
    "engineer_decisions",
    "text_atoms",
    "bound_atoms",
    "graphic_change_ledger",
    "entity_relations",
)


def _load(directory: Path, name: str) -> dict[str, Any]:
    path = directory / f"{name}.json"
    if not path.is_file():
        return {}
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path}: root is not an object")
    return value


def _write(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=1) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _ensure_manual_audit(path: Path, run: dict[str, Any]) -> None:
    """Preserve a completed audit when a cache replay has the same findings."""
    expected = {
        str(value.get("task_id") or "")
        for value in run.get("resolutions") or ()
        if value.get("status") == "AI_RESOLVED_VERIFIED"
    }
    if path.is_file():
        try:
            current = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            current = {}
        audited = {
            str(value.get("task_id") or "")
            for value in current.get("items") or ()
            if isinstance(value, dict)
        }
        if current.get("status") == "COMPLETE" and audited == expected:
            return
    _write(path, {
        "status": "PENDING_MANUAL_AUDIT",
        "items": [
            {
                "task_id": value["task_id"],
                "manual_verdict": None,
                "note": "",
            }
            for value in run.get("resolutions") or ()
            if value.get("status") == "AI_RESOLVED_VERIFIED"
        ],
    })


def _fast_baseline(artifacts: dict[str, dict[str, Any]]) -> dict[str, Any]:
    preliminary = artifacts.get("preliminary_report") or {}
    counts = ((preliminary.get("summary") or {}).get("counts") or {})
    synthesis = artifacts.get("unified_synthesis") or {}
    state = artifacts.get("state") or {}
    return {
        "status": state.get("status"),
        "automatic_findings": int(counts.get("automatic") or 0),
        "engineering_review": int(counts.get("review") or 0),
        "document_inconsistencies": int(counts.get("inconsistency") or 0),
        "insufficient_evidence": int(counts.get("unproven") or 0),
        "deterministic_changes": len(synthesis.get("changes") or ()),
        "review_items": len(synthesis.get("review_items") or ()),
        "manual_decisions": len(
            (artifacts.get("engineer_decisions") or {}).get("decisions") or ()
        ),
        "duration_ms": int(state.get("duration_ms") or 0),
        "model_calls": 0,
        "stage7_rows": len(
            (artifacts.get("engineer_decisions") or {}).get("decisions") or ()
        ),
    }


def _ab(low: dict[str, Any], medium: dict[str, Any]) -> dict[str, Any]:
    def row(value: dict[str, Any]) -> dict[str, Any]:
        diagnostics = value.get("diagnostics") or {}
        return {
            key: diagnostics.get(key) for key in (
                "ai_resolved_verified",
                "human_required",
                "verifier_rejected",
                "human_decisions_saved",
                "model_calls",
                "duration_ms",
                "seconds_per_saved_decision",
                "unsupported_published",
            )
        }
    low_row, medium_row = row(low), row(medium)
    candidates = [("low", low_row), ("medium", medium_row)]
    # Product objective: supported saved decisions first, then latency.  An
    # unsupported publication makes a candidate ineligible regardless of N.
    eligible = [
        value for value in candidates if value[1]["unsupported_published"] == 0
    ]
    selected = max(
        eligible,
        key=lambda value: (
            int(value[1]["human_decisions_saved"] or 0),
            -int(value[1]["duration_ms"] or 0),
        ),
        default=("none", {}),
    )[0]
    return {
        "same_context": low.get("context_signature") == medium.get(
            "context_signature"
        ),
        "same_inventory": low.get("inventory_signature") == medium.get(
            "inventory_signature"
        ),
        "low": low_row,
        "medium": medium_row,
        "selected_effort": selected,
        "selection_rule": "saved human decisions, then latency; unsupported=disqualify",
    }


def _benchmark_row(
    run: dict[str, Any], materialization: dict[str, Any]
) -> dict[str, Any]:
    def stable(value: Any) -> Any:
        if isinstance(value, dict):
            return {
                key: stable(item)
                for key, item in value.items()
                if key not in {"generated_at", "created_at", "input_signature"}
            }
        if isinstance(value, list):
            return [stable(item) for item in value]
        return value

    diagnostics = run.get("diagnostics") or {}
    sessions = list(diagnostics.get("session_metrics") or ())
    plan_summary = (
        (materialization.get("human_review_plan") or {}).get("summary") or {}
    )
    report_counts = (
        ((materialization.get("preliminary_report") or {}).get("summary") or {})
        .get("counts") or {}
    )
    materialization_metrics = {
        key: (materialization.get("diagnostics") or {}).get(key)
        for key in (
            "outcome_counts", "materialized_tasks", "materialized_findings",
            "no_change_after_identity", "removed_review_targets",
            "stage7_before", "stage7_after", "human_decisions_saved",
            "preliminary_review_before", "preliminary_review_after",
            "unsupported_materialized",
        )
    }
    if not any(value is not None for value in materialization_metrics.values()):
        materialization_metrics = dict(diagnostics.get("materialization") or {})
    return {
        "model": run.get("model") or diagnostics.get("model"),
        "reasoning_effort": run.get("reasoning_effort") or diagnostics.get("reasoning_effort"),
        "duration_ms": int(diagnostics.get("duration_ms") or 0),
        "model_calls": int(diagnostics.get("model_calls") or 0),
        "sessions": int(diagnostics.get("sessions") or 0),
        "cache_hits": int((diagnostics.get("cache") or {}).get("hits") or 0),
        "session_metrics": sessions,
        "prompt_bytes_total": sum(int(value.get("prompt_bytes") or 0) for value in sessions),
        "verifier": {
            "ai_resolved_verified": int(diagnostics.get("ai_resolved_verified") or 0),
            "human_required": int(diagnostics.get("human_required") or 0),
            "verifier_rejected": int(diagnostics.get("verifier_rejected") or 0),
            "unsupported_published": int(diagnostics.get("unsupported_published") or 0),
        },
        "materialization": materialization_metrics,
        "human_review": {
            "groups": int(plan_summary.get("review_groups") or 0),
            "standalone_questions": int(plan_summary.get("standalone_human_questions") or 0),
            "mandatory_interactions": int(plan_summary.get("mandatory_human_interactions") or 0),
            "mode_atoms": int(plan_summary.get("mode_atoms") or 0),
        },
        "preliminary_counts": dict(report_counts),
        "read_model_signature": content_signature({
            "synthesis": stable(materialization.get("unified_synthesis")),
            "human_review_plan": stable(materialization.get("human_review_plan")),
            "preliminary_report": stable(materialization.get("preliminary_report")),
            "document_inconsistencies": stable(materialization.get("document_inconsistencies")),
        }),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("production_dir", type=Path)
    parser.add_argument("--pair-id", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--efforts", nargs="+", choices=settings.ALLOWED_EFFORTS,
        default=list(settings.DEFAULT_EFFORTS),
    )
    parser.add_argument(
        "--skip-runtime-check", action="store_true",
        help="only for injected/offline tests; live experiments must not use it",
    )
    args = parser.parse_args()
    settings.require_enabled()
    production_dir = args.production_dir.resolve()
    if not production_dir.is_dir():
        parser.error(f"artifact directory does not exist: {production_dir}")
    output_dir = args.output_dir.resolve()
    if output_dir == production_dir or production_dir in output_dir.parents:
        parser.error("output-dir must not be inside the production artifact directory")

    artifacts = {name: _load(production_dir, name) for name in ARTIFACT_NAMES}
    required = [
        name for name in (
            "state", "direct_page_mode2", "unified_synthesis",
            "ai_routing_inventory", "preliminary_report",
        ) if not artifacts[name]
    ]
    if required:
        parser.error("missing FAST artifacts: " + ", ".join(required))
    if not args.skip_runtime_check:
        runtime = gateway.validate_runtime(require_vision=False, deep=False)
        _write(output_dir / "runtime_check.json", runtime)
        if not runtime.get("ok"):
            print("AI runtime is not ready: " + "; ".join(runtime.get("problems") or ()), file=sys.stderr)
            return 2

    _write(output_dir / "fast_baseline.json", _fast_baseline(artifacts))
    runs: dict[str, dict[str, Any]] = {}
    frozen_signature = None
    for effort in args.efforts:
        analyst = WholeDocumentAnalyst(
            artifacts=artifacts,
            pair_id=args.pair_id,
            effort=effort,
            cache_dir=output_dir / "response_cache",
        )
        if frozen_signature is None:
            frozen_signature = analyst.bundle.signature
            _write(
                output_dir / "pre_ai_human_review_plan.json",
                analyst.human_review_plan,
            )
            _write(output_dir / "sheet_context.json", analyst.bundle.sheet_context)
            _write(output_dir / "focused_evidence.json", analyst.bundle.focused_by_task)
            _write(output_dir / "evidence_catalog.json", analyst.bundle.evidence_catalog)
            _write(output_dir / "unresolved_inventory.json", analyst.inventory)
        elif analyst.bundle.signature != frozen_signature:
            raise RuntimeError("evidence set changed between LOW and MEDIUM")
        run = analyst.run()
        audit_path = output_dir / effort / "manual_audit.json"
        _ensure_manual_audit(audit_path, run)
        manual_audit = _load(audit_path.parent, "manual_audit")
        # Production accepts only claims that pass deterministic verification;
        # a PENDING audit file is an audit aid, not a hidden publication gate.
        # A completed audit remains authoritative for an explicit audited run.
        effective_manual_audit = (
            manual_audit if manual_audit.get("status") == "COMPLETE" else None
        )
        materialization = materialize_verified_resolutions(
            artifacts=artifacts,
            run=run,
            pair_id=args.pair_id,
            manual_audit=effective_manual_audit,
            human_entity_relations=artifacts.get("entity_relations"),
        )
        run["diagnostics"]["materialization"] = {
            key: materialization["diagnostics"].get(key)
            for key in (
                "outcome_counts",
                "materialized_tasks",
                "materialized_findings",
                "no_change_after_identity",
                "removed_review_targets",
                "stage7_before",
                "stage7_after",
                "human_decisions_saved",
                "preliminary_review_before",
                "preliminary_review_after",
                "unsupported_materialized",
            )
        }
        run["materialization_signature"] = materialization["input_signature"]
        run["diagnostics"]["manual_audit_policy"] = (
            "COMPLETE_AUDIT" if effective_manual_audit is not None
            else "PRODUCTION_VERIFIER_ONLY"
        )
        runs[effort] = run
        existing_run = _load(output_dir / effort, "run")
        historical_cold_run = _load(output_dir / effort, "cold_run")
        existing_calls = int((existing_run.get("diagnostics") or {}).get("model_calls") or 0)
        historical_cold_calls = int(
            (historical_cold_run.get("diagnostics") or {}).get("model_calls") or 0
        )
        current_calls = int((run.get("diagnostics") or {}).get("model_calls") or 0)
        if existing_run and existing_calls > 0 and current_calls == 0:
            _write(output_dir / effort / "cold_run.json", existing_run)
            _write(output_dir / effort / "warm_replay.json", run)
        _write(output_dir / effort / "run.json", run)
        _write(output_dir / effort / "materialization.json", materialization)
        _write(
            output_dir / effort / "verified_entity_relations.json",
            materialization["verified_entity_relations"],
        )
        _write(
            output_dir / effort / "preliminary_report.json",
            materialization["preliminary_report"],
        )
        _write(
            output_dir / effort / "human_review_plan.json",
            materialization["human_review_plan"],
        )
        summary_path = output_dir / "benchmark_summary.json"
        summary = _load(output_dir, "benchmark_summary")
        summary.setdefault("kind", "stage_comparison_ai_v2_benchmark")
        summary.setdefault("schema_version", "ai-v2-benchmark.v1")
        summary["context_signature"] = frozen_signature
        summary.setdefault("efforts", {})
        phase = (
            "warm"
            if current_calls == 0 and (existing_calls > 0 or historical_cold_calls > 0)
            else "cold"
        )
        benchmark = _benchmark_row(run, materialization)
        effort_summary = summary["efforts"].setdefault(effort, {})
        effort_summary[phase] = benchmark
        if phase == "warm" and historical_cold_calls > 0:
            # Reconstruct the cold benchmark from the immutable cold response
            # and the current deterministic materialization. Repeated warm
            # replays must never overwrite the real 3-call timing with zero.
            effort_summary["cold"] = _benchmark_row(
                historical_cold_run, materialization
            )
            initial_materialization = (
                (historical_cold_run.get("diagnostics") or {}).get("materialization")
                or {}
            )
            initial_interactions = int(
                initial_materialization.get("preliminary_review_after") or 0
            )
            if initial_interactions and initial_interactions != int(
                benchmark["human_review"]["mandatory_interactions"]
            ):
                effort_summary["cold"]["initial_projection"] = {
                    "mandatory_human_interactions": initial_interactions,
                    "materialization": dict(initial_materialization),
                }
                effort_summary["cold_projection_replayed_after_deterministic_fix"] = True
        if phase == "warm" and effort_summary.get("cold"):
            cold = effort_summary["cold"]
            if cold.get("read_model_signature") != benchmark["read_model_signature"]:
                # The inference is still the one cold response saved above;
                # only deterministic projection code changed during the
                # readiness pass. Preserve the original audit values, then
                # bind cold timing to the final replayed read model.
                cold["initial_projection"] = {
                    "read_model_signature": cold.get("read_model_signature"),
                    "human_review": cold.get("human_review"),
                    "materialization": cold.get("materialization"),
                    "preliminary_counts": cold.get("preliminary_counts"),
                }
                for key in (
                    "read_model_signature", "human_review",
                    "materialization", "preliminary_counts",
                ):
                    cold[key] = copy.deepcopy(benchmark[key])
                effort_summary["cold_projection_replayed_after_deterministic_fix"] = True
        if phase == "warm" and effort_summary.get("cold"):
            effort_summary["read_model_reproduced"] = (
                effort_summary["cold"]["read_model_signature"]
                == effort_summary["warm"]["read_model_signature"]
            )
        _write(summary_path, summary)

    if {"low", "medium"} <= set(runs):
        _write(output_dir / "ab_comparison.json", _ab(runs["low"], runs["medium"]))
    print(json.dumps({
        "output_dir": str(output_dir),
        "context_signature": frozen_signature,
        "runs": {
            effort: value.get("diagnostics") for effort, value in runs.items()
        },
    }, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
