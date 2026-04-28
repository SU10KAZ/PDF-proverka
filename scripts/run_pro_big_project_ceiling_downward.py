"""
Very cheap downward-search experiment for Gemini 3.1 Pro on stage 02 via OpenRouter.

Goal:
  After the big-project `b6 + r800` quality failure, find the safe batch ceiling
  downward instead of upward.

Constraints:
  - OpenRouter only
  - reuse the same big-project audit set
  - reuse the same Pro single-block reference
  - fixed config only: Pro / high / healing ON / parallelism=2 / r800
  - check only b4, then b2 only if b4 fails
  - no full-doc run
  - no b6+, no r1000+, no Claude, no Flash

Artifacts:
  <project_dir>/_experiments/pro_big_project_ceiling_downward/<timestamp>/
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

from run_gemini_openrouter_stage02_experiment import (  # noqa: E402
    apply_byte_cap_split,
    pack_blocks,
)
from run_pro_b6_r800_big_project_validation import (  # noqa: E402
    BudgetTracker,
    enrich_quality,
    hard_gate_passed,
)
from run_pro_variantA_small_budget import (  # noqa: E402
    aggregate_results,
    analyses_map_from_results,
    build_budget_timeline_md,
    build_side_by_side,
    run_call_set,
)
from run_stage02_recall_hybrid import resolve_project  # noqa: E402
from webapp.services.openrouter_block_batch import load_openrouter_block_batch_schema  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pro_big_project_ceiling_downward")


PDF_NAME = "13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf"
MODEL_ID = "google/gemini-3.1-pro-preview"
PROVIDER = "OpenRouter"
EXP_NAME = "pro_big_project_ceiling_downward"
REUSE_EXP_NAME = "pro_b6_r800_big_project_validation"
DEFAULT_TIMEOUT_SEC = 600
MAX_OUTPUT_TOKENS = 32768
DEFAULT_BUDGET_CAP_USD = 1.00
AUDIT_SET_SIZE = 12
PROFILE_ORDER = ("b4", "b2")
PROFILE_CONFIGS = {
    "b2": {
        "heavy": {"target": 1, "max": 1},
        "normal": {"target": 2, "max": 2},
        "light": {"target": 2, "max": 2},
    },
    "b4": {
        "heavy": {"target": 2, "max": 2},
        "normal": {"target": 4, "max": 4},
        "light": {"target": 4, "max": 4},
    },
}
PROFILE_ESTIMATE_MULTIPLIER = {
    "b4": 1.25,
    "b2": 1.60,
}
PROFILE_ESTIMATE_FALLBACK = {
    "b4": 0.32,
    "b2": 0.40,
}


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _save_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _save_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _normalize_path(path_value: str, base: Path) -> Path:
    path = Path(path_value)
    return path if path.is_absolute() else (base / path).resolve()


def _valid_reference_run_summary(summary: dict) -> bool:
    return (
        summary.get("model_id") == MODEL_ID
        and summary.get("mode") == "single_block"
        and summary.get("reasoning_effort") == "high"
        and summary.get("parallelism") == 2
        and summary.get("response_healing_initial") is True
        and summary.get("resolution") == "r800"
        and summary.get("coverage_pct") == 100.0
        and not summary.get("missing_block_ids")
    )


def find_latest_reuse_dir(project_dir: Path) -> Path | None:
    root = project_dir / "_experiments" / REUSE_EXP_NAME
    if not root.exists():
        return None
    candidates = sorted(
        [
            path
            for path in root.iterdir()
            if path.is_dir()
            and (path / "manifest.json").exists()
            and (path / "audit_set_block_ids.json").exists()
            and (path / "audit_set_manifest.md").exists()
            and (path / "reference_analyses.json").exists()
            and (path / "reference_run_summary.json").exists()
        ],
        reverse=True,
    )
    return candidates[0] if candidates else None


def load_reused_audit_context(project_dir: Path) -> tuple[Path, dict, list[str], str, dict[str, dict], dict, dict]:
    reuse_dir = find_latest_reuse_dir(project_dir)
    if reuse_dir is None:
        raise RuntimeError(
            "No reusable big-project validation artifacts found under "
            f"`_experiments/{REUSE_EXP_NAME}`."
        )

    manifest = _load_json(reuse_dir / "manifest.json")
    audit_ids = _load_json(reuse_dir / "audit_set_block_ids.json")
    audit_manifest_md = (reuse_dir / "audit_set_manifest.md").read_text(encoding="utf-8")
    reference_map = _load_json(reuse_dir / "reference_analyses.json")
    reference_summary = _load_json(reuse_dir / "reference_run_summary.json")
    smoke_payload = _load_json(reuse_dir / "audit_smoke_summary.json")

    errors: list[str] = []
    if manifest.get("pdf") != PDF_NAME:
        errors.append(f"pdf mismatch: {manifest.get('pdf')!r}")
    manifest_project_dir = _normalize_path(str(manifest.get("project_dir") or ""), _ROOT)
    if manifest_project_dir != project_dir.resolve():
        errors.append(f"project_dir mismatch: {manifest_project_dir!s}")
    if manifest.get("provider") != PROVIDER:
        errors.append(f"provider mismatch: {manifest.get('provider')!r}")
    if manifest.get("model_id") != MODEL_ID:
        errors.append(f"model mismatch: {manifest.get('model_id')!r}")
    if manifest.get("resolution") != "r800":
        errors.append(f"resolution mismatch: {manifest.get('resolution')!r}")
    if manifest.get("parallelism") != 2:
        errors.append(f"parallelism mismatch: {manifest.get('parallelism')!r}")
    if manifest.get("response_healing_initial") is not True:
        errors.append("response_healing_initial is not true")
    if manifest.get("full_run_performed") not in (False, None):
        errors.append("reuse source unexpectedly performed full run")

    if not isinstance(audit_ids, list) or len(audit_ids) != AUDIT_SET_SIZE:
        errors.append(f"unexpected audit_set size: {len(audit_ids) if isinstance(audit_ids, list) else 'invalid'}")
    if not _valid_reference_run_summary(reference_summary):
        errors.append("reference_run_summary is not a valid high/p2/r800 single-block reference")
    if reference_summary.get("requested_block_ids") != audit_ids:
        errors.append("reference requested_block_ids do not match audit_set_block_ids")
    if not isinstance(reference_map, dict):
        errors.append("reference_analyses.json is not a dict")
    else:
        missing_reference = [block_id for block_id in audit_ids if block_id not in reference_map]
        if missing_reference:
            errors.append("reference map missing ids: " + ", ".join(missing_reference))

    smoke_summary_row = smoke_payload.get("summary_row") or {}
    if not smoke_summary_row:
        errors.append("reuse source audit_smoke_summary.json is missing summary_row")

    if errors:
        raise RuntimeError(
            "Latest big-project validation run is not a valid reuse source:\n- "
            + "\n- ".join(errors)
        )

    return reuse_dir, manifest, audit_ids, audit_manifest_md, reference_map, reference_summary, smoke_payload


def build_audit_set_reuse_report(
    *,
    reuse_dir: Path,
    reuse_manifest: dict,
    audit_ids: list[str],
    audit_manifest_md: str,
    reference_summary: dict,
    prior_smoke_payload: dict,
) -> str:
    smoke_row = prior_smoke_payload.get("summary_row") or {}
    lines = [
        "# Audit Set Reuse Report\n",
        f"- Reused experiment dir: `{reuse_dir}`",
        f"- Reused timestamp: `{reuse_manifest.get('timestamp', '')}`",
        f"- PDF: `{reuse_manifest.get('pdf', '')}`",
        f"- Project dir: `{reuse_manifest.get('project_dir', '')}`",
        f"- Block source: `{reuse_manifest.get('block_source', '')}`",
        f"- Audit set size: {len(audit_ids)}",
        f"- Exact block IDs: {', '.join(f'`{block_id}`' for block_id in audit_ids)}",
        "- New audit set created: no",
        "- Reference rerun: no",
        "",
        "## Reused Single-Block Reference",
        f"- Coverage: {reference_summary.get('coverage_pct', 0.0):.2f}%",
        f"- Missing after reference run: {len(reference_summary.get('missing_block_ids') or [])}",
        f"- Reasoning / parallelism / resolution: `{reference_summary.get('reasoning_effort')}` / "
        f"{reference_summary.get('parallelism')} / `{reference_summary.get('resolution')}`",
        "",
        "## Prior b6 Smoke Used Only As Cost Prior",
        f"- Prior b6 smoke total cost: ${float(smoke_row.get('total_cost_usd', 0.0) or 0.0):.4f}",
        f"- Prior b6 smoke survived: {smoke_row.get('survived', False)}",
        "",
        "## Source Audit Manifest",
        audit_manifest_md.strip(),
        "",
    ]
    return "\n".join(lines)


def profile_batches_for_audit(audit_blocks: list[dict], profile_id: str) -> list[list[dict]]:
    if profile_id not in PROFILE_CONFIGS:
        raise KeyError(f"Unknown profile: {profile_id}")
    return apply_byte_cap_split(
        pack_blocks(audit_blocks, PROFILE_CONFIGS[profile_id], hard_cap=12),
        byte_cap_kb=9000,
    )


def estimate_profile_cost(
    profile_id: str,
    prior_b6_smoke_cost_usd: float,
    existing_rows: list[dict] | None = None,
) -> float:
    bases = [float(prior_b6_smoke_cost_usd or 0.0)]
    for row in existing_rows or []:
        if row.get("launched") and float(row.get("actual_cost_usd", 0.0) or 0.0) > 0:
            bases.append(float(row["actual_cost_usd"]))
    base = max(bases) if any(bases) else 0.0
    if base > 0:
        predicted = base * PROFILE_ESTIMATE_MULTIPLIER[profile_id]
    else:
        predicted = PROFILE_ESTIMATE_FALLBACK[profile_id]
    return round(predicted, 4)


def should_launch_profile(rows: list[dict], next_profile: str) -> tuple[bool, str]:
    by_id = {row["profile_id"]: row for row in rows}
    if next_profile == "b4":
        return True, ""
    if next_profile == "b2":
        b4 = by_id.get("b4")
        if not b4:
            return False, "b4 was not launched"
        if b4.get("survived"):
            return False, "b4 survived; downward search stops at b4"
        return True, ""
    return False, f"unknown profile {next_profile}"


def recommend_ceiling(rows: list[dict], budget: BudgetTracker) -> dict:
    by_id = {row["profile_id"]: row for row in rows}
    b4 = by_id.get("b4")
    b2 = by_id.get("b2")

    survived_b4 = bool(b4 and b4.get("survived"))
    launched_b2 = bool(b2 and b2.get("launched"))
    survived_b2 = bool(b2 and b2.get("survived"))

    if survived_b4:
        practical_ceiling = "b4"
        next_step_needed = False
        next_step_recommendation = "No further downward-search step is needed; `b4` is the current practical ceiling."
    elif survived_b2:
        practical_ceiling = "b2"
        next_step_needed = False
        next_step_recommendation = "No further downward-search step is needed; `b2` is the current practical ceiling."
    elif b4 and b4.get("launched") and launched_b2 and not survived_b2:
        practical_ceiling = "single-block only"
        next_step_needed = False
        next_step_recommendation = "No further batch-size search is needed; use single-block only on the big KJ project."
    elif b4 and b4.get("launched") and not survived_b4 and not launched_b2 and budget.stopped:
        practical_ceiling = "single-block only"
        next_step_needed = True
        next_step_recommendation = "Budget stopped the confirmatory `b2` check; current conservative recommendation is single-block only."
    elif b4 and b4.get("launched") and not survived_b4:
        practical_ceiling = "single-block only"
        next_step_needed = True
        next_step_recommendation = "A follow-up step is needed only if you want to resolve an ambiguity such as `b3`; otherwise stay conservative with single-block only."
    else:
        practical_ceiling = "single-block only"
        next_step_needed = True
        next_step_recommendation = "The downward search did not produce a decisive result."

    return {
        "survived_b4": survived_b4,
        "launched_b2": launched_b2,
        "survived_b2": survived_b2,
        "practical_ceiling": practical_ceiling,
        "next_step_needed": next_step_needed,
        "next_step_recommendation": next_step_recommendation,
    }


def make_summary_row(
    *,
    profile_id: str,
    launched: bool,
    source: str,
    summary: dict,
    quality: dict,
    estimated_cost_usd: float,
    actual_cost_usd: float,
    note: str,
) -> dict:
    verdicts = quality.get("verdict_counts") or {}
    return {
        "profile_id": profile_id,
        "source": source,
        "launched": launched,
        "hard_gate_passed": hard_gate_passed(summary),
        "quality_gate_passed": bool(quality.get("quality_gate_passed")),
        "survived": hard_gate_passed(summary) and bool(quality.get("quality_gate_passed")),
        "coverage_pct": float(summary.get("coverage_pct", 0.0) or 0.0),
        "missing_count": int(summary.get("missing_count", 0) or 0),
        "duplicate_count": int(summary.get("duplicate_count", 0) or 0),
        "extra_count": int(summary.get("extra_count", 0) or 0),
        "strict_success_count": int(summary.get("strict_success_count", 0) or 0),
        "strict_failure_count": int(summary.get("strict_failure_count", 0) or 0),
        "retry_triggered_count": int(summary.get("retry_triggered_count", 0) or 0),
        "retry_recovered_count": int(summary.get("retry_recovered_count", 0) or 0),
        "blocks_with_findings": int(summary.get("blocks_with_findings", 0) or 0),
        "total_findings": int(summary.get("total_findings", 0) or 0),
        "total_key_values": int(summary.get("total_key_values", 0) or 0),
        "elapsed_s": float(summary.get("elapsed_s", 0.0) or 0.0),
        "equivalent_count": int(verdicts.get("equivalent", 0) or 0),
        "likely_improved_count": int(verdicts.get("likely improved", 0) or 0),
        "likely_degraded_count": int(verdicts.get("likely degraded", 0) or 0),
        "uncertain_count": int(verdicts.get("uncertain", 0) or 0),
        "total_findings_delta": int(quality.get("total_findings_delta", 0) or 0),
        "blocks_with_findings_delta": int(quality.get("blocks_with_findings_delta", 0) or 0),
        "total_kv_delta": int(quality.get("total_kv_delta", 0) or 0),
        "hard_fail_reasons": list(quality.get("hard_fail_reasons", [])),
        "manual_review_block_ids": list(quality.get("manual_review_block_ids", [])),
        "estimated_cost_usd": round(float(estimated_cost_usd or 0.0), 6),
        "actual_cost_usd": round(float(actual_cost_usd or 0.0), 6),
        "note": note,
    }


def skipped_summary_row(profile_id: str, reason: str, source: str = "skipped") -> dict:
    empty_summary = {
        "coverage_pct": 0.0,
        "missing_count": AUDIT_SET_SIZE,
        "duplicate_count": 0,
        "extra_count": 0,
        "strict_success_count": 0,
        "strict_failure_count": AUDIT_SET_SIZE,
        "retry_triggered_count": 0,
        "retry_recovered_count": 0,
        "blocks_with_findings": 0,
        "total_findings": 0,
        "total_key_values": 0,
        "elapsed_s": 0.0,
        "fail_mode_distribution": {"skipped": 1},
    }
    empty_quality = {
        "quality_gate_passed": False,
        "hard_fail_reasons": [reason],
        "verdict_counts": {
            "equivalent": 0,
            "likely improved": 0,
            "likely degraded": 0,
            "uncertain": 0,
        },
        "total_findings_delta": 0,
        "blocks_with_findings_delta": 0,
        "total_kv_delta": 0,
        "manual_review_block_ids": [],
    }
    return make_summary_row(
        profile_id=profile_id,
        launched=False,
        source=source,
        summary=empty_summary,
        quality=empty_quality,
        estimated_cost_usd=0.0,
        actual_cost_usd=0.0,
        note=reason,
    )


def build_downward_search_summary_md(rows: list[dict]) -> str:
    lines = [
        "# Downward Search Summary\n",
        "| Profile | Source | Launched | Hard gate | Quality gate | Survived | Coverage | Missing/Dup/Extra | Retry trig/rec | Equivalent | Improved | Degraded | Uncertain | +Findings | +BlocksWithFindings | +KV | Cost USD | Est USD | Note |",
        "|---------|--------|----------|-----------|--------------|----------|----------|-------------------|----------------|------------|----------|----------|-----------|-----------|----------------------|-----|----------|---------|------|",
    ]
    for row in rows:
        lines.append(
            f"| {row['profile_id']} | {row['source']} | {row['launched']} | {row['hard_gate_passed']} | "
            f"{row['quality_gate_passed']} | {row['survived']} | {row['coverage_pct']:.2f}% | "
            f"{row['missing_count']}/{row['duplicate_count']}/{row['extra_count']} | "
            f"{row['retry_triggered_count']}/{row['retry_recovered_count']} | "
            f"{row['equivalent_count']} | {row['likely_improved_count']} | {row['likely_degraded_count']} | "
            f"{row['uncertain_count']} | {row['total_findings_delta']:+d} | "
            f"{row['blocks_with_findings_delta']:+d} | {row['total_kv_delta']:+d} | "
            f"${row['actual_cost_usd']:.4f} | ${row['estimated_cost_usd']:.4f} | {row['note']} |"
        )
    lines.append("")
    return "\n".join(lines)


def build_per_block_side_by_side_md(rows: list[dict]) -> str:
    lines = [
        "# Per-Block Side-by-Side\n",
        "| profile | block_id | verdict | ref_findings | cand_findings | ref_kv | cand_kv | ref_sum_len | cand_sum_len | reasons |",
        "|---------|----------|---------|--------------|---------------|--------|---------|-------------|--------------|---------|",
    ]
    for row in rows:
        lines.append(
            f"| {row['run_id']} | {row['block_id']} | {row['verdict']} | "
            f"{row['reference_findings_count']} | {row['candidate_findings_count']} | "
            f"{row['reference_kv_count']} | {row['candidate_kv_count']} | "
            f"{row['reference_summary_len']} | {row['candidate_summary_len']} | "
            f"{','.join(row['reasons'])} |"
        )
    lines.append("")
    return "\n".join(lines)


def build_manual_review_shortlist_md(rows: list[dict]) -> str:
    lines = ["# Manual Review Shortlist\n"]
    if not rows:
        lines.append("- No blocks shortlisted.")
        return "\n".join(lines) + "\n"
    for row in rows:
        lines.append(
            f"- `{row['block_id']}` [{row['run_id']}] -> {row['verdict']}: "
            f"{', '.join(row['reasons'])}"
        )
    lines.append("")
    return "\n".join(lines)


def build_winner_recommendation_md(*, recommendation: dict, rows: list[dict], budget: BudgetTracker) -> str:
    by_id = {row["profile_id"]: row for row in rows}
    b4 = by_id.get("b4")
    b2 = by_id.get("b2")
    lines = [
        "# Winner Recommendation\n",
        f"1. `b4` survived: {'YES' if recommendation['survived_b4'] else 'NO'}",
        f"2. `b2` launched: {'YES' if recommendation['launched_b2'] else 'NO'}",
        f"3. `b2` survived: {'YES' if recommendation['survived_b2'] else 'NO'}",
        f"4. Practical ceiling now: `{recommendation['practical_ceiling']}`",
        f"5. Next step needed: {'YES' if recommendation['next_step_needed'] else 'NO'}",
        "",
        "## Why",
    ]
    if b4:
        lines.append(
            f"- `b4`: launched={b4['launched']}, survived={b4['survived']}, "
            f"coverage={b4['coverage_pct']:.2f}%, degraded={b4['likely_degraded_count']}."
        )
    if b2:
        lines.append(
            f"- `b2`: launched={b2['launched']}, survived={b2['survived']}, "
            f"coverage={b2['coverage_pct']:.2f}%, degraded={b2['likely_degraded_count']}."
        )
    if budget.stopped:
        lines.append(f"- Budget stop: {budget.stop_reason}")
    lines.extend([
        "",
        "## Recommendation",
        f"- {recommendation['next_step_recommendation']}",
        f"- Additional spend used: ${budget.spent_usd:.4f} of ${budget.cap_usd:.2f}.",
        "",
    ])
    return "\n".join(lines)


async def main_async(args: argparse.Namespace) -> int:
    resolution = resolve_project(args.pdf, Path(args.project_dir) if args.project_dir else None)
    project_dir = resolution.project_dir
    project_info = _load_json(project_dir / "project_info.json")
    by_id = {block["block_id"]: block for block in resolution.blocks}

    reuse_dir, reuse_manifest, audit_ids, audit_manifest_md, reference_map, reference_summary, prior_smoke_payload = load_reused_audit_context(project_dir)
    missing_from_index = [block_id for block_id in audit_ids if block_id not in by_id]
    if missing_from_index:
        raise RuntimeError(
            "Reused audit set does not match current blocks/index.json: missing "
            + ", ".join(missing_from_index)
        )
    audit_blocks = [by_id[block_id] for block_id in audit_ids]

    ts = _ts()
    out_dir = project_dir / "_experiments" / EXP_NAME / ts
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "timestamp": ts,
        "pdf": args.pdf,
        "project_dir": str(project_dir),
        "block_source": resolution.block_source,
        "blocks_index_path": str(resolution.blocks_index_path),
        "block_count": resolution.total_blocks,
        "provider": PROVIDER,
        "model_id": MODEL_ID,
        "reasoning_effort": "high",
        "response_healing_initial": True,
        "parallelism": 2,
        "resolution": "r800",
        "strict_success_checks": True,
        "retry_on_non_success": True,
        "profiles_considered": list(PROFILE_ORDER),
        "full_doc_run_performed": False,
        "b6_or_higher_tested": False,
        "resolution_above_r800_tested": False,
        "claude_compared": False,
        "flash_touched": False,
        "production_defaults_changed": False,
        "audit_set_reused_from": str(reuse_dir),
        "reference_reused_from": str(reuse_dir),
        "budget_cap_usd": args.budget_cap_usd,
        "command": " ".join(sys.argv),
    }
    _save_json(out_dir / "manifest.json", manifest)
    _save_md(
        out_dir / "audit_set_reuse_report.md",
        build_audit_set_reuse_report(
            reuse_dir=reuse_dir,
            reuse_manifest=reuse_manifest,
            audit_ids=audit_ids,
            audit_manifest_md=audit_manifest_md,
            reference_summary=reference_summary,
            prior_smoke_payload=prior_smoke_payload,
        ),
    )

    prior_b6_smoke_cost = float(
        (prior_smoke_payload.get("summary_row") or {}).get("total_cost_usd", 0.0) or 0.0
    )
    schema = load_openrouter_block_batch_schema()
    budget = BudgetTracker(cap_usd=args.budget_cap_usd)
    summary_rows: list[dict] = []
    per_block_rows: list[dict] = []
    manual_review_rows: list[dict] = []

    for profile_id in PROFILE_ORDER:
        allowed, reason = should_launch_profile(summary_rows, profile_id)
        if not allowed:
            summary_rows.append(skipped_summary_row(profile_id, f"early_stop: {reason}"))
            budget.record_skip(step_id=profile_id, label=f"Run {profile_id}", note=f"early_stop: {reason}")
            continue

        estimated_cost = estimate_profile_cost(profile_id, prior_b6_smoke_cost, summary_rows)
        event = budget.preflight(
            step_id=profile_id,
            label=f"Audit smoke {profile_id}",
            predicted_usd=estimated_cost,
        )
        if not event.approved:
            summary_rows.append(
                skipped_summary_row(profile_id, event.note or "budget_stop", source="budget_stop")
            )
            break

        batches = profile_batches_for_audit(audit_blocks, profile_id)
        logger.info("Running %s on %d audit blocks across %d batches", profile_id, len(audit_ids), len(batches))
        results, elapsed = await run_call_set(
            project_dir=project_dir,
            blocks_or_batches=batches,
            project_info=project_info,
            reasoning_effort="high",
            response_healing_initial=True,
            schema=schema,
            timeout=DEFAULT_TIMEOUT_SEC,
            max_output_tokens=MAX_OUTPUT_TOKENS,
            parallelism=2,
            run_id=profile_id,
            resolution="r800",
        )
        actual_cost = sum(result.cost_usd for result in results)
        budget.commit(event, actual_cost, note=f"calls={len(results)} elapsed={elapsed:.1f}s")

        summary = aggregate_results(
            results=results,
            input_block_ids=audit_ids,
            elapsed_s=elapsed,
            label=f"{profile_id} audit smoke",
            mode="batch",
            reasoning_effort="high",
            parallelism=2,
            resolution="r800",
        )
        candidate_map = analyses_map_from_results(results)
        side = build_side_by_side(
            block_ids=audit_ids,
            reference_map=reference_map,
            candidate_map=candidate_map,
            phase="downward_search",
            run_id=profile_id,
        )
        side, quality = enrich_quality(
            side_by_side=side,
            block_ids=audit_ids,
            reference_map=reference_map,
            summary=summary,
        )
        row = make_summary_row(
            profile_id=profile_id,
            launched=True,
            source="executed",
            summary=summary,
            quality=quality,
            estimated_cost_usd=estimated_cost,
            actual_cost_usd=actual_cost,
            note=f"batches={len(batches)} elapsed={elapsed:.1f}s",
        )
        summary_rows.append(row)
        per_block_rows.extend(side)
        manual_review_rows.extend([item for item in side if item["verdict"] != "equivalent"])

    if not any(row["profile_id"] == "b2" for row in summary_rows):
        allowed, reason = should_launch_profile(summary_rows, "b2")
        if not allowed:
            summary_rows.append(skipped_summary_row("b2", f"early_stop: {reason}"))
            budget.record_skip(step_id="b2", label="Run b2", note=f"early_stop: {reason}")

    recommendation = recommend_ceiling(summary_rows, budget)

    _save_json(
        out_dir / "downward_search_summary.json",
        {
            "rows": summary_rows,
            "recommendation": recommendation,
        },
    )
    _save_csv(out_dir / "downward_search_summary.csv", summary_rows)
    _save_md(out_dir / "downward_search_summary.md", build_downward_search_summary_md(summary_rows))
    _save_json(out_dir / "per_block_side_by_side.json", per_block_rows)
    _save_md(out_dir / "per_block_side_by_side.md", build_per_block_side_by_side_md(per_block_rows))
    _save_md(out_dir / "manual_review_shortlist.md", build_manual_review_shortlist_md(manual_review_rows))
    _save_json(out_dir / "budget_timeline.json", budget.to_dict())
    _save_md(out_dir / "budget_timeline.md", build_budget_timeline_md(budget))
    _save_md(
        out_dir / "winner_recommendation.md",
        build_winner_recommendation_md(
            recommendation=recommendation,
            rows=summary_rows,
            budget=budget,
        ),
    )

    manifest.update(
        {
            "audit_set_size": len(audit_ids),
            "reference_reused": True,
            "additional_spend_usd": budget.spent_usd,
            "budget_stop_reason": budget.stop_reason,
            "survived_b4": recommendation["survived_b4"],
            "launched_b2": recommendation["launched_b2"],
            "survived_b2": recommendation["survived_b2"],
            "practical_ceiling": recommendation["practical_ceiling"],
        }
    )
    _save_json(out_dir / "manifest.json", manifest)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pdf", default=PDF_NAME)
    parser.add_argument("--project-dir", default="")
    parser.add_argument("--budget-cap-usd", type=float, default=DEFAULT_BUDGET_CAP_USD)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
