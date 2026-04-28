"""
Confirmatory big-project validation for Gemini 3.1 Pro on stage 02 via OpenRouter.

Goal:
  Confirm that the known-good technical profile
    - batch = b6
    - resolution = r800
  survives on the large KJ project, not just on the small project.

Important constraints:
  - OpenRouter only
  - no new matrix
  - no b8+
  - no r1000+
  - no Claude comparison
  - no Flash path changes
  - no production-default changes
  - no recrop / no rebuild blocks

Artifacts:
  <project_dir>/_experiments/pro_b6_r800_big_project_validation/<timestamp>/
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
    BATCH_PROFILES,
    apply_byte_cap_split,
    classify_risk,
    pack_blocks,
)
from run_pro_variantA_small_budget import (  # noqa: E402
    CallResult,
    aggregate_results,
    analyses_map_from_results,
    build_budget_timeline_md,
    build_side_by_side,
    evaluate_quality_preservation,
    run_call_set,
)
from run_stage02_recall_hybrid import resolve_project  # noqa: E402
from webapp.services.openrouter_block_batch import load_openrouter_block_batch_schema  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pro_b6_r800_big_project_validation")


PDF_NAME = "13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf"
MODEL_ID = "google/gemini-3.1-pro-preview"
EXP_NAME = "pro_b6_r800_big_project_validation"
DEFAULT_TIMEOUT_SEC = 600
MAX_OUTPUT_TOKENS = 32768
DEFAULT_BUDGET_CAP_USD = 9.00
AUDIT_HEAVY_COUNT = 6
AUDIT_NORMAL_COUNT = 4
AUDIT_RISKY_COUNT = 2
AUDIT_SET_SIZE = AUDIT_HEAVY_COUNT + AUDIT_NORMAL_COUNT + AUDIT_RISKY_COUNT
REFERENCE_PRIOR_FALLBACK_PER_BLOCK_USD = 0.113349
BATCH_ESTIMATE_MIN_PER_BLOCK_USD = 0.030
BATCH_ESTIMATE_MAX_PER_BLOCK_USD = 0.050
FULL_RUN_SMOKE_BUFFER = 1.15


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


def _risk_sort_key(risk: str) -> int:
    return {"heavy": 0, "normal": 1, "light": 2}.get(risk, 9)


def _complexity_tuple(block: dict) -> tuple:
    render = block.get("render_size") or [0, 0]
    render_long = max(render) if render else 0
    crop = block.get("crop_px") or [0, 0, 0, 0]
    crop_long = max((crop[2] - crop[0]), (crop[3] - crop[1])) if len(crop) == 4 else 0
    return (
        float(block.get("size_kb", 0) or 0),
        int(block.get("ocr_text_len", 0) or 0),
        int(render_long or 0),
        int(crop_long or 0),
        str(block.get("ocr_label") or ""),
    )


def _historical_score(block: dict) -> tuple:
    return (
        -_risk_sort_key(classify_risk(block)),
        *_complexity_tuple(block),
    )


@dataclass
class BudgetEvent:
    step_id: str
    label: str
    predicted_usd: float
    actual_usd: float = 0.0
    approved: bool = False
    remaining_before: float = 0.0
    remaining_after: float = 0.0
    note: str = ""


@dataclass
class BudgetTracker:
    cap_usd: float
    spent_usd: float = 0.0
    events: list[BudgetEvent] = field(default_factory=list)
    stopped: bool = False
    stop_reason: str = ""

    @property
    def remaining(self) -> float:
        return max(0.0, self.cap_usd - self.spent_usd)

    def preflight(self, *, step_id: str, label: str, predicted_usd: float) -> BudgetEvent:
        event = BudgetEvent(
            step_id=step_id,
            label=label,
            predicted_usd=round(predicted_usd, 4),
            approved=False,
            remaining_before=round(self.remaining, 4),
            remaining_after=round(self.remaining, 4),
        )
        if predicted_usd <= self.remaining:
            event.approved = True
        else:
            event.note = f"predicted ${predicted_usd:.4f} exceeds remaining ${self.remaining:.4f}"
            self.stopped = True
            self.stop_reason = f"[{step_id}] {event.note}"
        self.events.append(event)
        return event

    def commit(self, event: BudgetEvent, actual_usd: float, note: str = "") -> None:
        self.spent_usd = round(self.spent_usd + actual_usd, 6)
        event.actual_usd = round(actual_usd, 6)
        event.remaining_after = round(self.remaining, 6)
        if note:
            event.note = note

    def record_skip(self, *, step_id: str, label: str, note: str) -> None:
        self.events.append(
            BudgetEvent(
                step_id=step_id,
                label=label,
                predicted_usd=0.0,
                actual_usd=0.0,
                approved=False,
                remaining_before=round(self.remaining, 4),
                remaining_after=round(self.remaining, 4),
                note=note,
            )
        )

    def to_dict(self) -> dict:
        return {
            "cap_usd": self.cap_usd,
            "spent_usd": round(self.spent_usd, 6),
            "remaining_usd": round(self.remaining, 6),
            "stopped": self.stopped,
            "stop_reason": self.stop_reason,
            "events": [asdict(ev) for ev in self.events],
        }


def hard_gate_passed(summary: dict) -> bool:
    return (
        summary.get("coverage_pct") == 100.0
        and summary.get("missing_count") == 0
        and summary.get("duplicate_count") == 0
        and summary.get("extra_count") == 0
        and not summary.get("fail_mode_distribution")
    )


def annotate_verdicts(rows: list[dict]) -> list[dict]:
    annotated: list[dict] = []
    for row in rows:
        verdict = "equivalent"
        classification = row.get("classification")
        if classification == "likely_improved":
            verdict = "likely improved"
        elif classification == "likely_degraded":
            has_improvement_signal = any(
                row.get(key) for key in (
                    "summary_specificity_improved",
                    "kv_adequacy_improved",
                    "useful_findings_improved",
                )
            )
            verdict = "uncertain" if has_improvement_signal else "likely degraded"
        annotated.append({**row, "verdict": verdict})
    return annotated


def enrich_quality(
    *,
    side_by_side: list[dict],
    block_ids: list[str],
    reference_map: dict[str, dict],
    summary: dict,
) -> tuple[list[dict], dict]:
    annotated = annotate_verdicts(side_by_side)
    quality = evaluate_quality_preservation(side_by_side)
    verdict_counts = {
        "equivalent": sum(1 for row in annotated if row["verdict"] == "equivalent"),
        "likely improved": sum(1 for row in annotated if row["verdict"] == "likely improved"),
        "likely degraded": sum(1 for row in annotated if row["verdict"] == "likely degraded"),
        "uncertain": sum(1 for row in annotated if row["verdict"] == "uncertain"),
    }
    total_reference_findings = sum(len((reference_map[bid].get("findings") or [])) for bid in block_ids)
    total_reference_kv = sum(len((reference_map[bid].get("key_values_read") or [])) for bid in block_ids)
    reference_blocks_with_findings = sum(1 for bid in block_ids if reference_map[bid].get("findings"))
    quality = {
        **quality,
        "verdict_counts": verdict_counts,
        "total_findings_delta": int(summary["total_findings"] - total_reference_findings),
        "blocks_with_findings_delta": int(summary["blocks_with_findings"] - reference_blocks_with_findings),
        "total_kv_delta": int(summary["total_key_values"] - total_reference_kv),
    }
    return annotated, quality


def should_continue_after_smoke(smoke_result: dict) -> tuple[bool, str]:
    if not smoke_result.get("hard_gate_passed"):
        return False, "audit smoke failed hard completeness gate"
    if not smoke_result.get("quality_gate_passed"):
        return False, "audit smoke showed quality-preservation degradation"
    return True, ""


def recommend_validation(
    *,
    smoke_result: dict,
    full_result: dict | None,
    budget: BudgetTracker,
) -> dict:
    smoke_survived = bool(smoke_result.get("survived"))
    full_run_performed = bool(full_result and full_result.get("performed"))
    full_hard_gate = bool(full_result and full_result.get("hard_gate_passed"))
    full_quality_gate = bool(full_result and full_result.get("audit_quality_gate_passed"))
    full_survived = full_run_performed and full_hard_gate and full_quality_gate

    if not smoke_survived:
        drift_verdict = "clear smoke-level drift or completeness failure"
    elif not full_run_performed:
        drift_verdict = "smoke survived; full-project quality not executed"
    elif full_quality_gate:
        drift_verdict = "no clear audit-set drift"
    else:
        drift_verdict = "audit-set drift observed in full run"

    practical = smoke_survived and full_survived
    if not smoke_survived:
        next_step = "No. Fix smoke-level regressions before any larger validation."
    elif not full_run_performed and budget.stopped:
        next_step = "No. Current cap was reached before a justified full confirmatory run."
    elif not full_run_performed:
        next_step = "No. Smoke stop already answered the safety question for this step."
    elif practical:
        next_step = "No. This is enough to treat `b6 + r800` as the practical big-project config for now."
    else:
        next_step = "Yes. Investigate the shortlisted blocks before treating this profile as big-project safe."

    return {
        "smoke_survived": smoke_survived,
        "full_run_performed": full_run_performed,
        "full_run_survived": full_survived,
        "quality_drift_verdict": drift_verdict,
        "practical_big_project_config": practical,
        "recommended_profile": "b6 + r800" if practical else "not yet confirmed",
        "next_step_needed": not practical,
        "next_step_recommendation": next_step,
    }


def flatten_smoke_summary(
    *,
    summary: dict,
    quality: dict,
    survived: bool,
    estimated_cost_usd: float,
) -> dict:
    verdicts = quality.get("verdict_counts") or {}
    return {
        "profile_id": "b6",
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
        "avg_duration_s": float(summary.get("avg_duration_s", 0.0) or 0.0),
        "median_duration_s": float(summary.get("median_duration_s", 0.0) or 0.0),
        "p95_duration_s": float(summary.get("p95_duration_s", 0.0) or 0.0),
        "total_prompt_tokens": int(summary.get("total_prompt_tokens", 0) or 0),
        "total_output_tokens": int(summary.get("total_output_tokens", 0) or 0),
        "total_reasoning_tokens": int(summary.get("total_reasoning_tokens", 0) or 0),
        "total_cost_usd": round(float(summary.get("total_cost_usd", 0.0) or 0.0), 6),
        "estimated_cost_usd": round(float(estimated_cost_usd or 0.0), 6),
        "hard_gate_passed": hard_gate_passed(summary),
        "quality_gate_passed": bool(quality.get("quality_gate_passed")),
        "survived": survived,
        "equivalent_count": int(verdicts.get("equivalent", 0) or 0),
        "likely_improved_count": int(verdicts.get("likely improved", 0) or 0),
        "likely_degraded_count": int(verdicts.get("likely degraded", 0) or 0),
        "uncertain_count": int(verdicts.get("uncertain", 0) or 0),
        "total_findings_delta": int(quality.get("total_findings_delta", 0) or 0),
        "blocks_with_findings_delta": int(quality.get("blocks_with_findings_delta", 0) or 0),
        "total_kv_delta": int(quality.get("total_kv_delta", 0) or 0),
        "hard_fail_reasons": list(quality.get("hard_fail_reasons", [])),
    }


def load_historical_problematic_ids(project_dir: Path) -> tuple[list[str], list[str]]:
    patterns = [
        "_experiments/gemini_openrouter_stage02_singleblock/*/escalation_sample_block_ids.json",
        "_experiments/gemini_openrouter_stage02_budget/*/pro_fallback_sample_ids.json",
    ]
    seen: set[str] = set()
    collected: list[str] = []
    used_sources: list[str] = []
    for pattern in patterns:
        matches = sorted(project_dir.glob(pattern), reverse=True)
        if not matches:
            continue
        path = matches[0]
        try:
            ids = _load_json(path)
        except Exception:  # pragma: no cover - defensive
            continue
        if not isinstance(ids, list):
            continue
        used_sources.append(str(path))
        for bid in ids:
            if isinstance(bid, str) and bid not in seen:
                seen.add(bid)
                collected.append(bid)
    return collected, used_sources


def select_audit_set(
    blocks: list[dict],
    *,
    historical_problematic_ids: list[str],
) -> tuple[list[str], list[dict]]:
    by_id = {block["block_id"]: block for block in blocks}
    heavy = sorted(
        [block for block in blocks if classify_risk(block) == "heavy"],
        key=_complexity_tuple,
        reverse=True,
    )
    normal = sorted(
        [block for block in blocks if classify_risk(block) == "normal"],
        key=_complexity_tuple,
        reverse=True,
    )

    selected: list[str] = []
    manifest_rows: list[dict] = []

    def add(block: dict, bucket: str, reason: str) -> None:
        block_id = block["block_id"]
        if block_id in selected:
            return
        selected.append(block_id)
        manifest_rows.append(
            {
                "block_id": block_id,
                "page": block.get("page"),
                "risk": classify_risk(block),
                "size_kb": block.get("size_kb"),
                "ocr_text_len": block.get("ocr_text_len"),
                "selection_bucket": bucket,
                "reason": reason,
                "ocr_label": str(block.get("ocr_label") or ""),
            }
        )

    for idx, block in enumerate(heavy[:AUDIT_HEAVY_COUNT], start=1):
        add(block, "heavy", f"top_heavy_complexity_rank_{idx}")

    normal_added = 0
    for block in normal:
        if normal_added >= AUDIT_NORMAL_COUNT:
            break
        if block["block_id"] in selected:
            continue
        normal_added += 1
        add(block, "normal_dense", f"top_normal_dense_rank_{normal_added}")

    historical_candidates = [
        by_id[bid]
        for bid in historical_problematic_ids
        if bid in by_id and bid not in selected
    ]
    historical_candidates.sort(key=_historical_score, reverse=True)
    for idx, block in enumerate(historical_candidates[:AUDIT_RISKY_COUNT], start=1):
        add(block, "historical_risky", f"historical_problematic_rank_{idx}")

    if len(selected) < AUDIT_SET_SIZE:
        remaining = [
            block for block in blocks
            if block["block_id"] not in selected
        ]
        remaining.sort(
            key=lambda block: (
                -_risk_sort_key(classify_risk(block)),
                *_complexity_tuple(block),
            ),
            reverse=True,
        )
        fill_idx = 0
        for block in remaining:
            if len(selected) >= AUDIT_SET_SIZE:
                break
            fill_idx += 1
            add(block, "risk_heuristic_fill", f"risk_heuristic_fill_rank_{fill_idx}")

    if len(selected) != AUDIT_SET_SIZE:
        raise RuntimeError(f"Expected {AUDIT_SET_SIZE} audit blocks, got {len(selected)}")

    return selected, manifest_rows


def build_project_resolution_md(
    *,
    project_dir: Path,
    block_source: str,
    blocks_index_path: Path,
    total_blocks: int,
) -> str:
    return "\n".join([
        "# Project Resolution\n",
        f"- Resolved project dir: `{project_dir}`",
        f"- PDF: `{PDF_NAME}`",
        f"- Block source: `{block_source}`",
        f"- Blocks index: `{blocks_index_path}`",
        f"- Total blocks: {total_blocks}",
        "- Recrop performed: no",
        "- Rebuild performed: no",
        "",
    ])


def build_audit_set_manifest_md(rows: list[dict], historical_sources: list[str]) -> str:
    lines = [
        "# Audit Set Manifest\n",
        f"- Total audit blocks: {len(rows)}",
        f"- Heavy: {sum(1 for row in rows if row['selection_bucket'] == 'heavy')}",
        f"- Normal dense: {sum(1 for row in rows if row['selection_bucket'] == 'normal_dense')}",
        f"- Historical risky / heuristic: {sum(1 for row in rows if row['selection_bucket'] not in {'heavy', 'normal_dense'})}",
        "",
        "## Historical sources",
    ]
    if historical_sources:
        lines.extend(f"- `{source}`" for source in historical_sources)
    else:
        lines.append("- None found; fallback was purely heuristic.")
    lines.extend([
        "",
        "| block_id | page | risk | size_kb | ocr_text_len | bucket | reason |",
        "|----------|------|------|---------|--------------|--------|--------|",
    ])
    for row in rows:
        lines.append(
            f"| {row['block_id']} | {row.get('page')} | {row.get('risk')} | {row.get('size_kb')} | "
            f"{row.get('ocr_text_len')} | {row.get('selection_bucket')} | {row.get('reason')} |"
        )
    lines.append("")
    return "\n".join(lines)


def find_reference_cost_prior(project_dir: Path) -> tuple[float, str]:
    candidates = sorted(
        project_dir.glob(
            "_experiments/gemini_openrouter_stage02_singleblock/*/pro_escalation_sample_metrics.json"
        ),
        reverse=True,
    )
    for path in candidates:
        data = _load_json(path)
        if data.get("model_id") != MODEL_ID:
            continue
        if data.get("parallelism") != 2:
            continue
        cost_per_block = float(data.get("cost_per_valid_block", 0.0) or 0.0)
        if cost_per_block > 0:
            return cost_per_block, str(path)
    return REFERENCE_PRIOR_FALLBACK_PER_BLOCK_USD, "fallback_constant"


def estimate_reference_cost(block_count: int, prior_per_block: float) -> float:
    return round(block_count * prior_per_block * 1.02, 4)


def estimate_smoke_cost(block_count: int, reference_prior_per_block: float) -> float:
    derived_per_block = reference_prior_per_block * 0.45
    per_block = min(BATCH_ESTIMATE_MAX_PER_BLOCK_USD, max(BATCH_ESTIMATE_MIN_PER_BLOCK_USD, derived_per_block))
    return round(block_count * per_block, 4)


def estimate_full_cost(total_blocks: int, smoke_total_cost_usd: float, smoke_block_count: int) -> float:
    if smoke_block_count <= 0:
        return 0.0
    smoke_per_block = smoke_total_cost_usd / smoke_block_count
    return round(total_blocks * smoke_per_block * FULL_RUN_SMOKE_BUFFER, 4)


def build_reference_reuse_report(
    *,
    prior_run_dirs: list[str],
    requested_ids: list[str],
    reused_ids: list[str],
    missing_ids_before_run: list[str],
    newly_run_ids: list[str],
    final_missing_ids: list[str],
) -> str:
    lines = [
        "# Reference Reuse Report\n",
        f"- Requested audit blocks: {len(requested_ids)}",
        f"- Prior reusable runs inspected: {len(prior_run_dirs)}",
        f"- Reused block references: {len(reused_ids)}",
        f"- Missing before new reference run: {len(missing_ids_before_run)}",
        f"- Newly executed single-block references: {len(newly_run_ids)}",
        f"- Missing after fill: {len(final_missing_ids)}",
        "",
        "## Prior reusable runs",
    ]
    if prior_run_dirs:
        lines.extend(f"- `{item}`" for item in prior_run_dirs)
    else:
        lines.append("- None")
    lines.extend([
        "",
        "## Reused block_ids",
    ])
    if reused_ids:
        lines.extend(f"- `{bid}`" for bid in reused_ids)
    else:
        lines.append("- None")
    lines.extend([
        "",
        "## Newly executed block_ids",
    ])
    if newly_run_ids:
        lines.extend(f"- `{bid}`" for bid in newly_run_ids)
    else:
        lines.append("- None")
    if final_missing_ids:
        lines.extend([
            "",
            "## Missing after fill",
        ])
        lines.extend(f"- `{bid}`" for bid in final_missing_ids)
    lines.append("")
    return "\n".join(lines)


def _valid_reference_run_summary(summary: dict) -> bool:
    return (
        summary.get("model_id") == MODEL_ID
        and summary.get("reasoning_effort") == "high"
        and summary.get("parallelism") == 2
        and summary.get("response_healing_initial") is True
        and summary.get("resolution") == "r800"
        and summary.get("mode") == "single_block"
    )


def find_reusable_reference_outputs(
    project_dir: Path,
    requested_ids: list[str],
) -> tuple[dict[str, dict], dict[str, str], list[str]]:
    root = project_dir / "_experiments" / EXP_NAME
    if not root.exists():
        return {}, {}, []

    reused_map: dict[str, dict] = {}
    reused_sources: dict[str, str] = {}
    inspected_dirs: list[str] = []
    for run_dir in sorted([path for path in root.iterdir() if path.is_dir()], reverse=True):
        summary_path = run_dir / "reference_run_summary.json"
        analyses_path = run_dir / "reference_analyses.json"
        if not summary_path.exists() or not analyses_path.exists():
            continue
        summary = _load_json(summary_path)
        if not _valid_reference_run_summary(summary):
            continue
        analyses = _load_json(analyses_path)
        if not isinstance(analyses, dict):
            continue
        inspected_dirs.append(str(run_dir))
        for block_id in requested_ids:
            if block_id in reused_map:
                continue
            payload = analyses.get(block_id)
            if isinstance(payload, dict):
                reused_map[block_id] = payload
                reused_sources[block_id] = str(run_dir)
        if len(reused_map) == len(requested_ids):
            break
    return reused_map, reused_sources, inspected_dirs


def build_smoke_side_by_side_md(rows: list[dict]) -> str:
    lines = [
        "# Audit Smoke Side-by-Side\n",
        "| block_id | verdict | ref_findings | cand_findings | ref_kv | cand_kv | ref_sum_len | cand_sum_len | reasons |",
        "|----------|---------|--------------|---------------|--------|---------|-------------|--------------|---------|",
    ]
    for row in rows:
        lines.append(
            f"| {row['block_id']} | {row['verdict']} | {row['reference_findings_count']} | "
            f"{row['candidate_findings_count']} | {row['reference_kv_count']} | {row['candidate_kv_count']} | "
            f"{row['reference_summary_len']} | {row['candidate_summary_len']} | {','.join(row['reasons'])} |"
        )
    lines.append("")
    return "\n".join(lines)


def build_audit_smoke_summary_md(
    *,
    summary_row: dict,
    quality: dict,
) -> str:
    lines = [
        "# Audit Smoke Summary\n",
        f"- Smoke survived: {'YES' if summary_row['survived'] else 'NO'}",
        f"- Hard gate passed: {'YES' if summary_row['hard_gate_passed'] else 'NO'}",
        f"- Quality gate passed: {'YES' if summary_row['quality_gate_passed'] else 'NO'}",
        f"- Coverage: {summary_row['coverage_pct']:.2f}%",
        f"- Missing / Duplicate / Extra: {summary_row['missing_count']} / {summary_row['duplicate_count']} / {summary_row['extra_count']}",
        f"- Strict success / failure: {summary_row['strict_success_count']} / {summary_row['strict_failure_count']}",
        f"- Retry triggered / recovered: {summary_row['retry_triggered_count']} / {summary_row['retry_recovered_count']}",
        f"- Findings total: {summary_row['total_findings']} ({summary_row['total_findings_delta']:+d} vs reference)",
        f"- Blocks with findings delta: {summary_row['blocks_with_findings_delta']:+d}",
        f"- Total key values: {summary_row['total_key_values']} ({summary_row['total_kv_delta']:+d} vs reference)",
        f"- Verdicts: equivalent={summary_row['equivalent_count']}, likely improved={summary_row['likely_improved_count']}, likely degraded={summary_row['likely_degraded_count']}, uncertain={summary_row['uncertain_count']}",
        f"- Cost USD: ${summary_row['total_cost_usd']:.4f} (estimated ${summary_row['estimated_cost_usd']:.4f})",
        f"- Elapsed: {summary_row['elapsed_s']:.1f}s",
    ]
    hard_fail_reasons = quality.get("hard_fail_reasons") or []
    if hard_fail_reasons:
        lines.append(f"- Hard fail reasons: {', '.join(hard_fail_reasons)}")
    lines.append("")
    return "\n".join(lines)


def build_confirmatory_full_summary_md(payload: dict) -> str:
    if not payload.get("performed"):
        return "\n".join([
            "# Confirmatory Full Summary\n",
            f"- Full run performed: NO",
            f"- Reason: {payload.get('reason', 'not_run')}",
            "",
        ])

    summary = payload["summary"]
    quality = payload["audit_quality"]
    verdicts = quality.get("verdict_counts") or {}
    lines = [
        "# Confirmatory Full Summary\n",
        "- Full run performed: YES",
        f"- Coverage: {summary['coverage_pct']:.2f}%",
        f"- Missing / Duplicate / Extra: {summary['missing_count']} / {summary['duplicate_count']} / {summary['extra_count']}",
        f"- Strict success / failure: {summary['strict_success_count']} / {summary['strict_failure_count']}",
        f"- Retry triggered / recovered: {summary['retry_triggered_count']} / {summary['retry_recovered_count']}",
        f"- Fail mode distribution: {json.dumps(summary['fail_mode_distribution'], ensure_ascii=False)}",
        f"- Total findings: {summary['total_findings']}",
        f"- Blocks with findings: {summary['blocks_with_findings']}",
        f"- Total key values: {summary['total_key_values']}",
        f"- Cost USD: ${summary['total_cost_usd']:.4f}",
        f"- Elapsed: {summary['elapsed_s']:.1f}s",
        f"- Avg / median / p95 batch duration: {summary['avg_duration_s']:.1f}s / {summary['median_duration_s']:.1f}s / {summary['p95_duration_s']:.1f}s",
        f"- Prompt / output / reasoning tokens: {summary['total_prompt_tokens']} / {summary['total_output_tokens']} / {summary['total_reasoning_tokens']}",
        "",
        "## Audit Drift vs Single-Block Reference",
        f"- Quality gate passed: {'YES' if payload['audit_quality_gate_passed'] else 'NO'}",
        f"- Verdicts: equivalent={verdicts.get('equivalent', 0)}, likely improved={verdicts.get('likely improved', 0)}, likely degraded={verdicts.get('likely degraded', 0)}, uncertain={verdicts.get('uncertain', 0)}",
        f"- Findings delta: {quality.get('total_findings_delta', 0):+d}",
        f"- Blocks-with-findings delta: {quality.get('blocks_with_findings_delta', 0):+d}",
        f"- Key-values delta: {quality.get('total_kv_delta', 0):+d}",
    ]
    hard_fail_reasons = quality.get("hard_fail_reasons") or []
    if hard_fail_reasons:
        lines.append(f"- Audit drift reasons: {', '.join(hard_fail_reasons)}")
    suspicious = payload.get("manual_review_block_ids") or []
    lines.extend([
        "",
        "## Manual Review Shortlist",
        f"- Count: {len(suspicious)}",
    ])
    if suspicious:
        lines.append("- Block ids: " + ", ".join(f"`{bid}`" for bid in suspicious))
    else:
        lines.append("- Block ids: none")
    lines.append("")
    return "\n".join(lines)


def build_manual_review_shortlist_md(entries: list[dict]) -> str:
    lines = ["# Manual Review Shortlist\n"]
    if not entries:
        lines.append("- No blocks shortlisted.")
        return "\n".join(lines) + "\n"
    for row in entries:
        lines.append(
            f"- `{row['block_id']}` [{row['context']}] -> {row['verdict']}: "
            f"{', '.join(row['reasons'])}"
        )
    lines.append("")
    return "\n".join(lines)


def build_winner_recommendation_md(
    *,
    recommendation: dict,
    smoke_summary_row: dict,
    full_result: dict | None,
    budget: BudgetTracker,
) -> str:
    full_survived = bool(full_result and full_result.get("performed") and full_result.get("hard_gate_passed") and full_result.get("audit_quality_gate_passed"))
    drift_verdict = recommendation["quality_drift_verdict"]
    lines = [
        "# Winner Recommendation\n",
        f"1. `b6 + r800` survives on the big project: {'YES' if recommendation['practical_big_project_config'] else 'NO'}",
        f"2. Audit smoke gate passed: {'YES' if recommendation['smoke_survived'] else 'NO'}",
        f"3. Full confirmatory run passed: {'YES' if full_survived else 'NO' if recommendation['full_run_performed'] else 'NOT RUN'}",
        f"4. Audit-set drift verdict: {drift_verdict}",
        f"5. Practical big-project config: `{recommendation['recommended_profile']}`",
        f"6. Next step needed: {'YES' if recommendation['next_step_needed'] else 'NO'}",
        "",
        "## Why",
        f"- Smoke hard gate / quality gate: {smoke_summary_row['hard_gate_passed']} / {smoke_summary_row['quality_gate_passed']}.",
        f"- Smoke verdict counts: equivalent={smoke_summary_row['equivalent_count']}, likely improved={smoke_summary_row['likely_improved_count']}, likely degraded={smoke_summary_row['likely_degraded_count']}, uncertain={smoke_summary_row['uncertain_count']}.",
    ]
    if full_result and full_result.get("performed"):
        summary = full_result["summary"]
        lines.append(
            f"- Full run completeness: coverage={summary['coverage_pct']:.2f}%, "
            f"missing/dup/extra={summary['missing_count']}/{summary['duplicate_count']}/{summary['extra_count']}."
        )
        lines.append(
            f"- Full run cost/time: ${summary['total_cost_usd']:.4f} / {summary['elapsed_s']:.1f}s."
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
    blocks = resolution.blocks
    by_id = {block["block_id"]: block for block in blocks}

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
        "provider": "OpenRouter",
        "model_id": MODEL_ID,
        "reasoning_effort": "high",
        "response_healing_initial": True,
        "parallelism": 2,
        "batch_profile": "b6",
        "resolution": "r800",
        "strict_success_checks": True,
        "retry_on_non_success": True,
        "recrop_performed": False,
        "rebuild_blocks_performed": False,
        "new_matrix_performed": False,
        "higher_than_b6_tested": False,
        "resolution_above_r800_tested": False,
        "claude_compared": False,
        "flash_touched": False,
        "production_defaults_changed": False,
        "budget_cap_usd": args.budget_cap_usd,
        "command": " ".join(sys.argv),
    }
    _save_json(out_dir / "manifest.json", manifest)
    _save_md(
        out_dir / "project_resolution.md",
        build_project_resolution_md(
            project_dir=project_dir,
            block_source=resolution.block_source,
            blocks_index_path=resolution.blocks_index_path,
            total_blocks=resolution.total_blocks,
        ),
    )

    historical_ids, historical_sources = load_historical_problematic_ids(project_dir)
    audit_ids, audit_rows = select_audit_set(
        blocks,
        historical_problematic_ids=historical_ids,
    )
    _save_json(out_dir / "audit_set_block_ids.json", audit_ids)
    _save_md(out_dir / "audit_set_manifest.md", build_audit_set_manifest_md(audit_rows, historical_sources))

    prior_per_block_usd, prior_cost_source = find_reference_cost_prior(project_dir)
    schema = load_openrouter_block_batch_schema()
    budget = BudgetTracker(cap_usd=args.budget_cap_usd)

    reusable_reference_map, reusable_reference_sources, inspected_reference_dirs = find_reusable_reference_outputs(
        project_dir,
        audit_ids,
    )
    missing_reference_ids = [block_id for block_id in audit_ids if block_id not in reusable_reference_map]

    ref_estimate = estimate_reference_cost(len(missing_reference_ids), prior_per_block_usd)
    reference_preflight = budget.preflight(
        step_id="reference",
        label="Fill missing audit single-block references",
        predicted_usd=ref_estimate,
    )
    if not reference_preflight.approved:
        _save_md(
            out_dir / "reference_reuse_report.md",
            build_reference_reuse_report(
                prior_run_dirs=inspected_reference_dirs,
                requested_ids=audit_ids,
                reused_ids=sorted(reusable_reference_map),
                missing_ids_before_run=missing_reference_ids,
                newly_run_ids=[],
                final_missing_ids=missing_reference_ids,
            ),
        )
        _save_json(out_dir / "budget_timeline.json", budget.to_dict())
        _save_md(out_dir / "budget_timeline.md", build_budget_timeline_md(budget))
        recommendation = recommend_validation(
            smoke_result={"survived": False, "hard_gate_passed": False, "quality_gate_passed": False},
            full_result=None,
            budget=budget,
        )
        _save_md(
            out_dir / "winner_recommendation.md",
            build_winner_recommendation_md(
                recommendation=recommendation,
                smoke_summary_row={
                    "hard_gate_passed": False,
                    "quality_gate_passed": False,
                    "equivalent_count": 0,
                    "likely_improved_count": 0,
                    "likely_degraded_count": 0,
                    "uncertain_count": 0,
                },
                full_result=None,
                budget=budget,
            ),
        )
        manifest["budget_stop_reason"] = budget.stop_reason
        _save_json(out_dir / "manifest.json", manifest)
        return 0

    reference_results: list[CallResult] = []
    if missing_reference_ids:
        missing_reference_blocks = [by_id[block_id] for block_id in missing_reference_ids]
        reference_groups = [[block] for block in missing_reference_blocks]
        logger.info("Running %d missing single-block audit references", len(reference_groups))
        reference_results, reference_elapsed = await run_call_set(
            project_dir=project_dir,
            blocks_or_batches=reference_groups,
            project_info=project_info,
            reasoning_effort="high",
            response_healing_initial=True,
            schema=schema,
            timeout=DEFAULT_TIMEOUT_SEC,
            max_output_tokens=MAX_OUTPUT_TOKENS,
            parallelism=2,
            run_id="audit_reference",
            resolution="r800",
        )
        reference_actual = sum(result.cost_usd for result in reference_results)
        budget.commit(
            reference_preflight,
            reference_actual,
            note=f"single_block_calls={len(reference_results)} elapsed={reference_elapsed:.1f}s prior={prior_cost_source}",
        )
        new_reference_map = analyses_map_from_results(reference_results)
    else:
        reference_elapsed = 0.0
        new_reference_map = {}
        budget.commit(reference_preflight, 0.0, note="exact audit reference fully reused")

    reference_map = {**reusable_reference_map, **new_reference_map}
    final_missing_reference_ids = [block_id for block_id in audit_ids if block_id not in reference_map]
    _save_md(
        out_dir / "reference_reuse_report.md",
        build_reference_reuse_report(
            prior_run_dirs=inspected_reference_dirs,
            requested_ids=audit_ids,
            reused_ids=sorted(reusable_reference_map),
            missing_ids_before_run=missing_reference_ids,
            newly_run_ids=sorted(new_reference_map),
            final_missing_ids=final_missing_reference_ids,
        ),
    )

    reference_summary_payload = {
        "label": "audit_reference_single_block",
        "model_id": MODEL_ID,
        "mode": "single_block",
        "reasoning_effort": "high",
        "parallelism": 2,
        "response_healing_initial": True,
        "resolution": "r800",
        "requested_block_ids": audit_ids,
        "reused_block_ids": sorted(reusable_reference_map),
        "newly_run_block_ids": sorted(new_reference_map),
        "missing_block_ids": final_missing_reference_ids,
        "elapsed_s": round(reference_elapsed, 6),
        "coverage_pct": round(len(reference_map) / max(1, len(audit_ids)) * 100, 2),
    }
    _save_json(out_dir / "reference_results.json", [asdict(result) for result in reference_results])
    _save_json(out_dir / "reference_analyses.json", reference_map)
    _save_json(out_dir / "reference_run_summary.json", reference_summary_payload)

    if final_missing_reference_ids:
        budget.stopped = True
        budget.stop_reason = "reference fill incomplete for audit set"
        _save_json(out_dir / "budget_timeline.json", budget.to_dict())
        _save_md(out_dir / "budget_timeline.md", build_budget_timeline_md(budget))
        smoke_summary_row = {
            "hard_gate_passed": False,
            "quality_gate_passed": False,
            "survived": False,
            "equivalent_count": 0,
            "likely_improved_count": 0,
            "likely_degraded_count": 0,
            "uncertain_count": 0,
        }
        recommendation = recommend_validation(
            smoke_result=smoke_summary_row,
            full_result=None,
            budget=budget,
        )
        _save_md(
            out_dir / "audit_smoke_summary.md",
            "# Audit Smoke Summary\n\n- Smoke was not executed because the audit reference remained incomplete.\n",
        )
        _save_json(out_dir / "audit_smoke_summary.json", {"performed": False, "reason": "reference_incomplete"})
        _save_csv(out_dir / "audit_smoke_summary.csv", [{"performed": False, "reason": "reference_incomplete"}])
        _save_json(out_dir / "audit_smoke_side_by_side.json", [])
        _save_md(out_dir / "audit_smoke_side_by_side.md", "# Audit Smoke Side-by-Side\n\n- Not available.\n")
        _save_json(out_dir / "confirmatory_full_summary.json", {"performed": False, "reason": "reference_incomplete"})
        _save_md(
            out_dir / "confirmatory_full_summary.md",
            build_confirmatory_full_summary_md({"performed": False, "reason": "reference_incomplete"}),
        )
        _save_md(out_dir / "manual_review_shortlist.md", "# Manual Review Shortlist\n\n- Reference incomplete.\n")
        _save_md(
            out_dir / "winner_recommendation.md",
            build_winner_recommendation_md(
                recommendation=recommendation,
                smoke_summary_row=smoke_summary_row,
                full_result=None,
                budget=budget,
            ),
        )
        manifest["budget_stop_reason"] = budget.stop_reason
        _save_json(out_dir / "manifest.json", manifest)
        return 0

    audit_blocks = [by_id[block_id] for block_id in audit_ids]
    smoke_batches = apply_byte_cap_split(pack_blocks(audit_blocks, BATCH_PROFILES["b6"], hard_cap=12), byte_cap_kb=9000)
    smoke_estimate = estimate_smoke_cost(len(audit_ids), prior_per_block_usd)
    smoke_preflight = budget.preflight(
        step_id="audit_smoke",
        label="Audit smoke batch b6 r800",
        predicted_usd=smoke_estimate,
    )
    if not smoke_preflight.approved:
        _save_json(out_dir / "budget_timeline.json", budget.to_dict())
        _save_md(out_dir / "budget_timeline.md", build_budget_timeline_md(budget))
        smoke_summary_row = {
            "hard_gate_passed": False,
            "quality_gate_passed": False,
            "survived": False,
            "equivalent_count": 0,
            "likely_improved_count": 0,
            "likely_degraded_count": 0,
            "uncertain_count": 0,
        }
        recommendation = recommend_validation(
            smoke_result=smoke_summary_row,
            full_result=None,
            budget=budget,
        )
        _save_json(out_dir / "audit_smoke_summary.json", {"performed": False, "reason": budget.stop_reason})
        _save_csv(out_dir / "audit_smoke_summary.csv", [{"performed": False, "reason": budget.stop_reason}])
        _save_md(
            out_dir / "audit_smoke_summary.md",
            "# Audit Smoke Summary\n\n- Smoke was not executed because the predicted cost exceeded remaining budget.\n",
        )
        _save_json(out_dir / "audit_smoke_side_by_side.json", [])
        _save_md(out_dir / "audit_smoke_side_by_side.md", "# Audit Smoke Side-by-Side\n\n- Not available.\n")
        _save_json(out_dir / "confirmatory_full_summary.json", {"performed": False, "reason": "smoke_budget_stop"})
        _save_md(
            out_dir / "confirmatory_full_summary.md",
            build_confirmatory_full_summary_md({"performed": False, "reason": "smoke_budget_stop"}),
        )
        _save_md(out_dir / "manual_review_shortlist.md", "# Manual Review Shortlist\n\n- Smoke was not executed.\n")
        _save_md(
            out_dir / "winner_recommendation.md",
            build_winner_recommendation_md(
                recommendation=recommendation,
                smoke_summary_row=smoke_summary_row,
                full_result=None,
                budget=budget,
            ),
        )
        manifest["budget_stop_reason"] = budget.stop_reason
        _save_json(out_dir / "manifest.json", manifest)
        return 0

    logger.info("Running audit smoke on %d blocks across %d batches", len(audit_ids), len(smoke_batches))
    smoke_results, smoke_elapsed = await run_call_set(
        project_dir=project_dir,
        blocks_or_batches=smoke_batches,
        project_info=project_info,
        reasoning_effort="high",
        response_healing_initial=True,
        schema=schema,
        timeout=DEFAULT_TIMEOUT_SEC,
        max_output_tokens=MAX_OUTPUT_TOKENS,
        parallelism=2,
        run_id="audit_smoke_b6",
        resolution="r800",
    )
    smoke_actual = sum(result.cost_usd for result in smoke_results)
    budget.commit(smoke_preflight, smoke_actual, note=f"batches={len(smoke_results)} elapsed={smoke_elapsed:.1f}s")

    smoke_summary = aggregate_results(
        results=smoke_results,
        input_block_ids=audit_ids,
        elapsed_s=smoke_elapsed,
        label="audit smoke b6 r800",
        mode="batch",
        reasoning_effort="high",
        parallelism=2,
        resolution="r800",
    )
    smoke_candidate_map = analyses_map_from_results(smoke_results)
    smoke_side = build_side_by_side(
        block_ids=audit_ids,
        reference_map=reference_map,
        candidate_map=smoke_candidate_map,
        phase="audit_smoke",
        run_id="b6_smoke",
    )
    smoke_side, smoke_quality = enrich_quality(
        side_by_side=smoke_side,
        block_ids=audit_ids,
        reference_map=reference_map,
        summary=smoke_summary,
    )
    smoke_summary_row = flatten_smoke_summary(
        summary=smoke_summary,
        quality=smoke_quality,
        survived=hard_gate_passed(smoke_summary) and smoke_quality.get("quality_gate_passed", False),
        estimated_cost_usd=smoke_estimate,
    )

    _save_json(
        out_dir / "audit_smoke_summary.json",
        {
            "summary": smoke_summary,
            "summary_row": smoke_summary_row,
            "quality": smoke_quality,
            "batch_count": len(smoke_batches),
            "audit_block_count": len(audit_ids),
        },
    )
    _save_csv(out_dir / "audit_smoke_summary.csv", [smoke_summary_row])
    _save_md(out_dir / "audit_smoke_summary.md", build_audit_smoke_summary_md(summary_row=smoke_summary_row, quality=smoke_quality))
    _save_json(out_dir / "audit_smoke_side_by_side.json", smoke_side)
    _save_md(out_dir / "audit_smoke_side_by_side.md", build_smoke_side_by_side_md(smoke_side))

    manual_review_entries: list[dict] = [
        {
            "block_id": row["block_id"],
            "context": "audit_smoke",
            "verdict": row["verdict"],
            "reasons": row["reasons"],
        }
        for row in smoke_side
        if row["verdict"] != "equivalent"
    ]

    should_run_full, smoke_stop_reason = should_continue_after_smoke(smoke_summary_row)
    if not should_run_full:
        budget.record_skip(
            step_id="confirmatory_full",
            label="Full confirmatory b6 r800",
            note=f"early_stop: {smoke_stop_reason}",
        )
        full_result = {"performed": False, "reason": smoke_stop_reason}
        recommendation = recommend_validation(
            smoke_result=smoke_summary_row,
            full_result=full_result,
            budget=budget,
        )
        _save_json(out_dir / "confirmatory_full_summary.json", full_result)
        _save_md(out_dir / "confirmatory_full_summary.md", build_confirmatory_full_summary_md(full_result))
        _save_md(out_dir / "manual_review_shortlist.md", build_manual_review_shortlist_md(manual_review_entries))
        _save_json(out_dir / "budget_timeline.json", budget.to_dict())
        _save_md(out_dir / "budget_timeline.md", build_budget_timeline_md(budget))
        _save_md(
            out_dir / "winner_recommendation.md",
            build_winner_recommendation_md(
                recommendation=recommendation,
                smoke_summary_row=smoke_summary_row,
                full_result=full_result,
                budget=budget,
            ),
        )
        manifest["smoke_stop_reason"] = smoke_stop_reason
        manifest["full_run_performed"] = False
        _save_json(out_dir / "manifest.json", manifest)
        return 0

    full_batches = apply_byte_cap_split(pack_blocks(blocks, BATCH_PROFILES["b6"], hard_cap=12), byte_cap_kb=9000)
    full_estimate = estimate_full_cost(resolution.total_blocks, smoke_actual, len(audit_ids))
    full_preflight = budget.preflight(
        step_id="confirmatory_full",
        label="Full confirmatory b6 r800",
        predicted_usd=full_estimate,
    )
    if not full_preflight.approved:
        full_result = {"performed": False, "reason": budget.stop_reason}
        recommendation = recommend_validation(
            smoke_result=smoke_summary_row,
            full_result=full_result,
            budget=budget,
        )
        _save_json(out_dir / "confirmatory_full_summary.json", full_result)
        _save_md(out_dir / "confirmatory_full_summary.md", build_confirmatory_full_summary_md(full_result))
        _save_md(out_dir / "manual_review_shortlist.md", build_manual_review_shortlist_md(manual_review_entries))
        _save_json(out_dir / "budget_timeline.json", budget.to_dict())
        _save_md(out_dir / "budget_timeline.md", build_budget_timeline_md(budget))
        _save_md(
            out_dir / "winner_recommendation.md",
            build_winner_recommendation_md(
                recommendation=recommendation,
                smoke_summary_row=smoke_summary_row,
                full_result=full_result,
                budget=budget,
            ),
        )
        manifest["budget_stop_reason"] = budget.stop_reason
        manifest["full_run_performed"] = False
        _save_json(out_dir / "manifest.json", manifest)
        return 0

    logger.info("Running full confirmatory b6+r800 on %d blocks across %d batches", resolution.total_blocks, len(full_batches))
    full_results, full_elapsed = await run_call_set(
        project_dir=project_dir,
        blocks_or_batches=full_batches,
        project_info=project_info,
        reasoning_effort="high",
        response_healing_initial=True,
        schema=schema,
        timeout=DEFAULT_TIMEOUT_SEC,
        max_output_tokens=MAX_OUTPUT_TOKENS,
        parallelism=2,
        run_id="confirmatory_full_b6",
        resolution="r800",
    )
    full_actual = sum(result.cost_usd for result in full_results)
    budget.commit(full_preflight, full_actual, note=f"batches={len(full_results)} elapsed={full_elapsed:.1f}s")

    full_summary = aggregate_results(
        results=full_results,
        input_block_ids=[block["block_id"] for block in blocks],
        elapsed_s=full_elapsed,
        label="confirmatory full b6 r800",
        mode="batch",
        reasoning_effort="high",
        parallelism=2,
        resolution="r800",
    )
    full_candidate_map = analyses_map_from_results(full_results)
    full_audit_side = build_side_by_side(
        block_ids=audit_ids,
        reference_map=reference_map,
        candidate_map=full_candidate_map,
        phase="confirmatory_full",
        run_id="b6_full",
    )
    full_audit_side, full_audit_quality = enrich_quality(
        side_by_side=full_audit_side,
        block_ids=audit_ids,
        reference_map=reference_map,
        summary={
            **full_summary,
            "total_findings": sum(len((full_candidate_map.get(block_id, {}).get("findings") or [])) for block_id in audit_ids),
            "blocks_with_findings": sum(1 for block_id in audit_ids if (full_candidate_map.get(block_id, {}).get("findings") or [])),
            "total_key_values": sum(len((full_candidate_map.get(block_id, {}).get("key_values_read") or [])) for block_id in audit_ids),
        },
    )

    full_retry_block_ids = sorted(
        {
            block_id
            for result in full_results
            if result.retry_attempted or result.final_status != "success"
            for block_id in result.input_block_ids
        }
    )
    for row in full_audit_side:
        if row["verdict"] != "equivalent":
            manual_review_entries.append(
                {
                    "block_id": row["block_id"],
                    "context": "confirmatory_full_audit",
                    "verdict": row["verdict"],
                    "reasons": row["reasons"],
                }
            )
    for block_id in full_retry_block_ids:
        manual_review_entries.append(
            {
                "block_id": block_id,
                "context": "confirmatory_full_retry",
                "verdict": "uncertain",
                "reasons": ["retry_triggered_or_non_success"],
            }
        )

    dedup_entries: list[dict] = []
    seen_entry_keys: set[tuple[str, str]] = set()
    for entry in manual_review_entries:
        key = (entry["context"], entry["block_id"])
        if key in seen_entry_keys:
            continue
        seen_entry_keys.add(key)
        dedup_entries.append(entry)

    full_result = {
        "performed": True,
        "summary": full_summary,
        "hard_gate_passed": hard_gate_passed(full_summary),
        "audit_quality": full_audit_quality,
        "audit_quality_gate_passed": bool(full_audit_quality.get("quality_gate_passed")),
        "audit_side_by_side": full_audit_side,
        "manual_review_block_ids": sorted({entry["block_id"] for entry in dedup_entries}),
    }
    recommendation = recommend_validation(
        smoke_result=smoke_summary_row,
        full_result=full_result,
        budget=budget,
    )

    _save_json(out_dir / "confirmatory_full_summary.json", full_result)
    _save_md(out_dir / "confirmatory_full_summary.md", build_confirmatory_full_summary_md(full_result))
    _save_md(out_dir / "manual_review_shortlist.md", build_manual_review_shortlist_md(dedup_entries))
    _save_json(out_dir / "budget_timeline.json", budget.to_dict())
    _save_md(out_dir / "budget_timeline.md", build_budget_timeline_md(budget))
    _save_md(
        out_dir / "winner_recommendation.md",
        build_winner_recommendation_md(
            recommendation=recommendation,
            smoke_summary_row=smoke_summary_row,
            full_result=full_result,
            budget=budget,
        ),
    )

    manifest.update(
        {
            "audit_set_size": len(audit_ids),
            "historical_problematic_source_count": len(historical_sources),
            "reference_prior_cost_per_block_usd": prior_per_block_usd,
            "reference_prior_cost_source": prior_cost_source,
            "smoke_survived": smoke_summary_row["survived"],
            "full_run_performed": full_result["performed"],
            "full_run_survived": recommendation["full_run_survived"],
            "additional_spend_usd": budget.spent_usd,
            "budget_stop_reason": budget.stop_reason,
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
