"""
Cheap upper-bound experiment for Gemini 3.1 Pro on stage 02 via OpenRouter.

Goal:
  Find the practical upper bound above `b6` on the small KJ project without
  doing a full-document run and without changing production defaults.

Hard constraints:
  - reuse the existing stress set from the latest `pro_variantA_small_budget`
  - reuse the existing Pro single-block reference (`pro_high_p2`) when valid
  - fixed config only: OpenRouter / google-gemini-3.1-pro-preview / high /
    healing ON / parallelism=2 / r800 / strict completeness checks
  - check only: b6 (control), b8, b10
  - launch b10 only if b8 survives hard + quality gate
  - hard additional spend cap: USD 2.00
  - no recrop / no rebuild blocks / same PNG assets from the project `_output`

Artifacts:
  <project_dir>/_experiments/pro_batch_upper_bound_small/<timestamp>/
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
    load_blocks_index,
    pack_blocks,
)
from run_pro_variantA_small_budget import (  # noqa: E402
    CallResult,
    aggregate_results,
    analyses_map_from_results,
    build_budget_timeline_md,
    build_side_by_side,
    estimate_step_cost,
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
logger = logging.getLogger("pro_batch_upper_bound_small")


PDF_NAME = "13АВ-РД-КЖ5.1-К1К2 (2).pdf"
PROJECT_REL = f"projects/214. Alia (ASTERUS)/KJ/{PDF_NAME}"
MODEL_ID = "google/gemini-3.1-pro-preview"
REFERENCE_EXP_NAME = "pro_second_pass_reeval_small_project"
REFERENCE_CONFIG_ID = "pro_high_p2"
VARIANT_A_EXP_NAME = "pro_variantA_small_budget"
DEFAULT_TIMEOUT_SEC = 600
MAX_OUTPUT_TOKENS = 32768
DEFAULT_BUDGET_CAP_USD = 2.00
PROFILE_ORDER = ("b6", "b8", "b10")


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


def _normalize_variant_a_manifest(manifest: dict) -> dict:
    out = dict(manifest)
    out["pdf"] = str(out.get("pdf") or "")
    out["project_dir"] = str(out.get("project_dir") or "")
    out["reference_dir_reused"] = str(out.get("reference_dir_reused") or "")
    out["stress_set_size"] = int(out.get("stress_set_size") or 0)
    return out


def find_latest_variant_a_dir(project_dir: Path) -> Path | None:
    root = project_dir / "_experiments" / VARIANT_A_EXP_NAME
    if not root.exists():
        return None
    candidates = sorted(
        [
            p for p in root.iterdir()
            if p.is_dir()
            and (p / "manifest.json").exists()
            and (p / "stress_set_block_ids.json").exists()
            and (p / "b6_batch_results.json").exists()
        ],
        key=lambda p: p.name,
        reverse=True,
    )
    return candidates[0] if candidates else None


def validate_variant_a_dir(project_dir: Path, variant_dir: Path) -> dict:
    manifest = _normalize_variant_a_manifest(_load_json(variant_dir / "manifest.json"))
    errors: list[str] = []
    manifest_project_dir = Path(manifest["project_dir"])
    if not manifest_project_dir.is_absolute():
        manifest_project_dir = (_ROOT / manifest_project_dir).resolve()

    if manifest["pdf"] != PDF_NAME:
        errors.append(f"pdf mismatch: {manifest['pdf']!r}")
    if manifest_project_dir != project_dir.resolve():
        errors.append(f"project_dir mismatch: {manifest['project_dir']!r}")
    if manifest["stress_set_size"] != 8:
        errors.append(f"unexpected stress_set_size={manifest['stress_set_size']}")
    if manifest.get("openrouter_only") is not True:
        errors.append("openrouter_only flag is not true")
    if manifest.get("production_defaults_changed") is not False:
        errors.append("production_defaults_changed is not false")
    if manifest.get("claude_touched") is not False:
        errors.append("claude_touched is not false")
    if manifest.get("flash_touched") is not False:
        errors.append("flash_touched is not false")

    if errors:
        raise RuntimeError(
            "Latest Variant A run is not a valid reuse source:\n- "
            + "\n- ".join(errors)
        )
    return manifest


def load_reused_stress_set(project_dir: Path) -> tuple[Path, dict, list[str], str]:
    variant_dir = find_latest_variant_a_dir(project_dir)
    if variant_dir is None:
        raise RuntimeError(
            "No reusable Variant A run found. Need existing "
            "`_experiments/pro_variantA_small_budget/<timestamp>/` with stress set + b6 control."
        )
    manifest = validate_variant_a_dir(project_dir, variant_dir)
    stress_ids = _load_json(variant_dir / "stress_set_block_ids.json")
    if not isinstance(stress_ids, list) or len(stress_ids) != 8:
        raise RuntimeError("Variant A stress_set_block_ids.json is invalid or not 8 blocks")
    source_manifest_md = (variant_dir / "stress_set_manifest.md").read_text(encoding="utf-8")
    return variant_dir, manifest, stress_ids, source_manifest_md


def find_latest_reference_dir(project_dir: Path) -> Path | None:
    root = project_dir / "_experiments" / REFERENCE_EXP_NAME
    if not root.exists():
        return None
    candidates = sorted(
        [
            p for p in root.iterdir()
            if p.is_dir()
            and (p / f"{REFERENCE_CONFIG_ID}_summary.json").exists()
            and (p / f"{REFERENCE_CONFIG_ID}_per_block.json").exists()
        ],
        key=lambda p: p.name,
        reverse=True,
    )
    return candidates[0] if candidates else None


def load_reference_outputs(project_dir: Path, requested_ids: list[str]) -> tuple[Path, dict[str, dict], dict]:
    reference_dir = find_latest_reference_dir(project_dir)
    if reference_dir is None:
        raise RuntimeError(
            "No valid Pro single-block reference found under "
            f"`_experiments/{REFERENCE_EXP_NAME}`."
        )

    summary = _load_json(reference_dir / f"{REFERENCE_CONFIG_ID}_summary.json")
    rows = _load_json(reference_dir / f"{REFERENCE_CONFIG_ID}_per_block.json")
    if summary.get("coverage_pct") != 100.0:
        raise RuntimeError(
            f"Reference summary is not 100% complete: coverage={summary.get('coverage_pct')}"
        )
    if any(summary.get(key) != 0 for key in ("missing_count", "duplicate_count", "extra_count")):
        raise RuntimeError("Reference summary has missing/duplicate/extra blocks")
    if summary.get("reasoning_effort") != "high":
        raise RuntimeError("Reference summary is not high reasoning")
    if summary.get("parallelism") != 2:
        raise RuntimeError("Reference summary is not parallelism=2")
    if summary.get("response_healing_initial") is not True:
        raise RuntimeError("Reference summary is not healing ON")

    reference_map: dict[str, dict] = {}
    for row in rows:
        if row.get("success") and row.get("parsed_block"):
            reference_map[row["block_id"]] = row["parsed_block"]

    missing = [bid for bid in requested_ids if bid not in reference_map]
    if missing:
        raise RuntimeError(
            "Reference is incomplete for reused stress set: missing "
            + ", ".join(missing)
        )
    return reference_dir, reference_map, summary


def load_reused_control(variant_dir: Path, stress_ids: list[str]) -> tuple[list[CallResult], dict]:
    summary_payload = _load_json(variant_dir / "b6_batch_summary.json")
    summary = summary_payload["summary"]
    if summary.get("reasoning_effort") != "high":
        raise RuntimeError("Reused b6 control is not high reasoning")
    if summary.get("parallelism") != 2:
        raise RuntimeError("Reused b6 control is not parallelism=2")
    if summary.get("resolution") != "r800":
        raise RuntimeError("Reused b6 control is not r800")
    if summary.get("total_blocks") != len(stress_ids):
        raise RuntimeError("Reused b6 control uses a different stress-set size")

    rows = _load_json(variant_dir / "b6_batch_results.json")
    results = [CallResult(**row) for row in rows]
    result_ids = sorted({
        bid
        for res in results
        for bid in res.input_block_ids
    })
    if sorted(stress_ids) != result_ids:
        raise RuntimeError("Reused b6 control does not cover the exact reused stress set")
    return results, summary_payload


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
        annotated.append({
            **row,
            "verdict": verdict,
        })
    return annotated


def build_stress_set_reuse_report(
    *,
    variant_dir: Path,
    variant_manifest: dict,
    stress_ids: list[str],
    source_manifest_md: str,
    reference_dir: Path,
    reference_summary: dict,
    control_summary: dict,
) -> str:
    lines = [
        "# Stress Set Reuse Report\n",
        f"- Reused Variant A source: `{variant_dir}`",
        f"- Source manifest timestamp: `{variant_manifest['timestamp']}`",
        f"- PDF: `{variant_manifest['pdf']}`",
        f"- Project dir: `{variant_manifest['project_dir']}`",
        f"- Stress set size: {len(stress_ids)}",
        f"- Reuse enforced: yes",
        f"- Exact block IDs: {', '.join(f'`{bid}`' for bid in stress_ids)}",
        "",
        "## Reused Pro single-block reference",
        f"- Reference dir: `{reference_dir}`",
        f"- Config: `{reference_summary['config_id']}`",
        f"- Coverage: {reference_summary['coverage_pct']:.2f}%",
        f"- Missing / Duplicate / Extra: {reference_summary['missing_count']} / "
        f"{reference_summary['duplicate_count']} / {reference_summary['extra_count']}",
        "",
        "## Reused b6 control",
        f"- Source: `{variant_dir / 'b6_batch_results.json'}`",
        f"- Coverage: {control_summary['coverage_pct']:.2f}%",
        f"- Missing / Duplicate / Extra: {control_summary['missing_count']} / "
        f"{control_summary['duplicate_count']} / {control_summary['extra_count']}",
        f"- Parallelism: {control_summary['parallelism']}",
        f"- Resolution: `{control_summary['resolution']}`",
        "",
        "## Source Stress Manifest",
        source_manifest_md.strip(),
        "",
    ]
    return "\n".join(lines)


def profile_batches_for_stress(stress_blocks: list[dict], profile_id: str) -> list[list[dict]]:
    if profile_id not in BATCH_PROFILES:
        raise KeyError(f"Unknown profile: {profile_id}")
    batches = pack_blocks(stress_blocks, BATCH_PROFILES[profile_id], hard_cap=12)
    return apply_byte_cap_split(batches, byte_cap_kb=9000)


def hard_gate_passed(summary: dict) -> bool:
    return (
        summary.get("coverage_pct") == 100.0
        and summary.get("missing_count") == 0
        and summary.get("duplicate_count") == 0
        and summary.get("extra_count") == 0
        and not summary.get("fail_mode_distribution")
    )


def should_launch_profile(rows: list[dict], next_profile: str) -> tuple[bool, str]:
    by_id = {row["profile_id"]: row for row in rows}
    if next_profile == "b6":
        return True, ""
    if next_profile == "b8":
        control = by_id.get("b6")
        if not control or not control.get("survived"):
            return False, "b6 control did not survive"
        return True, ""
    if next_profile == "b10":
        b8 = by_id.get("b8")
        if not b8:
            return False, "b8 was not launched"
        if not b8.get("survived"):
            return False, "b8 did not survive"
        return True, ""
    return False, f"unknown profile {next_profile}"


def recommend_upper_bound(rows: list[dict]) -> dict:
    by_id = {row["profile_id"]: row for row in rows}
    b6 = by_id.get("b6")
    b8 = by_id.get("b8")
    b10 = by_id.get("b10")

    practical_upper_bound = "b6"
    degradation_starts_at = "b8"
    go_higher = False

    if b10 and b10.get("launched") and b10.get("survived"):
        practical_upper_bound = "b10"
        degradation_starts_at = "not observed up to b10"
    elif b8 and b8.get("launched") and b8.get("survived"):
        practical_upper_bound = "b8"
        if b10 and b10.get("launched") and not b10.get("survived"):
            degradation_starts_at = "b10"
        elif b10 and not b10.get("launched"):
            degradation_starts_at = "not established beyond b8"
        else:
            degradation_starts_at = "not observed up to b8"
    elif b8 and b8.get("launched") and not b8.get("survived"):
        practical_upper_bound = "b6"
        degradation_starts_at = "b8"
    elif b6 and b6.get("survived"):
        practical_upper_bound = "b6"
        degradation_starts_at = "not established above b6"

    return {
        "survived_b8": bool(b8 and b8.get("survived")),
        "launched_b10": bool(b10 and b10.get("launched")),
        "survived_b10": bool(b10 and b10.get("survived")),
        "degradation_starts_at": degradation_starts_at,
        "practical_upper_bound": practical_upper_bound,
        "go_higher_now": go_higher,
    }


def build_batch_upper_bound_summary_md(rows: list[dict]) -> str:
    lines = [
        "# Batch Upper Bound Summary\n",
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


def build_winner_recommendation_md(
    *,
    recommendation: dict,
    rows: list[dict],
    budget_cap_usd: float,
    budget_spent_usd: float,
) -> str:
    by_id = {row["profile_id"]: row for row in rows}
    b8 = by_id.get("b8")
    b10 = by_id.get("b10")

    lines = [
        "# Winner Recommendation\n",
        f"1. `b8` survived: {'YES' if recommendation['survived_b8'] else 'NO'}",
        f"2. `b10` launched: {'YES' if recommendation['launched_b10'] else 'NO'}",
        f"3. `b10` survived: {'YES' if recommendation['survived_b10'] else 'NO'}",
        f"4. Degradation starts at: `{recommendation['degradation_starts_at']}`",
        f"5. Practical upper bound now: `{recommendation['practical_upper_bound']}`",
        f"6. Go higher now: {'YES' if recommendation['go_higher_now'] else 'NO'}",
        "",
        "## Why",
    ]
    if recommendation["practical_upper_bound"] == "b6":
        lines.append("- `b6` remains the safest practical ceiling for this stress set.")
    elif recommendation["practical_upper_bound"] == "b8":
        lines.append("- `b8` survived the upper-bound gate and is the current practical ceiling.")
    else:
        lines.append("- `b10` survived the upper-bound gate and is the current practical ceiling.")

    if b8:
        lines.append(
            f"- `b8`: launched={b8['launched']}, survived={b8['survived']}, "
            f"coverage={b8['coverage_pct']:.2f}%, degraded={b8['likely_degraded_count']}."
        )
    if b10:
        lines.append(
            f"- `b10`: launched={b10['launched']}, survived={b10['survived']}, "
            f"coverage={b10['coverage_pct']:.2f}%, degraded={b10['likely_degraded_count']}."
        )

    lines.extend([
        "",
        "## Budget",
        f"- Cap: ${budget_cap_usd:.2f}",
        f"- Additional spend: ${budget_spent_usd:.4f}",
        "",
        "## Recommendation",
    ])
    if recommendation["practical_upper_bound"] == "b10":
        lines.append("- Adopt `b10` only for this narrow validated configuration; do not jump to `b12+` on this step.")
    elif recommendation["practical_upper_bound"] == "b8":
        lines.append("- Prefer `b8`; do not go higher until a separate capped follow-up is justified.")
    else:
        lines.append("- Keep `b6`; higher batch sizes are not justified by this experiment.")
    lines.append("")
    return "\n".join(lines)


def make_summary_row(
    *,
    profile_id: str,
    source: str,
    launched: bool,
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
        "quality_gate_passed": quality.get("quality_gate_passed", False),
        "survived": hard_gate_passed(summary) and quality.get("quality_gate_passed", False),
        "coverage_pct": float(summary.get("coverage_pct", 0.0) or 0.0),
        "missing_count": int(summary.get("missing_count", 0) or 0),
        "duplicate_count": int(summary.get("duplicate_count", 0) or 0),
        "extra_count": int(summary.get("extra_count", 0) or 0),
        "retry_triggered_count": int(summary.get("retry_triggered_count", 0) or 0),
        "retry_recovered_count": int(summary.get("retry_recovered_count", 0) or 0),
        "blocks_with_findings": int(summary.get("blocks_with_findings", 0) or 0),
        "total_findings": int(summary.get("total_findings", 0) or 0),
        "total_key_values": int(summary.get("total_key_values", 0) or 0),
        "elapsed_s": float(summary.get("elapsed_s", 0.0) or 0.0),
        "likely_equivalent_count": int(quality.get("likely_equivalent_count", 0) or 0),
        "likely_improved_count": int(quality.get("likely_improved_count", 0) or 0),
        "likely_degraded_count": int(quality.get("likely_degraded_count", 0) or 0),
        "equivalent_count": int(verdicts.get("equivalent", 0) or 0),
        "uncertain_count": int(verdicts.get("uncertain", 0) or 0),
        "total_findings_delta": int(quality.get("total_findings_delta", 0) or 0),
        "blocks_with_findings_delta": int(quality.get("blocks_with_findings_delta", 0) or 0),
        "total_kv_delta": int(quality.get("total_kv_delta", 0) or 0),
        "summary_specificity_improved_count": int(quality.get("summary_specificity_improved_count", 0) or 0),
        "kv_adequacy_improved_count": int(quality.get("kv_adequacy_improved_count", 0) or 0),
        "manual_review_block_ids": list(quality.get("manual_review_block_ids", [])),
        "hard_fail_reasons": list(quality.get("hard_fail_reasons", [])),
        "estimated_cost_usd": round(float(estimated_cost_usd or 0.0), 6),
        "actual_cost_usd": round(float(actual_cost_usd or 0.0), 6),
        "note": note,
    }


def enrich_quality(
    *,
    side_by_side: list[dict],
    stress_ids: list[str],
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
    quality = {
        **quality,
        "verdict_counts": verdict_counts,
        "total_findings_delta": summary["total_findings"] - sum(
            len(reference_map[bid].get("findings") or []) for bid in stress_ids
        ),
        "blocks_with_findings_delta": summary["blocks_with_findings"] - sum(
            1 for bid in stress_ids if reference_map[bid].get("findings")
        ),
        "total_kv_delta": summary["total_key_values"] - sum(
            len(reference_map[bid].get("key_values_read") or []) for bid in stress_ids
        ),
    }
    return annotated, quality


async def main_async(args: argparse.Namespace) -> int:
    resolution = resolve_project(args.pdf, Path(args.project_dir) if args.project_dir else None)
    project_dir = resolution.project_dir
    project_info = _load_json(project_dir / "project_info.json")
    all_blocks = resolution.blocks or load_blocks_index(project_dir)
    by_id = {b["block_id"]: b for b in all_blocks}

    ts = _ts()
    out_dir = project_dir / "_experiments" / "pro_batch_upper_bound_small" / ts
    out_dir.mkdir(parents=True, exist_ok=True)

    variant_dir, variant_manifest, stress_ids, source_manifest_md = load_reused_stress_set(project_dir)
    missing_from_index = [bid for bid in stress_ids if bid not in by_id]
    if missing_from_index:
        raise RuntimeError(
            "Reused stress set does not match current blocks/index.json: missing "
            + ", ".join(missing_from_index)
        )
    stress_blocks = [by_id[bid] for bid in stress_ids]

    reference_dir, reference_map, reference_summary = load_reference_outputs(project_dir, stress_ids)
    b6_control_results, b6_control_payload = load_reused_control(variant_dir, stress_ids)

    manifest = {
        "timestamp": ts,
        "pdf": args.pdf,
        "project_dir": str(project_dir),
        "block_source": resolution.block_source,
        "blocks_index": str(resolution.blocks_index_path),
        "total_blocks": resolution.total_blocks,
        "model_id": MODEL_ID,
        "provider": "OpenRouter",
        "reasoning_effort": "high",
        "response_healing_initial": True,
        "parallelism": 2,
        "resolution": "r800",
        "strict_success_checks": True,
        "retry_on_non_success": True,
        "reuse_variant_a_dir": str(variant_dir),
        "reuse_reference_dir": str(reference_dir),
        "budget_cap_usd": args.budget_cap_usd,
        "stress_set_size": len(stress_ids),
        "profiles": list(PROFILE_ORDER),
        "recrop_performed": False,
        "rebuild_blocks_performed": False,
        "full_document_run_performed": False,
        "claude_compared": False,
        "flash_touched": False,
        "production_defaults_changed": False,
    }
    _save_json(out_dir / "manifest.json", manifest)

    (out_dir / "stress_set_reuse_report.md").write_text(
        build_stress_set_reuse_report(
            variant_dir=variant_dir,
            variant_manifest=variant_manifest,
            stress_ids=stress_ids,
            source_manifest_md=source_manifest_md,
            reference_dir=reference_dir,
            reference_summary=reference_summary,
            control_summary=b6_control_payload["summary"],
        ),
        encoding="utf-8",
    )

    schema = load_openrouter_block_batch_schema()
    summary_rows: list[dict] = []
    per_block_rows: list[dict] = []
    manual_review_rows: list[dict] = []
    budget = BudgetTracker(cap_usd=args.budget_cap_usd)

    # b6 control is explicitly reused.
    b6_summary = aggregate_results(
        results=b6_control_results,
        input_block_ids=stress_ids,
        elapsed_s=float(b6_control_payload["summary"]["elapsed_s"]),
        label="b6 control (reused)",
        mode="batch",
        reasoning_effort="high",
        parallelism=2,
        resolution="r800",
    )
    b6_candidate_map = analyses_map_from_results(b6_control_results)
    b6_side = build_side_by_side(
        block_ids=stress_ids,
        reference_map=reference_map,
        candidate_map=b6_candidate_map,
        phase="upper_bound",
        run_id="b6",
    )
    b6_side, b6_quality = enrich_quality(
        side_by_side=b6_side,
        stress_ids=stress_ids,
        reference_map=reference_map,
        summary=b6_summary,
    )
    summary_rows.append(
        make_summary_row(
            profile_id="b6",
            source="reused_control",
            launched=False,
            summary=b6_summary,
            quality=b6_quality,
            estimated_cost_usd=0.0,
            actual_cost_usd=0.0,
            note=f"reused from {variant_dir.name}",
        )
    )
    per_block_rows.extend(b6_side)
    manual_review_rows.extend([row for row in b6_side if row["verdict"] != "equivalent"])
    budget.record_skip(
        step_id="control_b6",
        label="Reuse b6 control",
        note=f"reused from {variant_dir}",
    )

    for profile_id in ("b8", "b10"):
        allowed, reason = should_launch_profile(summary_rows, profile_id)
        if not allowed:
            empty_summary = {
                "coverage_pct": 0.0,
                "missing_count": len(stress_ids),
                "duplicate_count": 0,
                "extra_count": 0,
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
                "likely_equivalent_count": 0,
                "likely_improved_count": 0,
                "likely_degraded_count": 0,
                "summary_specificity_improved_count": 0,
                "kv_adequacy_improved_count": 0,
                "manual_review_block_ids": [],
                "verdict_counts": {
                    "equivalent": 0,
                    "likely improved": 0,
                    "likely degraded": 0,
                    "uncertain": 0,
                },
                "total_findings_delta": 0,
                "blocks_with_findings_delta": 0,
                "total_kv_delta": 0,
            }
            summary_rows.append(
                make_summary_row(
                    profile_id=profile_id,
                    source="skipped",
                    launched=False,
                    summary=empty_summary,
                    quality=empty_quality,
                    estimated_cost_usd=0.0,
                    actual_cost_usd=0.0,
                    note=f"early_stop: {reason}",
                )
            )
            budget.record_skip(
                step_id=profile_id,
                label=f"Run {profile_id}",
                note=f"early_stop: {reason}",
            )
            continue

        predicted = estimate_step_cost(
            mode="batch",
            reasoning_effort="high",
            resolution="r800",
            block_count=len(stress_ids),
        )
        event = budget.preflight(
            step_id=profile_id,
            label=f"Run {profile_id}",
            predicted_usd=predicted,
        )
        if not event.approved:
            empty_summary = {
                "coverage_pct": 0.0,
                "missing_count": len(stress_ids),
                "duplicate_count": 0,
                "extra_count": 0,
                "retry_triggered_count": 0,
                "retry_recovered_count": 0,
                "blocks_with_findings": 0,
                "total_findings": 0,
                "total_key_values": 0,
                "elapsed_s": 0.0,
                "fail_mode_distribution": {"budget_stop": 1},
            }
            empty_quality = {
                "quality_gate_passed": False,
                "hard_fail_reasons": [event.note],
                "likely_equivalent_count": 0,
                "likely_improved_count": 0,
                "likely_degraded_count": 0,
                "summary_specificity_improved_count": 0,
                "kv_adequacy_improved_count": 0,
                "manual_review_block_ids": [],
                "verdict_counts": {
                    "equivalent": 0,
                    "likely improved": 0,
                    "likely degraded": 0,
                    "uncertain": 0,
                },
                "total_findings_delta": 0,
                "blocks_with_findings_delta": 0,
                "total_kv_delta": 0,
            }
            summary_rows.append(
                make_summary_row(
                    profile_id=profile_id,
                    source="budget_stop",
                    launched=False,
                    summary=empty_summary,
                    quality=empty_quality,
                    estimated_cost_usd=predicted,
                    actual_cost_usd=0.0,
                    note=event.note,
                )
            )
            break

        batches = profile_batches_for_stress(stress_blocks, profile_id)
        logger.info("Running %s on %d stress blocks across %d batches", profile_id, len(stress_ids), len(batches))
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
        actual = sum(r.cost_usd for r in results)
        budget.commit(event, actual, note=f"calls={len(results)} elapsed={elapsed:.1f}s")

        summary = aggregate_results(
            results=results,
            input_block_ids=stress_ids,
            elapsed_s=elapsed,
            label=f"{profile_id} upper bound",
            mode="batch",
            reasoning_effort="high",
            parallelism=2,
            resolution="r800",
        )
        candidate_map = analyses_map_from_results(results)
        side = build_side_by_side(
            block_ids=stress_ids,
            reference_map=reference_map,
            candidate_map=candidate_map,
            phase="upper_bound",
            run_id=profile_id,
        )
        side, quality = enrich_quality(
            side_by_side=side,
            stress_ids=stress_ids,
            reference_map=reference_map,
            summary=summary,
        )
        per_block_rows.extend(side)
        manual_review_rows.extend([row for row in side if row["verdict"] != "equivalent"])
        summary_rows.append(
            make_summary_row(
                profile_id=profile_id,
                source="executed",
                launched=True,
                summary=summary,
                quality=quality,
                estimated_cost_usd=predicted,
                actual_cost_usd=actual,
                note="",
            )
        )
        _save_json(out_dir / f"{profile_id}_batch_results.json", [asdict(r) for r in results])
        _save_json(
            out_dir / f"{profile_id}_batch_summary.json",
            {
                "summary": summary,
                "quality": quality,
                "side_by_side": side,
            },
        )

    dedup_shortlist: dict[tuple[str, str], dict] = {}
    for row in manual_review_rows:
        dedup_shortlist[(row["run_id"], row["block_id"])] = row
    shortlist = list(dedup_shortlist.values())

    _save_json(out_dir / "batch_upper_bound_summary.json", summary_rows)
    _save_csv(out_dir / "batch_upper_bound_summary.csv", summary_rows)
    (out_dir / "batch_upper_bound_summary.md").write_text(
        build_batch_upper_bound_summary_md(summary_rows),
        encoding="utf-8",
    )
    _save_json(out_dir / "per_block_side_by_side.json", per_block_rows)
    (out_dir / "per_block_side_by_side.md").write_text(
        build_per_block_side_by_side_md(per_block_rows),
        encoding="utf-8",
    )
    (out_dir / "manual_review_shortlist.md").write_text(
        build_manual_review_shortlist_md(shortlist),
        encoding="utf-8",
    )

    recommendation = recommend_upper_bound(summary_rows)
    (out_dir / "winner_recommendation.md").write_text(
        build_winner_recommendation_md(
            recommendation=recommendation,
            rows=summary_rows,
            budget_cap_usd=args.budget_cap_usd,
            budget_spent_usd=budget.spent_usd,
        ),
        encoding="utf-8",
    )

    budget_payload = budget.to_dict()
    _save_json(out_dir / "budget_timeline.json", budget_payload)
    (out_dir / "budget_timeline.md").write_text(
        build_budget_timeline_md(budget),
        encoding="utf-8",
    )

    logger.info("Done. Artifacts: %s", out_dir)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pdf", default=PDF_NAME, help="Exact PDF name to resolve.")
    parser.add_argument(
        "--project-dir",
        default=str(_ROOT / PROJECT_REL),
        help="Optional project dir override.",
    )
    parser.add_argument(
        "--budget-cap-usd",
        type=float,
        default=DEFAULT_BUDGET_CAP_USD,
        help="Hard cap on additional spend for newly executed runs.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
