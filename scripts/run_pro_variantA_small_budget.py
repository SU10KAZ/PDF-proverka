"""
Cheap technical experiment for Gemini 3.1 Pro on stage 02 via OpenRouter.

Goal:
  1. Check whether Gemini 3.1 Pro can work not only in single-block mode,
     but also in small batches.
  2. Check whether raising block resolution from r800 to r1000 helps.

Important constraints:
  - no production-default changes
  - no Claude path changes
  - no Flash path changes
  - no direct Gemini API
  - no large expensive matrix
  - reuse existing artifacts wherever valid
  - hard additional spend cap: USD 5.00

Artifacts:
  <project_dir>/_experiments/pro_variantA_small_budget/<timestamp>/
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import math
import shutil
import statistics
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

from blocks import crop_blocks_to_dir, make_block_render_profile  # noqa: E402
from run_block_resolution_matrix import make_shadow_project  # noqa: E402
from run_gemini_openrouter_stage02_experiment import (  # noqa: E402
    apply_byte_cap_split,
    build_messages,
    build_page_contexts,
    build_system_prompt,
    classify_risk,
    load_blocks_index,
    pack_blocks,
)
from run_stage02_recall_hybrid import resolve_project  # noqa: E402
from webapp.services.llm_runner import run_llm  # noqa: E402
from webapp.services.openrouter_block_batch import (  # noqa: E402
    check_completeness,
    load_openrouter_block_batch_schema,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pro_variantA_budget")


PDF_NAME = "13АВ-РД-КЖ5.1-К1К2 (2).pdf"
PROJECT_REL = f"projects/214. Alia (ASTERUS)/KJ/{PDF_NAME}"
MODEL_PRO = "google/gemini-3.1-pro-preview"
MAX_BUDGET_USD = 5.00
DEFAULT_TIMEOUT_SEC = 600
MAX_OUTPUT_TOKENS = 32768
REFERENCE_EXP_NAME = "pro_second_pass_reeval_small_project"
REFERENCE_REQUIRED_CONFIG = "pro_high_p2"
HISTORICAL_PROBLEMATIC_IDS = ["6DRC-7KQL-9TJ", "4MQJ-6NXP-4YH"]
SMALL_BATCH_PROFILES = {
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
    "b6": {
        "heavy": {"target": 3, "max": 3},
        "normal": {"target": 6, "max": 6},
        "light": {"target": 6, "max": 6},
    },
}
RESOLUTION_PROFILES = {
    "r800": {"min_long_side_px": 800, "target_dpi": 100},
    "r1000": {"min_long_side_px": 1000, "target_dpi": 100},
}
SUMMARY_SHORT_THRESHOLD = 80

STATUS_SUCCESS = "success"
STATUS_API_ERROR = "api_error"
STATUS_NO_FIELD = "no_block_analyses_field"
STATUS_EMPTY = "empty_array"
STATUS_NON_DICT = "non_dict_analysis"
STATUS_MISSING = "missing"
STATUS_DUPLICATE = "duplicate"
STATUS_EXTRA = "extra"
STATUS_MISSING_DUPLICATE = "missing+duplicate"
STATUS_MISSING_EXTRA = "missing+extra"
STATUS_DUPLICATE_EXTRA = "duplicate+extra"
STATUS_MISSING_DUPLICATE_EXTRA = "missing+duplicate+extra"


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


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


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = int(len(s) * pct / 100)
    return s[min(idx, len(s) - 1)]


def _response_mode_label(initial_healing: bool) -> str:
    return "heal_on" if initial_healing else "heal_off"


def _retry_healing(initial_healing: bool) -> bool:
    return False if initial_healing else initial_healing


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


def _combined_fail_status(missing: list[str], duplicates: list[str], extra: list[str]) -> str:
    parts: list[str] = []
    if missing:
        parts.append(STATUS_MISSING)
    if duplicates:
        parts.append(STATUS_DUPLICATE)
    if extra:
        parts.append(STATUS_EXTRA)
    return "+".join(parts)


def _summary_specificity_signals(summary: str) -> dict[str, int]:
    text = str(summary or "").strip()
    has_digits = int(any(ch.isdigit() for ch in text))
    has_measure_units = int(any(token in text for token in ("мм", "m", "R", "x", "А500", "A500", "-0.")))
    return {
        "summary_len": len(text),
        "has_digits": has_digits,
        "has_measure_units": has_measure_units,
        "specificity_score": len(text) + 15 * has_digits + 20 * has_measure_units,
    }


def analysis_brief(analysis: dict | None) -> dict:
    analysis = analysis or {}
    summary = str(analysis.get("summary") or "").strip()
    kv = analysis.get("key_values_read") or []
    findings = analysis.get("findings") or []
    spec = _summary_specificity_signals(summary)
    return {
        "has_analysis": bool(analysis),
        "summary": summary,
        "summary_len": spec["summary_len"],
        "specificity_score": spec["specificity_score"],
        "unreadable_text": bool(analysis.get("unreadable_text")),
        "kv_count": len(kv),
        "findings_count": len(findings),
        "has_findings": len(findings) > 0,
    }


def compare_analysis_against_reference(
    block_id: str,
    candidate: dict | None,
    reference: dict | None,
    *,
    phase: str,
    run_id: str,
) -> dict:
    ref = analysis_brief(reference)
    cand = analysis_brief(candidate)

    reasons: list[str] = []
    findings_presence_delta = int(cand["has_findings"]) - int(ref["has_findings"])
    findings_delta = cand["findings_count"] - ref["findings_count"]
    kv_delta = cand["kv_count"] - ref["kv_count"]
    specificity_delta = cand["specificity_score"] - ref["specificity_score"]

    findings_presence_collapse = ref["has_findings"] and not cand["has_findings"]
    kv_collapse = (
        ref["kv_count"] >= 6 and cand["kv_count"] <= max(2, math.floor(ref["kv_count"] * 0.5))
    ) or (ref["kv_count"] > 0 and cand["kv_count"] == 0)
    generic_summary = (
        cand["summary_len"] < 50
        or (cand["summary_len"] < max(60, int(ref["summary_len"] * 0.55)) and cand["summary_len"] < ref["summary_len"])
    )
    noisy_kv_inflation = (
        cand["kv_count"] >= max(ref["kv_count"] * 2, ref["kv_count"] + 15)
        and findings_delta <= 0
        and specificity_delta <= 20
    )
    summary_specificity_improved = specificity_delta >= 40
    kv_adequacy_improved = cand["kv_count"] >= ref["kv_count"] + 4
    useful_findings_improved = findings_delta > 0

    if not cand["has_analysis"]:
        classification = "likely_degraded"
        reasons.append("missing_analysis")
    else:
        if findings_presence_collapse:
            reasons.append("findings_presence_collapse")
        if kv_collapse:
            reasons.append("kv_collapse")
        if generic_summary:
            reasons.append("generic_or_short_summary")
        if noisy_kv_inflation:
            reasons.append("noisy_kv_inflation")

        if reasons:
            classification = "likely_degraded"
        elif useful_findings_improved or summary_specificity_improved or kv_adequacy_improved:
            classification = "likely_improved"
            if useful_findings_improved:
                reasons.append("useful_findings_improved")
            if summary_specificity_improved:
                reasons.append("summary_specificity_improved")
            if kv_adequacy_improved:
                reasons.append("kv_adequacy_improved")
        else:
            classification = "likely_equivalent"
            reasons.append("no_obvious_regression")

    return {
        "phase": phase,
        "run_id": run_id,
        "block_id": block_id,
        "reference_findings_count": ref["findings_count"],
        "candidate_findings_count": cand["findings_count"],
        "reference_has_findings": ref["has_findings"],
        "candidate_has_findings": cand["has_findings"],
        "findings_presence_delta": findings_presence_delta,
        "findings_delta": findings_delta,
        "reference_kv_count": ref["kv_count"],
        "candidate_kv_count": cand["kv_count"],
        "kv_delta": kv_delta,
        "reference_summary_len": ref["summary_len"],
        "candidate_summary_len": cand["summary_len"],
        "summary_specificity_delta": specificity_delta,
        "classification": classification,
        "reasons": reasons,
        "findings_presence_collapse": findings_presence_collapse,
        "kv_collapse": kv_collapse,
        "generic_summary": generic_summary,
        "noisy_kv_inflation": noisy_kv_inflation,
        "summary_specificity_improved": summary_specificity_improved,
        "kv_adequacy_improved": kv_adequacy_improved,
        "useful_findings_improved": useful_findings_improved,
    }


def evaluate_quality_preservation(side_by_side: list[dict]) -> dict:
    degraded = [row for row in side_by_side if row["classification"] == "likely_degraded"]
    improved = [row for row in side_by_side if row["classification"] == "likely_improved"]
    equivalent = [row for row in side_by_side if row["classification"] == "likely_equivalent"]

    findings_presence_collapse = [row for row in side_by_side if row["findings_presence_collapse"]]
    kv_collapse = [row for row in side_by_side if row["kv_collapse"]]
    generic_summary = [row for row in side_by_side if row["generic_summary"]]
    noisy_kv_inflation = [row for row in side_by_side if row["noisy_kv_inflation"]]
    specificity_improved = [row for row in side_by_side if row["summary_specificity_improved"]]
    kv_improved = [row for row in side_by_side if row["kv_adequacy_improved"]]

    hard_fail_reasons: list[str] = []
    if findings_presence_collapse:
        hard_fail_reasons.append("findings_presence_collapse")
    if len(kv_collapse) >= 2:
        hard_fail_reasons.append("kv_collapse")
    if len(generic_summary) >= max(2, math.ceil(len(side_by_side) * 0.35)):
        hard_fail_reasons.append("systematic_generic_summaries")
    if len(noisy_kv_inflation) >= 2:
        hard_fail_reasons.append("systematic_noisy_kv_inflation")
    if len(degraded) > max(1, math.ceil(len(side_by_side) * 0.25)):
        hard_fail_reasons.append("too_many_likely_degraded_blocks")

    passed = not hard_fail_reasons
    return {
        "quality_gate_passed": passed,
        "hard_fail_reasons": hard_fail_reasons,
        "likely_equivalent_count": len(equivalent),
        "likely_improved_count": len(improved),
        "likely_degraded_count": len(degraded),
        "findings_presence_collapse_count": len(findings_presence_collapse),
        "kv_collapse_count": len(kv_collapse),
        "generic_summary_count": len(generic_summary),
        "noisy_kv_inflation_count": len(noisy_kv_inflation),
        "summary_specificity_improved_count": len(specificity_improved),
        "kv_adequacy_improved_count": len(kv_improved),
        "manual_review_block_ids": sorted({
            row["block_id"]
            for row in degraded + improved
            if row["classification"] != "likely_equivalent"
        }),
    }


def should_continue_batch_study(existing_rows: list[dict], next_profile: str) -> tuple[bool, str]:
    by_id = {row["profile_id"]: row for row in existing_rows}
    if next_profile == "b4":
        b2 = by_id.get("b2")
        if not b2 or not b2.get("survived"):
            return False, "b2 did not survive"
    if next_profile == "b6":
        b4 = by_id.get("b4")
        if not b4 or not b4.get("survived"):
            return False, "b4 did not survive"
    return True, ""


def recommend_final_config(
    *,
    best_resolution: str,
    batch_rows: list[dict],
    confirmatory_summary: dict | None,
) -> dict:
    surviving_batches = [
        row for row in batch_rows
        if row.get("survived")
    ]
    surviving_batches.sort(key=lambda row: int(row["profile_id"][1:]), reverse=True)

    if surviving_batches:
        candidate_mode = "batch"
        candidate_batch = surviving_batches[0]["profile_id"]
    else:
        candidate_mode = "single_block"
        candidate_batch = ""

    final_config = {
        "mode": candidate_mode,
        "batch_profile": candidate_batch,
        "resolution": best_resolution,
    }
    continue_batch_path = bool(surviving_batches)

    if confirmatory_summary:
        complete = (
            confirmatory_summary.get("coverage_pct") == 100.0
            and confirmatory_summary.get("missing_count") == 0
            and confirmatory_summary.get("duplicate_count") == 0
            and confirmatory_summary.get("extra_count") == 0
            and not confirmatory_summary.get("fail_mode_distribution")
        )
        if candidate_mode == "batch" and not complete:
            final_config = {
                "mode": "single_block",
                "batch_profile": "",
                "resolution": best_resolution,
            }
            continue_batch_path = False

    return {
        "candidate_before_confirmatory": {
            "mode": candidate_mode,
            "batch_profile": candidate_batch,
            "resolution": best_resolution,
        },
        "final_config": final_config,
        "continue_batch_path": continue_batch_path,
    }


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
            "events": [asdict(e) for e in self.events],
        }


def estimate_step_cost(
    *,
    mode: str,
    reasoning_effort: str,
    resolution: str,
    block_count: int,
) -> float:
    table = {
        ("single_block", "high", "r800"): 0.060,
        ("single_block", "high", "r1000"): 0.078,
        ("single_block", "low", "r800"): 0.012,
        ("batch", "high", "r800"): 0.050,
        ("batch", "high", "r1000"): 0.062,
    }
    per_block = table.get((mode, reasoning_effort, resolution), 0.060)
    return round(per_block * block_count, 4)


def find_latest_reference_dir(project_dir: Path) -> Path | None:
    root = project_dir / "_experiments" / REFERENCE_EXP_NAME
    if not root.exists():
        return None
    candidates = sorted(
        [p for p in root.iterdir() if p.is_dir() and (p / f"{REFERENCE_REQUIRED_CONFIG}_summary.json").exists()],
        key=lambda p: p.name,
        reverse=True,
    )
    return candidates[0] if candidates else None


def load_reference_outputs(project_dir: Path) -> tuple[Path | None, dict[str, dict]]:
    ref_dir = find_latest_reference_dir(project_dir)
    if ref_dir is None:
        return None, {}
    rows = _load_json(ref_dir / f"{REFERENCE_REQUIRED_CONFIG}_per_block.json")
    reference: dict[str, dict] = {}
    for row in rows:
        if row.get("success") and row.get("parsed_block"):
            reference[row["block_id"]] = row["parsed_block"]
    return ref_dir, reference


def load_additional_problematic_ids(project_dir: Path) -> list[str]:
    ref_dir = find_latest_reference_dir(project_dir)
    if not ref_dir:
        return []
    retry_log = ref_dir / "retry_log.json"
    if not retry_log.exists():
        return []
    rows = _load_json(retry_log)
    ids = []
    for row in rows:
        if row.get("retry_attempted"):
            bid = row.get("block_id")
            if bid and bid not in ids:
                ids.append(bid)
    return ids


def select_stress_set(
    blocks: list[dict],
    *,
    preferred_problematic_ids: list[str],
    extra_problematic_ids: list[str],
) -> tuple[list[str], list[dict]]:
    by_id = {b["block_id"]: b for b in blocks}
    heavy = sorted(
        [b for b in blocks if classify_risk(b) == "heavy"],
        key=_complexity_tuple,
        reverse=True,
    )
    normal = sorted(
        [b for b in blocks if classify_risk(b) == "normal"],
        key=_complexity_tuple,
        reverse=True,
    )

    selected: list[str] = []
    reasons: dict[str, list[str]] = {}

    def add(bid: str, reason: str) -> None:
        if bid in by_id and bid not in selected:
            selected.append(bid)
            reasons.setdefault(bid, []).append(reason)

    for bid in preferred_problematic_ids:
        add(bid, "historical_problematic")

    while sum(1 for bid in selected if classify_risk(by_id[bid]) == "heavy") < 4:
        for b in heavy:
            if b["block_id"] not in selected:
                add(b["block_id"], "heavy_complexity")
                break
        else:
            break

    while sum(1 for bid in selected if classify_risk(by_id[bid]) == "normal") < 2:
        for b in normal:
            if b["block_id"] not in selected:
                add(b["block_id"], "normal_complexity")
                break
        else:
            break

    for bid in extra_problematic_ids:
        if len(selected) >= 8:
            break
        add(bid, "recent_transient_problematic")

    normal_fill = [b for b in normal if b["block_id"] not in selected]
    light_fill = sorted(
        [b for b in blocks if classify_risk(b) == "light" and b["block_id"] not in selected],
        key=_complexity_tuple,
        reverse=True,
    )
    heavy_fill = [b for b in heavy if b["block_id"] not in selected]
    remaining = normal_fill + light_fill + heavy_fill
    for b in remaining:
        if len(selected) >= 8:
            break
        fill_reason = f"{classify_risk(b)}_complexity_fill"
        add(b["block_id"], fill_reason)

    selected_blocks = [by_id[bid] for bid in selected[:8]]
    heavy_ids = [b["block_id"] for b in selected_blocks if classify_risk(b) == "heavy"]
    nonheavy_ids = [b["block_id"] for b in selected_blocks if classify_risk(b) != "heavy"]
    ordered: list[str] = []
    for i in range(max(len(heavy_ids), len(nonheavy_ids))):
        if i < len(heavy_ids):
            ordered.append(heavy_ids[i])
        if i < len(nonheavy_ids):
            ordered.append(nonheavy_ids[i])

    manifest_rows = []
    for bid in ordered:
        block = by_id[bid]
        manifest_rows.append({
            "block_id": bid,
            "page": block.get("page"),
            "risk": classify_risk(block),
            "size_kb": block.get("size_kb"),
            "ocr_text_len": block.get("ocr_text_len"),
            "reasons": reasons.get(bid, []),
            "ocr_label": block.get("ocr_label", ""),
        })
    return ordered, manifest_rows


def select_resolution_set(blocks: list[dict]) -> tuple[list[str], list[dict]]:
    heavy = sorted(
        [b for b in blocks if classify_risk(b) == "heavy"],
        key=_complexity_tuple,
        reverse=True,
    )
    selected = heavy[:6]
    manifest_rows = []
    for b in selected:
        manifest_rows.append({
            "block_id": b["block_id"],
            "page": b.get("page"),
            "risk": classify_risk(b),
            "size_kb": b.get("size_kb"),
            "ocr_text_len": b.get("ocr_text_len"),
            "ocr_label": b.get("ocr_label", ""),
            "reason": "top_heavy_complexity",
        })
    return [b["block_id"] for b in selected], manifest_rows


def build_manifest_md(title: str, rows: list[dict]) -> str:
    lines = [
        f"# {title}\n",
        "| block_id | page | risk | size_kb | ocr_text_len | reasons | ocr_label |",
        "|----------|------|------|---------|--------------|---------|-----------|",
    ]
    for row in rows:
        reasons = row.get("reasons", row.get("reason", []))
        if isinstance(reasons, list):
            reasons_text = ",".join(reasons)
        else:
            reasons_text = str(reasons)
        lines.append(
            f"| {row['block_id']} | {row.get('page')} | {row.get('risk')} | {row.get('size_kb')} | "
            f"{row.get('ocr_text_len')} | {reasons_text} | {str(row.get('ocr_label', ''))[:80]} |"
        )
    lines.append("")
    return "\n".join(lines)


def build_reference_reuse_report(
    *,
    reference_dir: Path | None,
    requested_ids: list[str],
    reused_ids: list[str],
    missing_ids: list[str],
    newly_run_ids: list[str],
) -> str:
    lines = [
        "# Reference Reuse Report\n",
        f"- Reference dir: {reference_dir if reference_dir else 'not found'}",
        f"- Requested union size: {len(requested_ids)}",
        f"- Reused from existing `pro_high_p2`: {len(reused_ids)}",
        f"- Newly executed reference calls: {len(newly_run_ids)}",
        f"- Missing after fill: {len(missing_ids)}\n",
    ]
    if reused_ids:
        lines.append("## Reused block_ids")
        for bid in reused_ids:
            lines.append(f"- `{bid}`")
    if newly_run_ids:
        lines.append("\n## Newly executed reference block_ids")
        for bid in newly_run_ids:
            lines.append(f"- `{bid}`")
    if missing_ids:
        lines.append("\n## Missing after reference fill")
        for bid in missing_ids:
            lines.append(f"- `{bid}`")
    lines.append("")
    return "\n".join(lines)


@dataclass
class CallResult:
    run_id: str
    input_block_ids: list[str]
    batch_id: int
    mode: str
    reasoning_effort: str
    resolution: str
    initial_healing: bool
    success: bool = False
    primary_status: str = ""
    final_status: str = ""
    retry_attempted: bool = False
    retry_recovered: bool = False
    primary_response_id: str = ""
    retry_response_id: str = ""
    primary_finish_reason: str = ""
    retry_finish_reason: str = ""
    response_id: str = ""
    finish_reason: str = ""
    parsed_data: dict | None = None
    duration_ms: int = 0
    prompt_tokens: int = 0
    output_tokens: int = 0
    reasoning_tokens: int = 0
    cached_tokens: int = 0
    cost_usd: float = 0.0
    cost_source: str = "estimated"
    error_message: str = ""
    missing: list[str] = field(default_factory=list)
    duplicates: list[str] = field(default_factory=list)
    extra: list[str] = field(default_factory=list)


def classify_single_block_strict(parsed_data: dict | None, input_bid: str) -> tuple[str, dict | None]:
    if not isinstance(parsed_data, dict):
        return STATUS_NO_FIELD, None
    analyses = parsed_data.get("block_analyses")
    if not isinstance(analyses, list):
        return STATUS_NO_FIELD, None
    if len(analyses) == 0:
        return STATUS_EMPTY, None
    if len(analyses) != 1:
        completeness = check_completeness([input_bid], parsed_data, single_block_inference=False)
        status = _combined_fail_status(completeness.missing, completeness.duplicates, completeness.extra)
        return status or STATUS_DUPLICATE, None
    one = analyses[0]
    if not isinstance(one, dict):
        return STATUS_NON_DICT, None
    returned_bid = one.get("block_id") or ""
    if returned_bid != input_bid:
        return STATUS_EXTRA, None
    return STATUS_SUCCESS, one


def classify_batch_strict(parsed_data: dict | None, input_block_ids: list[str]) -> tuple[str, list[str], list[str], list[str]]:
    if not isinstance(parsed_data, dict):
        return STATUS_NO_FIELD, list(input_block_ids), [], []
    analyses = parsed_data.get("block_analyses")
    if not isinstance(analyses, list):
        return STATUS_NO_FIELD, list(input_block_ids), [], []
    if len(analyses) == 0:
        return STATUS_EMPTY, list(input_block_ids), [], []
    if any(not isinstance(a, dict) for a in analyses):
        return STATUS_NON_DICT, list(input_block_ids), [], []
    completeness = check_completeness(input_block_ids, parsed_data, single_block_inference=False)
    if completeness.ok:
        return STATUS_SUCCESS, [], [], []
    return (
        _combined_fail_status(completeness.missing, completeness.duplicates, completeness.extra),
        completeness.missing,
        completeness.duplicates,
        completeness.extra,
    )


async def run_stage02_call(
    *,
    project_dir: Path,
    blocks: list[dict],
    project_info: dict,
    system_prompt: str,
    page_contexts: dict[int, str],
    run_id: str,
    batch_id: int,
    reasoning_effort: str,
    response_healing_initial: bool,
    schema: dict,
    timeout: int,
    max_output_tokens: int,
) -> CallResult:
    messages = build_messages(
        blocks,
        project_dir,
        project_info,
        batch_id=batch_id,
        total_batches=1,
        system_prompt=system_prompt,
        page_contexts=page_contexts,
    )
    input_ids = [b["block_id"] for b in blocks]
    mode = "single_block" if len(input_ids) == 1 else "batch"
    extra_body = {"reasoning": {"effort": reasoning_effort}} if reasoning_effort else None

    async def _attempt(healing: bool):
        return await run_llm(
            stage="block_batch",
            messages=messages,
            model_override=MODEL_PRO,
            temperature=0.2,
            timeout=timeout,
            max_retries=3,
            strict_schema=schema,
            schema_name="block_batch",
            response_healing=healing,
            require_parameters=True,
            max_tokens_override=max_output_tokens,
            extra_body=extra_body,
        )

    primary = await _attempt(response_healing_initial)

    result = CallResult(
        run_id=run_id,
        input_block_ids=input_ids,
        batch_id=batch_id,
        mode=mode,
        reasoning_effort=reasoning_effort,
        resolution="r800",
        initial_healing=response_healing_initial,
        primary_response_id=primary.response_id or "",
        response_id=primary.response_id or "",
        primary_finish_reason=primary.finish_reason or "",
        finish_reason=primary.finish_reason or "",
        duration_ms=primary.duration_ms,
        prompt_tokens=primary.input_tokens,
        output_tokens=primary.output_tokens,
        reasoning_tokens=primary.reasoning_tokens,
        cached_tokens=primary.cached_tokens,
        cost_usd=primary.cost_usd,
        cost_source=primary.cost_source,
        error_message=primary.error_message or "",
    )

    if primary.is_error:
        primary_status = STATUS_API_ERROR
        parsed = None
    else:
        parsed = primary.json_data if isinstance(primary.json_data, dict) else None
        if mode == "single_block":
            primary_status, parsed_block = classify_single_block_strict(parsed, input_ids[0])
        else:
            primary_status, miss, dup, extra = classify_batch_strict(parsed, input_ids)
            result.missing = miss
            result.duplicates = dup
            result.extra = extra

    if mode == "single_block" and not primary.is_error:
        if primary_status == STATUS_SUCCESS:
            result.parsed_data = {"block_analyses": [parsed_block]}
        else:
            result.parsed_data = parsed
    else:
        result.parsed_data = parsed

    result.primary_status = primary_status
    if primary_status == STATUS_SUCCESS:
        result.success = True
        result.final_status = STATUS_SUCCESS
        return result

    result.retry_attempted = True
    retry = await _attempt(_retry_healing(response_healing_initial))
    result.duration_ms += retry.duration_ms
    result.prompt_tokens += retry.input_tokens
    result.output_tokens += retry.output_tokens
    result.reasoning_tokens += retry.reasoning_tokens
    result.cached_tokens += retry.cached_tokens
    result.cost_usd += retry.cost_usd
    if retry.cost_source == "actual":
        result.cost_source = "actual"
    result.retry_response_id = retry.response_id or ""
    result.retry_finish_reason = retry.finish_reason or ""
    result.response_id = retry.response_id or result.response_id
    result.finish_reason = retry.finish_reason or result.finish_reason

    if retry.is_error:
        result.final_status = STATUS_API_ERROR
        return result

    retry_parsed = retry.json_data if isinstance(retry.json_data, dict) else None
    if mode == "single_block":
        retry_status, retry_block = classify_single_block_strict(retry_parsed, input_ids[0])
        if retry_status == STATUS_SUCCESS:
            result.parsed_data = {"block_analyses": [retry_block]}
        else:
            result.parsed_data = retry_parsed
    else:
        retry_status, miss, dup, extra = classify_batch_strict(retry_parsed, input_ids)
        result.parsed_data = retry_parsed
        result.missing = miss
        result.duplicates = dup
        result.extra = extra

    result.final_status = retry_status
    if retry_status == STATUS_SUCCESS:
        result.success = True
        result.retry_recovered = True
    return result


async def run_call_set(
    *,
    project_dir: Path,
    blocks_or_batches: list[list[dict]],
    project_info: dict,
    reasoning_effort: str,
    response_healing_initial: bool,
    schema: dict,
    timeout: int,
    max_output_tokens: int,
    parallelism: int,
    run_id: str,
    resolution: str,
) -> tuple[list[CallResult], float]:
    system_prompt = build_system_prompt(project_info, sum(len(g) for g in blocks_or_batches))
    page_contexts = build_page_contexts(project_dir)
    sem = asyncio.Semaphore(parallelism)
    results: list[CallResult | None] = [None] * len(blocks_or_batches)

    async def _one(idx: int, group: list[dict]) -> None:
        async with sem:
            started = time.monotonic()
            res = await run_stage02_call(
                project_dir=project_dir,
                blocks=group,
                project_info=project_info,
                system_prompt=system_prompt,
                page_contexts=page_contexts,
                run_id=run_id,
                batch_id=idx + 1,
                reasoning_effort=reasoning_effort,
                response_healing_initial=response_healing_initial,
                schema=schema,
                timeout=timeout,
                max_output_tokens=max_output_tokens,
            )
            res.resolution = resolution
            wall = time.monotonic() - started
            logger.info(
                "[%s] %02d/%d blocks=%d primary=%s final=%s retry=%s wall=%.1fs cost=$%.4f",
                run_id,
                idx + 1,
                len(blocks_or_batches),
                len(group),
                res.primary_status,
                res.final_status,
                "yes" if res.retry_attempted else "no",
                wall,
                res.cost_usd,
            )
            results[idx] = res

    start = time.monotonic()
    await asyncio.gather(*(_one(i, g) for i, g in enumerate(blocks_or_batches)))
    elapsed = time.monotonic() - start
    return [r for r in results if r is not None], elapsed


def analyses_map_from_results(results: list[CallResult]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for res in results:
        parsed = res.parsed_data if isinstance(res.parsed_data, dict) else None
        analyses = (parsed or {}).get("block_analyses") or []
        for analysis in analyses:
            if isinstance(analysis, dict):
                bid = analysis.get("block_id") or ""
                if bid:
                    out[bid] = analysis
    return out


def aggregate_results(
    *,
    results: list[CallResult],
    input_block_ids: list[str],
    elapsed_s: float,
    label: str,
    mode: str,
    reasoning_effort: str,
    parallelism: int,
    resolution: str,
) -> dict:
    durations = [r.duration_ms / 1000.0 for r in results]
    analyses_map = analyses_map_from_results(results)
    returned_counts = Counter()
    for res in results:
        parsed = res.parsed_data if isinstance(res.parsed_data, dict) else None
        analyses = (parsed or {}).get("block_analyses") or []
        for analysis in analyses:
            if isinstance(analysis, dict):
                bid = analysis.get("block_id") or ""
                if bid:
                    returned_counts[bid] += 1

    missing_ids = [bid for bid in input_block_ids if returned_counts.get(bid, 0) == 0]
    duplicate_ids = sorted([bid for bid, c in returned_counts.items() if c > 1])
    extra_ids = sorted([bid for bid in returned_counts if bid not in set(input_block_ids)])

    kv_counts: list[int] = []
    blocks_with_findings = 0
    total_findings = 0
    total_kv = 0
    empty_summary_count = 0
    empty_kv_count = 0
    for bid in input_block_ids:
        analysis = analyses_map.get(bid)
        if not analysis:
            continue
        summary = str(analysis.get("summary") or "").strip()
        kv = analysis.get("key_values_read") or []
        findings = analysis.get("findings") or []
        if not summary:
            empty_summary_count += 1
        if not kv:
            empty_kv_count += 1
        else:
            kv_counts.append(len(kv))
            total_kv += len(kv)
        if findings:
            blocks_with_findings += 1
            total_findings += len(findings)

    fail_modes = Counter(r.final_status for r in results if r.final_status != STATUS_SUCCESS)
    primary_fail_modes = Counter(r.primary_status for r in results if r.primary_status != STATUS_SUCCESS)
    total_prompt_tokens = sum(r.prompt_tokens for r in results)
    total_output_tokens = sum(r.output_tokens for r in results)
    total_reasoning_tokens = sum(r.reasoning_tokens for r in results)
    total_cached_tokens = sum(r.cached_tokens for r in results)
    total_cost_usd = sum(r.cost_usd for r in results)

    strict_success_count = len(input_block_ids) - len(missing_ids)
    strict_failure_count = len(input_block_ids) - strict_success_count
    coverage_pct = round(strict_success_count / max(1, len(input_block_ids)) * 100, 2)

    return {
        "label": label,
        "mode": mode,
        "reasoning_effort": reasoning_effort,
        "parallelism": parallelism,
        "resolution": resolution,
        "total_blocks": len(input_block_ids),
        "total_calls": len(results),
        "coverage_pct": coverage_pct,
        "missing_count": len(missing_ids),
        "duplicate_count": len(duplicate_ids),
        "extra_count": len(extra_ids),
        "strict_success_count": strict_success_count,
        "strict_failure_count": strict_failure_count,
        "missing_block_ids": missing_ids,
        "duplicate_block_ids": duplicate_ids,
        "extra_block_ids": extra_ids,
        "retry_triggered_count": sum(1 for r in results if r.retry_attempted),
        "retry_recovered_count": sum(1 for r in results if r.retry_recovered),
        "fail_mode_distribution": dict(fail_modes),
        "fail_mode_distribution_before_retry": dict(primary_fail_modes),
        "blocks_with_findings": blocks_with_findings,
        "total_findings": total_findings,
        "total_key_values": total_kv,
        "median_key_values": round(statistics.median(kv_counts), 2) if kv_counts else 0.0,
        "empty_summary_count": empty_summary_count,
        "empty_key_values_count": empty_kv_count,
        "elapsed_s": elapsed_s,
        "avg_duration_s": round(sum(durations) / len(durations), 2) if durations else 0.0,
        "median_duration_s": round(statistics.median(durations), 2) if durations else 0.0,
        "p95_duration_s": round(_percentile(durations, 95), 2) if durations else 0.0,
        "total_prompt_tokens": total_prompt_tokens,
        "total_output_tokens": total_output_tokens,
        "total_reasoning_tokens": total_reasoning_tokens,
        "total_cached_tokens": total_cached_tokens,
        "total_cost_usd": round(total_cost_usd, 6),
        "cost_sources_actual": sum(1 for r in results if r.cost_source == "actual"),
        "cost_sources_estimated": sum(1 for r in results if r.cost_source != "actual"),
        "response_ids_available": sum(1 for r in results if r.response_id),
    }


def build_side_by_side(
    *,
    block_ids: list[str],
    reference_map: dict[str, dict],
    candidate_map: dict[str, dict],
    phase: str,
    run_id: str,
) -> list[dict]:
    return [
        compare_analysis_against_reference(
            bid,
            candidate_map.get(bid),
            reference_map.get(bid),
            phase=phase,
            run_id=run_id,
        )
        for bid in block_ids
    ]


def build_batch_summary_md(rows: list[dict]) -> str:
    lines = [
        "# Batch Screening Summary\n",
        "| Profile | Hard gate | Quality gate | Survived | Coverage | Missing/Dup/Extra | Retry trig/rec | Improved | Equivalent | Degraded | +Findings | +BlocksWithFindings | +KV | Cost USD | Elapsed s |",
        "|---------|-----------|--------------|----------|----------|-------------------|----------------|----------|------------|----------|-----------|----------------------|-----|----------|-----------|",
    ]
    for row in rows:
        lines.append(
            f"| {row['profile_id']} | {row['hard_gate_passed']} | {row['quality_gate_passed']} | {row['survived']} | "
            f"{row['coverage_pct']:.2f}% | {row['missing_count']}/{row['duplicate_count']}/{row['extra_count']} | "
            f"{row['retry_triggered_count']}/{row['retry_recovered_count']} | {row['likely_improved_count']} | "
            f"{row['likely_equivalent_count']} | {row['likely_degraded_count']} | {row['total_findings_delta']:+d} | "
            f"{row['blocks_with_findings_delta']:+d} | {row['total_kv_delta']:+d} | ${row['actual_cost_usd']:.4f} | {row['elapsed_s']:.1f} |"
        )
    lines.append("")
    return "\n".join(lines)


def build_resolution_summary_md(rows: list[dict]) -> str:
    lines = [
        "# Resolution Screening Summary\n",
        "| Resolution | Completeness OK | Candidate | Improved | Equivalent | Degraded | +Findings | +KV | Specificity improved | Cost USD | Elapsed s | Note |",
        "|------------|-----------------|-----------|----------|------------|----------|-----------|-----|---------------------|----------|-----------|------|",
    ]
    for row in rows:
        lines.append(
            f"| {row['resolution']} | {row['completeness_ok']} | {row['candidate']} | {row['likely_improved_count']} | "
            f"{row['likely_equivalent_count']} | {row['likely_degraded_count']} | {row['total_findings_delta']:+d} | "
            f"{row['total_kv_delta']:+d} | {row['summary_specificity_improved_count']} | ${row['actual_cost_usd']:.4f} | "
            f"{row['elapsed_s']:.1f} | {row['note']} |"
        )
    lines.append("")
    return "\n".join(lines)


def build_confirmatory_summary_md(summary: dict) -> str:
    lines = [
        "# Confirmatory Full Summary\n",
        f"- Label: `{summary['label']}`",
        f"- Mode: `{summary['mode']}`",
        f"- Resolution: `{summary['resolution']}`",
        f"- Coverage: {summary['coverage_pct']:.2f}%",
        f"- Missing / Duplicate / Extra: {summary['missing_count']} / {summary['duplicate_count']} / {summary['extra_count']}",
        f"- Strict success / failure: {summary['strict_success_count']} / {summary['strict_failure_count']}",
        f"- Retry triggered / recovered: {summary['retry_triggered_count']} / {summary['retry_recovered_count']}",
        f"- Fail modes: {json.dumps(summary['fail_mode_distribution'], ensure_ascii=False)}",
        f"- Blocks with findings: {summary['blocks_with_findings']}",
        f"- Total findings: {summary['total_findings']}",
        f"- Total key values: {summary['total_key_values']}",
        f"- Cost USD: ${summary['total_cost_usd']:.4f}",
        f"- Elapsed s: {summary['elapsed_s']:.1f}",
        "",
    ]
    return "\n".join(lines)


def build_side_by_side_md(side_by_side: list[dict]) -> str:
    lines = [
        "# Per-Block Side-by-Side\n",
        "| phase | run_id | block_id | class | ref_findings | cand_findings | ref_kv | cand_kv | ref_sum_len | cand_sum_len | reasons |",
        "|-------|--------|----------|-------|--------------|---------------|--------|---------|-------------|--------------|---------|",
    ]
    for row in side_by_side:
        lines.append(
            f"| {row['phase']} | {row['run_id']} | {row['block_id']} | {row['classification']} | "
            f"{row['reference_findings_count']} | {row['candidate_findings_count']} | {row['reference_kv_count']} | "
            f"{row['candidate_kv_count']} | {row['reference_summary_len']} | {row['candidate_summary_len']} | "
            f"{','.join(row['reasons'])} |"
        )
    lines.append("")
    return "\n".join(lines)


def build_manual_review_shortlist_md(entries: list[dict]) -> str:
    lines = ["# Manual Review Shortlist\n"]
    if not entries:
        lines.append("- No blocks shortlisted.")
        return "\n".join(lines) + "\n"
    for row in entries:
        lines.append(
            f"- `{row['block_id']}` [{row['phase']}/{row['run_id']}] -> {row['classification']}: "
            f"{', '.join(row['reasons'])}"
        )
    lines.append("")
    return "\n".join(lines)


def build_budget_timeline_md(budget: BudgetTracker) -> str:
    lines = [
        "# Budget Timeline\n",
        "| Step | Label | Predicted USD | Actual USD | Approved | Remaining before | Remaining after | Note |",
        "|------|-------|---------------|------------|----------|------------------|-----------------|------|",
    ]
    for ev in budget.events:
        lines.append(
            f"| {ev.step_id} | {ev.label} | ${ev.predicted_usd:.4f} | ${ev.actual_usd:.4f} | {ev.approved} | "
            f"${ev.remaining_before:.4f} | ${ev.remaining_after:.4f} | {ev.note} |"
        )
    lines.append("")
    lines.append(
        f"- Cap: ${budget.cap_usd:.2f} | Spent: ${budget.spent_usd:.4f} | Remaining: ${budget.remaining:.4f}"
    )
    if budget.stopped:
        lines.append(f"- Stop reason: {budget.stop_reason}")
    lines.append("")
    return "\n".join(lines)


def build_winner_recommendation_md(
    *,
    stress_survivors: list[str],
    max_surviving_batch: str,
    resolution_recommendation: str,
    final_recommendation: dict,
    batch_rows: list[dict],
    resolution_rows: list[dict],
    confirmatory_summary: dict | None,
) -> str:
    lines = [
        "# Winner Recommendation\n",
        f"1. Pro batch mode viable at all: {'YES' if stress_survivors else 'NO'}",
        f"2. Maximum small batch that survived: `{max_surviving_batch or 'none'}`",
        f"3. r1000 useful vs r800: {'YES' if resolution_recommendation == 'r1000' else 'NO'}",
        f"4. Practical recommended config: `{final_recommendation['final_config']['mode']}`"
        + (
            f" + `{final_recommendation['final_config']['batch_profile']}`"
            if final_recommendation['final_config']['batch_profile']
            else ""
        )
        + f" + `{final_recommendation['final_config']['resolution']}`",
        f"5. Continue Pro batch path: {'YES' if final_recommendation['continue_batch_path'] else 'NO'}",
        "",
        "## Batch survivors",
    ]
    if stress_survivors:
        for profile in stress_survivors:
            lines.append(f"- `{profile}` survived hard + quality gate.")
    else:
        lines.append("- No batch profile survived the screening gate.")

    lines.append("\n## Resolution")
    lines.append(f"- Recommended resolution: `{resolution_recommendation}`")

    if confirmatory_summary:
        lines.append("\n## Confirmatory full-project run")
        lines.append(
            f"- Coverage {confirmatory_summary['coverage_pct']:.2f}% "
            f"(missing/dup/extra = {confirmatory_summary['missing_count']}/"
            f"{confirmatory_summary['duplicate_count']}/{confirmatory_summary['extra_count']})"
        )
        lines.append(
            f"- Cost ${confirmatory_summary['total_cost_usd']:.4f}, elapsed {confirmatory_summary['elapsed_s']:.1f}s"
        )
    lines.append("")
    return "\n".join(lines)


def crop_shadow_project_for_profile(
    *,
    main_project_dir: Path,
    exp_dir: Path,
    profile_id: str,
    block_ids: list[str],
    suffix: str,
) -> tuple[Path, list[dict], dict]:
    crop_root = exp_dir / "crop_roots" / f"{profile_id}_{suffix}"
    shadow_project_dir = exp_dir / "shadow_projects" / f"{profile_id}_{suffix}"
    rp = make_block_render_profile(
        target_dpi=RESOLUTION_PROFILES[profile_id]["target_dpi"],
        min_long_side_px=RESOLUTION_PROFILES[profile_id]["min_long_side_px"],
        name=profile_id,
    )
    crop_result = crop_blocks_to_dir(
        str(main_project_dir),
        crop_root / "blocks",
        rp,
        block_ids=block_ids,
        force=False,
    )
    make_shadow_project(main_project_dir, shadow_project_dir, crop_root / "blocks")
    shadow_blocks = load_blocks_index(shadow_project_dir)
    return shadow_project_dir, shadow_blocks, crop_result


def load_subset_blocks(project_dir: Path, block_ids: list[str]) -> list[dict]:
    all_blocks = load_blocks_index(project_dir)
    by_id = {b["block_id"]: b for b in all_blocks}
    return [by_id[bid] for bid in block_ids if bid in by_id]


def profile_batches_for_stress(stress_blocks: list[dict], profile_id: str) -> list[list[dict]]:
    batches = pack_blocks(stress_blocks, SMALL_BATCH_PROFILES[profile_id], hard_cap=6)
    return apply_byte_cap_split(batches, byte_cap_kb=9000)


def profile_batches_for_full(all_blocks: list[dict], profile_id: str) -> list[list[dict]]:
    batches = pack_blocks(all_blocks, SMALL_BATCH_PROFILES[profile_id], hard_cap=6)
    return apply_byte_cap_split(batches, byte_cap_kb=9000)


async def ensure_reference_outputs(
    *,
    project_dir: Path,
    project_info: dict,
    requested_block_ids: list[str],
    existing_reference_map: dict[str, dict],
    out_dir: Path,
    budget: BudgetTracker,
    schema: dict,
) -> dict[str, dict]:
    missing_ids = [bid for bid in requested_block_ids if bid not in existing_reference_map]
    reused_ids = [bid for bid in requested_block_ids if bid in existing_reference_map]
    newly_run_ids: list[str] = []
    reference_map = dict(existing_reference_map)

    if missing_ids:
        pred = estimate_step_cost(
            mode="single_block",
            reasoning_effort="high",
            resolution="r800",
            block_count=len(missing_ids),
        )
        ev = budget.preflight(
            step_id="reference_fill",
            label="Missing Pro single-block reference fill (r800)",
            predicted_usd=pred,
        )
        if not ev.approved:
            _save_json(out_dir / "reference_fill_block_ids.json", missing_ids)
            return reference_map

        blocks = load_subset_blocks(project_dir, missing_ids)
        results, elapsed = await run_call_set(
            project_dir=project_dir,
            blocks_or_batches=[[b] for b in blocks],
            project_info=project_info,
            reasoning_effort="high",
            response_healing_initial=True,
            schema=schema,
            timeout=DEFAULT_TIMEOUT_SEC,
            max_output_tokens=MAX_OUTPUT_TOKENS,
            parallelism=2,
            run_id="reference_fill_r800",
            resolution="r800",
        )
        actual = sum(r.cost_usd for r in results)
        budget.commit(ev, actual, note=f"elapsed={elapsed:.1f}s")
        for res in results:
            if res.success and res.parsed_data:
                analysis = (res.parsed_data.get("block_analyses") or [None])[0]
                if isinstance(analysis, dict):
                    reference_map[res.input_block_ids[0]] = analysis
                    newly_run_ids.append(res.input_block_ids[0])
        _save_json(out_dir / "reference_fill_results.json", [asdict(r) for r in results])
    else:
        budget.record_skip(
            step_id="reference_fill",
            label="Missing Pro single-block reference fill (r800)",
            note="all requested reference outputs reused",
        )

    return reference_map


async def main_async(args: argparse.Namespace) -> int:
    resolution = resolve_project(args.pdf, Path(args.project_dir) if args.project_dir else None)
    project_dir = resolution.project_dir
    project_info = _load_json(project_dir / "project_info.json")
    all_blocks = resolution.blocks
    by_id = {b["block_id"]: b for b in all_blocks}

    ts = _ts()
    out_dir = project_dir / "_experiments" / "pro_variantA_small_budget" / ts
    out_dir.mkdir(parents=True, exist_ok=True)
    schema = load_openrouter_block_batch_schema()
    budget = BudgetTracker(cap_usd=args.budget_cap_usd)

    reference_dir, existing_reference_map = load_reference_outputs(project_dir)
    extra_problematic_ids = [
        bid for bid in load_additional_problematic_ids(project_dir)
        if bid not in HISTORICAL_PROBLEMATIC_IDS
    ]

    stress_ids, stress_manifest_rows = select_stress_set(
        all_blocks,
        preferred_problematic_ids=HISTORICAL_PROBLEMATIC_IDS,
        extra_problematic_ids=extra_problematic_ids,
    )
    resolution_ids, resolution_manifest_rows = select_resolution_set(all_blocks)
    stress_blocks = [by_id[bid] for bid in stress_ids]
    resolution_blocks = [by_id[bid] for bid in resolution_ids]
    reference_union_ids = sorted(set(stress_ids) | set(resolution_ids))

    manifest = {
        "timestamp": ts,
        "pdf": args.pdf,
        "project_dir": str(project_dir),
        "block_source": resolution.block_source,
        "blocks_index": str(resolution.blocks_index_path),
        "total_blocks": resolution.total_blocks,
        "openrouter_only": True,
        "production_defaults_changed": False,
        "claude_touched": False,
        "flash_touched": False,
        "reference_dir_reused": str(reference_dir) if reference_dir else None,
        "historical_problematic_ids": HISTORICAL_PROBLEMATIC_IDS,
        "additional_problematic_ids": extra_problematic_ids,
        "budget_cap_usd": args.budget_cap_usd,
        "stress_set_size": len(stress_ids),
        "resolution_set_size": len(resolution_ids),
    }
    _save_json(out_dir / "manifest.json", manifest)
    _save_json(out_dir / "stress_set_block_ids.json", stress_ids)
    (out_dir / "stress_set_manifest.md").write_text(
        build_manifest_md("Stress Set Manifest", stress_manifest_rows),
        encoding="utf-8",
    )
    _save_json(out_dir / "resolution_set_block_ids.json", resolution_ids)
    (out_dir / "resolution_set_manifest.md").write_text(
        build_manifest_md("Resolution Set Manifest", resolution_manifest_rows),
        encoding="utf-8",
    )

    reference_map = await ensure_reference_outputs(
        project_dir=project_dir,
        project_info=project_info,
        requested_block_ids=reference_union_ids,
        existing_reference_map=existing_reference_map,
        out_dir=out_dir,
        budget=budget,
        schema=schema,
    )
    missing_reference_ids = [bid for bid in reference_union_ids if bid not in reference_map]
    reused_ids = [bid for bid in reference_union_ids if bid in existing_reference_map]
    newly_run_ids = [bid for bid in reference_union_ids if bid in reference_map and bid not in existing_reference_map]
    (out_dir / "reference_reuse_report.md").write_text(
        build_reference_reuse_report(
            reference_dir=reference_dir,
            requested_ids=reference_union_ids,
            reused_ids=reused_ids,
            missing_ids=missing_reference_ids,
            newly_run_ids=newly_run_ids,
        ),
        encoding="utf-8",
    )
    _save_json(out_dir / "reference_outputs_reused.json", {bid: reference_map[bid] for bid in reference_union_ids if bid in reference_map})

    per_block_side_by_side: list[dict] = []
    manual_review_shortlist: list[dict] = []

    # Phase A — batch screening
    batch_rows: list[dict] = []
    batch_screen_order = [by_id[bid] for bid in stress_ids]
    for profile_id in ("b2", "b4", "b6"):
        allowed, reason = should_continue_batch_study(batch_rows, profile_id)
        if not allowed:
            budget.record_skip(
                step_id=f"phaseA_{profile_id}",
                label=f"Batch screening {profile_id}",
                note=f"early_stop: {reason}",
            )
            continue

        predicted = estimate_step_cost(
            mode="batch",
            reasoning_effort="high",
            resolution="r800",
            block_count=len(stress_ids),
        )
        ev = budget.preflight(
            step_id=f"phaseA_{profile_id}",
            label=f"Batch screening {profile_id}",
            predicted_usd=predicted,
        )
        if not ev.approved:
            break

        batches = profile_batches_for_stress(batch_screen_order, profile_id)
        logger.info("Running %s on stress set: %d batches", profile_id, len(batches))
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
        budget.commit(ev, actual, note=f"calls={len(results)} elapsed={elapsed:.1f}s")

        summary = aggregate_results(
            results=results,
            input_block_ids=stress_ids,
            elapsed_s=elapsed,
            label=f"Batch screening {profile_id}",
            mode="batch",
            reasoning_effort="high",
            parallelism=2,
            resolution="r800",
        )
        candidate_map = analyses_map_from_results(results)
        side_by_side = build_side_by_side(
            block_ids=stress_ids,
            reference_map=reference_map,
            candidate_map=candidate_map,
            phase="batch_screening",
            run_id=profile_id,
        )
        quality = evaluate_quality_preservation(side_by_side)
        per_block_side_by_side.extend(side_by_side)
        manual_review_shortlist.extend(
            [row for row in side_by_side if row["classification"] != "likely_equivalent"]
        )

        hard_gate_passed = (
            summary["coverage_pct"] == 100.0
            and summary["missing_count"] == 0
            and summary["duplicate_count"] == 0
            and summary["extra_count"] == 0
            and not summary["fail_mode_distribution"]
        )
        row = {
            "profile_id": profile_id,
            "hard_gate_passed": hard_gate_passed,
            "quality_gate_passed": quality["quality_gate_passed"],
            "survived": hard_gate_passed and quality["quality_gate_passed"],
            "coverage_pct": summary["coverage_pct"],
            "missing_count": summary["missing_count"],
            "duplicate_count": summary["duplicate_count"],
            "extra_count": summary["extra_count"],
            "strict_success_count": summary["strict_success_count"],
            "retry_triggered_count": summary["retry_triggered_count"],
            "retry_recovered_count": summary["retry_recovered_count"],
            "fail_mode_distribution": summary["fail_mode_distribution"],
            "likely_improved_count": quality["likely_improved_count"],
            "likely_equivalent_count": quality["likely_equivalent_count"],
            "likely_degraded_count": quality["likely_degraded_count"],
            "total_findings_delta": summary["total_findings"] - sum(
                len(reference_map[bid].get("findings") or []) for bid in stress_ids
            ),
            "blocks_with_findings_delta": summary["blocks_with_findings"] - sum(
                1 for bid in stress_ids if reference_map[bid].get("findings")
            ),
            "total_kv_delta": summary["total_key_values"] - sum(
                len(reference_map[bid].get("key_values_read") or []) for bid in stress_ids
            ),
            "summary_specificity_improved_count": quality["summary_specificity_improved_count"],
            "actual_cost_usd": summary["total_cost_usd"],
            "elapsed_s": summary["elapsed_s"],
            "hard_fail_reasons": quality["hard_fail_reasons"],
            "manual_review_block_ids": quality["manual_review_block_ids"],
        }
        batch_rows.append(row)
        _save_json(out_dir / f"{profile_id}_batch_results.json", [asdict(r) for r in results])
        _save_json(out_dir / f"{profile_id}_batch_summary.json", {"summary": summary, "quality": quality, "row": row})

    _save_json(out_dir / "batch_screening_summary.json", batch_rows)
    _save_csv(out_dir / "batch_screening_summary.csv", batch_rows)
    (out_dir / "batch_screening_summary.md").write_text(
        build_batch_summary_md(batch_rows),
        encoding="utf-8",
    )

    surviving_batches = [row["profile_id"] for row in batch_rows if row["survived"]]
    max_surviving_batch = ""
    if surviving_batches:
        max_surviving_batch = sorted(surviving_batches, key=lambda p: int(p[1:]), reverse=True)[0]

    # Phase B — resolution feasibility
    resolution_rows: list[dict] = []
    best_resolution = "r800"
    resolution_candidate = False

    predicted = estimate_step_cost(
        mode="single_block",
        reasoning_effort="high",
        resolution="r1000",
        block_count=len(resolution_ids),
    )
    ev = budget.preflight(
        step_id="phaseB_r1000",
        label="Resolution screening r1000 vs r800 reference",
        predicted_usd=predicted,
    )
    if ev.approved:
        shadow_r1000_dir, shadow_r1000_blocks_all, crop_result = crop_shadow_project_for_profile(
            main_project_dir=project_dir,
            exp_dir=out_dir,
            profile_id="r1000",
            block_ids=resolution_ids,
            suffix="resolution_set",
        )
        shadow_r1000_by_id = {b["block_id"]: b for b in shadow_r1000_blocks_all}
        shadow_r1000_blocks = [shadow_r1000_by_id[bid] for bid in resolution_ids]

        results, elapsed = await run_call_set(
            project_dir=shadow_r1000_dir,
            blocks_or_batches=[[b] for b in shadow_r1000_blocks],
            project_info=project_info,
            reasoning_effort="high",
            response_healing_initial=True,
            schema=schema,
            timeout=DEFAULT_TIMEOUT_SEC,
            max_output_tokens=MAX_OUTPUT_TOKENS,
            parallelism=2,
            run_id="r1000_single",
            resolution="r1000",
        )
        actual = sum(r.cost_usd for r in results)
        budget.commit(ev, actual, note=f"crop_cache={crop_result.get('cache_hit')} elapsed={elapsed:.1f}s")

        summary = aggregate_results(
            results=results,
            input_block_ids=resolution_ids,
            elapsed_s=elapsed,
            label="Resolution screening r1000",
            mode="single_block",
            reasoning_effort="high",
            parallelism=2,
            resolution="r1000",
        )
        candidate_map = analyses_map_from_results(results)
        side_by_side = build_side_by_side(
            block_ids=resolution_ids,
            reference_map=reference_map,
            candidate_map=candidate_map,
            phase="resolution_screening",
            run_id="r1000",
        )
        quality = evaluate_quality_preservation(side_by_side)
        per_block_side_by_side.extend(side_by_side)
        manual_review_shortlist.extend(
            [row for row in side_by_side if row["classification"] != "likely_equivalent"]
        )

        completeness_ok = (
            summary["coverage_pct"] == 100.0
            and summary["missing_count"] == 0
            and summary["duplicate_count"] == 0
            and summary["extra_count"] == 0
            and not summary["fail_mode_distribution"]
        )
        cost_ratio = summary["total_cost_usd"] / max(
            0.0001,
            sum(estimate_step_cost(mode="single_block", reasoning_effort="high", resolution="r800", block_count=1) for _ in resolution_ids)
        )
        resolution_candidate = (
            completeness_ok
            and quality["likely_degraded_count"] == 0
            and (
                quality["summary_specificity_improved_count"] > 0
                or quality["kv_adequacy_improved_count"] > 0
                or quality["likely_improved_count"] > 0
            )
            and cost_ratio <= 2.0
        )
        if resolution_candidate:
            best_resolution = "r1000"

        row = {
            "resolution": "r1000",
            "completeness_ok": completeness_ok,
            "candidate": resolution_candidate,
            "likely_improved_count": quality["likely_improved_count"],
            "likely_equivalent_count": quality["likely_equivalent_count"],
            "likely_degraded_count": quality["likely_degraded_count"],
            "summary_specificity_improved_count": quality["summary_specificity_improved_count"],
            "total_findings_delta": summary["total_findings"] - sum(
                len(reference_map[bid].get("findings") or []) for bid in resolution_ids
            ),
            "total_kv_delta": summary["total_key_values"] - sum(
                len(reference_map[bid].get("key_values_read") or []) for bid in resolution_ids
            ),
            "actual_cost_usd": summary["total_cost_usd"],
            "elapsed_s": summary["elapsed_s"],
            "note": "r1000 candidate" if resolution_candidate else "no clear technical benefit",
        }
        resolution_rows.append(row)
        _save_json(out_dir / "r1000_resolution_results.json", [asdict(r) for r in results])
        _save_json(out_dir / "r1000_resolution_summary.json", {"summary": summary, "quality": quality, "row": row, "crop_result": crop_result})
    else:
        resolution_rows.append({
            "resolution": "r1000",
            "completeness_ok": False,
            "candidate": False,
            "likely_improved_count": 0,
            "likely_equivalent_count": 0,
            "likely_degraded_count": 0,
            "summary_specificity_improved_count": 0,
            "total_findings_delta": 0,
            "total_kv_delta": 0,
            "actual_cost_usd": 0.0,
            "elapsed_s": 0.0,
            "note": "skipped_due_to_budget",
        })

    _save_json(out_dir / "resolution_screening_summary.json", resolution_rows)
    _save_csv(out_dir / "resolution_screening_summary.csv", resolution_rows)
    (out_dir / "resolution_screening_summary.md").write_text(
        build_resolution_summary_md(resolution_rows),
        encoding="utf-8",
    )

    # Phase C — confirmatory full run
    recommendation = recommend_final_config(
        best_resolution=best_resolution,
        batch_rows=batch_rows,
        confirmatory_summary=None,
    )
    confirmatory_summary: dict | None = None

    candidate_before = recommendation["candidate_before_confirmatory"]
    mode = candidate_before["mode"]
    batch_profile = candidate_before["batch_profile"]
    resolution_choice = candidate_before["resolution"]
    step_id = "phaseC_confirmatory"
    label = f"Confirmatory full run {mode} {batch_profile or 'single'} {resolution_choice}"
    predicted = estimate_step_cost(
        mode=mode,
        reasoning_effort="high",
        resolution=resolution_choice,
        block_count=len(all_blocks),
    )
    ev = budget.preflight(step_id=step_id, label=label, predicted_usd=predicted)

    if ev.approved:
        confirm_project_dir = project_dir
        confirm_blocks = all_blocks
        if resolution_choice == "r1000":
            shadow_full_dir, shadow_full_blocks_all, crop_result = crop_shadow_project_for_profile(
                main_project_dir=project_dir,
                exp_dir=out_dir,
                profile_id="r1000",
                block_ids=[b["block_id"] for b in all_blocks],
                suffix="full_confirmatory",
            )
            confirm_project_dir = shadow_full_dir
            confirm_blocks = shadow_full_blocks_all
        if mode == "single_block":
            groups = [[b] for b in confirm_blocks]
            parallelism = 2
        else:
            groups = profile_batches_for_full(confirm_blocks, batch_profile)
            parallelism = 2

        results, elapsed = await run_call_set(
            project_dir=confirm_project_dir,
            blocks_or_batches=groups,
            project_info=project_info,
            reasoning_effort="high",
            response_healing_initial=True,
            schema=schema,
            timeout=DEFAULT_TIMEOUT_SEC,
            max_output_tokens=MAX_OUTPUT_TOKENS,
            parallelism=parallelism,
            run_id="confirmatory_full",
            resolution=resolution_choice,
        )
        actual = sum(r.cost_usd for r in results)
        budget.commit(ev, actual, note=f"calls={len(results)} elapsed={elapsed:.1f}s")
        confirmatory_summary = aggregate_results(
            results=results,
            input_block_ids=[b["block_id"] for b in confirm_blocks],
            elapsed_s=elapsed,
            label=label,
            mode=mode,
            reasoning_effort="high",
            parallelism=parallelism,
            resolution=resolution_choice,
        )
        _save_json(out_dir / "confirmatory_full_results.json", [asdict(r) for r in results])
        _save_json(out_dir / "confirmatory_full_summary.json", confirmatory_summary)
        (out_dir / "confirmatory_full_summary.md").write_text(
            build_confirmatory_summary_md(confirmatory_summary),
            encoding="utf-8",
        )
    else:
        confirmatory_summary = {
            "label": label,
            "mode": mode,
            "resolution": resolution_choice,
            "coverage_pct": 0.0,
            "missing_count": len(all_blocks),
            "duplicate_count": 0,
            "extra_count": 0,
            "strict_success_count": 0,
            "strict_failure_count": len(all_blocks),
            "retry_triggered_count": 0,
            "retry_recovered_count": 0,
            "fail_mode_distribution": {"budget_stop": 1},
            "blocks_with_findings": 0,
            "total_findings": 0,
            "total_key_values": 0,
            "total_cost_usd": 0.0,
            "elapsed_s": 0.0,
        }
        _save_json(out_dir / "confirmatory_full_summary.json", confirmatory_summary)
        (out_dir / "confirmatory_full_summary.md").write_text(
            build_confirmatory_summary_md(confirmatory_summary),
            encoding="utf-8",
        )

    recommendation = recommend_final_config(
        best_resolution=best_resolution,
        batch_rows=batch_rows,
        confirmatory_summary=confirmatory_summary,
    )

    dedup_shortlist: dict[tuple[str, str, str], dict] = {}
    for row in manual_review_shortlist:
        key = (row["phase"], row["run_id"], row["block_id"])
        dedup_shortlist[key] = row
    shortlist_rows = list(dedup_shortlist.values())

    _save_json(out_dir / "per_block_side_by_side.json", per_block_side_by_side)
    (out_dir / "per_block_side_by_side.md").write_text(
        build_side_by_side_md(per_block_side_by_side),
        encoding="utf-8",
    )
    (out_dir / "manual_review_shortlist.md").write_text(
        build_manual_review_shortlist_md(shortlist_rows),
        encoding="utf-8",
    )

    winner_md = build_winner_recommendation_md(
        stress_survivors=surviving_batches,
        max_surviving_batch=max_surviving_batch,
        resolution_recommendation=best_resolution,
        final_recommendation=recommendation,
        batch_rows=batch_rows,
        resolution_rows=resolution_rows,
        confirmatory_summary=confirmatory_summary,
    )
    (out_dir / "winner_recommendation.md").write_text(winner_md, encoding="utf-8")

    _save_json(out_dir / "budget_timeline.json", budget.to_dict())
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
        default=MAX_BUDGET_USD,
        help="Hard cap on additional spend.",
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
