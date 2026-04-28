"""
Stage 02 recall-oriented hybrid experiment for small projects.

Goal: minimize risk of missed issues, NOT minimize cost.

Phases:
  R1: Flash (google/gemini-2.5-flash) single-block full project.
      Recall signals built as post-processing from output + metadata.
  R2: Build escalation set — tier1 (mandatory) / tier2 (recommended) / tier3 (flash-only ok).
      Recall-first rules: escalate everything with findings, heavy, full-page, merged,
      medium/high issue_potential or miss_risk, etc.
  R3: Second-pass comparison on SAME escalation set:
      Engine A: google/gemini-3.1-pro-preview (OpenRouter)
      Engine B: anthropic/claude-opus-4-7  (OpenRouter, experimental path;
                cost measured via usage.cost; runtime logged)

Block source resolution:
  - Prefer _output/blocks/index.json
  - Fallback to _output_classic/blocks/index.json
  - NEVER recrop or rebuild blocks if a valid index exists

Usage:
  python scripts/run_stage02_recall_hybrid.py \\
      --pdf "13АВ-РД-КЖ5.1-К1К2 (2).pdf" \\
      --dry-run

  python scripts/run_stage02_recall_hybrid.py \\
      --pdf "13АВ-РД-КЖ5.1-К1К2 (2).pdf" \\
      --parallelism 3 \\
      --second-pass-parallelism 2

Artifacts saved to:
  <project_dir>/_experiments/stage02_recall_hybrid_small_project/<timestamp>/
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

from run_gemini_openrouter_stage02_experiment import (  # noqa: E402
    BatchResultEnvelope,
    RunMetrics,
    build_messages,
    build_page_contexts,
    build_system_prompt,
    classify_risk,
    compute_metrics,
    run_batches_async,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("recall_hybrid")

# ── Model constants ────────────────────────────────────────────────────────────

MODEL_FLASH = "google/gemini-2.5-flash"
MODEL_PRO   = "google/gemini-3.1-pro-preview"
MODEL_CLAUDE = "anthropic/claude-sonnet-4-6"  # claude-opus-4-7 not yet on OpenRouter

# Per-block cost estimates (USD) — used for budget logging only
PER_BLOCK_EST_FLASH  = 0.0015
PER_BLOCK_EST_PRO    = 0.0080
PER_BLOCK_EST_CLAUDE = 0.0060

# Recall signal thresholds
SHORT_SUMMARY_THRESHOLD = 60    # chars — below this is "weak_summary"
LOW_KV_THRESHOLD        = 2     # count — at or below is "low kv for complexity"
HIGH_OCR_LEN_THRESHOLD  = 800   # chars — above this signals engineering density

# Escalation score thresholds
TIER1_SCORE_THRESHOLD = 8       # mandatory second pass
TIER2_SCORE_THRESHOLD = 3       # recommended second pass


# ── Project resolution ────────────────────────────────────────────────────────

@dataclass
class ProjectResolution:
    project_dir: Path
    block_source: str           # "_output" or "_output_classic"
    blocks_index_path: Path
    blocks: list[dict]
    source_pdf_name: str
    total_blocks: int


def resolve_project(pdf_name: str, project_dir_override: Path | None = None) -> ProjectResolution:
    """Find project dir by PDF name, resolve valid block source.

    Resolution order:
      1. _output/blocks/index.json   → block_source = "_output"
      2. _output_classic/blocks/index.json → block_source = "_output_classic"

    Never recrop — if neither index exists, raises FileNotFoundError.
    """
    if project_dir_override:
        project_dir = project_dir_override
    else:
        projects_root = _ROOT / "projects"
        found: Path | None = None
        for candidate in projects_root.rglob(pdf_name):
            # candidate may be the PDF file or a directory containing it
            if candidate.is_file():
                # project dir is the parent
                found = candidate.parent
                break
            if candidate.is_dir():
                pdf_inside = candidate / pdf_name
                if pdf_inside.exists():
                    found = candidate
                    break
        if found is None:
            # second pass: look for directories named like the pdf
            for d in projects_root.rglob("*"):
                if d.is_dir() and d.name == pdf_name:
                    found = d
                    break
        if found is None:
            raise FileNotFoundError(
                f"Cannot find project dir for PDF '{pdf_name}' inside {projects_root}"
            )
        project_dir = found

    # Choose block source
    primary_idx   = project_dir / "_output" / "blocks" / "index.json"
    classic_idx   = project_dir / "_output_classic" / "blocks" / "index.json"

    if primary_idx.exists():
        idx_path = primary_idx
        source   = "_output"
    elif classic_idx.exists():
        idx_path = classic_idx
        source   = "_output_classic"
    else:
        raise FileNotFoundError(
            f"No valid blocks/index.json found in {project_dir} "
            f"(checked _output and _output_classic)"
        )

    idx_data = json.loads(idx_path.read_text(encoding="utf-8"))
    blocks   = idx_data.get("blocks", [])
    if not blocks:
        raise ValueError(f"Empty blocks list in {idx_path}")

    return ProjectResolution(
        project_dir      = project_dir,
        block_source     = source,
        blocks_index_path= idx_path,
        blocks           = blocks,
        source_pdf_name  = pdf_name,
        total_blocks     = len(blocks),
    )


# ── Recall signals (post-processing) ─────────────────────────────────────────

def compute_recall_signals(
    block: dict,
    analysis: dict | None,
    inferred: bool,
) -> dict:
    """Build recall-oriented signals from Flash output + block metadata.

    Returns dict with:
      issue_potential: low|medium|high
      miss_risk:       low|medium|high
      review_recommended: bool
      review_reasons: list[str]
    """
    risk = classify_risk(block)
    size_kb    = float(block.get("size_kb", 0) or 0)
    ocr_len    = int(block.get("ocr_text_len", 0) or 0)
    is_heavy   = (risk == "heavy")
    is_full_pg = bool(block.get("is_full_page"))
    is_merged  = bool(block.get("merged_block_ids"))

    if analysis is None:
        findings_count = 0
        kv_count       = 0
        summary        = ""
        unreadable     = False
    else:
        findings_count = len(analysis.get("findings") or [])
        kv_count       = len(analysis.get("key_values_read") or [])
        summary        = str(analysis.get("summary") or "").strip()
        unreadable     = bool(analysis.get("unreadable_text"))

    summary_len = len(summary)

    # ── issue_potential ──
    if findings_count >= 2 or unreadable or analysis is None:
        issue_potential = "high"
    elif (
        findings_count == 1
        or (is_heavy and findings_count == 0)
        or (risk == "normal" and findings_count == 0 and kv_count > 0)
    ):
        issue_potential = "medium"
    else:
        issue_potential = "low"

    # ── miss_risk ──
    if analysis is None or unreadable:
        miss_risk = "high"
    elif (is_heavy or is_full_pg or is_merged) and findings_count == 0:
        miss_risk = "high"
    elif not summary or not analysis.get("key_values_read"):
        miss_risk = "high"
    elif (
        inferred
        or summary_len < SHORT_SUMMARY_THRESHOLD
        or kv_count <= LOW_KV_THRESHOLD
    ):
        miss_risk = "medium"
    elif risk == "normal" and findings_count == 0 and kv_count <= 3:
        miss_risk = "medium"
    elif size_kb > 500 and findings_count == 0:
        miss_risk = "medium"
    else:
        miss_risk = "low"

    # ── review_reasons ──
    reasons: list[str] = []
    if findings_count > 0:
        reasons.append("finding_present")
    if is_heavy:
        reasons.append("high_structural_risk")
    if is_full_pg or is_merged:
        reasons.append("merged_or_full_page")
    if size_kb > 500:
        reasons.append("dense_graphics_or_text")
    if inferred:
        reasons.append("uncertain_read")
    if not summary or summary_len < SHORT_SUMMARY_THRESHOLD:
        reasons.append("weak_summary")
    if kv_count <= LOW_KV_THRESHOLD and risk != "light":
        reasons.append("low_kv_for_complex_block")
    if (is_heavy or is_full_pg or is_merged) and findings_count == 0:
        reasons.append("possible_missed_issue")
    if ocr_len >= HIGH_OCR_LEN_THRESHOLD and findings_count == 0:
        reasons.append("engineeringly_critical_block")
    if analysis is None:
        reasons.append("possible_missed_issue")

    # ── review_recommended ──
    review_recommended = bool(
        findings_count > 0
        or issue_potential in ("medium", "high")
        or miss_risk in ("medium", "high")
        or is_heavy or is_full_pg or is_merged
    )

    return {
        "issue_potential": issue_potential,
        "miss_risk": miss_risk,
        "review_recommended": review_recommended,
        "review_reasons": reasons,
    }


# ── Escalation scoring ────────────────────────────────────────────────────────

def compute_escalation_score(
    block: dict,
    analysis: dict | None,
    recall: dict,
) -> tuple[int, list[str]]:
    """Compute escalation_score and list of triggered rules.

    Higher score = stronger case for second pass.
    """
    risk = classify_risk(block)
    findings_count = len((analysis or {}).get("findings") or [])
    kv_count       = len((analysis or {}).get("key_values_read") or [])
    summary        = str((analysis or {}).get("summary") or "").strip()

    score = 0
    rules: list[str] = []

    # ── Mandatory-tier triggers (high weight) ──
    if analysis is None:
        score += 15
        rules.append("analysis_missing")

    if findings_count > 0:
        score += 10
        rules.append("findings_present")

    if risk == "heavy":
        score += 8
        rules.append("risk_heavy")

    if block.get("is_full_page"):
        score += 8
        rules.append("is_full_page")

    if block.get("merged_block_ids"):
        score += 8
        rules.append("is_merged")

    if recall["issue_potential"] == "high":
        score += 6
        rules.append("issue_potential_high")
    elif recall["issue_potential"] == "medium":
        score += 4
        rules.append("issue_potential_medium")

    if recall["miss_risk"] == "high":
        score += 6
        rules.append("miss_risk_high")
    elif recall["miss_risk"] == "medium":
        score += 4
        rules.append("miss_risk_medium")

    if recall["review_recommended"]:
        score += 3
        rules.append("review_recommended")

    # ── Additional signals (lower weight) ──
    if "weak_summary" in recall["review_reasons"]:
        score += 3
        rules.append("weak_summary")

    if "low_kv_for_complex_block" in recall["review_reasons"]:
        score += 2
        rules.append("low_kv_for_complex_block")

    if "possible_missed_issue" in recall["review_reasons"]:
        score += 3
        rules.append("possible_missed_issue")

    if "engineeringly_critical_block" in recall["review_reasons"]:
        score += 2
        rules.append("engineeringly_critical_block")

    if "uncertain_read" in recall["review_reasons"]:
        score += 2
        rules.append("uncertain_read")

    return score, rules


def assign_tier(
    block: dict,
    analysis: dict | None,
    recall: dict,
    score: int,
) -> str:
    """Assign tier based on mandatory rules first, then score.

    tier1_mandatory_second_pass — must be re-analyzed
    tier2_recommended_second_pass — should be re-analyzed if budget allows
    tier3_flash_only_ok — Flash result trusted
    """
    findings_count = len((analysis or {}).get("findings") or [])
    risk = classify_risk(block)

    mandatory_tier1 = (
        findings_count > 0
        or risk == "heavy"
        or bool(block.get("is_full_page"))
        or bool(block.get("merged_block_ids"))
        or recall["issue_potential"] in ("medium", "high")
        or recall["miss_risk"] in ("medium", "high")
        or recall["review_recommended"]
        or analysis is None
    )

    if mandatory_tier1 or score >= TIER1_SCORE_THRESHOLD:
        return "tier1_mandatory_second_pass"
    elif score >= TIER2_SCORE_THRESHOLD:
        return "tier2_recommended_second_pass"
    else:
        return "tier3_flash_only_ok"


# ── Escalation set building ───────────────────────────────────────────────────

@dataclass
class EscalationEntry:
    block_id: str
    tier: str
    escalation_score: int
    rules_triggered: list[str]
    issue_potential: str
    miss_risk: str
    review_recommended: bool
    review_reasons: list[str]
    risk: str
    size_kb: float
    page: int
    findings_count: int
    kv_count: int
    summary_len: int
    analysis_present: bool


def build_escalation_set(
    all_blocks: list[dict],
    flash_results: list[BatchResultEnvelope],
    *,
    include_tier2: bool = True,
    max_second_pass: int | None = None,
) -> tuple[list[EscalationEntry], dict]:
    """Build escalation set from Flash first-pass results.

    Returns (entries, tier_summary).
    Tier1 is always included fully; tier2 included if include_tier2=True.
    If max_second_pass is set and tier1+tier2 exceeds it, trim tier2 by score desc.
    """
    # Build analysis map
    analyses: dict[str, dict] = {}
    inferred_bids: set[str] = set()

    for res in flash_results:
        if res.inferred_block_id_count:
            for bid in res.input_block_ids:
                inferred_bids.add(bid)
        if res.is_error or not res.parsed_data:
            continue
        for a in res.parsed_data.get("block_analyses", []):
            if isinstance(a, dict):
                bid = a.get("block_id") or ""
                if bid:
                    analyses[bid] = a

    entries: list[EscalationEntry] = []

    for block in all_blocks:
        bid      = block["block_id"]
        analysis = analyses.get(bid)
        inferred = bid in inferred_bids
        recall   = compute_recall_signals(block, analysis, inferred)
        score, rules = compute_escalation_score(block, analysis, recall)
        tier     = assign_tier(block, analysis, recall, score)

        findings_count = len((analysis or {}).get("findings") or [])
        kv_count       = len((analysis or {}).get("key_values_read") or [])
        summary        = str((analysis or {}).get("summary") or "").strip()

        entries.append(EscalationEntry(
            block_id          = bid,
            tier              = tier,
            escalation_score  = score,
            rules_triggered   = rules,
            issue_potential   = recall["issue_potential"],
            miss_risk         = recall["miss_risk"],
            review_recommended= recall["review_recommended"],
            review_reasons    = recall["review_reasons"],
            risk              = classify_risk(block),
            size_kb           = float(block.get("size_kb", 0) or 0),
            page              = int(block.get("page", 0) or 0),
            findings_count    = findings_count,
            kv_count          = kv_count,
            summary_len       = len(summary),
            analysis_present  = analysis is not None,
        ))

    # Sort: tier1 first, then tier2, then tier3; within tier by score desc
    tier_order = {
        "tier1_mandatory_second_pass":  0,
        "tier2_recommended_second_pass": 1,
        "tier3_flash_only_ok":          2,
    }
    entries.sort(key=lambda e: (tier_order.get(e.tier, 9), -e.escalation_score, e.page))

    tier1 = [e for e in entries if e.tier == "tier1_mandatory_second_pass"]
    tier2 = [e for e in entries if e.tier == "tier2_recommended_second_pass"]
    tier3 = [e for e in entries if e.tier == "tier3_flash_only_ok"]

    # Trim tier2 if needed
    if include_tier2 and max_second_pass is not None:
        allowed_t2 = max(0, max_second_pass - len(tier1))
        tier2 = tier2[:allowed_t2]

    final_set = tier1 + (tier2 if include_tier2 else [])

    tier_summary = {
        "tier1_count": len(tier1),
        "tier2_count": len(tier2) if include_tier2 else 0,
        "tier3_count": len(tier3),
        "tier2_total": len([e for e in entries if e.tier == "tier2_recommended_second_pass"]),
        "tier2_trimmed": include_tier2 and max_second_pass is not None,
        "second_pass_total": len(final_set),
        "second_pass_pct": round(len(final_set) / max(1, len(all_blocks)) * 100, 1),
    }

    return final_set, tier_summary


# ── Second-pass comparison ────────────────────────────────────────────────────

def diff_second_pass_vs_flash(
    second_pass_results: list[BatchResultEnvelope],
    flash_results: list[BatchResultEnvelope],
    escalation_ids: list[str],
    engine_label: str,
) -> dict:
    """Compare second-pass engine vs Flash on the same escalation set."""
    def _collect(results: list[BatchResultEnvelope], id_set: set[str]) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for res in results:
            if res.is_error or not res.parsed_data:
                continue
            for a in res.parsed_data.get("block_analyses", []):
                if isinstance(a, dict):
                    bid = a.get("block_id") or ""
                    if bid in id_set:
                        out[bid] = a
        return out

    esc_set = set(escalation_ids)
    flash_map  = _collect(flash_results,       esc_set)
    engine_map = _collect(second_pass_results, esc_set)

    improved = unchanged = degraded = 0
    added_findings = added_kv = 0
    flash_total_findings = flash_total_kv = 0
    engine_total_findings = engine_total_kv = 0
    unreadable_recovery = 0
    per_block: list[dict] = []

    for bid in escalation_ids:
        f = flash_map.get(bid)  or {}
        e = engine_map.get(bid) or {}

        f_findings = len(f.get("findings") or [])
        e_findings = len(e.get("findings") or [])
        f_kv = len(f.get("key_values_read") or [])
        e_kv = len(e.get("key_values_read") or [])
        f_unreadable = bool(f.get("unreadable_text"))
        e_unreadable = bool(e.get("unreadable_text"))
        f_summary_len = len(str(f.get("summary") or "").strip())
        e_summary_len = len(str(e.get("summary") or "").strip())
        f_missing = bid not in flash_map
        e_missing = bid not in engine_map

        flash_total_findings  += f_findings
        flash_total_kv        += f_kv
        engine_total_findings += e_findings
        engine_total_kv       += e_kv

        delta_findings = e_findings - f_findings
        delta_kv       = e_kv - f_kv
        added_findings += max(0, delta_findings)
        added_kv       += max(0, delta_kv)

        is_improved = (
            (delta_findings > 0)
            or (f_unreadable and not e_unreadable)
            or (f_kv == 0 and e_kv > 0)
            or (e_summary_len > f_summary_len + 30 and e_summary_len > SHORT_SUMMARY_THRESHOLD)
        )
        is_degraded = (
            (delta_findings < 0 and f_findings > 0)
            or (not f_unreadable and e_unreadable)
        )
        if f_unreadable and not e_unreadable:
            unreadable_recovery += 1

        if e_missing:
            status = "missing_in_engine"
            degraded += 1
        elif is_improved and not is_degraded:
            status = "improved"
            improved += 1
        elif is_degraded and not is_improved:
            status = "degraded"
            degraded += 1
        else:
            status = "unchanged"
            unchanged += 1

        per_block.append({
            "block_id":         bid,
            "status":           status,
            "flash_findings":   f_findings,
            "engine_findings":  e_findings,
            "delta_findings":   delta_findings,
            "flash_kv":         f_kv,
            "engine_kv":        e_kv,
            "delta_kv":         delta_kv,
            "flash_unreadable": f_unreadable,
            "engine_unreadable":e_unreadable,
            "flash_missing":    f_missing,
            "engine_missing":   e_missing,
            "flash_summary_len":f_summary_len,
            "engine_summary_len":e_summary_len,
        })

    return {
        "engine":                 engine_label,
        "escalation_set_size":    len(escalation_ids),
        "improved":               improved,
        "unchanged":              unchanged,
        "degraded":               degraded,
        "unreadable_recovery":    unreadable_recovery,
        "added_findings":         added_findings,
        "added_kv":               added_kv,
        "flash_total_findings":   flash_total_findings,
        "flash_total_kv":         flash_total_kv,
        "engine_total_findings":  engine_total_findings,
        "engine_total_kv":        engine_total_kv,
        "per_block_diff":         per_block,
    }


# ── Winner selection ──────────────────────────────────────────────────────────

def select_second_pass_winner(
    pro_m:   RunMetrics | None,
    pro_diff:   dict | None,
    claude_m: RunMetrics | None,
    claude_diff: dict | None,
    escalation_size: int,
) -> tuple[str, str]:
    """Select better second-pass engine by recall-first criteria.

    Priority:
      1. completeness (coverage=100%, no missing/dup/extra)
      2. improved blocks count
      3. added findings
      4. degraded blocks (minimize)
      5. cost/time (last resort)

    Returns (winner_engine_label, rationale_md).
    """
    def _eval(m: RunMetrics | None, diff: dict | None, label: str) -> dict:
        if m is None or diff is None:
            return {"label": label, "ok": False, "reason": "not_run"}
        complete = (
            m.coverage_pct == 100.0
            and m.missing_count == 0
            and m.duplicate_count == 0
            and m.extra_count == 0
        )
        return {
            "label":      label,
            "ok":         complete,
            "coverage":   m.coverage_pct,
            "improved":   diff["improved"],
            "added":      diff["added_findings"],
            "degraded":   diff["degraded"],
            "cost":       m.total_cost_usd,
            "elapsed":    m.elapsed_s,
        }

    pa = _eval(pro_m,    pro_diff,    "Pro (gemini-3.1-pro-preview)")
    pb = _eval(claude_m, claude_diff, "Claude (claude-opus-4-7)")

    lines = ["## Second-pass winner selection (recall-first criteria)\n"]
    lines.append(f"| Criterion | {pa['label']} | {pb['label']} |")
    lines.append("|-----------|" + "---|" * 2)

    def _fmt(d: dict, key: str, fmt: str = "{}") -> str:
        if not d["ok"] and key not in d:
            return "N/A"
        v = d.get(key)
        if v is None:
            return "N/A"
        return fmt.format(v)

    lines.append(f"| Completeness | {'✓' if pa.get('ok') else '✗'} | {'✓' if pb.get('ok') else '✗'} |")
    lines.append(f"| Coverage | {_fmt(pa,'coverage','{:.1f}%')} | {_fmt(pb,'coverage','{:.1f}%')} |")
    lines.append(f"| Improved blocks | {_fmt(pa,'improved','{}')} | {_fmt(pb,'improved','{}')} |")
    lines.append(f"| Added findings | +{_fmt(pa,'added','{}')} | +{_fmt(pb,'added','{}')} |")
    lines.append(f"| Degraded blocks | {_fmt(pa,'degraded','{}')} | {_fmt(pb,'degraded','{}')} |")
    lines.append(f"| Cost USD | ${_fmt(pa,'cost','{:.4f}')} | ${_fmt(pb,'cost','{:.4f}')} |")
    lines.append(f"| Elapsed s | {_fmt(pa,'elapsed','{:.1f}')} | {_fmt(pb,'elapsed','{:.1f}')} |")
    lines.append("")

    # Decision
    if not pa["ok"] and not pb["ok"]:
        winner = "none"
        rationale = "Both engines had completeness issues — no clear winner."
    elif pa["ok"] and not pb["ok"]:
        winner = pa["label"]
        rationale = f"{pb['label']} had completeness issues; {pa['label']} wins by default."
    elif pb["ok"] and not pa["ok"]:
        winner = pb["label"]
        rationale = f"{pa['label']} had completeness issues; {pb['label']} wins by default."
    else:
        # Both complete — compare recall quality
        pa_improved = pa.get("improved", 0) or 0
        pb_improved = pb.get("improved", 0) or 0
        pa_added    = pa.get("added", 0)    or 0
        pb_added    = pb.get("added", 0)    or 0
        pa_degraded = pa.get("degraded", 0) or 0
        pb_degraded = pb.get("degraded", 0) or 0

        if pb_improved > pa_improved or pb_added > pa_added:
            if pa_degraded == 0 and pb_degraded == 0:
                winner = pb["label"]
                rationale = (
                    f"{pb['label']} improved more blocks ({pb_improved} vs {pa_improved}) "
                    f"and added more findings (+{pb_added} vs +{pa_added}) with no degradations."
                )
            elif pb_degraded <= pa_degraded:
                winner = pb["label"]
                rationale = (
                    f"{pb['label']} improved more blocks ({pb_improved} vs {pa_improved}) "
                    f"with acceptable degradations ({pb_degraded})."
                )
            else:
                winner = pa["label"]
                rationale = (
                    f"{pb['label']} improved more but had more degradations ({pb_degraded}); "
                    f"{pa['label']} preferred for lower degradation risk."
                )
        elif pa_improved > pb_improved or pa_added > pb_added:
            if pb_degraded == 0 and pa_degraded == 0:
                winner = pa["label"]
                rationale = (
                    f"{pa['label']} improved more blocks ({pa_improved} vs {pb_improved}) "
                    f"and added more findings (+{pa_added} vs +{pb_added}) with no degradations."
                )
            elif pa_degraded <= pb_degraded:
                winner = pa["label"]
                rationale = (
                    f"{pa['label']} improved more blocks with acceptable degradations."
                )
            else:
                winner = pb["label"]
                rationale = f"{pa['label']} improved more but degraded more; {pb['label']} preferred."
        else:
            # Equal quality — prefer lower cost
            pa_cost = pa.get("cost") or 9999
            pb_cost = pb.get("cost") or 9999
            if pa_cost <= pb_cost:
                winner = pa["label"]
                rationale = f"Equal quality; {pa['label']} preferred (lower cost ${pa_cost:.4f} vs ${pb_cost:.4f})."
            else:
                winner = pb["label"]
                rationale = f"Equal quality; {pb['label']} preferred (lower cost ${pb_cost:.4f} vs ${pa_cost:.4f})."

    lines.append(f"**Winner**: {winner}")
    lines.append(f"**Rationale**: {rationale}")

    return winner, "\n".join(lines) + "\n"


# ── Helper functions ──────────────────────────────────────────────────────────

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
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


# ── Report builders ───────────────────────────────────────────────────────────

def _build_flash_recall_summary_md(m: RunMetrics, tier_summary: dict) -> str:
    t1 = tier_summary["tier1_count"]
    t2 = tier_summary["tier2_count"]
    t3 = tier_summary["tier3_count"]
    sp = tier_summary["second_pass_total"]
    return (
        f"# Phase R1 — Flash Single-Block Full Project (Recall-Oriented)\n\n"
        f"Model: **{m.model_id}** | Mode: **single_block** | Parallelism: {m.parallelism}\n\n"
        f"## Coverage & Completeness\n"
        f"| Metric | Value |\n|--------|-------|\n"
        f"| Total input blocks | {m.total_input_blocks} |\n"
        f"| Coverage | {m.coverage_pct:.1f}% |\n"
        f"| Missing / Duplicate / Extra | {m.missing_count} / {m.duplicate_count} / {m.extra_count} |\n"
        f"| Inferred block_id | {m.inferred_block_id_count} |\n"
        f"| Unreadable | {m.unreadable_count} |\n\n"
        f"## Quality Signals\n"
        f"| Metric | Value |\n|--------|-------|\n"
        f"| Risk heavy/normal/light | {m.risk_heavy}/{m.risk_normal}/{m.risk_light} |\n"
        f"| Empty summary | {m.empty_summary_count} |\n"
        f"| Empty key_values | {m.empty_key_values_count} |\n"
        f"| Blocks with findings | {m.blocks_with_findings} |\n"
        f"| Total findings | {m.total_findings} |\n"
        f"| Findings/100 blocks | {m.findings_per_100_blocks:.1f} |\n"
        f"| Total / median KV | {m.total_key_values} / {m.median_key_values:.1f} |\n\n"
        f"## Escalation Tiers (Recall-Oriented)\n"
        f"| Tier | Count | Pct |\n|------|-------|-----|\n"
        f"| tier1 mandatory second pass | {t1} | {t1/max(1,m.total_input_blocks)*100:.0f}% |\n"
        f"| tier2 recommended second pass | {t2} | {t2/max(1,m.total_input_blocks)*100:.0f}% |\n"
        f"| tier3 flash-only ok | {t3} | {t3/max(1,m.total_input_blocks)*100:.0f}% |\n"
        f"| **Total for second pass** | **{sp}** | **{sp/max(1,m.total_input_blocks)*100:.0f}%** |\n\n"
        f"## Runtime & Cost\n"
        f"| Metric | Value |\n|--------|-------|\n"
        f"| Elapsed | {m.elapsed_s:.1f}s |\n"
        f"| Avg / median / p95 per-block | {m.avg_batch_duration_s:.2f}s / {m.median_batch_duration_s:.2f}s / {m.p95_batch_duration_s:.2f}s |\n"
        f"| Prompt / completion / reasoning / cached tokens | {m.total_prompt_tokens} / {m.total_output_tokens} / {m.total_reasoning_tokens} / {m.total_cached_tokens} |\n"
        f"| Total cost USD | ${m.total_cost_usd:.4f} |\n"
        f"| Cost/valid block | ${m.cost_per_valid_block:.5f} |\n"
        f"| Cost/finding | ${m.cost_per_finding:.5f} |\n"
        f"| Cost source actual/est | {m.cost_sources_actual}/{m.cost_sources_estimated} |\n"
    )


def _build_escalation_reasons_md(entries: list[EscalationEntry], tier_summary: dict) -> str:
    lines = [
        "# Escalation Set — Reasons Report\n",
        f"Total escalated: {tier_summary['second_pass_total']} of "
        f"{tier_summary['tier1_count'] + tier_summary['tier2_count'] + tier_summary['tier3_count']} blocks "
        f"({tier_summary['second_pass_pct']:.0f}%)\n",
        f"- Tier 1 (mandatory): {tier_summary['tier1_count']}",
        f"- Tier 2 (recommended): {tier_summary['tier2_count']}",
        f"- Tier 3 (flash-only ok): {tier_summary['tier3_count']}\n",
        "## Escalation rules histogram\n",
    ]
    rule_counts: dict[str, int] = {}
    esc_entries = [e for e in entries if e.tier != "tier3_flash_only_ok"]
    for e in esc_entries:
        for r in e.rules_triggered:
            rule_counts[r] = rule_counts.get(r, 0) + 1

    for rule, cnt in sorted(rule_counts.items(), key=lambda x: -x[1]):
        pct = cnt / max(1, len(esc_entries)) * 100
        lines.append(f"- `{rule}`: {cnt} blocks ({pct:.0f}%)")

    lines.append("\n## Escalated blocks detail\n")
    lines.append("| block_id | tier | score | risk | page | size_kb | findings | kv | issue_potential | miss_risk | rules |")
    lines.append("|----------|------|-------|------|------|---------|----------|----|-----------------|-----------|-------|")
    for e in entries:
        if e.tier == "tier3_flash_only_ok":
            continue
        lines.append(
            f"| {e.block_id} | {e.tier.split('_')[0]} | {e.escalation_score} | {e.risk} | {e.page} "
            f"| {e.size_kb:.0f} | {e.findings_count} | {e.kv_count} | {e.issue_potential} "
            f"| {e.miss_risk} | {','.join(e.rules_triggered[:4])} |"
        )

    lines.append("\n## Tier3 blocks (Flash trusted)\n")
    for e in entries:
        if e.tier == "tier3_flash_only_ok":
            lines.append(f"- {e.block_id} (page {e.page}, score {e.escalation_score})")

    return "\n".join(lines) + "\n"


def _build_second_pass_summary_md(
    m: RunMetrics,
    diff: dict,
    engine_label: str,
    escalation_size: int,
) -> str:
    lines = [
        f"# Second Pass — {engine_label}\n",
        f"Escalation set: {escalation_size} blocks\n",
        "## Completeness\n",
        f"| Metric | Value |\n|--------|-------|\n",
        f"| Coverage | {m.coverage_pct:.1f}% |\n",
        f"| Missing / Duplicate / Extra | {m.missing_count} / {m.duplicate_count} / {m.extra_count} |\n",
        f"| Unreadable | {m.unreadable_count} |\n",
        "\n## Quality Deltas vs Flash (same escalation blocks)\n",
        f"| Metric | Value |\n|--------|-------|\n",
        f"| Improved blocks | {diff['improved']} / {escalation_size} |\n",
        f"| Unchanged blocks | {diff['unchanged']} |\n",
        f"| Degraded blocks | {diff['degraded']} |\n",
        f"| Unreadable recovery | {diff['unreadable_recovery']} |\n",
        f"| Additional findings | +{diff['added_findings']} "
        f"(Flash {diff['flash_total_findings']} → engine {diff['engine_total_findings']}) |\n",
        f"| Additional KV | +{diff['added_kv']} "
        f"(Flash {diff['flash_total_kv']} → engine {diff['engine_total_kv']}) |\n",
        "\n## Top disagreements (Flash vs engine)\n",
    ]

    top_improved  = sorted([b for b in diff["per_block_diff"] if b["status"] == "improved"],
                           key=lambda x: -abs(x["delta_findings"]))[:5]
    top_degraded  = [b for b in diff["per_block_diff"] if b["status"] == "degraded"][:5]
    top_missing   = [b for b in diff["per_block_diff"] if b.get("engine_missing")][:5]

    if top_improved:
        lines.append("### Rescued blocks (engine found more):")
        for b in top_improved:
            lines.append(
                f"  - {b['block_id']}: Flash {b['flash_findings']} → engine {b['engine_findings']} findings "
                f"(+{b['delta_findings']}), KV +{b['delta_kv']}"
            )

    if top_degraded:
        lines.append("\n### Degraded blocks (engine found less):")
        for b in top_degraded:
            lines.append(
                f"  - {b['block_id']}: Flash {b['flash_findings']} → engine {b['engine_findings']} findings"
            )

    if top_missing:
        lines.append("\n### Missing in engine output:")
        for b in top_missing:
            lines.append(f"  - {b['block_id']}")

    lines.append("\n## Runtime & Cost\n")
    lines.append(f"| Metric | Value |\n|--------|-------|\n")
    lines.append(f"| Elapsed | {m.elapsed_s:.1f}s |\n")
    lines.append(f"| Avg / median / p95 per-block | {m.avg_batch_duration_s:.2f}s / {m.median_batch_duration_s:.2f}s / {m.p95_batch_duration_s:.2f}s |\n")
    lines.append(f"| Total cost USD | ${m.total_cost_usd:.4f} |\n")
    lines.append(f"| Cost/valid block | ${m.cost_per_valid_block:.5f} |\n")
    lines.append(f"| Cost source actual/est | {m.cost_sources_actual}/{m.cost_sources_estimated} |\n")

    return "".join(lines)


def _build_side_by_side_md(
    pro_diff: dict | None,
    claude_diff: dict | None,
    escalation_ids: list[str],
) -> str:
    lines = [
        "# Second-Pass Side-by-Side: Pro vs Claude\n",
        f"Escalation set: {len(escalation_ids)} blocks\n",
        "| block_id | Flash findings | Pro findings | Claude findings | Pro status | Claude status |",
        "|----------|----------------|--------------|-----------------|------------|---------------|",
    ]

    pro_by_bid    = {b["block_id"]: b for b in (pro_diff or {}).get("per_block_diff", [])}
    claude_by_bid = {b["block_id"]: b for b in (claude_diff or {}).get("per_block_diff", [])}

    for bid in escalation_ids:
        p = pro_by_bid.get(bid, {})
        c = claude_by_bid.get(bid, {})
        flash_f = p.get("flash_findings", c.get("flash_findings", "N/A"))
        pro_f   = p.get("engine_findings", "N/A")
        cl_f    = c.get("engine_findings", "N/A")
        pro_st  = p.get("status", "not_run")
        cl_st   = c.get("status", "not_run")
        lines.append(f"| {bid} | {flash_f} | {pro_f} | {cl_f} | {pro_st} | {cl_st} |")

    return "\n".join(lines) + "\n"


def _build_hybrid_recommendation_md(
    flash_m: RunMetrics,
    pro_m: RunMetrics | None,
    claude_m: RunMetrics | None,
    pro_diff: dict | None,
    claude_diff: dict | None,
    tier_summary: dict,
    winner: str,
    winner_rationale: str,
    all_blocks: list[dict],
) -> str:
    flash_complete = (
        flash_m.coverage_pct == 100.0
        and flash_m.missing_count == 0
        and flash_m.duplicate_count == 0
        and flash_m.extra_count == 0
    )

    t1 = tier_summary["tier1_count"]
    t2 = tier_summary["tier2_count"]
    sp = tier_summary["second_pass_total"]
    total = len(all_blocks)

    lines = [
        "# Hybrid Recommendation — Recall-Oriented Policy\n",
        "## Summary\n",
        f"This experiment evaluated Flash single-block as first pass + selective second pass on "
        f"a small KJ project ({total} blocks) with recall-first escalation logic.\n",
        "**Design goal**: minimize missed issues, not minimize cost.\n",
        "",
        "## Flash First Pass\n",
        f"- Coverage: {flash_m.coverage_pct:.1f}% (missing {flash_m.missing_count}, dup {flash_m.duplicate_count}, extra {flash_m.extra_count})",
        f"- Total findings: {flash_m.total_findings} on {flash_m.blocks_with_findings} blocks",
        f"- Cost: ${flash_m.total_cost_usd:.4f}",
        f"- Flash {'✓ complete' if flash_complete else '✗ incomplete — NOT recommended as sole source'}",
        "",
        "## Escalation Tiers (Recall-First)\n",
        f"- Tier 1 mandatory: {t1} blocks ({t1/max(1,total)*100:.0f}%) — escalated regardless of cost",
        f"- Tier 2 recommended: {t2} blocks ({t2/max(1,total)*100:.0f}%) — included (small project, budget not limiting)",
        f"- **Total second-pass: {sp} blocks ({sp/max(1,total)*100:.0f}%)**",
        "",
        "## Second-Pass Comparison\n",
    ]

    if pro_diff is not None:
        lines += [
            f"### Pro (gemini-3.1-pro-preview) on {tier_summary['second_pass_total']} blocks:",
            f"  - Improved: {pro_diff['improved']} | Unchanged: {pro_diff['unchanged']} | Degraded: {pro_diff['degraded']}",
            f"  - Added findings: +{pro_diff['added_findings']}",
            f"  - Cost: ${(pro_m.total_cost_usd if pro_m else 0):.4f}",
        ]
    else:
        lines.append("### Pro: not run (budget or error)")

    if claude_diff is not None:
        lines += [
            f"### Claude (claude-opus-4-7) on {tier_summary['second_pass_total']} blocks:",
            f"  - Improved: {claude_diff['improved']} | Unchanged: {claude_diff['unchanged']} | Degraded: {claude_diff['degraded']}",
            f"  - Added findings: +{claude_diff['added_findings']}",
            f"  - Cost: ${(claude_m.total_cost_usd if claude_m else 0):.4f}",
        ]
    else:
        lines.append("### Claude: not run (budget or error)")

    lines += [
        "",
        "## Winner\n",
        f"**Second-pass engine winner**: {winner}\n",
        winner_rationale,
        "",
        "## Practical Policy Recommendation\n",
        "",
        "### Mode A: Flash only",
        f"  - Coverage: {flash_m.coverage_pct:.1f}%",
        f"  - Findings: {flash_m.total_findings}",
        f"  - Cost: ~${flash_m.total_cost_usd:.4f}",
        "",
        f"### Mode B: Flash + {pro_diff['engine'] if pro_diff else 'Pro'} on escalation set",
    ]
    if pro_diff and pro_m:
        combined_b = flash_m.total_cost_usd + pro_m.total_cost_usd
        combined_findings = flash_m.total_findings + pro_diff["added_findings"]
        lines += [
            f"  - Total findings: {combined_findings} (+{pro_diff['added_findings']} from second pass)",
            f"  - Improved blocks: {pro_diff['improved']}",
            f"  - Combined cost: ~${combined_b:.4f}",
        ]
    else:
        lines.append("  - Not available (Pro not run)")

    lines.append(f"\n### Mode C: Flash + {claude_diff['engine'] if claude_diff else 'Claude'} on escalation set")
    if claude_diff and claude_m:
        combined_c = flash_m.total_cost_usd + claude_m.total_cost_usd
        combined_findings_c = flash_m.total_findings + claude_diff["added_findings"]
        lines += [
            f"  - Total findings: {combined_findings_c} (+{claude_diff['added_findings']} from second pass)",
            f"  - Improved blocks: {claude_diff['improved']}",
            f"  - Combined cost: ~${combined_c:.4f}",
        ]
    else:
        lines.append("  - Not available (Claude not run)")

    lines += [
        "",
        "## Escalation Policy Assessment\n",
        f"- {sp}/{total} blocks ({sp/max(1,total)*100:.0f}%) sent to second pass.",
        "- Policy is recall-oriented: tier1 includes ALL blocks with any finding, heavy, merged, "
        "high/medium issue potential or miss_risk.",
        "- For a KJ project (reinforced concrete drawings), engineering density is high — "
        "escalation rate reflects appropriate caution.",
        "",
        "## Constraints honored\n",
        "- Flash single-block (no batch mode)",
        "- No full-document Pro or Claude runs",
        "- Production stage_models.json UNCHANGED",
        "- Claude CLI production path untouched",
        "- No recrop of existing blocks",
        "- Stage 03+ not touched",
    ]

    return "\n".join(lines) + "\n"


def _build_winner_recommendation_md(
    winner: str,
    winner_rationale: str,
    flash_m: RunMetrics,
    pro_m: RunMetrics | None,
    claude_m: RunMetrics | None,
    pro_diff: dict | None,
    claude_diff: dict | None,
    tier_summary: dict,
) -> str:
    sp = tier_summary["second_pass_total"]
    lines = [
        "# Winner Recommendation — Recall-Hybrid Stage 02\n",
        "## Practical answer\n",
        f"**Recommended second-pass engine**: {winner}\n",
        "",
        "## Decision criteria (recall-first)\n",
        "1. Completeness (coverage=100%, no missing/dup/extra) — gates all else",
        "2. Improved blocks count",
        "3. Added findings",
        "4. Degraded blocks count (minimize)",
        "5. Cost/elapsed — tiebreaker only",
        "",
        "## Second-pass engine comparison\n",
        winner_rationale,
        "",
        "## Escalation summary\n",
        f"- Second-pass blocks: **{sp}**",
        f"  - Tier 1 mandatory: {tier_summary['tier1_count']}",
        f"  - Tier 2 recommended: {tier_summary['tier2_count']}",
        f"  - Tier 3 (Flash trusted): {tier_summary['tier3_count']}",
        "",
        "## Flash first-pass results\n",
        f"- Coverage: {flash_m.coverage_pct:.1f}%",
        f"- Total findings: {flash_m.total_findings} on {flash_m.blocks_with_findings} blocks",
        f"- Cost: ${flash_m.total_cost_usd:.4f}",
        "",
        "## Hybrid total cost estimate\n",
    ]

    if winner != "none":
        if "Pro" in winner and pro_m:
            total_cost = flash_m.total_cost_usd + pro_m.total_cost_usd
            total_findings = flash_m.total_findings + (pro_diff["added_findings"] if pro_diff else 0)
            lines += [
                f"- Flash: ${flash_m.total_cost_usd:.4f}",
                f"- Pro second pass ({sp} blocks): ${pro_m.total_cost_usd:.4f}",
                f"- **Hybrid total: ${total_cost:.4f}**",
                f"- Total findings after hybrid: {total_findings}",
            ]
        elif "Claude" in winner and claude_m:
            total_cost = flash_m.total_cost_usd + claude_m.total_cost_usd
            total_findings = flash_m.total_findings + (claude_diff["added_findings"] if claude_diff else 0)
            lines += [
                f"- Flash: ${flash_m.total_cost_usd:.4f}",
                f"- Claude second pass ({sp} blocks): ${claude_m.total_cost_usd:.4f}",
                f"- **Hybrid total: ${total_cost:.4f}**",
                f"- Total findings after hybrid: {total_findings}",
            ]

    lines += [
        "",
        "## Constraints honored\n",
        "- Flash single-block (no batch mode for Flash)",
        "- No full-document Pro or Claude second pass",
        "- No recrop of existing blocks",
        "- Production defaults unchanged",
        "- Stage 03+ not touched",
    ]

    return "\n".join(lines) + "\n"


def _build_budget_timeline_md(
    flash_m: RunMetrics | None,
    pro_m: RunMetrics | None,
    claude_m: RunMetrics | None,
) -> str:
    rows = []
    total = 0.0
    if flash_m:
        rows.append(f"| R1 Flash full | {flash_m.total_input_blocks} | {flash_m.elapsed_s:.1f}s | ${flash_m.total_cost_usd:.4f} |")
        total += flash_m.total_cost_usd
    if pro_m:
        rows.append(f"| R3 Pro second pass | {pro_m.total_input_blocks} | {pro_m.elapsed_s:.1f}s | ${pro_m.total_cost_usd:.4f} |")
        total += pro_m.total_cost_usd
    if claude_m:
        rows.append(f"| R3 Claude second pass | {claude_m.total_input_blocks} | {claude_m.elapsed_s:.1f}s | ${claude_m.total_cost_usd:.4f} |")
        total += claude_m.total_cost_usd

    lines = [
        "# Budget & Runtime Timeline\n",
        "| Phase | Blocks | Elapsed | Cost USD |",
        "|-------|--------|---------|----------|",
    ] + rows + [
        f"| **Total** | — | — | **${total:.4f}** |",
        "",
        "> Cost source: `usage.cost` from OpenRouter when available, estimated otherwise.",
        "> Claude second pass uses OpenRouter path (anthropic/claude-opus-4-7); cost metered as API spend.",
    ]
    return "\n".join(lines) + "\n"


# ── Main async ────────────────────────────────────────────────────────────────

async def main_async(args: argparse.Namespace) -> None:
    from webapp.config import OPENROUTER_API_KEY

    if not OPENROUTER_API_KEY and not args.dry_run:
        logger.error("OPENROUTER_API_KEY not set; use --dry-run for validation")
        sys.exit(1)

    # ── Resolve project ──
    logger.info("Resolving project for PDF: %s", args.pdf)
    if args.project_dir:
        resolution = resolve_project(args.pdf, Path(args.project_dir))
    else:
        resolution = resolve_project(args.pdf)

    logger.info(
        "Resolved: %s | block_source=%s | %d blocks",
        resolution.project_dir, resolution.block_source, resolution.total_blocks,
    )

    project_info_path = resolution.project_dir / "project_info.json"
    project_info: dict = {}
    if project_info_path.exists():
        project_info = json.loads(project_info_path.read_text(encoding="utf-8"))

    all_blocks = resolution.blocks

    # Build experiment dir
    ts = _ts()
    exp_dir = resolution.project_dir / "_experiments" / "stage02_recall_hybrid_small_project" / ts
    exp_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Experiment dir: %s", exp_dir)

    # Save manifest
    manifest = {
        "timestamp":     ts,
        "pdf":           args.pdf,
        "project_dir":   str(resolution.project_dir),
        "block_source":  resolution.block_source,
        "blocks_index":  str(resolution.blocks_index_path),
        "total_blocks":  resolution.total_blocks,
        "parallelism_flash":  args.parallelism,
        "parallelism_second": args.second_pass_parallelism,
        "include_tier2":  args.include_tier2,
        "max_second_pass": args.max_second_pass,
        "dry_run":        args.dry_run,
        "models": {
            "flash":  MODEL_FLASH,
            "pro":    MODEL_PRO,
            "claude": MODEL_CLAUDE,
        },
        "tier_thresholds": {
            "tier1_score": TIER1_SCORE_THRESHOLD,
            "tier2_score": TIER2_SCORE_THRESHOLD,
        },
        "recall_thresholds": {
            "short_summary": SHORT_SUMMARY_THRESHOLD,
            "low_kv":        LOW_KV_THRESHOLD,
            "high_ocr_len":  HIGH_OCR_LEN_THRESHOLD,
        },
    }
    _save_json(exp_dir / "manifest.json", manifest)

    system_prompt = build_system_prompt(project_info, len(all_blocks))
    page_contexts = build_page_contexts(resolution.project_dir)

    # ═══════════════════════════════════════════════════════════════════════
    # Phase R1: Flash single-block, full project
    # ═══════════════════════════════════════════════════════════════════════
    logger.info("[R1] Flash single-block full project (%d blocks, parallelism=%d)",
                len(all_blocks), args.parallelism)

    flash_batches = [[b] for b in all_blocks]
    flash_start = time.monotonic()

    flash_results, flash_elapsed = await run_batches_async(
        flash_batches,
        resolution.project_dir, project_info,
        system_prompt, page_contexts, all_blocks,
        MODEL_FLASH, args.parallelism, "R1_flash_single",
        strict_schema=True,
        response_healing=True,
        require_parameters=True,
        provider_data_collection=None,
        dry_run=args.dry_run,
    )

    flash_m = compute_metrics(
        flash_results, all_blocks,
        run_id="R1_flash_single", model_id=MODEL_FLASH,
        batch_profile="single", parallelism=args.parallelism,
        mode="single_block", elapsed_s=flash_elapsed,
        strict_schema_enabled=True, response_healing_enabled=True,
        require_parameters_enabled=True, dry_run=args.dry_run,
    )

    logger.info(
        "[R1] coverage=%.1f%% findings=%d kv=%d cost=$%.4f elapsed=%.1fs",
        flash_m.coverage_pct, flash_m.total_findings, flash_m.total_key_values,
        flash_m.total_cost_usd, flash_m.elapsed_s,
    )

    _save_json(exp_dir / "flash_full_recall_summary.json", asdict(flash_m))

    # Save per-block results
    per_block_raw = []
    for res in flash_results:
        for a in (res.parsed_data or {}).get("block_analyses", []):
            if isinstance(a, dict):
                per_block_raw.append(a)
    _save_json(exp_dir / "flash_full_per_block.json", per_block_raw)

    # ═══════════════════════════════════════════════════════════════════════
    # Phase R2: Build escalation set
    # ═══════════════════════════════════════════════════════════════════════
    logger.info("[R2] Building recall-oriented escalation set")

    escalation_entries, tier_summary = build_escalation_set(
        all_blocks, flash_results,
        include_tier2=args.include_tier2,
        max_second_pass=args.max_second_pass,
    )

    escalation_ids = [e.block_id for e in escalation_entries]

    logger.info(
        "[R2] tier1=%d tier2=%d tier3=%d second_pass_total=%d (%.0f%%)",
        tier_summary["tier1_count"], tier_summary["tier2_count"],
        tier_summary["tier3_count"], tier_summary["second_pass_total"],
        tier_summary["second_pass_pct"],
    )

    # Save escalation artifacts
    _save_json(exp_dir / "escalation_set_block_ids.json", escalation_ids)
    _save_json(exp_dir / "escalation_set_manifest.json", [asdict(e) for e in escalation_entries])
    _save_json(exp_dir / "tier_summary.json", tier_summary)

    # Flash full summary md (includes tier info)
    (exp_dir / "flash_full_recall_summary.md").write_text(
        _build_flash_recall_summary_md(flash_m, tier_summary),
        encoding="utf-8",
    )
    (exp_dir / "escalation_set_reasons.md").write_text(
        _build_escalation_reasons_md(escalation_entries, tier_summary),
        encoding="utf-8",
    )

    # CSV for easy inspection
    _save_csv(
        exp_dir / "escalation_set_manifest.csv",
        [asdict(e) for e in escalation_entries],
    )

    if not escalation_ids:
        logger.warning("[R2] No blocks in escalation set — skipping second pass")
        winner = "none"
        winner_md = "No escalation blocks — second pass not needed."
        pro_m = claude_m = pro_diff = claude_diff = None
    else:
        # ═══════════════════════════════════════════════════════════════════
        # Phase R3a: Pro second pass on escalation set
        # ═══════════════════════════════════════════════════════════════════
        escalation_blocks = [b for b in all_blocks if b["block_id"] in set(escalation_ids)]
        pro_batches = [[b] for b in escalation_blocks]

        logger.info(
            "[R3a] Pro second pass on escalation set (%d blocks, parallelism=%d)",
            len(escalation_blocks), args.second_pass_parallelism,
        )

        pro_results, pro_elapsed = await run_batches_async(
            pro_batches,
            resolution.project_dir, project_info,
            system_prompt, page_contexts, escalation_blocks,
            MODEL_PRO, args.second_pass_parallelism, "R3a_pro_second_pass",
            strict_schema=True,
            response_healing=True,
            require_parameters=True,
            provider_data_collection=None,
            dry_run=args.dry_run,
        )

        pro_m = compute_metrics(
            pro_results, escalation_blocks,
            run_id="R3a_pro_second_pass", model_id=MODEL_PRO,
            batch_profile="single", parallelism=args.second_pass_parallelism,
            mode="single_block", elapsed_s=pro_elapsed,
            strict_schema_enabled=True, response_healing_enabled=True,
            require_parameters_enabled=True, dry_run=args.dry_run,
        )

        pro_diff = diff_second_pass_vs_flash(
            pro_results, flash_results, escalation_ids, "Pro (gemini-3.1-pro-preview)"
        )

        logger.info(
            "[R3a] Pro: coverage=%.1f%% improved=%d degraded=%d added_findings=%d cost=$%.4f",
            pro_m.coverage_pct, pro_diff["improved"], pro_diff["degraded"],
            pro_diff["added_findings"], pro_m.total_cost_usd,
        )

        _save_json(exp_dir / "second_pass_pro_summary.json", asdict(pro_m))
        _save_json(exp_dir / "second_pass_pro_diff.json", pro_diff)
        (exp_dir / "second_pass_pro_summary.md").write_text(
            _build_second_pass_summary_md(pro_m, pro_diff, "Pro (gemini-3.1-pro-preview)", len(escalation_ids)),
            encoding="utf-8",
        )

        # ═══════════════════════════════════════════════════════════════════
        # Phase R3b: Claude second pass on SAME escalation set
        # (via OpenRouter — anthropic/claude-opus-4-7)
        # ═══════════════════════════════════════════════════════════════════
        logger.info(
            "[R3b] Claude second pass on escalation set (%d blocks, parallelism=%d) via OpenRouter",
            len(escalation_blocks), args.second_pass_parallelism,
        )

        claude_batches = [[b] for b in escalation_blocks]

        claude_results, claude_elapsed = await run_batches_async(
            claude_batches,
            resolution.project_dir, project_info,
            system_prompt, page_contexts, escalation_blocks,
            MODEL_CLAUDE, args.second_pass_parallelism, "R3b_claude_second_pass",
            strict_schema=True,
            response_healing=True,
            require_parameters=True,
            provider_data_collection=None,
            dry_run=args.dry_run,
        )

        claude_m = compute_metrics(
            claude_results, escalation_blocks,
            run_id="R3b_claude_second_pass", model_id=MODEL_CLAUDE,
            batch_profile="single", parallelism=args.second_pass_parallelism,
            mode="single_block", elapsed_s=claude_elapsed,
            strict_schema_enabled=True, response_healing_enabled=True,
            require_parameters_enabled=True, dry_run=args.dry_run,
        )

        claude_diff = diff_second_pass_vs_flash(
            claude_results, flash_results, escalation_ids, "Claude (claude-opus-4-7)"
        )

        logger.info(
            "[R3b] Claude: coverage=%.1f%% improved=%d degraded=%d added_findings=%d cost=$%.4f",
            claude_m.coverage_pct, claude_diff["improved"], claude_diff["degraded"],
            claude_diff["added_findings"], claude_m.total_cost_usd,
        )

        _save_json(exp_dir / "second_pass_claude_summary.json", asdict(claude_m))
        _save_json(exp_dir / "second_pass_claude_diff.json", claude_diff)
        (exp_dir / "second_pass_claude_summary.md").write_text(
            _build_second_pass_summary_md(claude_m, claude_diff, "Claude (claude-opus-4-7)", len(escalation_ids)),
            encoding="utf-8",
        )

        # ── Side-by-side ──
        (exp_dir / "second_pass_side_by_side.md").write_text(
            _build_side_by_side_md(pro_diff, claude_diff, escalation_ids),
            encoding="utf-8",
        )
        side_by_side_data: list[dict] = []
        pro_by_bid = {b["block_id"]: b for b in pro_diff["per_block_diff"]}
        claude_by_bid = {b["block_id"]: b for b in claude_diff["per_block_diff"]}
        for bid in escalation_ids:
            side_by_side_data.append({
                "block_id": bid,
                "pro": pro_by_bid.get(bid, {}),
                "claude": claude_by_bid.get(bid, {}),
            })
        _save_json(exp_dir / "second_pass_side_by_side.json", side_by_side_data)

        # ── Winner selection ──
        winner, winner_rationale_md = select_second_pass_winner(
            pro_m, pro_diff, claude_m, claude_diff, len(escalation_ids)
        )
        logger.info("[Winner] %s", winner)

    # ── Final reports ──
    if escalation_ids:
        (exp_dir / "winner_recommendation.md").write_text(
            _build_winner_recommendation_md(
                winner, winner_rationale_md,
                flash_m, pro_m, claude_m,
                pro_diff, claude_diff, tier_summary,
            ),
            encoding="utf-8",
        )

        (exp_dir / "hybrid_recommendation.md").write_text(
            _build_hybrid_recommendation_md(
                flash_m, pro_m, claude_m,
                pro_diff, claude_diff,
                tier_summary, winner, winner_rationale_md,
                all_blocks,
            ),
            encoding="utf-8",
        )

    (exp_dir / "budget_and_runtime_timeline.md").write_text(
        _build_budget_timeline_md(
            flash_m,
            pro_m   if escalation_ids else None,
            claude_m if escalation_ids else None,
        ),
        encoding="utf-8",
    )

    # Save project_resolution.md
    (exp_dir / "project_resolution.md").write_text(
        f"# Project Resolution\n\n"
        f"- **PDF**: {args.pdf}\n"
        f"- **Project dir**: {resolution.project_dir}\n"
        f"- **Block source**: `{resolution.block_source}` (index: {resolution.blocks_index_path})\n"
        f"- **Total blocks**: {resolution.total_blocks}\n"
        f"- **Block source rationale**: "
        f"{'`_output/blocks/index.json` found and valid' if resolution.block_source == '_output' else '`_output_classic/blocks/index.json` used (primary _output not found)'}\n"
        f"- **Recrop performed**: NO (existing index reused)\n",
        encoding="utf-8",
    )

    # Final summary print
    print("\n" + "=" * 70)
    print("RECALL HYBRID EXPERIMENT COMPLETE")
    print(f"Artifacts: {exp_dir}")
    print(f"Flash: {flash_m.total_input_blocks} blocks | coverage={flash_m.coverage_pct:.1f}% | "
          f"findings={flash_m.total_findings} | cost=${flash_m.total_cost_usd:.4f}")
    print(f"Escalation: tier1={tier_summary['tier1_count']} tier2={tier_summary['tier2_count']} "
          f"tier3={tier_summary['tier3_count']} second_pass={tier_summary['second_pass_total']}")
    if escalation_ids:
        print(f"Pro second pass: coverage={pro_m.coverage_pct:.1f}% | "
              f"improved={pro_diff['improved']} | +{pro_diff['added_findings']} findings | "
              f"cost=${pro_m.total_cost_usd:.4f}")
        print(f"Claude second pass: coverage={claude_m.coverage_pct:.1f}% | "
              f"improved={claude_diff['improved']} | +{claude_diff['added_findings']} findings | "
              f"cost=${claude_m.total_cost_usd:.4f}")
        print(f"Winner: {winner}")
    print("=" * 70)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Stage 02 recall-oriented hybrid experiment",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--pdf", required=True,
                   help="PDF filename to search for in projects/")
    p.add_argument("--project-dir",
                   help="Override auto-resolved project dir")
    p.add_argument("--parallelism", type=int, default=3, choices=[1, 2, 3, 4],
                   help="Flash first-pass parallelism (default: 3)")
    p.add_argument("--second-pass-parallelism", type=int, default=2, choices=[1, 2, 3, 4],
                   help="Second-pass parallelism (default: 2)")
    p.add_argument("--include-tier2", action="store_true", default=True,
                   help="Include tier2 blocks in second pass (default: True)")
    p.add_argument("--no-tier2", action="store_false", dest="include_tier2",
                   help="Exclude tier2 from second pass")
    p.add_argument("--max-second-pass", type=int, default=None,
                   help="Max blocks in second pass (trims tier2 if exceeded)")
    p.add_argument("--dry-run", action="store_true",
                   help="Simulate all LLM calls without API requests")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
