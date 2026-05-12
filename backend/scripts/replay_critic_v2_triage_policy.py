#!/usr/bin/env python3
"""
replay_critic_v2_triage_policy.py
-----------------------------------
Apply offline triage policy (with configurable profiles) to previously saved
benchmark/matrix artifacts WITHOUT calling any LLM.

Reads:
  {benchmark_output_dir}/human_benchmark_records.json
  {benchmark_output_dir}/critic_v2_llm_taxonomy_decisions.json  (optional)

Or from a matrix experiment sub-dir:
  {matrix_output_dir}/{experiment}/human_benchmark_records.json
  {matrix_output_dir}/{experiment}/critic_v2_llm_taxonomy_decisions.json

Outputs to --output-dir:
  critic_v2_triage.json
  critic_v2_triage_metrics.json
  critic_v2_hidden_by_critic.json
  critic_v2_suggested_reject.json
  critic_v2_visible_by_default.json
  critic_v2_risky_hidden_cases.json
  triage_replay_summary.md
  triage_replay_summary.json
  ar_f001_diagnostic.json
  (with --profile all: sub-dirs per profile with profile comparison)

Usage:
  python backend/scripts/replay_critic_v2_triage_policy.py \\
      --benchmark-output-dir /tmp/benchmark_human \\
      --output-dir /tmp/triage_replay

  python backend/scripts/replay_critic_v2_triage_policy.py \\
      --matrix-output-dir /tmp/critic_v2_matrix_real_full_guard \\
      --profile all \\
      --output-dir /tmp/critic_v2_triage_profiles_replay

NOT connected to production pipeline.
Does NOT modify production artifacts.
Does NOT call any LLM.
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

# ─── sys.path bootstrap ───────────────────────────────────────────────────────

_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# ─── Imports ──────────────────────────────────────────────────────────────────

from backend.app.pipeline.stages.findings_review.critic_v2 import (
    EVIDENCE_NONE,
    EVIDENCE_PARTIAL,
    EVIDENCE_VALID,
    EVIDENCE_WEAK,
)
from backend.app.pipeline.stages.findings_review.critic_v2.models import QualityDecision
from backend.app.pipeline.stages.findings_review.critic_v2.triage import (
    PROFILE_AGGRESSIVE,
    PROFILE_ASSISTED,
    PROFILE_CONSERVATIVE,
    QUEUE_BORDERLINE,
    QUEUE_HIDDEN,
    QUEUE_MAIN_REVIEW,
    QUEUE_NEEDS_CONTEXT,
    QUEUE_STRONG_KEEP,
    QUEUE_SUGGESTED_REJECT,
    VALID_PROFILES,
    TriageDecision,
    TriageMetrics,
    assign_triage_queue,
    build_business_workload_view,
    build_triage_artifacts,
    build_ui_export,
    compute_triage_metrics,
    get_ar_f001_diagnostic,
    get_profile_config,
    render_ui_export_markdown,
    triage_decision_to_dict,
    triage_metrics_to_dict,
)

ALL_PROFILES = [PROFILE_CONSERVATIVE, PROFILE_ASSISTED, PROFILE_AGGRESSIVE]

# ─── Loaders ─────────────────────────────────────────────────────────────────


def _load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        print(f"  WARN: could not read {path}: {e}", file=sys.stderr)
        return default


def _load_benchmark_records(output_dir: Path) -> list[dict]:
    path = output_dir / "human_benchmark_records.json"
    data = _load_json(path, [])
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("records", [])
    return []


def _load_llm_decisions(output_dir: Path) -> dict[str, dict]:
    """Load critic_v2_llm_taxonomy_decisions.json → dict finding_id → decision dict."""
    path = output_dir / "critic_v2_llm_taxonomy_decisions.json"
    data = _load_json(path, [])
    if isinstance(data, list):
        return {d.get("finding_id", ""): d for d in data if isinstance(d, dict)}
    if isinstance(data, dict):
        items = data.get("decisions", [])
        return {d.get("finding_id", ""): d for d in items if isinstance(d, dict)}
    return {}


# ─── QualityDecision reconstruction ──────────────────────────────────────────


def _record_to_quality_decision(rec: dict) -> QualityDecision:
    dec = rec.get("critic_decision") or rec.get("final_decision") or "accept"
    return QualityDecision(
        finding_id=rec.get("finding_id", ""),
        decision=dec,
        usefulness_score=int(rec.get("critic_score") or rec.get("usefulness_score") or 0),
        reject_reason=rec.get("critic_reject_reason") or rec.get("reject_reason"),
        reject_explanation=rec.get("reject_explanation"),
        merged_into=rec.get("merged_into"),
        impact_area=rec.get("impact_area"),
        severity=rec.get("severity"),
        has_evidence=bool(rec.get("has_evidence", True)),
        has_action=bool(rec.get("has_action", True)),
        has_impact=bool(rec.get("has_impact", True)),
        evidence_quality=rec.get("evidence_quality") or EVIDENCE_NONE,
    )


class _LLMDecProxy:
    """Thin proxy wrapping an LLM decision dict to match LLMCriticDecision interface."""

    def __init__(self, d: dict) -> None:
        self._d = d

    @property
    def finding_id(self) -> str:
        return self._d.get("finding_id", "")

    @property
    def llm_decision(self) -> Optional[str]:
        return self._d.get("llm_decision")

    @property
    def human_taxonomy_reason(self) -> Optional[str]:
        return self._d.get("human_taxonomy_reason") or self._d.get("taxonomy_reason")

    @property
    def confidence(self) -> Optional[float]:
        v = self._d.get("confidence")
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    @property
    def evidence_checked(self) -> bool:
        return bool(self._d.get("evidence_checked", False))

    @property
    def source_dependency(self) -> str:
        return self._d.get("source_dependency") or "enough_source"

    @property
    def explanation(self) -> str:
        return self._d.get("explanation", "")


# ─── Composite ID helper ──────────────────────────────────────────────────────


def _composite_id(rec: dict) -> str:
    """Build project:finding_id composite key to avoid cross-project collisions."""
    proj = rec.get("project_name") or rec.get("project") or rec.get("project_path") or ""
    fid = rec.get("finding_id", "")
    if proj:
        return f"{proj}:{fid}"
    return fid


# ─── Replay core ─────────────────────────────────────────────────────────────


def replay_triage_on_records(
    records: list[dict],
    llm_decisions_by_id: dict[str, dict],
    include_human_labels: bool = True,
    profile: str = PROFILE_CONSERVATIVE,
) -> tuple[list[TriageDecision], TriageMetrics]:
    """
    Apply triage policy (with given profile) to a list of benchmark records.

    Uses composite project:finding_id keys to avoid cross-project collisions.
    Returns (triage_decisions, triage_metrics).
    """
    triage_decisions: list[TriageDecision] = []

    human_labels: dict[str, str] = {}
    if include_human_labels:
        for rec in records:
            cid = _composite_id(rec)
            hdec = rec.get("human_decision", "")
            if cid and hdec in ("accepted", "rejected"):
                human_labels[cid] = hdec

    for rec in records:
        fid = rec.get("finding_id", "")
        if not fid:
            continue

        cid = _composite_id(rec)
        det_dec = _record_to_quality_decision(rec)
        llm_raw = llm_decisions_by_id.get(fid)
        llm_proxy = _LLMDecProxy(llm_raw) if llm_raw else None

        # Use composite id as the triage finding_id
        det_dec = dataclasses.replace(det_dec, finding_id=cid)

        finding: dict = {
            "id": cid,
            "severity": rec.get("severity", ""),
            "category": rec.get("category", ""),
            "title": rec.get("title", ""),
            "description": rec.get("description", ""),
        }

        td = assign_triage_queue(
            finding=finding,
            deterministic_decision=det_dec,
            final_decision=det_dec,
            llm_decision=llm_proxy,
            profile=profile,
        )
        triage_decisions.append(td)

    metrics = compute_triage_metrics(
        triage_decisions,
        human_decisions=human_labels if include_human_labels else None,
        profile=profile,
    )

    return triage_decisions, metrics


# ─── Multi-profile replay ─────────────────────────────────────────────────────


def replay_all_profiles(
    records: list[dict],
    llm_by_id: dict[str, dict],
    include_human_labels: bool = True,
    quiet: bool = False,
) -> dict[str, tuple[list[TriageDecision], TriageMetrics]]:
    """Run triage for all three profiles on the same records."""
    results: dict[str, tuple[list[TriageDecision], TriageMetrics]] = {}
    for prof in ALL_PROFILES:
        triage, metrics = replay_triage_on_records(
            records, llm_by_id,
            include_human_labels=include_human_labels,
            profile=prof,
        )
        results[prof] = (triage, metrics)
        if not quiet:
            _print_triage_summary_inline(prof, metrics)
    return results


# ─── Section breakdown ────────────────────────────────────────────────────────


def compute_section_breakdown(
    records: list[dict],
    triage_decisions: list[TriageDecision],
) -> dict[str, dict]:
    section_by_fid: dict[str, str] = {}
    for rec in records:
        cid = _composite_id(rec)
        sec = rec.get("section", "unknown")
        if cid:
            section_by_fid[cid] = sec

    by_section: dict[str, dict] = defaultdict(lambda: {
        "total": 0,
        "strong_keep": 0,
        "main_review": 0,
        "borderline": 0,
        "needs_context": 0,
        "suggested_reject": 0,
        "hidden_by_critic": 0,
        "visible_by_default": 0,
        "collapsed": 0,
        "human_accepted": 0,
        "human_rejected": 0,
        "hidden_accepted": 0,
        "suggested_accepted": 0,
    })

    human_by_fid: dict[str, str] = {
        _composite_id(rec): rec.get("human_decision", "")
        for rec in records if rec.get("finding_id")
    }

    for td in triage_decisions:
        sec = section_by_fid.get(td.finding_id, "unknown")
        s = by_section[sec]
        s["total"] += 1
        if td.human_queue == QUEUE_STRONG_KEEP:
            s["strong_keep"] += 1
        elif td.human_queue == QUEUE_MAIN_REVIEW:
            s["main_review"] += 1
        elif td.human_queue == QUEUE_BORDERLINE:
            s["borderline"] += 1
        elif td.human_queue == QUEUE_NEEDS_CONTEXT:
            s["needs_context"] += 1
        elif td.human_queue == QUEUE_SUGGESTED_REJECT:
            s["suggested_reject"] += 1
            if human_by_fid.get(td.finding_id) == "accepted":
                s["suggested_accepted"] += 1
        elif td.human_queue == QUEUE_HIDDEN:
            s["hidden_by_critic"] += 1
            if human_by_fid.get(td.finding_id) == "accepted":
                s["hidden_accepted"] += 1
        if td.visible_by_default:
            s["visible_by_default"] += 1
        if td.collapsed_by_default:
            s["collapsed"] += 1
        hdec = human_by_fid.get(td.finding_id, "")
        if hdec == "accepted":
            s["human_accepted"] += 1
        elif hdec == "rejected":
            s["human_rejected"] += 1

    return {k: dict(v) for k, v in sorted(by_section.items())}


# ─── Markdown report ──────────────────────────────────────────────────────────


def render_triage_markdown(
    experiment: str,
    records_count: int,
    metrics: TriageMetrics,
    section_breakdown: dict,
    risky_cases: list[dict],
    llm_decisions_count: int,
    profile: str = PROFILE_CONSERVATIVE,
) -> str:
    m = metrics
    pcfg = get_profile_config(profile)
    total = max(m.total_findings, 1)

    lines = [
        "# Critic V2 Triage Replay Report",
        "",
        f"**Experiment:** `{experiment}`  |  **Profile:** `{profile}` — _{pcfg.label}_",
        f"**Records:** {records_count}  |  **LLM decisions used:** {llm_decisions_count}",
    ]
    if pcfg.non_production:
        lines.append("> ⚠️ **NON-PRODUCTION profile** — for experimentation only")
    lines.append("")

    lines += [
        "## Triage Queue Summary",
        "",
        "| Queue | Count | % of Total | Type |",
        "|-------|-------|------------|------|",
        f"| strong_keep | **{m.strong_keep_count}** | {m.strong_keep_count/total*100:.1f}% | visible |",
        f"| main_review | {m.main_review_count} | {m.main_review_count/total*100:.1f}% | visible |",
        f"| borderline | {m.borderline_count} | {m.borderline_count/total*100:.1f}% | visible |",
        f"| needs_context | {m.needs_context_count} | {m.needs_context_count/total*100:.1f}% | visible |",
        f"| **suggested_reject** | **{m.suggested_reject_count}** | {m.suggested_reject_count/total*100:.1f}% | **collapsed** |",
        f"| hidden_by_critic | {m.hidden_by_critic_count} | {m.hidden_by_critic_count/total*100:.1f}% | collapsed |",
        "",
    ]

    # Business workload view
    bwv = build_business_workload_view(m)
    lines += [
        "## Business Workload View",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Total findings | {m.total_findings} |",
        f"| **Primary queue (visible)** | **{m.primary_visible_count}** |",
        f"| Collapsed queue | {m.primary_collapsed_count} |",
        f"| → suggested_reject | {m.suggested_reject_count} |",
        f"| → hidden_by_critic | {m.hidden_by_critic_count} |",
        f"| **Primary queue reduction** | **{m.primary_queue_reduction_percent:.1f}%** |",
    ]

    if m.accepted_primary_visible_recall is not None:
        pv_recall_pct = m.accepted_primary_visible_recall * 100
        nh_recall_pct = (m.accepted_not_hidden_recall or 0) * 100
        risk_pv = " ⚠️ LOW" if pv_recall_pct < 90 else " ✓"
        risk_nh = " ⚠️ LOW" if nh_recall_pct < 95 else " ✓"
        lines += [
            f"| **Accepted primary visible recall** | **{pv_recall_pct:.1f}%{risk_pv}** |",
            f"| **Accepted not-hidden recall** | **{nh_recall_pct:.1f}%{risk_nh}** |",
            f"| Human accepted in suggested_reject | {m.suggested_reject_human_accepted_count} |",
            f"| Human accepted hidden | **{m.hidden_human_accepted_count}** |",
        ]
        if m.hidden_precision_against_human is not None:
            lines.append(f"| hidden precision (vs human) | {m.hidden_precision_against_human*100:.1f}% |")
        if m.suggested_reject_precision_against_human is not None:
            lines.append(
                f"| suggested_reject precision (vs human) | "
                f"{m.suggested_reject_precision_against_human*100:.1f}% |"
            )
    lines.append("")

    # Risky hidden cases
    if risky_cases:
        lines += [
            "## ⚠️ Risky Hidden Cases (hidden by critic but human accepted)",
            "",
            "| Finding | EV | Score | Reason | Taxonomy | Det | LLM |",
            "|---------|-----|-------|--------|----------|-----|-----|",
        ]
        for c in risky_cases[:20]:
            lines.append(
                f"| {c.get('finding_id')} "
                f"| {c.get('ev')} "
                f"| {c.get('score')} "
                f"| {c.get('reason')} "
                f"| {c.get('taxonomy') or '—'} "
                f"| {c.get('det_decision')} "
                f"| {c.get('llm_decision') or '—'} |"
            )
        if len(risky_cases) > 20:
            lines.append(f"_...and {len(risky_cases)-20} more_")
        lines.append("")
    else:
        lines += ["## Risky Hidden Cases", "", "_None — all hidden findings were human-rejected._", ""]

    # Section breakdown
    if section_breakdown:
        lines += [
            "## Per-Section Triage Breakdown",
            "",
            "| Section | Total | SK | MR | BD | NC | SR | HC | Primary | Collapse% | H.Acc | H.Rej | SR.Acc | HC.Acc |",
            "|---------|-------|----|----|----|----|----|----|---------|-----------|-------|-------|--------|--------|",
        ]
        for sec, s in sorted(section_breakdown.items()):
            tot = s["total"] or 1
            primary = s["strong_keep"] + s["main_review"] + s["borderline"] + s["needs_context"]
            coll_pct = f"{s['collapsed']/tot*100:.0f}%"
            lines.append(
                f"| {sec} | {s['total']} "
                f"| {s['strong_keep']} "
                f"| {s['main_review']} "
                f"| {s['borderline']} "
                f"| {s['needs_context']} "
                f"| {s['suggested_reject']} "
                f"| {s['hidden_by_critic']} "
                f"| {primary} "
                f"| {coll_pct} "
                f"| {s['human_accepted']} "
                f"| {s['human_rejected']} "
                f"| {s.get('suggested_accepted', 0)} "
                f"| {s['hidden_accepted']} |"
            )
        lines.append("")
        lines += [
            "_SK=strong_keep, MR=main_review, BD=borderline, NC=needs_context, "
            "SR=suggested_reject, HC=hidden_by_critic, SR.Acc=suggested_reject human accepted, "
            "HC.Acc=hidden human accepted_",
            "",
        ]

    lines += [
        "---",
        "_Generated by replay_critic_v2_triage_policy.py. No LLM was called. Production pipeline NOT modified._",
    ]
    return "\n".join(lines) + "\n"


def render_profile_comparison_markdown(
    profile_results: dict[str, tuple[list[TriageDecision], TriageMetrics]],
    experiment: str = "experiment",
    records_count: int = 0,
    llm_decisions_count: int = 0,
) -> str:
    """Render a comparison table across all three profiles."""
    lines = [
        "# Critic V2 Triage Profile Comparison",
        "",
        f"**Experiment:** `{experiment}`  |  **Records:** {records_count}  "
        f"|  **LLM decisions:** {llm_decisions_count}",
        "",
        "## Queue Distribution by Profile",
        "",
        "| Profile | Total | SK | MR | BD | NC | SR | HC | Primary | Collapse% |",
        "|---------|-------|----|----|----|----|----|----|---------|-----------|",
    ]

    for prof in ALL_PROFILES:
        if prof not in profile_results:
            lines.append(f"| {prof} | — | — | — | — | — | — | — | — | — |")
            continue
        _, m = profile_results[prof]
        total = max(m.total_findings, 1)
        primary = m.primary_visible_count
        pcfg = get_profile_config(prof)
        np_flag = " ⚠️" if pcfg.non_production else ""
        lines.append(
            f"| {prof}{np_flag} "
            f"| {m.total_findings} "
            f"| {m.strong_keep_count} "
            f"| {m.main_review_count} "
            f"| {m.borderline_count} "
            f"| {m.needs_context_count} "
            f"| {m.suggested_reject_count} "
            f"| {m.hidden_by_critic_count} "
            f"| {primary} "
            f"| {m.primary_queue_reduction_percent:.1f}% |"
        )
    lines.append("")

    # Business workload KPIs
    has_human = any(
        profile_results[p][1].accepted_primary_visible_recall is not None
        for p in ALL_PROFILES if p in profile_results
    )

    if has_human:
        lines += [
            "## Business Workload KPIs by Profile",
            "",
            "| Profile | Queue Reduction | Accepted PV Recall | Accepted Not-Hidden | SR Human Acc | HC Human Acc |",
            "|---------|----------------|-------------------|---------------------|-------------|-------------|",
        ]
        for prof in ALL_PROFILES:
            if prof not in profile_results:
                continue
            _, m = profile_results[prof]
            pcfg = get_profile_config(prof)
            np_flag = " ⚠️" if pcfg.non_production else ""
            pv_r = f"{m.accepted_primary_visible_recall*100:.1f}%" if m.accepted_primary_visible_recall is not None else "—"
            nh_r = f"{m.accepted_not_hidden_recall*100:.1f}%" if m.accepted_not_hidden_recall is not None else "—"
            sr_ha = str(m.suggested_reject_human_accepted_count)
            hc_ha = str(m.hidden_human_accepted_count)
            # Risk flags
            if m.accepted_primary_visible_recall is not None and m.accepted_primary_visible_recall < 0.90:
                pv_r += " ⚠️"
            if m.hidden_human_accepted_count > 0:
                hc_ha += " ⚠️"
            lines.append(
                f"| {prof}{np_flag} "
                f"| {m.primary_queue_reduction_percent:.1f}% "
                f"| {pv_r} "
                f"| {nh_r} "
                f"| {sr_ha} "
                f"| {hc_ha} |"
            )
        lines.append("")

    # Best profile recommendation
    lines += ["## Best Profile Recommendation", ""]
    candidates = []
    for prof in ALL_PROFILES:
        if prof not in profile_results:
            continue
        _, m = profile_results[prof]
        pcfg = get_profile_config(prof)
        if (
            (m.accepted_not_hidden_recall is None or m.accepted_not_hidden_recall >= 0.95)
            and m.hidden_human_accepted_count == 0
        ):
            candidates.append((prof, m.primary_queue_reduction_percent, pcfg.non_production))

    prod_candidates = [(p, r) for p, r, np in candidates if not np]
    if prod_candidates:
        best = max(prod_candidates, key=lambda x: x[1])
        lines.append(
            f"**Recommended for UI:** `{best[0]}` — "
            f"primary_queue_reduction={best[1]:.1f}%, "
            f"hidden_accepted=0, not-hidden recall≥95%"
        )
    elif candidates:
        best = max(candidates, key=lambda x: x[1])
        lines.append(
            f"**Best available (including non-production):** `{best[0]}` — "
            f"primary_queue_reduction={best[1]:.1f}%"
        )
    else:
        # Find safest (min hidden_accepted)
        all_res = [(p, profile_results[p][1]) for p in ALL_PROFILES if p in profile_results]
        if all_res:
            safest = min(all_res, key=lambda x: x[1].hidden_human_accepted_count)
            lines.append(
                f"_No profile achieves not-hidden recall≥95% with zero hidden accepted. "
                f"Safest: `{safest[0]}` with hidden_accepted={safest[1].hidden_human_accepted_count}_"
            )
    lines.append("")

    lines += [
        "---",
        "_SK=strong_keep, MR=main_review, BD=borderline, NC=needs_context, SR=suggested_reject, HC=hidden_by_critic_",
        "_PV Recall = human_accepted in primary_visible / total. Not-Hidden = human_accepted NOT in HC / total._",
        "_⚠️ profile = non-production (experimental)_",
        "",
        "_Generated by replay_critic_v2_triage_policy.py. No LLM was called. Production pipeline NOT modified._",
    ]
    return "\n".join(lines) + "\n"


def render_multi_experiment_markdown(
    results_by_exp: dict[str, dict],
    profile: str = PROFILE_CONSERVATIVE,
) -> str:
    """Render comparison table across multiple experiment replay results for one profile."""
    lines = [
        f"# Critic V2 Triage Replay — Multi-Experiment Comparison (profile={profile})",
        "",
        "| Experiment | Total | SK | MR | BD | NC | SR | HC | Primary | Collapse% | PV Recall% | Not-Hidden% | HC.Acc |",
        "|------------|-------|----|----|----|----|----|----|---------|-----------|------------|-------------|--------|",
    ]

    for exp, res in sorted(results_by_exp.items()):
        if "error" in res:
            lines.append(f"| {exp} | ERROR | — | — | — | — | — | — | — | — | — | — | — |")
            continue
        m: TriageMetrics = res["metrics"]
        total = max(m.total_findings, 1)
        pv_recall = f"{m.accepted_primary_visible_recall*100:.1f}%" if m.accepted_primary_visible_recall is not None else "—"
        nh_recall = f"{m.accepted_not_hidden_recall*100:.1f}%" if m.accepted_not_hidden_recall is not None else "—"
        lines.append(
            f"| {exp} "
            f"| {m.total_findings} "
            f"| {m.strong_keep_count} "
            f"| {m.main_review_count} "
            f"| {m.borderline_count} "
            f"| {m.needs_context_count} "
            f"| {m.suggested_reject_count} "
            f"| {m.hidden_by_critic_count} "
            f"| {m.primary_visible_count} "
            f"| {m.primary_queue_reduction_percent:.1f}% "
            f"| {pv_recall} "
            f"| {nh_recall} "
            f"| {m.hidden_human_accepted_count} |"
        )

    lines.append("")
    lines += [
        "---",
        "_SK=strong_keep, MR=main_review, BD=borderline, NC=needs_context, SR=suggested_reject, HC=hidden_by_critic_",
        "_PV Recall = accepted in primary_visible. Not-Hidden = accepted NOT in HC._",
        "",
        "_Generated by replay_critic_v2_triage_policy.py. No LLM was called. Production pipeline NOT modified._",
    ]
    return "\n".join(lines) + "\n"


# ─── Output writer ────────────────────────────────────────────────────────────


def write_triage_outputs(
    output_dir: Path,
    experiment: str,
    triage_decisions: list[TriageDecision],
    metrics: TriageMetrics,
    section_breakdown: dict,
    records: list[dict],
    llm_decisions_count: int,
    profile: str = PROFILE_CONSERVATIVE,
    with_ui_export: bool = False,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    artifacts = build_triage_artifacts(triage_decisions, metrics)

    def _write(name: str, data: Any) -> None:
        (output_dir / name).write_text(
            json.dumps(data, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

    _write("critic_v2_triage.json", artifacts["critic_v2_triage"])
    _write("critic_v2_triage_metrics.json", artifacts["critic_v2_triage_metrics"])
    _write("critic_v2_hidden_by_critic.json", artifacts["critic_v2_hidden_by_critic"])
    _write("critic_v2_suggested_reject.json", artifacts["critic_v2_suggested_reject"])
    _write("critic_v2_visible_by_default.json", artifacts["critic_v2_visible_by_default"])
    _write("critic_v2_risky_hidden_cases.json", artifacts["critic_v2_risky_hidden_cases"])
    _write("ar_f001_diagnostic.json", get_ar_f001_diagnostic())

    summary = {
        "experiment": experiment,
        "profile": profile,
        "records_count": len(records),
        "llm_decisions_count": llm_decisions_count,
        "metrics": triage_metrics_to_dict(metrics),
        "business_workload_view": build_business_workload_view(metrics),
        "section_breakdown": section_breakdown,
    }
    _write("triage_replay_summary.json", summary)

    md = render_triage_markdown(
        experiment=experiment,
        records_count=len(records),
        metrics=metrics,
        section_breakdown=section_breakdown,
        risky_cases=metrics.risky_hidden_cases,
        llm_decisions_count=llm_decisions_count,
        profile=profile,
    )
    (output_dir / "triage_replay_summary.md").write_text(md, encoding="utf-8")

    if with_ui_export:
        records_by_id: dict[str, dict] = {}
        for rec in records:
            cid = _composite_id(rec)
            if cid:
                records_by_id[cid] = rec

        human_decisions: dict[str, str] = {}
        for rec in records:
            cid = _composite_id(rec)
            hdec = rec.get("human_decision", "")
            if cid and hdec in ("accepted", "rejected"):
                human_decisions[cid] = hdec

        ui_export = build_ui_export(
            triage_decisions=triage_decisions,
            metrics=metrics,
            records_by_id=records_by_id,
            human_decisions=human_decisions,
        )
        _write("critic_v2_triage_ui.json", ui_export)
        ui_md = render_ui_export_markdown(ui_export)
        (output_dir / "critic_v2_triage_ui_preview.md").write_text(
            ui_md, encoding="utf-8"
        )


# ─── Multi-experiment replay ──────────────────────────────────────────────────


def replay_matrix_experiment(
    matrix_output_dir: Path,
    experiment_name: str,
    include_human_labels: bool = True,
    quiet: bool = False,
    profile: str = PROFILE_CONSERVATIVE,
) -> dict:
    exp_dir = matrix_output_dir / experiment_name
    if not exp_dir.exists():
        return {"experiment": experiment_name, "error": f"Directory not found: {exp_dir}"}

    records = _load_benchmark_records(exp_dir)
    if not records:
        return {"experiment": experiment_name, "error": f"No benchmark records in {exp_dir}"}

    llm_by_id = _load_llm_decisions(exp_dir)

    if not quiet:
        print(f"    {experiment_name}: {len(records)} records, {len(llm_by_id)} LLM decisions")

    triage_decisions, metrics = replay_triage_on_records(
        records, llm_by_id, include_human_labels=include_human_labels, profile=profile,
    )
    artifacts = build_triage_artifacts(triage_decisions, metrics)

    return {
        "experiment": experiment_name,
        "records_count": len(records),
        "llm_decisions_count": len(llm_by_id),
        "triage_decisions": triage_decisions,
        "metrics": metrics,
        "artifacts": artifacts,
        "records": records,
        "llm_by_id": llm_by_id,
    }


# ─── Console helpers ──────────────────────────────────────────────────────────


def _print_triage_summary(
    experiment: str, records_count: int, metrics: TriageMetrics, quiet: bool,
    profile: str = "",
) -> None:
    if quiet:
        return
    m = metrics
    pv_recall = f"{m.accepted_primary_visible_recall*100:.1f}%" if m.accepted_primary_visible_recall is not None else "n/a"
    nh_recall = f"{m.accepted_not_hidden_recall*100:.1f}%" if m.accepted_not_hidden_recall is not None else "n/a"
    prof_str = f"[{profile}] " if profile else ""
    print(
        f"    {prof_str}→ total={m.total_findings} "
        f"SK={m.strong_keep_count} MR={m.main_review_count} BD={m.borderline_count} "
        f"NC={m.needs_context_count} SR={m.suggested_reject_count} HC={m.hidden_by_critic_count} "
        f"| collapse={m.primary_queue_reduction_percent:.1f}% "
        f"pv_recall={pv_recall} nh_recall={nh_recall} "
        f"hc_acc={m.hidden_human_accepted_count} sr_acc={m.suggested_reject_human_accepted_count}"
    )


def _print_triage_summary_inline(prof: str, m: TriageMetrics) -> None:
    pv_recall = f"{m.accepted_primary_visible_recall*100:.1f}%" if m.accepted_primary_visible_recall is not None else "n/a"
    nh_recall = f"{m.accepted_not_hidden_recall*100:.1f}%" if m.accepted_not_hidden_recall is not None else "n/a"
    print(
        f"      [{prof:<12}] SK={m.strong_keep_count} SR={m.suggested_reject_count} "
        f"HC={m.hidden_by_critic_count} "
        f"collapse={m.primary_queue_reduction_percent:.1f}% "
        f"pv={pv_recall} nh={nh_recall} "
        f"hc_acc={m.hidden_human_accepted_count} sr_acc={m.suggested_reject_human_accepted_count}"
    )


def _print_profile_comparison(profile_results: dict[str, tuple]) -> None:
    print()
    print(f"  {'Profile':<14} {'Collapse%':>10} {'PV Recall':>10} {'NH Recall':>10} "
          f"{'SR.Acc':>7} {'HC.Acc':>7}")
    print(f"  {'-'*14} {'-'*10} {'-'*10} {'-'*10} {'-'*7} {'-'*7}")
    for prof in ALL_PROFILES:
        if prof not in profile_results:
            continue
        _, m = profile_results[prof]
        pv_r = f"{m.accepted_primary_visible_recall*100:.1f}%" if m.accepted_primary_visible_recall is not None else "n/a"
        nh_r = f"{m.accepted_not_hidden_recall*100:.1f}%" if m.accepted_not_hidden_recall is not None else "n/a"
        pcfg = get_profile_config(prof)
        np_flag = " [NP]" if pcfg.non_production else ""
        print(
            f"  {prof+np_flag:<14} {m.primary_queue_reduction_percent:>9.1f}% "
            f"{pv_r:>10} {nh_r:>10} "
            f"{m.suggested_reject_human_accepted_count:>7} "
            f"{m.hidden_human_accepted_count:>7}"
        )
    print()


# ─── Main ─────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Replay critic_v2 triage policy on existing benchmark/matrix artifacts. "
            "Supports multiple profiles. Does NOT call any LLM."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # From benchmark output, all profiles
  python %(prog)s \\
      --benchmark-output-dir /tmp/benchmark_human \\
      --profile all \\
      --output-dir /tmp/triage_replay

  # From matrix, all profiles
  python %(prog)s \\
      --matrix-output-dir /tmp/critic_v2_matrix_real_full_guard \\
      --profile all \\
      --output-dir /tmp/critic_v2_triage_profiles_replay

  # From matrix, specific experiment + profile
  python %(prog)s \\
      --matrix-output-dir /tmp/critic_v2_matrix_real_full_guard \\
      --experiment llm_context_policy \\
      --profile assisted \\
      --output-dir /tmp/triage_replay_assisted
""",
    )
    parser.add_argument("--benchmark-output-dir", type=Path, default=None)
    parser.add_argument("--matrix-output-dir", type=Path, default=None)
    parser.add_argument(
        "--experiment",
        choices=["det_only", "llm_no_context", "llm_context_policy"],
        default=None,
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--profile",
        choices=list(VALID_PROFILES) + ["all"],
        default=PROFILE_CONSERVATIVE,
        help="Triage profile(s) to run. 'all' runs all three.",
    )
    parser.add_argument("--no-human-labels", action="store_true", default=False)
    parser.add_argument("--quiet", action="store_true", default=False)
    parser.add_argument(
        "--ui-export",
        action="store_true",
        default=False,
        help=(
            "Also write critic_v2_triage_ui.json + critic_v2_triage_ui_preview.md "
            "with 4-tab UI layout (primary / needs_context / suggested_reject / "
            "hidden_by_critic). Offline artifact only — does NOT modify the "
            "production pipeline or 03_findings_review.json."
        ),
    )
    args = parser.parse_args()

    if args.benchmark_output_dir is None and args.matrix_output_dir is None:
        parser.error("One of --benchmark-output-dir or --matrix-output-dir is required.")

    profiles_to_run = ALL_PROFILES if args.profile == "all" else [args.profile]
    include_human = not args.no_human_labels
    t_start = time.monotonic()

    # ── Mode A: single benchmark dir ─────────────────────────────────────────
    if args.benchmark_output_dir is not None:
        bdir = args.benchmark_output_dir
        if not bdir.exists():
            print(f"ERROR: --benchmark-output-dir not found: {bdir}", file=sys.stderr)
            return 1

        records = _load_benchmark_records(bdir)
        if not records:
            print(f"ERROR: No benchmark records in {bdir}", file=sys.stderr)
            return 1

        llm_by_id = _load_llm_decisions(bdir)

        if not args.quiet:
            print(f"Triage replay: benchmark mode")
            print(f"  Input dir  : {bdir}")
            print(f"  Records    : {len(records)}")
            print(f"  LLM decs   : {len(llm_by_id)}")
            print(f"  Profiles   : {profiles_to_run}")
            print(f"  Output dir : {args.output_dir}")
            print()

        if len(profiles_to_run) > 1:
            profile_results = replay_all_profiles(
                records, llm_by_id, include_human_labels=include_human, quiet=args.quiet
            )
            for prof, (triage_decisions, metrics) in profile_results.items():
                prof_out = args.output_dir / prof
                section_breakdown = compute_section_breakdown(records, triage_decisions)
                write_triage_outputs(
                    output_dir=prof_out, experiment="benchmark",
                    triage_decisions=triage_decisions, metrics=metrics,
                    section_breakdown=section_breakdown, records=records,
                    llm_decisions_count=len(llm_by_id), profile=prof,
                    with_ui_export=args.ui_export,
                )
            # Profile comparison report
            args.output_dir.mkdir(parents=True, exist_ok=True)
            comp_md = render_profile_comparison_markdown(
                profile_results, experiment="benchmark",
                records_count=len(records), llm_decisions_count=len(llm_by_id),
            )
            (args.output_dir / "profile_comparison.md").write_text(comp_md, encoding="utf-8")
            comp_json = {
                prof: triage_metrics_to_dict(metrics)
                for prof, (_, metrics) in profile_results.items()
            }
            (args.output_dir / "profile_comparison.json").write_text(
                json.dumps(comp_json, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
            )
            if not args.quiet:
                _print_profile_comparison(profile_results)
        else:
            prof = profiles_to_run[0]
            triage_decisions, metrics = replay_triage_on_records(
                records, llm_by_id, include_human_labels=include_human, profile=prof,
            )
            section_breakdown = compute_section_breakdown(records, triage_decisions)
            write_triage_outputs(
                output_dir=args.output_dir, experiment="benchmark",
                triage_decisions=triage_decisions, metrics=metrics,
                section_breakdown=section_breakdown, records=records,
                llm_decisions_count=len(llm_by_id), profile=prof,
                with_ui_export=args.ui_export,
            )
            _print_triage_summary("benchmark", len(records), metrics, args.quiet, prof)

    # ── Mode B: matrix dir ────────────────────────────────────────────────────
    else:
        mdir = args.matrix_output_dir
        if not mdir.exists():
            print(f"ERROR: --matrix-output-dir not found: {mdir}", file=sys.stderr)
            return 1

        known_experiments = ["det_only", "llm_no_context", "llm_context_policy"]
        if args.experiment:
            experiments_to_run = [args.experiment]
        else:
            experiments_to_run = [
                exp for exp in known_experiments
                if (mdir / exp / "human_benchmark_records.json").exists()
            ]
            if not experiments_to_run:
                print(f"ERROR: No experiment sub-dirs found in {mdir}", file=sys.stderr)
                return 1

        if not args.quiet:
            print(f"Triage replay: matrix mode")
            print(f"  Matrix dir   : {mdir}")
            print(f"  Experiments  : {experiments_to_run}")
            print(f"  Profiles     : {profiles_to_run}")
            print(f"  Output dir   : {args.output_dir}")
            print()

        # For each experiment, replay all profiles
        for exp_name in experiments_to_run:
            if not args.quiet:
                print(f"  {exp_name}:")

            exp_dir = mdir / exp_name
            records = _load_benchmark_records(exp_dir)
            llm_by_id = _load_llm_decisions(exp_dir)

            if not records:
                print(f"    ERROR: No records in {exp_dir}", file=sys.stderr)
                continue

            if len(profiles_to_run) > 1:
                profile_results = replay_all_profiles(
                    records, llm_by_id, include_human_labels=include_human, quiet=args.quiet
                )
                for prof, (triage_decisions, metrics) in profile_results.items():
                    prof_out = args.output_dir / exp_name / prof
                    section_breakdown = compute_section_breakdown(records, triage_decisions)
                    write_triage_outputs(
                        output_dir=prof_out, experiment=exp_name,
                        triage_decisions=triage_decisions, metrics=metrics,
                        section_breakdown=section_breakdown, records=records,
                        llm_decisions_count=len(llm_by_id), profile=prof,
                        with_ui_export=args.ui_export,
                    )
                # Per-experiment profile comparison
                exp_comp_out = args.output_dir / exp_name
                exp_comp_out.mkdir(parents=True, exist_ok=True)
                comp_md = render_profile_comparison_markdown(
                    profile_results, experiment=exp_name,
                    records_count=len(records), llm_decisions_count=len(llm_by_id),
                )
                (exp_comp_out / "profile_comparison.md").write_text(comp_md, encoding="utf-8")
                (exp_comp_out / "profile_comparison.json").write_text(
                    json.dumps(
                        {p: triage_metrics_to_dict(m) for p, (_, m) in profile_results.items()},
                        ensure_ascii=False, indent=2, default=str,
                    ), encoding="utf-8"
                )
            else:
                prof = profiles_to_run[0]
                triage_decisions, metrics = replay_triage_on_records(
                    records, llm_by_id, include_human_labels=include_human, profile=prof,
                )
                section_breakdown = compute_section_breakdown(records, triage_decisions)
                write_triage_outputs(
                    output_dir=args.output_dir / exp_name,
                    experiment=exp_name, triage_decisions=triage_decisions,
                    metrics=metrics, section_breakdown=section_breakdown,
                    records=records, llm_decisions_count=len(llm_by_id), profile=prof,
                    with_ui_export=args.ui_export,
                )
                _print_triage_summary(exp_name, len(records), metrics, args.quiet, prof)

        # Cross-experiment comparison (for each profile)
        args.output_dir.mkdir(parents=True, exist_ok=True)
        for prof in profiles_to_run:
            results_by_exp: dict[str, dict] = {}
            for exp_name in experiments_to_run:
                summary_path = args.output_dir / exp_name / prof / "triage_replay_summary.json"
                if not summary_path.exists():
                    summary_path = args.output_dir / exp_name / "triage_replay_summary.json"
                if summary_path.exists():
                    try:
                        s = json.loads(summary_path.read_text(encoding="utf-8"))
                        # Reconstruct metrics stub for rendering
                        class _MetricsStub:
                            pass
                        m_stub = _MetricsStub()
                        for k, v in s.get("metrics", {}).items():
                            setattr(m_stub, k, v)
                        results_by_exp[exp_name] = {"metrics": s.get("metrics", {}), "experiment": exp_name}
                    except Exception:
                        pass
            if results_by_exp:
                # Simple comparison JSON
                (args.output_dir / f"experiment_comparison_{prof}.json").write_text(
                    json.dumps(results_by_exp, ensure_ascii=False, indent=2, default=str),
                    encoding="utf-8",
                )

        if not args.quiet and len(profiles_to_run) > 1:
            # Summary across experiments for the assisted profile (most relevant)
            print(f"\n  Summary (per experiment, assisted profile):")
            for exp_name in experiments_to_run:
                summary_path = args.output_dir / exp_name / PROFILE_ASSISTED / "triage_replay_summary.json"
                if summary_path.exists():
                    try:
                        s = json.loads(summary_path.read_text(encoding="utf-8"))
                        m = s.get("metrics", {})
                        pv = m.get("accepted_primary_visible_recall")
                        nh = m.get("accepted_not_hidden_recall")
                        print(
                            f"    {exp_name:<22} collapse={m.get('primary_queue_reduction_percent', 0):.1f}% "
                            f"pv={pv*100:.1f}% nh={nh*100:.1f}% "
                            f"hc_acc={m.get('hidden_human_accepted_count', 0)} "
                            f"sr_acc={m.get('suggested_reject_human_accepted_count', 0)}"
                            if pv is not None else f"    {exp_name:<22} (no human labels)"
                        )
                    except Exception:
                        pass

    elapsed_ms = int((time.monotonic() - t_start) * 1000)
    if not args.quiet:
        print(f"\n  Done in {elapsed_ms}ms. Output: {args.output_dir}")
        print("  NOTE: Production pipeline NOT modified. No LLM was called.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
