#!/usr/bin/env python3
"""
analyze_critic_v2_batch.py
--------------------------
Analyzes borderline findings from a batch critic v2 output directory.

Reads:
  <batch-output-dir>/batch_results.json               (per-project summary)
  <batch-output-dir>/batch_summary.json               (cross-project summary)
  <batch-output-dir>/<project_slug>/critic_v2_decisions.json  (per project)
  <batch-output-dir>/<project_slug>/critic_v2_legacy_comparison.json  (optional)

Writes:
  <batch-output-dir>/borderline_analysis.json
  <batch-output-dir>/borderline_analysis.md
  <batch-output-dir>/borderline_samples.csv            (optional, --export-csv)

NOT connected to production pipeline.
Does NOT read or modify any production artifacts.

Usage:
    python backend/scripts/analyze_critic_v2_batch.py \\
        --batch-output-dir /tmp/critic_v2_batch \\
        --top-n 30 \\
        --export-csv \\
        --export-md

    # After running batch:
    python backend/scripts/batch_critic_v2.py --section EOM --limit 6
    python backend/scripts/analyze_critic_v2_batch.py \\
        --batch-output-dir /tmp/critic_v2_batch
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path
from typing import Optional

# ─── sys.path bootstrap ───────────────────────────────────────────────────────

_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# ─── Constants ────────────────────────────────────────────────────────────────

EVIDENCE_NONE = "none"
EVIDENCE_WEAK = "weak"
EVIDENCE_PARTIAL = "partial"
EVIDENCE_VALID = "valid"

_SAFE_REJECT_EVIDENCE = {EVIDENCE_NONE, EVIDENCE_WEAK}
_SAFE_ACCEPT_EVIDENCE = {EVIDENCE_VALID}
_SAFE_ACCEPT_PARTIAL_EVIDENCE = {EVIDENCE_PARTIAL}

_HIGH_VALUE_IMPACT_AREAS = {
    "safety", "cost_schedule", "acceptance", "legal",
    "cost", "schedule", "contract", "procurement", "construction",
}
_LOW_VALUE_IMPACT_AREAS = {
    "none", "documentation", "cosmetic", "normative", None,
}
_MED_VALUE_IMPACT_AREAS = {"reliability", "operations"}

_HIGH_SEVERITY_SET = {"КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ"}
_LOW_SEVERITY_SET = {"РЕКОМЕНДАТЕЛЬНОЕ", "ПРОВЕРИТЬ ПО СМЕЖНЫМ", "unknown", ""}

_GENERIC_MARKERS = re.compile(
    r"\b(необходимо проверить|требуется проверить|следует проверить"
    r"|рекомендуется проверить|нужно проверить|следует уточнить"
    r"|необходимо уточнить|требуется уточнить|рекомендуется уточнить"
    r"|требует уточнения|требует проверки|требует дополнительной проверки"
    r"|рекомендуется рассмотреть|возможно|вероятно|предположительно"
    r"|по предварительной оценке|может свидетельствовать|потенциально)\b",
    re.IGNORECASE,
)

_CAP_SOURCE_SCORES = {
    # Scores at edge of evidence cap: weak→cap5, partial→cap6/7
    # If score == cap → "capped"
}


# ─── Borderline classification ────────────────────────────────────────────────

def classify_borderline(d: dict) -> str:
    """
    Classify a borderline finding into:
      safe_reject_candidate
      safe_accept_candidate
      keep_borderline

    Input dict is a QualityDecision serialised to JSON.
    """
    ev = d.get("evidence_quality", EVIDENCE_NONE)
    score = d.get("usefulness_score", 0)
    severity = (d.get("severity") or "").upper()
    impact = d.get("impact_area") or "none"
    reject_reason = d.get("reject_reason")

    # Already has a reject reason → safe reject
    if reject_reason:
        return "safe_reject_candidate"

    # --- Safe reject criteria ---
    is_low_ev = ev in _SAFE_REJECT_EVIDENCE
    is_low_score = score <= 5
    is_low_severity = severity in _LOW_SEVERITY_SET or severity not in _HIGH_SEVERITY_SET
    is_low_impact = impact in _LOW_VALUE_IMPACT_AREAS
    has_generic = _GENERIC_MARKERS.search(
        f"{d.get('title', '')} {d.get('description', '')}"
    )
    action = (d.get("action_required") or "").strip()
    has_no_action = len(action) < 10

    if is_low_ev and is_low_score and is_low_severity and is_low_impact:
        return "safe_reject_candidate"

    if is_low_ev and has_generic:
        return "safe_reject_candidate"

    if is_low_ev and has_no_action and is_low_impact:
        return "safe_reject_candidate"

    # --- Safe accept criteria ---
    is_high_ev = ev in _SAFE_ACCEPT_EVIDENCE
    is_high_score = score >= 6
    is_high_impact = impact in _HIGH_VALUE_IMPACT_AREAS
    is_high_severity = severity in _HIGH_SEVERITY_SET
    has_action = len(action) >= 10

    if is_high_ev and is_high_score and is_high_impact and has_action:
        return "safe_accept_candidate"

    if is_high_ev and is_high_score and is_high_severity:
        return "safe_accept_candidate"

    # Partial evidence + high severity + high impact at score 6-7
    if ev == EVIDENCE_PARTIAL and is_high_severity and is_high_score and is_high_impact:
        return "safe_accept_candidate"

    # --- Keep borderline (needs human/LLM judgment) ---
    # partial evidence, cross-document dependency, unclear but potentially important
    return "keep_borderline"


def _detect_borderline_source(d: dict) -> str:
    """
    What caused this finding to land in borderline rather than accept/reject?

    Priority (highest → lowest):
      cross_section_dep     — title/desc mentions cross-document dependency
      generic_wording       — generic 'please check' markers present
      low_impact_axis       — no identifiable high-value impact area
      evidence_cap_weak     — evidence_quality=weak, score capped at 5
      evidence_cap_partial  — evidence_quality=partial, score capped at 6 (or 7 for critical)
      score_56_no_cap       — valid evidence but score naturally 5-6
      unknown
    """
    ev = d.get("evidence_quality", EVIDENCE_NONE)
    score = d.get("usefulness_score", 0)
    impact = d.get("impact_area") or "none"
    title_desc = f"{d.get('title', '')} {d.get('description', '')}"

    # Text/semantic signals take priority over evidence cap signals
    if re.search(
        r"\b(смежн|раздел[е ]|по смежным|cross.section|другой раздел"
        r"|проект[е ].*раздел|совместно)\b",
        title_desc, re.IGNORECASE,
    ):
        return "cross_section_dep"

    if _GENERIC_MARKERS.search(title_desc):
        return "generic_wording"

    if impact in _LOW_VALUE_IMPACT_AREAS:
        return "low_impact_axis"

    # Evidence cap signals
    if ev == EVIDENCE_WEAK and score == 5:
        return "evidence_cap_weak"

    if ev == EVIDENCE_PARTIAL and score in (6, 7):
        return "evidence_cap_partial"

    if ev == EVIDENCE_VALID and score in (5, 6):
        return "score_56_no_cap"

    return "unknown"


# ─── Loading batch output ─────────────────────────────────────────────────────

def load_batch_decisions(batch_output_dir: Path) -> list[dict]:
    """
    Load all decisions from per-project critic_v2_decisions.json files.
    Falls back to critic_v2_final_decisions.json if present (LLM gate run).

    Adds 'project', 'project_slug' keys to each decision dict.
    """
    decisions_all: list[dict] = []
    results_path = batch_output_dir / "batch_results.json"

    if not results_path.exists():
        # Fallback: scan subdirs directly
        for subdir in sorted(batch_output_dir.iterdir()):
            if not subdir.is_dir():
                continue
            _load_project_decisions(subdir, subdir.name, decisions_all)
        return decisions_all

    try:
        results = json.loads(results_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return decisions_all

    for r in results:
        if r.get("skipped"):
            continue
        out_dir = Path(r.get("output_dir", ""))
        if not out_dir.exists():
            continue
        project_name = r.get("project", out_dir.name)
        _load_project_decisions(out_dir, project_name, decisions_all)

    return decisions_all


def _load_project_decisions(
    out_dir: Path,
    project_name: str,
    target: list[dict],
) -> None:
    """Load decisions from a single project output dir into target list."""
    # Prefer final decisions (after LLM gate) if present
    for fname in ("critic_v2_final_decisions.json", "critic_v2_decisions.json"):
        fpath = out_dir / fname
        if fpath.exists():
            try:
                items = json.loads(fpath.read_text(encoding="utf-8"))
                for item in items:
                    item["project"] = project_name
                    item["project_slug"] = out_dir.name
                target.extend(items)
            except (json.JSONDecodeError, OSError):
                pass
            return  # only load one file per project


def load_batch_summary(batch_output_dir: Path) -> Optional[dict]:
    path = batch_output_dir / "batch_summary.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def load_legacy_comparison_rows(batch_output_dir: Path) -> list[dict]:
    """Load all legacy comparison rows from per-project comparison files."""
    rows: list[dict] = []
    results_path = batch_output_dir / "batch_results.json"
    if not results_path.exists():
        return rows
    try:
        results = json.loads(results_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return rows
    for r in results:
        out_dir = Path(r.get("output_dir", ""))
        cmp_path = out_dir / "critic_v2_legacy_comparison.json"
        if cmp_path.exists():
            try:
                cmp = json.loads(cmp_path.read_text(encoding="utf-8"))
                for row in cmp.get("rows", []):
                    row["project"] = r.get("project", "?")
                    rows.append(row)
            except (json.JSONDecodeError, OSError):
                pass
    return rows


# ─── Analysis logic ───────────────────────────────────────────────────────────

def analyze_borderline(
    all_decisions: list[dict],
    top_n: int = 30,
) -> dict:
    """
    Produce a full borderline analysis report from all decisions.

    Returns structured analysis dict.
    """
    borderline = [d for d in all_decisions if d.get("decision") in ("borderline", "low_priority")]
    accepted = [d for d in all_decisions if d.get("decision") == "accept"]
    rejected = [d for d in all_decisions if d.get("decision") == "reject"]
    merged = [d for d in all_decisions if d.get("decision") == "merge"]
    total = len(all_decisions)

    # ── Breakdowns ────────────────────────────────────────────────────────────
    ev_breakdown = _count_by(borderline, "evidence_quality")
    sev_breakdown = _count_by(borderline, "severity")
    impact_breakdown = _count_by(borderline, "impact_area")
    score_dist = _score_distribution(borderline)
    source_breakdown = _count_by_func(borderline, _detect_borderline_source)
    classification_breakdown = _count_by_func(borderline, classify_borderline)

    safe_reject = [d for d in borderline if classify_borderline(d) == "safe_reject_candidate"]
    safe_accept = [d for d in borderline if classify_borderline(d) == "safe_accept_candidate"]
    keep = [d for d in borderline if classify_borderline(d) == "keep_borderline"]

    # ── Top patterns ──────────────────────────────────────────────────────────
    top_sources = sorted(source_breakdown.items(), key=lambda x: -x[1])
    top_ev = sorted(ev_breakdown.items(), key=lambda x: -x[1])
    top_sev = sorted(sev_breakdown.items(), key=lambda x: -x[1])

    # ── Sample findings ───────────────────────────────────────────────────────
    samples = _select_samples(borderline, top_n)

    # ── Recommendations ───────────────────────────────────────────────────────
    recs = _build_recommendations(
        borderline=borderline,
        safe_reject=safe_reject,
        safe_accept=safe_accept,
        keep=keep,
        ev_breakdown=ev_breakdown,
        source_breakdown=source_breakdown,
    )

    return {
        "overview": {
            "total_decisions": total,
            "borderline_count": len(borderline),
            "borderline_rate": round(len(borderline) / total, 3) if total else 0.0,
            "accepted_count": len(accepted),
            "rejected_count": len(rejected),
            "merged_count": len(merged),
        },
        "breakdown": {
            "by_evidence_quality": dict(ev_breakdown),
            "by_severity": dict(sev_breakdown),
            "by_impact_area": dict(impact_breakdown),
            "by_source": dict(top_sources),
            "score_distribution": score_dist,
        },
        "classification": {
            "safe_reject_candidates": len(safe_reject),
            "safe_accept_candidates": len(safe_accept),
            "keep_borderline": len(keep),
            "by_class": dict(classification_breakdown),
        },
        "safe_reject_ids": [d.get("finding_id") for d in safe_reject],
        "safe_accept_ids": [d.get("finding_id") for d in safe_accept],
        "samples": samples,
        "recommendations": recs,
    }


def _count_by(items: list[dict], key: str) -> dict:
    counts: dict[str, int] = {}
    for d in items:
        v = str(d.get(key) or "none")
        counts[v] = counts.get(v, 0) + 1
    return dict(sorted(counts.items(), key=lambda x: -x[1]))


def _count_by_func(items: list[dict], fn) -> dict:
    counts: dict[str, int] = {}
    for d in items:
        v = fn(d)
        counts[v] = counts.get(v, 0) + 1
    return dict(sorted(counts.items(), key=lambda x: -x[1]))


def _score_distribution(items: list[dict]) -> dict[str, int]:
    dist: dict[str, int] = {}
    for d in items:
        s = str(d.get("usefulness_score", 0))
        dist[s] = dist.get(s, 0) + 1
    return dict(sorted(dist.items()))


def _select_samples(borderline: list[dict], top_n: int) -> list[dict]:
    """
    Select top_n representative borderline samples.
    Prioritises diversity across evidence quality and source.
    Returns compact dicts with key fields only.
    """
    # Sort by: evidence_quality desc (valid first), score desc
    ev_order = {EVIDENCE_VALID: 0, EVIDENCE_PARTIAL: 1, EVIDENCE_WEAK: 2, EVIDENCE_NONE: 3}
    sorted_bl = sorted(
        borderline,
        key=lambda d: (
            ev_order.get(d.get("evidence_quality", EVIDENCE_NONE), 3),
            -(d.get("usefulness_score") or 0),
        ),
    )
    seen_sources: dict[str, int] = {}
    result = []
    # First pass: 1 per source type (for diversity)
    for d in sorted_bl:
        src = _detect_borderline_source(d)
        if seen_sources.get(src, 0) == 0:
            seen_sources[src] = 1
            result.append(_sample_dict(d))
    # Second pass: fill remaining up to top_n
    for d in sorted_bl:
        if len(result) >= top_n:
            break
        fid = d.get("finding_id")
        if not any(s["finding_id"] == fid for s in result):
            result.append(_sample_dict(d))
    return result[:top_n]


def _sample_dict(d: dict) -> dict:
    return {
        "finding_id": d.get("finding_id"),
        "project": d.get("project"),
        "decision": d.get("decision"),
        "usefulness_score": d.get("usefulness_score"),
        "evidence_quality": d.get("evidence_quality"),
        "severity": d.get("severity"),
        "impact_area": d.get("impact_area"),
        "reject_reason": d.get("reject_reason"),
        "borderline_source": _detect_borderline_source(d),
        "classification": classify_borderline(d),
        "has_evidence": d.get("has_evidence"),
        "has_action": d.get("has_action"),
        "has_impact": d.get("has_impact"),
    }


def _build_recommendations(
    borderline: list[dict],
    safe_reject: list[dict],
    safe_accept: list[dict],
    keep: list[dict],
    ev_breakdown: dict,
    source_breakdown: dict,
) -> dict:
    """Build actionable recommendations based on borderline analysis."""
    recs: dict[str, list[str]] = {
        "safe_reject": [],
        "safe_accept": [],
        "keep_borderline": [],
        "tuning_suggestions": [],
    }

    n_bl = len(borderline)
    n_sr = len(safe_reject)
    n_sa = len(safe_accept)

    # Safe reject recommendations
    weak_count = ev_breakdown.get(EVIDENCE_WEAK, 0)
    if weak_count > 0:
        recs["safe_reject"].append(
            f"{weak_count} borderline with evidence_quality=weak and score≤5 "
            "can be moved to reject. These have insufficient grounding and low impact."
        )

    cap_weak = source_breakdown.get("evidence_cap_weak", 0)
    if cap_weak > 0:
        recs["safe_reject"].append(
            f"{cap_weak} findings were capped at score=5 due to weak evidence. "
            "Consider lowering the weak evidence cap from 5 to 4 to auto-reject these."
        )

    generic_count = source_breakdown.get("generic_wording", 0)
    low_impact_count = source_breakdown.get("low_impact_axis", 0)
    if generic_count + low_impact_count > n_bl * 0.2:
        recs["safe_reject"].append(
            f"{generic_count + low_impact_count} borderline findings have "
            "generic wording or no impact axis — safe to reject."
        )

    # Safe accept recommendations
    cap_partial = source_breakdown.get("evidence_cap_partial", 0)
    if cap_partial > 0:
        recs["safe_accept"].append(
            f"{cap_partial} findings are capped at score=6-7 due to partial evidence. "
            "If severity is КРИТИЧЕСКОЕ/ЭКОНОМИЧЕСКОЕ and impact is high, "
            "consider raising the partial evidence exception to score=7 for economic findings too."
        )

    no_cap_56 = source_breakdown.get("score_56_no_cap", 0)
    if no_cap_56 > 0:
        recs["safe_accept"].append(
            f"{no_cap_56} findings have valid evidence but natural score 5-6. "
            "If they have clear action and high impact, consider lowering accept threshold from 7 to 6."
        )

    # Keep borderline
    cross = source_breakdown.get("cross_section_dep", 0)
    if cross > 0:
        recs["keep_borderline"].append(
            f"{cross} borderline findings involve cross-section dependencies. "
            "Keep as borderline — they need cross-document validation before accept/reject."
        )

    # Tuning suggestions
    if n_sr > n_bl * 0.4:
        recs["tuning_suggestions"].append(
            f"High safe_reject rate ({n_sr}/{n_bl}={n_sr*100//n_bl if n_bl else 0}%). "
            "Rule tuning recommendation: lower weak evidence cap from 5→4 to push "
            "weak+low_impact findings into reject automatically."
        )
    if n_sa > n_bl * 0.2:
        recs["tuning_suggestions"].append(
            f"Safe accept rate ({n_sa}/{n_bl}={n_sa*100//n_bl if n_bl else 0}%). "
            "Consider lowering accept threshold for valid+high_impact findings from 7→6."
        )
    if source_breakdown.get("evidence_cap_partial", 0) > n_bl * 0.3:
        recs["tuning_suggestions"].append(
            "Most borderline come from partial evidence cap. "
            "Consider extending the КРИТИЧЕСКОЕ+partial exception to ЭКОНОМИЧЕСКОЕ+partial."
        )

    return recs


# ─── Markdown report ──────────────────────────────────────────────────────────

def render_markdown(
    analysis: dict,
    batch_summary: Optional[dict],
    legacy_rows: list[dict],
) -> str:
    ov = analysis["overview"]
    br = analysis["breakdown"]
    cl = analysis["classification"]
    recs = analysis["recommendations"]
    samples = analysis["samples"]

    lines = [
        "# Borderline Analysis — Critic V2",
        "",
        "## Overview",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Total decisions | {ov['total_decisions']} |",
        f"| Borderline | **{ov['borderline_count']}** ({ov['borderline_rate']*100:.1f}%) |",
        f"| Accepted | {ov['accepted_count']} ({ov['accepted_count']*100//ov['total_decisions'] if ov['total_decisions'] else 0}%) |",
        f"| Rejected | {ov['rejected_count']} ({ov['rejected_count']*100//ov['total_decisions'] if ov['total_decisions'] else 0}%) |",
        f"| Merged | {ov['merged_count']} |",
        "",
        "## Borderline Classification",
        "",
        f"| Class | Count |",
        f"|-------|-------|",
        f"| safe_reject_candidate | **{cl['safe_reject_candidates']}** |",
        f"| safe_accept_candidate | **{cl['safe_accept_candidates']}** |",
        f"| keep_borderline | {cl['keep_borderline']} |",
        "",
    ]

    # Breakdowns
    lines += [
        "## Breakdown",
        "",
        "### By Evidence Quality",
        "",
        "| Evidence Quality | Count |",
        "|-----------------|-------|",
    ]
    for k, v in br["by_evidence_quality"].items():
        lines.append(f"| {k} | {v} |")

    lines += [
        "",
        "### By Severity",
        "",
        "| Severity | Count |",
        "|----------|-------|",
    ]
    for k, v in br["by_severity"].items():
        lines.append(f"| {k} | {v} |")

    lines += [
        "",
        "### By Impact Area",
        "",
        "| Impact Area | Count |",
        "|-------------|-------|",
    ]
    for k, v in list(br["by_impact_area"].items())[:10]:
        lines.append(f"| {k} | {v} |")

    lines += [
        "",
        "### By Borderline Source (Why Borderline?)",
        "",
        "| Source | Count | Meaning |",
        "|--------|-------|---------|",
    ]
    _source_meanings = {
        "evidence_cap_weak": "Score capped at 5 due to weak evidence",
        "evidence_cap_partial": "Score capped at 6/7 due to partial evidence",
        "score_56_no_cap": "Valid evidence but natural score 5-6",
        "cross_section_dep": "Requires cross-document validation",
        "generic_wording": "Generic 'please check' language detected",
        "low_impact_axis": "No identifiable high-value impact area",
        "unknown": "Did not match specific source pattern",
    }
    for src, cnt in br["by_source"].items():
        meaning = _source_meanings.get(src, "")
        lines.append(f"| {src} | {cnt} | {meaning} |")

    lines += [
        "",
        "### Score Distribution (Borderline)",
        "",
        "| Score | Count |",
        "|-------|-------|",
    ]
    for s, c in br["score_distribution"].items():
        lines.append(f"| {s} | {c} |")

    # Recommendations
    lines += ["", "## Recommendations", ""]

    if recs["safe_reject"]:
        lines += ["### A) Safe Reject Candidates", ""]
        for rec in recs["safe_reject"]:
            lines.append(f"- {rec}")
        lines.append("")

    if recs["safe_accept"]:
        lines += ["### B) Safe Accept Candidates", ""]
        for rec in recs["safe_accept"]:
            lines.append(f"- {rec}")
        lines.append("")

    if recs["keep_borderline"]:
        lines += ["### C) Keep Borderline", ""]
        for rec in recs["keep_borderline"]:
            lines.append(f"- {rec}")
        lines.append("")

    if recs["tuning_suggestions"]:
        lines += ["### D) Tuning Suggestions for Next Pass", ""]
        for sug in recs["tuning_suggestions"]:
            lines.append(f"- {sug}")
        lines.append("")

    # Legacy comparison summary
    if legacy_rows:
        bl_legacy_rows = [r for r in legacy_rows if r.get("v2_decision") in ("borderline", "low_priority")]
        pass_then_border = [r for r in bl_legacy_rows if r.get("legacy_verdict") == "pass"]
        lines += [
            "## Legacy Comparison (Borderline)",
            "",
            f"Of {len(bl_legacy_rows)} borderline findings that also have legacy verdicts:",
            f"- **{len(pass_then_border)}** were `pass` in legacy but now borderline in v2",
            f"- {len(bl_legacy_rows) - len(pass_then_border)} were already flagged as problem in legacy",
            "",
        ]

    # Samples
    if samples:
        lines += [
            f"## Borderline Samples (top {len(samples)})",
            "",
            "| ID | Project | Score | EV Quality | Severity | Impact | Source | Class |",
            "|----|---------|-------|------------|----------|--------|--------|-------|",
        ]
        for s in samples:
            fid = (s.get("finding_id") or "?")[:20]
            proj = (s.get("project") or "?")[:20]
            score = s.get("usefulness_score", 0)
            ev = s.get("evidence_quality", "?")
            sev = (s.get("severity") or "?")[:12]
            imp = (s.get("impact_area") or "none")[:15]
            src = s.get("borderline_source", "?")[:22]
            cls = s.get("classification", "?")[:22]
            lines.append(f"| {fid} | {proj} | {score} | {ev} | {sev} | {imp} | {src} | {cls} |")

    lines += ["", "---", "_Generated by analyze_critic_v2_batch.py. Production pipeline NOT modified._"]
    return "\n".join(lines) + "\n"


# ─── CSV export ───────────────────────────────────────────────────────────────

def export_csv(borderline_decisions: list[dict], csv_path: Path) -> None:
    fields = [
        "finding_id", "project", "decision", "usefulness_score",
        "evidence_quality", "severity", "impact_area",
        "reject_reason", "has_evidence", "has_action", "has_impact",
        "borderline_source", "classification",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for d in borderline_decisions:
            row = {k: d.get(k) for k in fields}
            row["borderline_source"] = _detect_borderline_source(d)
            row["classification"] = classify_borderline(d)
            writer.writerow(row)


# ─── Main ─────────────────────────────────────────────────────────────────────

def run_analysis(
    batch_output_dir: Path,
    top_n: int = 30,
    export_csv_flag: bool = False,
    export_md_flag: bool = True,
    quiet: bool = False,
) -> dict:
    """
    Main analysis entry point. Returns analysis dict.
    Writes borderline_analysis.json and optionally .md and .csv.
    """
    if not batch_output_dir.exists():
        raise FileNotFoundError(f"Batch output dir not found: {batch_output_dir}")

    if not quiet:
        print(f"Loading decisions from: {batch_output_dir}")

    all_decisions = load_batch_decisions(batch_output_dir)
    batch_summary = load_batch_summary(batch_output_dir)
    legacy_rows = load_legacy_comparison_rows(batch_output_dir)

    if not quiet:
        print(f"  Loaded {len(all_decisions)} decisions total")
        borderline_count = sum(1 for d in all_decisions if d.get("decision") in ("borderline", "low_priority"))
        print(f"  Borderline: {borderline_count}")

    if not all_decisions:
        if not quiet:
            print("  No decisions found — empty analysis.")
        analysis = {
            "overview": {
                "total_decisions": 0, "borderline_count": 0, "borderline_rate": 0.0,
                "accepted_count": 0, "rejected_count": 0, "merged_count": 0,
            },
            "breakdown": {
                "by_evidence_quality": {}, "by_severity": {}, "by_impact_area": {},
                "by_source": {}, "score_distribution": {},
            },
            "classification": {
                "safe_reject_candidates": 0, "safe_accept_candidates": 0,
                "keep_borderline": 0, "by_class": {},
            },
            "safe_reject_ids": [],
            "safe_accept_ids": [],
            "samples": [],
            "recommendations": {
                "safe_reject": [], "safe_accept": [],
                "keep_borderline": [], "tuning_suggestions": [],
            },
        }
    else:
        analysis = analyze_borderline(all_decisions, top_n=top_n)

    # Write JSON
    json_path = batch_output_dir / "borderline_analysis.json"
    json_path.write_text(
        json.dumps(analysis, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    if not quiet:
        print(f"  Written: {json_path}")

    # Write Markdown
    md_path = batch_output_dir / "borderline_analysis.md"
    md = render_markdown(analysis, batch_summary, legacy_rows)
    md_path.write_text(md, encoding="utf-8")
    if not quiet:
        print(f"  Written: {md_path}")

    # Write CSV
    if export_csv_flag:
        borderline = [d for d in all_decisions if d.get("decision") in ("borderline", "low_priority")]
        csv_path = batch_output_dir / "borderline_samples.csv"
        export_csv(borderline, csv_path)
        if not quiet:
            print(f"  Written: {csv_path}")

    return analysis


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Analyze borderline findings from a batch critic v2 output directory.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze default batch output
  python %(prog)s --batch-output-dir /tmp/critic_v2_batch

  # With CSV export and top 30 samples
  python %(prog)s --batch-output-dir /tmp/critic_v2_batch --top-n 30 --export-csv

  # Quiet mode (no console output)
  python %(prog)s --batch-output-dir /tmp/critic_v2_batch --quiet
""",
    )
    parser.add_argument(
        "--batch-output-dir", type=Path, required=True,
        help="Directory with batch critic v2 output (contains batch_results.json).",
    )
    parser.add_argument(
        "--top-n", type=int, default=30,
        help="Number of borderline samples to include in report (default: 30).",
    )
    parser.add_argument(
        "--export-csv", action="store_true",
        help="Export borderline findings as borderline_samples.csv.",
    )
    parser.add_argument(
        "--export-md", action="store_true", default=True,
        help="Export markdown report (default: always on).",
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="Suppress console output.",
    )
    args = parser.parse_args()

    try:
        analysis = run_analysis(
            batch_output_dir=args.batch_output_dir,
            top_n=args.top_n,
            export_csv_flag=args.export_csv,
            export_md_flag=args.export_md,
            quiet=args.quiet,
        )
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    if not args.quiet:
        ov = analysis["overview"]
        cl = analysis["classification"]
        recs = analysis["recommendations"]
        print()
        print("=" * 60)
        print("BORDERLINE ANALYSIS SUMMARY")
        print("=" * 60)
        print(f"  Total decisions : {ov['total_decisions']}")
        print(f"  Borderline      : {ov['borderline_count']} ({ov['borderline_rate']*100:.1f}%)")
        print(f"  Accepted        : {ov['accepted_count']}")
        print(f"  Rejected        : {ov['rejected_count']}")
        print()
        print(f"  ── Classification ──────────────────────────────")
        print(f"  safe_reject_candidate : {cl['safe_reject_candidates']}")
        print(f"  safe_accept_candidate : {cl['safe_accept_candidates']}")
        print(f"  keep_borderline       : {cl['keep_borderline']}")
        print()
        br = analysis["breakdown"]
        print(f"  ── By Evidence Quality ─────────────────────────")
        for k, v in br["by_evidence_quality"].items():
            print(f"  {k:<10} : {v}")
        print()
        print(f"  ── By Borderline Source ────────────────────────")
        for src, cnt in list(br["by_source"].items())[:6]:
            print(f"  {src:<28} : {cnt}")
        print()
        if recs["tuning_suggestions"]:
            print("  ── Tuning Suggestions ──────────────────────────")
            for sug in recs["tuning_suggestions"]:
                print(f"  • {sug[:72]}")
            print()
        print(f"  Output: {args.batch_output_dir}/borderline_analysis.{{json,md}}")
        if args.export_csv:
            print(f"          {args.batch_output_dir}/borderline_samples.csv")
        print("=" * 60)
        print("  NOTE: Production pipeline NOT modified.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
