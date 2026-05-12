#!/usr/bin/env python3
"""
run_critic_v2_experiment_matrix.py
------------------------------------
Run all critic_v2 experiment modes on the same manifest-pinned project set
and produce a comparison report.

The three canonical experiments:
  A. det_only           — deterministic critic_v2 only, no LLM, no context
  B. llm_no_context     — deterministic + LLM taxonomy gate, no context enrichment
  C. llm_context_policy — deterministic + LLM + context enrichment + policy v3

Usage:
    # Mock LLM (fast, repeatable, no API calls)
    python backend/scripts/run_critic_v2_experiment_matrix.py \\
        --manifest /tmp/manifest/human_benchmark_manifest.json \\
        --output-dir /tmp/matrix_mock \\
        --llm-provider mock

    # Real LLM (slow, requires claude_runner or openrouter key)
    python backend/scripts/run_critic_v2_experiment_matrix.py \\
        --manifest /tmp/manifest/human_benchmark_manifest.json \\
        --output-dir /tmp/matrix_real \\
        --llm-provider claude_runner \\
        --llm-model claude-haiku-4-5-20251001 \\
        --with-real-llm

    # Dry run (validate manifest, don't run)
    python backend/scripts/run_critic_v2_experiment_matrix.py \\
        --manifest /tmp/manifest/human_benchmark_manifest.json \\
        --output-dir /tmp/matrix_dry \\
        --dry-run

NOT connected to production pipeline. Read-only access to production artifacts.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Optional

# ─── sys.path bootstrap ───────────────────────────────────────────────────────

_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

_BENCHMARK_SCRIPT = _SCRIPT_DIR / "benchmark_critic_v2_against_human.py"

# ─── Triage imports (lazy, only when --triage is used) ───────────────────────

def _load_triage_module():
    from backend.app.pipeline.stages.findings_review.critic_v2.triage import (
        triage_metrics_to_dict,
    )
    from backend.scripts.replay_critic_v2_triage_policy import (
        _load_benchmark_records,
        _load_llm_decisions,
        replay_triage_on_records,
        compute_section_breakdown,
        build_triage_artifacts,
        triage_metrics_to_dict as _tmt,
    )
    return {
        "load_records": _load_benchmark_records,
        "load_llm": _load_llm_decisions,
        "replay": replay_triage_on_records,
        "section_breakdown": compute_section_breakdown,
        "build_artifacts": build_triage_artifacts,
        "metrics_to_dict": _tmt,
    }

# ─── Experiment definitions ───────────────────────────────────────────────────

EXPERIMENTS = {
    "det_only": {
        "label": "A. Deterministic only",
        "description": "critic_v2 deterministic engine, no LLM, no context enrichment",
        "llm_gate": False,
        "context_enrichment": False,
    },
    "llm_no_context": {
        "label": "B. LLM gate, no context",
        "description": "deterministic + LLM taxonomy gate, no context enrichment",
        "llm_gate": True,
        "context_enrichment": False,
    },
    "llm_context_policy": {
        "label": "C. LLM gate + context enrichment (policy v3)",
        "description": "deterministic + LLM gate + context enrichment + policy v3",
        "llm_gate": True,
        "context_enrichment": True,
    },
}

# ─── Manifest loader ──────────────────────────────────────────────────────────

def load_manifest(manifest_path: Path) -> dict:
    """Load and validate manifest JSON."""
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid manifest JSON: {e}")
    if "records" not in manifest:
        raise ValueError("Manifest missing 'records' key")
    return manifest


def filter_manifest_records(
    records: list[dict],
    sections: Optional[list[str]] = None,
    limit_per_section: Optional[int] = None,
) -> list[dict]:
    """Filter and optionally limit records."""
    section_filter = {s.upper() for s in sections} if sections else None
    filtered = [
        r for r in records
        if not section_filter or r.get("section", "").upper() in section_filter
    ]
    if limit_per_section:
        by_section: dict[str, list] = defaultdict(list)
        for r in filtered:
            by_section[r.get("section", "?")].append(r)
        limited = []
        for sec_records in by_section.values():
            limited.extend(sec_records[:limit_per_section])
        return limited
    return filtered


# ─── Single experiment runner ─────────────────────────────────────────────────

def run_one_experiment(
    experiment_name: str,
    experiment_def: dict,
    project_paths: list[str],
    output_dir: Path,
    llm_provider: str = "mock",
    llm_model: Optional[str] = None,
    llm_timeout: int = 180,
    max_candidates: int = 50,
    quiet: bool = False,
    triage: bool = False,
    candidate_strategy: str = "conservative",
) -> dict:
    """
    Run one experiment mode on the given project paths.
    Invokes benchmark_critic_v2_against_human.py as a subprocess.

    Returns the parsed summary dict from human_benchmark_summary.json.
    """
    exp_output_dir = output_dir / experiment_name
    exp_output_dir.mkdir(parents=True, exist_ok=True)

    # Write project paths to a temp file to pass as --project args
    cmd = [
        sys.executable, str(_BENCHMARK_SCRIPT),
        "--output-dir", str(exp_output_dir),
        "--max-candidates", str(max_candidates),
    ]
    for path in project_paths:
        cmd += ["--project", path]

    if experiment_def["llm_gate"]:
        cmd += [
            "--llm-gate",
            "--llm-provider", llm_provider,
        ]
        if llm_model:
            cmd += ["--llm-model", llm_model]
        cmd += ["--llm-timeout", str(llm_timeout)]

    if experiment_def["context_enrichment"]:
        cmd.append("--context-enrichment")

    if quiet:
        cmd.append("--quiet")

    if triage:
        cmd.append("--triage")

    if candidate_strategy != "conservative":
        cmd += ["--candidate-strategy", candidate_strategy]

    t0 = time.monotonic()
    proc = subprocess.run(cmd, capture_output=True, text=True)
    elapsed_ms = int((time.monotonic() - t0) * 1000)

    summary_path = exp_output_dir / "human_benchmark_summary.json"
    if not summary_path.exists():
        return {
            "experiment": experiment_name,
            "error": f"benchmark script failed (rc={proc.returncode}). stderr: {proc.stderr[:300]}",
            "elapsed_ms": elapsed_ms,
        }

    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        return {
            "experiment": experiment_name,
            "error": f"could not parse summary: {e}",
            "elapsed_ms": elapsed_ms,
        }

    summary["experiment"] = experiment_name
    summary["elapsed_ms"] = elapsed_ms

    # Load triage metrics from artifact if --triage was requested
    if triage:
        triage_metrics_path = exp_output_dir / "critic_v2_triage_metrics.json"
        if triage_metrics_path.exists():
            try:
                summary["triage_metrics"] = json.loads(
                    triage_metrics_path.read_text(encoding="utf-8")
                )
            except (json.JSONDecodeError, OSError):
                pass

    # Load candidate selection aggregate
    cand_path = exp_output_dir / "critic_v2_llm_candidate_selection.json"
    if cand_path.exists():
        try:
            summary["candidate_selection_aggregate"] = json.loads(
                cand_path.read_text(encoding="utf-8")
            )
        except (json.JSONDecodeError, OSError):
            pass

    # Store candidate strategy in run_config for downstream readers
    if "run_config" not in summary:
        summary["run_config"] = {}
    summary["run_config"]["candidate_strategy"] = candidate_strategy

    return summary


# ─── Comparison report builder ────────────────────────────────────────────────

def _experiment_metrics(summary: dict) -> dict:
    """Extract key metrics from a summary dict."""
    om = summary.get("overall_metrics") or {}
    llm = summary.get("llm_impact") or {}
    ctx = summary.get("context_enrichment") or {}
    cfg = summary.get("run_config") or {}

    tax = llm.get("taxonomy_reason_breakdown") or {}
    total_tax = sum(tax.values())
    other_count = tax.get("other", 0)
    other_rate = round(other_count / total_tax * 100, 1) if total_tax else None

    false_reject = om.get("false_reject", 0)
    is_danger = false_reject > 0

    return {
        "experiment": summary.get("experiment", "?"),
        "total_findings": om.get("total_findings", 0),
        "total_mapped": om.get("total_mapped", 0),
        "human_accepted": om.get("human_accepted", 0),
        "human_rejected": om.get("human_rejected", 0),
        "critic_accepted": om.get("critic_accepted", 0),
        "critic_rejected": om.get("critic_rejected", 0),
        "critic_borderline": om.get("critic_borderline", 0),
        "false_reject": false_reject,
        "false_reject_rate": om.get("false_reject_rate", 0.0),
        "false_accept": om.get("false_accept", 0),
        "false_accept_rate": om.get("false_accept_rate", 0.0),
        "agreement_rate": om.get("agreement_rate", 0.0),
        # LLM-specific
        "llm_gate_used": cfg.get("llm_gate_used", False),
        "context_enrichment_used": cfg.get("context_enrichment_used", False),
        "needs_human_count": llm.get("needs_human_count"),
        "reject_by_llm_count": llm.get("reject_by_llm_count"),
        "other_unclassified_count": other_count if total_tax else None,
        "other_unclassified_rate": other_rate,
        # Safety
        "is_danger": is_danger,
        "false_rejects_detail": summary.get("false_rejects_from_file"),
        # Context stats
        "avg_context_blocks": ctx.get("avg_context_blocks"),
        "pct_with_any_context": ctx.get("pct_with_any_context"),
        # Provider errors
        "provider_errors_count": len(summary.get("provider_errors") or []),
        # Elapsed
        "elapsed_ms": summary.get("elapsed_ms", 0),
        # Triage metrics (populated if --triage was used)
        "triage": summary.get("triage_metrics"),
        # Candidate selection stats
        "candidate_strategy": summary.get("run_config", {}).get("candidate_strategy"),
        "candidate_count": summary.get("candidate_selection_aggregate", {}).get("candidate_count"),
        "candidate_rate": summary.get("candidate_selection_aggregate", {}).get("candidate_rate"),
    }


def _compute_candidate_score(m: dict) -> float:
    """
    Compute a candidate score for promotion ranking.

    Higher = better candidate. Rules:
    - false_reject > 0 → score = -inf (automatic disqualification)
    - provider_errors > 0 → heavy penalty
    - false_accept lower is better (main metric)
    - other_unclassified lower is better
    - needs_human not too high (capped at reasonable level)

    Score is NOT a production promotion decision — it's an ordering hint for
    human reviewers to focus attention.
    """
    if m.get("is_danger"):
        return float("-inf")
    if (m.get("provider_errors_count") or 0) > 0:
        base = -100.0
    else:
        base = 0.0

    # False accept rate: lower is better (0-100 scale, inverted)
    fa_rate = m.get("false_accept_rate") or 0.0
    base -= fa_rate * 100

    # Other unclassified rate: lower is better (only penalise when LLM gate was used)
    # With mock provider, other_rate=100% is expected and not meaningful
    other_rate = m.get("other_unclassified_rate")
    if other_rate is not None and m.get("llm_gate_used") and (m.get("reject_by_llm_count") or 0) > 0:
        base -= other_rate * 0.3

    # Agreement rate: higher is better
    base += (m.get("agreement_rate") or 0.0) * 50

    return round(base, 3)


def build_matrix_summary(
    manifest: dict,
    experiment_summaries: list[dict],
    experiment_names: list[str],
) -> dict:
    """
    Build the cross-experiment comparison summary.

    Uses matched human decision counts (human_accepted_matched /
    human_rejected_matched) from the manifest when available.
    Falls back to raw counts for backward compatibility with manifest v1.
    Surfaces manifest_warnings so reviewers know about data quality issues.
    """
    metrics_by_exp: dict[str, dict] = {}
    false_rejects_by_exp: dict[str, list] = {}

    for summary in experiment_summaries:
        exp_name = summary.get("experiment", "?")
        if "error" in summary:
            metrics_by_exp[exp_name] = {
                "experiment": exp_name, "error": summary["error"],
                "is_danger": False, "false_reject": 0,
            }
            continue

        m = _experiment_metrics(summary)
        metrics_by_exp[exp_name] = m

        # Load false_rejects.json from experiment output if present
        exp_output = summary.get("_output_dir")
        if exp_output:
            fr_path = Path(exp_output) / "false_rejects.json"
            if fr_path.exists():
                try:
                    false_rejects_by_exp[exp_name] = json.loads(fr_path.read_text())
                except (json.JSONDecodeError, OSError):
                    pass

    # Per-section breakdown
    by_section_by_exp: dict[str, dict[str, dict]] = defaultdict(dict)
    for summary in experiment_summaries:
        exp_name = summary.get("experiment", "?")
        if "error" in summary:
            continue
        by_sec = summary.get("by_section") or {}
        for sec, sec_data in by_sec.items():
            by_section_by_exp[sec][exp_name] = sec_data

    # Ranking
    ranking = sorted(
        [m for m in metrics_by_exp.values() if "error" not in m],
        key=lambda m: _compute_candidate_score(m),
        reverse=True,
    )

    any_danger = any(m.get("is_danger") for m in metrics_by_exp.values())

    # Use matched counts when available (manifest v2+); fall back to raw for v1
    stats = manifest.get("stats", {})
    matched_accepted = stats.get("total_accepted_matched", stats.get("total_accepted", 0))
    matched_rejected = stats.get("total_rejected_matched", stats.get("total_rejected", 0))
    manifest_warnings = manifest.get("manifest_warnings") or []

    # Augment manifest_stats with matched counts for downstream consumers
    manifest_stats_augmented = dict(stats)
    manifest_stats_augmented.setdefault("total_accepted_matched", matched_accepted)
    manifest_stats_augmented.setdefault("total_rejected_matched", matched_rejected)

    return {
        "manifest_stats": manifest_stats_augmented,
        "manifest_warnings": manifest_warnings,
        "experiments_run": experiment_names,
        "any_danger": any_danger,
        "metrics_by_experiment": metrics_by_exp,
        "false_rejects_by_experiment": false_rejects_by_exp,
        "by_section": {sec: dict(by_exp) for sec, by_exp in by_section_by_exp.items()},
        "ranking": [
            {
                "experiment": m["experiment"],
                "candidate_score": _compute_candidate_score(m),
                "false_reject": m.get("false_reject", 0),
                "false_accept": m.get("false_accept"),
                "false_accept_rate_pct": round((m.get("false_accept_rate") or 0.0) * 100, 1),
                "agreement_rate_pct": round((m.get("agreement_rate") or 0.0) * 100, 1),
                "is_danger": m.get("is_danger", False),
            }
            for m in ranking
        ],
    }


# ─── Markdown renderer ────────────────────────────────────────────────────────

def render_matrix_markdown(matrix_summary: dict) -> str:
    mstats = matrix_summary["manifest_stats"]
    exps = matrix_summary["experiments_run"]
    metrics = matrix_summary["metrics_by_experiment"]
    ranking = matrix_summary["ranking"]
    any_danger = matrix_summary["any_danger"]
    by_section = matrix_summary.get("by_section", {})
    manifest_warnings = matrix_summary.get("manifest_warnings") or []

    lines = [
        "# Critic V2 Experiment Matrix — Comparison Report",
        "",
    ]

    if any_danger:
        lines += [
            "## ⚠️ DANGER: False Rejects Detected",
            "",
            "One or more experiments introduced false rejects (human-accepted findings "
            "incorrectly rejected by critic_v2). These experiments are **NOT candidates** "
            "for production promotion.",
            "",
        ]

    if manifest_warnings:
        lines += [
            "## ⚠️ Manifest Data Quality Warnings",
            "",
            "_These warnings were detected in the source manifest. Benchmark metrics use "
            "matched decisions only, so FR/FA counts are unaffected — but raw totals in "
            "the summary below may not match the actual matched decision count._",
            "",
        ]
        for w in manifest_warnings:
            lines.append(f"- {w}")
        lines.append("")

    acc_matched = mstats.get('total_accepted_matched', mstats.get('total_accepted', 0))
    rej_matched = mstats.get('total_rejected_matched', mstats.get('total_rejected', 0))
    lines += [
        "## Manifest Summary",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Total projects | {mstats.get('total_projects', 0)} |",
        f"| Total findings | {mstats.get('total_findings', 0)} |",
        f"| Human accepted (raw) | {mstats.get('total_accepted', 0)} |",
        f"| Human rejected (raw) | {mstats.get('total_rejected', 0)} |",
        f"| Human accepted (matched) | **{acc_matched}** |",
        f"| Human rejected (matched) | **{rej_matched}** |",
        "",
        "## Experiment Comparison",
        "",
        "| Experiment | FR | FA | FA% | FR% | Agreement% | needs_human | other% | Score |",
        "|------------|-----|-----|-----|-----|------------|------------|--------|-------|",
    ]

    for exp in exps:
        m = metrics.get(exp, {})
        if "error" in m:
            lines.append(f"| {exp} | ERR | ERR | — | — | — | — | — | — |")
            continue
        danger = "⚠️ " if m.get("is_danger") else ""
        score = _compute_candidate_score(m)
        score_str = f"{score:.1f}" if score != float("-inf") else "**-∞ DANGER**"
        lines.append(
            f"| {danger}{exp} "
            f"| {m.get('false_reject', 0)} "
            f"| {m.get('false_accept', '?')} "
            f"| {m.get('false_accept_rate', 0)*100:.1f}% "
            f"| {m.get('false_reject_rate', 0)*100:.1f}% "
            f"| {m.get('agreement_rate', 0)*100:.1f}% "
            f"| {m.get('needs_human_count') or '—'} "
            f"| {m.get('other_unclassified_rate') or '—'} "
            f"| {score_str} |"
        )
    lines.append("")

    # Safety section
    danger_exps = [e for e in exps if metrics.get(e, {}).get("is_danger")]
    if danger_exps:
        lines += ["## ⚠️ False Reject Detail", ""]
        for exp in danger_exps:
            fr_list = matrix_summary.get("false_rejects_by_experiment", {}).get(exp, [])
            lines.append(f"### `{exp}` — {len(fr_list)} false rejects")
            lines.append("")
            for r in fr_list[:5]:
                lines.append(
                    f"- `{r.get('finding_id')}` [{r.get('section')}] "
                    f"reason={r.get('critic_reject_reason')} "
                    f"| {r.get('title', '')[:60]}"
                )
            if len(fr_list) > 5:
                lines.append(f"- _...and {len(fr_list)-5} more_")
            lines.append("")

    # Per-section breakdown
    if by_section:
        lines += ["## Per-Section Breakdown", ""]
        for sec, by_exp in sorted(by_section.items()):
            lines += [f"### Section: `{sec}`", ""]
            lines += [
                f"| Experiment | Projects | Findings | H.Acc | H.Rej | FR | FA |",
                f"|------------|----------|----------|-------|-------|-----|-----|",
            ]
            for exp in exps:
                sd = by_exp.get(exp, {})
                if not sd:
                    continue
                lines.append(
                    f"| {exp} "
                    f"| {sd.get('projects', '?')} "
                    f"| {sd.get('total_mapped', sd.get('total_findings', '?'))} "
                    f"| {sd.get('human_accepted', '?')} "
                    f"| {sd.get('human_rejected', '?')} "
                    f"| {sd.get('false_reject', '?')} "
                    f"| {sd.get('false_accept', '?')} |"
                )
            lines.append("")

    # Ranking
    lines += ["## Candidate Ranking", ""]
    lines += [
        "_Note: Ranking is an ordering hint for human reviewers. "
        "false_reject > 0 disqualifies from promotion._",
        "",
        "| Rank | Experiment | Score | FR | FA | FA% | Agreement% | Eligible |",
        "|------|------------|-------|-----|-----|-----|------------|---------|",
    ]
    for i, r in enumerate(ranking, 1):
        danger_marker = "❌ DANGER" if r.get("is_danger") else "✓"
        lines.append(
            f"| {i} | {r['experiment']} "
            f"| {r['candidate_score']:.1f} "
            f"| {r['false_reject']} "
            f"| {r.get('false_accept', '?')} "
            f"| {r.get('false_accept_rate_pct', '?')}% "
            f"| {r.get('agreement_rate_pct', '?')}% "
            f"| {danger_marker} |"
        )
    lines.append("")

    # Triage section (only when any experiment has triage data)
    any_triage = any(
        metrics.get(exp, {}).get("triage") is not None
        for exp in exps
    )
    if any_triage:
        lines += [
            "## Triage Policy Metrics",
            "",
            "| Experiment | Total | SK | MR | BD | NC | SR | HC | VD | Collapse% | Recall% | Hidden.Acc |",
            "|------------|-------|----|----|----|----|----|----|-----|-----------|---------|------------|",
        ]
        for exp in exps:
            m = metrics.get(exp, {})
            triage = m.get("triage")
            if triage is None:
                lines.append(f"| {exp} | — | — | — | — | — | — | — | — | — | — | — |")
                continue
            tot = triage.get("total_findings") or 1
            wr = triage.get("workload_reduction_percent", 0)
            recall = triage.get("accepted_visible_recall")
            recall_str = f"{recall*100:.1f}%" if recall is not None else "—"
            lines.append(
                f"| {exp} "
                f"| {triage.get('total_findings', 0)} "
                f"| {triage.get('strong_keep_count', 0)} "
                f"| {triage.get('main_review_count', 0)} "
                f"| {triage.get('borderline_count', 0)} "
                f"| {triage.get('needs_context_count', 0)} "
                f"| {triage.get('suggested_reject_count', 0)} "
                f"| {triage.get('hidden_by_critic_count', 0)} "
                f"| {triage.get('visible_by_default_count', 0)} "
                f"| {wr:.1f}% "
                f"| {recall_str} "
                f"| {triage.get('hidden_human_accepted_count', 0)} |"
            )
        lines.append("")
        lines += [
            "_SK=strong_keep, MR=main_review, BD=borderline, NC=needs_context, "
            "SR=suggested_reject, HC=hidden_by_critic, VD=visible_by_default_",
            "_Recall = human_accepted NOT in hidden / total_human_accepted_",
            "",
        ]

    lines += [
        "---",
        "_Generated by run_critic_v2_experiment_matrix.py. Production pipeline NOT modified._",
    ]
    return "\n".join(lines) + "\n"


# ─── Console summary ──────────────────────────────────────────────────────────

def print_matrix_summary(matrix_summary: dict) -> None:
    mstats = matrix_summary["manifest_stats"]
    exps = matrix_summary["experiments_run"]
    metrics = matrix_summary["metrics_by_experiment"]
    ranking = matrix_summary["ranking"]
    any_danger = matrix_summary["any_danger"]
    manifest_warnings = matrix_summary.get("manifest_warnings") or []

    print()
    print("=" * 72)
    print("CRITIC V2 EXPERIMENT MATRIX — COMPARISON")
    print("=" * 72)
    print(f"  Manifest: {mstats.get('total_projects', 0)} projects, "
          f"{mstats.get('total_findings', 0)} findings")
    acc_matched = mstats.get('total_accepted_matched', mstats.get('total_accepted', 0))
    rej_matched = mstats.get('total_rejected_matched', mstats.get('total_rejected', 0))
    print(f"  Human (raw):     acc={mstats.get('total_accepted', 0)}, "
          f"rej={mstats.get('total_rejected', 0)}")
    print(f"  Human (matched): acc={acc_matched}, rej={rej_matched}")
    if manifest_warnings:
        print()
        print("  ⚠️  Manifest warnings:")
        for w in manifest_warnings:
            print(f"      {w}")
        print("  Metrics use matched counts. Raw totals may include unmatched/duplicate decisions.")
    print()
    print(f"  {'Experiment':<25} {'FR':>4} {'FA':>5} {'FA%':>6} {'Agr%':>6} "
          f"{'NH':>5} {'Oth%':>6} {'Scr':>7}")
    print(f"  {'-'*25} {'-'*4} {'-'*5} {'-'*6} {'-'*6} {'-'*5} {'-'*6} {'-'*7}")
    for exp in exps:
        m = metrics.get(exp, {})
        if "error" in m:
            print(f"  {exp:<25}  ERR")
            continue
        danger = " ⚠️ " if m.get("is_danger") else "   "
        score = _compute_candidate_score(m)
        score_str = f"{score:.1f}" if score != float("-inf") else "-inf"
        nh = m.get("needs_human_count")
        nh_str = str(nh) if nh is not None else "  -"
        oth = m.get("other_unclassified_rate")
        oth_str = f"{oth:.1f}%" if oth is not None else "   -"
        print(
            f"  {exp:<25}{danger}{m.get('false_reject', 0):>3} "
            f"{m.get('false_accept', '?'):>5} "
            f"{m.get('false_accept_rate', 0)*100:>5.1f}% "
            f"{m.get('agreement_rate', 0)*100:>5.1f}% "
            f"{nh_str:>5} {oth_str:>6} {score_str:>7}"
        )

    print()
    if any_danger:
        print("  ⚠️  DANGER: Some experiments have false_reject > 0!")
        print("     Check false_rejects.json in those experiment directories.")
        print()

    print("  ── Ranking (higher = better, -inf = disqualified) ───────────────")
    for i, r in enumerate(ranking, 1):
        marker = "  ← BEST" if i == 1 and not r.get("is_danger") else ""
        danger = " ⚠️ DANGER" if r.get("is_danger") else ""
        print(f"  {i}. {r['experiment']:<25} score={r['candidate_score']:>7.1f}"
              f"  FR={r['false_reject']}  FA={r.get('false_accept','?')}"
              f"{danger}{marker}")
    print()
    print("=" * 72)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run critic_v2 experiment matrix on a manifest-pinned project set "
            "and produce a comparison report."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Mock LLM (fast, no API)
  python %(prog)s \\
      --manifest /tmp/manifest/human_benchmark_manifest.json \\
      --output-dir /tmp/matrix_mock \\
      --llm-provider mock

  # Real LLM
  python %(prog)s \\
      --manifest /tmp/manifest/human_benchmark_manifest.json \\
      --output-dir /tmp/matrix_real \\
      --llm-provider claude_runner \\
      --llm-model claude-haiku-4-5-20251001 \\
      --with-real-llm

  # KJ only, 5 projects, dry run
  python %(prog)s \\
      --manifest /tmp/manifest/human_benchmark_manifest.json \\
      --sections KJ --limit-per-section 5 \\
      --output-dir /tmp/matrix_kj \\
      --dry-run
""",
    )
    parser.add_argument(
        "--manifest", type=Path, required=True,
        help="Path to human_benchmark_manifest.json",
    )
    parser.add_argument(
        "--output-dir", type=Path, required=True,
        help="Where to write experiment results.",
    )
    parser.add_argument(
        "--sections", nargs="+", default=None,
        help="Run only on these sections (e.g. KJ AR).",
    )
    parser.add_argument(
        "--limit-per-section", type=int, default=None,
        help="Max projects per section.",
    )
    parser.add_argument(
        "--experiments", nargs="+",
        default=["det_only", "llm_no_context", "llm_context_policy"],
        choices=list(EXPERIMENTS.keys()),
        help="Which experiments to run (default: all three).",
    )
    parser.add_argument(
        "--llm-provider", default="mock",
        choices=["mock", "noop", "claude_runner", "openrouter"],
        help="LLM provider for LLM-gate experiments (default: mock).",
    )
    parser.add_argument(
        "--llm-model", default=None,
        help="LLM model override for real providers.",
    )
    parser.add_argument(
        "--llm-timeout", type=int, default=180,
        help="LLM request timeout in seconds (default: 180).",
    )
    parser.add_argument(
        "--max-candidates", type=int, default=50,
        help="Max candidates per project for LLM gate (default: 50).",
    )
    parser.add_argument(
        "--with-real-llm", action="store_true",
        help=(
            "Required flag when using claude_runner or openrouter to confirm "
            "real API calls will be made (costs money/time)."
        ),
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Validate manifest and print plan without running benchmarks.",
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="Suppress per-project progress output.",
    )
    parser.add_argument(
        "--triage", action="store_true",
        help=(
            "Run offline triage policy after each experiment and include triage metrics "
            "in matrix summary. Does NOT call any LLM. Writes triage artifacts per experiment."
        ),
    )
    parser.add_argument(
        "--candidate-strategy",
        choices=["conservative", "expanded", "broad"],
        default="conservative",
        help=(
            "LLM candidate selection strategy. "
            "'conservative' = original (accept/borderline, evidence!=none). "
            "'expanded' = adds high-rejection-risk findings. "
            "'broad' = all accept/borderline (non-production). "
            "Default: conservative."
        ),
    )
    args = parser.parse_args()

    # Safety check for real LLM
    if args.llm_provider in ("claude_runner", "openrouter") and not args.with_real_llm:
        print(
            f"ERROR: --llm-provider {args.llm_provider} requires --with-real-llm flag. "
            "This confirms you accept the time/cost of real API calls.",
            file=sys.stderr,
        )
        return 1

    # Load manifest
    try:
        manifest = load_manifest(args.manifest)
    except (FileNotFoundError, ValueError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    records = filter_manifest_records(
        manifest["records"],
        sections=args.sections,
        limit_per_section=args.limit_per_section,
    )

    if not records:
        print("ERROR: No projects match the filter criteria.", file=sys.stderr)
        return 1

    project_paths = [r["project_path"] for r in records]
    experiments_to_run = args.experiments

    print(f"\nCritic V2 Experiment Matrix")
    print(f"  Manifest     : {args.manifest}")
    print(f"  Projects     : {len(records)}")
    print(f"  Experiments  : {', '.join(experiments_to_run)}")
    print(f"  LLM provider : {args.llm_provider}" + (f" / {args.llm_model}" if args.llm_model else ""))
    print(f"  Output dir   : {args.output_dir}")

    if args.sections:
        print(f"  Sections     : {args.sections}")
    if args.limit_per_section:
        print(f"  Limit/section: {args.limit_per_section}")

    # Section breakdown
    from collections import Counter
    sec_counts = Counter(r.get("section", "?") for r in records)
    print(f"  By section   : {dict(sorted(sec_counts.items()))}")
    print()

    if args.dry_run:
        print("  DRY RUN: not executing. Would run experiments:")
        for exp in experiments_to_run:
            edef = EXPERIMENTS[exp]
            print(f"    {exp}: llm={edef['llm_gate']}, context={edef['context_enrichment']}")
        return 0

    # Run experiments
    args.output_dir.mkdir(parents=True, exist_ok=True)
    experiment_summaries: list[dict] = []

    for exp_name in experiments_to_run:
        edef = EXPERIMENTS[exp_name]
        print(f"\n{'='*60}")
        print(f"Running: {edef['label']}")
        print(f"{'='*60}")

        t0 = time.monotonic()
        summary = run_one_experiment(
            experiment_name=exp_name,
            experiment_def=edef,
            project_paths=project_paths,
            output_dir=args.output_dir,
            llm_provider=args.llm_provider,
            llm_model=args.llm_model,
            llm_timeout=args.llm_timeout,
            max_candidates=args.max_candidates,
            quiet=args.quiet,
            triage=args.triage,
            candidate_strategy=args.candidate_strategy,
        )
        elapsed = int((time.monotonic() - t0) * 1000)
        summary["_output_dir"] = str(args.output_dir / exp_name)

        if "error" in summary:
            print(f"\n  ERROR in {exp_name}: {summary['error']}")
        else:
            om = summary.get("overall_metrics") or {}
            fr = om.get("false_reject", 0)
            fa = om.get("false_accept", 0)
            danger = " ← DANGER" if fr > 0 else " ✓ SAFE"
            print(f"\n  Completed in {elapsed//1000}s: FR={fr}{danger}  FA={fa}")

        experiment_summaries.append(summary)

    # Build comparison report
    matrix_summary = build_matrix_summary(
        manifest=manifest,
        experiment_summaries=experiment_summaries,
        experiment_names=experiments_to_run,
    )

    # Write outputs
    (args.output_dir / "experiment_matrix_summary.json").write_text(
        json.dumps(matrix_summary, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    md = render_matrix_markdown(matrix_summary)
    (args.output_dir / "experiment_matrix_summary.md").write_text(md, encoding="utf-8")

    print_matrix_summary(matrix_summary)

    print(f"  Output files:")
    print(f"    {args.output_dir}/experiment_matrix_summary.json")
    print(f"    {args.output_dir}/experiment_matrix_summary.md")
    for exp in experiments_to_run:
        print(f"    {args.output_dir}/{exp}/human_benchmark_summary.json")
    print()
    print("  NOTE: Production pipeline NOT modified.")
    print("        All project artifacts are read-only.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
