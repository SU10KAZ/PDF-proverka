#!/usr/bin/env python3
"""
batch_critic_v2.py
------------------
Batch offline runner for critic v2 experiments across multiple projects.

NOT connected to production pipeline.
Does NOT modify any production artifacts (03_findings.json, 03_findings_review.json, etc.).
Reads only: 03_findings.json and optionally 02_blocks_analysis.json per project.
Writes only: to --output-dir (default: /tmp/critic_v2_batch).

Pipeline per project:
    read 03_findings.json
    → deterministic critic v2 (normalize → rule_filter → score → dedup)
    → optional LLM gate (--llm-gate --llm-provider mock|noop)
    → per-project JSON artifacts in output-dir/<project_slug>/
    → cross-project summary report

Usage examples:
    # Run on 6 EOM projects (auto-discover from projects/ root)
    python backend/scripts/batch_critic_v2.py --section EOM --limit 6

    # Run on specific projects by path
    python backend/scripts/batch_critic_v2.py \\
        --projects "projects/213.*/EOM/133_23-ГК-ЭМ1" \\
                   "projects/213.*/EOM/133_23-ГК-ЭМ2"

    # All projects, with blocks index, mock LLM gate
    python backend/scripts/batch_critic_v2.py \\
        --section EOM --limit 6 --with-blocks --llm-gate --llm-provider mock

    # Compare with legacy critic verdicts
    python backend/scripts/batch_critic_v2.py \\
        --section EOM --limit 6 --compare-legacy

    # Full run across all 109 projects
    python backend/scripts/batch_critic_v2.py --all --output-dir /tmp/batch_all
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Optional

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
    run_critic_v2_offline,
)
from backend.app.pipeline.stages.findings_review.critic_v2.llm_gate import (
    merge_llm_decisions,
    run_llm_gate,
)

# ─── Constants ────────────────────────────────────────────────────────────────

DEFAULT_OUTPUT_DIR = Path("/tmp/critic_v2_batch")
PROJECTS_ROOT = _PROJECT_ROOT / "projects"
LEGACY_PASS_VERDICTS = {"pass"}
LEGACY_PROBLEM_VERDICTS = {
    "no_evidence", "phantom_block", "weak_evidence",
    "page_mismatch", "contradicts_text", "not_practical",
}


# ─── Project discovery ────────────────────────────────────────────────────────

def discover_projects(
    section: Optional[str] = None,
    limit: Optional[int] = None,
    project_patterns: Optional[list[str]] = None,
    all_projects: bool = False,
) -> list[Path]:
    """
    Discover project directories that have 03_findings.json.

    Returns list of project dirs (parent of _output/).
    """
    findings_files = sorted(PROJECTS_ROOT.rglob("03_findings.json"))

    if not findings_files:
        print(f"  No 03_findings.json found under {PROJECTS_ROOT}", file=sys.stderr)
        return []

    # Filter by explicit patterns
    if project_patterns:
        import re
        filtered = []
        for p in findings_files:
            project_dir = p.parent.parent
            project_str = str(project_dir)
            for pat in project_patterns:
                if re.search(pat, project_str):
                    filtered.append(p)
                    break
        findings_files = filtered

    # Filter by section
    if section:
        filtered = []
        for p in findings_files:
            project_dir = p.parent.parent
            info_file = project_dir / "project_info.json"
            proj_section = "unknown"
            if info_file.exists():
                try:
                    info = json.loads(info_file.read_text(encoding="utf-8"))
                    proj_section = info.get("section", "unknown").upper()
                except (json.JSONDecodeError, OSError):
                    pass
            # Also check directory hierarchy
            parts = project_dir.parts
            if proj_section == section.upper() or section.upper() in [p.upper() for p in parts]:
                filtered.append(p)
        findings_files = filtered

    # Apply limit
    if limit:
        findings_files = findings_files[:limit]

    return [p.parent.parent for p in findings_files]


# ─── Blocks index loader ─────────────────────────────────────────────────────

def load_blocks_index_for_project(project_dir: Path) -> Optional[set[str]]:
    """Extract block_ids from 02_blocks_analysis.json if present."""
    blocks_path = project_dir / "_output" / "02_blocks_analysis.json"
    if not blocks_path.exists():
        return None
    try:
        data = json.loads(blocks_path.read_text(encoding="utf-8"))
        block_ids: set[str] = set()
        analyses = data.get("block_analyses", [])
        if isinstance(analyses, list):
            for item in analyses:
                if isinstance(item, dict):
                    bid = item.get("block_id")
                    if bid:
                        block_ids.add(str(bid))
        return block_ids if block_ids else None
    except (json.JSONDecodeError, OSError):
        return None


# ─── Legacy comparison ────────────────────────────────────────────────────────

def load_legacy_review(project_dir: Path) -> Optional[dict]:
    """Load existing 03_findings_review.json produced by production critic."""
    review_path = project_dir / "_output" / "03_findings_review.json"
    if not review_path.exists():
        return None
    try:
        return json.loads(review_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def compare_with_legacy(
    v2_decisions: list,
    legacy_review: dict,
) -> dict:
    """
    Compare critic v2 decisions with legacy critic verdicts.

    Returns comparison stats dict.
    """
    legacy_map: dict[str, str] = {}
    for r in legacy_review.get("reviews", []):
        fid = r.get("finding_id") or r.get("id")
        if fid:
            legacy_map[fid] = r.get("verdict", "unknown")

    v2_map = {d.finding_id: d.decision for d in v2_decisions}

    agreement = 0
    disagree_v2_stricter = 0   # v2 reject/borderline, legacy pass
    disagree_v2_looser = 0     # v2 accept, legacy problem verdict
    only_in_v2 = 0             # not in legacy
    only_in_legacy = 0         # not in v2

    comparison_rows = []
    for fid, v2_dec in v2_map.items():
        leg_verdict = legacy_map.get(fid)
        if leg_verdict is None:
            only_in_v2 += 1
            continue

        leg_pass = leg_verdict in LEGACY_PASS_VERDICTS
        v2_pass = v2_dec == "accept"
        v2_reject = v2_dec in ("reject",)
        v2_border = v2_dec == "borderline"

        if leg_pass and v2_pass:
            agreement += 1
            row_type = "agree_pass"
        elif not leg_pass and v2_reject:
            agreement += 1
            row_type = "agree_reject"
        elif leg_pass and (v2_reject or v2_border):
            disagree_v2_stricter += 1
            row_type = "v2_stricter"
        elif not leg_pass and v2_pass:
            disagree_v2_looser += 1
            row_type = "v2_looser"
        else:
            row_type = "other"

        comparison_rows.append({
            "finding_id": fid,
            "legacy_verdict": leg_verdict,
            "v2_decision": v2_dec,
            "type": row_type,
        })

    for fid, leg_verdict in legacy_map.items():
        if fid not in v2_map:
            only_in_legacy += 1

    total_compared = len([r for r in comparison_rows])
    return {
        "total_compared": total_compared,
        "agreement": agreement,
        "disagree_v2_stricter": disagree_v2_stricter,
        "disagree_v2_looser": disagree_v2_looser,
        "only_in_v2": only_in_v2,
        "only_in_legacy": only_in_legacy,
        "rows": comparison_rows,
    }


# ─── Per-project runner ───────────────────────────────────────────────────────

def run_one_project(
    project_dir: Path,
    output_base: Path,
    with_blocks: bool = False,
    llm_gate: bool = False,
    llm_provider: str = "mock",
    prompt_path: Optional[Path] = None,
    max_candidates: int = 50,
    compare_legacy: bool = False,
) -> dict:
    """
    Run critic v2 on one project. Returns summary dict.
    """
    findings_path = project_dir / "_output" / "03_findings.json"
    if not findings_path.exists():
        return {
            "project": project_dir.name,
            "error": f"03_findings.json not found: {findings_path}",
            "skipped": True,
        }

    # Load findings
    try:
        raw = json.loads(findings_path.read_text(encoding="utf-8"))
        findings = raw.get("findings", raw.get("items", []))
        if not findings:
            return {"project": project_dir.name, "error": "Empty findings", "skipped": True}
    except (json.JSONDecodeError, OSError) as e:
        return {"project": project_dir.name, "error": str(e), "skipped": True}

    # Optional blocks index
    blocks_index = load_blocks_index_for_project(project_dir) if with_blocks else None

    findings_by_id = {f.get("id", f"idx_{i}"): f for i, f in enumerate(findings)}

    # Deterministic critic v2
    t0 = time.monotonic()
    det_result = run_critic_v2_offline(findings, blocks_index=blocks_index)
    det_ms = int((time.monotonic() - t0) * 1000)

    final_decisions = det_result.decisions
    final_accepted = det_result.accepted_findings
    final_rejected = det_result.rejected_findings
    final_borderline = det_result.borderline_findings
    llm_gate_result = None

    # Optional LLM gate
    if llm_gate:
        t1 = time.monotonic()
        gate = run_llm_gate(
            det_result.decisions,
            findings_by_id,
            provider=llm_provider,
            prompt_path=prompt_path,
            max_candidates=max_candidates,
        )
        llm_ms = int((time.monotonic() - t1) * 1000)
        final_decisions, final_accepted, final_rejected, final_borderline = merge_llm_decisions(
            det_result.decisions, gate.decisions, findings_by_id,
        )
        llm_gate_result = gate
    else:
        llm_ms = 0

    # Save artifacts
    slug = _project_slug(project_dir)
    out_dir = output_base / slug
    out_dir.mkdir(parents=True, exist_ok=True)

    def _dts(d) -> dict:
        return {
            "finding_id": d.finding_id,
            "decision": d.decision,
            "usefulness_score": d.usefulness_score,
            "reject_reason": d.reject_reason,
            "reject_explanation": d.reject_explanation,
            "merged_into": d.merged_into,
            "impact_area": d.impact_area,
            "severity": d.severity,
            "has_evidence": d.has_evidence,
            "evidence_quality": d.evidence_quality,
        }

    def _w(fname, data):
        (out_dir / fname).write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    _w("critic_v2_decisions.json", [_dts(d) for d in det_result.decisions])
    _w("critic_v2_metrics.json", {
        "total_input": det_result.metrics.total_input,
        "accepted": det_result.metrics.accepted,
        "borderline": det_result.metrics.borderline,
        "merged": det_result.metrics.merged,
        "rejected_by_rules": det_result.metrics.rejected_by_rules,
        "rejected_by_score": det_result.metrics.rejected_by_score,
        "rejection_reasons": det_result.metrics.rejection_reasons,
        "average_usefulness_score": det_result.metrics.average_usefulness_score,
        "blocks_index_used": blocks_index is not None,
        "blocks_count": len(blocks_index) if blocks_index else 0,
    })

    if llm_gate_result:
        _w("critic_v2_llm_decisions.json", [
            {
                "finding_id": d.finding_id,
                "llm_decision": d.llm_decision,
                "usefulness_score": d.usefulness_score,
                "reject_reason": d.reject_reason,
                "explanation": d.explanation,
                "rewritten_title": d.rewritten_title,
                "provider": d.provider,
            }
            for d in llm_gate_result.decisions
        ])
        _w("critic_v2_final_decisions.json", [_dts(d) for d in final_decisions])

    _w("critic_v2_accepted.json", final_accepted)
    _w("critic_v2_rejected.json", final_rejected)
    _w("critic_v2_borderline.json", final_borderline)

    # Legacy comparison
    comparison = None
    if compare_legacy:
        legacy_review = load_legacy_review(project_dir)
        if legacy_review:
            comparison = compare_with_legacy(final_decisions, legacy_review)
            _w("critic_v2_legacy_comparison.json", comparison)

    m = det_result.metrics
    ev_breakdown = _evidence_breakdown(det_result.decisions)

    return {
        "project": project_dir.name,
        "project_path": str(project_dir),
        "section": _detect_section(project_dir),
        "total_findings": m.total_input,
        "accepted": sum(1 for d in final_decisions if d.decision == "accept"),
        "borderline": sum(1 for d in final_decisions if d.decision in ("borderline", "low_priority")),
        "rejected": sum(1 for d in final_decisions if d.decision == "reject"),
        "merged": m.merged,
        "rejected_by_rules": m.rejected_by_rules,
        "rejected_by_score": m.rejected_by_score,
        "rejection_reasons": m.rejection_reasons,
        "average_usefulness_score": m.average_usefulness_score,
        "evidence_breakdown": ev_breakdown,
        "blocks_index_used": blocks_index is not None,
        "llm_gate_used": llm_gate,
        "llm_provider": llm_provider if llm_gate else None,
        "llm_candidates_sent": llm_gate_result.candidates_sent if llm_gate_result else 0,
        "det_ms": det_ms,
        "llm_ms": llm_ms,
        "output_dir": str(out_dir),
        "comparison": comparison,
        "skipped": False,
        "error": None,
    }


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _project_slug(project_dir: Path) -> str:
    """Create a filesystem-safe slug from project path."""
    parts = []
    for part in project_dir.parts:
        if "projects" in part.lower():
            continue
        clean = part.replace(" ", "_").replace('"', "").replace("'", "")
        clean = "".join(c for c in clean if c.isalnum() or c in "-_.")
        if clean:
            parts.append(clean)
    return "__".join(parts[-3:]) if len(parts) >= 3 else "__".join(parts)


def _detect_section(project_dir: Path) -> str:
    """Detect discipline section from project_info.json or directory hierarchy."""
    info_file = project_dir / "project_info.json"
    if info_file.exists():
        try:
            info = json.loads(info_file.read_text(encoding="utf-8"))
            section = info.get("section", "")
            if section:
                return str(section).upper()
        except (json.JSONDecodeError, OSError):
            pass
    # Fallback: scan parent dirs
    for part in reversed(project_dir.parts):
        if 2 <= len(part) <= 5 and part.isalpha():
            return part.upper()
    return "unknown"


def _evidence_breakdown(decisions) -> dict:
    counts = {EVIDENCE_NONE: 0, EVIDENCE_WEAK: 0, EVIDENCE_PARTIAL: 0, EVIDENCE_VALID: 0}
    for d in decisions:
        eq = d.evidence_quality
        if eq in counts:
            counts[eq] += 1
    return counts


# ─── Summary report ───────────────────────────────────────────────────────────

def build_summary(results: list[dict], output_base: Path, compare_legacy: bool) -> dict:
    """Build cross-project summary and write report files."""
    ok = [r for r in results if not r.get("skipped")]
    skipped = [r for r in results if r.get("skipped")]

    total_findings = sum(r["total_findings"] for r in ok)
    total_accepted = sum(r["accepted"] for r in ok)
    total_borderline = sum(r["borderline"] for r in ok)
    total_rejected = sum(r["rejected"] for r in ok)
    total_merged = sum(r["merged"] for r in ok)

    # Aggregate rejection reasons
    all_reasons: dict[str, int] = {}
    for r in ok:
        for reason, count in r.get("rejection_reasons", {}).items():
            all_reasons[reason] = all_reasons.get(reason, 0) + count

    # Evidence breakdown totals
    ev_totals: dict[str, int] = {
        EVIDENCE_NONE: 0, EVIDENCE_WEAK: 0, EVIDENCE_PARTIAL: 0, EVIDENCE_VALID: 0
    }
    for r in ok:
        for k, v in r.get("evidence_breakdown", {}).items():
            ev_totals[k] = ev_totals.get(k, 0) + v

    # Legacy comparison aggregate
    compare_agg = None
    if compare_legacy:
        compared_projects = [r for r in ok if r.get("comparison")]
        if compared_projects:
            compare_agg = {
                "projects_with_legacy": len(compared_projects),
                "total_compared": sum(r["comparison"]["total_compared"] for r in compared_projects),
                "agreement": sum(r["comparison"]["agreement"] for r in compared_projects),
                "disagree_v2_stricter": sum(r["comparison"]["disagree_v2_stricter"] for r in compared_projects),
                "disagree_v2_looser": sum(r["comparison"]["disagree_v2_looser"] for r in compared_projects),
            }
            n = compare_agg["total_compared"]
            compare_agg["agreement_rate"] = round(compare_agg["agreement"] / n, 3) if n else 0.0

    # Per-section breakdown
    sections: dict[str, dict] = {}
    for r in ok:
        sec = r.get("section", "unknown")
        if sec not in sections:
            sections[sec] = {"projects": 0, "findings": 0, "accepted": 0, "rejected": 0}
        sections[sec]["projects"] += 1
        sections[sec]["findings"] += r["total_findings"]
        sections[sec]["accepted"] += r["accepted"]
        sections[sec]["rejected"] += r["rejected"]

    llm_providers = list({r.get("llm_provider", "mock") for r in ok if r.get("llm_gate_used")})
    summary = {
        "run_config": {
            "projects_processed": len(ok),
            "projects_skipped": len(skipped),
            "llm_gate_used": any(r.get("llm_gate_used") for r in ok),
            "llm_provider": llm_providers[0] if llm_providers else None,
            "blocks_index_used": any(r.get("blocks_index_used") for r in ok),
        },
        "totals": {
            "total_findings": total_findings,
            "accepted": total_accepted,
            "borderline": total_borderline,
            "rejected": total_rejected,
            "merged": total_merged,
            "accept_rate": round(total_accepted / total_findings, 3) if total_findings else 0.0,
            "reject_rate": round(total_rejected / total_findings, 3) if total_findings else 0.0,
        },
        "evidence_breakdown": ev_totals,
        "rejection_reasons": dict(sorted(all_reasons.items(), key=lambda x: -x[1])),
        "by_section": sections,
        "legacy_comparison": compare_agg,
        "per_project": [
            {
                "project": r["project"],
                "section": r.get("section", "?"),
                "findings": r["total_findings"],
                "accepted": r["accepted"],
                "borderline": r["borderline"],
                "rejected": r["rejected"],
                "merged": r["merged"],
                "avg_score": r["average_usefulness_score"],
                "ev_valid": r.get("evidence_breakdown", {}).get(EVIDENCE_VALID, 0),
                "ev_weak": r.get("evidence_breakdown", {}).get(EVIDENCE_WEAK, 0),
            }
            for r in ok
        ],
        "skipped": [{"project": r["project"], "error": r.get("error")} for r in skipped],
    }
    return summary


# ─── Console printer ──────────────────────────────────────────────────────────

def print_summary(summary: dict, compare_legacy: bool) -> None:
    totals = summary["totals"]
    cfg = summary["run_config"]
    print()
    print("=" * 72)
    print("CRITIC V2 BATCH RUN — SUMMARY")
    print("=" * 72)
    print(f"  Projects processed : {cfg['projects_processed']}"
          f"  (skipped: {cfg['projects_skipped']})")
    print(f"  LLM gate           : {'yes (' + str(cfg.get('llm_provider', 'mock')) + ')' if cfg['llm_gate_used'] else 'no'}")
    print(f"  Blocks index       : {'yes' if cfg['blocks_index_used'] else 'no'}")
    print()
    print(f"  ── Totals ─────────────────────────────────────────────────────")
    print(f"  Total findings     : {totals['total_findings']}")
    print(f"  Accepted           : {totals['accepted']}  ({totals['accept_rate']*100:.1f}%)")
    print(f"  Borderline         : {totals['borderline']}")
    print(f"  Rejected           : {totals['rejected']}  ({totals['reject_rate']*100:.1f}%)")
    print(f"  Merged (dupes)     : {totals['merged']}")
    print()

    ev = summary.get("evidence_breakdown", {})
    print(f"  ── Evidence Quality ───────────────────────────────────────────")
    print(f"  valid   : {ev.get(EVIDENCE_VALID, 0)}")
    print(f"  partial : {ev.get(EVIDENCE_PARTIAL, 0)}")
    print(f"  weak    : {ev.get(EVIDENCE_WEAK, 0)}")
    print(f"  none    : {ev.get(EVIDENCE_NONE, 0)}")
    print()

    reasons = summary.get("rejection_reasons", {})
    if reasons:
        print(f"  ── Rejection Reasons ──────────────────────────────────────────")
        for reason, count in list(reasons.items())[:8]:
            print(f"  {reason:<35}: {count}")
        print()

    sections = summary.get("by_section", {})
    if sections:
        print(f"  ── By Section ─────────────────────────────────────────────────")
        print(f"  {'Section':<12} {'Projects':>8} {'Findings':>9} {'Accept':>7} {'Reject':>7}")
        print(f"  {'-'*12} {'-'*8} {'-'*9} {'-'*7} {'-'*7}")
        for sec, s in sorted(sections.items()):
            acc_rate = f"{s['accepted']/s['findings']*100:.0f}%" if s["findings"] else "n/a"
            print(f"  {sec:<12} {s['projects']:>8} {s['findings']:>9} "
                  f"{s['accepted']:>5} {acc_rate:>2}  {s['rejected']:>7}")
        print()

    if compare_legacy and summary.get("legacy_comparison"):
        cmp = summary["legacy_comparison"]
        print(f"  ── Legacy Critic Comparison ───────────────────────────────────")
        print(f"  Projects compared    : {cmp['projects_with_legacy']}")
        print(f"  Total pairs          : {cmp['total_compared']}")
        print(f"  Agreement            : {cmp['agreement']} ({cmp['agreement_rate']*100:.1f}%)")
        print(f"  v2 stricter (was pass, now reject/border) : {cmp['disagree_v2_stricter']}")
        print(f"  v2 looser   (was problem, now accept)     : {cmp['disagree_v2_looser']}")
        print()

    # Per-project table
    per = summary.get("per_project", [])
    if per:
        print(f"  ── Per Project ────────────────────────────────────────────────")
        print(f"  {'Project':<35} {'N':>4} {'Acc':>4} {'Brd':>4} {'Rej':>4} {'Score':>6}")
        print(f"  {'-'*35} {'-'*4} {'-'*4} {'-'*4} {'-'*4} {'-'*6}")
        for r in per:
            name = r["project"][:34]
            print(f"  {name:<35} {r['findings']:>4} {r['accepted']:>4} "
                  f"{r['borderline']:>4} {r['rejected']:>4} {r['avg_score']:>6.2f}")
        print()

    print("=" * 72)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Batch critic v2 runner across multiple projects (offline, no LLM by default).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 6 EOM projects, deterministic only
  python %(prog)s --section EOM --limit 6

  # EOM projects with blocks index and mock LLM gate
  python %(prog)s --section EOM --limit 6 --with-blocks --llm-gate --llm-provider mock

  # AR projects, compare with legacy critic
  python %(prog)s --section AR --limit 5 --compare-legacy

  # All 109 projects (may take a minute)
  python %(prog)s --all --output-dir /tmp/critic_v2_all

  # Specific projects by glob pattern
  python %(prog)s --projects "133_23-ГК-ЭМ" "133_23-ГК-ЭО" --limit 4
""",
    )
    parser.add_argument(
        "--section", default=None,
        help="Filter by discipline section (EOM, AR, AI, GP, OV, PT, SS, TX, ...).",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max number of projects to process.",
    )
    parser.add_argument(
        "--projects", nargs="+", default=None,
        help="Regex patterns to filter project paths (substring match).",
    )
    parser.add_argument(
        "--all", dest="all_projects", action="store_true",
        help="Process all projects (ignores --limit).",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--with-blocks", action="store_true",
        help="Load 02_blocks_analysis.json per project for phantom block detection.",
    )
    parser.add_argument(
        "--llm-gate", action="store_true",
        help="Run optional LLM gate after deterministic critic.",
    )
    parser.add_argument(
        "--llm-provider", default="mock",
        choices=["mock", "noop"],
        help="LLM provider (default: mock). mock=deterministic passthrough.",
    )
    parser.add_argument(
        "--prompt-path", type=Path, default=None,
        help="Custom critic prompt path.",
    )
    parser.add_argument(
        "--max-candidates", type=int, default=50,
        help="Max candidates per project sent to LLM gate (default: 50).",
    )
    parser.add_argument(
        "--compare-legacy", action="store_true",
        help="Compare v2 decisions with legacy 03_findings_review.json per project.",
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="Suppress per-project progress output.",
    )
    args = parser.parse_args()

    # Discover projects
    limit = None if args.all_projects else args.limit
    projects = discover_projects(
        section=args.section,
        limit=limit,
        project_patterns=args.projects,
        all_projects=args.all_projects,
    )

    if not projects:
        print("ERROR: No projects found matching criteria.", file=sys.stderr)
        print(f"  Searched in: {PROJECTS_ROOT}", file=sys.stderr)
        if args.section:
            print(f"  Section filter: {args.section}", file=sys.stderr)
        return 1

    args.output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nBatch critic v2 run")
    print(f"  Projects found : {len(projects)}")
    print(f"  LLM gate       : {'yes (' + args.llm_provider + ')' if args.llm_gate else 'no'}")
    print(f"  Blocks index   : {'yes' if args.with_blocks else 'no'}")
    print(f"  Compare legacy : {'yes' if args.compare_legacy else 'no'}")
    print(f"  Output dir     : {args.output_dir}")
    print()

    results = []
    t_batch_start = time.monotonic()

    for i, project_dir in enumerate(projects, 1):
        if not args.quiet:
            print(f"  [{i:03d}/{len(projects):03d}] {project_dir.name} ...", end=" ", flush=True)
        t0 = time.monotonic()

        result = run_one_project(
            project_dir=project_dir,
            output_base=args.output_dir,
            with_blocks=args.with_blocks,
            llm_gate=args.llm_gate,
            llm_provider=args.llm_provider,
            prompt_path=args.prompt_path,
            max_candidates=args.max_candidates,
            compare_legacy=args.compare_legacy,
        )
        results.append(result)

        if not args.quiet:
            elapsed = int((time.monotonic() - t0) * 1000)
            if result.get("skipped"):
                print(f"SKIP ({result.get('error', '')})")
            else:
                acc = result["accepted"]
                brd = result["borderline"]
                rej = result["rejected"]
                total = result["total_findings"]
                print(f"OK  {total}F acc={acc} brd={brd} rej={rej}  [{elapsed}ms]")

    batch_ms = int((time.monotonic() - t_batch_start) * 1000)

    # Build and save summary
    summary = build_summary(results, args.output_dir, args.compare_legacy)
    summary_path = args.output_dir / "batch_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    # Save per-project results list
    results_path = args.output_dir / "batch_results.json"
    results_path.write_text(
        json.dumps(
            [r for r in results if not r.get("skipped")],
            ensure_ascii=False, indent=2,
        ),
        encoding="utf-8",
    )

    print_summary(summary, args.compare_legacy)
    print(f"  Total batch time: {batch_ms}ms ({batch_ms//1000}s)")
    print(f"  Output: {args.output_dir}")
    print(f"    batch_summary.json")
    print(f"    batch_results.json")
    print(f"    <project_slug>/ per project artifacts")
    print()
    print("  NOTE: Production pipeline not modified.")
    print("        No production artifacts changed.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
