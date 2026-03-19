#!/usr/bin/env python3
"""
regression_audit.py — полный regression + acceptance аудит пайплайна.

Прогоняет все проекты через новые классификаторы и генерирует:
- baseline_metrics.json
- regression_report
- sampled skips audit
- cost/stability analysis

Использование:
    python tools/regression_audit.py
"""
import json
import glob
import sys
import os
import random
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from webapp.services.grounding_service import classify_grounding_level
from norm_contract import (
    classify_norm_status, classify_norm_quote_status,
    compute_norm_confidence, enrich_findings_from_norm_checks,
    should_review_norm, NORM_CONFIDENCE_THRESHOLDS,
)


def load_project_findings(fp):
    """Load findings + optional norm_checks for one project."""
    with open(fp, "r", encoding="utf-8") as f:
        data = json.load(f)
    findings = data.get("findings", [])

    # Try enriching from norm_checks
    nc_path = fp.replace("03_findings.json", "norm_checks.json")
    norm_checks = None
    if os.path.exists(nc_path):
        try:
            with open(nc_path, "r", encoding="utf-8") as f:
                norm_checks = json.load(f)
            enrich_findings_from_norm_checks(findings, norm_checks)
        except (json.JSONDecodeError, OSError):
            for finding in findings:
                finding["norm_status"] = classify_norm_status(finding)
    else:
        for finding in findings:
            finding["norm_status"] = classify_norm_status(finding)

    return findings, norm_checks


def compute_project_metrics(findings, project_name):
    """Compute all metrics for one project."""
    m = {
        "project": project_name,
        "total": len(findings),
    }

    # Grounding levels
    levels = Counter()
    for f in findings:
        level = classify_grounding_level(f)
        f["_level"] = level
        levels[level] += 1
    m["grounded_strong"] = levels.get("grounded_strong", 0)
    m["grounded_weak"] = levels.get("grounded_weak", 0)
    m["ungrounded"] = levels.get("ungrounded", 0)

    # Norm status distribution
    norm_statuses = Counter()
    quote_statuses = Counter()
    for f in findings:
        ns = f.get("norm_status") or classify_norm_status(f)
        qs = f.get("norm_quote_status") or classify_norm_quote_status(f)
        norm_statuses[ns] += 1
        quote_statuses[qs] += 1
    m["norm_statuses"] = dict(norm_statuses)
    m["quote_statuses"] = dict(quote_statuses)

    # Review decisions (old vs new)
    old_review = 0
    new_review = 0
    new_skip = 0
    for f in findings:
        level = f["_level"]
        conf = f.get("norm_confidence", 1.0)

        # Old logic
        if level != "grounded_strong" or (conf is not None and conf < 0.8):
            old_review += 1

        # New logic
        is_risky = False
        if level != "grounded_strong":
            is_risky = True
        elif should_review_norm(f):
            is_risky = True

        if is_risky:
            new_review += 1
        else:
            new_skip += 1

    m["old_review"] = old_review
    m["new_review"] = new_review
    m["new_skip"] = new_skip
    m["old_review_rate"] = round(old_review / max(len(findings), 1), 3)
    m["new_review_rate"] = round(new_review / max(len(findings), 1), 3)

    # Severity distribution
    severities = Counter()
    for f in findings:
        severities[f.get("severity", "?")] += 1
    m["severities"] = dict(severities)

    return m


def audit_skipped_findings(all_findings_with_projects):
    """Audit skipped findings for safety."""
    skipped = []
    for proj, f in all_findings_with_projects:
        level = f.get("_level", classify_grounding_level(f))
        if level == "grounded_strong" and not should_review_norm(f):
            skipped.append((proj, f))

    # Sample 50 random + oversample КРИТИЧЕСКОЕ / ЭКОНОМИЧЕСКОЕ
    critical_skipped = [(p, f) for p, f in skipped if f.get("severity") == "КРИТИЧЕСКОЕ"]
    economic_skipped = [(p, f) for p, f in skipped if f.get("severity") == "ЭКОНОМИЧЕСКОЕ"]
    other_skipped = [(p, f) for p, f in skipped
                     if f.get("severity") not in ("КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ")]

    sample = []
    sample.extend(critical_skipped[:20])  # все критические
    sample.extend(economic_skipped[:15])  # все экономические
    remaining = 50 - len(sample)
    if remaining > 0 and other_skipped:
        sample.extend(random.sample(other_skipped, min(remaining, len(other_skipped))))

    # Analyze
    results = []
    unsafe = []
    for proj, f in sample:
        fid = f.get("id", "?")
        sev = f.get("severity", "?")
        norm = (f.get("norm") or "")[:50]
        ns = f.get("norm_status", "?")
        conf = f.get("norm_confidence", "?")
        evidence = f.get("evidence", [])
        related = f.get("related_block_ids", [])
        source = f.get("source_block_ids", [])

        # Safety check
        has_real_evidence = any(
            isinstance(e, dict) and e.get("type") == "image"
            and e.get("source") != "grounding_service"
            for e in evidence
        )

        is_unsafe = False
        reason = ""
        if sev == "КРИТИЧЕСКОЕ" and not has_real_evidence:
            is_unsafe = True
            reason = "CRITICAL without real image evidence"
        elif sev == "КРИТИЧЕСКОЕ" and not related:
            is_unsafe = True
            reason = "CRITICAL without related_block_ids"
        elif not has_real_evidence and not related:
            is_unsafe = True
            reason = "No evidence and no related blocks"

        entry = {
            "project": proj,
            "id": fid,
            "severity": sev,
            "norm_status": ns,
            "norm_confidence": conf,
            "norm": norm,
            "has_real_evidence": has_real_evidence,
            "related_count": len(related),
            "is_unsafe": is_unsafe,
            "reason": reason,
        }
        results.append(entry)
        if is_unsafe:
            unsafe.append(entry)

    return {
        "total_skipped": len(skipped),
        "sampled": len(sample),
        "critical_skipped": len(critical_skipped),
        "economic_skipped": len(economic_skipped),
        "unsafe_count": len(unsafe),
        "unsafe_details": unsafe,
        "sample": results,
    }


def analyze_no_norm_cited(all_findings_with_projects):
    """Analyze no_norm_cited findings by severity and type."""
    no_norm = [(p, f) for p, f in all_findings_with_projects
               if (f.get("norm_status") or classify_norm_status(f)) == "no_norm_cited"]

    by_severity = Counter()
    by_category = Counter()
    for p, f in no_norm:
        by_severity[f.get("severity", "?")] += 1
        by_category[f.get("category", "?")] += 1

    # Which severities should require norm?
    norm_required_analysis = {}
    for sev in sorted(by_severity.keys()):
        count = by_severity[sev]
        if sev in ("КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ"):
            norm_required_analysis[sev] = {
                "count": count,
                "recommendation": "norm_required=true — критические/экономические ДОЛЖНЫ иметь норму",
            }
        elif sev == "ЭКСПЛУАТАЦИОННОЕ":
            norm_required_analysis[sev] = {
                "count": count,
                "recommendation": "norm_recommended — желательна, но не блокирует",
            }
        else:
            norm_required_analysis[sev] = {
                "count": count,
                "recommendation": "norm_optional — допустимо без нормы",
            }

    return {
        "total_no_norm": len(no_norm),
        "by_severity": dict(by_severity),
        "by_category": dict(by_category),
        "norm_required_analysis": norm_required_analysis,
    }


def analyze_norm_checks_costs(projects_root):
    """Analyze norm verification costs."""
    stats = {
        "projects_with_norm_checks": 0,
        "total_checks": 0,
        "from_deterministic": 0,
        "from_websearch": 0,
        "paragraph_checks_total": 0,
        "paragraph_verified_true": 0,
        "paragraph_verified_false": 0,
    }

    for fp in sorted(glob.glob(str(projects_root / "**/_output/norm_checks.json"), recursive=True)):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                nc = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        stats["projects_with_norm_checks"] += 1
        checks = nc.get("checks", [])
        stats["total_checks"] += len(checks)

        for c in checks:
            via = c.get("verified_via", "")
            if via == "deterministic":
                stats["from_deterministic"] += 1
            elif via in ("websearch", "pending_websearch", "cache_stale"):
                stats["from_websearch"] += 1

        for pc in nc.get("paragraph_checks", []):
            stats["paragraph_checks_total"] += 1
            if pc.get("paragraph_verified"):
                stats["paragraph_verified_true"] += 1
            else:
                stats["paragraph_verified_false"] += 1

    return stats


def main():
    projects_root = Path("projects")
    findings_files = sorted(glob.glob("projects/**/_output/03_findings.json", recursive=True))

    all_metrics = []
    all_findings_with_projects = []

    for fp in findings_files:
        findings, nc = load_project_findings(fp)
        if not findings:
            continue
        proj_name = Path(fp).parent.parent.name
        metrics = compute_project_metrics(findings, proj_name)
        all_metrics.append(metrics)
        for f in findings:
            all_findings_with_projects.append((proj_name, f))

    # Aggregate metrics
    total_f = sum(m["total"] for m in all_metrics)
    total_strong = sum(m["grounded_strong"] for m in all_metrics)
    total_weak = sum(m["grounded_weak"] for m in all_metrics)
    total_ung = sum(m["ungrounded"] for m in all_metrics)
    total_old_review = sum(m["old_review"] for m in all_metrics)
    total_new_review = sum(m["new_review"] for m in all_metrics)
    total_new_skip = sum(m["new_skip"] for m in all_metrics)

    agg_norm = Counter()
    agg_quote = Counter()
    agg_sev = Counter()
    for m in all_metrics:
        for k, v in m.get("norm_statuses", {}).items():
            agg_norm[k] += v
        for k, v in m.get("quote_statuses", {}).items():
            agg_quote[k] += v
        for k, v in m.get("severities", {}).items():
            agg_sev[k] += v

    # Safety audit
    skip_audit = audit_skipped_findings(all_findings_with_projects)

    # No-norm analysis
    no_norm_analysis = analyze_no_norm_cited(all_findings_with_projects)

    # Cost analysis
    cost_stats = analyze_norm_checks_costs(projects_root)

    # ─── OUTPUT ───
    print(f"\n{'='*70}")
    print(f"  REGRESSION AUDIT — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*70}")
    print(f"  Projects: {len(all_metrics)}")
    print(f"  Findings: {total_f}")

    print(f"\n  === BEFORE/AFTER COMPARISON ===")
    print(f"  {'Metric':<35} {'Old':>8} {'New':>8} {'Delta':>8}")
    print(f"  {'-'*35} {'-'*8} {'-'*8} {'-'*8}")
    print(f"  {'Review rate':<35} {100*total_old_review/total_f:.0f}%{'':<4} {100*total_new_review/total_f:.0f}%{'':<4} {100*(total_new_review-total_old_review)/total_f:+.0f}%")
    print(f"  {'Review count':<35} {total_old_review:>8} {total_new_review:>8} {total_new_review-total_old_review:>+8}")
    print(f"  {'Skip count':<35} {total_f-total_old_review:>8} {total_new_skip:>8} {total_new_skip-(total_f-total_old_review):>+8}")

    print(f"\n  === GROUNDING LEVELS ===")
    print(f"  grounded_strong: {total_strong:>5} ({100*total_strong/total_f:.0f}%)")
    print(f"  grounded_weak:   {total_weak:>5} ({100*total_weak/total_f:.0f}%)")
    print(f"  ungrounded:      {total_ung:>5} ({100*total_ung/total_f:.0f}%)")

    print(f"\n  === NORM STATUS DISTRIBUTION ===")
    for ns, count in sorted(agg_norm.items(), key=lambda x: -x[1]):
        print(f"  {ns:<30} {count:>5} ({100*count/total_f:.0f}%)")

    print(f"\n  === QUOTE COVERAGE ===")
    for qs, count in sorted(agg_quote.items(), key=lambda x: -x[1]):
        print(f"  {qs:<30} {count:>5} ({100*count/total_f:.0f}%)")

    print(f"\n  === SAFETY AUDIT (skipped findings) ===")
    print(f"  Total skipped:     {skip_audit['total_skipped']}")
    print(f"  Sampled:           {skip_audit['sampled']}")
    print(f"  Critical skipped:  {skip_audit['critical_skipped']}")
    print(f"  Economic skipped:  {skip_audit['economic_skipped']}")
    print(f"  UNSAFE:            {skip_audit['unsafe_count']}")
    if skip_audit["unsafe_details"]:
        for u in skip_audit["unsafe_details"][:5]:
            print(f"    {u['id']} [{u['severity']}] {u['reason']}")

    print(f"\n  === NO_NORM_CITED ANALYSIS ===")
    print(f"  Total no_norm: {no_norm_analysis['total_no_norm']}")
    print(f"  By severity:")
    for sev, count in sorted(no_norm_analysis["by_severity"].items(), key=lambda x: -x[1]):
        rec = no_norm_analysis["norm_required_analysis"].get(sev, {}).get("recommendation", "")
        print(f"    {sev:<25} {count:>5} | {rec}")

    print(f"\n  === COST ANALYSIS ===")
    print(f"  Projects with norm_checks: {cost_stats['projects_with_norm_checks']}")
    print(f"  Total norm checks:         {cost_stats['total_checks']}")
    print(f"  Deterministic:             {cost_stats['from_deterministic']}")
    print(f"  WebSearch needed:          {cost_stats['from_websearch']}")
    det_rate = cost_stats["from_deterministic"] / max(cost_stats["total_checks"], 1)
    print(f"  Deterministic rate:        {100*det_rate:.0f}%")
    print(f"  Paragraph checks:          {cost_stats['paragraph_checks_total']}")
    print(f"    verified=true:           {cost_stats['paragraph_verified_true']}")
    print(f"    verified=false:          {cost_stats['paragraph_verified_false']}")

    print(f"\n  === PRODUCTION DECISION ===")
    unsafe_count = skip_audit["unsafe_count"]
    if unsafe_count == 0 and total_new_review / total_f < 0.5:
        print(f"  VERDICT: GO")
        print(f"  - 0 unsafe skips")
        print(f"  - Review rate {100*total_new_review/total_f:.0f}% (target <50%)")
        print(f"  - Evidence gate: 0 false strong (from Step 8 audit)")
    elif unsafe_count <= 3:
        print(f"  VERDICT: CONDITIONAL GO")
        print(f"  - {unsafe_count} unsafe skips — review manually")
    else:
        print(f"  VERDICT: NO-GO")
        print(f"  - {unsafe_count} unsafe skips — fix before production")

    # Save metrics
    output = {
        "timestamp": datetime.now().isoformat(),
        "projects": len(all_metrics),
        "total_findings": total_f,
        "grounding": {
            "strong": total_strong,
            "weak": total_weak,
            "ungrounded": total_ung,
        },
        "review_rate": {
            "old": round(total_old_review / total_f, 3),
            "new": round(total_new_review / total_f, 3),
        },
        "norm_statuses": dict(agg_norm),
        "quote_statuses": dict(agg_quote),
        "safety": {
            "unsafe_skips": unsafe_count,
            "critical_skipped": skip_audit["critical_skipped"],
        },
        "cost": cost_stats,
        "per_project": all_metrics,
    }

    os.makedirs("audit_step1_post_refactor", exist_ok=True)
    with open("audit_step1_post_refactor/baseline_metrics.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n  Saved: audit_step1_post_refactor/baseline_metrics.json")


if __name__ == "__main__":
    main()
