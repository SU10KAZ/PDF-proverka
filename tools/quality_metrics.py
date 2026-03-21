#!/usr/bin/env python3
"""Метрики качества аудита — компактный summary для baseline/current сравнения.

Использование:
    python tools/quality_metrics.py test/baseline/АР_133-23-ГК-АР1
    python tools/quality_metrics.py projects/EM/133_23-ГК-ЭМ1/_output
    python tools/quality_metrics.py --all-baselines

Выход:
    JSON-файл metrics_summary.json + текстовое summary в stdout.
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def compute_metrics(output_dir: Path) -> dict:
    """Вычислить метрики качества для одного проекта.

    Args:
        output_dir: папка с output-файлами (03_findings.json, norm_checks.json, ...).
                    Может быть _output/ или baseline-папка напрямую.

    Returns:
        dict с метриками.
    """
    metrics = {
        "source": str(output_dir),
        "findings": {},
        "grounding": {},
        "norms": {},
        "pipeline": {},
    }

    # --- Findings ----------------------------------------------
    findings_path = output_dir / "03_findings.json"
    if findings_path.exists():
        fd = json.loads(findings_path.read_text(encoding="utf-8"))
        findings = fd.get("findings", [])
        total = len(findings)

        with_evidence = sum(1 for f in findings if f.get("evidence"))
        with_related = sum(1 for f in findings if f.get("related_block_ids"))
        with_candidates = sum(1 for f in findings if f.get("grounding_candidates"))
        with_norm_quote = sum(1 for f in findings if f.get("norm_quote"))
        low_confidence = sum(1 for f in findings
                            if f.get("norm_confidence") is not None
                            and f["norm_confidence"] < 0.8)
        quality_items = [
            f.get("quality") for f in findings
            if isinstance(f.get("quality"), dict)
        ]
        high_relevance = sum(
            1 for q in quality_items
            if q.get("engineering_relevance") == "high"
        )
        likely_formal_only = sum(
            1 for q in quality_items
            if q.get("likely_formal_only")
        )
        high_severity_formal_only = sum(
            1 for f in findings
            if isinstance(f.get("quality"), dict)
            and f["quality"].get("likely_formal_only")
            and f.get("severity") in ("КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ", "ЭКСПЛУАТАЦИОННОЕ")
        )

        by_severity = {}
        for f in findings:
            s = f.get("severity", "?")
            by_severity[s] = by_severity.get(s, 0) + 1

        metrics["findings"] = {
            "total": total,
            "by_severity": by_severity,
            "evidence_coverage": round(with_evidence / total, 3) if total else 0,
            "related_block_ids_coverage": round(with_related / total, 3) if total else 0,
            "grounding_candidates_coverage": round(with_candidates / total, 3) if total else 0,
            "norm_quote_coverage": round(with_norm_quote / total, 3) if total else 0,
            "low_confidence_count": low_confidence,
            "quality_coverage": round(len(quality_items) / total, 3) if total else 0,
            "high_relevance_count": high_relevance,
            "likely_formal_only_count": likely_formal_only,
            "high_severity_formal_only_count": high_severity_formal_only,
        }

    # --- Selective review -------------------------------------
    review_input = output_dir / "03_findings_review_input.json"
    if review_input.exists():
        rd = json.loads(review_input.read_text(encoding="utf-8"))
        meta = rd.get("meta", {})
        metrics["grounding"]["risky_findings_count"] = meta.get("risky_count", 0)
        metrics["grounding"]["selective_critic_skipped_count"] = meta.get("skipped_count", 0)

    # --- Norm checks ------------------------------------------
    norm_path = output_dir / "norm_checks.json"
    if norm_path.exists():
        nd = json.loads(norm_path.read_text(encoding="utf-8"))
        checks = nd.get("checks", [])
        meta = nd.get("meta", {})

        deterministic = sum(1 for c in checks if c.get("verified_via") == "deterministic")
        websearch = sum(1 for c in checks if c.get("verified_via") == "websearch")
        cache_stale = sum(1 for c in checks if c.get("verified_via") == "cache_stale")
        pending = sum(1 for c in checks if c.get("verified_via") == "pending_websearch")
        needs_revision = sum(1 for c in checks if c.get("needs_revision"))
        policy_violations = len(meta.get("policy_violations", []))

        # Paragraph cache hits/misses
        paragraph_checks = nd.get("paragraph_checks", [])
        paragraph_verified = sum(1 for p in paragraph_checks if p.get("paragraph_verified"))
        paragraph_total = len(paragraph_checks)

        metrics["norms"] = {
            "total_checks": len(checks),
            "deterministic_count": deterministic,
            "websearch_count": websearch,
            "cache_stale_count": cache_stale,
            "pending_websearch_count": pending,
            "needs_revision_count": needs_revision,
            "policy_violations_count": policy_violations,
            "paragraph_cache_total": paragraph_total,
            "paragraph_cache_verified": paragraph_verified,
            "paragraph_cache_misses": paragraph_total - paragraph_verified,
        }

    # --- Pipeline log -----------------------------------------
    pipeline_path = output_dir / "pipeline_log.json"
    if pipeline_path.exists():
        pd = json.loads(pipeline_path.read_text(encoding="utf-8"))
        stages = pd.get("stages", {})
        completed = sum(1 for s in stages.values() if s.get("status") == "done")
        errors = sum(1 for s in stages.values() if s.get("status") in ("error", "interrupted"))
        metrics["pipeline"] = {
            "total_stages": len(stages),
            "completed": completed,
            "errors": errors,
        }

    return metrics


def format_summary(metrics: dict) -> str:
    """Текстовое summary метрик."""
    lines = [f"Metrics: {metrics['source']}", ""]

    f = metrics.get("findings", {})
    if f:
        lines.append(f"Findings: {f.get('total', 0)}")
        lines.append(f"  evidence coverage:     {f.get('evidence_coverage', 0):.1%}")
        lines.append(f"  related_block_ids:     {f.get('related_block_ids_coverage', 0):.1%}")
        lines.append(f"  grounding_candidates:  {f.get('grounding_candidates_coverage', 0):.1%}")
        lines.append(f"  norm_quote coverage:   {f.get('norm_quote_coverage', 0):.1%}")
        lines.append(f"  low confidence:        {f.get('low_confidence_count', 0)}")
        lines.append(f"  quality coverage:      {f.get('quality_coverage', 0):.1%}")
        lines.append(f"  high relevance:        {f.get('high_relevance_count', 0)}")
        lines.append(f"  likely formal only:    {f.get('likely_formal_only_count', 0)}")
        lines.append(f"  high-sev formal only:  {f.get('high_severity_formal_only_count', 0)}")
        lines.append(f"  by severity: {f.get('by_severity', {})}")

    g = metrics.get("grounding", {})
    if g:
        lines.append(f"\nGrounding:")
        lines.append(f"  risky findings:        {g.get('risky_findings_count', '?')}")
        lines.append(f"  critic skipped:        {g.get('selective_critic_skipped_count', '?')}")

    n = metrics.get("norms", {})
    if n:
        lines.append(f"\nNorms: {n.get('total_checks', 0)} checks")
        lines.append(f"  deterministic:         {n.get('deterministic_count', 0)}")
        lines.append(f"  websearch:             {n.get('websearch_count', 0)}")
        lines.append(f"  needs revision:        {n.get('needs_revision_count', 0)}")
        lines.append(f"  policy violations:     {n.get('policy_violations_count', 0)}")
        lines.append(f"  paragraph verified:    {n.get('paragraph_cache_verified', 0)}/{n.get('paragraph_cache_total', 0)}")

    p = metrics.get("pipeline", {})
    if p:
        lines.append(f"\nPipeline: {p.get('completed', 0)}/{p.get('total_stages', 0)} done, {p.get('errors', 0)} errors")

    return "\n".join(lines)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    if sys.argv[1] == "--all-baselines":
        baseline_root = ROOT / "test" / "baseline"
        if not baseline_root.exists():
            print("test/baseline/ не найден")
            sys.exit(1)
        all_metrics = []
        for d in sorted(baseline_root.iterdir()):
            if d.is_dir():
                m = compute_metrics(d)
                all_metrics.append(m)
                print(format_summary(m))
                print("-" * 40)

        out = baseline_root / "metrics_summary.json"
        out.write_text(json.dumps(all_metrics, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\nСохранено: {out}")
    else:
        target = Path(sys.argv[1])
        if not target.exists():
            print(f"Путь не найден: {target}")
            sys.exit(1)
        m = compute_metrics(target)
        print(format_summary(m))

        out = target / "metrics_summary.json"
        out.write_text(json.dumps(m, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\nСохранено: {out}")


if __name__ == "__main__":
    main()
