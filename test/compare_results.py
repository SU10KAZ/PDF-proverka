#!/usr/bin/env python3
"""
Сравнение результатов анализа: baseline vs current.

Использование:
    python test/compare_results.py                    # сравнить все проекты
    python test/compare_results.py АР_133-23-ГК-АР1   # один проект

Метрики:
    1. Количество замечаний (total, по severity)
    2. Наличие evidence[] и related_block_ids[] (новые поля)
    3. norm_confidence: средний, медианный, % < 0.8
    4. Количество проверенных норм, % outdated/replaced
    5. Время выполнения этапов (из pipeline_log.json)
"""
import json
import sys
from pathlib import Path
from datetime import datetime


BASE_DIR = Path(__file__).parent.parent
BASELINE_DIR = Path(__file__).parent / "baseline"
PROJECTS_DIR = BASE_DIR / "projects"

# Маппинг safe_name → project_path
PROJECT_MAP = {
    "АР_133-23-ГК-АР1": "АР/133-23-ГК-АР1",
    "OV_133-23-ГК-ОВ2.2_(7)": "OV/133-23-ГК-ОВ2.2 (7)",
    "АИ_133-23-ГК-АИ1_18.02.2026": "АИ/133-23-ГК-АИ1 18.02.2026",
    "ТХ_133_23-ГК-ТХ.О_(2)": "ТХ/133_23-ГК-ТХ.О (2)",
    "АР_133_23-ГК-АР3_изм.8": "АР/133_23-ГК-АР3_изм.8",
}


def load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def extract_findings_metrics(data: dict) -> dict:
    """Извлечь метрики из 03_findings.json."""
    findings = data.get("findings", [])
    total = len(findings)

    by_severity = {}
    confidences = []
    has_evidence = 0
    has_related = 0
    has_norm_quote = 0

    for f in findings:
        sev = f.get("severity", "НЕИЗВЕСТНО")
        by_severity[sev] = by_severity.get(sev, 0) + 1

        conf = f.get("norm_confidence")
        if conf is not None:
            confidences.append(conf)

        if f.get("evidence") and isinstance(f["evidence"], list) and len(f["evidence"]) > 0:
            has_evidence += 1
        if f.get("related_block_ids") and isinstance(f["related_block_ids"], list) and len(f["related_block_ids"]) > 0:
            has_related += 1
        if f.get("norm_quote"):
            has_norm_quote += 1

    avg_conf = sum(confidences) / len(confidences) if confidences else 0
    med_conf = sorted(confidences)[len(confidences) // 2] if confidences else 0
    low_conf = sum(1 for c in confidences if c < 0.8)

    return {
        "total_findings": total,
        "by_severity": by_severity,
        "has_evidence": has_evidence,
        "has_related_block_ids": has_related,
        "has_norm_quote": has_norm_quote,
        "avg_norm_confidence": round(avg_conf, 3),
        "median_norm_confidence": round(med_conf, 3),
        "low_confidence_count": low_conf,
        "pct_with_evidence": round(has_evidence / total * 100, 1) if total else 0,
    }


def extract_norm_metrics(data: dict) -> dict:
    """Извлечь метрики из norm_checks.json."""
    checks = data.get("checks", [])
    meta = data.get("meta", {})
    results = meta.get("results", {})
    paragraph_checks = data.get("paragraph_checks", [])

    deterministic_count = sum(
        1 for c in checks if c.get("verified_via") == "deterministic"
    )
    websearch_count = sum(
        1 for c in checks
        if c.get("verified_via") in ("websearch", "cache+websearch")
    )

    return {
        "total_norms": len(checks),
        "results": results,
        "deterministic": deterministic_count,
        "websearch": websearch_count,
        "paragraph_checks": len(paragraph_checks),
        "paragraphs_verified": sum(
            1 for p in paragraph_checks if p.get("paragraph_verified")
        ),
        "needs_revision": sum(1 for c in checks if c.get("needs_revision")),
    }


def extract_pipeline_metrics(data: dict) -> dict:
    """Извлечь метрики из pipeline_log.json."""
    stages = data.get("stages", {})
    metrics = {}
    for stage_name, stage_data in stages.items():
        if isinstance(stage_data, dict):
            duration = stage_data.get("duration_sec")
            status = stage_data.get("status")
            metrics[stage_name] = {
                "duration_sec": duration,
                "status": status,
            }
    total_sec = data.get("total_duration_sec")
    if total_sec:
        metrics["_total"] = total_sec
    return metrics


def compare_project(safe_name: str, verbose: bool = True) -> dict:
    """Сравнить baseline с текущими результатами одного проекта."""
    proj_path = PROJECT_MAP.get(safe_name)
    if not proj_path:
        print(f"  [!] Неизвестный проект: {safe_name}")
        return {}

    baseline_dir = BASELINE_DIR / safe_name
    current_dir = PROJECTS_DIR / proj_path / "_output"

    result = {"project": safe_name, "project_path": proj_path}

    # --- Findings ---
    for label, dirpath in [("baseline", baseline_dir), ("current", current_dir)]:
        findings_data = load_json(dirpath / "03_findings.json")
        if findings_data:
            result[f"{label}_findings"] = extract_findings_metrics(findings_data)
        else:
            result[f"{label}_findings"] = None

    # --- Norms ---
    for label, dirpath in [("baseline", baseline_dir), ("current", current_dir)]:
        norms_data = load_json(dirpath / "norm_checks.json")
        if norms_data:
            result[f"{label}_norms"] = extract_norm_metrics(norms_data)
        else:
            result[f"{label}_norms"] = None

    # --- Pipeline ---
    for label, dirpath in [("baseline", baseline_dir), ("current", current_dir)]:
        pipeline_data = load_json(dirpath / "pipeline_log.json")
        if pipeline_data:
            result[f"{label}_pipeline"] = extract_pipeline_metrics(pipeline_data)
        else:
            result[f"{label}_pipeline"] = None

    # --- Новые файлы (только current) ---
    result["has_document_graph"] = (current_dir / "document_graph.json").exists()
    result["has_findings_review"] = (current_dir / "03_findings_review.json").exists()

    if verbose:
        _print_comparison(result)

    return result


def _print_comparison(r: dict):
    """Красиво вывести сравнение."""
    print(f"\n{'='*70}")
    print(f"  {r['project']}  ({r['project_path']})")
    print(f"{'='*70}")

    bf = r.get("baseline_findings")
    cf = r.get("current_findings")

    if bf and cf:
        print(f"\n  Замечания:")
        print(f"    {'Метрика':<35} {'Baseline':>10} {'Current':>10} {'Diff':>8}")
        print(f"    {'-'*63}")
        print(f"    {'Всего':<35} {bf['total_findings']:>10} {cf['total_findings']:>10} {cf['total_findings'] - bf['total_findings']:>+8}")

        all_sevs = sorted(set(list(bf['by_severity'].keys()) + list(cf['by_severity'].keys())))
        for sev in all_sevs:
            b = bf['by_severity'].get(sev, 0)
            c = cf['by_severity'].get(sev, 0)
            print(f"    {sev:<35} {b:>10} {c:>10} {c - b:>+8}")

        print(f"    {'% с evidence[]':<35} {bf['pct_with_evidence']:>9}% {cf['pct_with_evidence']:>9}% {cf['pct_with_evidence'] - bf['pct_with_evidence']:>+7.1f}%")
        print(f"    {'has_related_block_ids':<35} {bf['has_related_block_ids']:>10} {cf['has_related_block_ids']:>10} {cf['has_related_block_ids'] - bf['has_related_block_ids']:>+8}")
        print(f"    {'Средний norm_confidence':<35} {bf['avg_norm_confidence']:>10.3f} {cf['avg_norm_confidence']:>10.3f} {cf['avg_norm_confidence'] - bf['avg_norm_confidence']:>+8.3f}")
        print(f"    {'Low confidence (<0.8)':<35} {bf['low_confidence_count']:>10} {cf['low_confidence_count']:>10} {cf['low_confidence_count'] - bf['low_confidence_count']:>+8}")
    elif bf:
        print(f"\n  Замечания: baseline есть ({bf['total_findings']}), current НЕТ")
    elif cf:
        print(f"\n  Замечания: baseline НЕТ, current есть ({cf['total_findings']})")

    bn = r.get("baseline_norms")
    cn = r.get("current_norms")
    if bn and cn:
        print(f"\n  Верификация норм:")
        print(f"    {'Всего норм':<35} {bn['total_norms']:>10} {cn['total_norms']:>10}")
        print(f"    {'Детерминированные':<35} {bn.get('deterministic', 0):>10} {cn.get('deterministic', 0):>10}")
        print(f"    {'WebSearch':<35} {bn.get('websearch', 0):>10} {cn.get('websearch', 0):>10}")
        print(f"    {'Paragraph checks':<35} {bn['paragraph_checks']:>10} {cn['paragraph_checks']:>10}")
        print(f"    {'Требуют пересмотра':<35} {bn['needs_revision']:>10} {cn['needs_revision']:>10}")

    print(f"\n  Новые компоненты:")
    print(f"    document_graph.json:       {'YES' if r.get('has_document_graph') else 'NO'}")
    print(f"    03_findings_review.json:   {'YES' if r.get('has_findings_review') else 'NO'}")


def main():
    filter_name = sys.argv[1] if len(sys.argv) > 1 else None

    projects = list(PROJECT_MAP.keys())
    if filter_name:
        projects = [p for p in projects if filter_name in p]
        if not projects:
            print(f"Проект '{filter_name}' не найден в baseline")
            return

    all_results = []
    for safe_name in projects:
        result = compare_project(safe_name)
        all_results.append(result)

    # Сводка
    if len(all_results) > 1:
        print(f"\n{'='*70}")
        print(f"  СВОДКА ({len(all_results)} проектов)")
        print(f"{'='*70}")
        total_b = sum(r.get("baseline_findings", {}).get("total_findings", 0) for r in all_results)
        total_c = sum(r.get("current_findings", {}).get("total_findings", 0) for r in all_results)
        print(f"  Всего замечаний: baseline={total_b}, current={total_c}, Δ={total_c - total_b:+d}")

        ev_b = sum(r.get("baseline_findings", {}).get("has_evidence", 0) for r in all_results)
        ev_c = sum(r.get("current_findings", {}).get("has_evidence", 0) for r in all_results)
        print(f"  С evidence[]:   baseline={ev_b}, current={ev_c}, Δ={ev_c - ev_b:+d}")

    # Сохранить результат
    output_path = Path(__file__).parent / "comparison_result.json"
    output_path.write_text(
        json.dumps(all_results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\n  Результат сохранён: {output_path}")


if __name__ == "__main__":
    main()
