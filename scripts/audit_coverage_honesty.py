#!/usr/bin/env python3
"""reserc.md #96 (дешёвые метрики) — честность покрытия аудита по всему парку.

Без экспертного эталона. Считает по каждому проекту, насколько ЧЕСТНО аудит
сообщает о непокрытых/исключённых блоках (Stage 02 coverage propagation пишет
`analysis_coverage.summary` в 03_findings.json). Метрики, которые это даёт:

  - projects_total / with_findings / with_coverage_metadata;
  - legacy_no_coverage — есть 03_findings.json, но НЕТ analysis_coverage
    (старые аудиты до coverage-propagation: их покрытие непроверяемо → честность
    под вопросом, кандидаты на пере-аудит);
  - partial / full — сколько проектов с excluded_from_full_analysis_count > 0;
  - суммарные excluded / gemma_uncovered / single_block_failed / crop_missing;
  - worst offenders — топ проектов по числу исключённых блоков.

Парная метрика к data-integrity чекеру (scripts/audit_decisions_log_integrity.py).
Обе — «дешёвые» (без эксперта), пригодны как CI-наблюдаемость и число «до/после».

READ-ONLY. Version-aware: обходит проекты канонически (project_service.
iter_project_dirs — одна primary-версия на контейнер).

    python scripts/audit_coverage_honesty.py
    python scripts/audit_coverage_honesty.py --json out.json
    python scripts/audit_coverage_honesty.py --worst 20
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_SUMMARY_NUMERIC = (
    "excluded_from_full_analysis_count",
    "gemma_uncovered_count",
    "single_block_failed_count",
    "stage02_crop_missing_count",
    "base_only_blocks_count",
    "upgraded_to_300_count",
    "base_gemma_covered_count",
    "base_gemma_total_count",
)


def read_findings_coverage(findings_path: Path) -> dict:
    """Прочитать состояние покрытия одного проекта из 03_findings.json.

    Возвращает {status, summary}. status:
      no_findings | error | no_coverage | full | partial
    """
    if not findings_path.exists():
        return {"status": "no_findings", "summary": {}}
    try:
        data = json.loads(findings_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"status": "error", "summary": {}}
    if not isinstance(data, dict):
        return {"status": "error", "summary": {}}
    cov = data.get("analysis_coverage")
    if not isinstance(cov, dict):
        return {"status": "no_coverage", "summary": {}}
    summary = cov.get("summary") if isinstance(cov.get("summary"), dict) else {}
    excluded = int(summary.get("excluded_from_full_analysis_count") or 0)
    return {"status": "partial" if excluded > 0 else "full", "summary": summary}


def aggregate_coverage(items: list[dict]) -> dict:
    """Свести per-project записи [{project_id, status, summary}] в отчёт парка.

    Чистая функция (тестируемая, без файловой системы).
    """
    totals = {k: 0 for k in _SUMMARY_NUMERIC}
    counts = {
        "projects_total": len(items),
        "with_findings": 0,
        "with_coverage_metadata": 0,
        "legacy_no_coverage": 0,
        "errors": 0,
        "full": 0,
        "partial": 0,
    }
    partials = []
    for it in items:
        st = it.get("status")
        if st == "no_findings":
            continue
        counts["with_findings"] += 1
        if st == "error":
            counts["errors"] += 1
            continue
        if st == "no_coverage":
            counts["legacy_no_coverage"] += 1
            continue
        counts["with_coverage_metadata"] += 1
        summary = it.get("summary") or {}
        for k in _SUMMARY_NUMERIC:
            totals[k] += int(summary.get(k) or 0)
        if st == "partial":
            counts["partial"] += 1
            partials.append({
                "project_id": it.get("project_id"),
                "excluded": int(summary.get("excluded_from_full_analysis_count") or 0),
                "gemma_uncovered": int(summary.get("gemma_uncovered_count") or 0),
                "single_block_failed": int(summary.get("single_block_failed_count") or 0),
            })
        elif st == "full":
            counts["full"] += 1

    partials.sort(key=lambda x: x["excluded"], reverse=True)
    cov_meta = counts["with_coverage_metadata"]
    honesty = round(cov_meta / counts["with_findings"], 3) if counts["with_findings"] else None
    return {
        "counts": counts,
        "coverage_metadata_ratio": honesty,  # доля аудитов с проверяемым покрытием
        "totals": totals,
        "worst_offenders": partials,
    }


def build_fleet_report(worst: int = 15) -> dict:
    from backend.app.services.common import project_service

    items = []
    for pid, path in project_service.iter_project_dirs():
        cov = read_findings_coverage(Path(path) / "_output" / "03_findings.json")
        items.append({"project_id": pid, **cov})
    report = aggregate_coverage(items)
    report["worst_offenders"] = report["worst_offenders"][:worst]
    return report


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Честность покрытия аудита (reserc.md #96)")
    ap.add_argument("--json", metavar="PATH", help="записать полный отчёт в JSON")
    ap.add_argument("--worst", type=int, default=15, help="сколько worst offenders показать")
    args = ap.parse_args(argv)

    report = build_fleet_report(worst=args.worst)
    c = report["counts"]

    if args.json:
        Path(args.json).write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"Отчёт записан: {args.json}")

    print("=== Честность покрытия аудита (reserc.md #96) ===")
    print(f"  проектов всего:            {c['projects_total']}")
    print(f"  с аудитом (03_findings):   {c['with_findings']}")
    print(f"  с coverage-метаданными:    {c['with_coverage_metadata']}")
    print(f"  legacy без coverage:       {c['legacy_no_coverage']}  (покрытие непроверяемо → кандидаты на пере-аудит)")
    print(f"  битых 03_findings.json:    {c['errors']}")
    print(f"  full / partial:            {c['full']} / {c['partial']}")
    print(f"  доля проверяемого покрытия: {report['coverage_metadata_ratio']}")
    t = report["totals"]
    print("  --- суммарно блоков ---")
    print(f"  исключено из полного анализа: {t['excluded_from_full_analysis_count']}")
    print(f"  gemma непокрыто:             {t['gemma_uncovered_count']}")
    print(f"  single-block провалов:       {t['single_block_failed_count']}")
    print(f"  stage02 crop пропущено:      {t['stage02_crop_missing_count']}")
    if report["worst_offenders"]:
        print("  --- worst offenders (по excluded) ---")
        for p in report["worst_offenders"]:
            print(f"    {p['project_id']}: excluded={p['excluded']} "
                  f"(gemma_uncov={p['gemma_uncovered']}, sb_fail={p['single_block_failed']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
