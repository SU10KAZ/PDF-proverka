#!/usr/bin/env python3
"""11H §34 — сравнить результат Codex с историческим аудитом ТОГО ЖЕ PDF.

Сравнение честное ровно в той мере, в какой оно вообще возможно: у документов
совпадает `pdf_sha256`, но прогоны шли РАЗНЫМИ конвейерами (исторический — с
локальным обогащением Gemma, которого на платформе больше нет, и с ансамблем
Claude+Codex на оптимизации). Поэтому здесь нет ни «победителя», ни процентов
качества — только сопоставимые счётчики и то, что из них следует.

Дополнительных обращений к модели не делает (§34).
"""
from __future__ import annotations

import argparse
import collections
import json
from pathlib import Path
from typing import Any


def load(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def findings_of(latest: Path) -> list[dict]:
    """Замечания с тем же приоритетом файлов, что и у самой платформы."""
    for name in ("03a_norms_verified.json", "03_findings.json"):
        data = load(latest / name)
        rows = data.get("findings")
        if isinstance(rows, list) and rows:
            return rows
    return []


def profile(latest: Path, label: str) -> dict[str, Any]:
    rows = findings_of(latest)
    optimization = load(latest / "optimization.json")
    items = optimization.get("items")
    norms = load(latest / "norm_checks.json")
    with_norm = sum(1 for r in rows if (r.get("norm") or "").strip())
    with_clause = sum(
        1 for r in rows
        if (r.get("norm_clause") or "").strip()
        or "п." in str(r.get("norm") or "")
    )
    pages = [r.get("page") for r in rows if isinstance(r.get("page"), int)]
    return {
        "label": label,
        "findings": len(rows),
        "severity": dict(collections.Counter(
            str(r.get("severity") or "?") for r in rows)),
        "categories_top": dict(collections.Counter(
            str(r.get("category") or "?") for r in rows).most_common(10)),
        "with_norm": with_norm,
        "with_clause": with_clause,
        "norm_share_pct": round(100 * with_norm / len(rows)) if rows else 0,
        "clause_share_pct": round(100 * with_clause / len(rows)) if rows else 0,
        "pages_covered": len(set(pages)),
        "from_blocks": sum(1 for r in rows if r.get("source_block_ids")),
        "from_text": sum(
            1 for r in rows
            if (r.get("source_finding_ids") and not r.get("source_block_ids"))
        ),
        "optimization_items": (
            len(items) if isinstance(items, list)
            else (items if isinstance(items, int) else None)
        ),
        "norm_checks_present": bool(norms),
        "norm_checks_count": len(norms.get("checks") or []) if norms else 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--new", required=True, help="03_analysis/latest нового аудита")
    parser.add_argument("--historical", default="", help="03_analysis/latest исторического")
    parser.add_argument("--out", default="")
    args = parser.parse_args()

    new = profile(Path(args.new), "11H — Codex на удалённом воркере")
    historical = (
        profile(Path(args.historical), "исторический прогон 29.07.2026")
        if args.historical else None
    )

    report: dict[str, Any] = {
        "stage": "11H",
        "question": "как выглядит результат Codex рядом с историческим по ТОМУ ЖЕ PDF",
        "extra_model_calls": 0,
        "new": new,
        "historical": historical,
        "comparability": {
            "same_pdf_sha256": True,
            "same_pipeline": False,
            "why_not": (
                "исторический прогон шёл с локальным обогащением графики (Gemma, "
                "с платформы удалено) и ансамблем Claude+Codex на оптимизации; "
                "11H — единственный провайдер Codex и детерминированный контекст блоков"
            ),
            "what_the_numbers_mean": (
                "сопоставимы объём (число замечаний), нормативная привязка и покрытие "
                "страниц. НЕ сопоставимы: severity (разные калибровки), категории "
                "(свободный словарь модели)"
            ),
        },
    }
    if historical:
        report["deltas"] = {
            "findings": new["findings"] - historical["findings"],
            "with_norm_pct": new["norm_share_pct"] - historical["norm_share_pct"],
            "with_clause_pct": new["clause_share_pct"] - historical["clause_share_pct"],
            "pages_covered": new["pages_covered"] - historical["pages_covered"],
        }
    print(json.dumps(report, ensure_ascii=False, indent=1)[:2000])
    if args.out:
        Path(args.out).write_text(
            json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
