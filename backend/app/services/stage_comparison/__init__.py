"""Stage Comparison service — сравнение двух стадий проектной документации.

MVP-сервис:
  - сканирует две папки на PDF/MD/result.json,
  - сопоставляет PDF между стадиями по имени (нормализация + fuzzy),
  - нормализует блоки из result.json,
  - сохраняет сессии в backend/app/data/stage_comparison_sessions/<id>.json.

Дополнительно поддерживает ручные/автоматические связи блоков, text-diff
MD-файлов, графическое сравнение через LLM (в gated-режиме, без авто-запуска).
"""
from backend.app.services.stage_comparison import (
    scanner, store, blocks, diff_text, alignment, paths,
    findings, jobs, reports,
    enriched_comparison, unified_analysis, unified_analysis_jobs, unified_findings,
)

__all__ = [
    "scanner", "store", "blocks", "diff_text", "alignment", "paths",
    "findings", "jobs", "reports",
    "enriched_comparison", "unified_analysis", "unified_analysis_jobs", "unified_findings",
]
