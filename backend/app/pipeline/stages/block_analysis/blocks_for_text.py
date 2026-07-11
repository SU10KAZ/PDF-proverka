"""Компактный view анализа блоков (Stage 02) для текстового этапа (Stage 01).

Когда конвейер идёт в порядке block→text (флаг PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED),
текстовый этап (Opus) читает результат блоков и сверяет с ним свои T-замечания. Подавать
в Opus сырой 01_blocks_analysis.json нельзя — он большой (многоабзацные findings), это лишние
токены/внимание/стоимость. Здесь собирается компактная проекция ровно из тех полей, что нужны
для верификации: block_id/page/sheet/coverage_status + findings (id/severity/category/finding/
value_found/block_evidence/highlight_regions).

Компактизация:
- включаются только блоки с непустыми findings ИЛИ с не-ok coverage_status (остальные — «чистые»
  без замечаний — для сверки бесполезны, их число фиксируется в meta.blocks_omitted_clean);
- длинные строки усечены (finding/value_found/block_evidence);
- всякое усечение отражается в meta (без «тихого» обрезания).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

from backend.app.services.storage.stage_artifacts import (
    BLOCKS_ANALYSIS_FILENAME,
    BLOCKS_FOR_TEXT_FILENAME,
    BLOCKS_FOR_TEXT_STAGE,
    resolve_existing,
)

_SOURCE_FILENAME = BLOCKS_ANALYSIS_FILENAME

# Лимиты компактизации
_MAX_FINDING_CHARS = 700
_MAX_VALUE_CHARS = 300
_MAX_EVIDENCE_CHARS = 300
# Мягкий потолок на общее число включённых findings (страховка от гигантских листов).
_MAX_TOTAL_FINDINGS = 600

_OK_COVERAGE = "ok"


def _trim(value: Any, limit: int) -> Any:
    if not isinstance(value, str):
        return value
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "…"


def _compact_finding(finding: dict) -> dict:
    provenance = finding.get("provenance") or {}
    return {
        "id": finding.get("id"),
        "severity": finding.get("severity"),
        "category": finding.get("category"),
        "finding": _trim(finding.get("finding"), _MAX_FINDING_CHARS),
        "value_found": _trim(finding.get("value_found"), _MAX_VALUE_CHARS),
        "block_evidence": _trim(finding.get("block_evidence"), _MAX_EVIDENCE_CHARS),
        "highlight_regions": finding.get("highlight_regions") or [],
        "provenance": {
            "found_by": provenance.get("found_by") or [],
            "detector_summary": provenance.get("detector_summary") or "unattributed",
        },
    }


def build_compact_view(data: dict) -> dict:
    """Собрать компактную проекцию из содержимого 01_blocks_analysis.json."""
    block_analyses = data.get("block_analyses")
    if not isinstance(block_analyses, list):
        block_analyses = []

    total_blocks = len(block_analyses)
    omitted_clean = 0
    findings_budget = _MAX_TOTAL_FINDINGS
    truncated_findings = False
    compact_blocks: list[dict] = []

    for block in block_analyses:
        if not isinstance(block, dict):
            continue
        findings = [f for f in (block.get("findings") or []) if isinstance(f, dict)]
        coverage = block.get("coverage_status")
        # «Чистый» блок: нет findings и покрытие ok — для сверки бесполезен.
        if not findings and coverage == _OK_COVERAGE:
            omitted_clean += 1
            continue

        compact_findings: list[dict] = []
        for f in findings:
            if findings_budget <= 0:
                truncated_findings = True
                break
            compact_findings.append(_compact_finding(f))
            findings_budget -= 1

        compact_blocks.append(
            {
                "block_id": block.get("block_id"),
                "page": block.get("page"),
                "sheet": block.get("sheet"),
                "coverage_status": coverage,
                "findings": compact_findings,
            }
        )

    return {
        "stage": BLOCKS_FOR_TEXT_STAGE,
        "source": _SOURCE_FILENAME,
        "meta": {
            "total_blocks": total_blocks,
            "blocks_included": len(compact_blocks),
            "blocks_omitted_clean": omitted_clean,
            "findings_truncated": truncated_findings,
            "max_total_findings": _MAX_TOTAL_FINDINGS,
        },
        "blocks": compact_blocks,
    }


def write_blocks_for_text_compact(output_dir: Path) -> Optional[Path]:
    """Прочитать 01_blocks_analysis.json из output_dir, записать 01_blocks_for_text.json.

    Возвращает путь к записанному файлу, либо None если исходник отсутствует/битый
    (fail-soft: текстовый этап штатно работает и без блочного контекста).
    """
    output_dir = Path(output_dir)
    src = resolve_existing(output_dir, BLOCKS_ANALYSIS_FILENAME)
    if not src.is_file():
        return None
    try:
        data = json.loads(src.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if not isinstance(data, dict):
        return None

    view = build_compact_view(data)
    dst = output_dir / BLOCKS_FOR_TEXT_FILENAME
    dst.write_text(json.dumps(view, ensure_ascii=False, indent=2), encoding="utf-8")
    return dst
