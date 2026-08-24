"""Canonical block-context artifact and legacy Gemma-summary adapter."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from backend.app.services.storage.stage_artifacts import (
    BLOCK_CONTEXT_SUMMARY_FILENAME,
    resolve_existing,
)
from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    GEMMA_BLOCKS_DIRNAME,
    STAGE02_BLOCKS_DIRNAME,
)

SCHEMA_VERSION = 2
STAGE = "block_context"
STAGE_TITLE = "Векторные графы блоков"
SOURCE_KINDS = {
    "structured_singleline",
    "structured_system_graph",
    "structured_electrical",
    "structured_general_plan",
    "structured_architecture",
    "structured_structure",
    "structured_technology",
    "structured_hvac",
    "structured_water",
    "structured_alia_scheme",
    # Профиль «Условные обозначения»: производится роутером
    # (block_source_router.py:825), но в этот список добавлен не был — и любой
    # проект, где нашёлся блок легенды, падал на валидации контракта с
    # «unknown source_kind». Поймано вживую 06.08.2026 на СТ26_01-14-ОВ1-1-РД
    # (11 блоков в 4 проектах). Против повторения — тест
    # test_block_context_source_kinds_cover_router.
    "structured_legend",
    "raw_vector",
    "image_only",
    "missing",
    "no_sources",
    "block_not_found",
    "error",
    "legacy_enrichment",
}

# Эти источники не содержат встроенного векторного текста PDF. В частности,
# legacy_enrichment — старое OCR/vision-описание PNG, а не векторный слой.
NO_VECTOR_TEXT_SOURCE_KINDS = {
    "image_only",
    "gemma_fallback",
    "legacy_enrichment",
    "missing",
    "no_sources",
    "block_not_found",
    "error",
}
VECTOR_GRAPH_MISSING_MESSAGE = "Векторный граф блока отсутствует"


def source_has_vector_text(source_kind: Any) -> bool:
    """Есть ли у источника блока пригодный векторный текст PDF."""
    return str(source_kind or "error") not in NO_VECTOR_TEXT_SOURCE_KINDS


def block_context_sources(summary: Any) -> dict[str, str]:
    """Источник графа по block_id из результата стадии block_context."""
    if not isinstance(summary, dict):
        return {}
    return {
        str(item.get("block_id")): str(item.get("source_kind") or "")
        for item in summary.get("blocks") or []
        if isinstance(item, dict) and item.get("block_id") and item.get("source_kind")
    }


def decorate_blocks_vector_state(blocks: Any, summary: Any) -> None:
    """Добавить UI-признак наличия векторного текста, не меняя JSON на диске."""
    sources = block_context_sources(summary)
    for block in blocks or []:
        if not isinstance(block, dict):
            continue
        source = sources.get(str(block.get("block_id") or ""))
        if not source:
            continue
        available = source_has_vector_text(source)
        block["vector_text_available"] = available
        block["vector_graph_source_kind"] = source
        block["vector_graph_message"] = (
            None if available else VECTOR_GRAPH_MISSING_MESSAGE
        )


def resolve_blocks_dir(output_dir: Path) -> Path:
    """Prefer the canonical Stage 01 PNG directory, with read-only fallbacks."""
    output_dir = Path(output_dir)
    for name in (STAGE02_BLOCKS_DIRNAME, GEMMA_BLOCKS_DIRNAME, "blocks"):
        candidate = output_dir / name
        if (candidate / "index.json").is_file():
            return candidate
    return output_dir / STAGE02_BLOCKS_DIRNAME


def resolve_blocks_index(output_dir: Path) -> Path:
    return resolve_blocks_dir(output_dir) / "index.json"


#: PNG меньше этого размера считаем непригодным (обрезанная/пустая запись).
#: Порог согласован с проверкой `size_kb > 1` в crop_blocks/blocks.py.
MIN_USABLE_CROP_BYTES = 1024


def block_file_for(entry: dict[str, Any]) -> str:
    """Имя файла кропа для записи index.json.

    Авторитетно именно поле ``file``: галерея векторных графов пишет
    ``block_<id>.webp`` (backend/scripts/build_vector_graph_gallery.py), а
    несколько мест исторически собирали имя как ``block_{id}.png`` и потому
    отдавали 404 на таких блоках. Fallback оставлен для старых индексов.
    """
    name = str(entry.get("file") or "").strip()
    if name:
        return name
    return f"block_{entry.get('block_id')}.png"


def crops_materialized(
    blocks_dir: Path,
    index: dict[str, Any] | None = None,
) -> tuple[bool, list[str]]:
    """Есть ли на диске PNG для КАЖДОГО блока, заявленного в index.json.

    Возвращает ``(всё_на_месте, [block_id отсутствующих])``.

    Зачем: состояние «index.json есть, PNG нет» достижимо УЖЕ СЕЙЧАС (resume
    засевает run-папку одним index.json, manager.py), а после включения
    эвакуации кропов оно становится штатным. Все проверки готовности при этом
    смотрели только на существование index.json и рапортовали «кропы готовы»,
    после чего анализ блоков шёл вслепую и возвращал `PNG missing` по каждому
    блоку. Этот предикат — единственная точка правды о наличии картинок.

    Проверяем КАЖДУЮ запись, а не только количество файлов: обрезанный PNG
    (например, от падения посреди записи) сохраняет количество, но картинкой не
    является — и молча уехал бы в модель. Пара сотен ``stat`` на вызов, а зовут
    эту функцию только на контрольных точках готовности.
    """
    blocks_dir = Path(blocks_dir)
    if index is None:
        index_path = blocks_dir / "index.json"
        if not index_path.is_file():
            return True, []  # индекса нет — это не наш случай, решает вызывающий
        try:
            index = json.loads(index_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return True, []

    entries = index if isinstance(index, list) else (index.get("blocks") or [])
    entries = [e for e in entries if isinstance(e, dict)]
    if not entries:
        return True, []

    if not blocks_dir.is_dir():
        return False, [str(e.get("block_id") or "") for e in entries]

    missing: list[str] = []
    for entry in entries:
        path = blocks_dir / block_file_for(entry)
        try:
            if path.stat().st_size >= MIN_USABLE_CROP_BYTES:
                continue
        except OSError:
            pass
        missing.append(str(entry.get("block_id") or ""))
    return (not missing), missing


def summary_path(output_dir: Path) -> Path:
    return Path(output_dir) / BLOCK_CONTEXT_SUMMARY_FILENAME


def block_context_up_to_date(
    output_dir: Path,
    *,
    blocks_index_path: Path | None = None,
) -> dict[str, Any]:
    """Готов ли контекст блоков для ТЕКУЩЕГО набора кропов.

    Отвечает на вопрос «можно ли пропустить пересборку»: одной валидности
    сводки мало — она могла быть построена по прошлому набору блоков. Поэтому
    дополнительно сверяем, что каждый block_id из index.json уже покрыт
    сводкой. Обратное несовпадение (в сводке блоков больше) допустимо: лишние
    записи не мешают, а перекроп мог отсеять мелочь.

    Возвращает ``{"ready": bool, "reason": str, "summary": dict}``.
    """
    output_dir = Path(output_dir)
    validation = validate_block_context_summary(output_dir, canonical_only=True)
    if not validation.get("valid"):
        return {"ready": False, "reason": str(validation.get("reason") or "summary невалиден")}

    summary = validation.get("summary") or {}
    index_path = Path(blocks_index_path) if blocks_index_path else resolve_blocks_index(output_dir)
    if not index_path.is_file():
        return {"ready": False, "reason": "index.json кропов отсутствует", "summary": summary}
    try:
        index = json.loads(index_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return {"ready": False, "reason": f"index.json не читается: {exc}", "summary": summary}

    entries = index if isinstance(index, list) else (index.get("blocks") or [])
    indexed_ids = {
        str(e.get("block_id"))
        for e in entries
        if isinstance(e, dict) and e.get("block_id")
    }
    covered_ids = {
        str(b.get("block_id"))
        for b in (summary.get("blocks") or [])
        if isinstance(b, dict) and b.get("block_id")
    }
    uncovered = indexed_ids - covered_ids
    if uncovered:
        return {
            "ready": False,
            "reason": f"контекст не покрывает {len(uncovered)} блоков из index.json",
            "summary": summary,
            "uncovered": sorted(uncovered),
        }
    return {"ready": True, "reason": "", "summary": summary}


def _legacy_source(block: dict[str, Any]) -> str:
    response_source = str(block.get("base_response_source") or "")
    if response_source == "vector_skip":
        return "raw_vector"
    if response_source == "stage_disabled_skip":
        return "image_only"
    final_profile = str(block.get("final_profile") or "")
    if final_profile and final_profile != "none":
        return "legacy_enrichment"
    return "missing"


def adapt_legacy_summary(payload: dict[str, Any]) -> dict[str, Any]:
    blocks = []
    counts: dict[str, int] = {}
    for item in payload.get("blocks") or []:
        if not isinstance(item, dict) or not item.get("block_id"):
            continue
        source = _legacy_source(item)
        counts[source] = counts.get(source, 0) + 1
        blocks.append({
            "block_id": str(item["block_id"]),
            "page": item.get("page"),
            "source_kind": source,
            "coverage_status": "ready" if source != "missing" else "error",
            "context_hash": None,
            "warnings": list(item.get("warnings") or []),
            "legacy": True,
        })
    total = int(payload.get("blocks_total") or len(blocks))
    failed = int(payload.get("blocks_failed") or sum(b["coverage_status"] != "ready" for b in blocks))
    ready = int(payload.get("blocks_ok") or max(0, total - failed))
    return {
        "schema_version": SCHEMA_VERSION,
        "stage": STAGE,
        "pipeline_block": "block_vector_graph",
        "pipeline_block_title": STAGE_TITLE,
        "status": "ok" if blocks else str(payload.get("status") or "no_blocks"),
        "blocks_total": total,
        "blocks_ready": ready,
        "blocks_failed": failed,
        "source_counts": counts,
        "blocks": blocks,
        "legacy_source": "gemma_enrichment_summary.json",
    }


def load_block_context_summary(output_dir: Path) -> dict[str, Any]:
    path = resolve_existing(output_dir, BLOCK_CONTEXT_SUMMARY_FILENAME)
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if path.name != BLOCK_CONTEXT_SUMMARY_FILENAME:
        return adapt_legacy_summary(payload)
    return payload if isinstance(payload, dict) else {}


def validate_block_context_summary(output_dir: Path, *, canonical_only: bool = False) -> dict[str, Any]:
    path = summary_path(output_dir) if canonical_only else resolve_existing(
        output_dir, BLOCK_CONTEXT_SUMMARY_FILENAME
    )
    if not path.is_file():
        return {"valid": False, "reason": f"{path.name} отсутствует"}
    summary = load_block_context_summary(output_dir)
    if not summary:
        return {"valid": False, "reason": "summary не читается"}
    if path.name != BLOCK_CONTEXT_SUMMARY_FILENAME and not summary.get("blocks"):
        return {"valid": False, "reason": "legacy summary не содержит block entries"}
    if summary.get("schema_version") != SCHEMA_VERSION or summary.get("stage") != STAGE:
        return {"valid": False, "reason": "schema/stage mismatch"}
    if path.name == BLOCK_CONTEXT_SUMMARY_FILENAME:
        catalog = summary.get("reference_catalog")
        if not isinstance(catalog, dict) or catalog.get("runtime_source") != "pipeline_stage_embedded_catalog":
            return {"valid": False, "reason": "встроенный каталог эталонов не указан"}
        if int(catalog.get("records_total") or 0) <= 0:
            return {"valid": False, "reason": "встроенный каталог эталонов пуст"}
    blocks = summary.get("blocks")
    if not isinstance(blocks, list):
        return {"valid": False, "reason": "blocks должен быть списком"}
    for block in blocks:
        if not isinstance(block, dict) or not block.get("block_id"):
            return {"valid": False, "reason": "block entry invalid"}
        if block.get("source_kind") not in SOURCE_KINDS:
            return {"valid": False, "reason": "unknown source_kind"}
    return {"valid": True, "path": str(path), "summary": summary}
