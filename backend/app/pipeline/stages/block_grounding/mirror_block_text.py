"""OCR-подмена («зеркало»): чистый вектор-текст блока для промпта Stage 02.

Мотив (замер спеки ЭМ-К1, 2026-07-06): вектор-слой PDF vs Chandra-OCR — 30/31 кабеля
идентичны, ЕДИНСТВЕННОЕ расхождение = OCR-ошибка Chandra `3х1.5`→`3x15` (потеряна точка).
Vision-модель (Gemma/GPT) на CAD-чертежах так же путает значения → ~66% брака аудита =
«нейронка не так прочитала графику». Вектор-слой этих ошибок физически не делает.

Источник — тот же `pdfplumber_text` из result.json, что уже индексирует
``singleline_graph_geometry._result_blocks_vector_index`` (block-scoped текст-слой блока).
Никакого нового артефакта/открытия PDF: берём готовый чистый текст блока.

Аддитивно: НЕ удаляем enrichment (там структура/сущности от Gemma), а ДОБАВЛЯЕМ вектор-текст
как приоритетный источник ЧИСЕЛ. Только где есть вектор-слой (скан → None, fail-soft на OCR).

Используется реальным Stage 02 (call_gpt_for_block) и превью /blocks/llm-text — через флаг
``config.MIRROR_OCR_ENABLED`` (default OFF), чтобы поведение совпадало и прод не менялся.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from .singleline_graph_geometry import _result_blocks_vector_index

# минимум символов, чтобы считать вектор-текст содержательным (короче — шум/пусто/скан)
_MIN_CHARS = 40


def _find_result_json(version_dir) -> Optional[Path]:
    """result.json блока. Прод-раскладка V2 = 02_work/result.json; legacy projects/ = *_result.json."""
    vd = Path(version_dir)
    rp = vd / "02_work" / "result.json"
    if rp.exists():
        return rp
    legacy = sorted(vd.glob("*_result.json"))
    return legacy[0] if legacy else None


def resolve_mirror_block_text(version_dir, block_id: str, *, min_chars: int = _MIN_CHARS) -> Optional[str]:
    """Чистый вектор-текст блока (block-scoped `pdfplumber_text`) или None. fail-soft.

    None — если нет result.json, блока в индексе, или текст короче ``min_chars``
    (скан/растр без вектор-слоя → остаётся за OCR).
    """
    try:
        rp = _find_result_json(version_dir)
        if rp is None:
            return None
        idx = _result_blocks_vector_index(str(rp), rp.stat().st_mtime)
        entry = idx.get(str(block_id)) or {}
        text = (entry.get("text") or "").strip()
        return text if len(text) >= min_chars else None
    except (OSError, ValueError, KeyError):
        return None


def inject_mirror_text(user_text: str, vector_text: str) -> str:
    """Добавить вектор-текст блока в промпт как приоритетный источник ЧИСЕЛ (над OCR-описанием)."""
    return (
        f"{user_text}\n\n"
        f"## Точный текст блока из вектор-слоя PDF (без ошибок OCR):\n"
        f"При РАСХОЖДЕНИИ значений с описанием выше — доверяй ЧИСЛАМ/маркам/сечениям ОТСЮДА "
        f"(это встроенный текст чертежа, не распознавание).\n"
        f"```\n{vector_text}\n```"
    )
