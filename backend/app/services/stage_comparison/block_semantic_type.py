"""Классификация семантики блока по его содержимому.

Модуль выделен из удалённого ``pipeline_v2_prepared_ingest`` (ветка Pipeline V2 β
снята с платформы), потому что ``prepared_document`` — живой модуль новой
детерминированной цепочки сравнения — зависит только от этого классификатора.

Ничего не распознаёт заново: работает по уже извлечённым полям блока
(``block_type``, ``category_code``, OCR-текст, ``stamp_data``) и относительному
размеру блока на странице. Ни сети, ни моделей.
"""
from __future__ import annotations

from typing import Any

__all__ = ["classify_block_semantic_type"]


_KW_TITLE = ("титульный лист", "титульник", "обложка", "title page")
_KW_SCHEME = (
    "структурная схема", "принципиальная схема", "однолинейн", "схема",
    "scheme", "графическая часть",
)
_KW_PLAN = ("план ", "план\t", "план расположения", "план этажа", "генплан", "разрез", "фасад")
_KW_TABLE = (
    "ведомость", "спецификация", "таблица", "перечень элементов", "опросный лист",
)
_KW_LEGEND = ("условные обозначения", "экспликация", "перечень условных")
_KW_STAMP = ("штамп", "основная надпись", "title block")

# Доля площади страницы, выше которой схема считается «большой» (large_scheme).
_LARGE_SCHEME_AREA_RATIO = 0.45


def _clean_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _kw(text: Any) -> str:
    """Канонизировать строку для keyword-сопоставления: lower + ё→е.

    Russian OCR непредсказуемо чередует ё/е («внесённых» vs «внесенных»),
    поэтому ключевые слова храним в форме с «е», а вход приводим сюда.
    """
    return _clean_str(text).lower().replace("ё", "е")


def _area_ratio(coords_norm: list[float], coords_px: list[float],
                page_width: int, page_height: int) -> float:
    """Доля площади страницы, занимаемая блоком (0..1). 0.0 если неизвестно."""
    if len(coords_norm) >= 4:
        w = abs(coords_norm[2] - coords_norm[0])
        h = abs(coords_norm[3] - coords_norm[1])
        ratio = w * h
        if 0.0 <= ratio <= 1.0:
            return ratio
    if len(coords_px) >= 4 and page_width > 0 and page_height > 0:
        w = abs(coords_px[2] - coords_px[0])
        h = abs(coords_px[3] - coords_px[1])
        denom = float(page_width) * float(page_height)
        if denom > 0:
            return max(0.0, min(1.0, (w * h) / denom))
    return 0.0


def classify_block_semantic_type(block: dict) -> str:
    """Классифицировать семантику блока.

    Возвращает один из:
        stamp | text | table | scheme | large_scheme | plan | legend | title |
        unknown

    Учитывает block_type, category_code, ocr_json (content_summary/clean_ocr_text),
    stamp_data.sheet_name, наличие crop_url и относительный размер блока.
    """
    block_type = block.get("block_type") or "unknown"
    cat = (block.get("category_code") or "").lower()

    summary = block.get("ocr_summary") or ""
    clean = block.get("ocr_clean") or ""
    ocr_text = block.get("ocr_text") or ""
    sd = block.get("stamp_data") if isinstance(block.get("stamp_data"), dict) else {}
    sheet_name = _clean_str(sd.get("sheet_name"))
    text_blob = _kw(" ".join([sheet_name, summary, clean, ocr_text, cat]))

    # 1) Штамп (основная надпись): ocr_json сам является штампным словарём,
    #    либо явный category_code/ключевое слово.
    if block.get("ocr_json_is_stamp") or "stamp" in cat or any(k in text_blob for k in _KW_STAMP):
        return "stamp"

    # 2) Легенда / условные обозначения (раньше table — экспликация спорна).
    if any(k in text_blob for k in _KW_LEGEND):
        return "legend"

    # 3) Текстовый блок: схема/план — графические типы, поэтому текст НЕ может
    #    стать scheme/plan, даже если в OCR упоминается «схема»/«графическая
    #    часть» (это перечень/описание, а не сама схема).
    if block_type == "text":
        if any(k in text_blob for k in _KW_TITLE):
            return "title"
        if any(k in text_blob for k in _KW_TABLE):
            return "table"
        return "text"

    # 4) Не-текст (image/table/unknown): таблица.
    if block_type == "table" or any(k in text_blob for k in _KW_TABLE):
        return "table"

    # 5) Титульный лист.
    if any(k in text_blob for k in _KW_TITLE):
        return "title"

    # 6) Схемы / планы (только для графических блоков).
    if any(k in text_blob for k in _KW_SCHEME):
        area = _area_ratio(block.get("coords_norm") or [], block.get("coords_px") or [],
                           block.get("page_width") or 0, block.get("page_height") or 0)
        if area >= _LARGE_SCHEME_AREA_RATIO or "структурная схема" in text_blob:
            return "large_scheme"
        return "scheme"
    if any(k in text_blob for k in _KW_PLAN):
        return "plan"

    # image/unknown без ключевых слов — недостаточно сигналов.
    return "unknown"
