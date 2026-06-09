# -*- coding: utf-8 -*-
"""Pipeline V2 — Prepared Package Ingest (этап 1, backend-only, изолированный).

Новый режим сравнения стадий («Pipeline V2») строится поверх УЖЕ подготовленного
комплекта документации, который приходит от внешнего OCR/нарезчика:

    document.pdf      ← исходный PDF (source of truth для пикселей)
    *_result.json     ← геометрия + блоки + штампы + ссылки на PDF-кропы (SOT)
    *_document.md     ← человекочитаемый Markdown-слой (Chandra OCR), debug/fallback
    *_ocr.html        ← HTML-слой OCR (debug)

Страницы уже обведены, блоки разрезаны по координатам, у image-блоков есть
``crop_url`` / ``image_file``, ``coords_px`` / ``coords_norm``, ``block_type``,
``ocr_text`` / ``ocr_html`` / ``pdfplumber_text`` / ``ocr_json`` и ``stamp_data``.

Этот модуль НЕ запускает Qwen/Opus/OCR/PDF-render и НЕ ходит в сеть (в т.ч. НЕ
скачивает ``crop_url``). Он только читает локальные JSON/MD/HTML, нормализует их
в стабильную «модель документа» (``normalized_document_model``) и собирает
диагностику качества входа. Это фундамент для будущего block-level pipeline
(block matching → entity extraction → deterministic diff → Opus explanation →
critic → grouping → UI).

Все функции чистые (детерминированные, без побочных эффектов), кроме
``write_normalized_document_model`` (атомарная запись артефакта на диск).

См. docs/stage_comparison_pipeline_v2_prepared_package_ingest.md.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Optional

MODEL_VERSION = 1
MODEL_KIND = "stage_comparison_pipeline_v2_normalized_document"

# Длины срезов текста, чтобы модель оставалась компактной.
_TEXT_EXCERPT_LEN = 600
_PDFPLUMBER_EXCERPT_LEN = 600

# ─── Markdown page-разметка (для опционального cross-check / fallback имён) ──

_MD_PAGE_HEADING_RE = re.compile(
    r"^\s{0,3}#{1,6}\s+(?:СТРАНИЦА|Страница|PAGE|Page)\s*[:№#]?\s*(\d+)\s*$",
    re.MULTILINE,
)
_MD_SHEET_NAME_RE = re.compile(r"\*\*Наименование листа:\*\*\s*(.+)")

# ─── Ключевые слова классификации (lower-case) ──────────────────────────────

# Ключевые слова храним в канонической форме (lower + ё→е); вход приводится _kw().
_KW_CHANGE_LOG = (
    "справка о внесен", "ведомость изменен", "лист регистрации изменен",
    "таблица изменен", "регистрация изменен",
)
_KW_CONTENTS = (
    "содержание тома", "содержание раздела", "состав проектной документац",
    "состав тома",
)
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
_KW_TEXT_PART = ("текстовая часть", "пояснительная записка", "общие данные", "общие указания")
_KW_STAMP = ("штамп", "основная надпись", "title block")

# stamp_data / ocr_json содержат «штампные» ключи — сильный признак stamp-блока.
_STAMP_DICT_KEYS = ("document_code", "sheet_number", "sheet_name", "stage", "organization")

# Доля площади страницы, выше которой схема считается «большой» (large_scheme).
_LARGE_SCHEME_AREA_RATIO = 0.45


# ─── Низкоуровневые помощники ───────────────────────────────────────────────


def _safe_load_json(path: str | Path | None) -> Any:
    if not path:
        return None
    try:
        p = Path(path)
        if not p.exists():
            return None
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError, UnicodeDecodeError, ValueError):
        return None


def _safe_read_text(path: str | Path | None) -> Optional[str]:
    if not path:
        return None
    try:
        p = Path(path)
        if not p.exists():
            return None
        return p.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def _to_int_safe(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        return int(float(v))
    except (TypeError, ValueError):
        return default


def _as_float_list(value: Any) -> list[float]:
    """Привести координаты к плоскому списку float. Не угадывает формат —
    лишь безопасно копирует числа (bbox или плоский polygon)."""
    if not isinstance(value, (list, tuple)):
        return []
    out: list[float] = []
    for item in value:
        if isinstance(item, (int, float)):
            out.append(float(item))
        elif isinstance(item, (list, tuple)):
            for sub in item:
                if isinstance(sub, (int, float)):
                    out.append(float(sub))
    return out


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


def _truncate(text: str, limit: int) -> str:
    text = text or ""
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def _normalize_block_type(raw: dict) -> str:
    """Привести block_type к {text, image, table, unknown}."""
    t = _clean_str(raw.get("block_type") or raw.get("type") or raw.get("label")).lower()
    if not t:
        return "unknown"
    if any(k in t for k in ("table", "таблиц")):
        return "table"
    if any(k in t for k in ("image", "imagine", "picture", "figure", "карт", "схем", "рис")):
        return "image"
    if any(k in t for k in ("text", "para", "header", "title", "stamp", "текст")):
        return "text"
    return "unknown"


def _normalize_shape_type(raw: dict) -> str:
    s = _clean_str(raw.get("shape_type")).lower()
    if s in ("rectangle", "rect", "bbox", "box"):
        return "rectangle"
    if s in ("polygon", "poly", "freeform"):
        return "polygon"
    if raw.get("polygon_points") or raw.get("polygon_points_norm"):
        return "polygon"
    return "unknown"


def _ocr_json_text(ocr_json: Any) -> tuple[str, str]:
    """Вернуть (content_summary, clean_ocr_text) из ocr_json, если это словарь
    с человекочитаемым OCR (а не штампный словарь)."""
    if not isinstance(ocr_json, dict):
        return "", ""
    summary = _clean_str(ocr_json.get("content_summary"))
    clean = _clean_str(ocr_json.get("clean_ocr_text"))
    return summary, clean


def _is_stamp_dict(value: Any) -> bool:
    """Похож ли словарь на штамп (основную надпись), а не на обычный OCR-блок."""
    if not isinstance(value, dict):
        return False
    keys = set(value.keys())
    # «штампные» поля: достаточно пары признаков (sheet_number/document_code/…).
    hits = sum(1 for k in _STAMP_DICT_KEYS if k in keys)
    return hits >= 2


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


# ─── 1. normalize_result_json ───────────────────────────────────────────────


def _page_stamp_info(page_blocks: list[dict]) -> dict:
    """Извлечь страничную инфо из stamp_data блоков (sheet_number/sheet_name/код)."""
    best: dict = {"sheet_number": "", "total_sheets": "", "sheet_name": "",
                  "document_code": ""}
    for raw in page_blocks:
        sd = raw.get("stamp_data")
        if not isinstance(sd, dict):
            continue
        sn = _clean_str(sd.get("sheet_number"))
        snm = _clean_str(sd.get("sheet_name"))
        ts = _clean_str(sd.get("total_sheets"))
        dc = _clean_str(sd.get("document_code"))
        if sn and not best["sheet_number"]:
            best["sheet_number"] = sn
        if snm and not best["sheet_name"]:
            best["sheet_name"] = snm
        if ts and not best["total_sheets"]:
            best["total_sheets"] = ts
        if dc and not best["document_code"]:
            best["document_code"] = dc
    return best


def _normalize_one_block(raw: dict, *, page_number: int, page_index: int,
                         page_width: int, page_height: int, seq: int) -> dict:
    """Нормализовать один сырой блок из result.json в промежуточную форму."""
    raw_id = raw.get("id") if raw.get("id") not in (None, "") else raw.get("block_id")
    had_id = raw_id not in (None, "")
    block_id = _clean_str(raw_id) or f"blk{seq:04d}"

    coords_px = _as_float_list(raw.get("coords_px") or raw.get("bbox") or raw.get("coords"))
    coords_norm = _as_float_list(raw.get("coords_norm") or raw.get("bbox_norm"))

    blk_page_index = page_index
    if raw.get("page_index") is not None:
        blk_page_index = _to_int_safe(raw.get("page_index"), page_index)

    ocr_json = raw.get("ocr_json")
    summary, clean = _ocr_json_text(ocr_json)
    ocr_text = _clean_str(raw.get("ocr_text") or raw.get("text"))
    pdfplumber_text = _clean_str(raw.get("pdfplumber_text"))

    return {
        "block_id": block_id,
        "had_explicit_id": had_id,
        "page_number": page_number,
        "page_index": blk_page_index,
        "block_type": _normalize_block_type(raw),
        "raw_block_type": _clean_str(raw.get("block_type") or raw.get("type")),
        "category_code": _clean_str(raw.get("category_code")) or None,
        "coords_px": coords_px,
        "coords_norm": coords_norm,
        "shape_type": _normalize_shape_type(raw),
        "source": _clean_str(raw.get("source")) or "unknown",
        "crop_url": _clean_str(raw.get("crop_url")) or None,
        "image_file": _clean_str(raw.get("image_file")) or None,
        "ocr_text": ocr_text,
        "ocr_html_present": bool(raw.get("ocr_html")),
        "pdfplumber_text": pdfplumber_text,
        "ocr_json": ocr_json if isinstance(ocr_json, dict) else None,
        "ocr_json_is_stamp": _is_stamp_dict(ocr_json),
        "ocr_summary": summary,
        "ocr_clean": clean,
        "stamp_data": raw.get("stamp_data") if isinstance(raw.get("stamp_data"), dict) else None,
        "page_width": page_width,
        "page_height": page_height,
    }


def normalize_result_json(result_json_path: str | Path) -> dict:
    """Прочитать ``result.json`` и вернуть промежуточную нормализованную форму.

    Возвращает::

        {
          "ok": bool,
          "source_pdf_path": str | None,   # data["pdf_path"], если есть
          "pages": [ {page_number, page_index, width, height,
                      sheet_number, total_sheets, sheet_name, document_code,
                      block_ids: [...] } ],
          "blocks": [ <intermediate block dict>, ... ],
        }

    Поддерживает основной формат A (``data["pages"][].blocks``) и flat-fallback
    формат B (``data["blocks"]``). Сетевых вызовов и рендера нет.
    """
    data = _safe_load_json(result_json_path)
    if not isinstance(data, dict):
        return {"ok": False, "source_pdf_path": None, "pages": [], "blocks": []}

    blocks: list[dict] = []
    pages: list[dict] = []
    seq = 0
    source_pdf_path = _clean_str(data.get("pdf_path")) or None

    raw_pages = data.get("pages")
    if isinstance(raw_pages, list) and raw_pages:
        for p_idx, page in enumerate(raw_pages):
            if not isinstance(page, dict):
                continue
            page_number = _to_int_safe(
                page.get("page_number") or page.get("page") or (p_idx + 1), p_idx + 1)
            page_index = _to_int_safe(page.get("page_index"), page_number - 1)
            pw = _to_int_safe(page.get("width"))
            ph = _to_int_safe(page.get("height"))
            page_blocks = page.get("blocks") if isinstance(page.get("blocks"), list) else []

            stamp = _page_stamp_info(page_blocks)
            block_ids: list[str] = []
            for raw in page_blocks:
                if not isinstance(raw, dict):
                    continue
                seq += 1
                nb = _normalize_one_block(raw, page_number=page_number,
                                          page_index=page_index, page_width=pw,
                                          page_height=ph, seq=seq)
                blocks.append(nb)
                block_ids.append(nb["block_id"])

            pages.append({
                "page_number": page_number,
                "page_index": page_index,
                "width": pw,
                "height": ph,
                "sheet_number": stamp["sheet_number"],
                "total_sheets": stamp["total_sheets"],
                "sheet_name": stamp["sheet_name"],
                "document_code": stamp["document_code"],
                "block_ids": block_ids,
            })
        return {"ok": True, "source_pdf_path": source_pdf_path,
                "pages": pages, "blocks": blocks}

    # Формат B: плоский список блоков.
    flat = data.get("blocks")
    if isinstance(flat, list):
        by_page: dict[int, list[str]] = {}
        page_dims: dict[int, tuple[int, int]] = {}
        for raw in flat:
            if not isinstance(raw, dict):
                continue
            seq += 1
            pn = _to_int_safe(raw.get("page_number") or raw.get("page")
                              or ((raw.get("page_index") or 0) + 1), 1)
            pw = _to_int_safe(raw.get("page_width"))
            ph = _to_int_safe(raw.get("page_height"))
            nb = _normalize_one_block(raw, page_number=pn,
                                      page_index=_to_int_safe(raw.get("page_index"), pn - 1),
                                      page_width=pw, page_height=ph, seq=seq)
            blocks.append(nb)
            by_page.setdefault(pn, []).append(nb["block_id"])
            page_dims.setdefault(pn, (pw, ph))
        for pn in sorted(by_page.keys()):
            page_blocks = [b for b in blocks if b["page_number"] == pn]
            stamp = _page_stamp_info(
                [{"stamp_data": b["stamp_data"]} for b in page_blocks if b["stamp_data"]])
            pw, ph = page_dims.get(pn, (0, 0))
            pages.append({
                "page_number": pn,
                "page_index": pn - 1,
                "width": pw,
                "height": ph,
                "sheet_number": stamp["sheet_number"],
                "total_sheets": stamp["total_sheets"],
                "sheet_name": stamp["sheet_name"],
                "document_code": stamp["document_code"],
                "block_ids": by_page[pn],
            })
        return {"ok": True, "source_pdf_path": source_pdf_path,
                "pages": pages, "blocks": blocks}

    return {"ok": False, "source_pdf_path": source_pdf_path, "pages": [], "blocks": []}


# ─── 2. classify_block_semantic_type ────────────────────────────────────────


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


# ─── 3. classify_page_type ──────────────────────────────────────────────────


def _block_semantic(block: dict) -> str:
    st = block.get("semantic_type")
    if st:
        return st
    return classify_block_semantic_type(block)


def classify_page_type(page: dict, page_blocks: list[dict]) -> str:
    """Классифицировать тип страницы.

    Возвращает: title | change_log | contents | text | scheme | table | mixed |
    unknown.

    ``page`` — страничная мета (sheet_name/document_code), ``page_blocks`` —
    нормализованные блоки страницы (могут уже нести ``semantic_type``).
    """
    name = _kw(page.get("sheet_name"))
    parts = [name]
    for b in page_blocks:
        parts.append(_kw(b.get("ocr_summary")))
        parts.append(_kw(b.get("ocr_clean")))
        parts.append(_kw((b.get("ocr_text") or "")[:200]))
    text_blob = " ".join(p for p in parts if p)

    # 1) Очень специфичные документные типы.
    if any(k in text_blob for k in _KW_CHANGE_LOG):
        return "change_log"
    if any(k in text_blob for k in _KW_CONTENTS) or name == "содержание":
        return "contents"
    if any(k in text_blob for k in _KW_TITLE):
        return "title"

    # 2) Состав «видов» содержимого на странице.
    kinds: set[str] = set()
    for b in page_blocks:
        st = _block_semantic(b)
        if st in ("scheme", "large_scheme", "plan"):
            kinds.add("scheme")
        elif st == "table":
            kinds.add("table")
        elif st == "text":
            # учитываем только содержательный текст (не пустой)
            if len((b.get("ocr_text") or b.get("ocr_clean") or "").strip()) >= 20:
                kinds.add("text")
        # stamp / legend / title / unknown в композицию не идут
        if b.get("block_type") == "image" and b.get("crop_url"):
            kinds.add("scheme")

    # 3) Сигналы из имени листа.
    if any(k in name for k in _KW_SCHEME) or any(k in name for k in _KW_PLAN):
        kinds.add("scheme")
    if any(k in name for k in _KW_TABLE):
        kinds.add("table")
    if any(k in name for k in _KW_TEXT_PART):
        kinds.add("text")

    if len(kinds) >= 2:
        return "mixed"
    if kinds == {"scheme"}:
        return "scheme"
    if kinds == {"table"}:
        return "table"
    if kinds == {"text"}:
        return "text"
    return "unknown"


# ─── 4. extract_document_stamp_summary ──────────────────────────────────────


_DOC_FIELD_ALIASES = {
    "document_code": ("document_code", "doc_code", "code", "cipher", "шифр"),
    "project_name": ("project_name", "object_name", "object", "объект"),
    "stage": ("stage", "стадия"),
    "organization": ("organization", "org", "организация"),
}


def extract_document_stamp_summary(blocks: list[dict]) -> dict:
    """Собрать документ-уровневую сводку из штампов блоков.

    Каждый блок несёт ``stamp_data`` с (как правило одинаковыми по всему
    документу) полями. Берём самое частое непустое значение каждого поля.

    Возвращает ``{document_code, project_name, stage, organization}`` (строки;
    пустая строка, если поле нигде не заполнено).
    """
    from collections import Counter

    counters: dict[str, "Counter[str]"] = {k: Counter() for k in _DOC_FIELD_ALIASES}
    for b in blocks:
        sd = b.get("stamp_data")
        if not isinstance(sd, dict):
            # ocr_json штампного вида тоже годится как источник
            sd = b.get("ocr_json") if b.get("ocr_json_is_stamp") else None
        if not isinstance(sd, dict):
            continue
        for field_name, aliases in _DOC_FIELD_ALIASES.items():
            for alias in aliases:
                val = _clean_str(sd.get(alias))
                if val:
                    counters[field_name][val] += 1
                    break

    out: dict[str, str] = {}
    for field_name, counter in counters.items():
        out[field_name] = counter.most_common(1)[0][0] if counter else ""
    return out


# ─── 5. build_block_registry (per-block model + quality flags) ──────────────


def _block_quality_flags(block: dict) -> list[str]:
    flags: list[str] = []
    if not block.get("had_explicit_id"):
        flags.append("missing_block_id")
    if not block.get("coords_px") and not block.get("coords_norm"):
        flags.append("missing_coords")
    if block.get("block_type") == "image" and not block.get("crop_url") \
            and not block.get("image_file"):
        flags.append("image_block_without_crop_or_image_file")
    if block.get("block_type") == "unknown":
        flags.append("unknown_block_type")

    sd = block.get("stamp_data")
    if sd is None:
        flags.append("empty_stamp_data")
    elif isinstance(sd, dict):
        if not any(_clean_str(sd.get(k)) for k in
                   ("document_code", "sheet_number", "sheet_name", "stage", "organization")):
            flags.append("empty_stamp_data")
        elif not _clean_str(sd.get("sheet_number")) and not _clean_str(sd.get("sheet_name")):
            flags.append("partial_stamp_data")

    pn = block.get("page_number")
    pi = block.get("page_index")
    if not isinstance(pn, int) or pn < 1:
        flags.append("strange_page_number")
    if isinstance(pi, int) and pi < 0:
        flags.append("strange_page_index")

    has_text = bool((block.get("ocr_text") or block.get("ocr_clean")
                     or block.get("pdfplumber_text")))
    if not has_text and not block.get("ocr_html_present"):
        flags.append("empty_ocr")

    if block.get("block_type") == "image" and block.get("pdfplumber_text") \
            and not block.get("ocr_json"):
        flags.append("pdfplumber_without_ocr_json")

    if block.get("crop_url"):
        # информационный флаг: облачный кроп есть, но мы его НЕ скачиваем
        flags.append("has_cloud_crop_url")

    return flags


def _build_block_model(block: dict) -> dict:
    """Финальная per-block модель (формат, описанный в docs)."""
    semantic = classify_block_semantic_type(block)
    sd = block.get("stamp_data") if isinstance(block.get("stamp_data"), dict) else {}
    return {
        "block_id": block["block_id"],
        "page_number": block["page_number"],
        "page_index": block["page_index"],
        "block_type": block["block_type"],
        "semantic_type": semantic,
        "coords_px": block.get("coords_px") or [],
        "coords_norm": block.get("coords_norm") or [],
        "shape_type": block.get("shape_type") or "unknown",
        "source": block.get("source") or "unknown",
        "crop_url": block.get("crop_url"),
        "image_file": block.get("image_file"),
        "has_crop_pdf": bool(block.get("crop_url")),
        "has_image_file": bool(block.get("image_file")),
        "has_pdfplumber_text": bool(block.get("pdfplumber_text")),
        "has_ocr_json": bool(block.get("ocr_json")),
        "has_stamp_data": bool(block.get("stamp_data")),
        "text_excerpt": _truncate(block.get("ocr_text") or block.get("ocr_clean") or "",
                                  _TEXT_EXCERPT_LEN),
        "pdfplumber_text_excerpt": _truncate(block.get("pdfplumber_text") or "",
                                             _PDFPLUMBER_EXCERPT_LEN),
        "stamp_data": dict(sd),
        "quality_flags": _block_quality_flags(block),
    }


def build_block_registry(blocks: list[dict]) -> dict[str, dict]:
    """Собрать реестр блоков ``{block_id: <per-block model>}``.

    При коллизии ``block_id`` (дубликаты в result.json) добавляется суффикс
    ``#N``, чтобы реестр оставался полным и не терял блоки.
    """
    registry: dict[str, dict] = {}
    for b in blocks:
        model = _build_block_model(b)
        bid = model["block_id"]
        if bid in registry:
            n = 2
            while f"{bid}#{n}" in registry:
                n += 1
            bid = f"{bid}#{n}"
            model = dict(model)
            model["block_id"] = bid
            model.setdefault("quality_flags", [])
            if "duplicate_block_id" not in model["quality_flags"]:
                model["quality_flags"].append("duplicate_block_id")
        registry[bid] = model
    return registry


# ─── 6. build_normalized_document_model ─────────────────────────────────────


def _parse_md_sheet_names(md_text: str) -> dict[int, str]:
    """Из Markdown извлечь {page_number: sheet_name} по разметке Chandra."""
    names: dict[int, str] = {}
    if not md_text:
        return names
    # Разбиваем по заголовкам страниц, ищем имя листа в каждом сегменте.
    matches = list(_MD_PAGE_HEADING_RE.finditer(md_text))
    for i, m in enumerate(matches):
        page_no = _to_int_safe(m.group(1), 0)
        if page_no <= 0:
            continue
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(md_text)
        seg = md_text[start:end]
        nm = _MD_SHEET_NAME_RE.search(seg)
        if nm:
            names[page_no] = nm.group(1).strip().strip("-* ")
    return names


def build_normalized_document_model(
    result_json_path: str | Path,
    *,
    document_md_path: str | Path | None = None,
    ocr_html_path: str | Path | None = None,
    pdf_path: str | Path | None = None,
) -> dict:
    """Построить ``normalized_document_model`` из подготовленного комплекта.

    Только локальное чтение (JSON + опционально MD/HTML), нормализация и
    диагностика. Никаких Qwen/Opus/OCR/PDF-render и сетевых вызовов.
    """
    norm = normalize_result_json(result_json_path)
    raw_blocks = norm["blocks"]
    raw_pages = norm["pages"]

    registry = build_block_registry(raw_blocks)
    # Соответствие block_id (из реестра) ↔ исходный block_id норм-блока:
    # дубликаты получили суффикс, поэтому идём по порядку реестра.
    models = list(registry.values())

    # Опциональные слои.
    md_text = _safe_read_text(document_md_path)
    ocr_html_text = _safe_read_text(ocr_html_path)
    md_sheet_names = _parse_md_sheet_names(md_text or "")
    md_pages_count = len(_MD_PAGE_HEADING_RE.findall(md_text or "")) if md_text else 0

    warnings: list[str] = []

    # Группировка моделей по странице (для page_type и summary).
    models_by_page: dict[int, list[dict]] = {}
    for m in models:
        models_by_page.setdefault(m["page_number"], []).append(m)

    pages_out: list[dict] = []
    by_page_type: dict[str, int] = {}
    for pg in raw_pages:
        pn = pg["page_number"]
        page_models = models_by_page.get(pn, [])
        # MD-fallback для имени листа.
        sheet_name = pg.get("sheet_name") or md_sheet_names.get(pn, "")
        page_for_classify = dict(pg)
        page_for_classify["sheet_name"] = sheet_name
        page_type = classify_page_type(page_for_classify, page_models)
        by_page_type[page_type] = by_page_type.get(page_type, 0) + 1

        if not page_models:
            warnings.append(f"page_without_blocks: page {pn}")
        if not isinstance(pn, int) or pn < 1:
            warnings.append(f"strange_page_number: {pn!r}")

        pages_out.append({
            "page_number": pn,
            "page_index": pg.get("page_index"),
            "width": pg.get("width", 0),
            "height": pg.get("height", 0),
            "sheet_number": pg.get("sheet_number", ""),
            "total_sheets": pg.get("total_sheets", ""),
            "sheet_name": sheet_name,
            "document_code": pg.get("document_code", ""),
            "page_type": page_type,
            "blocks": [m["block_id"] for m in page_models],
        })

    # Document-level сводка по штампам.
    stamp_summary = extract_document_stamp_summary(raw_blocks)
    # title: имя титульной страницы, иначе sheet_name первой страницы.
    title = ""
    for p in pages_out:
        if p["page_type"] == "title" and p["sheet_name"]:
            title = p["sheet_name"]
            break
    if not title and pages_out:
        title = pages_out[0].get("sheet_name") or ""

    # ─── summary counters ───
    by_block_type: dict[str, int] = {}
    by_semantic_type: dict[str, int] = {}
    image_total = image_with_crop = image_with_file = 0
    text_total = stamp_total = table_total = scheme_total = 0
    for m in models:
        bt = m["block_type"]
        st = m["semantic_type"]
        by_block_type[bt] = by_block_type.get(bt, 0) + 1
        by_semantic_type[st] = by_semantic_type.get(st, 0) + 1
        if bt == "image":
            image_total += 1
            if m["has_crop_pdf"]:
                image_with_crop += 1
            if m["has_image_file"]:
                image_with_file += 1
        if bt == "text":
            text_total += 1
        if st == "stamp":
            stamp_total += 1
        if st == "table" or bt == "table":
            table_total += 1
        if st in ("scheme", "large_scheme"):
            scheme_total += 1

    # ─── document-level warnings ───
    if not norm["ok"]:
        warnings.append("result_json_unreadable_or_empty")
    if not raw_pages:
        warnings.append("no_pages_in_result_json")
    if not any(stamp_summary.values()):
        warnings.append("document_stamp_summary_empty")
    if document_md_path and md_text is None:
        warnings.append("document_md_path_unreadable")
    if ocr_html_path and ocr_html_text is None:
        warnings.append("ocr_html_path_unreadable")
    if md_pages_count and raw_pages and md_pages_count != len(raw_pages):
        warnings.append(
            f"md_page_count_mismatch: md={md_pages_count} result_json={len(raw_pages)}")
    unknown_bt = by_block_type.get("unknown", 0)
    if unknown_bt:
        warnings.append(f"unknown_block_type_count: {unknown_bt}")

    model = {
        "version": MODEL_VERSION,
        "kind": MODEL_KIND,
        "source": {
            "pdf_path": _clean_str(pdf_path) or norm.get("source_pdf_path") or None,
            "result_json_path": _clean_str(result_json_path) or None,
            "document_md_path": _clean_str(document_md_path) or None,
            "ocr_html_path": _clean_str(ocr_html_path) or None,
        },
        "document": {
            "document_code": stamp_summary["document_code"],
            "project_name": stamp_summary["project_name"],
            "stage": stamp_summary["stage"],
            "organization": stamp_summary["organization"],
            "title": title,
            "pages_total": len(pages_out),
        },
        "summary": {
            "pages_total": len(pages_out),
            "blocks_total": len(models),
            "by_block_type": by_block_type,
            "by_semantic_type": by_semantic_type,
            "by_page_type": by_page_type,
            "image_blocks_total": image_total,
            "image_blocks_with_crop_url": image_with_crop,
            "image_blocks_with_image_file": image_with_file,
            "text_blocks_total": text_total,
            "stamp_blocks_total": stamp_total,
            "table_blocks_total": table_total,
            "scheme_blocks_total": scheme_total,
            "warnings_count": len(warnings),
            "md_pages_detected": md_pages_count,
        },
        "pages": pages_out,
        "blocks": registry,
        "warnings": warnings,
    }
    return model


# ─── 7. write_normalized_document_model (атомарная запись) ───────────────────


def write_normalized_document_model(out_path: str | Path, model: dict) -> Path:
    """Атомарно записать модель в JSON-файл и вернуть путь.

    Запись идёт во временный файл в том же каталоге и затем ``os.replace`` —
    читатель никогда не видит частично записанный JSON.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(model, ensure_ascii=False, indent=2)
    fd, tmp_name = tempfile.mkstemp(
        prefix=out_path.name + ".", suffix=".tmp", dir=str(out_path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, out_path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
    return out_path


__all__ = [
    "MODEL_VERSION",
    "MODEL_KIND",
    "normalize_result_json",
    "classify_page_type",
    "classify_block_semantic_type",
    "extract_document_stamp_summary",
    "build_block_registry",
    "build_normalized_document_model",
    "write_normalized_document_model",
]
