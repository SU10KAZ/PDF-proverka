"""Структурный классификатор региона текстового фрагмента: штамп — только по доказательству.

До этого модуля план проверки объявлял «штампом» всё, чей центр лежал правее
x ≥ 0,72.  На реальном корпусе из 468 таких строк словарь штампа содержали 10:
остальное — экспликации помещений, таблицы оборудования и легенды у правого
края листа, спрятанные в сворачиваемый раздел «оформление» вместе с 31
доказанным изменением проекта.

Координата сама по себе штампа не даёт.  Доказательствами считаются только:

* прямой bbox фрагмента внутри нативного блока PDF из зоны штампа (тот же
  читатель, что и у ``sheet_identity``: поворот страницы учтён), в котором
  распознана идентификация листа или словарь полей штампа;
* проверенный словарь полей штампа в самом фрагменте (ячейка «Изм.», «Лист»,
  «Дата», «ГИП Иванов» — не «с изм. 1» внутри ссылки на СП) вместе с прямым
  bbox в нижней полосе, если нативный слой страницы прочитать нельзя;
* структура Markdown: блок-таблица с шапкой/заголовком (экспликация, таблица
  оборудования, просто таблица), заголовок раздела над абзацем.

Ни одно правило не обращается к модели.
"""
from __future__ import annotations

import re
from typing import Any, Callable, Iterable, Mapping

from . import sheet_identity

CLASSIFIER_VERSION = "text-region-classifier.v1"

STAMP = "STAMP"
TITLE_BLOCK = "TITLE_BLOCK"
EXPLICATION = "EXPLICATION"
EQUIPMENT_TABLE = "EQUIPMENT_TABLE"
TABLE = "TABLE"
TEXT_SECTION = "TEXT_SECTION"
OTHER = "OTHER"
UNKNOWN = "UNKNOWN"
STRUCTURES = (STAMP, TITLE_BLOCK, EXPLICATION, EQUIPMENT_TABLE, TABLE, TEXT_SECTION, OTHER, UNKNOWN)

#: Подписи полей штампа по ГОСТ Р 21.101 — как САМОСТОЯТЕЛЬНЫЕ подписи ячейки
#: (возможно с именем/датой после них), а не как слово внутри предложения.
_STAMP_FIELD_LABEL = (
    r"(?:гип|разраб\.?|разработал|проверил|н\.?\s?контр\.?|нач\.?\s?отд\.?|"
    r"утв\.?|утвердил|согласовано|изм\.?|кол\.?\s?уч\.?|№\s?док\.?|подп\.?|подпись|дата|"
    r"стадия|лист(?:ов)?|формат|взам\.?\s?инв\.?\s?№?|инв\.?\s?№\s?подл\.?|подп\.?\s?и\s?дата)"
)
_STAMP_CELL_RE = re.compile(
    rf"^\s*{_STAMP_FIELD_LABEL}(?:\s+[A-ZА-ЯЁ][^|]{{0,40}})?\s*$",
    re.IGNORECASE,
)
_STAMP_LINE_RE = re.compile(
    rf"^\s*(?:{_STAMP_FIELD_LABEL}\s*){{2,}}[A-ZА-ЯЁ0-9./ -]{{0,40}}$",
    re.IGNORECASE,
)
_ADMIN_HINT_RE = re.compile(
    r"(?:^|\b)(?:гип|разраб\.?|проверил|н\.?\s*контр\.?|формат|лист|подп\.?|дата|"
    r"изм\.?|кол\.?\s*уч\.?)(?:\b|$)",
    re.IGNORECASE,
)
_NUMBER_CELL_RE = re.compile(r"^[+-]?\d+(?:[.,]\d+)?(?:\s*%)?$")
_EXPLICATION_TITLE_RE = re.compile(r"эксплика", re.IGNORECASE)
_ROOM_HEADER_RE = re.compile(r"помещен", re.IGNORECASE)
_EQUIPMENT_HEADER_RE = re.compile(
    r"наименован|марк|тип|масса|кол-?во|количеств|размер|мощност|обозначен|"
    r"характеристик|давлен|расход|производител",
    re.IGNORECASE,
)
_EQUIPMENT_TITLE_RE = re.compile(r"оборудован|спецификац|ведомост|перечень", re.IGNORECASE)
_KEY_VALUE_RE = re.compile(r"^\s*[A-Za-zА-Яа-яЁё][^:=]{1,60}?\s*[:=]\s*\S")


def _cells(fragment: Mapping[str, Any]) -> list[str]:
    parts = [str(value) for value in fragment.get("location_parts") or [] if str(value).strip()]
    return parts or [str(fragment.get("text") or "")]


def has_stamp_field_vocabulary(fragment: Mapping[str, Any]) -> bool:
    """Проверенный словарь: подпись поля штампа занимает ячейку или всю строку."""
    text = " ".join(str(fragment.get("text") or "").split())
    if not text:
        return False
    if fragment.get("source_kind") == "table_row":
        cells = _cells(fragment)
        if any(_STAMP_CELL_RE.match(cell) for cell in cells):
            return True
    if _STAMP_CELL_RE.match(text) or _STAMP_LINE_RE.match(text):
        return True
    return False


def has_administrative_hint(text: Any) -> bool:
    """Мягкий словарь прежнего правила — оставлен для подписей ячеек таблиц."""
    return bool(_ADMIN_HINT_RE.search(str(text or "")))


def fragment_center(fragment: Mapping[str, Any]) -> tuple[float, float] | None:
    boxes = [box for box in fragment.get("bboxes") or () if isinstance(box, Mapping)]
    if not boxes:
        return None
    xs = [float(box.get("x") or 0) + float(box.get("width") or 0) / 2 for box in boxes]
    ys = [float(box.get("y") or 0) + float(box.get("height") or 0) / 2 for box in boxes]
    return sum(xs) / len(xs), sum(ys) / len(ys)


# ── Нативная зона штампа ────────────────────────────────────────────────────

def read_stamp_zone_blocks(pdf_path: str, page_number: int) -> list[dict[str, Any]]:
    """Нативные блоки PDF в зоне штампа отображаемой страницы (поворот учтён).

    Возвращает пустой список, если PDF нет или PyMuPDF недоступен: отсутствие
    нативного слоя — это отсутствие доказательства, а не штамп.
    """
    try:
        import fitz  # noqa: WPS433 — lazily: classifier must stay importable without PyMuPDF
    except ImportError:
        return []
    try:
        document = fitz.open(pdf_path)
    except Exception:  # noqa: BLE001 — unreadable PDF is «no evidence»
        return []
    try:
        if page_number < 1 or page_number > document.page_count:
            return []
        page = document[page_number - 1]
        width = float(page.rect.width) or 1.0
        height = float(page.rect.height) or 1.0
        output: list[dict[str, Any]] = []
        for block in page.get_text("blocks"):
            rect = fitz.Rect(block[:4]) * page.rotation_matrix
            rect.normalize()
            x0, y0 = float(rect.x0) / width, float(rect.y0) / height
            x1, y1 = float(rect.x1) / width, float(rect.y1) / height
            if y0 < sheet_identity.STAMP_ZONE_MIN_Y0 or x1 < sheet_identity.STAMP_ZONE_MIN_X1:
                continue
            text = sheet_identity._normalize_text(block[4])
            if not text:
                continue
            identity = sheet_identity.parse_stamp_title(text, page=page_number)
            output.append({
                "x0": round(x0, 4), "y0": round(y0, 4), "x1": round(x1, 4), "y1": round(y1, 4),
                "identity": identity is not None,
                "vocabulary": bool(_ADMIN_HINT_RE.search(text)) or bool(sheet_identity._parse_designation(text)),
                "text": text[:120],
                "page_rotation": int(page.rotation),
            })
        return output
    finally:
        document.close()


StampZoneIndex = dict[tuple[str, int], list[dict[str, Any]]]


def build_stamp_zone_index(
    text_preparation: Mapping[str, Any] | None,
    pages: Iterable[tuple[str, int]],
    *,
    reader: Callable[[str, int], list[dict[str, Any]]] = read_stamp_zone_blocks,
) -> StampZoneIndex:
    """Индекс {(SIDE, page): блоки} для перечисленных страниц; без PDF — пусто."""
    documents = (text_preparation or {}).get("documents") or {}
    index: StampZoneIndex = {}
    for side, page in sorted({(str(side).upper(), int(page)) for side, page in pages}):
        document = documents.get(side) if isinstance(documents, Mapping) else None
        pdf = (document or {}).get("pdf") if isinstance(document, Mapping) else None
        path = pdf.get("path") if isinstance(pdf, Mapping) else None
        if not path:
            continue
        index[(side, page)] = reader(str(path), page)
    return index


def _blocks_containing(center: tuple[float, float], blocks: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    x, y = center
    return [
        block for block in blocks
        if float(block.get("x0", 1)) - 0.005 <= x <= float(block.get("x1", 0)) + 0.005
        and float(block.get("y0", 1)) - 0.005 <= y <= float(block.get("y1", 0)) + 0.005
    ]


# ── Табличная структура блока Markdown ──────────────────────────────────────

def table_context(
    fragment: Mapping[str, Any],
    group_rows: list[Mapping[str, Any]],
    block_fragments: list[Mapping[str, Any]],
) -> dict[str, Any]:
    """Шапка, заголовок и повтор ширины для строки таблицы (по блоку, не по близости)."""
    cells = _cells(fragment)
    rows = sorted(group_rows, key=lambda value: (int(value.get("pdf_page") or 0), int(value.get("order") or 0)))
    first = rows[0] if rows else None
    header = None
    if first is not None and str(first.get("id")) != str(fragment.get("id")):
        head_cells = _cells(first)
        data_rows = [
            row for row in rows[1:]
            if len(_cells(row)) == len(head_cells) and any(_NUMBER_CELL_RE.match(cell.strip()) for cell in _cells(row))
        ]
        if (
            len(head_cells) >= 2
            and not any(_NUMBER_CELL_RE.match(cell.strip()) for cell in head_cells)
            and all(len(re.sub(r"[^a-zа-яё]", "", cell.casefold())) >= 2 for cell in head_cells)
            and len(data_rows) >= 3
            and len(head_cells) == len(cells)
        ):
            header = first
    title = None
    ordered = sorted(block_fragments, key=lambda value: (int(value.get("pdf_page") or 0), int(value.get("order") or 0)))
    first_row_position = next((index for index, value in enumerate(ordered) if value.get("source_kind") == "table_row"), None)
    if first_row_position is not None:
        for value in reversed(ordered[:first_row_position]):
            if value.get("source_kind") in ("paragraph", "heading"):
                title = str(value.get("text") or "")
                break
    same_width = sum(1 for row in rows if len(_cells(row)) == len(cells))
    return {
        "cells": cells,
        "header": header,
        "header_cells": _cells(header) if header is not None else None,
        "title": title,
        "rows": len(rows),
        "rows_same_width": same_width,
        "table_proven": len(rows) >= 3 and same_width >= 3,
    }


# ── Классификация ───────────────────────────────────────────────────────────

def classify_fragment(
    fragment: Mapping[str, Any],
    *,
    stamp_blocks: Iterable[Mapping[str, Any]] | None,
    native_available: bool,
    table: Mapping[str, Any] | None = None,
    heading_above: str | None = None,
) -> dict[str, Any]:
    """Одна структура на фрагмент.  ``stamp_blocks`` — нативные блоки его страницы.

    ``native_available`` говорит, читался ли нативный слой страницы вообще:
    если да, то только он решает вопрос о штампе; если нет — допускается ярус
    «проверенный словарь + прямой bbox в нижней полосе», потому что сама
    подпись поля штампа есть структурное доказательство, а координата без неё
    доказательством не является.
    """
    center = fragment_center(fragment)
    evidence: list[str] = []
    vocabulary = has_stamp_field_vocabulary(fragment)
    if center is not None:
        hits = _blocks_containing(center, stamp_blocks or ())
        if any(block.get("identity") for block in hits):
            evidence.append("direct_bbox_inside_native_stamp_block_with_sheet_identity")
            if vocabulary:
                evidence.append("stamp_field_vocabulary")
                return _result(STAMP, evidence, center)
            return _result(TITLE_BLOCK, evidence, center)
        if vocabulary and any(block.get("vocabulary") for block in hits):
            evidence.extend(["direct_bbox_inside_native_stamp_block_with_vocabulary", "stamp_field_vocabulary"])
            return _result(STAMP, evidence, center)
        if (
            vocabulary and not native_available
            and center[1] >= sheet_identity.STAMP_ZONE_MIN_Y0
            and center[0] >= sheet_identity.STAMP_ZONE_MIN_X1
        ):
            evidence.extend(["stamp_field_vocabulary", "direct_bbox_in_stamp_strip", "native_layer_unavailable"])
            return _result(STAMP, evidence, center)
    kind = str(fragment.get("source_kind") or "")
    if kind == "table_row" and table is not None:
        header_text = " ".join(table.get("header_cells") or [])
        title = str(table.get("title") or "")
        if _EXPLICATION_TITLE_RE.search(title) or (_ROOM_HEADER_RE.search(header_text) and "площад" in header_text.casefold()):
            evidence.append("explication_title_or_room_header")
            return _result(EXPLICATION, evidence, center)
        if table.get("header") is not None and (_EQUIPMENT_HEADER_RE.search(header_text) or _EQUIPMENT_TITLE_RE.search(title)):
            evidence.append("table_header_labels")
            return _result(EQUIPMENT_TABLE, evidence, center)
        if table.get("table_proven"):
            evidence.append(f"markdown_table_block_rows_same_width={table.get('rows_same_width')}")
            return _result(TABLE, evidence, center)
        evidence.append("table_row_without_repeated_structure")
        return _result(OTHER, evidence, center)
    if kind == "heading":
        return _result(TEXT_SECTION, ["heading"], center)
    if heading_above:
        return _result(TEXT_SECTION, ["heading_above"], center)
    if _KEY_VALUE_RE.match(str(fragment.get("text") or "")):
        return _result(TEXT_SECTION, ["explicit_key_value"], center)
    if center is None:
        return _result(UNKNOWN, ["no_geometry_no_structure"], None)
    return _result(OTHER, ["free_text_no_structure"], center)


def _result(structure: str, evidence: list[str], center: tuple[float, float] | None) -> dict[str, Any]:
    return {
        "classifier": CLASSIFIER_VERSION,
        "structure": structure,
        "is_stamp": structure in (STAMP, TITLE_BLOCK),
        "evidence": list(evidence),
        "center_x": None if center is None else round(center[0], 6),
        "center_y": None if center is None else round(center[1], 6),
    }


__all__ = [
    "CLASSIFIER_VERSION",
    "STRUCTURES",
    "STAMP", "TITLE_BLOCK", "EXPLICATION", "EQUIPMENT_TABLE", "TABLE", "TEXT_SECTION", "OTHER", "UNKNOWN",
    "build_stamp_zone_index",
    "classify_fragment",
    "fragment_center",
    "has_administrative_hint",
    "has_stamp_field_vocabulary",
    "read_stamp_zone_blocks",
    "table_context",
]
