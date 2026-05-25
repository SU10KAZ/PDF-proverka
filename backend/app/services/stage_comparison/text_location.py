"""Резолвер «где в PDF находится текстовое изменение, найденное Claude».

Вход — pair (см. store.get_session) и one `change` из text_llm_diff.json. Выход —
страницы (`left_page`/`right_page`), номер alignment-slot и метод/уверенность
матчинга.

Алгоритм (по убыванию приоритета):

  1. **md_page_marker** — ищем цитату evidence из change в MD-файле стороны и
     берём ближайший выше маркер `## СТРАНИЦА N` (Chandra OCR пишет именно
     так). Это самый надёжный путь — Chandra гарантирует один маркер
     на страницу PDF.
  2. **heading_match** — если цитаты не нашли, пытаемся опереться на
     `evidence.section` (например `Содержание тома`) и ищем такой заголовок
     в MD. Низкая уверенность.
  3. **result_text_block** — fallback на result.json (Chandra blocks).
     Не реализован: нужен полный OCR-текст по блокам, который сейчас в
     `blocks.py` не нормализуется. Оставлено как точка расширения.
  4. **not_found** — ни одна стратегия не сработала. finding всё равно
     валидный; page/slot будут None.

Уверенность:
  - 1.0 — обе стороны (left/right) нашли страницу + одинаковый alignment_slot.
  - 0.7 — нашли страницу с одной стороны, slot есть.
  - 0.5 — нашли страницу с одной стороны, slot не определён.
  - 0.3 — heading_match.
  - 0.0 — not_found.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


# Chandra OCR пишет страницы в MD как `## СТРАНИЦА N` (см. реальные
# документы в comparison_sources/). На случай вариаций — допускаем
# числовые маркеры в кириллице/латинице.
_PAGE_MARKER_RE = re.compile(
    r"^\s*##\s+(?:СТРАНИЦА|СТРАНИЦА|PAGE|СТР\.?|Лист)\s+(\d+)\b",
    re.IGNORECASE,
)
# Короткие маркеры внутри страницы (`**Лист:** N`) тоже учитываем для
# heuristic — Chandra ставит их под `## СТРАНИЦА`, чтобы привязать
# номер штампа к PDF-странице. На сам page_no они не влияют, но
# нам они полезны как доп. подсказка.
_SHEET_LINE_RE = re.compile(
    r"^\s*\*\*\s*Лист\s*:\*\*\s+(\d+)",
    re.IGNORECASE,
)


def _normalize_quote(s: str) -> str:
    """Срезаем кавычки/мусор и нормализуем whitespace, чтобы матчить устойчивее.

    LLM любит обрамлять цитаты «французскими» / прямыми кавычками; иногда
    добавляет хвостовое многоточие. Мы это убираем.
    """
    if not s:
        return ""
    s = s.strip()
    # «…» / "…" / "…"
    for ch in ('«', '»', '"', '“', '”', '‘', '’', "'"):
        s = s.replace(ch, "")
    s = s.replace("…", "")
    # Хвост `...`
    s = re.sub(r"\.{3,}\s*$", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _build_page_index(md_text: str) -> list[tuple[int, int]]:
    """Список `[(line_idx, page_no), ...]` для каждого `## СТРАНИЦА N` маркера.

    Сортирован по line_idx. Возвращает пустой список, если маркеров нет.
    """
    pages: list[tuple[int, int]] = []
    for idx, line in enumerate(md_text.splitlines()):
        m = _PAGE_MARKER_RE.match(line)
        if not m:
            continue
        try:
            pages.append((idx, int(m.group(1))))
        except (TypeError, ValueError):
            continue
    return pages


def _find_page_for_line(pages: list[tuple[int, int]], line_idx: int) -> Optional[int]:
    """Найти page_no для строки line_idx — ближайший выше `## СТРАНИЦА`."""
    if not pages:
        return None
    last = None
    for li, pno in pages:
        if li <= line_idx:
            last = pno
        else:
            break
    return last


def _find_quote_in_md(md_text: str, quote: str) -> Optional[int]:
    """Найти подстроку quote в md_text. Вернуть line_idx первого вхождения.

    Сначала пробуем точное совпадение, затем — нормализованное (без кавычек,
    одиночные пробелы). Возвращает None, если не найдено.
    """
    if not quote or not md_text:
        return None
    # Точное совпадение
    pos = md_text.find(quote)
    if pos >= 0:
        return md_text.count("\n", 0, pos)
    # Нормализованное
    norm_quote = _normalize_quote(quote)
    if not norm_quote or len(norm_quote) < 8:
        # Слишком короткое — пропускаем чтобы не получать ложных матчей
        return None
    # Прогоняем построчно: дешевле чем нормализовать весь md_text
    for idx, line in enumerate(md_text.splitlines()):
        if not line.strip():
            continue
        norm_line = _normalize_quote(line)
        if norm_line and norm_quote in norm_line:
            return idx
    return None


def _find_heading_in_md(md_text: str, heading: str) -> Optional[int]:
    """Найти строку, содержащую `heading` (заголовок раздела)."""
    if not heading or not md_text:
        return None
    heading_norm = _normalize_quote(heading)
    if not heading_norm:
        return None
    for idx, line in enumerate(md_text.splitlines()):
        if not line.startswith("#"):
            continue
        if heading_norm in _normalize_quote(line):
            return idx
    return None


def _slot_for_pages(alignment_items: list[dict], left_page: Optional[int],
                    right_page: Optional[int]) -> Optional[int]:
    """Найти slot в alignment_items, у которого совпали обе стороны.

    Если обе стороны заданы — нужен exact match. Если одна сторона None —
    ищем по той, что есть.
    """
    if not alignment_items:
        return None
    if left_page is None and right_page is None:
        return None
    # Сначала пробуем точное совпадение обеих сторон.
    if left_page is not None and right_page is not None:
        for it in alignment_items:
            if it.get("left_page") == left_page and it.get("right_page") == right_page:
                slot = it.get("slot")
                return int(slot) if isinstance(slot, (int, float)) else None
    # Иначе по одной стороне.
    for it in alignment_items:
        if left_page is not None and it.get("left_page") == left_page:
            slot = it.get("slot")
            return int(slot) if isinstance(slot, (int, float)) else None
        if right_page is not None and it.get("right_page") == right_page:
            slot = it.get("slot")
            return int(slot) if isinstance(slot, (int, float)) else None
    return None


def _read_md(md_path: Optional[str]) -> str:
    if not md_path:
        return ""
    try:
        return Path(md_path).read_text(encoding="utf-8", errors="replace")
    except (OSError, UnicodeError):
        return ""


def _extract_quote(change: dict, side: str) -> str:
    """Достать quote/old_value/new_value для нужной стороны."""
    if side == "left":
        ev = change.get("evidence_left") if isinstance(change, dict) else None
        if isinstance(ev, dict) and ev.get("quote"):
            return str(ev.get("quote"))
        return str(change.get("old_value") or "")
    ev = change.get("evidence_right") if isinstance(change, dict) else None
    if isinstance(ev, dict) and ev.get("quote"):
        return str(ev.get("quote"))
    return str(change.get("new_value") or "")


def _extract_section(change: dict, side: str) -> str:
    ev_key = "evidence_left" if side == "left" else "evidence_right"
    ev = change.get(ev_key) if isinstance(change, dict) else None
    if isinstance(ev, dict):
        return str(ev.get("section") or "")
    return ""


def resolve_text_change_location(
    pair: dict,
    change: dict,
    *,
    alignment_items: Optional[list[dict]] = None,
) -> dict:
    """Резолвить местоположение текстового изменения в PDF.

    Параметры:
        pair: запись пары из session (нужны `left.md_path`, `right.md_path`).
        change: одна запись из `text_llm_diff.json["changes"]`.
        alignment_items: items из page_alignment.json (опционально). Без них
            slot не вычисляется, но page_no мы всё равно возвращаем.

    Возвращает dict со схемой:
        {
            "left_page":   int | None,
            "right_page":  int | None,
            "alignment_slot": int | None,
            "confidence":  float (0.0–1.0),
            "method":      "md_page_marker" | "heading_match" | "not_found",
        }
    """
    left_pair = pair.get("left") if isinstance(pair, dict) else {}
    right_pair = pair.get("right") if isinstance(pair, dict) else {}
    left_md_path = (left_pair or {}).get("md_path") if isinstance(left_pair, dict) else None
    right_md_path = (right_pair or {}).get("md_path") if isinstance(right_pair, dict) else None

    left_md = _read_md(left_md_path)
    right_md = _read_md(right_md_path)

    left_pages = _build_page_index(left_md) if left_md else []
    right_pages = _build_page_index(right_md) if right_md else []

    method = "not_found"
    left_page: Optional[int] = None
    right_page: Optional[int] = None

    # Шаг 1: матчим evidence quote (или old_value/new_value) в MD.
    left_quote = _extract_quote(change, "left")
    right_quote = _extract_quote(change, "right")

    if left_md and left_quote:
        li = _find_quote_in_md(left_md, left_quote)
        if li is not None:
            left_page = _find_page_for_line(left_pages, li)
            if left_page is not None:
                method = "md_page_marker"
    if right_md and right_quote:
        li = _find_quote_in_md(right_md, right_quote)
        if li is not None:
            right_page = _find_page_for_line(right_pages, li)
            if right_page is not None:
                method = "md_page_marker"

    # Шаг 2: heading fallback, если quote не помог.
    if method == "not_found":
        left_section = _extract_section(change, "left")
        right_section = _extract_section(change, "right")
        if left_md and left_section:
            li = _find_heading_in_md(left_md, left_section)
            if li is not None:
                left_page = _find_page_for_line(left_pages, li)
                if left_page is not None:
                    method = "heading_match"
        if right_md and right_section:
            li = _find_heading_in_md(right_md, right_section)
            if li is not None:
                right_page = _find_page_for_line(right_pages, li)
                if right_page is not None:
                    method = "heading_match"

    # Шаг 3: alignment_slot из page_alignment, если есть.
    alignment_slot: Optional[int] = None
    if alignment_items:
        alignment_slot = _slot_for_pages(alignment_items, left_page, right_page)

    # Уверенность.
    if method == "md_page_marker":
        if left_page is not None and right_page is not None and alignment_slot is not None:
            confidence = 1.0
        elif (left_page is not None or right_page is not None) and alignment_slot is not None:
            confidence = 0.7
        elif left_page is not None or right_page is not None:
            confidence = 0.5
        else:
            confidence = 0.0
            method = "not_found"
    elif method == "heading_match":
        confidence = 0.3
    else:
        confidence = 0.0

    return {
        "left_page": left_page,
        "right_page": right_page,
        "alignment_slot": alignment_slot,
        "confidence": round(confidence, 2),
        "method": method,
    }


__all__ = ["resolve_text_change_location"]
