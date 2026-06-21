# -*- coding: utf-8 -*-
"""reserc.md #98 — единый модуль нормализации текста сравнения стадий.

Канонические функции с ЯВНЫМИ режимами вместо расходившихся копий ``_norm_text``:

  * :func:`norm_for_grounding` — NFKC + ё→е + collapse whitespace + strip + lower.
    База для grounding/дедупа (evidence-verification, merge-сигнатуры).
  * :func:`normalize_block_content` — :func:`strip_html` + снятие debug-префикса
    ``BLOCK: <id>`` + :func:`norm_for_grounding`. Для сравнения эквивалентности
    текстовых блоков, где ``ocr_text`` HTML-обёрнут (``<div data-bbox=…>``).
  * :func:`strip_html` — снять HTML-теги и декодировать сущности.
  * :func:`salient_numbers` — значимые числовые токены (сечения/номиналы),
    canonical ``,``→``.`` ``х``/``×``→``x``; ``min_len`` конфигурируем.

Почему модуль появился: один и тот же ``ocr_text`` (HTML-обёрнутый) сравнивался
разными нормализаторами — без strip_html слой давал ПРОТИВОПОЛОЖНЫЕ verdicts
относительно слоя со strip_html (исторически reserc.md #60/#13). Единый источник
правды снимает этот класс расхождений.

Специализированные нормализаторы НАМЕРЕННО оставлены отдельными (см. их
комментарии-указатели): ``block_equivalence_precheck.canonicalize_text`` — строгий
режим (регистр и переводы строк сохраняются), ``v2_review._norm_text`` — varargs
для marker-substring матчинга, ``large_sheet_feeder_matching._norm_text`` —
доменная транслитерация board-токенов + пунктуация→пробел,
``enriched_comparison._salient_numbers`` — числовой grounding с min-len=3.
"""
from __future__ import annotations

import html
import re
import unicodedata

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_BLOCK_PREFIX_RE = re.compile(r"(?im)^\s*block\s*[:#]?\s*[0-9a-z_\-]+\s*")
_WS_RUN = re.compile(r"\s+")
_NUM_TOKEN_RE = re.compile(r"\d+(?:[.,]\d+)*(?:\s*[xх×]\s*\d+(?:[.,]\d+)*)*")


def strip_html(text: str | None) -> str:
    """``<div data-bbox=...>...</div>`` → внутренний текст. Координаты bbox в
    атрибутах — форматный шум, не контент."""
    if not text:
        return ""
    no_tags = _HTML_TAG_RE.sub(" ", str(text))
    return html.unescape(no_tags)


def norm_for_grounding(s: object) -> str:
    """NFKC + ё→е + collapse whitespace + strip + lower.

    Снимает форматный шум, СОХРАНЯЯ контент (числа/марки/даты значимы)."""
    if not s:
        return ""
    t = unicodedata.normalize("NFKC", str(s))
    t = t.replace("ё", "е").replace("Ё", "Е")
    t = _WS_RUN.sub(" ", t)
    return t.strip().lower()


def normalize_block_content(text: str | None) -> str:
    """:func:`strip_html` + снять префикс ``BLOCK: <id>`` + :func:`norm_for_grounding`."""
    if not text:
        return ""
    t = strip_html(str(text))
    t = _BLOCK_PREFIX_RE.sub(" ", t)
    return norm_for_grounding(t)


def salient_numbers(text: str | None, *, min_len: int = 1) -> list[str]:
    """Нормализованные значимые числовые токены: ``,``→``.``, ``х``/``×``→``x``,
    пробелы внутри токена снимаются. ``min_len`` отсекает шум (номера пунктов)."""
    if not text:
        return []
    out: list[str] = []
    for m in _NUM_TOKEN_RE.findall(str(text)):
        tok = m.replace(",", ".").replace("х", "x").replace("×", "x")
        tok = _WS_RUN.sub("", tok)
        if len(tok) >= min_len:
            out.append(tok)
    return out
