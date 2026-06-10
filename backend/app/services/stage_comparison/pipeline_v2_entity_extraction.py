# -*- coding: utf-8 -*-
"""Pipeline V2 — Entity Extraction по matched blocks (этап 3, backend-only).

Третий слой нового режима сравнения стадий. Принимает:

  * ``normalized_document_model`` OLD (этап 1 —
    [pipeline_v2_prepared_ingest](pipeline_v2_prepared_ingest.py));
  * ``normalized_document_model`` NEW;
  * ``block_matching_report`` (этап 2 —
    [pipeline_v2_block_matching](pipeline_v2_block_matching.py)),

и извлекает из блоков НОРМАЛИЗОВАННЫЕ СУЩНОСТИ: поля штампа, текстовые
требования, нормативные ссылки, оборудование, кабели, электропитание, строки
таблиц, строки справки изменений / содержания, компоненты схем и подсказки
связей.

Это ещё НЕ diff. Этап готовит сравнимые наборы сущностей для следующего этапа
(deterministic entity diff OLD↔NEW). Опираясь на блоки и листы, мы уходим от
«один большой Opus сжимает детализацию всего тома» — сущности заякорены на
конкретный блок/лист и сравниваются точечно.

Модуль НЕ ходит в сеть, НЕ скачивает ``crop_url``, НЕ вызывает Qwen/Opus/OCR/
PDF-render и НЕ создаёт findings. Только stdlib.

Все функции чистые, кроме ``write_entity_extraction_report`` (атомарная запись).

См. docs/stage_comparison_pipeline_v2_entity_extraction.md.
"""
from __future__ import annotations

import html
import json
import os
import re
import tempfile
import unicodedata
from collections import Counter
from pathlib import Path
from typing import Any, Optional

REPORT_VERSION = 1
REPORT_KIND = "stage_comparison_pipeline_v2_entity_extraction"

_VALUE_MAX = 240
_QUOTE_MAX = 240

# ─── Ключевые слова / паттерны (канон: lower + ё→е) ─────────────────────────

_REQUIREMENT_KW = (
    "должен", "должны", "должна", "должно", "предусматривается", "предусмотрен",
    "необходимо", "выполняется", "выполнить", "устанавливается", "установить",
    "прокладывается", "проложить",
)

_EQUIPMENT_KW = (
    "коммутатор", "шкаф", "видеорегистратор", "ибп", "камера", "контроллер",
    "считыватель", "вызывная панель", "арм", "кросс", "патч-панель", "патч панель",
)

# Кабели — паттерны (учитываем нг/FRLS/LS/HF/cat.5e и марки).
_CABLE_PATTERNS = [
    r"utp", r"ftp", r"кпсввнг(?:\([а-я]+\))?", r"кпсвв", r"lan",
    r"cat\.?\s?5e", r"cat\.?\s?6", r"frls", r"lsltx", r"ввгнг(?:\([а-я]+\))?",
    r"ввг", r"вок", r"\bнг\b", r"\bls\b", r"\bhf\b",
]
_CABLE_RE = re.compile("|".join(_CABLE_PATTERNS), re.IGNORECASE)

# Электропитание.
_POWER_VOLTAGE_RE = re.compile(r"\d+(?:[.,]\d+)?\s*в\b", re.IGNORECASE)
_POWER_CURRENT_RE = re.compile(r"\d+(?:[.,]\d+)?\s*а\b", re.IGNORECASE)
_POWER_UPS_RE = re.compile(r"\bибп\b", re.IGNORECASE)
_POWER_CATEGORY_RE = re.compile(r"\b[iv]+\s*-?\s*(?:я\s+)?категори\w*", re.IGNORECASE)

# Нормативные ссылки. Lookbehind отсекает совпадение внутри слова («ОСП-3»,
# «Аккорд-512», «способ»); дефис-формы «СП-1»/«РД-082» НЕ считаются нормами
# (марки оборудования/шифры) — у норм РФ номер пишется через пробел. Номер
# может начинаться с МЭК/ИСО/IEC/ISO («ГОСТ Р МЭК 61140-2000») или римской
# группы («СНиП II-12-77»).
_NORM_RE = re.compile(
    r"(?<![а-яёa-z0-9])(гост\s*р|гост|снип|сп|фз|пуэ|рд)\s*[№n]?\s*"
    r"((?:мэк|исо|iec|iso)\s*\d[\d.\-/]*"
    r"|[ivx]+[-–—]\d[\d.\-/]*"
    r"|\d[\d.\-/]*)?",
    re.IGNORECASE,
)
# Формы федеральных законов: «ФЗ-384» (дефис допустим ТОЛЬКО для ФЗ) и
# «384-ФЗ» / «№ 384-ФЗ» (номер ПЕРЕД ключевым словом).
_NORM_FZ_PREFIX_RE = re.compile(r"(?<![а-яёa-z0-9])фз\s*[-–—]\s*(\d+)\b",
                                re.IGNORECASE)
_NORM_FZ_SUFFIX_RE = re.compile(r"\b(\d+)\s*-\s*фз\b", re.IGNORECASE)
# Ключевые слова, валидные БЕЗ номера (ПУЭ — самостоятельная ссылка).
_NORM_STANDALONE_OK = {"пуэ"}

# Подсказки связей на схемах (без построения полноценного графа).
_CONNECTION_KW = (
    "подключается", "подключение", "подключен", "питание", "ethernet",
    "соединяется", "идет к", "идёт к",
)

# Заголовки разделов (document_section).
_SECTION_RE = re.compile(
    r"^(раздел|глава|часть|общие данные|общие указания|пояснительная записка|"
    r"текстовая часть)\b",
    re.IGNORECASE,
)


# ─── Текстовая нормализация / id ────────────────────────────────────────────

_WS_RE = re.compile(r"\s+")

# HTML-разметка: ocr_text в result.json бывает HTML-обёрнут (теги + data-bbox/
# data-label атрибуты) — без стрипа разметка протекает в entity values.
_HTML_TAG_RE = re.compile(r"</?[a-zA-Z][^>]*>")
_HTML_CELL_SEP_RE = re.compile(r"</\s*t[dh]\s*>\s*<\s*t[dh][^>]*>", re.IGNORECASE)
_HTML_ROW_BREAK_RE = re.compile(
    r"</\s*tr\s*>|<\s*br\s*/?\s*>|</\s*(?:p|div|li|h[1-6])\s*>", re.IGNORECASE)

_INFORMATIVE_RE = re.compile(r"[0-9a-zа-яё]", re.IGNORECASE)


def strip_html_markup(value: Any) -> str:
    """Убрать HTML-разметку, сохранив текст и структуру строк/таблиц.

    ``</td><td>`` → `` | `` (ячейки → pipe-таблица), ``</tr>``/``<br>``/блочные
    закрытия → перенос строки, остальные теги (включая атрибуты ``data-bbox``/
    ``data-label``) — пробел; HTML-entities декодируются. Текст без тегов
    (включая markdown pipe-таблицы) возвращается без изменений.
    """
    s = "" if value is None else str(value)
    if "<" not in s or not _HTML_TAG_RE.search(s):
        return s
    s = _HTML_CELL_SEP_RE.sub(" | ", s)
    s = _HTML_ROW_BREAK_RE.sub("\n", s)
    s = _HTML_TAG_RE.sub(" ", s)
    # хвостовой обрезанный тег («…</t» от upstream-truncation excerpt'а до 600
    # символов) не имеет закрывающего «>» и не ловится _HTML_TAG_RE; якорим на
    # реальное начало тега «<буква»/«</», чтобы не съесть сравнение «t < 5 °C»
    s = re.sub(r"</?[a-zA-Z][^>]*\Z", "", s)
    s = html.unescape(s)
    lines = (_WS_RE.sub(" ", ln).strip() for ln in s.splitlines())
    return "\n".join(ln for ln in lines if ln)


def clean_entity_value(value: Any) -> str:
    """Очистить одиночное значение сущности: strip HTML + схлопнуть пробелы."""
    return _WS_RE.sub(" ", strip_html_markup(value)).strip()


def _is_informative(text: str) -> bool:
    """Есть ли в тексте хоть одна буква/цифра (фильтр пустых HTML-огрызков)."""
    return bool(_INFORMATIVE_RE.search(text or ""))


def normalize_entity_text(value: Any) -> str:
    """Канонизировать строку сущности: NFKC, lower, ё→е, схлопнуть пробелы."""
    s = "" if value is None else str(value)
    s = unicodedata.normalize("NFKC", s).lower().replace("ё", "е")
    return _WS_RE.sub(" ", s).strip()


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _safe_id(value: Any) -> str:
    s = _clean(value) or "x"
    return "".join(c if (c.isalnum() or c in "-_") else "_" for c in s)[:48]


def make_entity_id(side: str, block_id: Any, seq: int) -> str:
    """Детерминированный id сущности: ``ent_<side>_<block>_<seq>``."""
    sp = {"left": "l", "right": "r"}.get(side, "x")
    return f"ent_{sp}_{_safe_id(block_id)}_{seq:02d}"


def _truncate(text: str, limit: int) -> str:
    text = text or ""
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


# ─── Partial-entity конструктор ─────────────────────────────────────────────


def _p(entity_type: str, semantic_group: str, *, subject: str = "", name: str = "",
       value: str = "", unit: str = "", fields: Optional[dict] = None,
       confidence: float = 0.5, quote: str = "", source: str = "heuristic") -> dict:
    """Промежуточная сущность (без id/side/page — их добавит finalize)."""
    return {
        "entity_type": entity_type,
        "semantic_group": semantic_group,
        "subject": _truncate(_clean(subject), _VALUE_MAX),
        "name": _truncate(_clean(name), _VALUE_MAX),
        "value": _truncate(_clean(value), _VALUE_MAX),
        "unit": _clean(unit),
        "fields": fields or {},
        "confidence": round(float(confidence), 3),
        "_quote": _truncate(_clean(quote), _QUOTE_MAX),
        "_source": source,
    }


# ─── Источники текста блока ─────────────────────────────────────────────────


def _primary_text(block: dict) -> tuple[str, str]:
    """Главный текст блока + источник (text_excerpt > pdfplumber_text_excerpt).

    HTML-разметка снимается здесь, чтобы ВСЕ extractors ниже работали с чистым
    текстом; чисто-разметочный excerpt (теги без текста) считается пустым.
    """
    te = strip_html_markup(_clean(block.get("text_excerpt")))
    if te:
        return te, "text_excerpt"
    pp = strip_html_markup(_clean(block.get("pdfplumber_text_excerpt")))
    if pp:
        return pp, "pdfplumber_text"
    return "", "heuristic"


def _scheme_text_sources(block: dict) -> list[tuple[str, str]]:
    """Тексты схемного блока + источники (для сканеров)."""
    out: list[tuple[str, str]] = []
    summ = block.get("ocr_json_summary")
    if isinstance(summ, dict):
        if _clean(summ.get("content_summary")):
            out.append((strip_html_markup(_clean(summ["content_summary"])), "ocr_json"))
        if _clean(summ.get("detailed_description")):
            out.append((strip_html_markup(_clean(summ["detailed_description"])), "ocr_json"))
    te = strip_html_markup(_clean(block.get("text_excerpt")))
    if te:
        out.append((te, "text_excerpt"))
    pp = strip_html_markup(_clean(block.get("pdfplumber_text_excerpt")))
    if pp:
        out.append((pp, "pdfplumber_text"))
    return out


# ─── Сканеры (общие для text/scheme) ────────────────────────────────────────


def _scan_norms(text: str, source: str, stats: Optional[Counter] = None) -> list[dict]:
    out: list[dict] = []
    # формы ФЗ с дефисом извлекаются отдельными регэкспами; их спаны помнят,
    # чтобы вырожденный хвост «фз» из основного регэкспа не считался шумом
    fz_spans: list[tuple[int, int]] = []
    for rx in (_NORM_FZ_PREFIX_RE, _NORM_FZ_SUFFIX_RE):
        for m in rx.finditer(text):
            ref = f"ФЗ {m.group(1)}"
            out.append(_p("norm_reference", "text", name=ref, value=ref,
                          confidence=0.7, quote=m.group(0), source=source))
            fz_spans.append(m.span())
    for m in _NORM_RE.finditer(text):
        kw = _WS_RE.sub(" ", m.group(1)).strip()
        num = _clean(m.group(2))
        if not num:
            if normalize_entity_text(kw) in _NORM_STANDALONE_OK:
                pass  # ПУЭ валиден standalone
            elif any(s <= m.start() < e for s, e in fz_spans):
                continue  # часть формы «ФЗ-384»/«384-ФЗ» — уже извлечена выше
            else:
                # одиночное «СП»/«ГОСТ»/«ФЗ» без номера (часто кусок слова —
                # «способ», «спецификация») — это не ссылка на норму
                if stats is not None:
                    stats["degenerate_norm_reference_suppressed"] += 1
                continue
        ref = f"{kw} {num}".strip()
        out.append(_p("norm_reference", "text", name=ref, value=ref,
                      confidence=0.7, quote=m.group(0), source=source))
    return out


def _scan_requirements(text: str, source: str) -> list[dict]:
    out: list[dict] = []
    clauses = re.split(r"[.;\n]+", text)
    for cl in clauses:
        low = normalize_entity_text(cl)
        if not low:
            continue
        if any(kw in low for kw in _REQUIREMENT_KW):
            out.append(_p("requirement", "text", subject=cl.strip()[:60],
                          value=cl.strip(), confidence=0.6,
                          quote=cl.strip(), source=source))
    return out


def _scan_equipment(text: str, source: str) -> list[dict]:
    low = normalize_entity_text(text)
    out: list[dict] = []
    for kw in _EQUIPMENT_KW:
        if kw in low:
            out.append(_p("equipment", "equipment", name=kw, value=kw,
                          confidence=0.55, quote=kw, source=source))
    return out


def _canon_power(token: str) -> str:
    # «220 В» → «220В», «0,5 А» → «0,5А», «+12В» → «12В»
    return _WS_RE.sub("", token).strip().lstrip("+")


def _power_entities_from_token(tok: str, source: str) -> list[dict]:
    """power_supply-сущности из произвольного токена (схемный key_entity).

    Значения канонизируются до номиналов («Ввод ~220 В» → «220В»), unit
    выставляется как в ``_scan_power`` — иначе тот же номинал с двух сторон
    даёт мусорную unit-дельту ``'' → 'В'``. Составной токен («ИБП 220В»,
    «Ввод 220В, 16А») даёт ОТДЕЛЬНУЮ сущность на каждый факт — first-match
    схлопывание прятало реальные изменения номиналов. Токен без числа /
    ИБП / категории → пустой список (НЕ power_supply).
    """
    out: list[dict] = []
    if _POWER_UPS_RE.search(tok):
        out.append(_p("power_supply", "power", name="ИБП", value="ИБП",
                      confidence=0.6, quote=tok, source=source))
    for m in _POWER_CATEGORY_RE.finditer(tok):
        val = _WS_RE.sub(" ", m.group(0)).strip()
        out.append(_p("power_supply", "power", name=val, value=val,
                      confidence=0.55, quote=tok, source=source))
    for m in _POWER_VOLTAGE_RE.finditer(tok):
        val = _canon_power(m.group(0))
        out.append(_p("power_supply", "power", name=val, value=val, unit="В",
                      confidence=0.6, quote=tok, source=source))
    for m in _POWER_CURRENT_RE.finditer(tok):
        val = _canon_power(m.group(0))
        out.append(_p("power_supply", "power", name=val, value=val, unit="А",
                      confidence=0.6, quote=tok, source=source))
    return out


def _scan_cables(text: str, source: str) -> list[dict]:
    out: list[dict] = []
    for m in _CABLE_RE.finditer(text):
        tok = m.group(0).strip()
        out.append(_p("cable", "cable", name=tok, value=tok, confidence=0.6,
                      quote=tok, source=source))
    return out


def _scan_power(text: str, source: str) -> list[dict]:
    out: list[dict] = []
    for rx, unit in ((_POWER_VOLTAGE_RE, "В"), (_POWER_CURRENT_RE, "А")):
        for m in rx.finditer(text):
            val = _canon_power(m.group(0))
            out.append(_p("power_supply", "power", name=val, value=val, unit=unit,
                          confidence=0.6, quote=m.group(0), source=source))
    for m in _POWER_UPS_RE.finditer(text):
        out.append(_p("power_supply", "power", name="ИБП", value="ИБП",
                      confidence=0.6, quote=m.group(0), source=source))
    for m in _POWER_CATEGORY_RE.finditer(text):
        val = _WS_RE.sub(" ", m.group(0)).strip()
        out.append(_p("power_supply", "power", name=val, value=val,
                      confidence=0.55, quote=val, source=source))
    return out


def _scan_sections(text: str, source: str) -> list[dict]:
    out: list[dict] = []
    for line in text.splitlines():
        if _SECTION_RE.match(line.strip()):
            out.append(_p("document_section", "text", subject=line.strip()[:60],
                          name=line.strip(), value=line.strip(), confidence=0.6,
                          quote=line.strip(), source=source))
    return out


def _scan_connection_hints(text: str, source: str) -> list[dict]:
    low = normalize_entity_text(text)
    out: list[dict] = []
    for kw in _CONNECTION_KW:
        if kw in low:
            out.append(_p("scheme_connection_hint", "scheme", name=kw, value=kw,
                          confidence=0.45, quote=kw, source=source))
    return out


def _classify_scheme_token(token: str) -> tuple[str, str]:
    """Классифицировать key_entity-токен схемы → (entity_type, semantic_group)."""
    low = normalize_entity_text(token)
    if _CABLE_RE.search(low):
        return "cable", "cable"
    if (_POWER_VOLTAGE_RE.search(low) or _POWER_CURRENT_RE.search(low)
            or _POWER_UPS_RE.search(low) or _POWER_CATEGORY_RE.search(low)):
        return "power_supply", "power"
    if any(kw in low for kw in _EQUIPMENT_KW):
        return "equipment", "equipment"
    return "scheme_component", "scheme"


# ─── Per-type extractors ────────────────────────────────────────────────────


_STAMP_SCALAR_FIELDS = (
    "document_code", "project_name", "sheet_name", "stage", "sheet_number",
    "total_sheets", "organization",
)


def extract_stamp_entities(block: dict, page: Optional[dict] = None) -> list[dict]:
    """Отдельная stamp_field-сущность на каждое поле штампа."""
    sd = block.get("stamp_data") if isinstance(block.get("stamp_data"), dict) else {}
    out: list[dict] = []
    for f in _STAMP_SCALAR_FIELDS:
        if f in sd:
            val = _clean(sd.get(f))
            out.append(_p("stamp_field", "stamp", subject=f, name=f, value=val,
                          confidence=0.9 if val else 0.4, quote=val, source="stamp_data"))
    # подписи: role/surname/date
    sigs = sd.get("signatures")
    if isinstance(sigs, list):
        for sig in sigs:
            if not isinstance(sig, dict):
                continue
            role = _clean(sig.get("role") or sig.get("position"))
            surname = _clean(sig.get("surname") or sig.get("name"))
            date = _clean(sig.get("date"))
            val = " ".join(x for x in (role, surname, date) if x)
            if val:
                out.append(_p("stamp_field", "stamp", subject="signature", name=role,
                              value=val, fields={"role": role, "surname": surname,
                                                 "date": date},
                              confidence=0.7, quote=val, source="stamp_data"))
    # ревизии
    revs = sd.get("revisions")
    if isinstance(revs, list):
        for rev in revs:
            if isinstance(rev, dict):
                val = _clean(rev.get("description") or rev.get("note") or json.dumps(
                    rev, ensure_ascii=False))
            else:
                val = _clean(rev)
            if val:
                out.append(_p("stamp_field", "stamp", subject="revision", name="revision",
                              value=val, confidence=0.6, quote=val, source="stamp_data"))
    return out


def extract_text_entities(block: dict, page: Optional[dict] = None,
                          stats: Optional[Counter] = None) -> list[dict]:
    """document_section / requirement / norm_reference / equipment / cable / power."""
    text, source = _primary_text(block)
    if not text:
        return []
    out: list[dict] = []
    out += _scan_sections(text, source)
    out += _scan_norms(text, source, stats=stats)
    out += _scan_requirements(text, source)
    out += _scan_equipment(text, source)
    out += _scan_cables(text, source)
    out += _scan_power(text, source)
    return out


def _parse_table_rows(text: str) -> list[list[str]]:
    """Распарсить markdown/pipe-таблицу в список ячеек по строкам."""
    rows: list[list[str]] = []
    for line in text.splitlines():
        s = line.strip()
        if "|" not in s:
            continue
        if set(s) <= set("|-: "):  # строка-разделитель шапки
            continue
        cells = [c.strip() for c in s.strip("|").split("|")]
        if any(cells):
            rows.append(cells)
    return rows


def extract_table_entities(block: dict, page: Optional[dict] = None) -> list[dict]:
    """Каждая строка таблицы → table_row (массив ячеек + распознанные поля)."""
    text, source = _primary_text(block)
    rows = _parse_table_rows(text)
    out: list[dict] = []
    for cells in rows:
        joined = " | ".join(cells)
        fields: dict[str, Any] = {"cells": cells}
        for c in cells:
            if re.search(r"[A-ZА-Я]{1,4}[/\-]?\d", c) and "code" not in fields:
                fields["code"] = c
            if re.match(r"(?i)лист", c) and "sheet" not in fields:
                fields["sheet"] = c
        out.append(_p("table_row", "table", value=joined, fields=fields,
                      confidence=0.5, quote=joined, source="table"))
    return out


def _columns_map(header_cells: list[str], aliases: dict[str, tuple[str, ...]]) -> dict[int, str]:
    out: dict[int, str] = {}
    for idx, c in enumerate(header_cells):
        low = normalize_entity_text(c)
        for field, keys in aliases.items():
            if any(k in low for k in keys):
                out[idx] = field
                break
    return out


_CHANGE_LOG_HEADER_KW = ("изм", "лист", "содержание изменен", "содержание изменений",
                         "описание", "код", "примечан", "изменение")
# Внимание: «изм» — подстрока «изменений», поэтому для номера изменения берём
# «изм.»/«№», а описание ловим по «содержание»/«описание» (не по «изм»).
_CHANGE_LOG_ALIASES = {
    "change_no": ("изм.", "№ изм", "номер изм"),
    "sheet": ("лист",),
    "description": ("содержание", "описание"),
    "code": ("код",),
    "note": ("примечан",),
}


def extract_change_log_entities(block: dict, page: Optional[dict] = None) -> list[dict]:
    """Строки справки изменений → change_log_item."""
    text, source = _primary_text(block)
    if not text:
        return []
    out: list[dict] = []
    rows = _parse_table_rows(text)
    if rows:
        header_idx = None
        for i, cells in enumerate(rows):
            low = " ".join(normalize_entity_text(c) for c in cells)
            if any(k in low for k in _CHANGE_LOG_HEADER_KW):
                header_idx = i
                break
        colmap: dict[int, str] = {}
        start = 0
        if header_idx is not None:
            colmap = _columns_map(rows[header_idx], _CHANGE_LOG_ALIASES)
            start = header_idx + 1
        positional = ("change_no", "sheet", "description", "code", "note")
        for cells in rows[start:]:
            fields: dict[str, Any] = {}
            if colmap:
                for idx, c in enumerate(cells):
                    if idx in colmap:
                        fields[colmap[idx]] = c
            else:
                for idx, c in enumerate(cells):
                    if idx < len(positional):
                        fields[positional[idx]] = c
            desc = fields.get("description") or " | ".join(cells)
            out.append(_p("change_log_item", "change_log",
                          subject=f"Изм. {fields.get('change_no', '')}".strip(),
                          name=_clean(fields.get("change_no")), value=desc, fields=fields,
                          confidence=0.6, quote=" | ".join(cells), source="table"))
        return out
    # текстовый fallback: строки с маркерами «Изм.»/«Лист»
    for line in text.splitlines():
        s = line.strip()
        low = normalize_entity_text(s)
        if not s:
            continue
        if "изм" in low or "содержание изменен" in low:
            out.append(_p("change_log_item", "change_log", subject=s[:60], value=s,
                          confidence=0.5, quote=s, source="text_excerpt"))
    return out


_CONTENTS_ALIASES = {
    "document_code": ("обозначение", "шифр", "код"),
    "sheet_name": ("наименование", "название"),
    "page_or_note": ("стр", "лист", "примечан"),
}


def extract_contents_entities(block: dict, page: Optional[dict] = None) -> list[dict]:
    """Строки содержания тома → contents_item."""
    text, source = _primary_text(block)
    if not text:
        return []
    out: list[dict] = []
    rows = _parse_table_rows(text)
    if rows:
        header_idx = None
        for i, cells in enumerate(rows):
            low = " ".join(normalize_entity_text(c) for c in cells)
            if any(k in low for k in ("обозначение", "наименование", "содержание тома")):
                header_idx = i
                break
        colmap: dict[int, str] = {}
        start = 0
        if header_idx is not None:
            colmap = _columns_map(rows[header_idx], _CONTENTS_ALIASES)
            start = header_idx + 1
        positional = ("document_code", "sheet_name", "page_or_note")
        for cells in rows[start:]:
            fields: dict[str, Any] = {}
            if colmap:
                for idx, c in enumerate(cells):
                    if idx in colmap:
                        fields[colmap[idx]] = c
            else:
                for idx, c in enumerate(cells):
                    if idx < len(positional):
                        fields[positional[idx]] = c
            name = fields.get("sheet_name") or " | ".join(cells)
            out.append(_p("contents_item", "contents",
                          subject=_clean(fields.get("document_code")), name=name,
                          value=name, fields=fields, confidence=0.6,
                          quote=" | ".join(cells), source="table"))
        return out
    # текстовый fallback: непустые ИНФОРМАТИВНЫЕ строки (после HTML-стрипа
    # огрызки разметки/пунктуации не должны становиться contents_item)
    for line in text.splitlines():
        s = line.strip()
        if len(s) >= 3 and _is_informative(s):
            out.append(_p("contents_item", "contents", name=s, value=s,
                          confidence=0.45, quote=s, source="text_excerpt"))
    return out


def extract_scheme_entities(block: dict, page: Optional[dict] = None) -> list[dict]:
    """scheme_component / equipment / cable / power_supply / scheme_connection_hint."""
    out: list[dict] = []
    summ = block.get("ocr_json_summary") if isinstance(block.get("ocr_json_summary"), dict) else {}
    key_entities = summ.get("key_entities") if isinstance(summ.get("key_entities"), list) else []
    for ke in key_entities:
        tok = clean_entity_value(ke)
        if not tok:
            continue
        etype, group = _classify_scheme_token(tok)
        if etype == "power_supply":
            ents = _power_entities_from_token(tok, "ocr_json")
            # классификатор и хелпер используют одни регэкспы; fallback на
            # старое поведение — страховка от рассинхрона
            out.extend(ents if ents
                       else [_p(etype, group, name=tok, value=tok,
                                confidence=0.6, quote=tok, source="ocr_json")])
            continue
        out.append(_p(etype, group, name=tok, value=tok, confidence=0.6,
                      quote=tok, source="ocr_json"))
    # сканеры по всем текстам схемы (покрывают и блоки без key_entities)
    for text, source in _scheme_text_sources(block):
        out += _scan_equipment(text, source)
        out += _scan_cables(text, source)
        out += _scan_power(text, source)
        out += _scan_connection_hints(text, source)
    return out


# ─── Block-level quality flags (для группировки в отчёте) ───────────────────


def _block_extraction_flags(block: dict, page: Optional[dict] = None) -> list[str]:
    flags: list[str] = []
    st = block.get("semantic_type") or "unknown"
    if st in ("scheme", "large_scheme", "plan"):
        summ = block.get("ocr_json_summary")
        has_keys = isinstance(summ, dict) and bool(summ.get("key_entities"))
        if not has_keys:
            flags.append("scheme_without_key_entities")
        if not block.get("has_crop_pdf"):
            flags.append("scheme_without_crop")
    return flags


# ─── Dedup + finalize ───────────────────────────────────────────────────────


def _dedup_key(p: dict) -> tuple[str, str]:
    base = p.get("subject") or p.get("name") or p.get("value")
    return (p["entity_type"], normalize_entity_text(base))


def _entity_quality_flags(p: dict) -> list[str]:
    flags: list[str] = []
    if not p.get("_quote"):
        flags.append("empty_evidence")
    base = normalize_entity_text(p.get("value") or p.get("name") or p.get("subject"))
    if len(base) < 2:
        flags.append("low_information_entity")
    if p.get("_source") in ("text_excerpt", "pdfplumber_text"):
        flags.append("from_excerpt_only")
    if p["entity_type"] == "stamp_field" and not _clean(p.get("value")):
        flags.append("stamp_field_missing_value")
    # ocr-шум: значение без букв/цифр или из одной не-словесной литеры
    if base and not re.search(r"[0-9a-zа-я]", base):
        flags.append("possible_ocr_noise")
    return flags


def extract_entities_for_block(block: dict, page: Optional[dict] = None,
                               options: Optional[dict] = None) -> list[dict]:
    """Извлечь все сущности блока (dispatch + dedup + finalize)."""
    options = options or {}
    side = options.get("side", "unknown")
    page = page or {}
    page_type = _clean(page.get("page_type")) or "unknown"
    st = block.get("semantic_type") or "unknown"

    raw: list[dict] = []
    # page-type специфичные (change_log / contents) — для их text/table-блоков
    if page_type == "change_log" and st in ("text", "table", "legend", "unknown"):
        raw += extract_change_log_entities(block, page)
    elif page_type == "contents" and st in ("text", "table", "legend", "unknown"):
        raw += extract_contents_entities(block, page)
    # semantic-type экстракторы
    if st == "stamp":
        raw += extract_stamp_entities(block, page)
    elif st == "table":
        if page_type not in ("change_log", "contents"):
            raw += extract_table_entities(block, page)
    elif st in ("scheme", "large_scheme", "plan"):
        raw += extract_scheme_entities(block, page)
    elif st in ("text", "legend", "title", "unknown"):
        if page_type not in ("change_log", "contents"):
            raw += extract_text_entities(block, page, stats=options.get("_stats"))

    # dedup внутри блока
    deduped: list[dict] = []
    seen: dict[tuple[str, str], dict] = {}
    for p in raw:
        k = _dedup_key(p)
        if k in seen:
            kept = seen[k]
            if "duplicate_entity_suppressed" not in kept.setdefault("_extra_flags", []):
                kept["_extra_flags"].append("duplicate_entity_suppressed")
            continue
        seen[k] = p
        deduped.append(p)

    # finalize
    doc_code = _clean(page.get("document_code"))
    if not doc_code:
        sd = block.get("stamp_data") if isinstance(block.get("stamp_data"), dict) else {}
        doc_code = _clean(sd.get("document_code"))
    block_id = block.get("block_id")
    out: list[dict] = []
    for seq, p in enumerate(deduped):
        flags = _entity_quality_flags(p) + p.get("_extra_flags", [])
        out.append({
            "entity_id": make_entity_id(side, block_id, seq),
            "entity_type": p["entity_type"],
            "semantic_group": p["semantic_group"],
            "side": side,
            "document_code": doc_code,
            "page_number": block.get("page_number"),
            "page_type": page_type,
            "block_id": block_id,
            "block_semantic_type": st,
            "subject": p["subject"],
            "name": p["name"],
            "value": p["value"],
            "unit": p["unit"],
            "fields": p["fields"],
            "confidence": p["confidence"],
            "evidence": {
                "quote": p["_quote"],
                "source": p["_source"],
                "block_id": block_id,
                "page_number": block.get("page_number"),
            },
            "quality_flags": flags,
        })
    return out


# ─── Документ / matched documents ───────────────────────────────────────────


def _extract_side(model: dict, side: str,
                  options: Optional[dict]) -> tuple[list[dict], dict, Counter]:
    pages = {p.get("page_number"): p for p in (model.get("pages") or [])}
    entities: list[dict] = []
    by_block: dict[str, dict] = {}
    stats: Counter = Counter()
    for bid, block in (model.get("blocks") or {}).items():
        page = pages.get(block.get("page_number"))
        ents = extract_entities_for_block(
            block, page=page,
            options={**(options or {}), "side": side, "_stats": stats})
        by_block[bid] = {
            "page_number": block.get("page_number"),
            "semantic_type": block.get("semantic_type"),
            "entity_ids": [e["entity_id"] for e in ents],
            "flags": _block_extraction_flags(block, page),
        }
        entities.extend(ents)
    return entities, by_block, stats


def extract_document_entities(model: dict, options: Optional[dict] = None) -> list[dict]:
    """Извлечь сущности всех блоков ОДНОЙ модели (side из ``options``)."""
    side = (options or {}).get("side", "unknown")
    entities, _, _ = _extract_side(model or {}, side, options)
    return entities


def _entity_type_counts(ids: list[str], by_id: dict[str, dict]) -> dict[str, int]:
    c: Counter = Counter()
    for i in ids:
        e = by_id.get(i)
        if e:
            c[e["entity_type"]] += 1
    return dict(c)


def extract_entities_for_matched_documents(
        left_model: dict, right_model: dict, block_matching_report: dict,
        options: Optional[dict] = None) -> dict:
    """Полное извлечение сущностей с привязкой к block_matching_report."""
    left_model = left_model or {}
    right_model = right_model or {}
    report = block_matching_report or {}

    left_entities, left_by_block, left_stats = _extract_side(left_model, "left", options)
    right_entities, right_by_block, right_stats = _extract_side(right_model, "right", options)

    left_by_id = {e["entity_id"]: e for e in left_entities}
    right_by_id = {e["entity_id"]: e for e in right_entities}

    # matched block entities — по block_matches этапа 2
    matched_block_entities: list[dict] = []
    matched_left_bids: set = set()
    matched_right_bids: set = set()
    for bmatch in report.get("block_matches") or []:
        lbid = bmatch.get("left_block_id")
        rbid = bmatch.get("right_block_id")
        lentry = left_by_block.get(lbid, {})
        rentry = right_by_block.get(rbid, {})
        l_ids = lentry.get("entity_ids", [])
        r_ids = rentry.get("entity_ids", [])
        matched_left_bids.add(lbid)
        matched_right_bids.add(rbid)
        counts = _entity_type_counts(l_ids, left_by_id)
        for k, v in _entity_type_counts(r_ids, right_by_id).items():
            counts[k] = counts.get(k, 0) + v
        flags = sorted(set(lentry.get("flags", [])) | set(rentry.get("flags", [])))
        matched_block_entities.append({
            "block_match_id": bmatch.get("match_id"),
            "left_block_id": lbid,
            "right_block_id": rbid,
            "left_entities": l_ids,
            "right_entities": r_ids,
            "entity_type_counts": counts,
            "quality_flags": flags,
        })

    def _unmatched(by_block: dict, matched: set) -> list[dict]:
        out = []
        for bid, entry in by_block.items():
            if bid in matched:
                continue
            out.append({
                "block_id": bid,
                "page_number": entry.get("page_number"),
                "semantic_type": entry.get("semantic_type"),
                "entities": entry.get("entity_ids", []),
                "quality_flags": entry.get("flags", []),
            })
        return out

    unmatched_left = _unmatched(left_by_block, matched_left_bids)
    unmatched_right = _unmatched(right_by_block, matched_right_bids)

    # warnings
    warnings: list[str] = []
    if not (report.get("block_matches") or []) and (left_by_block or right_by_block):
        warnings.append("no_block_matches")
    scheme_no_keys = sum(
        1 for entry in list(left_by_block.values()) + list(right_by_block.values())
        if "scheme_without_key_entities" in entry.get("flags", []))
    if scheme_no_keys:
        warnings.append(f"scheme_blocks_without_key_entities: {scheme_no_keys}")
    empty_blocks = sum(
        1 for entry in list(left_by_block.values()) + list(right_by_block.values())
        if not entry.get("entity_ids"))
    if empty_blocks:
        warnings.append(f"blocks_without_entities: {empty_blocks}")
    degenerate_norms = (left_stats["degenerate_norm_reference_suppressed"]
                        + right_stats["degenerate_norm_reference_suppressed"])
    if degenerate_norms:
        warnings.append(f"degenerate_norm_reference_suppressed: {degenerate_norms}")

    # summary
    all_entities = left_entities + right_entities
    by_type: Counter = Counter(e["entity_type"] for e in all_entities)
    by_group: Counter = Counter(e["semantic_group"] for e in all_entities)
    by_source: Counter = Counter(e["evidence"]["source"] for e in all_entities)
    blocks_processed = len(left_by_block) + len(right_by_block)

    report_out = {
        "version": REPORT_VERSION,
        "kind": REPORT_KIND,
        "left": {
            "document_code": (left_model.get("document") or {}).get("document_code", ""),
            "entities_total": len(left_entities),
        },
        "right": {
            "document_code": (right_model.get("document") or {}).get("document_code", ""),
            "entities_total": len(right_entities),
        },
        "summary": {
            "entities_total": len(all_entities),
            "left_entities_total": len(left_entities),
            "right_entities_total": len(right_entities),
            "by_entity_type": dict(by_type),
            "by_semantic_group": dict(by_group),
            "by_source": dict(by_source),
            "blocks_processed": blocks_processed,
            "matched_blocks_processed": len(matched_block_entities),
            "unmatched_blocks_processed": len(unmatched_left) + len(unmatched_right),
            "warnings_count": len(warnings),
        },
        "left_entities": left_entities,
        "right_entities": right_entities,
        "matched_block_entities": matched_block_entities,
        "unmatched_left_block_entities": unmatched_left,
        "unmatched_right_block_entities": unmatched_right,
        "warnings": warnings,
    }
    return report_out


# ─── write_entity_extraction_report (атомарная запись) ──────────────────────


def write_entity_extraction_report(out_path: str | Path, report: dict) -> Path:
    """Атомарно записать отчёт в JSON-файл (tmp + ``os.replace``)."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(report, ensure_ascii=False, indent=2)
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
    "REPORT_VERSION",
    "REPORT_KIND",
    "extract_document_entities",
    "extract_entities_for_block",
    "extract_entities_for_matched_documents",
    "extract_stamp_entities",
    "extract_text_entities",
    "extract_table_entities",
    "extract_scheme_entities",
    "extract_change_log_entities",
    "extract_contents_entities",
    "normalize_entity_text",
    "strip_html_markup",
    "clean_entity_value",
    "make_entity_id",
    "write_entity_extraction_report",
]
