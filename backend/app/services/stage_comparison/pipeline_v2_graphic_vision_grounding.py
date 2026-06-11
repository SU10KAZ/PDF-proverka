# -*- coding: utf-8 -*-
"""Pipeline V2 — Graphic Vision Grounding.

Проверяет результат tiled/single-shot vision-обогащения
(`graphic_vision_enrichment_report.json`) против ТЕКСТОВОГО/ВЕКТОРНОГО слоя
блока (anchors из normalized document model: `pdfplumber_text_excerpt`,
OCR `key_entities`/summary, stamp), чтобы отделить реально считанные номиналы
от галлюцинаций и достроенных «типовых рядов».

Почему это нужно (pilot ГРЩ ИОС 1.1, blocks 7EMD↔763U):
* tiled vision прочитал реальные значения (QF5 400А→200А, 4х185→4х120, SA
  1600А, T1/T2 1250кВА), которые single-shot high_res не видел;
* НО также сгенерировал галлюцинации: фабрикованный стандартный ряд
  «2P 25…800А», повторяющиеся «QF3…QF17 100А», no-op «изменения».

Этот слой:
* подтверждает каждую сущность/число по anchor-тексту нужной стороны
  (OLD→left, NEW→right);
* помечает достроенные ряды `rejected_artificial_series`;
* снимает no-op changes (`rejected_noop`);
* выставляет `grounded` / `weakly_grounded` / `ungrounded` /
  `no_anchor_available`.

Принципы:
* НЕ запускает vision/LLM, не ходит в сеть, ничего не пишет в runtime;
* НЕ удаляет и НЕ меняет сырой vision report — это отдельный артефакт;
* fail-soft: любая ошибка по item'у → item помечается warning'ом, отчёт жив;
* нормализация консервативна: канонизирует ФОРМАТ (гомоглифы кириллица/латиница,
  разделители, единицы, пробелы), но НЕ склеивает разные цифры в одно.
"""
from __future__ import annotations

import json
import os
import re
import unicodedata
from pathlib import Path
from typing import Any, Optional

REPORT_VERSION = 1
REPORT_KIND = "stage_comparison_pipeline_v2_graphic_vision_grounding"

# ─── grounding statuses ──────────────────────────────────────────────────────
GROUNDED = "grounded"
WEAKLY_GROUNDED = "weakly_grounded"
UNGROUNDED = "ungrounded"
REJECTED_ARTIFICIAL_SERIES = "rejected_artificial_series"
REJECTED_DESIGNATOR_RANGE = "rejected_designator_range"
REJECTED_NOOP = "rejected_noop"
REJECTED_INVALID_FORMAT = "rejected_invalid_format"
NO_ANCHOR_AVAILABLE = "no_anchor_available"

# Понятные reason-коды (в дополнение к status; backward-compat не ломают).
REASON_RATING_LADDER = "artificial_rating_ladder"
REASON_REPEATED_RATING = "repeated_same_rating"
REASON_DESIGNATOR_RANGE = "artificial_designator_range"
REASON_NOOP = "noop_change"
REASON_NOT_FOUND = "not_found_in_anchors"
REASON_NO_ANCHOR = "no_anchor_available"
REASON_GROUNDED = "grounded"
REASON_PARTIAL = "partial_match"

# ─── нормализация ────────────────────────────────────────────────────────────

# Кириллические буквы-гомоглифы → латиница (только нижний регистр; строку
# предварительно lowercase'им). Канонизирует «А»(А-cyr)↔«A», «ТА»↔«TA» и т.п.
# НЕ трогает цифры — разные номиналы не склеиваются.
_HOMOGLYPH_LOWER = {
    "а": "a", "в": "b", "е": "e", "к": "k", "м": "m", "н": "h", "о": "o",
    "р": "p", "с": "c", "т": "t", "у": "y", "х": "x",
}
_HOMOGLYPH_TABLE = str.maketrans(_HOMOGLYPH_LOWER)

# Единицы: канонизируем ДО гомоглиф-мэппинга (на кириллице), чтобы
# «кВАр»/«квар» → «kvar», «кВт» → «kw», «кВА» → «kva».
_UNIT_REPLACEMENTS = [
    ("квар", "kvar"), ("кв·ар", "kvar"), ("кв.ар", "kvar"),
    ("ква", "kva"), ("кв·а", "kva"),
    ("квт", "kw"), ("квтч", "kwh"), ("квт·ч", "kwh"),
]

# Метки мощности/тока: Pp/Рр/Pрасч → pp, Ip/Iр/Iрасч → ip (best-effort).
_LABEL_REPLACEMENTS = [
    ("pрасчётное", "pp"), ("pрасчетное", "pp"), ("pрасч", "pp"),
    ("ppасч", "pp"), ("ррасч", "pp"), ("py", "pp"),
    ("iрасч", "ip"), ("iр", "ip"), ("iрасчётный", "ip"), ("iрасчетный", "ip"),
]

_DASHES = {"–": "-", "—": "-", "−": "-", "‒": "-", "―": "-", "­": ""}


def normalize_engineering_token(value: Any) -> str:
    """Канонизировать инженерный токен/строку для grounding-сравнения.

    NFKC → lower → канон единиц (кириллица) → канон разделителей (×/x, тире) →
    десятичная запятая между цифрами → точка → гомоглифы кириллица→латиница →
    схлоп пробелов в ОДИН + склейка «число+единица» («400 А»→«400a»). Пробелы
    между РАЗНЫМИ токенами сохраняются, поэтому дизайнатор и номинал не
    слипаются («1QF5 400А»→«1qf5 400a», номинал извлекается). «4х185»→«4x185»,
    «ТА1–ТА3»→«ta1-ta3».
    """
    if not isinstance(value, str):
        if value is None:
            return ""
        value = str(value)
    t = unicodedata.normalize("NFKC", value).lower()
    for a, b in _DASHES.items():
        t = t.replace(a, b)
    t = t.replace("×", "x")
    # десятичная запятая ТОЛЬКО между цифрами (233,6 → 233.6); «QF1, QF2» цел
    t = re.sub(r"(\d),(\d)", r"\1.\2", t)
    for a, b in _UNIT_REPLACEMENTS:
        t = t.replace(a, b)
    for a, b in _LABEL_REPLACEMENTS:
        t = t.replace(a, b)
    t = t.translate(_HOMOGLYPH_TABLE)
    t = re.sub(r"\s+", " ", t).strip()
    # склейка «число + единица» (но НЕ дизайнатор+номинал): 400 a → 400a
    t = re.sub(r"(\d)\s+(a|kva|kvar|kwh|kw)\b", r"\1\2", t)
    return t


def _compact(value: Any) -> str:
    """Нормализованная форма БЕЗ пробелов — для substring/маркировка-сравнения.

    «QF 5»→«qf5», «ТА1–ТА3»→«ta1-ta3». НЕ используется для извлечения номиналов
    (там нужны границы токенов), только для подстрочного поиска маркировок.
    """
    return normalize_engineering_token(value).replace(" ", "")


# извлечение «значимых» токенов из нормализованной (spaced) строки.
# Номинал = число, ЗА которым сразу 'a' (ампер), и НЕ часть «kva»/слова
# (т.е. перед числом не цифра — иначе это хвост другого числа).
_RE_RATING = re.compile(r"(?<!\d)(\d+(?:\.\d+)?)a(?![a-z])")
_RE_SECTION = re.compile(r"\d+x\d+(?:[+x]\d+)*")
_RE_POWER = re.compile(r"\d+(?:\.\d+)?(?:kw|kva|kvar|kwh)")
# дизайнатор аппарата: латиница/гомоглифные буквы + цифры (qf5, sa1, ta3, 1qf2)
_RE_DESIGNATOR = re.compile(r"[a-zщцшгджзфйюя]{1,6}\d+[a-z0-9.]*")


def _salient_values(norm: str) -> dict:
    """Извлечь номиналы/сечения/мощности из НОРМАЛИЗОВАННОЙ строки."""
    ratings = {f"{m.group(1)}a" for m in _RE_RATING.finditer(norm)}
    sections = set(_RE_SECTION.findall(norm))
    powers = set(_RE_POWER.findall(norm))
    return {"ratings": ratings, "sections": sections, "powers": powers}


def _rating_amps(token: str) -> Optional[float]:
    m = re.match(r"(\d+(?:\.\d+)?)a$", token)
    return float(m.group(1)) if m else None


# ─── anchors ─────────────────────────────────────────────────────────────────

# Поля блока normalized model, несущие текстовый/векторный слой (anchors).
# В самой normalized model хранятся ТОЛЬКО excerpt'ы (600 симв.) — полный
# текст-слой берётся отдельно из result.json (см. _load_full_block_texts) и
# передаётся в collect_block_text_anchors как full_texts (приоритетный).
_ANCHOR_TEXT_FIELDS = (
    "pdfplumber_text", "pdfplumber_text_excerpt",
    "ocr_text", "text", "text_excerpt",
)
# Жёсткий cap полного текст-слоя ВНУТРИ grounding (в report не сохраняется —
# там только короткие matched-snippets/наборы). Защита от патологий.
_FULL_ANCHOR_CAP = 40000


def _block_anchor_strings(block: Any,
                          full_texts: Optional[list] = None) -> tuple[list, str]:
    """Собрать сырые anchor-строки блока (без нормализации) + метку источника.

    Приоритет: полный текст-слой (full_texts: full pdfplumber → full OCR) →
    excerpt'ы блока → key_entities → stamp. Возвращает (strings, source).
    """
    out: list[str] = []
    source = "none"
    for v in (full_texts or []):
        if isinstance(v, str) and v.strip():
            out.append(v[:_FULL_ANCHOR_CAP])
            source = "full_text"
    if not isinstance(block, dict):
        return out, source
    for f in _ANCHOR_TEXT_FIELDS:
        v = block.get(f)
        if isinstance(v, str) and v.strip():
            out.append(v)
            if source == "none":
                source = "excerpt"
    raw = block.get("raw")
    if isinstance(raw, dict):
        for f in _ANCHOR_TEXT_FIELDS:
            v = raw.get(f)
            if isinstance(v, str) and v.strip():
                out.append(v)
                if source == "none":
                    source = "excerpt"
    ocr = block.get("ocr_json_summary")
    if isinstance(ocr, dict):
        for f in ("content_summary", "detailed_description"):
            v = ocr.get(f)
            if isinstance(v, str) and v.strip():
                out.append(v)
        ke = ocr.get("key_entities")
        if isinstance(ke, list):
            out.extend(str(x) for x in ke if x)
            if source == "none" and ke:
                source = "key_entities"
    stamp = block.get("stamp_data")
    if isinstance(stamp, dict):
        for f in ("sheet_name", "document_code"):
            v = stamp.get(f)
            if isinstance(v, str) and v.strip():
                out.append(v)
                if source == "none":
                    source = "stamp"
    return out, source


class BlockAnchors:
    """Нормализованный anchor-корпус блока + предвычисленные value-наборы."""

    __slots__ = ("block_id", "available", "blob", "compact", "ratings",
                 "sections", "powers", "raw_count", "char_count", "source")

    def __init__(self, block_id: str, strings: list[str], *, source: str = "none"):
        self.block_id = block_id
        self.source = source
        self.raw_count = len(strings)
        # spaced blob — для извлечения номиналов (нужны границы токенов);
        # «|»-сепаратор не даёт соседним токенам слипнуться через regex.
        norm_parts = [normalize_engineering_token(s) for s in strings]
        self.blob = " | ".join(p for p in norm_parts if p)
        # compact blob — для substring-поиска маркировок («qf5» в «1qf5»)
        self.compact = "|".join(p.replace(" ", "") for p in norm_parts if p)
        self.char_count = len(self.compact)
        self.available = bool(self.blob)
        vals = _salient_values(self.blob)
        self.ratings = vals["ratings"]
        self.sections = vals["sections"]
        self.powers = vals["powers"]

    def has_value(self, token: str) -> bool:
        """Найдено ли конкретное значение (rating/section/power) в anchors."""
        if not token:
            return False
        if token in self.ratings or token in self.sections or token in self.powers:
            return True
        return token.replace(" ", "") in self.compact

    def has_text(self, token: str) -> bool:
        """Найдена ли произвольная нормализованная подстрока (маркировка)."""
        return bool(token) and token.replace(" ", "") in self.compact

    def to_diag(self) -> dict:
        # короткая диагностика: НИКАКОГО полного текста — только метка
        # источника, счётчики и наборы значений (cap 40)
        return {"block_id": self.block_id, "available": self.available,
                "source": self.source,
                "anchor_strings": self.raw_count, "anchor_chars": self.char_count,
                "ratings": sorted(self.ratings)[:40],
                "sections": sorted(self.sections)[:40]}

    @classmethod
    def merge(cls, *anchors: "BlockAnchors") -> "BlockAnchors":
        """Объединить anchors нескольких блоков (для anti-false-rejection).

        Значение, реальное на ЛЮБОЙ стороне пары, не должно ошибочно
        попадать в «достроенный ряд».
        """
        m = cls.__new__(cls)
        m.block_id = "+".join(a.block_id for a in anchors if a.block_id)
        m.raw_count = sum(a.raw_count for a in anchors)
        m.blob = " | ".join(a.blob for a in anchors if a.blob)
        m.compact = "|".join(a.compact for a in anchors if a.compact)
        m.char_count = len(m.compact)
        m.available = bool(m.blob)
        m.ratings = set().union(*(a.ratings for a in anchors)) if anchors else set()
        m.sections = set().union(*(a.sections for a in anchors)) if anchors else set()
        m.powers = set().union(*(a.powers for a in anchors)) if anchors else set()
        m.source = "+".join(sorted({a.source for a in anchors if a.source != "none"})) or "none"
        return m


def collect_block_text_anchors(block: Any, *, block_id: str = "",
                               full_texts: Optional[list] = None) -> BlockAnchors:
    """Построить :class:`BlockAnchors` из блока normalized model + полного текста.

    ``full_texts`` (приоритетный): полный pdfplumber_text / OCR-текст блока из
    result.json. Если не передан — fallback на excerpt'ы блока (текущая логика).
    """
    bid = block_id or (block.get("block_id") if isinstance(block, dict) else "") or ""
    strings, source = _block_anchor_strings(block, full_texts)
    return BlockAnchors(str(bid), strings, source=source)


# ─── grounding одной сущности ────────────────────────────────────────────────

def _entity_marking_present(norm: str, anchors: BlockAnchors) -> bool:
    """Есть ли в anchors хотя бы один дизайнатор/маркировка сущности."""
    for m in _RE_DESIGNATOR.finditer(norm):
        tok = m.group(0)
        if len(tok) < 2:
            continue
        if anchors.has_text(tok):
            return True
        # дизайнатор без ведущей секции-цифры: «1qf2» anchor vs «qf2» vision
        stripped = re.sub(r"^\d+", "", tok)
        if len(stripped) >= 2 and anchors.has_text(stripped):
            return True
    return False


def ground_vision_entity(entity: Any, anchors: BlockAnchors) -> dict:
    """Заземлить ОДНУ vision-сущность по anchors стороны.

    Возвращает ``{value, normalized, status, matched_values, missing_values}``.
    """
    raw = entity if isinstance(entity, str) else json.dumps(entity, ensure_ascii=False)
    norm = normalize_engineering_token(raw)
    if not norm:
        return {"value": raw, "normalized": "", "status": REJECTED_INVALID_FORMAT,
                "matched_values": [], "missing_values": []}
    if not anchors.available:
        return {"value": raw, "normalized": norm, "status": NO_ANCHOR_AVAILABLE,
                "matched_values": [], "missing_values": []}

    vals = _salient_values(norm)
    all_vals = sorted(vals["ratings"] | vals["sections"] | vals["powers"])
    matched = [v for v in all_vals if anchors.has_value(v)]
    missing = [v for v in all_vals if v not in matched]
    marking_present = _entity_marking_present(norm, anchors)

    if all_vals:
        if not missing and (marking_present or not _entity_marking_present(norm, anchors)):
            status = GROUNDED if not missing else WEAKLY_GROUNDED
        elif matched or marking_present:
            status = WEAKLY_GROUNDED
        else:
            status = UNGROUNDED
        # уточнение: все значения найдены → grounded (даже без маркировки —
        # значение и есть основной якорь)
        if not missing:
            status = GROUNDED
    else:
        # сущность без числовых значений — судим по маркировке/подстроке
        if marking_present or anchors.has_text(norm):
            status = GROUNDED
        else:
            status = UNGROUNDED

    return {"value": raw, "normalized": norm, "status": status,
            "matched_values": matched, "missing_values": missing}


# ─── changes: noop + old/new grounding ───────────────────────────────────────

# разделители «старое → новое» в формулировках Qwen
_CHANGE_SPLIT = re.compile(
    r"\s*(?:->|→|=>|⇒|\bна\b|\bзаменен[оы]? на\b|\bзамена на\b|\bстал[ои]?\b)\s*",
    re.IGNORECASE)
_FROM_TO = re.compile(
    r"\bс\s+(?P<old>.+?)\s+на\s+(?P<new>.+)", re.IGNORECASE)


def _split_change(change: str) -> Optional[tuple[str, str]]:
    """Выделить (old, new) из строки изменения, если есть явный переход."""
    ft = _FROM_TO.search(change)
    if ft:
        return ft.group("old").strip(), ft.group("new").strip()
    parts = _CHANGE_SPLIT.split(change, maxsplit=1)
    if len(parts) == 2 and parts[0].strip() and parts[1].strip():
        return parts[0].strip(), parts[1].strip()
    return None


def detect_noop_change(change: Any) -> bool:
    """True, если old/new после нормализации совпадают (форматное «изменение»)."""
    if not isinstance(change, str):
        return False
    pair = _split_change(change)
    if not pair:
        return False
    old_n = normalize_engineering_token(pair[0])
    new_n = normalize_engineering_token(pair[1])
    if not old_n or not new_n:
        return False
    if old_n == new_n:
        return True
    # значимые значения по обе стороны совпали → форматное no-op
    ov, nv = _salient_values(old_n), _salient_values(new_n)
    o_all = ov["ratings"] | ov["sections"] | ov["powers"]
    n_all = nv["ratings"] | nv["sections"] | nv["powers"]
    return bool(o_all) and o_all == n_all


_ADDED_MARKERS = ("добавлен", "появил", "нов", "added", "new")
_REMOVED_MARKERS = ("удал", "убра", "исключ", "removed", "delet")


def ground_observed_change(change: Any, left_anchors: BlockAnchors,
                           right_anchors: BlockAnchors,
                           *, artificial_values: Optional[set] = None) -> dict:
    """Заземлить ОДНО наблюдаемое изменение (old→left, new→right)."""
    raw = change if isinstance(change, str) else json.dumps(change, ensure_ascii=False)
    norm = normalize_engineering_token(raw)
    if not norm:
        return {"value": raw, "status": REJECTED_INVALID_FORMAT}
    if detect_noop_change(raw):
        return {"value": raw, "status": REJECTED_NOOP}

    art = artificial_values or set()
    pair = _split_change(raw)
    if pair:
        old_n = normalize_engineering_token(pair[0])
        new_n = normalize_engineering_token(pair[1])
        old_vals = _salient_values(old_n)
        new_vals = _salient_values(new_n)
        old_all = sorted(old_vals["ratings"] | old_vals["sections"] | old_vals["powers"])
        new_all = sorted(new_vals["ratings"] | new_vals["sections"] | new_vals["powers"])
        # артефактный ряд: новое значение из достроенной серии без anchor
        if art and any(v in art for v in new_all):
            return {"value": raw, "status": REJECTED_ARTIFICIAL_SERIES,
                    "side": "new"}
        old_ok = (any(left_anchors.has_value(v) for v in old_all)
                  or _entity_marking_present(old_n, left_anchors)) if left_anchors.available else None
        new_ok = (any(right_anchors.has_value(v) for v in new_all)
                  or _entity_marking_present(new_n, right_anchors)) if right_anchors.available else None
        if left_anchors.available is False and right_anchors.available is False:
            return {"value": raw, "status": NO_ANCHOR_AVAILABLE}
        flags = [f for f in (old_ok, new_ok) if f is not None]
        if flags and all(flags):
            status = GROUNDED
        elif any(flags):
            status = WEAKLY_GROUNDED
        else:
            status = UNGROUNDED
        return {"value": raw, "status": status,
                "old_values": old_all, "new_values": new_all}

    # описательное изменение без явного перехода (added/removed/renamed)
    lower = raw.lower()
    side = ("right" if any(m in lower for m in _ADDED_MARKERS)
            else "left" if any(m in lower for m in _REMOVED_MARKERS) else "both")
    vals = _salient_values(norm)
    all_vals = sorted(vals["ratings"] | vals["sections"] | vals["powers"])
    if art and all_vals and all(v in art for v in all_vals):
        return {"value": raw, "status": REJECTED_ARTIFICIAL_SERIES}
    targets = []
    if side in ("left", "both") and left_anchors.available:
        targets.append(left_anchors)
    if side in ("right", "both") and right_anchors.available:
        targets.append(right_anchors)
    if not targets:
        return {"value": raw, "status": NO_ANCHOR_AVAILABLE}
    found = any(a.has_value(v) for a in targets for v in all_vals) or \
        any(_entity_marking_present(norm, a) for a in targets)
    if all_vals and any(a.has_value(v) for a in targets for v in all_vals) and \
            any(_entity_marking_present(norm, a) for a in targets):
        status = GROUNDED
    elif found:
        status = WEAKLY_GROUNDED
    else:
        status = UNGROUNDED
    return {"value": raw, "status": status, "side": side}


# ─── artificial series ───────────────────────────────────────────────────────

# Стандартный ряд номиналов автоматов (МЭК E-серия, А).
STANDARD_RATINGS = [
    6, 8, 10, 13, 16, 20, 25, 32, 40, 50, 63, 80, 100, 125, 160, 200, 250,
    315, 400, 500, 630, 800, 1000, 1250, 1600, 2000, 2500, 3200, 4000, 5000,
    6300,
]
_STD_INDEX = {v: i for i, v in enumerate(STANDARD_RATINGS)}
_SERIES_MIN_LEN = 6      # длина ряда для подозрения
_REPEAT_MIN = 6          # повтор одинакового значения


def detect_artificial_series(values: Any, anchors: Optional[BlockAnchors] = None
                             ) -> dict:
    """Найти достроенные «типовые ряды» среди номиналов, которых нет в anchors.

    ``values`` — список строк (vision-сущностей/значений). Возвращает
    ``{artificial_tokens:set, reasons:[...]}``: токены вида «25a», которые
    выглядят как фабрикованная стандартная лесенка или искусственный повтор и
    при этом НЕ подтверждены anchor-текстом.

    Критично: реальные номиналы автоматов ВСЕГДА из стандартного ряда, поэтому
    «похоже на ряд» — слабый сигнал. Сильный сигнал = значение НЕ найдено в
    anchors. Поэтому И лесенка, И повтор детектируются ТОЛЬКО среди UNGROUNDED
    токенов — любое значение, присутствующее в anchor-тексте, защищено.
    """
    if isinstance(values, str):
        values = [values]
    if not isinstance(values, (list, tuple)):
        return {"artificial_tokens": set(), "reasons": []}

    def _grounded(tok: str) -> bool:
        return bool(anchors) and anchors.available and anchors.has_value(tok)

    # собрать rating-токены (с повторами, в порядке появления)
    seq: list[str] = []
    for v in values:
        norm = normalize_engineering_token(v)
        seq.extend(m.group(0) for m in _RE_RATING.finditer(norm))
    # рассматриваем ТОЛЬКО ungrounded — anchor-present значения реальны
    ungrounded_seq = [t for t in seq if not _grounded(t)]

    artificial: set[str] = set()
    reasons: list[str] = []
    token_reasons: dict[str, str] = {}   # token → понятный reason-код

    # 1) повторяющееся ОДИНАКОВОЕ ungrounded значение ≥ _REPEAT_MIN
    from collections import Counter
    counts = Counter(ungrounded_seq)
    for tok, c in counts.items():
        if c >= _REPEAT_MIN:
            artificial.add(tok)
            token_reasons[tok] = REASON_REPEATED_RATING
            reasons.append(f"repeated_value:{tok}x{c}")

    # 2) монотонная стандартная лесенка ≥ _SERIES_MIN_LEN среди ungrounded
    #    distinct значений (контур фабрикованного каталога/sweep'а)
    amps = []
    seen = set()
    for tok in ungrounded_seq:
        a = _rating_amps(tok)
        if a is not None and a == int(a) and int(a) in _STD_INDEX and tok not in seen:
            seen.add(tok)
            amps.append((int(a), tok))
    amps.sort(key=lambda x: x[0])
    run: list[tuple[int, str]] = []
    best: list[tuple[int, str]] = []
    prev_idx = None
    for a, tok in amps:
        idx = _STD_INDEX[a]
        if prev_idx is None or 1 <= idx - prev_idx <= 3:
            run.append((a, tok))
        else:
            if len(run) > len(best):
                best = run
            run = [(a, tok)]
        prev_idx = idx
    if len(run) > len(best):
        best = run
    if len(best) >= _SERIES_MIN_LEN:
        for _, tok in best:
            artificial.add(tok)
            token_reasons.setdefault(tok, REASON_RATING_LADDER)
        reasons.append(f"standard_ladder:len={len(best)}")

    return {"artificial_tokens": artificial, "reasons": reasons,
            "token_reasons": token_reasons}


# ─── designator-range hallucination ──────────────────────────────────────────

# Диапазон дизайнаторов: «QF1...QF100», «QF1-QF1000», «KM1…KM10», «1ТТ1...1ТТ19».
# Допускаем опциональный второй префикс и разные разделители диапазона.
_RE_DESIGNATOR_RANGE = re.compile(
    r"([a-zщцшгджзфйюяё]{1,6})\s*(\d+)\s*"
    r"(?:\.{2,3}|…|—|–|-|\bдо\b)\s*"
    r"([a-zщцшгджзфйюяё]{0,6})\s*(\d+)")
# Подозрительный охват диапазона: реальные щиты МКД редко имеют >_RANGE_SUSPECT
# подряд идущих отходящих линий одной серии; фабрикации дают 50/100/1000.
_RANGE_SUSPECT_SPAN = 8


def detect_artificial_designator_range(text: Any,
                                       anchors: Optional[BlockAnchors] = None
                                       ) -> dict:
    """Найти галлюцинированные диапазоны дизайнаторов («QF1...QF100»).

    Возвращает ``{is_artificial, ranges:[...], reason}``. Длинный диапазон
    (span ≥ _RANGE_SUSPECT_SPAN), верхний конец которого НЕ найден в anchors,
    считается достроенной enumeration-галлюцинацией. Короткие валидные
    диапазоны («TA1-TA3», «QF1-QF6») и любые, чей верхний конец есть в anchors,
    НЕ отвергаются.
    """
    s = text if isinstance(text, str) else json.dumps(text or "", ensure_ascii=False)
    norm = normalize_engineering_token(s)
    ranges: list[dict] = []
    artificial = False
    for m in _RE_DESIGNATOR_RANGE.finditer(norm):
        pfx1, n1, pfx2, n2 = m.group(1), m.group(2), m.group(3), m.group(4)
        # второй префикс, если есть, должен совпадать с первым (QF1...QF100),
        # иначе это не диапазон одной серии (QF1-TA3 — две разные сущности)
        if pfx2 and pfx2 != pfx1:
            continue
        try:
            lo, hi = int(n1), int(n2)
        except ValueError:
            continue
        span = hi - lo
        if span < _RANGE_SUSPECT_SPAN:
            continue   # короткий валидный диапазон — не трогаем
        # верхний конец диапазона есть в anchors? (реальная длинная серия)
        hi_tok = f"{pfx1}{hi}"
        anchored = bool(anchors and anchors.available
                        and (anchors.has_text(hi_tok) or anchors.has_text(f"{pfx1}{n2}")))
        ranges.append({"prefix": pfx1, "lo": lo, "hi": hi, "span": span,
                       "anchored": anchored})
        if not anchored:
            artificial = True
    return {"is_artificial": artificial, "ranges": ranges,
            "reason": REASON_DESIGNATOR_RANGE if artificial else None}


# ─── item + report ───────────────────────────────────────────────────────────

def _blocks_by_id(model: Any) -> dict:
    if not isinstance(model, dict):
        return {}
    blocks = model.get("blocks")
    if isinstance(blocks, dict):
        return {k: v for k, v in blocks.items() if isinstance(v, dict)}
    if isinstance(blocks, list):
        return {b.get("block_id"): b for b in blocks if isinstance(b, dict)}
    return {}


# разбиение запятой-сепаратором, НЕ десятичной (после запятой пробел и
# не-цифра) — «QF1 (63А), QF2 (400А)» делится, «Pp=233,6» цел
_ENTITY_SPLIT = re.compile(r",(?=\s+\D)")


def _atomize_entry(entry: str) -> list[str]:
    """Разбить comma-joined список сущностей на атомарные («QF1 (63А)»…)."""
    parts = [p.strip(" ;") for p in _ENTITY_SPLIT.split(entry)]
    return [p for p in parts if p]


def _entity_entries(result: dict, key: str, *, atomize: bool = True) -> list[str]:
    vals = result.get(key)
    if not isinstance(vals, list):
        return []
    out: list[str] = []
    for v in vals:
        if not (isinstance(v, str) and v.strip()):
            continue
        out.extend(_atomize_entry(v) if atomize else [v])
    return out


_REJECTED_STATUSES = {REJECTED_ARTIFICIAL_SERIES, REJECTED_DESIGNATOR_RANGE,
                      REJECTED_NOOP, REJECTED_INVALID_FORMAT}

# Поля result.json (block-level), несущие ПОЛНЫЙ текст-слой (не excerpt).
_FULL_TEXT_FIELDS = ("pdfplumber_text", "ocr_text", "ocr_clean")


def _load_full_block_texts(model: Any) -> dict:
    """Прочитать полный текст-слой блоков из source.result_json_path.

    Возвращает ``{block_id: [pdfplumber_text, ocr_text, ...]}``. Fail-soft:
    нет пути / файла / ошибка чтения → пустой dict (fallback на excerpt).
    Используется ТОЛЬКО внутри grounding; в артефакты полный текст не пишется.
    """
    if not isinstance(model, dict):
        return {}
    src = model.get("source") if isinstance(model.get("source"), dict) else {}
    rjp = src.get("result_json_path")
    if not rjp or not isinstance(rjp, str):
        return {}
    try:
        p = Path(rjp)
        if not p.exists():
            return {}
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 — fail-soft
        return {}
    out: dict[str, list] = {}
    pages = data.get("pages") if isinstance(data, dict) else None
    for pg in (pages or []):
        blocks = pg.get("blocks") if isinstance(pg, dict) else None
        for b in (blocks or []):
            if not isinstance(b, dict):
                continue
            bid = b.get("id") or b.get("block_id")
            if not bid:
                continue
            texts = [b.get(f) for f in _FULL_TEXT_FIELDS
                     if isinstance(b.get(f), str) and b.get(f).strip()]
            if texts:
                out[str(bid)] = texts
    return out


def _entity_reason(g: dict) -> str:
    """Понятный reason-код по статусу заземления сущности/изменения."""
    st = g.get("status")
    if st == GROUNDED:
        return REASON_GROUNDED
    if st == WEAKLY_GROUNDED:
        return REASON_PARTIAL
    if st == UNGROUNDED:
        return REASON_NOT_FOUND
    if st == NO_ANCHOR_AVAILABLE:
        return REASON_NO_ANCHOR
    if st == REJECTED_NOOP:
        return REASON_NOOP
    return g.get("reason") or st


def _ground_item(item: dict, left_blocks: dict, right_blocks: dict,
                 left_full: Optional[dict] = None,
                 right_full: Optional[dict] = None) -> dict:
    lid = item.get("left_block_id")
    rid = item.get("right_block_id")
    left_anchors = collect_block_text_anchors(
        left_blocks.get(lid), block_id=lid or "",
        full_texts=(left_full or {}).get(lid))
    right_anchors = collect_block_text_anchors(
        right_blocks.get(rid), block_id=rid or "",
        full_texts=(right_full or {}).get(rid))
    result = item.get("result") if isinstance(item.get("result"), dict) else {}
    warnings: list[str] = []

    ent_old = _entity_entries(result, "engineering_entities_old")
    ent_new = _entity_entries(result, "engineering_entities_new")
    changes = _entity_entries(result, "observed_changes", atomize=False)

    # artificial-series детектируем против ОБЪЕДИНЁННЫХ anchors пары: значение,
    # реальное на любой стороне, не должно ошибочно попасть в «достроенный ряд».
    combined = BlockAnchors.merge(left_anchors, right_anchors)
    art = detect_artificial_series(ent_new + ent_old, combined)
    art_tokens = art["artificial_tokens"]
    art_token_reasons = art.get("token_reasons", {})

    def _ground_entities(entries: list[str], anchors: BlockAnchors,
                         artificial: set) -> tuple[list, list]:
        grounded, rejected = [], []
        for e in entries:
            g = ground_vision_entity(e, anchors)
            e_ratings = _salient_values(g["normalized"])["ratings"]
            # (a) designator-range галлюцинация («QF1...QF100») без anchor
            dr = detect_artificial_designator_range(e, combined)
            if dr["is_artificial"]:
                g["status"] = REJECTED_DESIGNATOR_RANGE
                g["reason"] = REASON_DESIGNATOR_RANGE
                g["designator_ranges"] = dr["ranges"]
            # (b) rejected только если ВСЕ номиналы сущности из артефактного ряда
            #     и ни один не подтверждён напрямую (защита реальных значений)
            elif (artificial and e_ratings and e_ratings <= artificial
                    and not (set(g["matched_values"]) & e_ratings)):
                g["status"] = REJECTED_ARTIFICIAL_SERIES
                g["reason"] = next((art_token_reasons[t] for t in e_ratings
                                    if t in art_token_reasons), REASON_RATING_LADDER)
            g.setdefault("reason", _entity_reason(g))
            (rejected if g["status"] in _REJECTED_STATUSES else grounded).append(g)
        return grounded, rejected

    g_old, r_old = _ground_entities(ent_old, left_anchors, art_tokens)
    g_new, r_new = _ground_entities(ent_new, right_anchors, art_tokens)

    g_changes, r_changes = [], []
    for c in changes:
        gc = ground_observed_change(c, left_anchors, right_anchors,
                                    artificial_values=art_tokens)
        dr = detect_artificial_designator_range(c, combined)
        if dr["is_artificial"] and gc["status"] not in (GROUNDED, WEAKLY_GROUNDED):
            gc["status"] = REJECTED_DESIGNATOR_RANGE
            gc["reason"] = REASON_DESIGNATOR_RANGE
            gc["designator_ranges"] = dr["ranges"]
        gc.setdefault("reason", _entity_reason(gc))
        (r_changes if gc["status"] in _REJECTED_STATUSES else g_changes).append(gc)

    if not left_anchors.available:
        warnings.append(f"no anchors for left block {lid}")
    if not right_anchors.available:
        warnings.append(f"no anchors for right block {rid}")

    return {
        "item_id": item.get("item_id"),
        "left_block_id": lid, "right_block_id": rid,
        "graphic_type": item.get("graphic_type"),
        "vision_status": item.get("vision_status"),
        "left_anchors": left_anchors.to_diag(),
        "right_anchors": right_anchors.to_diag(),
        "grounded_entities_old": g_old,
        "grounded_entities_new": g_new,
        "grounded_changes": g_changes,
        "rejected_entities": r_old + r_new,
        "rejected_changes": r_changes,
        "artificial_series_reasons": art["reasons"],
        "warnings": warnings,
    }


def _count_status(items: list, key: str, status: str) -> int:
    return sum(1 for it in items for g in it.get(key, []) if g.get("status") == status)


def _anchor_source_counts(items: list) -> dict:
    """Сколько блоков (сторон) грунтовалось по full_text / excerpt / … ."""
    from collections import Counter
    c: Counter = Counter()
    for it in items:
        for side in ("left_anchors", "right_anchors"):
            src = (it.get(side) or {}).get("source")
            if src:
                c[src] += 1
    return dict(c)


def build_graphic_vision_grounding_report(vision_report: Any, *,
                                          left_model: Any = None,
                                          right_model: Any = None,
                                          left_full_texts: Optional[dict] = None,
                                          right_full_texts: Optional[dict] = None,
                                          use_full_text: bool = True) -> dict:
    """Построить grounding-отчёт по vision enrichment report + normalized models.

    Полный текст-слой блоков (full pdfplumber/OCR) подтягивается из
    ``source.result_json_path`` для повышения recall; ``left_full_texts`` /
    ``right_full_texts`` — явная подмена (для тестов). ``use_full_text=False``
    отключает подтяжку (fallback на excerpt). Сырой vision report НЕ
    изменяется. fail-soft: ошибка по item'у не валит отчёт; отсутствие vision
    report → status=failed с диагностикой.
    """
    warnings: list[str] = []
    if not isinstance(vision_report, dict):
        return _empty_report("failed", ["vision report missing or not a dict"])

    items_in = vision_report.get("items")
    if not isinstance(items_in, list):
        return _empty_report("failed", ["vision report has no items[]"])

    left_blocks = _blocks_by_id(left_model)
    right_blocks = _blocks_by_id(right_model)

    # полный текст-слой (fail-soft, в артефакты не пишется) — приоритетный anchor
    left_full = dict(left_full_texts) if left_full_texts else {}
    right_full = dict(right_full_texts) if right_full_texts else {}
    if use_full_text:
        if not left_full:
            left_full = _load_full_block_texts(left_model)
        if not right_full:
            right_full = _load_full_block_texts(right_model)

    out_items: list[dict] = []
    for item in items_in:
        if not isinstance(item, dict):
            continue
        if not isinstance(item.get("result"), dict):
            continue   # нет vision-результата — нечего грунтовать
        try:
            out_items.append(_ground_item(item, left_blocks, right_blocks,
                                          left_full, right_full))
        except Exception as exc:  # noqa: BLE001 — item не валит отчёт
            warnings.append(
                f"grounding failed for item {item.get('item_id')}: "
                f"{type(exc).__name__}: {exc}")

    ent_keys = ("grounded_entities_old", "grounded_entities_new")
    entities_total = sum(len(it.get(k, [])) for it in out_items for k in ent_keys)
    entities_total += sum(len(it.get("rejected_entities", [])) for it in out_items)
    summary = {
        "items_total": len(out_items),
        "entities_total": entities_total,
        "entities_grounded": (_count_status(out_items, "grounded_entities_old", GROUNDED)
                              + _count_status(out_items, "grounded_entities_new", GROUNDED)),
        "entities_weakly_grounded": (_count_status(out_items, "grounded_entities_old", WEAKLY_GROUNDED)
                                     + _count_status(out_items, "grounded_entities_new", WEAKLY_GROUNDED)),
        "entities_ungrounded": (_count_status(out_items, "grounded_entities_old", UNGROUNDED)
                               + _count_status(out_items, "grounded_entities_new", UNGROUNDED)
                               + _count_status(out_items, "grounded_entities_old", NO_ANCHOR_AVAILABLE)
                               + _count_status(out_items, "grounded_entities_new", NO_ANCHOR_AVAILABLE)),
        "changes_total": sum(len(it.get("grounded_changes", []))
                             + len(it.get("rejected_changes", [])) for it in out_items),
        "changes_grounded": _count_status(out_items, "grounded_changes", GROUNDED),
        "changes_weakly_grounded": _count_status(out_items, "grounded_changes", WEAKLY_GROUNDED),
        "changes_rejected": sum(len(it.get("rejected_changes", [])) for it in out_items),
        "artificial_series_rejected": (_count_status(out_items, "rejected_entities", REJECTED_ARTIFICIAL_SERIES)
                                       + _count_status(out_items, "rejected_changes", REJECTED_ARTIFICIAL_SERIES)),
        "designator_range_rejected": (_count_status(out_items, "rejected_entities", REJECTED_DESIGNATOR_RANGE)
                                      + _count_status(out_items, "rejected_changes", REJECTED_DESIGNATOR_RANGE)),
        "noop_changes_rejected": _count_status(out_items, "rejected_changes", REJECTED_NOOP),
        "anchor_source_counts": _anchor_source_counts(out_items),
    }
    for it in out_items:
        for w in it.get("warnings", []):
            warnings.append(f"{it.get('item_id')}: {w}")

    status = "ok"
    if not out_items:
        status = "completed_with_warnings"
        warnings.append("no vision items with results to ground")
    elif warnings:
        status = "completed_with_warnings"

    return {
        "version": REPORT_VERSION,
        "kind": REPORT_KIND,
        "status": status,
        "summary": summary,
        "items": out_items,
        "warnings": warnings,
    }


def _empty_report(status: str, warnings: list[str]) -> dict:
    return {
        "version": REPORT_VERSION,
        "kind": REPORT_KIND,
        "status": status,
        "summary": {
            "items_total": 0, "entities_total": 0, "entities_grounded": 0,
            "entities_weakly_grounded": 0, "entities_ungrounded": 0,
            "changes_total": 0, "changes_grounded": 0,
            "changes_weakly_grounded": 0, "changes_rejected": 0,
            "artificial_series_rejected": 0, "designator_range_rejected": 0,
            "noop_changes_rejected": 0, "anchor_source_counts": {},
        },
        "items": [],
        "warnings": warnings,
    }


def write_graphic_vision_grounding_report(out_path: str | Path, report: dict) -> Path:
    """Атомарно записать grounding-отчёт (tmp + os.replace)."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_text(json.dumps(report, ensure_ascii=False, indent=2),
                   encoding="utf-8")
    os.replace(tmp, out_path)
    return out_path


__all__ = [
    "REPORT_VERSION", "REPORT_KIND",
    "GROUNDED", "WEAKLY_GROUNDED", "UNGROUNDED",
    "REJECTED_ARTIFICIAL_SERIES", "REJECTED_DESIGNATOR_RANGE",
    "REJECTED_NOOP", "REJECTED_INVALID_FORMAT",
    "NO_ANCHOR_AVAILABLE", "STANDARD_RATINGS",
    "normalize_engineering_token", "collect_block_text_anchors",
    "ground_vision_entity", "ground_observed_change",
    "detect_artificial_series", "detect_artificial_designator_range",
    "detect_noop_change",
    "build_graphic_vision_grounding_report",
    "write_graphic_vision_grounding_report",
]
