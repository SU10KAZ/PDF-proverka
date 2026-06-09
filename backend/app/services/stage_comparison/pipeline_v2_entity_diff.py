# -*- coding: utf-8 -*-
"""Pipeline V2 — Deterministic Entity Diff OLD↔NEW (этап 4, backend-only).

Четвёртый слой нового режима сравнения стадий. Принимает
``entity_extraction_report`` (этап 3 —
[pipeline_v2_entity_extraction](pipeline_v2_entity_extraction.py)) и
ДЕТЕРМИНИРОВАННО строит список атомарных отличий OLD↔NEW (deltas): добавленные /
удалённые / изменённые сущности.

```text
entity_extraction_report
  → per matched block pair:
        match_entities (exact_key → normalized_key/numeric_overlap → fuzzy)
        → compare_matched_entities (field-level, с нормализацией)
        → added / removed / changed / unchanged / uncertain
  → unmatched blocks: one-sided added / removed (low-info → uncertain)
  → entity_diff_report.json
```

Здесь НЕТ Opus/critic/UI. Сравнение делается по уже извлечённым сущностям, чтобы
Opus больше НЕ искал отличия по всему тому: на следующем этапе LLM лишь объясняет
готовые deltas.

Ключевые принципы:
  * нормализация гасит косметику (регистр, пробелы, ё/е, `220В`↔`220 В`,
    `cat.5e`↔`cat. 5Е`, `PoE`↔`POE`↔`РоЕ`) — это НЕ отличие;
  * реальные изменения (сечение/категория кабеля, напряжение, количество, номер
    листа, стадия, шифр, наличие/отсутствие) — это delta;
  * не плодить шум: одинаковые после нормализации сущности → unchanged; одинаковая
    норма с разной OCR-разметкой → не delta; low-info `unknown` без evidence →
    `uncertain`/warning, а не high-confidence delta.

Модуль НЕ ходит в сеть, НЕ вызывает Qwen/Opus/OCR/PDF-render, НЕ скачивает
`crop_url`. Только stdlib (включая ``difflib``). Все функции чистые, кроме
``write_entity_diff_report`` (атомарная запись).

См. docs/stage_comparison_pipeline_v2_entity_diff.md.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
import unicodedata
from collections import Counter
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Optional

REPORT_VERSION = 1
REPORT_KIND = "stage_comparison_pipeline_v2_entity_diff"

_DEFAULTS = {
    "fuzzy_threshold": 0.62,        # порог сопоставления requirement по тексту
    "low_match_score": 0.50,        # ниже → low_match_score flag
}


def _opt(options: Optional[dict], key: str) -> Any:
    if options and key in options and options[key] is not None:
        return options[key]
    return _DEFAULTS[key]


# ─── Нормализация ───────────────────────────────────────────────────────────

_WS_RE = re.compile(r"\s+")

# Кириллические гомоглифы → латиница (для кабелей/питания/оборудования, где
# латиница и кириллица смешиваются). Применяется СИММЕТРИЧНО к обеим сторонам.
_HOMOGLYPH = str.maketrans({
    "а": "a", "е": "e", "о": "o", "с": "c", "р": "p", "х": "x", "к": "k",
    "м": "m", "т": "t", "н": "h", "у": "y", "в": "b", "і": "i", "ј": "j",
})


def normalize_entity_value(value: Any) -> str:
    """Базовая нормализация значения: NFKC, lower, ё→е, схлопнуть пробелы."""
    s = "" if value is None else str(value)
    s = unicodedata.normalize("NFKC", s).lower().replace("ё", "е")
    return _WS_RE.sub(" ", s).strip()


def normalize_entity_subject(value: Any) -> str:
    """Нормализация subject/имени поля (как значение, без хвостовой пунктуации)."""
    s = normalize_entity_value(value)
    return s.strip(" .:-")


def normalize_cable_value(value: Any) -> str:
    """Канонизировать кабель (марка+категория+сечение).

    `LAN U/UTP cat. 5Е` и `UTP cat.5e` → `utpcat5e`;
    `КПСВВнг(А)-LS 1x2x0,5` сохраняет сечение `1x2x0.5` (изменение сечения = delta).
    """
    s = unicodedata.normalize("NFKC", str(value or "")).lower().replace("ё", "е")
    s = s.translate(_HOMOGLYPH)
    s = s.replace(",", ".")
    s = re.sub(r"\bu/?utp\b", "utp", s)
    s = re.sub(r"\bf/?utp\b", "futp", s)
    s = re.sub(r"\blan\b", " ", s)
    s = re.sub(r"cat\.?\s*", "cat", s)
    s = re.sub(r"[()\s\-]+", "", s)
    return s.strip()


def normalize_power_value(value: Any) -> str:
    """Канонизировать электропитание: `220В`/`220 В`→`220b`; `+12В`→`12b`;
    `0.5А`/`0.5A`→`0.5a`."""
    s = unicodedata.normalize("NFKC", str(value or "")).lower().replace("ё", "е")
    s = s.translate(_HOMOGLYPH)
    s = s.replace(",", ".").lstrip("+")
    return re.sub(r"\s+", "", s).strip()


def extract_numeric_tokens(value: Any) -> list[str]:
    """Числовые токены значения (учёт `1x2x0,5` → ['1','2','0.5'])."""
    s = str(value or "").replace(",", ".")
    return re.findall(r"\d+(?:\.\d+)?", s)


# ─── Внутренние канонизаторы по типам ───────────────────────────────────────


def _norm_full(value: Any) -> str:
    """Полная норма (`сп 256.1325800.2016`)."""
    return normalize_entity_value(value)


def _norm_base(value: Any) -> str:
    """База нормы без года/редакции (`сп 256.1325800`, `гост 31565`)."""
    b = _norm_full(value)
    b = re.sub(r"[-.]\d{4}\b", "", b)        # хвостовой год -2012 / .2016
    b = re.sub(r"\(ред[^)]*\)", "", b)        # «(ред. …)»
    b = re.sub(r"изм\.?\s*[\d\-,]+", "", b)   # «изм. 1-6»
    return _WS_RE.sub(" ", b).strip()


def _equip_canon(value: Any) -> str:
    """Канон оборудования + синонимы (PoE/POE/РоЕ→poe_switch, шкаф СВН→shk_svn)."""
    s = normalize_entity_value(value).translate(_HOMOGLYPH)
    if "poe" in s and ("коммутатор" in s or "kommytatop" in s or "switch" in s):
        return "poe_switch"
    if ("шкаф" in s or "шк" in s) and "свн" in s:
        return "shk_svn"
    return s


def _cable_family(value: Any) -> str:
    """Семейство/категория кабеля без сечения (для identity-сопоставления)."""
    fam = normalize_cable_value(value)
    fam = re.sub(r"\d+x\d+x[\d.]+", "", fam)   # 1x2x0.5
    fam = re.sub(r"\d+x[\d.]+", "", fam)        # 2x0.5
    return fam.strip()


def _power_kind(value: Any) -> str:
    raw = normalize_entity_value(value)
    s = normalize_power_value(value)
    if "ибп" in raw:
        return "ups"
    if "категори" in raw:
        return "category"
    if s.endswith("b"):
        return "voltage"
    if s.endswith("a"):
        return "current"
    return "power"


# ─── identity / match keys ──────────────────────────────────────────────────


def _f(entity: dict, key: str) -> str:
    fields = entity.get("fields") or {}
    return _clean(fields.get(key))


def make_entity_match_key(entity: dict) -> str:
    """Точный ключ (exact match: одинаковая сущность с одинаковым значением)."""
    et = entity.get("entity_type") or "unknown"
    if et == "stamp_field":
        return f"stamp_field|{normalize_entity_subject(entity.get('subject') or entity.get('name'))}"
    if et == "norm_reference":
        return f"norm_reference|{_norm_full(entity.get('value') or entity.get('name'))}"
    if et == "equipment":
        return f"equipment|{_equip_canon(entity.get('value') or entity.get('name'))}"
    if et == "cable":
        return f"cable|{normalize_cable_value(entity.get('value') or entity.get('name'))}"
    if et == "power_supply":
        return f"power_supply|{normalize_power_value(entity.get('value') or entity.get('name'))}"
    if et == "contents_item":
        return f"contents_item|{normalize_entity_value(_f(entity, 'document_code'))}|" \
               f"{normalize_entity_value(_f(entity, 'sheet_name') or entity.get('name'))}"
    if et == "change_log_item":
        return f"change_log_item|{normalize_entity_value(_f(entity, 'change_no') or entity.get('name'))}|" \
               f"{normalize_entity_value(_f(entity, 'sheet'))}"
    if et == "table_row":
        return f"table_row|{normalize_entity_value(entity.get('value'))}"
    return f"{et}|{normalize_entity_value(entity.get('value') or entity.get('name') or entity.get('subject'))}"


def make_entity_identity_key(entity: dict) -> str:
    """Грубый «логический» ключ: та же сущность, даже если значение изменилось."""
    et = entity.get("entity_type") or "unknown"
    if et == "norm_reference":
        return f"norm_reference|{_norm_base(entity.get('value') or entity.get('name'))}"
    if et == "cable":
        return f"cable|{_cable_family(entity.get('value') or entity.get('name'))}"
    if et == "power_supply":
        return f"power_supply|{_power_kind(entity.get('value') or entity.get('name'))}"
    if et == "contents_item":
        return f"contents_item|{normalize_entity_value(_f(entity, 'sheet_name') or entity.get('name'))}"
    if et == "change_log_item":
        return f"change_log_item|{normalize_entity_value(_f(entity, 'change_no') or entity.get('name'))}"
    if et == "table_row":
        cells = (entity.get("fields") or {}).get("cells") or []
        first = cells[0] if isinstance(cells, list) and cells else entity.get("value")
        return f"table_row|{normalize_entity_value(first)}"
    if et == "requirement":
        toks = normalize_entity_value(entity.get("value")).split()
        return f"requirement|{' '.join(toks[:6])}"
    # stamp/equipment/scheme_component/document_section: identity == match
    return make_entity_match_key(entity)


# ─── helpers ────────────────────────────────────────────────────────────────


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _safe(value: Any) -> str:
    s = _clean(value) or "na"
    return "".join(c if (c.isalnum() or c in "-_") else "_" for c in s)[:48]


def make_delta_id(left_entity_id: Any, right_entity_id: Any, field: Any) -> str:
    """Детерминированный id дельты (entity_id'ы уникальны → id уникален)."""
    return f"delta_{_safe(left_entity_id)}__{_safe(right_entity_id)}__{_safe(field or 'presence')}"


def _ev(entity: Optional[dict]) -> dict:
    e = (entity or {}).get("evidence") if entity else None
    e = e or {}
    return {
        "quote": _clean(e.get("quote")),
        "source": _clean(e.get("source")),
        "block_id": e.get("block_id"),
        "page_number": e.get("page_number"),
    }


def _possible_ocr_noise(value: str) -> bool:
    n = normalize_entity_value(value)
    return bool(n) and not re.search(r"[0-9a-zа-я]", n)


def _is_low_info(entity: dict) -> bool:
    et = entity.get("entity_type")
    quote = _clean((entity.get("evidence") or {}).get("quote"))
    val = normalize_entity_value(entity.get("value") or entity.get("name"))
    return (et in ("unknown", "scheme_connection_hint")) and (not quote or len(val) < 2)


# ─── value compare per type ─────────────────────────────────────────────────

_FIELDS_ONLY_TYPES = {"table_row", "contents_item", "change_log_item"}


def _norm_value_for_type(entity_type: str, value: Any) -> str:
    if entity_type == "cable":
        return normalize_cable_value(value)
    if entity_type == "power_supply":
        return normalize_power_value(value)
    if entity_type == "norm_reference":
        return _norm_full(value)
    if entity_type == "equipment":
        return _equip_canon(value)
    return normalize_entity_value(value)


def _type_changed_flag(entity_type: str) -> Optional[str]:
    if entity_type == "stamp_field":
        return "stamp_field_changed"
    if entity_type == "requirement":
        return "requirement_text_changed"
    return None


def compare_entity_fields(left_entity: dict, right_entity: dict,
                          options: Optional[dict] = None) -> list[dict]:
    """Сравнить словари ``fields`` двух сущностей (ключ за ключом)."""
    lf = left_entity.get("fields") or {}
    rf = right_entity.get("fields") or {}
    out: list[dict] = []
    for k in sorted(set(lf.keys()) | set(rf.keys())):
        lv, rv = lf.get(k), rf.get(k)
        if k == "cells":
            ln = normalize_entity_value(" | ".join(str(c) for c in (lv or [])))
            rn = normalize_entity_value(" | ".join(str(c) for c in (rv or [])))
            if ln != rn:
                out.append({"field": "fields.cells",
                            "old_value": " | ".join(str(c) for c in (lv or [])),
                            "new_value": " | ".join(str(c) for c in (rv or [])),
                            "flags": ["table_row_changed"],
                            "numeric_change": extract_numeric_tokens(lv) != extract_numeric_tokens(rv)})
            continue
        if normalize_entity_value(lv) != normalize_entity_value(rv):
            out.append({"field": f"fields.{k}", "old_value": _clean(lv),
                        "new_value": _clean(rv), "flags": [],
                        "numeric_change": extract_numeric_tokens(lv) != extract_numeric_tokens(rv)})
    return out


def compare_matched_entities(left_entity: dict, right_entity: dict,
                             options: Optional[dict] = None) -> list[dict]:
    """Список field-level отличий пары сущностей ([] = unchanged)."""
    et = left_entity.get("entity_type") or right_entity.get("entity_type") or "unknown"
    out: list[dict] = []
    if et not in _FIELDS_ONLY_TYPES:
        # скалярное значение
        lv = left_entity.get("value") or left_entity.get("name")
        rv = right_entity.get("value") or right_entity.get("name")
        if _norm_value_for_type(et, lv) != _norm_value_for_type(et, rv):
            flags = []
            tflag = _type_changed_flag(et)
            if tflag:
                flags.append(tflag)
            out.append({"field": "value", "old_value": _clean(lv), "new_value": _clean(rv),
                        "flags": flags,
                        "numeric_change": extract_numeric_tokens(lv) != extract_numeric_tokens(rv)})
        # unit
        lu, ru = left_entity.get("unit"), right_entity.get("unit")
        if normalize_entity_value(lu) != normalize_entity_value(ru):
            out.append({"field": "unit", "old_value": _clean(lu), "new_value": _clean(ru),
                        "flags": [], "numeric_change": False})
    out += compare_entity_fields(left_entity, right_entity, options)
    return out


# ─── matching внутри пары блоков ────────────────────────────────────────────


def build_entity_match_candidates(left_entities: list[dict], right_entities: list[dict],
                                  options: Optional[dict] = None) -> list[dict]:
    """Кандидаты сопоставления (exact_key → identity → fuzzy). Сорт по score."""
    fuzzy_thr = _opt(options, "fuzzy_threshold")
    cands: list[dict] = []

    # exact (match_key)
    rk: dict[str, list[int]] = {}
    for ri, re_ in enumerate(right_entities):
        rk.setdefault(make_entity_match_key(re_), []).append(ri)
    for li, le in enumerate(left_entities):
        for ri in rk.get(make_entity_match_key(le), []):
            if le.get("entity_type") != right_entities[ri].get("entity_type"):
                continue
            method = "subject_type" if le.get("entity_type") == "stamp_field" else "exact_key"
            cands.append({"li": li, "ri": ri, "method": method, "score": 1.0,
                          "reasons": ["match_key"]})

    # identity (logical same entity, value may differ)
    rik: dict[str, list[int]] = {}
    for ri, re_ in enumerate(right_entities):
        rik.setdefault(make_entity_identity_key(re_), []).append(ri)
    for li, le in enumerate(left_entities):
        et = le.get("entity_type")
        for ri in rik.get(make_entity_identity_key(le), []):
            if et != right_entities[ri].get("entity_type"):
                continue
            method = "numeric_overlap" if et in ("cable", "power_supply") else "normalized_key"
            cands.append({"li": li, "ri": ri, "method": method, "score": 0.8,
                          "reasons": ["identity_key"]})

    # fuzzy (requirement / document_section по тексту)
    for li, le in enumerate(left_entities):
        et = le.get("entity_type")
        if et not in ("requirement", "document_section"):
            continue
        lt = normalize_entity_value(le.get("value") or le.get("name"))
        for ri, re_ in enumerate(right_entities):
            if re_.get("entity_type") != et:
                continue
            rt = normalize_entity_value(re_.get("value") or re_.get("name"))
            ratio = SequenceMatcher(None, lt, rt).ratio() if lt and rt else 0.0
            if ratio >= fuzzy_thr:
                cands.append({"li": li, "ri": ri, "method": "fuzzy",
                              "score": round(ratio, 4), "reasons": [f"text_ratio:{ratio:.2f}"]})

    cands.sort(key=lambda c: (-c["score"], c["li"], c["ri"]))
    return cands


def match_entities(left_entities: list[dict], right_entities: list[dict],
                   options: Optional[dict] = None) -> dict:
    """Жадно (1:1) сопоставить сущности. Возвращает pairs + unmatched индексы."""
    cands = build_entity_match_candidates(left_entities, right_entities, options)
    used_l: set[int] = set()
    used_r: set[int] = set()
    pairs: list[dict] = []
    for c in cands:
        if c["li"] in used_l or c["ri"] in used_r:
            continue
        used_l.add(c["li"])
        used_r.add(c["ri"])
        pairs.append(c)
    unmatched_l = [i for i in range(len(left_entities)) if i not in used_l]
    unmatched_r = [i for i in range(len(right_entities)) if i not in used_r]
    return {"pairs": pairs, "unmatched_left": unmatched_l, "unmatched_right": unmatched_r}


# ─── confidence / delta builders ────────────────────────────────────────────


def _confidence(delta_type: str, method: str, score: float, flags: list[str]) -> float:
    if delta_type == "changed":
        base = 0.85 if method in ("exact_key", "subject_type") else (
            0.65 if method in ("normalized_key", "numeric_overlap") else 0.5)
        if "numeric_change" in flags:
            base = max(base, 0.72)
        if method == "fuzzy":
            base = min(base, max(0.45, score))
    elif delta_type in ("added", "removed"):
        base = 0.7
    elif delta_type == "uncertain":
        base = 0.3
    else:
        base = 0.5
    if "left_evidence_missing" in flags or "right_evidence_missing" in flags:
        base -= 0.08
    if "low_match_score" in flags:
        base -= 0.08
    return round(max(0.0, min(1.0, base)), 3)


def _conf_bucket(conf: float) -> str:
    if conf >= 0.75:
        return "high"
    if conf >= 0.45:
        return "medium"
    return "low"


def _changed_delta(le: dict, re_: dict, fdiff: dict, match: dict,
                   block_match_id: Optional[str]) -> dict:
    flags = list(fdiff.get("flags", []))
    if fdiff.get("numeric_change"):
        flags.append("numeric_change")
    if match["method"] == "fuzzy":
        flags.append("fuzzy_match")
    if match["score"] < _DEFAULTS["low_match_score"]:
        flags.append("low_match_score")
    if not _clean((le.get("evidence") or {}).get("quote")):
        flags.append("left_evidence_missing")
    if not _clean((re_.get("evidence") or {}).get("quote")):
        flags.append("right_evidence_missing")
    if _possible_ocr_noise(fdiff["old_value"]) or _possible_ocr_noise(fdiff["new_value"]):
        flags.append("possible_ocr_noise")
    if match["method"] == "fuzzy" or "low_match_score" in flags:
        flags.append("needs_human_review")
    flags = sorted(set(flags))
    conf = _confidence("changed", match["method"], match["score"], flags)
    et = le.get("entity_type") or re_.get("entity_type")
    return {
        "delta_id": make_delta_id(le.get("entity_id"), re_.get("entity_id"), fdiff["field"]),
        "delta_type": "changed",
        "entity_type": et,
        "semantic_group": le.get("semantic_group") or re_.get("semantic_group") or "unknown",
        "left_entity_id": le.get("entity_id"),
        "right_entity_id": re_.get("entity_id"),
        "left_block_id": le.get("block_id"),
        "right_block_id": re_.get("block_id"),
        "block_match_id": block_match_id,
        "page_numbers": {"left": le.get("page_number"), "right": re_.get("page_number")},
        "subject": le.get("subject") or re_.get("subject") or le.get("name") or re_.get("name"),
        "field": fdiff["field"],
        "old_value": fdiff["old_value"],
        "new_value": fdiff["new_value"],
        "change_summary": f"{et}: {fdiff['field']} «{fdiff['old_value']}» → «{fdiff['new_value']}»",
        "confidence": conf,
        "evidence": {"left": _ev(le), "right": _ev(re_)},
        "match": {"method": match["method"], "score": round(float(match["score"]), 4),
                  "reasons": match.get("reasons", [])},
        "quality_flags": flags,
    }


def _one_sided_delta(entity: dict, side: str, block_match_id: Optional[str],
                     on_unmatched_block: bool) -> dict:
    """side='left' → removed; side='right' → added; low-info → uncertain."""
    low = _is_low_info(entity)
    delta_type = "uncertain" if low else ("removed" if side == "left" else "added")
    flags = ["one_sided_entity"]
    if on_unmatched_block:
        flags.append("one_sided_entity")
    if side == "left":
        flags.append("right_evidence_missing")
    else:
        flags.append("left_evidence_missing")
    if _possible_ocr_noise(entity.get("value") or entity.get("name") or ""):
        flags.append("possible_ocr_noise")
    if low:
        flags.append("needs_human_review")
    flags = sorted(set(flags))
    method = "fallback" if on_unmatched_block else "exact_key"
    conf = _confidence(delta_type, method, 1.0, flags)
    if on_unmatched_block and not low:
        conf = round(min(conf, 0.6), 3)
    et = entity.get("entity_type")
    val = entity.get("value") or entity.get("name") or entity.get("subject")
    is_left = side == "left"
    return {
        "delta_id": make_delta_id(entity.get("entity_id") if is_left else None,
                                  None if is_left else entity.get("entity_id"), "presence"),
        "delta_type": delta_type,
        "entity_type": et,
        "semantic_group": entity.get("semantic_group") or "unknown",
        "left_entity_id": entity.get("entity_id") if is_left else None,
        "right_entity_id": None if is_left else entity.get("entity_id"),
        "left_block_id": entity.get("block_id") if is_left else None,
        "right_block_id": None if is_left else entity.get("block_id"),
        "block_match_id": block_match_id,
        "page_numbers": {"left": entity.get("page_number") if is_left else None,
                         "right": None if is_left else entity.get("page_number")},
        "subject": entity.get("subject") or entity.get("name"),
        "field": "presence",
        "old_value": _clean(val) if is_left else "",
        "new_value": "" if is_left else _clean(val),
        "change_summary": (f"{delta_type}: {et} «{_clean(val)}»"),
        "confidence": conf,
        "evidence": {"left": _ev(entity) if is_left else _ev(None),
                     "right": _ev(None) if is_left else _ev(entity)},
        "match": {"method": method, "score": 0.0, "reasons": ["one_sided"]},
        "quality_flags": flags,
    }


# ─── per-block diff ─────────────────────────────────────────────────────────


def _entity_index(report: dict) -> tuple[dict, dict]:
    left_by_id = {e["entity_id"]: e for e in report.get("left_entities") or []}
    right_by_id = {e["entity_id"]: e for e in report.get("right_entities") or []}
    return left_by_id, right_by_id


def diff_matched_block_entities(entity_report: dict, block_entity_pair: dict,
                                options: Optional[dict] = None) -> dict:
    """Diff сущностей одной пары сопоставленных блоков (этап 3)."""
    options = options or {}
    left_by_id = options.get("_left_index")
    right_by_id = options.get("_right_index")
    if left_by_id is None or right_by_id is None:
        left_by_id, right_by_id = _entity_index(entity_report)

    bm_id = block_entity_pair.get("block_match_id")
    lbid = block_entity_pair.get("left_block_id")
    rbid = block_entity_pair.get("right_block_id")
    left_ents = [left_by_id[i] for i in block_entity_pair.get("left_entities", []) if i in left_by_id]
    right_ents = [right_by_id[i] for i in block_entity_pair.get("right_entities", []) if i in right_by_id]

    res = match_entities(left_ents, right_ents, options)
    deltas: list[dict] = []
    matched_pairs: list[dict] = []
    unmatched_left_briefs: list[dict] = []
    unmatched_right_briefs: list[dict] = []
    matched_total = 0
    matched_unchanged = 0

    for c in res["pairs"]:
        le, re_ = left_ents[c["li"]], right_ents[c["ri"]]
        fdiffs = compare_matched_entities(le, re_, options)
        matched_total += 1
        delta_ids: list[str] = []
        for fd in fdiffs:
            d = _changed_delta(le, re_, fd, c, bm_id)
            deltas.append(d)
            delta_ids.append(d["delta_id"])
        if not fdiffs:
            matched_unchanged += 1
        matched_pairs.append({
            "left_entity_id": le.get("entity_id"),
            "right_entity_id": re_.get("entity_id"),
            "block_match_id": bm_id,
            "entity_type": le.get("entity_type"),
            "method": c["method"],
            "score": round(float(c["score"]), 4),
            "changed": bool(fdiffs),
            "delta_ids": delta_ids,
        })

    for i in res["unmatched_left"]:
        le = left_ents[i]
        d = _one_sided_delta(le, "left", bm_id, on_unmatched_block=False)
        deltas.append(d)
        unmatched_left_briefs.append(_brief(le, d["delta_id"]))
    for i in res["unmatched_right"]:
        re_ = right_ents[i]
        d = _one_sided_delta(re_, "right", bm_id, on_unmatched_block=False)
        deltas.append(d)
        unmatched_right_briefs.append(_brief(re_, d["delta_id"]))

    block_summary = {
        "block_match_id": bm_id,
        "left_block_id": lbid,
        "right_block_id": rbid,
        "deltas_total": len(deltas),
        "added_total": sum(1 for d in deltas if d["delta_type"] == "added"),
        "removed_total": sum(1 for d in deltas if d["delta_type"] == "removed"),
        "changed_total": sum(1 for d in deltas if d["delta_type"] == "changed"),
        "uncertain_total": sum(1 for d in deltas if d["delta_type"] == "uncertain"),
        "quality_flags": list(block_entity_pair.get("quality_flags", [])),
    }
    return {
        "block_match_id": bm_id, "left_block_id": lbid, "right_block_id": rbid,
        "deltas": deltas, "matched_pairs": matched_pairs,
        "matched_total": matched_total, "matched_unchanged": matched_unchanged,
        "unmatched_left": unmatched_left_briefs, "unmatched_right": unmatched_right_briefs,
        "block_summary": block_summary,
    }


def _brief(entity: dict, delta_id: Optional[str]) -> dict:
    return {
        "entity_id": entity.get("entity_id"),
        "entity_type": entity.get("entity_type"),
        "block_id": entity.get("block_id"),
        "page_number": entity.get("page_number"),
        "delta_id": delta_id,
    }


# ─── orchestration ──────────────────────────────────────────────────────────


def diff_entity_extraction_report(entity_report: dict, options: Optional[dict] = None) -> dict:
    """Полный deterministic diff по entity_extraction_report (этап 3)."""
    entity_report = entity_report or {}
    options = dict(options or {})
    left_by_id, right_by_id = _entity_index(entity_report)
    options["_left_index"] = left_by_id
    options["_right_index"] = right_by_id

    deltas: list[dict] = []
    matched_pairs: list[dict] = []
    block_summaries: list[dict] = []
    unmatched_left: list[dict] = []
    unmatched_right: list[dict] = []
    matched_total = 0
    matched_unchanged = 0

    for bpair in entity_report.get("matched_block_entities") or []:
        res = diff_matched_block_entities(entity_report, bpair, options)
        deltas.extend(res["deltas"])
        matched_pairs.extend(res["matched_pairs"])
        block_summaries.append(res["block_summary"])
        unmatched_left.extend(res["unmatched_left"])
        unmatched_right.extend(res["unmatched_right"])
        matched_total += res["matched_total"]
        matched_unchanged += res["matched_unchanged"]

    # сущности целиком непарных блоков (нет block_match) → one-sided
    for entry in entity_report.get("unmatched_left_block_entities") or []:
        for eid in entry.get("entities", []):
            e = left_by_id.get(eid)
            if not e:
                continue
            d = _one_sided_delta(e, "left", None, on_unmatched_block=True)
            deltas.append(d)
            unmatched_left.append(_brief(e, d["delta_id"]))
    for entry in entity_report.get("unmatched_right_block_entities") or []:
        for eid in entry.get("entities", []):
            e = right_by_id.get(eid)
            if not e:
                continue
            d = _one_sided_delta(e, "right", None, on_unmatched_block=True)
            deltas.append(d)
            unmatched_right.append(_brief(e, d["delta_id"]))

    # warnings
    warnings: list[str] = []
    if not (entity_report.get("matched_block_entities") or []):
        warnings.append("no_matched_block_entities")
    uncertain_n = sum(1 for d in deltas if d["delta_type"] == "uncertain")
    if uncertain_n:
        warnings.append(f"uncertain_deltas: {uncertain_n}")
    ocr_noise_n = sum(1 for d in deltas if "possible_ocr_noise" in d["quality_flags"])
    if ocr_noise_n:
        warnings.append(f"possible_ocr_noise_deltas: {ocr_noise_n}")

    # summary
    by_type: Counter = Counter(d["entity_type"] for d in deltas)
    by_group: Counter = Counter(d["semantic_group"] for d in deltas)
    by_delta: Counter = Counter(d["delta_type"] for d in deltas)
    by_conf = {"high": 0, "medium": 0, "low": 0}
    for d in deltas:
        by_conf[_conf_bucket(d["confidence"])] += 1

    summary = {
        "deltas_total": len(deltas),
        "added_total": by_delta.get("added", 0),
        "removed_total": by_delta.get("removed", 0),
        "changed_total": by_delta.get("changed", 0),
        "uncertain_total": by_delta.get("uncertain", 0),
        "matched_entities_total": matched_total,
        "matched_unchanged_total": matched_unchanged,
        "unmatched_left_entities_total": len(unmatched_left),
        "unmatched_right_entities_total": len(unmatched_right),
        "by_entity_type": dict(by_type),
        "by_semantic_group": dict(by_group),
        "by_delta_type": dict(by_delta),
        "by_confidence": by_conf,
        "warnings_count": len(warnings),
    }

    return {
        "version": REPORT_VERSION,
        "kind": REPORT_KIND,
        "summary": summary,
        "deltas": deltas,
        "matched_entity_pairs": matched_pairs,
        "unmatched_left_entities": unmatched_left,
        "unmatched_right_entities": unmatched_right,
        "block_summaries": block_summaries,
        "warnings": warnings,
    }


# ─── write_entity_diff_report (атомарная запись) ────────────────────────────


def write_entity_diff_report(out_path: str | Path, report: dict) -> Path:
    """Атомарно записать отчёт diff в JSON-файл (tmp + ``os.replace``)."""
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
    "diff_entity_extraction_report",
    "diff_matched_block_entities",
    "match_entities",
    "build_entity_match_candidates",
    "make_entity_match_key",
    "make_entity_identity_key",
    "compare_matched_entities",
    "compare_entity_fields",
    "normalize_entity_value",
    "normalize_entity_subject",
    "normalize_cable_value",
    "normalize_power_value",
    "extract_numeric_tokens",
    "make_delta_id",
    "write_entity_diff_report",
]
