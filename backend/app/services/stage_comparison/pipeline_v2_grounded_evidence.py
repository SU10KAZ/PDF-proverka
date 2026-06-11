# -*- coding: utf-8 -*-
"""Pipeline V2 — Grounded Vision Evidence Integration (mark-only).

Связывает deterministic дельты (`entity_diff_report.deltas`) с подтверждёнными
визуальными сущностями/изменениями из `graphic_vision_grounding_report` и строит
``grounded_evidence_report.json``.

Это **mark-only evidence layer**:
* НЕ создаёт замечаний;
* НЕ enforce'ит и не меняет deterministic дельты;
* НЕ применяет связи;
* `rejected_*` / `ungrounded` НИКОГДА не используются как факт для critic'а.

Поток:

    entity_diff_report.deltas
      + graphic_vision_grounding_report (grounded/weak/rejected сущности+изменения)
      + (optional) block_link_preview / visual_equivalence_gate (page-контекст)
      = per-delta evidence cards (grounded | weak | none | conflict | rejected_only)

Каждая дельта, чьи блоки покрыты grounding-итемом, получает карточку с уровнем
evidence и пометкой `use_in_critic` (True только для grounded/weak). Карточки
потом подмешиваются в delta-explanation prompt как supporting / weak evidence.

Нормализация и извлечение значимых токенов переиспользуются из grounding-модуля,
чтобы основной путь и evidence-слой канонизировали значения одинаково.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Optional

from .pipeline_v2_graphic_vision_grounding import (
    GROUNDED,
    WEAKLY_GROUNDED,
    UNGROUNDED,
    REJECTED_NOOP,
    normalize_engineering_token,
    _compact as _compact_token,
    _salient_values,
)

REPORT_VERSION = 1
REPORT_KIND = "stage_comparison_pipeline_v2_grounded_evidence"

# ─── evidence levels (per-delta) ─────────────────────────────────────────────
LEVEL_GROUNDED = "grounded"
LEVEL_WEAK = "weak"
LEVEL_NONE = "none"
LEVEL_CONFLICT = "conflict"
LEVEL_REJECTED_ONLY = "rejected_only"

# ─── fact levels (per-evidence entry) ────────────────────────────────────────
FACT_CONFIRMED = "confirmed"
FACT_WEAK = "weak"
FACT_NOT_FACT = "not_fact"
FACT_REJECTED = "rejected"

_USABLE_LEVELS = {LEVEL_GROUNDED, LEVEL_WEAK}

# Какие grounding-статусы считаются «rejected» (никогда не факт).
_REJECTED_STATUS_PREFIX = "rejected_"

# Сколько evidence-записей максимум кладём в карточку (защита от раздувания).
_MAX_EVIDENCE_PER_DELTA = 6

# Минимальный score, ниже которого совпадение НЕ считается evidence вообще.
_MIN_MATCH_SCORE = 0.30

# дизайнатор аппарата в нормализованной строке: «qf5», «sa1», «1qf2», «ta3»
_RE_DESIGNATOR = re.compile(r"\d*[a-zщцшгджзфйюя]{1,6}\d+[a-z0-9.]*")


# ─── нормализация / токены ───────────────────────────────────────────────────


def normalize_evidence_token(value: Any, *, compact: bool = False) -> str:
    """Канонизировать значение для evidence-сравнения.

    Делегирует в `normalize_engineering_token` (NFKC, lower, «А»→«A», «х/×»→«x»,
    дефисы, «400 А»→«400a», «QF 5»→«qf5» при compact, «4х185»→«4x185», метки
    «Pp/Рр/Ip/Iр»). `compact=True` убирает пробелы (для подстрочного поиска
    маркировок). Канонизация консервативна — разные номиналы не схлопываются.
    """
    if compact:
        return _compact_token(value)
    return normalize_engineering_token(value)


def _salient_token_set(value: Any) -> set:
    """Набор значимых токенов значения: номиналы + сечения + мощности."""
    norm = normalize_evidence_token(value)
    if not norm:
        return set()
    sv = _salient_values(norm)
    out = set()
    out |= sv.get("ratings", set())
    out |= sv.get("sections", set())
    out |= sv.get("powers", set())
    return out


def _designators(value: Any) -> set:
    """Дизайнаторы (qf5, sa1, ta3 …) из значения."""
    norm = normalize_evidence_token(value)
    return {m.group(0) for m in _RE_DESIGNATOR.finditer(norm)
            # отбросить «голые» номиналы вида «400a» (число+a) — это рейтинг
            if not re.fullmatch(r"\d+(?:\.\d+)?a", m.group(0))}


def _designator_rating_pairs(value: Any) -> set:
    """Пары (дизайнатор, номинал) из значения вида «QF5 (400А)» → {('qf5','400a')}."""
    norm = normalize_evidence_token(value)
    desigs = _designators(value)
    ratings = _salient_values(norm).get("ratings", set())
    if not desigs or not ratings:
        return set()
    # типичный случай: один дизайнатор + один номинал в скобках
    return {(d, r) for d in desigs for r in ratings}


# ─── grounding index ─────────────────────────────────────────────────────────


def _fact_level_for_status(status: str) -> str:
    s = (status or "").strip().lower()
    if s == GROUNDED:
        return FACT_CONFIRMED
    if s == WEAKLY_GROUNDED:
        return FACT_WEAK
    if s.startswith(_REJECTED_STATUS_PREFIX):
        return FACT_REJECTED
    return FACT_NOT_FACT  # ungrounded / unknown


def _candidate_from_entity(entity: dict, side: str, item: dict) -> dict:
    value = entity.get("value")
    status = entity.get("status") or ""
    norm = normalize_evidence_token(value)
    return {
        "origin": "entity",
        "side": side,                      # "old" | "new"
        "status": status,
        "fact_level": _fact_level_for_status(status),
        "value": value,
        "normalized": norm,
        "tokens": _salient_token_set(value),
        "designators": _designators(value),
        "designator_ratings": _designator_rating_pairs(value),
        "matched_values": {normalize_evidence_token(v)
                           for v in (entity.get("matched_values") or [])},
        "reason": entity.get("reason"),
    }


def _candidate_from_change(change: dict, item: dict, *, rejected: bool = False) -> dict:
    value = change.get("value")
    status = change.get("status") or ""
    old_vals = {normalize_evidence_token(v) for v in (change.get("old_values") or [])}
    new_vals = {normalize_evidence_token(v) for v in (change.get("new_values") or [])}
    # часть значимых токенов может сидеть только в тексте value
    text_tokens = _salient_token_set(value)
    return {
        "origin": "rejected_change" if rejected else "change",
        "side": change.get("side"),
        "status": status,
        "fact_level": _fact_level_for_status(status),
        "value": value,
        "normalized": normalize_evidence_token(value),
        "old_values": old_vals,
        "new_values": new_vals,
        "text_tokens": text_tokens,
        "reason": change.get("reason"),
    }


def build_grounding_index(grounding_report: Any) -> dict:
    """Индекс grounding-итемов по блок-парам + одиночным блокам.

    Возвращает ``{"items": [...], "by_pair": {...}, "by_left": {...},
    "by_right": {...}}``. Каждый item — нормализованный набор кандидатов
    (entities old/new, changes, rejected) + контекст блоков.
    """
    items: list[dict] = []
    by_pair: dict = {}
    by_left: dict = {}
    by_right: dict = {}
    report = grounding_report if isinstance(grounding_report, dict) else {}
    for raw in report.get("items") or []:
        if not isinstance(raw, dict):
            continue
        lb = raw.get("left_block_id")
        rb = raw.get("right_block_id")
        item = {
            "item_id": raw.get("item_id"),
            "left_block_id": lb,
            "right_block_id": rb,
            "graphic_type": raw.get("graphic_type"),
            "entities_old": [_candidate_from_entity(e, "old", raw)
                             for e in (raw.get("grounded_entities_old") or [])
                             if isinstance(e, dict)],
            "entities_new": [_candidate_from_entity(e, "new", raw)
                             for e in (raw.get("grounded_entities_new") or [])
                             if isinstance(e, dict)],
            "changes": [_candidate_from_change(c, raw)
                        for c in (raw.get("grounded_changes") or [])
                        if isinstance(c, dict)],
            "rejected": ([_candidate_from_entity(e, "either", raw)
                          for e in (raw.get("rejected_entities") or [])
                          if isinstance(e, dict)]
                         + [_candidate_from_change(c, raw, rejected=True)
                            for c in (raw.get("rejected_changes") or [])
                            if isinstance(c, dict)]),
        }
        items.append(item)
        if lb and rb:
            by_pair.setdefault((lb, rb), []).append(item)
        if lb:
            by_left.setdefault(lb, []).append(item)
        if rb:
            by_right.setdefault(rb, []).append(item)
    return {"items": items, "by_pair": by_pair, "by_left": by_left, "by_right": by_right}


def build_page_index(*reports: Any) -> dict:
    """Карта block-pair / одиночный block → (left_page, right_page).

    Источники (в порядке приоритета): visual_equivalence_gate.block_pairs,
    block_link_preview.block_links. Fail-soft: мусор пропускается.
    """
    by_pair: dict = {}
    by_block: dict = {}
    for rep in reports:
        if not isinstance(rep, dict):
            continue
        rows = rep.get("block_pairs")
        if not isinstance(rows, list):
            rows = rep.get("block_links") if isinstance(rep.get("block_links"), list) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            lb = row.get("left_block_id")
            rb = row.get("right_block_id")
            lp = row.get("left_page_number")
            rp = row.get("right_page_number")
            if lb and rb:
                by_pair.setdefault((lb, rb), (lp, rp))
            if lb and lb not in by_block:
                by_block[lb] = (lp, rp)
            if rb and rb not in by_block:
                by_block[rb] = (lp, rp)
    return {"by_pair": by_pair, "by_block": by_block}


def _items_for_delta(delta: dict, index: dict) -> list[dict]:
    """grounding-итемы, покрывающие блоки дельты (по паре, затем по сторонам)."""
    lb = delta.get("left_block_id")
    rb = delta.get("right_block_id")
    seen_ids = set()
    out: list[dict] = []

    def _add(items):
        for it in items or []:
            key = id(it)
            if key not in seen_ids:
                seen_ids.add(key)
                out.append(it)

    if lb and rb:
        _add(index["by_pair"].get((lb, rb)))
    if lb:
        _add(index["by_left"].get(lb))
    if rb:
        _add(index["by_right"].get(rb))
    return out


# ─── scoring ─────────────────────────────────────────────────────────────────


def score_grounded_evidence_match(delta: dict, candidate: dict) -> float:
    """Оценить, насколько grounding-кандидат подтверждает дельту (0.0–1.0).

    Учитывает тип дельты (changed нуждается в old И new; added/removed — в одной
    стороне) и совпадение значимых токенов (номиналы/сечения/мощности).
    Возвращает 0.0, если кандидат не относится к нужной стороне дельты.
    """
    dtype = (delta.get("delta_type") or "").strip().lower()
    old_tokens = _salient_token_set(delta.get("old_value"))
    new_tokens = _salient_token_set(delta.get("new_value"))

    origin = candidate.get("origin")

    # — change-кандидат (несёт обе стороны) —
    if origin in ("change", "rejected_change"):
        c_old = set(candidate.get("old_values") or set())
        c_new = set(candidate.get("new_values") or set())
        text = set(candidate.get("text_tokens") or set())
        if dtype == "changed":
            old_hit = bool(old_tokens & (c_old | text)) if old_tokens else False
            new_hit = bool(new_tokens & (c_new | text)) if new_tokens else False
            if old_hit and new_hit:
                return 0.95
            if old_hit or new_hit:
                return 0.6
            return 0.0
        # added/removed — сравниваем с релевантной стороной change'а
        side_tokens = new_tokens if dtype == "added" else old_tokens
        pool = (c_new | text) if dtype == "added" else (c_old | text)
        if side_tokens and side_tokens & pool:
            return 0.8
        return 0.0

    # — entity-кандидат (одна сторона) —
    side = candidate.get("side")
    ctokens = set(candidate.get("tokens") or set()) | set(candidate.get("matched_values") or set())
    if dtype == "added":
        if side in ("new", "either") and new_tokens and (new_tokens & ctokens):
            return 0.85
        return 0.0
    if dtype == "removed":
        if side in ("old", "either") and old_tokens and (old_tokens & ctokens):
            return 0.85
        return 0.0
    # changed: одиночная entity подтверждает только ОДНУ сторону → частично
    if dtype == "changed":
        if side in ("old", "either") and old_tokens and (old_tokens & ctokens):
            return 0.55
        if side in ("new", "either") and new_tokens and (new_tokens & ctokens):
            return 0.55
        return 0.0
    return 0.0


# ─── matching ────────────────────────────────────────────────────────────────


def _page_context(delta: dict, page_index: Optional[dict]) -> tuple:
    if not isinstance(page_index, dict):
        return (None, None)
    lb = delta.get("left_block_id")
    rb = delta.get("right_block_id")
    if lb and rb and (lb, rb) in page_index.get("by_pair", {}):
        return page_index["by_pair"][(lb, rb)]
    by_block = page_index.get("by_block", {})
    lp = by_block.get(lb, (None, None))[0] if lb else None
    rp = by_block.get(rb, (None, None))[1] if rb else None
    return (lp, rp)


def _designator_anchored_pair(delta: dict, item: dict) -> Optional[dict]:
    """Найти дизайнатор D с D(old_rating)=delta.old и D(new_rating)=delta.new.

    Это самый сильный сигнал: один и тот же аппарат сменил номинал, и оба
    номинала grounded по anchors. Возвращает evidence-dict или None.
    """
    if (delta.get("delta_type") or "").lower() != "changed":
        return None
    old_tokens = _salient_token_set(delta.get("old_value"))
    new_tokens = _salient_token_set(delta.get("new_value"))
    if not old_tokens or not new_tokens:
        return None
    old_by_desig: dict = {}
    for e in item["entities_old"]:
        if e["fact_level"] not in (FACT_CONFIRMED, FACT_WEAK):
            continue
        for d, r in e["designator_ratings"]:
            old_by_desig.setdefault(d, []).append((r, e))
    new_by_desig: dict = {}
    for e in item["entities_new"]:
        if e["fact_level"] not in (FACT_CONFIRMED, FACT_WEAK):
            continue
        for d, r in e["designator_ratings"]:
            new_by_desig.setdefault(d, []).append((r, e))
    for d in set(old_by_desig) & set(new_by_desig):
        old_match = next((e for r, e in old_by_desig[d] if r in old_tokens), None)
        new_match = next((e for r, e in new_by_desig[d] if r in new_tokens), None)
        if old_match and new_match:
            fl = (FACT_CONFIRMED
                  if old_match["fact_level"] == FACT_CONFIRMED
                  and new_match["fact_level"] == FACT_CONFIRMED
                  else FACT_WEAK)
            return {
                "source": "graphic_vision_grounding",
                "fact_level": fl,
                "status": (GROUNDED if fl == FACT_CONFIRMED else WEAKLY_GROUNDED),
                "kind": "designator_pair",
                "designator": d,
                "old_anchor": old_match["value"],
                "new_anchor": new_match["value"],
                "left_block_id": item.get("left_block_id"),
                "right_block_id": item.get("right_block_id"),
                "graphic_type": item.get("graphic_type"),
                "match_score": 0.97 if fl == FACT_CONFIRMED else 0.7,
                "reason": (f"designator {d.upper()} грунтован на обеих сторонах "
                           "с совпадающими номиналами"),
            }
    return None


def _value_pair_evidence(delta: dict, item: dict) -> Optional[dict]:
    """changed: delta.old ∈ grounded OLD entity И delta.new ∈ grounded NEW entity.

    Без общего дизайнатора (слабее, чем _designator_anchored_pair).
    """
    if (delta.get("delta_type") or "").lower() != "changed":
        return None
    old_tokens = _salient_token_set(delta.get("old_value"))
    new_tokens = _salient_token_set(delta.get("new_value"))
    if not old_tokens or not new_tokens:
        return None
    old_e = _best_entity(item["entities_old"], old_tokens, ("old", "either"))
    new_e = _best_entity(item["entities_new"], new_tokens, ("new", "either"))
    if not old_e or not new_e:
        return None
    fl = (FACT_CONFIRMED
          if old_e["fact_level"] == FACT_CONFIRMED and new_e["fact_level"] == FACT_CONFIRMED
          else FACT_WEAK)
    return {
        "source": "graphic_vision_grounding",
        "fact_level": fl,
        "status": (GROUNDED if fl == FACT_CONFIRMED else WEAKLY_GROUNDED),
        "kind": "value_pair",
        "old_anchor": old_e["value"],
        "new_anchor": new_e["value"],
        "left_block_id": item.get("left_block_id"),
        "right_block_id": item.get("right_block_id"),
        "graphic_type": item.get("graphic_type"),
        "match_score": 0.85 if fl == FACT_CONFIRMED else 0.6,
        "reason": "оба значения дельты найдены среди grounded сущностей блока",
    }


def _best_entity(entities: list, tokens: set, sides: tuple) -> Optional[dict]:
    """Лучшая (confirmed > weak) entity, чьи токены пересекают tokens."""
    best = None
    best_rank = -1
    for e in entities:
        if e["side"] not in sides:
            continue
        etok = set(e["tokens"]) | set(e["matched_values"])
        if not (tokens & etok):
            continue
        rank = 2 if e["fact_level"] == FACT_CONFIRMED else (1 if e["fact_level"] == FACT_WEAK else 0)
        if rank > best_rank:
            best, best_rank = e, rank
    return best


def _noop_contradicts(delta: dict, item: dict) -> Optional[dict]:
    """changed-дельта, но grounding пометил это изменение как noop (нет смены).

    Возвращает rejected-evidence (conflict-сигнал) или None.
    """
    if (delta.get("delta_type") or "").lower() != "changed":
        return None
    old_tokens = _salient_token_set(delta.get("old_value"))
    new_tokens = _salient_token_set(delta.get("new_value"))
    if not (old_tokens or new_tokens):
        return None
    for c in item["rejected"]:
        if c.get("status") != REJECTED_NOOP:
            continue
        text = set(c.get("text_tokens") or set()) | set(c.get("old_values") or set()) | set(c.get("new_values") or set())
        if (old_tokens & text) or (new_tokens & text):
            return {
                "source": "graphic_vision_grounding",
                "fact_level": FACT_REJECTED,
                "status": REJECTED_NOOP,
                "kind": "rejected_noop",
                "old_anchor": c.get("value"),
                "new_anchor": c.get("value"),
                "left_block_id": item.get("left_block_id"),
                "right_block_id": item.get("right_block_id"),
                "graphic_type": item.get("graphic_type"),
                "match_score": 0.5,
                "reason": "grounding пометил это изменение как noop (без изменений)",
            }
    return None


def _rejected_evidence(delta: dict, item: dict) -> list[dict]:
    """rejected кандидаты (designator_range / artificial_series / noop / …),
    чьи токены пересекают значения дельты — НИКОГДА не факт, только пометка."""
    old_tokens = _salient_token_set(delta.get("old_value"))
    new_tokens = _salient_token_set(delta.get("new_value"))
    all_tokens = old_tokens | new_tokens
    out: list[dict] = []
    for c in item["rejected"]:
        if c.get("origin") == "rejected_change":
            ctok = set(c.get("text_tokens") or set()) | set(c.get("old_values") or set()) | set(c.get("new_values") or set())
        else:
            ctok = set(c.get("tokens") or set()) | set(c.get("matched_values") or set())
            # designator-range отвергается по дизайнатору: добавим дизайнаторы
            # отвергнутого значения, чтобы поймать совпадение по аппарату
            ctok |= _designators(c.get("value"))
        delta_desigs = _designators(delta.get("old_value")) | _designators(delta.get("new_value"))
        if not (all_tokens & ctok) and not (delta_desigs & ctok):
            continue
        out.append({
            "source": "graphic_vision_grounding",
            "fact_level": FACT_REJECTED,
            "status": c.get("status"),
            "kind": "rejected",
            "old_anchor": c.get("value"),
            "new_anchor": c.get("value"),
            "left_block_id": item.get("left_block_id"),
            "right_block_id": item.get("right_block_id"),
            "graphic_type": item.get("graphic_type"),
            "match_score": 0.4,
            "reason": c.get("reason") or "rejected vision output — не факт",
        })
    return out


def _single_side_evidence(delta: dict, item: dict) -> list[dict]:
    """added/removed: значение дельты ∈ grounded/weak entity нужной стороны.
    Для changed — частичное подтверждение одной стороны."""
    out: list[dict] = []
    dtype = (delta.get("delta_type") or "").lower()
    pool = []
    if dtype == "added":
        pool = [("new", item["entities_new"], _salient_token_set(delta.get("new_value")), delta.get("new_value"))]
    elif dtype == "removed":
        pool = [("old", item["entities_old"], _salient_token_set(delta.get("old_value")), delta.get("old_value"))]
    else:  # changed — частичные одиночные подтверждения
        pool = [
            ("old", item["entities_old"], _salient_token_set(delta.get("old_value")), delta.get("old_value")),
            ("new", item["entities_new"], _salient_token_set(delta.get("new_value")), delta.get("new_value")),
        ]
    for side, entities, tokens, raw_val in pool:
        if not tokens:
            continue
        e = _best_entity(entities, tokens, (side, "either"))
        if not e or e["fact_level"] not in (FACT_CONFIRMED, FACT_WEAK):
            continue
        score = score_grounded_evidence_match(delta, e)
        if score < _MIN_MATCH_SCORE:
            continue
        out.append({
            "source": "graphic_vision_grounding",
            "fact_level": e["fact_level"],
            "status": e["status"],
            "kind": "entity_single",
            "side": side,
            "old_anchor": e["value"] if side == "old" else None,
            "new_anchor": e["value"] if side == "new" else None,
            "left_block_id": item.get("left_block_id"),
            "right_block_id": item.get("right_block_id"),
            "graphic_type": item.get("graphic_type"),
            "match_score": round(score, 3),
            "reason": e.get("reason") or "значение найдено среди grounded сущностей",
        })
    return out


def match_delta_to_grounded_vision(delta: dict, grounding_index: dict,
                                   page_index: Optional[dict] = None) -> list[dict]:
    """Собрать список evidence-записей grounding для одной дельты.

    Возвращает ПУСТОЙ список, если блоки дельты не покрыты grounding-итемами или
    ни одно значение не подтвердилось. Записи отсортированы по убыванию
    match_score; визуальные факты (confirmed/weak) идут раньше rejected.
    """
    items = _items_for_delta(delta, grounding_index)
    if not items:
        return []
    lp, rp = _page_context(delta, page_index)
    evidence: list[dict] = []
    seen = set()

    def _push(ev: Optional[dict]):
        if not ev:
            return
        ev.setdefault("left_page_number", lp)
        ev.setdefault("right_page_number", rp)
        key = (ev.get("kind"), ev.get("status"),
               ev.get("old_anchor"), ev.get("new_anchor"),
               ev.get("left_block_id"), ev.get("right_block_id"))
        if key in seen:
            return
        seen.add(key)
        evidence.append(ev)

    for item in items:
        # самый сильный сигнал — общий дизайнатор со сменой номинала; если он
        # confirmed, value_pair/одиночные частичные подтверждения той же дельты
        # избыточны (и могут указать чужой дизайнатор с тем же номиналом)
        dpair = _designator_anchored_pair(delta, item)
        _push(dpair)
        confirmed_pair = bool(dpair and dpair.get("fact_level") == FACT_CONFIRMED)
        if not confirmed_pair:
            _push(_value_pair_evidence(delta, item))
            for ev in _single_side_evidence(delta, item):
                _push(ev)
        _push(_noop_contradicts(delta, item))
        for ev in _rejected_evidence(delta, item):
            _push(ev)

    _FACT_ORDER = {FACT_CONFIRMED: 0, FACT_WEAK: 1, FACT_NOT_FACT: 2, FACT_REJECTED: 3}
    evidence.sort(key=lambda e: (_FACT_ORDER.get(e.get("fact_level"), 9),
                                 -float(e.get("match_score") or 0.0)))
    return evidence[:_MAX_EVIDENCE_PER_DELTA]


# ─── card ────────────────────────────────────────────────────────────────────


def _aggregate_level(evidence: list[dict], delta: dict, has_noop_conflict: bool) -> str:
    has_conf = any(e.get("fact_level") == FACT_CONFIRMED for e in evidence)
    has_weak = any(e.get("fact_level") == FACT_WEAK for e in evidence)
    has_rej = any(e.get("fact_level") == FACT_REJECTED for e in evidence)
    if not evidence:
        return LEVEL_NONE
    if has_noop_conflict and (has_conf or has_weak):
        return LEVEL_CONFLICT
    if has_conf and has_rej:
        return LEVEL_CONFLICT
    if has_conf:
        return LEVEL_GROUNDED
    if has_weak and has_rej:
        return LEVEL_CONFLICT
    if has_weak:
        return LEVEL_WEAK
    if has_rej:
        return LEVEL_CONFLICT if has_noop_conflict else LEVEL_REJECTED_ONLY
    return LEVEL_NONE


def build_delta_evidence_card(delta: dict, evidence: list[dict]) -> dict:
    """Собрать per-delta evidence card с уровнем и пометкой use_in_critic."""
    has_noop_conflict = any(e.get("kind") == "rejected_noop" for e in evidence)
    level = _aggregate_level(evidence, delta, has_noop_conflict)
    use_in_critic = level in _USABLE_LEVELS

    warnings: list[str] = []
    if level == LEVEL_CONFLICT:
        warnings.append("grounding противоречит дельте — не использовать как факт")
    if level == LEVEL_REJECTED_ONLY:
        warnings.append("дельта совпала только с rejected vision — не факт")

    pages = delta.get("page_numbers") or {}
    return {
        "delta_id": delta.get("delta_id"),
        "entity_type": delta.get("entity_type"),
        "semantic_group": delta.get("semantic_group"),
        "delta_type": delta.get("delta_type"),
        "old_value": delta.get("old_value"),
        "new_value": delta.get("new_value"),
        "left_block_id": delta.get("left_block_id"),
        "right_block_id": delta.get("right_block_id"),
        "left_page_number": (evidence[0].get("left_page_number") if evidence
                             else pages.get("left")),
        "right_page_number": (evidence[0].get("right_page_number") if evidence
                              else pages.get("right")),
        "evidence_level": level,
        "use_in_critic": use_in_critic,
        "evidence": evidence,
        "warnings": warnings,
    }


# ─── report ──────────────────────────────────────────────────────────────────


def _empty_report(status: str, warnings: list[str]) -> dict:
    return {
        "version": REPORT_VERSION,
        "kind": REPORT_KIND,
        "status": status,
        "summary": {
            "deltas_total": 0,
            "deltas_total_all_diff": 0,
            "deltas_with_grounded_evidence": 0,
            "deltas_with_weak_evidence": 0,
            "deltas_without_evidence": 0,
            "deltas_with_rejected_conflicts": 0,
            "evidence_links_total": 0,
            "grounded_links": 0,
            "weak_links": 0,
            "rejected_links": 0,
        },
        "delta_evidence": [],
        "warnings": warnings,
    }


def build_grounded_evidence_report(entity_diff_report: Any,
                                   grounding_report: Any, *,
                                   block_link_report: Any = None,
                                   visual_gate_report: Any = None,
                                   enrichment_report: Any = None,
                                   left_model: Any = None,
                                   right_model: Any = None,
                                   options: Optional[dict] = None) -> dict:
    """Построить ``grounded_evidence_report`` (mark-only).

    Минимально обязательны ``entity_diff_report`` и ``grounding_report``. Если
    grounding отсутствует/пуст → ``status=skipped_no_grounding`` (не падает).
    Остальные отчёты — optional page-контекст. fail-soft по каждой дельте.
    """
    warnings: list[str] = []
    diff = entity_diff_report if isinstance(entity_diff_report, dict) else {}
    deltas = [d for d in (diff.get("deltas") or []) if isinstance(d, dict)]
    total_all = len(deltas)

    has_grounding = (isinstance(grounding_report, dict)
                     and isinstance(grounding_report.get("items"), list)
                     and len(grounding_report.get("items") or []) > 0)
    if not has_grounding:
        rep = _empty_report("skipped_no_grounding",
                            warnings + ["grounding report missing or empty"])
        rep["summary"]["deltas_total_all_diff"] = total_all
        return rep

    try:
        index = build_grounding_index(grounding_report)
        page_index = build_page_index(visual_gate_report, block_link_report)

        delta_evidence: list[dict] = []
        for delta in deltas:
            try:
                items = _items_for_delta(delta, index)
                if not items:
                    continue  # дельта вне зоны grounding — не vision-relevant
                evidence = match_delta_to_grounded_vision(delta, index, page_index)
                card = build_delta_evidence_card(delta, evidence)
                delta_evidence.append(card)
            except Exception as dexc:  # noqa: BLE001 — одна дельта не валит слой
                warnings.append(
                    f"delta {delta.get('delta_id')}: {type(dexc).__name__}: {dexc}")

        # summary counts
        grounded = sum(1 for c in delta_evidence if c["evidence_level"] == LEVEL_GROUNDED)
        weak = sum(1 for c in delta_evidence if c["evidence_level"] == LEVEL_WEAK)
        none_ = sum(1 for c in delta_evidence if c["evidence_level"] == LEVEL_NONE)
        rej_conf = sum(1 for c in delta_evidence
                       if c["evidence_level"] in (LEVEL_CONFLICT, LEVEL_REJECTED_ONLY))
        links = [e for c in delta_evidence for e in c["evidence"]]
        glinks = sum(1 for e in links if e.get("fact_level") == FACT_CONFIRMED)
        wlinks = sum(1 for e in links if e.get("fact_level") == FACT_WEAK)
        rlinks = sum(1 for e in links if e.get("fact_level") == FACT_REJECTED)

        status = "completed_with_warnings" if warnings else "ok"
        return {
            "version": REPORT_VERSION,
            "kind": REPORT_KIND,
            "status": status,
            "summary": {
                "deltas_total": len(delta_evidence),
                "deltas_total_all_diff": total_all,
                "deltas_with_grounded_evidence": grounded,
                "deltas_with_weak_evidence": weak,
                "deltas_without_evidence": none_,
                "deltas_with_rejected_conflicts": rej_conf,
                "evidence_links_total": len(links),
                "grounded_links": glinks,
                "weak_links": wlinks,
                "rejected_links": rlinks,
            },
            "delta_evidence": delta_evidence,
            "warnings": warnings,
        }
    except Exception as exc:  # noqa: BLE001 — слой не критичен
        rep = _empty_report("failed", warnings + [f"{type(exc).__name__}: {exc}"])
        rep["summary"]["deltas_total_all_diff"] = total_all
        return rep


def grounded_evidence_by_delta_id(report: Any) -> dict:
    """Индекс delta_id → evidence card (для delta-explanation prompt)."""
    out: dict = {}
    if not isinstance(report, dict):
        return out
    for card in report.get("delta_evidence") or []:
        if isinstance(card, dict) and card.get("delta_id"):
            out[card["delta_id"]] = card
    return out


# ─── writer ──────────────────────────────────────────────────────────────────


def write_grounded_evidence_report(out_path: str | Path, report: dict) -> Path:
    """Атомарно записать отчёт (tmp + ``os.replace``)."""
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
    "LEVEL_GROUNDED",
    "LEVEL_WEAK",
    "LEVEL_NONE",
    "LEVEL_CONFLICT",
    "LEVEL_REJECTED_ONLY",
    "normalize_evidence_token",
    "build_grounding_index",
    "build_page_index",
    "score_grounded_evidence_match",
    "match_delta_to_grounded_vision",
    "build_delta_evidence_card",
    "build_grounded_evidence_report",
    "grounded_evidence_by_delta_id",
    "write_grounded_evidence_report",
]
