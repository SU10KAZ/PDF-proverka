# -*- coding: utf-8 -*-
"""Pipeline V2 — offline UI payload adapter (контракт будущего портала).

Превращает ГОТОВЫЕ артефакты Pipeline V2 (``pipeline_v2_summary.json`` +
``entity_diff_report`` + ``delta_explanation_report``) в компактный
UI-friendly payload для будущего портала: секции с карточками дельт,
headline-счётчики, фильтры, готовность графики.

Что этот модуль НЕ делает:

* НЕ endpoint и НЕ роут — к API/порталу не подключён;
* НЕ frontend — только данные-контракт;
* НЕ вызывает LLM и НЕ ходит в сеть (stdlib-only);
* НЕ перечитывает исходные PDF/MD/crop — вход только готовые отчёты;
* НЕ меняет селекцию/prompt/diff — read-only поверх артефактов.

Секционирование дельт повторяет ``delta_sections`` из summary
(см. ``pipeline_v2_dry_run.build_delta_sections``); если в summary секций
нет (старый формат), они пересобираются из отчётов тем же кодом.
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Optional

# канонический набор weak-флагов графики — единый с coverage_notes
# (pipeline_v2_delta_explanation._build_coverage_notes), чтобы
# weak_blocks_preview и headline.coverage_notes_total не расходились
from .pipeline_v2_delta_explanation import (  # noqa: F401 — re-use, не копия
    _WEAK_GRAPHIC_FLAGS as _WEAK_GRAPHIC_FLAGS,
)

PAYLOAD_VERSION = 1
PAYLOAD_KIND = "stage_comparison_pipeline_v2_ui_payload"

_VALID_STATUSES = ("ok", "completed_with_warnings", "failed")

# UX-правила секций (контракт для портала):
#   confirmed_changes / needs_review — показывать по умолчанию;
#   weak_graphic_review — показывать по умолчанию, но как предупреждение;
#   likely_noise_hidden_by_default — скрывать по умолчанию;
#   llm_failed_or_skipped — скрывать по умолчанию, показывать в диагностике.
_SECTION_META = [
    {
        "key": "confirmed_changes",
        "title": "Подтверждённые изменения",
        "badge": "confirmed",
        "default_visible": True,
        "display_hint": "normal",
        "show_in_diagnostics": False,
        "description": ("Принятые критиком и подтверждённые evidence изменения — "
                        "показывать инженеру в первую очередь."),
    },
    {
        "key": "needs_review",
        "title": "На ручную проверку",
        "badge": "review",
        "default_visible": True,
        "display_hint": "normal",
        "show_in_diagnostics": False,
        "description": ("Дельты, требующие взгляда инженера: критик не уверен "
                        "или дельта односторонняя/неоднозначная."),
    },
    {
        "key": "weak_graphic_review",
        "title": "Слабая графика / нужна доработка",
        "badge": "weak_graphic",
        "default_visible": True,
        "display_hint": "warning",
        "show_in_diagnostics": False,
        "description": ("Дельты на блоках со слабой/непригодной графикой — "
                        "показывать как предупреждение: сначала нужна "
                        "дообработка vision, потом вывод."),
    },
    {
        "key": "likely_noise_hidden_by_default",
        "title": "Вероятный шум",
        "badge": "noise",
        "default_visible": False,
        "display_hint": "hidden",
        "show_in_diagnostics": False,
        "description": ("OCR/типографский шум и отвергнутые критиком дельты — "
                        "скрывать по умолчанию, доступно по фильтру."),
    },
    {
        "key": "llm_failed_or_skipped",
        "title": "Необъяснённые / ошибки LLM",
        "badge": "failed",
        "default_visible": False,
        "display_hint": "diagnostics",
        "show_in_diagnostics": True,
        "description": ("Выбранные дельты без успешного объяснения (сбой, "
                        "пропуск runner'а, нечитаемый ответ) — скрывать по "
                        "умолчанию, показывать в диагностике."),
    },
]

_SECTION_BADGES = {m["key"]: m["badge"] for m in _SECTION_META}
_KNOWN_SECTION_KEYS = {m["key"] for m in _SECTION_META}

_CARD_TEXT_MAX = 160
_TITLE_MAX = 120
_MAX_CARDS_PER_SECTION = 100
_MAX_WEAK_BLOCKS_PREVIEW = 20
_WEAK_READINESS = ("low", "not_usable")

_FILTER_KEYS = ("entity_types", "risk_levels", "critic_verdicts", "delta_types")

# grounded evidence (compact для UI)
_GE_CARDS_MAX = 100
_GE_TOP_ANCHORS_MAX = 3
_GE_ANCHOR_TEXT_MAX = 80
_GE_BADGE_BY_LEVEL = {
    "grounded": "grounded",
    "weak": "weak",
    "conflict": "conflict",
    "rejected_only": "conflict",
    "none": "none",
}
_GE_LABEL_BY_LEVEL = {
    "grounded": "Grounded vision evidence",
    "weak": "Weak vision evidence",
    "conflict": "Rejected/conflict evidence",
    "rejected_only": "Rejected/conflict evidence",
    "none": "",
}


def _ge_badge(level: str) -> str:
    return _GE_BADGE_BY_LEVEL.get((level or "").strip().lower(), "none")


def _ge_label(level: str) -> str:
    return _GE_LABEL_BY_LEVEL.get((level or "").strip().lower(), "")


def _compact_ge_anchor(ev: dict) -> dict:
    """Компактный top-anchor: короткие строки, без raw/full-text."""
    ev = ev if isinstance(ev, dict) else {}
    return {
        "status": _clean(ev.get("status")) or None,
        "old_anchor": truncate_ui_text(ev.get("old_anchor"), _GE_ANCHOR_TEXT_MAX),
        "new_anchor": truncate_ui_text(ev.get("new_anchor"), _GE_ANCHOR_TEXT_MAX),
        "designator": _clean(ev.get("designator")) or None,
        "left_page_number": ev.get("left_page_number"),
        "right_page_number": ev.get("right_page_number"),
        "left_block_id": _clean(ev.get("left_block_id")) or None,
        "right_block_id": _clean(ev.get("right_block_id")) or None,
        "match_score": ev.get("match_score"),
    }


def build_grounded_evidence_compact(card: dict) -> dict:
    """Компактный per-delta evidence-блок для UI-карточки.

    Берёт ТОЛЬКО usable (confirmed/weak) top-anchors как факт; для
    conflict/rejected_only anchors-факты НЕ отдаются (только badge+warnings).
    Никакого raw vision-ответа / full-text — лишь короткие якоря.
    """
    card = card if isinstance(card, dict) else {}
    level = (card.get("evidence_level") or "none").strip().lower()
    evidence = card.get("evidence") if isinstance(card.get("evidence"), list) else []
    if level in ("grounded", "weak"):
        usable = [e for e in evidence if isinstance(e, dict)
                  and e.get("fact_level") in ("confirmed", "weak")]
    else:
        usable = []   # rejected/conflict/none → не отдаём anchors как факт
    return {
        "evidence_level": level,
        "badge": _ge_badge(level),
        "label": _ge_label(level),
        "use_in_critic": bool(card.get("use_in_critic")),
        "top_anchors": [_compact_ge_anchor(e) for e in usable[:_GE_TOP_ANCHORS_MAX]],
        "warnings": _str_list(card.get("warnings")),
    }


def build_grounded_evidence_ui(report: Any, summary_section: Any = None) -> Optional[dict]:
    """Собрать UI-блок grounded_evidence (summary + compact cards).

    ``report`` — `grounded_evidence_report` (dict) или None. ``summary_section``
    — секция `grounded_evidence` из pipeline_v2_summary (fallback на counts,
    если полный report не передан). Возвращает None, если слой недоступен
    (старый payload не ломается).
    """
    rep = report if isinstance(report, dict) else None
    sec = summary_section if isinstance(summary_section, dict) else None

    # источник counts: предпочитаем report.summary, иначе summary-секцию
    rep_sum = (rep.get("summary") if rep and isinstance(rep.get("summary"), dict)
               else {})
    if not rep and not (sec and sec.get("enabled")):
        return None

    status = _clean((rep or {}).get("status")) or _clean((sec or {}).get("status")) \
        or "unknown"
    available = status not in ("disabled", "not_run", "unknown",
                               "skipped_no_grounding")

    def _count(key: str) -> int:
        if key in rep_sum:
            return _safe_count(rep_sum.get(key))
        return _safe_count((sec or {}).get(key))

    cards: list[dict] = []
    if rep:
        # сортируем «интересные» (grounded/weak/conflict/rejected) ВПЕРЁД, чтобы
        # cap=100 не отрезал подтверждённые/конфликтные дельты в пользу «none»
        _LEVEL_ORDER = {"grounded": 0, "weak": 1, "conflict": 2,
                        "rejected_only": 3, "none": 4}
        ordered = sorted(
            (c for c in (rep.get("delta_evidence") or []) if isinstance(c, dict)),
            key=lambda c: _LEVEL_ORDER.get(
                (c.get("evidence_level") or "none").strip().lower(), 9))
        for c in ordered[:_GE_CARDS_MAX]:
            if not isinstance(c, dict):
                continue
            compact = build_grounded_evidence_compact(c)
            pages = {"left": c.get("left_page_number"),
                     "right": c.get("right_page_number")}
            cards.append({
                "delta_id": _clean(c.get("delta_id")) or None,
                "entity_type": _clean(c.get("entity_type")) or None,
                "delta_type": _clean(c.get("delta_type")) or None,
                "old_value": truncate_ui_text(c.get("old_value"), _CARD_TEXT_MAX),
                "new_value": truncate_ui_text(c.get("new_value"), _CARD_TEXT_MAX),
                "evidence_level": compact["evidence_level"],
                "use_in_critic": compact["use_in_critic"],
                "badge": compact["badge"],
                "label": compact["label"],
                "page_numbers": pages,
                "top_anchors": compact["top_anchors"],
                "warnings": compact["warnings"],
            })

    out = {
        "available": available,
        "status": status,
        "deltas_with_grounded_evidence": _count("deltas_with_grounded_evidence"),
        "deltas_with_weak_evidence": _count("deltas_with_weak_evidence"),
        "deltas_without_evidence": _count("deltas_without_evidence"),
        "deltas_with_rejected_conflicts": _count("deltas_with_rejected_conflicts"),
        "cards": cards,
    }
    err = _clean((rep or {}).get("status") == "failed" and "report_failed") \
        or _clean((sec or {}).get("error"))
    if err:
        out["error"] = err
    return out


def grounded_evidence_compact_by_delta_id(report: Any) -> dict:
    """Индекс delta_id → compact evidence-блок (для attach к delta cards)."""
    out: dict = {}
    if not isinstance(report, dict):
        return out
    for c in report.get("delta_evidence") or []:
        if isinstance(c, dict) and c.get("delta_id"):
            out[c["delta_id"]] = build_grounded_evidence_compact(c)
    return out


def _opt(options: Optional[dict], key: str, default: Any = None) -> Any:
    if isinstance(options, dict) and key in options:
        return options[key]
    return default


def _clean(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _safe_count(value: Any, fallback: int = 0) -> int:
    """int-коэрция artifact-значения: junk/None/строка → fallback, не краш."""
    if isinstance(value, bool):
        return fallback
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _str_list(value: Any) -> list[str]:
    """Список строк из artifact-поля: не-список/смешанные типы → fail-soft."""
    if not isinstance(value, (list, tuple)):
        return []
    return [str(x) for x in value if _clean(x)]


def truncate_ui_text(value: Any, limit: int = _CARD_TEXT_MAX) -> str:
    """Обрезать текст для карточки UI (с многоточием), None → ''."""
    s = _clean(value)
    if limit <= 1:
        return s[:max(limit, 0)]
    if len(s) <= limit:
        return s
    return s[: limit - 1].rstrip() + "…"


def classify_ui_badge(section_name: Any, explanation: Optional[dict] = None,
                      delta: Optional[dict] = None) -> str:
    """Бейдж карточки/секции по имени секции.

    ``explanation``/``delta`` зарезервированы для будущих уточнений бейджа
    (например, эскалация по risk_level); сейчас бейдж определяется секцией.
    Неизвестная секция → безопасный ``review``.
    """
    return _SECTION_BADGES.get(_clean(section_name), "review")


# ─── карточка дельты ─────────────────────────────────────────────────────────


def format_ui_delta_card(delta: dict, explanation: Optional[dict] = None,
                         options: Optional[dict] = None) -> dict:
    """Компактная UI-карточка одной дельты (preview без полного diff).

    Работает и без explanation (фолбэк на поля дельты), и без дельты
    (фолбэк на ``explanation.input_delta``) — payload не ломается от
    неполных входов. Длинные значения обрезаются, гигантские quotes не
    тащатся: evidence представлен кратко через page/block ids.
    """
    d = delta if isinstance(delta, dict) else {}
    e = explanation if isinstance(explanation, dict) else {}
    input_d = e.get("input_delta") if isinstance(e.get("input_delta"), dict) else {}
    limit = int(_opt(options, "card_text_limit", _CARD_TEXT_MAX) or _CARD_TEXT_MAX)

    entity_type = d.get("entity_type") or input_d.get("entity_type") or "unknown"
    delta_type = d.get("delta_type") or input_d.get("delta_type") or "unknown"
    # old/new: '' — легитимное значение (one-sided), поэтому через `in`
    old_value = d["old_value"] if "old_value" in d else input_d.get("old_value")
    new_value = d["new_value"] if "new_value" in d else input_d.get("new_value")
    subject = _clean(d.get("subject"))
    field = _clean(d.get("field"))

    critic = e.get("critic") if isinstance(e.get("critic"), dict) else {}
    groundedness = (e.get("groundedness")
                    if isinstance(e.get("groundedness"), dict) else {})
    risk = _clean(e.get("risk_level")) or None

    section = _clean(_opt(options, "section"))
    if not section and e:
        # lazy import: секционирование живёт в dry_run; ленивый импорт
        # исключает цикл при будущей интеграции dry_run → ui_payload
        from .pipeline_v2_dry_run import classify_explained_delta_section
        section = classify_explained_delta_section(e, d)

    title = truncate_ui_text(
        _clean(e.get("summary")) or _clean(d.get("change_summary"))
        or f"{entity_type}/{delta_type}: {subject or field or '—'}",
        int(_opt(options, "card_title_limit", _TITLE_MAX) or _TITLE_MAX))
    subtitle_parts = [entity_type, delta_type]
    if risk and risk != "unknown":
        subtitle_parts.append(f"{risk} risk")
    pages = d.get("page_numbers") if isinstance(d.get("page_numbers"), dict) else {}

    flags = sorted(set(_str_list(d.get("quality_flags"))
                       + _str_list(e.get("quality_flags"))))
    return {
        "delta_id": d.get("delta_id") or e.get("delta_id"),
        "section": section or None,
        "badge": classify_ui_badge(section, e, d),
        "title": title,
        "subtitle": " · ".join(subtitle_parts),
        "entity_type": entity_type,
        "delta_type": delta_type,
        "field": field,
        "subject": truncate_ui_text(subject, limit),
        "old_value": truncate_ui_text(old_value, limit),
        "new_value": truncate_ui_text(new_value, limit),
        "confidence": d.get("confidence"),
        "risk_level": risk,
        "critic_verdict": _clean(critic.get("verdict")) or None,
        "groundedness": _clean(groundedness.get("verdict")) or None,
        "should_show_to_engineer": critic.get("should_show_to_engineer"),
        "summary": truncate_ui_text(e.get("summary"), limit),
        "contractor_impact": truncate_ui_text(e.get("contractor_impact"), limit),
        "quality_flags": flags,
        "page_numbers": {"left": pages.get("left"), "right": pages.get("right")},
        "block_ids": {"left": d.get("left_block_id"),
                      "right": d.get("right_block_id")},
    }


def build_ui_section_cards(section_name: str, delta_ids: list,
                           deltas_by_id: dict, explanations_by_delta_id: dict,
                           options: Optional[dict] = None) -> list[dict]:
    """Карточки секции по списку delta_ids (порядок сохраняется).

    Дельта без каких-либо данных (нет ни в diff, ни в explanations)
    пропускается — её id остаётся в ``delta_ids``/count секции, а вызывающий
    код фиксирует warning о расхождении.
    """
    max_cards = _safe_count(_opt(options, "max_cards_per_section"),
                            _MAX_CARDS_PER_SECTION) or _MAX_CARDS_PER_SECTION
    cards: list[dict] = []
    for did in list(delta_ids or [])[:max_cards]:
        d = (deltas_by_id or {}).get(did)
        e = (explanations_by_delta_id or {}).get(did)
        if not isinstance(d, dict) and not isinstance(e, dict):
            continue
        opts = dict(options) if isinstance(options, dict) else {}
        opts["section"] = section_name
        cards.append(format_ui_delta_card(d or {}, e, opts))
    return cards


# ─── фильтры / графика ───────────────────────────────────────────────────────


def build_ui_filters(payload: dict) -> dict:
    """Собрать доступные значения фильтров из карточек payload."""
    acc: dict[str, set] = {k: set() for k in _FILTER_KEYS}
    for sec in (payload or {}).get("sections") or []:
        for card in (sec or {}).get("cards") or []:
            if not isinstance(card, dict):
                continue
            for key, field in (("entity_types", "entity_type"),
                               ("risk_levels", "risk_level"),
                               ("critic_verdicts", "critic_verdict"),
                               ("delta_types", "delta_type")):
                val = _clean(card.get(field))
                if val:
                    acc[key].add(val)
    return {k: sorted(v) for k, v in acc.items()}


def _graphic_readiness_section(graphic_descriptor: Any) -> dict:
    """Компактная сводка готовности графики из summary.graphic_descriptor.

    Счётчики коэрцируются fail-soft: null/строка/junk в artifact-JSON не
    должны ронять payload целиком.
    """
    gd = graphic_descriptor if isinstance(graphic_descriptor, dict) else {}
    total = (_safe_count(gd.get("left_graphic_blocks_total"))
             + _safe_count(gd.get("right_graphic_blocks_total")))
    usable = (_safe_count(gd.get("left_usable_for_diff_total"))
              + _safe_count(gd.get("right_usable_for_diff_total")))
    vision = (_safe_count(gd.get("left_needs_vision_enrichment_total"))
              + _safe_count(gd.get("right_needs_vision_enrichment_total")))
    manual = (_safe_count(gd.get("left_manual_review_recommended_total"))
              + _safe_count(gd.get("right_manual_review_recommended_total")))
    by_readiness = (dict(gd["by_readiness"])
                    if isinstance(gd.get("by_readiness"), dict) else {})
    not_usable = _safe_count(by_readiness.get("not_usable"))
    if total == 0:
        status = "no_graphic_blocks"
    elif not_usable > 0 or manual > 0:
        status = "manual_review_required"
    elif vision > 0:
        status = "needs_vision_enrichment"
    else:
        status = "ok"
    return {
        "status": status,
        "graphic_blocks_total": total,
        "usable_for_diff_total": usable,
        "needs_vision_enrichment_total": vision,
        "manual_review_recommended_total": manual,
        "by_readiness": by_readiness,
    }


def _iter_descriptors(gdr: Any):
    """Дескрипторы из single-side {'descriptors': []} / combined {left,right} /
    списка отчётов — без падений на чужих формах."""
    if isinstance(gdr, dict):
        if isinstance(gdr.get("descriptors"), list):
            for d in gdr["descriptors"]:
                if isinstance(d, dict):
                    yield d
            return
        for key in ("left", "right"):
            yield from _iter_descriptors(gdr.get(key))
    elif isinstance(gdr, (list, tuple)):
        for item in gdr:
            yield from _iter_descriptors(item)


def _collect_weak_blocks_preview(graphic_descriptor_reports: Any,
                                 limit: int = _MAX_WEAK_BLOCKS_PREVIEW) -> list[dict]:
    out: list[dict] = []
    for d in _iter_descriptors(graphic_descriptor_reports):
        readiness = (d.get("diff_readiness") or {}).get("readiness", "unknown")
        flags = set(d.get("quality_flags") or [])
        if readiness in _WEAK_READINESS or flags & _WEAK_GRAPHIC_FLAGS:
            out.append({"block_id": d.get("block_id"),
                        "page_number": d.get("page_number"),
                        "readiness": readiness})
        if len(out) >= limit:
            break
    return out


# ─── основной builder ────────────────────────────────────────────────────────


def build_pipeline_v2_ui_payload(summary: dict,
                                 entity_diff_report: Optional[dict] = None,
                                 delta_explanation_report: Optional[dict] = None,
                                 graphic_descriptor_reports: Any = None,
                                 options: Optional[dict] = None,
                                 grounded_evidence_report: Optional[dict] = None) -> dict:
    """Собрать UI payload из готовых артефактов Pipeline V2 (offline).

    ``summary`` — обязательный вход (``pipeline_v2_summary.json``);
    остальные отчёты опциональны: без них payload строится по счётчикам
    summary, карточки деградируют (warning, не падение).

    ``grounded_evidence_report`` (optional) — `grounded_evidence_report.json`;
    если передан, payload получает per-delta compact evidence (badges,
    top-anchors) на карточках дельт + блок `grounded_evidence.cards`. Без него
    блок строится по counts из summary (или отсутствует) — старый UI не ломается.
    """
    adapter_warnings: list[str] = []
    s = summary if isinstance(summary, dict) else {}
    if not s:
        adapter_warnings.append("summary_missing_or_invalid")

    raw_status = _clean(s.get("status"))
    if raw_status in _VALID_STATUSES:
        status = raw_status
    elif not s:
        status = "failed"
    else:
        status = "completed_with_warnings"
        adapter_warnings.append(f"unknown_summary_status:{raw_status or 'empty'}")

    diff = entity_diff_report if isinstance(entity_diff_report, dict) else {}
    de = (delta_explanation_report
          if isinstance(delta_explanation_report, dict) else {})
    if entity_diff_report is None:
        adapter_warnings.append(
            "entity_diff_report_missing: cards built from explanations only")
    if delta_explanation_report is None:
        adapter_warnings.append(
            "delta_explanation_report_missing: cards without critic fields")

    ds = s.get("delta_sections")
    if not isinstance(ds, dict) or not ds:
        if diff or de:
            # старый summary без delta_sections — пересобираем тем же кодом,
            # что и dry run (lazy import: без цикла при будущей интеграции);
            # build_delta_sections fail-soft к каждому из отчётов по
            # отдельности, поэтому достаточно ОДНОГО из них
            from .pipeline_v2_dry_run import build_delta_sections
            ds = build_delta_sections(diff, de)
            adapter_warnings.append("delta_sections_rebuilt_from_reports")
        else:
            ds = {}
            adapter_warnings.append("delta_sections_missing")

    deltas_by_id = {d.get("delta_id"): d
                    for d in (diff.get("deltas") or []) if isinstance(d, dict)}
    explanations_by_id = {e.get("delta_id"): e
                          for e in (de.get("explanations") or [])
                          if isinstance(e, dict)}

    sections: list[dict] = []

    max_cards = _safe_count(_opt(options, "max_cards_per_section"),
                            _MAX_CARDS_PER_SECTION) or _MAX_CARDS_PER_SECTION
    explicit_cap = isinstance(options, dict) and "max_cards_per_section" in options

    def _append_section(meta: dict, sec_src: dict) -> None:
        raw_ids = sec_src.get("delta_ids")
        if isinstance(raw_ids, (list, tuple)):
            delta_ids = [did for did in raw_ids if isinstance(did, str) and did]
            if len(delta_ids) != len(raw_ids):
                adapter_warnings.append(
                    f"section_{meta['key']}: invalid delta_ids dropped")
        else:
            delta_ids = []
            if raw_ids:
                adapter_warnings.append(
                    f"section_{meta['key']}: invalid delta_ids ignored")
        cards = build_ui_section_cards(meta["key"], delta_ids, deltas_by_id,
                                       explanations_by_id, options)
        count = sec_src.get("count")
        count = (int(count) if isinstance(count, int)
                 and not isinstance(count, bool) else len(delta_ids))
        # warning только о ДЕЛЬТАХ БЕЗ ДАННЫХ среди реально рассмотренных
        # (до effective cap), а не о срезанных лимитом карточках
        examined = delta_ids[:max_cards]
        missing = sum(1 for did in examined
                      if not isinstance(deltas_by_id.get(did), dict)
                      and not isinstance(explanations_by_id.get(did), dict))
        if missing:
            adapter_warnings.append(
                f"section_{meta['key']}: {missing} delta(s) without card data")
        # срез ДЕФОЛТНЫМ cap'ом — не молчим (silent caps запрещены);
        # явный options-cap — осознанный выбор вызывающего, без warning'а
        if len(delta_ids) > max_cards and not explicit_cap:
            adapter_warnings.append(
                f"section_{meta['key']}: cards truncated to "
                f"{max_cards} of {len(delta_ids)}")
        sections.append({
            "key": meta["key"],
            "title": meta["title"],
            "badge": meta["badge"],
            "default_visible": meta["default_visible"],
            "display_hint": meta["display_hint"],
            "show_in_diagnostics": meta["show_in_diagnostics"],
            "count": count,
            "description": meta["description"],
            "delta_ids": delta_ids,
            "cards": cards,
        })

    for meta in _SECTION_META:
        sec_src = ds.get(meta["key"])
        _append_section(meta, sec_src if isinstance(sec_src, dict) else {})

    # неизвестные будущие секции в delta_sections — не теряем, но помечаем
    for key, val in ds.items():
        if (key in _KNOWN_SECTION_KEYS or key in ("selected_total", "coverage_notes")
                or not isinstance(val, dict) or "delta_ids" not in val):
            continue
        adapter_warnings.append(f"unknown_delta_section:{key}")
        _append_section({
            "key": key, "title": key, "badge": classify_ui_badge(key),
            "default_visible": True, "display_hint": "normal",
            "show_in_diagnostics": False,
            "description": _clean(val.get("description")),
        }, val)

    def _count_of(key: str) -> int:
        return next((sec["count"] for sec in sections if sec["key"] == key), 0)

    stages = s.get("stages") if isinstance(s.get("stages"), dict) else {}
    entity_diff_stage = (stages.get("entity_diff")
                         if isinstance(stages.get("entity_diff"), dict) else {})
    deltas_total = entity_diff_stage.get("deltas_total")
    if deltas_total is None:
        diff_summary = (diff.get("summary")
                        if isinstance(diff.get("summary"), dict) else {})
        deltas_total = diff_summary.get("deltas_total", 0)
    cov = ds.get("coverage_notes") if isinstance(ds.get("coverage_notes"), dict) else {}

    headline = {
        "deltas_total": _safe_count(deltas_total),
        "selected_for_explanation_total": _safe_count(ds.get("selected_total")),
        "confirmed_total": _count_of("confirmed_changes"),
        "needs_review_total": _count_of("needs_review"),
        "weak_graphic_total": _count_of("weak_graphic_review"),
        "hidden_noise_total": _count_of("likely_noise_hidden_by_default"),
        "failed_or_skipped_total": _count_of("llm_failed_or_skipped"),
        "coverage_notes_total": _safe_count(cov.get("count")),
    }

    graphic_readiness = _graphic_readiness_section(s.get("graphic_descriptor"))
    if graphic_descriptor_reports is not None:
        graphic_readiness["weak_blocks_preview"] = _collect_weak_blocks_preview(
            graphic_descriptor_reports)
    # visual equivalence gate (mark-only) — добавляется ТОЛЬКО если секция
    # есть в summary (старые payload'ы без неё полностью совместимы)
    ve = s.get("visual_equivalence_gate")
    if isinstance(ve, dict) and ve.get("enabled"):
        ve_sec = {
            "status": _clean(ve.get("status")) or "unknown",
            "compared_total": _safe_count(ve.get("compared_total")),
            "exclude_from_vision": _safe_count(ve.get("exclude_from_vision")),
            "send_to_vision": _safe_count(ve.get("send_to_vision")),
            "manual_review": _safe_count(ve.get("manual_review")),
            "changed_visual": _safe_count(ve.get("changed_visual")),
            "uncertain": _safe_count(ve.get("uncertain")),
        }
        # упавший gate НЕ должен выглядеть как чистый прогон с нулями
        if ve.get("error"):
            ve_sec["error"] = str(ve["error"])
        graphic_readiness["visual_equivalence"] = ve_sec

    # graphic vision enrichment — добавляется ТОЛЬКО если секция есть в
    # summary и слой был включён (старые payload'ы полностью совместимы)
    gv = s.get("graphic_vision")
    graphic_vision_section = None
    if isinstance(gv, dict) and gv.get("enabled"):
        gv_status = _clean(gv.get("status")) or "unknown"
        graphic_vision_section = {
            "available": gv_status not in ("disabled", "not_run", "unknown"),
            "status": gv_status,
            "selected_total": _safe_count(gv.get("selected_total")),
            "vision_calls_succeeded": _safe_count(
                gv.get("vision_calls_succeeded")),
            "vision_calls_failed": _safe_count(gv.get("vision_calls_failed")),
            "skipped_no_runner": _safe_count(gv.get("skipped_no_runner")),
        }
        if gv.get("error"):
            graphic_vision_section["error"] = str(gv["error"])

    # graphic vision grounding — сводка проверки vision по anchor-тексту;
    # добавляется ТОЛЬКО если секция есть в summary и слой включён
    gvg = s.get("graphic_vision_grounding")
    graphic_vision_grounding_section = None
    if isinstance(gvg, dict) and gvg.get("enabled"):
        gvg_status = _clean(gvg.get("status")) or "unknown"
        graphic_vision_grounding_section = {
            "available": gvg_status not in ("disabled", "not_run", "unknown"),
            "status": gvg_status,
            "entities_grounded": _safe_count(gvg.get("entities_grounded")),
            "entities_weakly_grounded": _safe_count(
                gvg.get("entities_weakly_grounded")),
            "entities_ungrounded": _safe_count(gvg.get("entities_ungrounded")),
            "changes_grounded": _safe_count(gvg.get("changes_grounded")),
            "changes_rejected": _safe_count(gvg.get("changes_rejected")),
            "artificial_series_rejected": _safe_count(
                gvg.get("artificial_series_rejected")),
            "designator_range_rejected": _safe_count(
                gvg.get("designator_range_rejected")),
            "noop_changes_rejected": _safe_count(
                gvg.get("noop_changes_rejected")),
        }
        if gvg.get("error"):
            graphic_vision_grounding_section["error"] = str(gvg["error"])

    # grounded vision evidence — сводка + compact per-delta cards. Источник:
    # полный grounded_evidence_report (предпочтительно) ИЛИ counts из summary.
    # Добавляется ТОЛЬКО если report передан ИЛИ summary-секция включена;
    # старый payload без обоих не меняется.
    ge_summary_section = s.get("grounded_evidence")
    ge_summary_enabled = (isinstance(ge_summary_section, dict)
                          and ge_summary_section.get("enabled"))
    grounded_evidence_section = None
    if isinstance(grounded_evidence_report, dict) or ge_summary_enabled:
        grounded_evidence_section = build_grounded_evidence_ui(
            grounded_evidence_report,
            ge_summary_section if ge_summary_enabled else None)

    # entity alignment preview — компактная сводка mark-only классификации
    # выравнивания граф. сущностей; добавляется ТОЛЬКО если секция есть в
    # summary и слой включён (старые payload'ы полностью совместимы)
    eap = s.get("entity_alignment_preview")
    entity_alignment_section = None
    if isinstance(eap, dict) and eap.get("enabled"):
        eap_status = _clean(eap.get("status")) or "unknown"
        entity_alignment_section = {
            "available": eap_status not in ("disabled", "not_run", "unknown"),
            "status": eap_status,
            "graphic_pairs_total": _safe_count(eap.get("graphic_pairs_total")),
            "same_entity_likely": _safe_count(eap.get("same_entity_likely")),
            "possible_rename": _safe_count(eap.get("possible_rename")),
            "scope_reorganized": _safe_count(eap.get("scope_reorganized")),
            "mismatch_likely": _safe_count(eap.get("mismatch_likely")),
            "link_validation_candidate": _safe_count(
                eap.get("link_validation_candidate")),
            "needs_manual_mapping": _safe_count(eap.get("needs_manual_mapping")),
        }
        if eap.get("error"):
            entity_alignment_section["error"] = str(eap["error"])

    # link validation summary (mark-only проверка manual mapping через vision);
    # добавляется ТОЛЬКО если секция есть в summary и слой включён
    lv = s.get("link_validation")
    link_validation_section = None
    if isinstance(lv, dict) and lv.get("enabled"):
        lv_status = _clean(lv.get("status")) or "unknown"
        link_validation_section = {
            "available": lv_status not in ("disabled", "not_run", "unknown"),
            "status": lv_status,
            "candidates_total": _safe_count(lv.get("candidates_total")),
            "attempted": _safe_count(lv.get("attempted")),
            "valid_mapping": _safe_count(lv.get("valid_mapping")),
            "manual_review": _safe_count(lv.get("manual_review")),
            "reject_mapping": _safe_count(lv.get("reject_mapping")),
            "conflicts_with_manual_mapping": _safe_count(
                lv.get("conflicts_with_manual_mapping")),
        }
        if lv.get("error"):
            link_validation_section["error"] = str(lv["error"])

    # exclusion preview v2 — mark-only сводка (exclude/review/keep/
    # link_validation_required). auto_enforce_enabled всегда false.
    xp = s.get("exclusion_preview_v2")
    exclusion_preview_section = None
    if isinstance(xp, dict) and xp.get("enabled"):
        xp_status = _clean(xp.get("status")) or "unknown"
        exclusion_preview_section = {
            "available": xp_status not in ("disabled", "not_run", "unknown"),
            "status": xp_status,
            "items_total": _safe_count(xp.get("items_total")),
            "candidate_exclude": _safe_count(xp.get("candidate_exclude")),
            "review_only": _safe_count(xp.get("review_only")),
            "keep": _safe_count(xp.get("keep")),
            "link_validation_required": _safe_count(xp.get("link_validation_required")),
            "high_confidence_exclude": _safe_count(xp.get("high_confidence_exclude")),
            "manual_vision_conflict": _safe_count(xp.get("manual_vision_conflict")),
            "repeated_reject_transitions": _safe_count(
                xp.get("repeated_reject_transitions")),
            "auto_enforce_enabled": False,
        }
        if xp.get("error"):
            exclusion_preview_section["error"] = str(xp["error"])

    sr = s.get("skip_readiness_v2")
    skip_readiness_section = None
    if isinstance(sr, dict) and sr.get("enabled"):
        sr_status = _clean(sr.get("status")) or "unknown"
        skip_readiness_section = {
            "available": sr_status not in ("disabled", "not_run", "unknown",
                                           "missing_input"),
            "status": sr_status,
            "ready_to_skip": _safe_count(sr.get("ready_to_skip")),
            "blocked": _safe_count(sr.get("blocked")),
            "needs_review": _safe_count(sr.get("needs_review")),
            "keep": _safe_count(sr.get("keep")),
            "operator_approved": _safe_count(sr.get("operator_approved")),
            "operator_rejected": _safe_count(sr.get("operator_rejected")),
            "missing_operator_decision": _safe_count(sr.get("missing_operator_decision")),
            # HARD INVARIANT: всегда False
            "auto_enforce_enabled": False,
        }
        if sr.get("error"):
            skip_readiness_section["error"] = str(sr["error"])

    ce = s.get("controlled_enforce_preflight")
    controlled_enforce_section = None
    if isinstance(ce, dict) and ce.get("enabled"):
        ce_status = _clean(ce.get("status")) or "unknown"
        controlled_enforce_section = {
            "available": ce_status not in ("disabled", "not_run", "unknown"),
            "status": ce_status,
            "ready_to_skip_items": _safe_count(ce.get("ready_to_skip_items")),
            "eligible_items": _safe_count(ce.get("eligible_items")),
            "blocked_items": _safe_count(ce.get("blocked_items")),
            "fatal_blocks": _safe_count(ce.get("fatal_blocks")),
            # HARD INVARIANTS: всегда False в preflight
            "would_apply": False,
            "enforce_enabled": False,
        }
        if ce.get("error"):
            controlled_enforce_section["error"] = str(ce["error"])

    cedr = s.get("controlled_enforce_dry_run")
    controlled_enforce_dry_run_section = None
    if isinstance(cedr, dict) and cedr.get("enabled"):
        cedr_status = _clean(cedr.get("status")) or "unknown"
        controlled_enforce_dry_run_section = {
            "available": cedr_status not in ("disabled", "not_run", "unknown"),
            "status": cedr_status,
            "eligible_items": _safe_count(cedr.get("eligible_items")),
            "logical_transitions": _safe_count(cedr.get("logical_transitions")),
            "would_skip_block_pairs": _safe_count(cedr.get("would_skip_block_pairs")),
            # HARD INVARIANTS: всегда False в dry-run
            "would_apply": False,
            "enforce_enabled": False,
        }
        if cedr.get("error"):
            controlled_enforce_dry_run_section["error"] = str(cedr["error"])

    # Controlled Enforce STATE — read-only видимость active state (что первый
    # controlled skip пометил исключённым из будущего enrichment). Это НЕ enforce.
    ces = s.get("controlled_enforce_state")
    controlled_enforce_state_section = None
    if isinstance(ces, dict) and ces.get("available"):
        controlled_enforce_state_section = {
            "available": True,
            "active_exclusions": _safe_count(ces.get("active_exclusions")),
            "active_transitions": _safe_count(ces.get("active_transitions")),
            "active_block_pairs": _safe_count(ces.get("active_block_pairs")),
            "scope_enrichment_only": bool(ces.get("scope_enrichment_only", True)),
            "transition": _clean(ces.get("transition")) or None,
            "rollback_id": _clean(ces.get("rollback_id")) or None,
            # HARD INVARIANTS — это видимость, не enforce/apply
            "would_apply": False,
            "enforce_enabled": False,
        }
        if ces.get("error"):
            controlled_enforce_state_section["error"] = str(ces["error"])

    # Controlled Enforce SELECTION OBSERVE — observe-only сравнение
    # default OFF vs state ON (Qwen НЕ вызывался).
    ceso = s.get("controlled_enforce_selection_observe")
    controlled_enforce_selection_observe_section = None
    if isinstance(ceso, dict) and ceso.get("available"):
        controlled_enforce_selection_observe_section = {
            "available": True,
            "default_selected": _safe_count(ceso.get("default_selected")),
            "state_on_selected": _safe_count(ceso.get("state_on_selected")),
            "excluded_by_state": _safe_count(ceso.get("excluded_by_state")),
            "excluded_logical_transitions":
                _safe_count(ceso.get("excluded_logical_transitions")),
            "qwen_calls": _safe_count(ceso.get("qwen_calls")),
            # HARD INVARIANTS — observe-only
            "would_modify_runtime": False,
            "runtime_not_modified_by_selection": True,
        }
        if ceso.get("error"):
            controlled_enforce_selection_observe_section["error"] = str(ceso["error"])

    # per-delta compact evidence → attach к карточкам дельт (по delta_id).
    # Успешный attach — НЕ warning (не деградирует статус); счётчик кладём
    # в саму grounded_evidence-секцию для прозрачности.
    ge_compact_by_id = grounded_evidence_compact_by_delta_id(grounded_evidence_report)
    if ge_compact_by_id:
        attached = 0
        for sec in sections:
            for card in sec.get("cards") or []:
                if not isinstance(card, dict):
                    continue
                comp = ge_compact_by_id.get(card.get("delta_id"))
                if comp:
                    card["grounded_evidence"] = comp
                    attached += 1
        if isinstance(grounded_evidence_section, dict):
            grounded_evidence_section["attached_to_cards"] = attached

    summary_warnings = _str_list(s.get("warnings"))
    if s.get("warnings") and not summary_warnings:
        adapter_warnings.append("summary_warnings_invalid_ignored")
    artifacts = s.get("artifacts") if isinstance(s.get("artifacts"), dict) else {}
    if s.get("artifacts") and not artifacts:
        adapter_warnings.append("artifact_refs_invalid_ignored")

    # adapter-warning'и = неполнота payload → честная деградация статуса;
    # warnings самого summary статус не меняют (dry run уже учёл их сам)
    if status == "ok" and adapter_warnings:
        status = "completed_with_warnings"

    payload = {
        "version": PAYLOAD_VERSION,
        "kind": PAYLOAD_KIND,
        "status": status,
        "headline": headline,
        "sections": sections,
        "filters": {},
        "graphic_readiness": graphic_readiness,
        "warnings": adapter_warnings + summary_warnings,
        "artifact_refs": {str(k): str(v) for k, v in artifacts.items()},
    }
    if graphic_vision_section is not None:
        payload["graphic_vision"] = graphic_vision_section
    if graphic_vision_grounding_section is not None:
        payload["graphic_vision_grounding"] = graphic_vision_grounding_section
    if grounded_evidence_section is not None:
        payload["grounded_evidence"] = grounded_evidence_section
    if entity_alignment_section is not None:
        payload["entity_alignment_preview"] = entity_alignment_section
    if link_validation_section is not None:
        payload["link_validation"] = link_validation_section
    if exclusion_preview_section is not None:
        payload["exclusion_preview_v2"] = exclusion_preview_section
    if skip_readiness_section is not None:
        payload["skip_readiness"] = skip_readiness_section
    if controlled_enforce_section is not None:
        payload["controlled_enforce_preflight"] = controlled_enforce_section
    if controlled_enforce_dry_run_section is not None:
        payload["controlled_enforce_dry_run"] = controlled_enforce_dry_run_section
    if controlled_enforce_state_section is not None:
        payload["controlled_enforce_state"] = controlled_enforce_state_section
    if controlled_enforce_selection_observe_section is not None:
        payload["controlled_enforce_selection_observe"] = \
            controlled_enforce_selection_observe_section
    payload["filters"] = build_ui_filters(payload)
    return payload


def write_pipeline_v2_ui_payload(out_path: str | Path, payload: dict) -> Path:
    """Атомарно записать UI payload JSON (tmp + os.replace)."""
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    fd, tmp = tempfile.mkstemp(dir=str(out.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        os.replace(tmp, out)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return out


__all__ = [
    "PAYLOAD_VERSION",
    "PAYLOAD_KIND",
    "build_pipeline_v2_ui_payload",
    "build_ui_section_cards",
    "classify_ui_badge",
    "format_ui_delta_card",
    "truncate_ui_text",
    "build_ui_filters",
    "write_pipeline_v2_ui_payload",
]
