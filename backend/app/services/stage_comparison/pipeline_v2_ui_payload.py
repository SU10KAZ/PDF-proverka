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
                                 options: Optional[dict] = None) -> dict:
    """Собрать UI payload из готовых артефактов Pipeline V2 (offline).

    ``summary`` — обязательный вход (``pipeline_v2_summary.json``);
    остальные отчёты опциональны: без них payload строится по счётчикам
    summary, карточки деградируют (warning, не падение).
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
