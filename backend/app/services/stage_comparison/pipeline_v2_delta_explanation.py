# -*- coding: utf-8 -*-
"""Pipeline V2 — LLM Delta Explanation / Critic (первый LLM-слой, fail-soft).

Объясняет и критически проверяет УЖЕ найденные deterministic deltas (этап 4 —
[pipeline_v2_entity_diff](pipeline_v2_entity_diff.py)). Ключевой инвариант:

    LLM НЕ ищет отличия по всему тому.
    LLM НЕ добавляет новые дельты.
    LLM НЕ заменяет deterministic diff.
    LLM только объясняет/проверяет конкретную переданную дельту.

```text
entity_diff_report.deltas (+ optional graphic_descriptor readiness)
  → select_deltas_for_explanation (priority_only / changed / low_conf / …)
  → per delta:
        build_graphic_context_for_delta
        build_delta_explanation_prompt  (строгий контракт: только эта дельта)
        llm_runner(prompt) -> raw        (INJECTABLE; в тестах — fake)
        parse_delta_explanation_response (fail-soft JSON)
  → build_delta_explanation_report (+ coverage_notes по слабой графике)
  → delta_explanation_report.json
```

Безопасность:
  * `llm_runner` ИНЪЕКТИРУЕТСЯ. Модуль НЕ импортирует claude/provider напрямую и
    НЕ делает сетевых вызовов сам. `llm_runner=None` → fail-soft noop
    (`skipped_no_runner`), а не падение.
  * Любая ошибка/битый JSON от runner'а не валит модуль — дельта получает
    `failed`/`needs_human_review`, остальные обрабатываются дальше.
  * Graphic readiness учитывается: для `not_usable`/`needs_vision_enrichment`
    блоков не делается вывод «изменений нет» — ставится `possible_weak_graphic`
    и coverage_note «нужна дообработка графики».

Все функции чистые/детерминированные (при детерминированном `llm_runner`), кроме
`write_delta_explanation_report` (атомарная запись). Только stdlib.

См. docs/stage_comparison_pipeline_v2_delta_explanation.md.
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Callable, Optional

REPORT_VERSION = 1
REPORT_KIND = "stage_comparison_pipeline_v2_delta_explanation"

# Тип инъектируемого раннера: callable(prompt:str) -> str | dict
LLMRunner = Callable[[str], Any]

_DEFAULTS = {
    "mode": "explain_and_critic",            # explain | critic | explain_and_critic
    "selection_strategy": "priority_only",   # all|priority_only|low_confidence|needs_human_review|changed_only|engineering_first
    "max_deltas": 20,
    "include_high_confidence": False,
    "high_confidence_threshold": 0.75,
}

# Флаги дельты, повышающие приоритет для объяснения.
_PRIORITY_FLAGS = {
    "needs_human_review", "possible_ocr_noise", "fuzzy_match",
    "low_match_score", "one_sided_entity",
}

# ── engineering_first: selection-группы по entity_type ──
# Инженерное содержание — в приоритете; штампы/оглавление — квотированы,
# чтобы high-confidence stamp_field не вытеснял cable/power/scheme.
_SELECTION_ENGINEERING_TYPES = {
    "cable", "equipment", "power_supply", "scheme_component",
    "scheme_connection_hint", "table_row", "requirement", "norm_reference",
}
_SELECTION_STAMP_TYPES = {"stamp_field"}
_SELECTION_NAVIGATION_TYPES = {"contents_item", "document_section",
                               "change_log_item"}
# Флаги-маркеры вероятного артефакта извлечения/слабого матча. Сюда НЕ входят
# left/right_evidence_missing (их имеет каждая one-sided дельта) и fuzzy_match
# (легитимный матч-метод).
_SELECTION_WEAK_FLAGS = {"possible_ocr_noise", "low_match_score"}

_SELECTION_GROUP_ORDER = ("engineering", "admin_stamp",
                          "navigation_contents", "weak_or_artifact")

_ENGINEERING_FIRST_DEFAULTS = {
    "engineering_quota": 12,
    "admin_stamp_quota": 4,
    "navigation_quota": 2,
    "weak_quota": 2,
    "per_subject_cap": 2,
}

_DELTA_TYPE_RANK = {"changed": 0, "added": 1, "removed": 2, "uncertain": 3}

_WEAK_GRAPHIC_READINESS = {"low", "not_usable"}
_WEAK_GRAPHIC_FLAGS = {"needs_vision_enrichment", "manual_review_recommended",
                       "graphic_without_key_entities", "graphic_without_text_layer",
                       "large_dense_graphic"}
_MATCHED_RISK_FLAGS = {"one_side_not_usable", "low_token_overlap",
                       "discipline_mismatch", "graphic_type_mismatch"}

_FIELD_MAX = 1200
_SHORT_MAX = 400

_RISK_LEVELS = {"high", "medium", "low", "none", "unknown"}
_CRITIC_VERDICTS = {"accept", "reject", "needs_human_review",
                    "possible_ocr_noise", "possible_weak_graphic"}
_GROUNDEDNESS = {"grounded", "partially_grounded", "not_grounded", "unclear"}


def _opt(options: Optional[dict], key: str) -> Any:
    if options and key in options and options[key] is not None:
        return options[key]
    return _DEFAULTS[key]


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _cap(value: Any, limit: int = _FIELD_MAX) -> str:
    s = _clean(value)
    if len(s) <= limit:
        return s
    return s[: limit - 1].rstrip() + "…"


def _safe(value: Any) -> str:
    s = _clean(value) or "na"
    return "".join(c if (c.isalnum() or c in "-_") else "_" for c in s)[:64]


# ─── selection ───────────────────────────────────────────────────────────────


def _is_priority_delta(d: dict, high_thr: float, include_high: bool) -> bool:
    if include_high:
        return True
    flags = set(d.get("quality_flags") or [])
    if flags & _PRIORITY_FLAGS:
        return True
    dt = d.get("delta_type")
    if dt in ("uncertain", "added", "removed"):
        return True
    # changed: приоритет только если уверенность не высокая
    try:
        conf = float(d.get("confidence") or 0.0)
    except (TypeError, ValueError):
        conf = 0.0
    return conf < high_thr


def classify_selection_group(delta: dict) -> str:
    """Selection-группа дельты для engineering_first.

    Приоритет: weak-маркеры (артефакты/шум) выносятся в weak_or_artifact
    независимо от entity_type; нераспознанные НЕ-weak типы трактуются как
    engineering (forward-compat: новые контентные типы не должны падать в
    хвост выборки).
    """
    d = delta if isinstance(delta, dict) else {}
    et = _clean(d.get("entity_type")).lower()
    flags = set(d.get("quality_flags") or [])
    if not et or et == "unknown" or flags & _SELECTION_WEAK_FLAGS:
        return "weak_or_artifact"
    if et in _SELECTION_STAMP_TYPES:
        return "admin_stamp"
    if et in _SELECTION_NAVIGATION_TYPES:
        return "navigation_contents"
    return "engineering"


def build_selection_group_key(delta: dict) -> str:
    """Ключ «одного события» для per_subject_cap.

    Композитная подпись штампа дробится diff'ом на атомарные дельты
    (composite + role + surname + date) с одним subject и одной парой
    страниц — cap не даёт одному событию занять всю квоту группы.
    """
    d = delta if isinstance(delta, dict) else {}
    et = _clean(d.get("entity_type")).lower() or "unknown"
    subj = (_clean(d.get("subject")) or _clean(d.get("field"))).lower()
    pages = d.get("page_numbers") if isinstance(d.get("page_numbers"), dict) else {}
    return f"{et}|{subj}|{pages.get('left')}|{pages.get('right')}"


def _selection_sort_key(d: dict):
    """Детерминированный порядок внутри группы: changed → added → removed,
    внутри типа — по убыванию confidence, затем по delta_id."""
    try:
        conf = float(d.get("confidence") or 0.0)
    except (TypeError, ValueError):
        conf = 0.0
    return (_DELTA_TYPE_RANK.get(d.get("delta_type"), 9), -conf,
            str(d.get("delta_id")))


def _engineering_first_config(options: Optional[dict]) -> dict:
    raw = (options or {}).get("engineering_first")
    raw = raw if isinstance(raw, dict) else {}
    cfg = dict(_ENGINEERING_FIRST_DEFAULTS)
    for key in cfg:
        try:
            if raw.get(key) is not None:
                cfg[key] = max(0, int(raw[key]))
        except (TypeError, ValueError):
            pass
    return cfg


def _select_engineering_first(deltas: list, max_deltas: Optional[int],
                              high_thr: float, include_high: bool,
                              options: Optional[dict]) -> list[dict]:
    """engineering_first: квотированная выборка по selection-группам.

    1) кандидаты фильтруются как в priority_only (include_high_confidence
       сохраняет прежнюю семантику);
    2) группы сортируются детерминированно (changed раньше added/removed,
       внутри — по confidence);
    3) per_subject_cap отсекает дробление одного события (излишки — в
       overflow, используются последними);
    4) проход 1 — квоты групп; проход 2 — остаток слотов из leftovers в
       порядке приоритета групп; проход 3 — overflow. max_deltas строгий.
    """
    cfg = _engineering_first_config(options)
    candidates = [d for d in deltas
                  if isinstance(d, dict)
                  and _is_priority_delta(d, high_thr, include_high)]

    groups: dict[str, list] = {g: [] for g in _SELECTION_GROUP_ORDER}
    for d in candidates:
        groups[classify_selection_group(d)].append(d)

    per_cap = cfg["per_subject_cap"]
    kept: dict[str, list] = {}
    overflow: list = []
    for g in _SELECTION_GROUP_ORDER:
        seen: dict[str, int] = {}
        kept[g] = []
        for d in sorted(groups[g], key=_selection_sort_key):
            key = build_selection_group_key(d)
            seen[key] = seen.get(key, 0) + 1
            if per_cap > 0 and seen[key] > per_cap:
                overflow.append(d)
            else:
                kept[g].append(d)

    limit = max_deltas if (max_deltas is not None and max_deltas >= 0) else None
    quotas = {"engineering": cfg["engineering_quota"],
              "admin_stamp": cfg["admin_stamp_quota"],
              "navigation_contents": cfg["navigation_quota"],
              "weak_or_artifact": cfg["weak_quota"]}

    selected: list = []
    leftovers: dict[str, list] = {}

    def _room() -> Optional[int]:
        return None if limit is None else max(0, limit - len(selected))

    # проход 1: квоты
    for g in _SELECTION_GROUP_ORDER:
        take = quotas[g]
        room = _room()
        if room is not None:
            take = min(take, room)
        selected.extend(kept[g][:take])
        leftovers[g] = kept[g][take:]
    # проход 2: добор из leftovers в порядке приоритета (если в инженерной
    # группе пусто — слоты достаются stamp/navigation, но НЕ наоборот)
    for g in _SELECTION_GROUP_ORDER:
        room = _room()
        if room == 0:
            break
        selected.extend(leftovers[g][:room] if room is not None else leftovers[g])
    # проход 3: overflow per_subject_cap — последним
    room = _room()
    if room != 0 and overflow:
        selected.extend(overflow[:room] if room is not None else overflow)

    return selected[:limit] if limit is not None else selected


def select_deltas_for_explanation(entity_diff_report: dict,
                                  options: Optional[dict] = None) -> list[dict]:
    """Выбрать дельты для отправки в LLM по стратегии (без отправки всего тома)."""
    deltas = list((entity_diff_report or {}).get("deltas") or [])
    strategy = _opt(options, "selection_strategy")
    high_thr = float(_opt(options, "high_confidence_threshold"))
    include_high = bool(_opt(options, "include_high_confidence"))
    max_deltas = int(_opt(options, "max_deltas"))

    if strategy == "all":
        selected = list(deltas)
    elif strategy == "engineering_first":
        selected = _select_engineering_first(deltas, max_deltas, high_thr,
                                             include_high, options)
    elif strategy == "changed_only":
        selected = [d for d in deltas if d.get("delta_type") == "changed"]
    elif strategy == "low_confidence":
        def _low(d):
            try:
                return float(d.get("confidence") or 0.0) < high_thr
            except (TypeError, ValueError):
                return True
        selected = [d for d in deltas if _low(d)]
    elif strategy == "needs_human_review":
        selected = [d for d in deltas
                    if "needs_human_review" in (d.get("quality_flags") or [])]
    else:  # priority_only (default)
        selected = [d for d in deltas if _is_priority_delta(d, high_thr, include_high)]

    if max_deltas is not None and max_deltas >= 0:
        selected = selected[:max_deltas]
    return selected


# ─── graphic context ─────────────────────────────────────────────────────────


def _collect_descriptors(gdr: Any) -> list[dict]:
    if not isinstance(gdr, dict):
        return []
    out: list[dict] = []
    if isinstance(gdr.get("descriptors"), list):
        out += gdr["descriptors"]
    for side in ("left", "right"):
        sub = gdr.get(side)
        if isinstance(sub, dict) and isinstance(sub.get("descriptors"), list):
            out += sub["descriptors"]
    return out


def _collect_matched(gdr: Any) -> list[dict]:
    if not isinstance(gdr, dict):
        return []
    out: list[dict] = []
    if isinstance(gdr.get("matched_graphic_blocks"), list):
        out += gdr["matched_graphic_blocks"]
    sub = gdr.get("matched")
    if isinstance(sub, list):
        out += sub
    elif isinstance(sub, dict) and isinstance(sub.get("matched_graphic_blocks"), list):
        out += sub["matched_graphic_blocks"]
    return out


def build_graphic_context_for_delta(delta: dict,
                                    graphic_descriptor_report: Any = None) -> Optional[dict]:
    """Graphic readiness для блока дельты (или None, если графики нет)."""
    descriptors = _collect_descriptors(graphic_descriptor_report)
    if not descriptors:
        return None
    ids = {delta.get("left_block_id"), delta.get("right_block_id")} - {None}
    desc = next((d for d in descriptors if d.get("block_id") in ids), None)
    if desc is None:
        return None
    readiness = (desc.get("diff_readiness") or {}).get("readiness", "unknown")
    flags = desc.get("quality_flags") or []
    return {
        "readiness": readiness,
        "needs_vision_enrichment": "needs_vision_enrichment" in flags,
        "manual_review_recommended": "manual_review_recommended" in flags,
        "notes": sorted(f for f in flags if f in _WEAK_GRAPHIC_FLAGS),
    }


def _is_weak_graphic(graphic_context: Optional[dict]) -> bool:
    if not graphic_context:
        return False
    return (graphic_context.get("readiness") in _WEAK_GRAPHIC_READINESS
            or bool(graphic_context.get("needs_vision_enrichment"))
            or bool(graphic_context.get("manual_review_recommended")))


# ─── prompt ──────────────────────────────────────────────────────────────────


_PROMPT_PREAMBLE = (
    "Ты — инженер-эксперт по проектной документации. Пользователь — генподрядчик.\n"
    "ВАЖНЫЕ ОГРАНИЧЕНИЯ (соблюдай строго):\n"
    "- Ты НЕ ищешь новые отличия и НЕ просматриваешь весь том.\n"
    "- Ты НЕ добавляешь новые замечания и НЕ выдумываешь дельты.\n"
    "- Анализируй ТОЛЬКО переданную ниже одну дельту (изменение).\n"
    "- Используй ТОЛЬКО приведённые evidence слева (старая стадия) и справа (новая).\n"
    "- Если evidence недостаточно — verdict `needs_human_review`.\n"
    "- Если значение похоже на OCR-шум — verdict `possible_ocr_noise`.\n"
    "- Если графический блок weak/not_usable — verdict `possible_weak_graphic` и НЕ\n"
    "  утверждай, что изменений нет (нужна дообработка графики).\n"
    "- Верни СТРОГО валидный JSON по схеме ниже, без markdown и пояснений.\n"
)

_PROMPT_SCHEMA = (
    '{\n'
    '  "summary": "краткое описание изменения",\n'
    '  "engineering_meaning": "инженерный смысл",\n'
    '  "contractor_impact": "влияние для генподрядчика",\n'
    '  "risk_level": "high|medium|low|none|unknown",\n'
    '  "groundedness": {"verdict": "grounded|partially_grounded|not_grounded|unclear",\n'
    '                   "reason": "...", "uses_left_evidence": true, "uses_right_evidence": true},\n'
    '  "critic": {"verdict": "accept|reject|needs_human_review|possible_ocr_noise|possible_weak_graphic",\n'
    '             "reason": "...", "should_show_to_engineer": true}\n'
    '}'
)


def build_delta_explanation_prompt(delta: dict, graphic_context: Optional[dict] = None,
                                   options: Optional[dict] = None) -> str:
    """Собрать строгий prompt по ОДНОЙ дельте (контракт «не ищи новые отличия»)."""
    mode = _opt(options, "mode")
    ev = delta.get("evidence") or {}
    ev_l = ev.get("left") or {}
    ev_r = ev.get("right") or {}
    pages = delta.get("page_numbers") or {}

    lines = [_PROMPT_PREAMBLE, ""]
    lines.append(f"Режим: {mode}")
    lines.append("")
    lines.append("=== ДЕЛЬТА (только её анализируй) ===")
    lines.append(f"delta_id: {delta.get('delta_id')}")
    lines.append(f"delta_type: {delta.get('delta_type')}")
    lines.append(f"entity_type: {delta.get('entity_type')}")
    lines.append(f"semantic_group: {delta.get('semantic_group')}")
    lines.append(f"subject: {_cap(delta.get('subject'), _SHORT_MAX)}")
    lines.append(f"field: {delta.get('field')}")
    lines.append(f"old_value: {_cap(delta.get('old_value'), _SHORT_MAX)}")
    lines.append(f"new_value: {_cap(delta.get('new_value'), _SHORT_MAX)}")
    lines.append(f"confidence: {delta.get('confidence')}")
    lines.append(f"quality_flags: {', '.join(delta.get('quality_flags') or []) or '—'}")
    lines.append(f"page_numbers: left={pages.get('left')} right={pages.get('right')}")
    lines.append(f"block_ids: left={delta.get('left_block_id')} right={delta.get('right_block_id')}")
    lines.append("")
    lines.append("=== EVIDENCE (единственный источник; не выходи за его пределы) ===")
    lines.append(f"left.quote: {_cap(ev_l.get('quote'), _SHORT_MAX)}")
    lines.append(f"left.source: {ev_l.get('source')}")
    lines.append(f"right.quote: {_cap(ev_r.get('quote'), _SHORT_MAX)}")
    lines.append(f"right.source: {ev_r.get('source')}")
    lines.append("")
    if graphic_context:
        lines.append("=== GRAPHIC READINESS блока ===")
        lines.append(f"readiness: {graphic_context.get('readiness')}")
        lines.append(f"needs_vision_enrichment: {graphic_context.get('needs_vision_enrichment')}")
        lines.append(f"manual_review_recommended: {graphic_context.get('manual_review_recommended')}")
        if graphic_context.get("notes"):
            lines.append(f"notes: {', '.join(graphic_context['notes'])}")
        lines.append("")
    lines.append("=== ВЕРНИ СТРОГО JSON ПО СХЕМЕ ===")
    lines.append(_PROMPT_SCHEMA)
    return "\n".join(lines)


# ─── parsing ─────────────────────────────────────────────────────────────────


def _extract_json(raw: str) -> Optional[dict]:
    raw = raw or ""
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else None
    except (json.JSONDecodeError, ValueError):
        pass
    start = raw.find("{")
    end = raw.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            obj = json.loads(raw[start:end + 1])
            return obj if isinstance(obj, dict) else None
        except (json.JSONDecodeError, ValueError):
            return None
    return None


def _norm_enum(value: Any, allowed: set, default: str) -> str:
    v = _clean(value).lower()
    return v if v in allowed else default


def parse_delta_explanation_response(raw_response: Any, delta: Optional[dict] = None) -> dict:
    """Fail-soft разбор JSON-ответа LLM в нормализованный фрагмент explanation."""
    obj = _extract_json(_clean(raw_response))
    if obj is None:
        return {
            "parse_ok": False, "parse_error": "json_not_found",
            "summary": "", "engineering_meaning": "", "contractor_impact": "",
            "risk_level": "unknown",
            "groundedness": {"verdict": "unclear", "reason": "", "uses_left_evidence": False,
                             "uses_right_evidence": False},
            "critic": {"verdict": "needs_human_review", "reason": "llm_response_parse_failed",
                       "should_show_to_engineer": True},
        }
    g = obj.get("groundedness") if isinstance(obj.get("groundedness"), dict) else {}
    c = obj.get("critic") if isinstance(obj.get("critic"), dict) else {}
    return {
        "parse_ok": True,
        "parse_error": None,
        "summary": _cap(obj.get("summary")),
        "engineering_meaning": _cap(obj.get("engineering_meaning")),
        "contractor_impact": _cap(obj.get("contractor_impact")),
        "risk_level": _norm_enum(obj.get("risk_level"), _RISK_LEVELS, "unknown"),
        "groundedness": {
            "verdict": _norm_enum(g.get("verdict"), _GROUNDEDNESS, "unclear"),
            "reason": _cap(g.get("reason"), _SHORT_MAX),
            "uses_left_evidence": bool(g.get("uses_left_evidence")),
            "uses_right_evidence": bool(g.get("uses_right_evidence")),
        },
        "critic": {
            "verdict": _norm_enum(c.get("verdict"), _CRITIC_VERDICTS, "needs_human_review"),
            "reason": _cap(c.get("reason"), _SHORT_MAX),
            "should_show_to_engineer": bool(c.get("should_show_to_engineer", True)),
        },
    }


# ─── single delta ────────────────────────────────────────────────────────────


def _invoke_runner(llm_runner: LLMRunner,
                   prompt: str) -> tuple[str, str, Optional[str], dict]:
    """Вызвать инъектированный runner.

    Возвращает (raw_text, raw_status, error, runner_meta). ``runner_meta`` —
    self-reported идентификация runner'а (``provider``/``model`` из dict-ответа);
    пустой dict для string-ответов и ошибок.
    """
    try:
        out = llm_runner(prompt)
    except Exception as exc:  # noqa: BLE001 — fail-soft по контракту
        return "", "failed", f"{type(exc).__name__}: {exc}", {}
    if isinstance(out, str):
        return ((out, "ok", None, {}) if out.strip()
                else ("", "failed", "empty_response", {}))
    if isinstance(out, dict):
        meta = {k: _clean(out.get(k)) for k in ("provider", "model")
                if _clean(out.get(k))}
        raw = _clean(out.get("raw_response") or out.get("text") or out.get("response"))
        status = _clean(out.get("status") or out.get("raw_status"))
        err = out.get("error")
        if status.lower() in ("skipped", "disabled"):
            # runner-заглушка (noop: disabled / provider_not_available) —
            # сознательный пропуск, НЕ сбой
            return raw, "skipped", err or status, meta
        if raw and status in ("", "ok", "completed", "success"):
            return raw, "ok", err, meta
        return raw, "failed", err or (status or "no_response"), meta
    return "", "failed", "unsupported_runner_return", {}


def _status_from_critic(verdict: str) -> str:
    if verdict == "accept":
        return "explained"
    if verdict == "reject":
        return "critic_rejected"
    return "needs_human_review"  # needs_human_review / possible_ocr_noise / possible_weak_graphic


def explain_single_delta(delta: dict, graphic_context: Optional[dict] = None,
                         options: Optional[dict] = None, llm_runner: Optional[LLMRunner] = None) -> dict:
    """Объяснить/проверить ОДНУ дельту (fail-soft, runner инъектируется)."""
    mode = _opt(options, "mode")
    weak = _is_weak_graphic(graphic_context)
    quality_flags: list[str] = []
    input_delta = {
        "delta_type": delta.get("delta_type"),
        "entity_type": delta.get("entity_type"),
        "old_value": _cap(delta.get("old_value"), _SHORT_MAX),
        "new_value": _cap(delta.get("new_value"), _SHORT_MAX),
    }
    gc = graphic_context or {}
    graphic_section = {
        "readiness": gc.get("readiness", "unknown"),
        "needs_vision_enrichment": bool(gc.get("needs_vision_enrichment")),
        "manual_review_recommended": bool(gc.get("manual_review_recommended")),
        "notes": list(gc.get("notes") or []),
    }

    base = {
        "explanation_id": f"expl_{_safe(delta.get('delta_id'))}",
        "delta_id": delta.get("delta_id"),
        "mode": mode,
        "summary": "", "engineering_meaning": "", "contractor_impact": "",
        "risk_level": "unknown",
        "groundedness": {"verdict": "unclear", "reason": "", "uses_left_evidence": False,
                         "uses_right_evidence": False},
        "critic": {"verdict": "needs_human_review", "reason": "", "should_show_to_engineer": True},
        "graphic_context": graphic_section,
        "input_delta": input_delta,
        "model": {"provider": "none", "raw_status": "skipped", "error": None},
        "quality_flags": quality_flags,
    }

    if weak:
        quality_flags.append("possible_weak_graphic")

    # noop / нет runner'а → fail-soft skipped
    if llm_runner is None:
        base["status"] = "skipped_no_runner"
        base["critic"]["verdict"] = "possible_weak_graphic" if weak else "needs_human_review"
        base["critic"]["reason"] = ("graphic block weak/not_usable" if weak
                                    else "no llm_runner provided")
        base["quality_flags"] = sorted(set(quality_flags + ["skipped_no_runner"]))
        return base

    prompt = build_delta_explanation_prompt(delta, graphic_context, options)
    raw, raw_status, err, runner_meta = _invoke_runner(llm_runner, prompt)
    # provider НЕ хардкодится: runner self-report'ит provider/model в dict-ответе
    # (реальный claude-wrapper передаст provider="claude"); string-ответ →
    # анонимный инъектированный runner → "custom_runner"
    base["model"] = {"provider": runner_meta.get("provider") or "custom_runner",
                     "raw_status": raw_status, "error": err}
    if runner_meta.get("model"):
        base["model"]["model"] = runner_meta["model"]

    if raw_status == "skipped":
        # noop-runner (disabled / provider_not_available) — сознательный
        # пропуск, та же семантика, что llm_runner=None, а НЕ сбой
        base["status"] = "skipped_no_runner"
        base["critic"]["verdict"] = "possible_weak_graphic" if weak else "needs_human_review"
        base["critic"]["reason"] = err or "runner_skipped"
        base["quality_flags"] = sorted(set(quality_flags + ["skipped_no_runner"]))
        return base

    if raw_status != "ok":
        base["status"] = "failed"
        base["critic"]["verdict"] = "needs_human_review"
        base["critic"]["reason"] = err or "llm_invoke_failed"
        base["quality_flags"] = sorted(set(quality_flags + ["llm_invoke_failed"]))
        return base

    parsed = parse_delta_explanation_response(raw, delta)
    base["summary"] = parsed["summary"]
    base["engineering_meaning"] = parsed["engineering_meaning"]
    base["contractor_impact"] = parsed["contractor_impact"]
    base["risk_level"] = parsed["risk_level"]
    base["groundedness"] = parsed["groundedness"]
    base["critic"] = parsed["critic"]

    if not parsed["parse_ok"]:
        quality_flags.append("llm_response_parse_failed")
        base["status"] = "needs_human_review"
        base["quality_flags"] = sorted(set(quality_flags))
        return base

    base["status"] = _status_from_critic(parsed["critic"]["verdict"])
    base["quality_flags"] = sorted(set(quality_flags))
    return base


# ─── coverage notes (графика) ────────────────────────────────────────────────


def _build_coverage_notes(graphic_descriptor_report: Any) -> list[dict]:
    notes: list[dict] = []
    for d in _collect_descriptors(graphic_descriptor_report):
        readiness = (d.get("diff_readiness") or {}).get("readiness", "unknown")
        flags = set(d.get("quality_flags") or [])
        weak = readiness in _WEAK_GRAPHIC_READINESS or bool(flags & _WEAK_GRAPHIC_FLAGS)
        if weak:
            notes.append({
                "kind": "weak_graphic",
                "block_id": d.get("block_id"),
                "page_number": d.get("page_number"),
                "readiness": readiness,
                "flags": sorted(flags & (_WEAK_GRAPHIC_FLAGS | {"low_token_count"})),
                "message": ("Графический блок слабо распознан "
                            f"(readiness={readiness}) — пустой diff может быть из-за "
                            "графики, нужна дообработка (vision enrichment), а не "
                            "вывод «изменений нет»."),
            })
    for m in _collect_matched(graphic_descriptor_report):
        risks = set(m.get("risk_flags") or []) & _MATCHED_RISK_FLAGS
        if risks:
            notes.append({
                "kind": "matched_risk",
                "block_match_id": m.get("block_match_id"),
                "left_block_id": m.get("left_block_id"),
                "right_block_id": m.get("right_block_id"),
                "risk_flags": sorted(risks),
                "message": ("Сопоставленная графическая пара ненадёжна "
                            f"({', '.join(sorted(risks))}) — diff по ней требует проверки."),
            })
    return notes


# ─── report ──────────────────────────────────────────────────────────────────


def build_delta_explanation_report(entity_diff_report: dict, explanations: list[dict],
                                   options: Optional[dict] = None,
                                   coverage_notes: Optional[list] = None,
                                   selection: Optional[dict] = None) -> dict:
    """Собрать итоговый delta_explanation_report."""
    explanations = explanations or []
    coverage_notes = coverage_notes or []
    deltas_total = len((entity_diff_report or {}).get("deltas") or [])

    by_status: dict[str, int] = {}
    by_risk: dict[str, int] = {}
    accepted = rejected = nhr = ocr_noise = weak_graphic = 0
    for e in explanations:
        st = e.get("status", "unknown")
        by_status[st] = by_status.get(st, 0) + 1
        rl = e.get("risk_level", "unknown")
        by_risk[rl] = by_risk.get(rl, 0) + 1
        verdict = (e.get("critic") or {}).get("verdict")
        flags = set(e.get("quality_flags") or [])
        if verdict == "accept":
            accepted += 1
        elif verdict == "reject":
            rejected += 1
        elif verdict == "needs_human_review":
            nhr += 1
        if verdict == "possible_ocr_noise" or "possible_ocr_noise" in flags:
            ocr_noise += 1
        if verdict == "possible_weak_graphic" or "possible_weak_graphic" in flags:
            weak_graphic += 1

    explained_total = by_status.get("explained", 0)
    skipped_total = by_status.get("skipped_no_runner", 0)
    failed_total = by_status.get("failed", 0)
    nhr_status_total = by_status.get("needs_human_review", 0)

    warnings: list[str] = []
    parse_failed = sum(1 for e in explanations
                       if "llm_response_parse_failed" in (e.get("quality_flags") or []))
    if parse_failed:
        warnings.append(f"llm_response_parse_failed: {parse_failed}")
    if failed_total:
        warnings.append(f"llm_failed_explanations: {failed_total}")
    if skipped_total and not explained_total and explanations:
        warnings.append("no_llm_runner: all explanations skipped")

    summary = {
        "deltas_total": deltas_total,
        "selected_total": len(explanations),
        "explained_total": explained_total,
        "skipped_total": skipped_total,
        "failed_total": failed_total,
        "accepted_total": accepted,
        "rejected_total": rejected,
        "needs_human_review_total": max(nhr, nhr_status_total),
        "possible_ocr_noise_total": ocr_noise,
        "possible_weak_graphic_total": weak_graphic,
        "by_risk_level": by_risk,
        "by_status": by_status,
        "warnings_count": len(warnings),
    }

    if selection is None:
        selection = {"strategy": _opt(options, "selection_strategy"),
                     "selected_delta_ids": [e.get("delta_id") for e in explanations]}

    return {
        "version": REPORT_VERSION,
        "kind": REPORT_KIND,
        "summary": summary,
        "selection": selection,
        "explanations": explanations,
        "coverage_notes": coverage_notes,
        "warnings": warnings,
    }


def explain_entity_diff_report(entity_diff_report: dict,
                               graphic_descriptor_report: Any = None,
                               options: Optional[dict] = None,
                               llm_runner: Optional[LLMRunner] = None) -> dict:
    """Полный прогон: выбрать дельты → объяснить/проверить → собрать отчёт.

    `llm_runner=None` → все объяснения `skipped_no_runner` (fail-soft, не падает).
    Coverage notes по слабой графике строятся независимо от наличия runner'а.
    """
    entity_diff_report = entity_diff_report or {}
    selected = select_deltas_for_explanation(entity_diff_report, options)

    explanations: list[dict] = []
    for delta in selected:
        gctx = build_graphic_context_for_delta(delta, graphic_descriptor_report)
        explanations.append(explain_single_delta(delta, gctx, options, llm_runner))

    coverage_notes = _build_coverage_notes(graphic_descriptor_report)
    selection = {
        "strategy": _opt(options, "selection_strategy"),
        "selected_delta_ids": [d.get("delta_id") for d in selected],
    }
    return build_delta_explanation_report(
        entity_diff_report, explanations, options, coverage_notes, selection)


# ─── writer ──────────────────────────────────────────────────────────────────


def write_delta_explanation_report(out_path: str | Path, report: dict) -> Path:
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
    "explain_entity_diff_report",
    "select_deltas_for_explanation",
    "classify_selection_group",
    "build_selection_group_key",
    "build_delta_explanation_prompt",
    "parse_delta_explanation_response",
    "explain_single_delta",
    "build_graphic_context_for_delta",
    "build_delta_explanation_report",
    "write_delta_explanation_report",
]
