"""Схемы ответов этапов (строгий структурный вывод ``--output-schema``) и контракт report_v1.

Подмножество JSON Schema — то, что проверяет ``response_contract``: все свойства обязательны,
«нет значения» — пустая строка. Ссылки на источник — номера строк оригинального MD
(``md_from``/``md_to``); лист и страницы PDF подставляет программа из разбора.
"""
from __future__ import annotations

from typing import Any

from .. import settings as ai_settings

SCOPES = ("Нет изменений содержания", "Только оформление", "Локальные изменения", "Существенные изменения",
          "Широкая переработка", "Не установлено")
SIGNIFICANCE = ("Нет новых технических изменений", "Низкая", "Средняя", "Высокая", "Не установлена")
CONTENT = ("technical", "formatting_only", "no_content_changes", "unknown")
VERDICTS = ("supported", "qualified", "contradictory", "insufficient", "rejected")
PRIORITIES = ("P1", "P2", "P3")
TECHNICAL_SCOPES = ("Локальные изменения", "Существенные изменения", "Широкая переработка")

# Порядок шкал: оценка перехода, сведённая из частей, не может быть ниже оценки любой проверенной части.
SCOPE_RANK = {"Не установлено": -1, "Нет изменений содержания": 0, "Только оформление": 1, "Локальные изменения": 2,
              "Существенные изменения": 3, "Широкая переработка": 4}
SIGNIFICANCE_RANK = {"Не установлена": -1, "Нет новых технических изменений": 0, "Низкая": 1, "Средняя": 2,
                     "Высокая": 3}

# Синонимы шкалы (образец report_v1 пишет «Большой», «Локальный» …): смысл сохраняется, форма — каноническая.
SCOPE_SYNONYMS = {
    "без изменений": "Нет изменений содержания", "нет изменений": "Нет изменений содержания",
    "нет изменений содержания": "Нет изменений содержания", "только оформление": "Только оформление",
    "оформление": "Только оформление", "локальный": "Локальные изменения", "локальные изменения": "Локальные изменения",
    "существенный": "Существенные изменения", "существенные изменения": "Существенные изменения",
    "большой": "Широкая переработка", "очень большой": "Широкая переработка",
    "широкая переработка": "Широкая переработка", "не установлено": "Не установлено",
}
SIGNIFICANCE_SYNONYMS = {
    "нет новых технических изменений": "Нет новых технических изменений", "низкая": "Низкая",
    "средняя": "Средняя", "высокая": "Высокая", "не установлена": "Не установлена",
    "неопределённая": "Не установлена", "неопределенная": "Не установлена", "неизвестно": "Не установлена",
}


def normalize_scope(value: str) -> str | None:
    return SCOPE_SYNONYMS.get((value or "").strip().lower().rstrip("."))


def normalize_significance(value: str) -> str | None:
    return SIGNIFICANCE_SYNONYMS.get((value or "").strip().lower().rstrip("."))


def _s(desc: str = "") -> dict:
    return {"type": "string", "description": desc} if desc else {"type": "string"}


def _list(item: dict, desc: str = "") -> dict:
    return {"type": "array", "items": item, "description": desc} if desc else {"type": "array", "items": item}


def _obj(props: dict[str, Any], desc: str = "") -> dict:
    out = {"type": "object", "additionalProperties": False, "required": list(props), "properties": props}
    if desc:
        out["description"] = desc
    return out


REF = _obj({"md_from": {"type": "integer"}, "md_to": {"type": "integer"}}, "включительный диапазон строк MD")
SRC = _obj({"md_from": {"type": "integer"}, "md_to": {"type": "integer"},
            "context": _s("что подтверждает фрагмент")})
NOTE = _obj({"text": _s(), "evidence": _list(REF)})

ANALYST_SCHEMA: dict = _obj({
    "run_id": _s(), "group_id": _s(),
    "processed_block_ids": _list(_s()), "unprocessed_block_ids": _list(_s()),
    "partial": {"type": "boolean"}, "input_problems": _list(_s()),
    "transitions": _list(_obj({
        "transition_id": _s(), "analysis_complete": {"type": "boolean"},
        "content_status": {"type": "string", "enum": list(CONTENT)},
        "change_scope": {"type": "string", "enum": list(SCOPES)},
        "significance": {"type": "string", "enum": list(SIGNIFICANCE)},
        "assessment_basis": _s("короткое предметное основание оценок"),
        "systems": _list(_s()),
        "facts": _list(_obj({
            "fact_id": _s(), "object": _s(), "system": _s(), "before": _s(), "after": _s(),
            "possible_consequence": _s(), "evidence": _list(REF), "uncertainty": _s(),
            "related_objects": _list(_s("точное обозначение")), "serious_candidate": {"type": "boolean"},
        })),
        "construction_impact": _s(), "explicit_conditions": _list(NOTE), "contradictions": _list(NOTE),
        "missing_information": _list(_s()),
    })),
})

VERIFIER_SCHEMA: dict = _obj({
    "run_id": _s(), "unit_id": _s(),
    "claims": _list(_obj({
        "claim_id": _s(), "verdict": {"type": "string", "enum": list(VERDICTS)},
        "verified_wording": _s(), "evidence": _list(REF), "brief_reason": _s(),
        "missing_information": _list(_s()), "linked_claim_ids": _list(_s()),
    })),
    "transitions": _list(_obj({
        "transition_id": _s(), "content_status": {"type": "string", "enum": list(CONTENT)},
        "change_scope": {"type": "string", "enum": list(SCOPES)},
        "significance": {"type": "string", "enum": list(SIGNIFICANCE)},
        "assessment_basis": _s(), "main_changes": _s(), "construction_impact": _s(),
        "key_claim_ids": _list(_s()), "evidence": _list(REF),
    })),
    "significant_candidates": _list(_obj({
        "title": _s(), "claim_ids": _list(_s()), "version_range": _s(), "before_after": _s(),
        "consequence": _s(), "evidence": _list(REF),
    })),
    "unresolved_questions": _list(_s()), "missing_transitions_or_blocks": _list(_s()),
})

COMPOSER_SCHEMA: dict = _obj({
    "run_id": _s(),
    "summary": _list(_s()), "summary_sources": _list(SRC),
    "transitions": _list(_obj({
        "transition_id": _s(), "change_scope": {"type": "string", "enum": list(SCOPES)},
        "significance": {"type": "string", "enum": list(SIGNIFICANCE)},
        "main_changes": _s(), "construction_impact": _s(), "sources": _list(SRC),
    })),
    "significant_changes": _list(_obj({
        "priority": {"type": "string", "enum": list(PRIORITIES)}, "title": _s(), "version_range": _s(),
        "before_after": _s(), "consequence": _s(), "claim_ids": _list(_s()), "sources": _list(SRC),
    })),
    "limitations": _list(_s()),
})


def digest() -> str:
    return ai_settings.digest({"analyst": ANALYST_SCHEMA, "verifier": VERIFIER_SCHEMA, "composer": COMPOSER_SCHEMA})


# ------------------------------------------------------------------ контракт report_v1 (пользовательский отчёт)

def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _check_source(src: Any, where: str, errors: list[str], lines_total: int | None) -> None:
    if not isinstance(src, dict):
        errors.append(f"{where}: ожидался объект источника")
        return
    for k in ("transition", "sheet"):
        if src.get(k) is not None and not isinstance(src.get(k), str):
            errors.append(f"{where}.{k}: строка или null")
    for k in ("pdf_before", "pdf_after"):
        if src.get(k) is not None and not _is_int(src.get(k)):
            errors.append(f"{where}.{k}: целое или null")
    ml = src.get("md_lines")
    if not (isinstance(ml, list) and len(ml) == 2 and all(_is_int(x) for x in ml) and 1 <= ml[0] <= ml[1]):
        errors.append(f"{where}.md_lines: ожидался диапазон [начало, конец]")
    elif lines_total and ml[1] > lines_total:
        errors.append(f"{where}.md_lines: {ml} за пределами файла ({lines_total} строк)")
    if not isinstance(src.get("context"), str):
        errors.append(f"{where}.context: строка")


def validate_report(rep: Any, lines_total: int | None = None) -> list[str]:
    """Проверка отчёта report_v1 (подходит и к образцу, и к результату запуска). Пустой список — годен."""
    errors: list[str] = []
    if not isinstance(rep, dict):
        return ["отчёт должен быть объектом"]
    if rep.get("schema_version") != "report_v1":
        errors.append("schema_version: ожидалось report_v1")
    for k in ("project_code", "title", "input_filename"):
        if not isinstance(rep.get(k), str) or not rep.get(k):
            errors.append(f"{k}: непустая строка")
    ab = rep.get("analysis_basis")
    if not isinstance(ab, dict) or not isinstance(ab.get("source_pdf_checked"), bool) \
            or not isinstance(ab.get("type"), str) or not isinstance(ab.get("description"), str):
        errors.append("analysis_basis: {type, description, source_pdf_checked: bool}")
    for k in ("summary", "limitations"):
        v = rep.get(k)
        if not isinstance(v, list) or not all(isinstance(x, str) and x.strip() for x in v):
            errors.append(f"{k}: список непустых строк")
    if not rep.get("summary"):
        errors.append("summary: общий вывод пуст")
    for i, s in enumerate(rep.get("summary_sources") or []):
        _check_source(s, f"summary_sources[{i}]", errors, lines_total)
    trs = rep.get("transitions")
    if not isinstance(trs, list) or not trs:
        errors.append("transitions: непустой список")
        trs = []
    for i, t in enumerate(trs):
        w = f"transitions[{i}]"
        if not isinstance(t, dict):
            errors.append(f"{w}: объект")
            continue
        for k in ("from_version", "to_version", "main_changes", "construction_impact"):
            if not isinstance(t.get(k), str) or not t.get(k).strip():
                errors.append(f"{w}.{k}: непустая строка")
        for k in ("from_date", "to_date"):
            if t.get(k) is not None and not isinstance(t.get(k), str):
                errors.append(f"{w}.{k}: строка или null")
        if normalize_scope(t.get("change_scope")) is None:
            errors.append(f"{w}.change_scope: неизвестное значение «{t.get('change_scope')}»")
        if normalize_significance(t.get("significance")) is None:
            errors.append(f"{w}.significance: неизвестное значение «{t.get('significance')}»")
        if not isinstance(t.get("sources"), list):
            errors.append(f"{w}.sources: список")
        for j, s in enumerate(t.get("sources") or []):
            _check_source(s, f"{w}.sources[{j}]", errors, lines_total)
    sig = rep.get("significant_changes")
    if not isinstance(sig, list):
        errors.append("significant_changes: список (может быть пустым)")
        sig = []
    for i, c in enumerate(sig):
        w = f"significant_changes[{i}]"
        if not isinstance(c, dict):
            errors.append(f"{w}: объект")
            continue
        for k in ("priority", "title", "version_range", "before_after", "consequence"):
            if not isinstance(c.get(k), str) or not c.get(k).strip():
                errors.append(f"{w}.{k}: непустая строка")
        if not c.get("sources"):
            errors.append(f"{w}.sources: у серьёзного изменения должен быть источник")
        for j, s in enumerate(c.get("sources") or []):
            _check_source(s, f"{w}.sources[{j}]", errors, lines_total)
    return errors
