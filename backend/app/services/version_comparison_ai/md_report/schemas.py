"""Схемы ответов этапов (строгий структурный вывод ``--output-schema``) и контракт report_v2.

Подмножество JSON Schema — то, что проверяет ``response_contract``: все свойства обязательны,
«нет значения» — пустая строка. Аналитик и проверяющий ссылаются на номера строк оригинального MD
(``md_from``/``md_to``); составитель — на ID фактов реестра («Ф0123»). Лист, страницы PDF и строку
источника подставляет программа из паспорта факта.
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

FACT_IDS = _list(_s("ID факта из реестра, например «Ф0123»"))
COMPOSER_SCHEMA: dict = _obj({
    "run_id": _s(),
    "summary": _list(_s()), "summary_fact_ids": FACT_IDS,
    "transitions": _list(_obj({
        "transition_id": _s(), "change_scope": {"type": "string", "enum": list(SCOPES)},
        "significance": {"type": "string", "enum": list(SIGNIFICANCE)},
        "main_changes": _s("1 предложение: характер изменений перехода"),
        "changes": _list(_obj({
            "object": _s("линия, аппарат, материал или узел с точным обозначением"),
            "before": _s("было"), "after": _s("стало"), "fact_ids": FACT_IDS,
        }), "до 6 главных изменений «объект — было → стало»"),
        "construction_impact": _s("условное последствие для работ по предыдущей выдаче"),
        "impact_fact_ids": FACT_IDS,
    })),
    "significant_changes": _list(_obj({
        "priority": {"type": "string", "enum": list(PRIORITIES)}, "title": _s(), "version_range": _s(),
        "before_after": _s(), "consequence": _s(), "claim_ids": _list(_s()), "fact_ids": FACT_IDS,
    })),
    "limitations": _list(_s()),
})


def digest() -> str:
    return ai_settings.digest({"analyst": ANALYST_SCHEMA, "verifier": VERIFIER_SCHEMA, "composer": COMPOSER_SCHEMA})


# ------------------------------------------------------------------ контракт report_v2 (пользовательский отчёт)

def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _check_ids(ids: Any, where: str, errors: list[str], known: set[str]) -> None:
    if not isinstance(ids, list) or not all(isinstance(x, str) for x in ids):
        errors.append(f"{where}: список ID фактов")
        return
    bad = [x for x in ids if x not in known]
    if bad:
        errors.append(f"{where}: ID без паспорта в реестре источников: {', '.join(bad[:10])}")


def validate_report(rep: Any, lines_total: int | None = None) -> list[str]:
    """Проверка отчёта report_v2. Пустой список — годен. Источник — ID факта, паспорт которого есть
    в реестре ``facts`` отчёта (переход, лист, страницы, строка MD в пределах файла)."""
    errors: list[str] = []
    if not isinstance(rep, dict):
        return ["отчёт должен быть объектом"]
    if rep.get("schema_version") != "report_v2":
        errors.append("schema_version: ожидалось report_v2")
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
    facts = rep.get("facts")
    if not isinstance(facts, list):
        errors.append("facts: реестр источников — список")
        facts = []
    known = set()
    for i, f in enumerate(facts):
        w = f"facts[{i}]"
        if not isinstance(f, dict) or not isinstance(f.get("fact_id"), str):
            errors.append(f"{w}: паспорт с fact_id")
            continue
        known.add(f["fact_id"])
        ln = f.get("md_line")
        if not _is_int(ln) or ln < 1 or (lines_total and ln > lines_total):
            errors.append(f"{w}.md_line: строка MD в пределах файла")
        if not isinstance(f.get("transition"), str) or not f.get("transition"):
            errors.append(f"{w}.transition: переход")
        for k in ("pages_a", "pages_b"):
            if not isinstance(f.get(k), list) or not all(_is_int(x) for x in f.get(k) or []):
                errors.append(f"{w}.{k}: список страниц")
    _check_ids(rep.get("summary_sources") or [], "summary_sources", errors, known)
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
        if not isinstance(t.get("completeness"), dict) or not isinstance(t["completeness"].get("text"), str):
            errors.append(f"{w}.completeness: полнота по реестру покрытия")
        for j, c in enumerate(t.get("changes") or []):
            if not all(isinstance(c.get(k), str) for k in ("object", "before", "after")):
                errors.append(f"{w}.changes[{j}]: объект, было, стало — строки")
            _check_ids(c.get("sources"), f"{w}.changes[{j}].sources", errors, known)
            if not c.get("sources"):
                errors.append(f"{w}.changes[{j}].sources: у изменения должен быть факт")
        _check_ids(t.get("impact_sources") or [], f"{w}.impact_sources", errors, known)
        _check_ids(t.get("sources"), f"{w}.sources", errors, known)
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
        _check_ids(c.get("sources") or [], f"{w}.sources", errors, known)
    return errors
