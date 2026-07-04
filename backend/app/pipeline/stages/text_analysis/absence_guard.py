"""Детерминированное принуждение «стража отсутствия» (Stage 01 post-pass).

Исследование браков (03.07) показало: ~32% отклонений эксперта — «данные ЕСТЬ, ИИ
не увидел». Промпт-правило (см. task_builder._ABSENCE_GUARD_TEXT) просит модель перед
утверждением об отсутствии просканировать весь документ и заполнить `absence_checked`.
Но промпт-правила выполняются слабо (ср. findings-критик), поэтому здесь — жёсткая
машинная проверка: замечание-об-отсутствии без непустого `absence_checked` считается
непроверенным и понижается до «ПРОВЕРИТЬ ПО СМЕЖНЫМ» (не удаляется — инвариант «не reject»).

Работает только при PIPELINE_ABSENCE_GUARD_ENABLED. Fail-soft: любые кривые записи
пропускаются, стадия не падает.
"""
from __future__ import annotations

import re
from typing import Any

_VERIFY_SEVERITY = "ПРОВЕРИТЬ ПО СМЕЖНЫМ"

# Маркеры замечания-об-отсутствии (утверждение, что чего-то НЕТ). Намеренно НЕ ловим
# «указано неверно», «не соответствует» и т.п. — это претензии к значению, не к отсутствию.
_ABSENCE_PATTERNS = [
    r"не\s+указ\w+",          # не указан/указано/указана/указаны
    r"отсутств\w+",           # отсутствует/отсутствуют/отсутствие
    r"не\s+привед\w+",        # не приведён/приведена/приведено
    r"не\s+показ\w+",         # не показан/показана/показано
    r"не\s+обознач\w+",       # не обозначен/обозначена
    r"не\s+задан\w*",         # не задан/задана/задано
    r"не\s+определ\w+",       # не определён/определена
    r"не\s+предусмотр\w+",    # не предусмотрен/предусмотрена
    r"не\s+прораб\w+",        # не проработан
    r"не\s+детализир\w*",     # не детализировано
    r"нет\s+(?:данн\w+|сведен\w+|информац\w+)",  # нет данных/сведений/информации
    r"не\s+хват\w+",          # не хватает
    r"недоста(?:ёт|ет|точн\w+)",  # недостаёт/недостаточно
    r"пропущ\w+",             # пропущен/пропущено
]
_ABSENCE_RE = re.compile("|".join(_ABSENCE_PATTERNS), re.IGNORECASE)


def _is_absence_claim(finding: dict) -> bool:
    """Замечание утверждает отсутствие чего-либо?"""
    text = " ".join(
        str(finding.get(k) or "")
        for k in ("finding", "problem", "description", "category")
    )
    return bool(_ABSENCE_RE.search(text))


def _has_absence_evidence(finding: dict) -> bool:
    """Модель указала конкретные проверенные места (`absence_checked` непуст)?"""
    checked = finding.get("absence_checked")
    if isinstance(checked, list):
        return any(str(x).strip() for x in checked)
    if isinstance(checked, str):
        return bool(checked.strip())
    return False


def enforce_absence_guard(findings: list[Any]) -> dict:
    """Понизить непроверенные замечания-об-отсутствии до «ПРОВЕРИТЬ ПО СМЕЖНЫМ».

    Мутирует записи списка на месте. Возвращает статистику для лога.
    Не удаляет ничего (инвариант «не reject»).
    """
    scanned = 0
    absence_claims = 0
    downgraded = 0

    for f in findings:
        if not isinstance(f, dict):
            continue
        scanned += 1
        if not _is_absence_claim(f):
            continue
        absence_claims += 1

        severity = str(f.get("severity") or "").strip()
        if severity == _VERIFY_SEVERITY:
            continue  # уже мягкое — трогать нечего
        if _has_absence_evidence(f):
            continue  # модель проверила и назвала места — доверяем

        # Непроверенное утверждение об отсутствии → понижаем (не удаляем).
        f["absence_guard_downgraded"] = True
        f["absence_guard_original_severity"] = severity
        f["severity"] = _VERIFY_SEVERITY
        downgraded += 1

    return {
        "scanned": scanned,
        "absence_claims": absence_claims,
        "downgraded": downgraded,
    }
