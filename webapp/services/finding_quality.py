"""Deterministic quality heuristics for audit findings.

This layer does not replace the LLM judgement. It adds a practical usefulness
signal so the pipeline can:
1. keep truly engineering-critical findings prominent;
2. detect suspicious high-severity findings that look mostly formal;
3. measure how much "paper noise" is left in the output.
"""

from __future__ import annotations

import json
import re
from pathlib import Path


HIGH_SEVERITY = {
    "КРИТИЧЕСКОЕ",
    "ЭКОНОМИЧЕСКОЕ",
    "ЭКСПЛУАТАЦИОННОЕ",
}

_FORMAL_CATEGORIES = {
    "normative_refs",
    "reference_list",
    "references",
}

_IMPACT_KEYWORDS = {
    "safety": (
        "эвакуац",
        "пожар",
        "огнестойк",
        "обруш",
        "авар",
        "травм",
        "безопас",
        "дым",
        "взрыв",
    ),
    "cost": (
        "стоим",
        "бюджет",
        "перерасход",
        "удорож",
        "дороже",
        "эконом",
        "штраф",
    ),
    "schedule": (
        "срок",
        "задерж",
        "срыв",
        "приостан",
        "согласован",
        "перенос",
    ),
    "rework": (
        "передел",
        "доработ",
        "демонтаж",
        "повторн",
        "исправлен",
        "перемонт",
    ),
    "construction": (
        "монтаж",
        "закупк",
        "изготов",
        "подряд",
        "поставка",
        "заказ",
        "совместим",
    ),
    "operations": (
        "эксплуатац",
        "обслужив",
        "ремонт",
        "доступ",
        "сервис",
    ),
    "coordination": (
        "смежн",
        "координац",
        "вк",
        "овик",
        "эом",
        "подвод",
        "коллиз",
    ),
}

_IMPACT_WEIGHTS = {
    "safety": 28,
    "cost": 18,
    "schedule": 18,
    "rework": 16,
    "construction": 14,
    "operations": 12,
    "coordination": 12,
}

_TYPO_KEYWORDS = (
    "опечат",
    "описк",
    "ошибк в номере",
    "неверный номер",
    "некорректное наименование",
)

_FORMAL_NOTICE_KEYWORDS = (
    "формальн",
    "замечани экспертизы",
    "экспертизы",
)

_REFERENCE_LIST_KEYWORDS = (
    "ведомост",
    "ссылочн",
    "нормативн",
)

_REFERENCE_MAINTENANCE_KEYWORDS = (
    "не включ",
    "отсутствует",
    "ссылка на",
    "устарев",
    "несуществующ",
)

_EDITORIAL_SOLUTION_KEYWORDS = (
    "исправить опечат",
    "исправить номер",
    "исправить наименован",
    "исправить ссылк",
    "включить",
    "добавить в ведомост",
    "скорректировать ведомост",
)

_QUANTIFIED_RE = re.compile(
    r"(\b\d+(?:[.,]\d+)?\s*(?:мм|см|м|кн|кг|квт|%|шт|м2|м²)\b|[<>]=?\s*\d)",
    re.IGNORECASE,
)


def _stringify(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple, set)):
        return " ".join(_stringify(item) for item in value)
    if isinstance(value, dict):
        return " ".join(_stringify(v) for v in value.values())
    return str(value)


def _finding_text(finding: dict) -> str:
    return " ".join(
        _stringify(finding.get(field))
        for field in (
            "category",
            "problem",
            "description",
            "risk",
            "solution",
            "norm",
            "norm_quote",
        )
    ).lower()


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords)


def _append_flag(flags: list[str], flag: str):
    if flag not in flags:
        flags.append(flag)


def _looks_editorial_solution(solution_text: str) -> bool:
    if not solution_text:
        return False
    lowered = solution_text.lower()
    if "ведомост" in lowered or "ссылоч" in lowered or "номер документ" in lowered:
        return True
    return _contains_any(lowered, _EDITORIAL_SOLUTION_KEYWORDS)


def evaluate_finding_practicality(finding: dict) -> dict:
    """Compute a deterministic usefulness signal for one finding."""
    severity = str(finding.get("severity") or "").upper()
    category = str(finding.get("category") or "").lower()
    text = _finding_text(finding)
    risk_text = _stringify(finding.get("risk")).lower()
    solution_text = _stringify(finding.get("solution")).lower()

    impact_axes: list[str] = []
    formalism_flags: list[str] = []
    positive = 0
    negative = 0

    evidence = finding.get("evidence") or []
    related_blocks = finding.get("related_block_ids") or []
    if evidence:
        positive += 5
    if related_blocks:
        positive += 5
    if _QUANTIFIED_RE.search(_stringify(finding.get("problem")) + " " + _stringify(finding.get("description"))):
        positive += 10

    for axis, keywords in _IMPACT_KEYWORDS.items():
        if _contains_any(text, keywords):
            impact_axes.append(axis)
            positive += _IMPACT_WEIGHTS[axis]

    if category in _FORMAL_CATEGORIES:
        _append_flag(formalism_flags, "formal_reference_category")
        negative += 18

    if _contains_any(text, _TYPO_KEYWORDS):
        _append_flag(formalism_flags, "editorial_issue")
        negative += 10

    if _contains_any(text, _FORMAL_NOTICE_KEYWORDS):
        _append_flag(formalism_flags, "expertise_formality")
        negative += 10

    if _contains_any(text, _REFERENCE_LIST_KEYWORDS) and (
        "ведомост" in text or "ссылоч" in text
    ):
        _append_flag(formalism_flags, "reference_register")
        negative += 12

    if _contains_any(text, _REFERENCE_MAINTENANCE_KEYWORDS):
        _append_flag(formalism_flags, "reference_maintenance")
        negative += 8

    if _looks_editorial_solution(solution_text):
        _append_flag(formalism_flags, "editorial_fix")
        negative += 8

    if "минимальн" in risk_text and not impact_axes:
        _append_flag(formalism_flags, "minimal_declared_impact")
        negative += 8

    likely_formal_only = bool(formalism_flags) and not impact_axes and negative >= 18
    practicality_score = max(0, min(100, 35 + positive - negative))

    if practicality_score >= 70:
        engineering_relevance = "high"
    elif practicality_score >= 40:
        engineering_relevance = "medium"
    else:
        engineering_relevance = "low"

    severity_mismatch = severity in HIGH_SEVERITY and engineering_relevance == "low"

    if severity_mismatch or (severity in HIGH_SEVERITY and engineering_relevance == "high"):
        review_priority = "high"
    elif likely_formal_only:
        review_priority = "normal"
    else:
        review_priority = "low"

    return {
        "practicality_score": practicality_score,
        "engineering_relevance": engineering_relevance,
        "impact_axes": impact_axes,
        "formalism_flags": formalism_flags,
        "likely_formal_only": likely_formal_only,
        "severity_mismatch": severity_mismatch,
        "review_priority": review_priority,
    }


def should_review_practicality(finding: dict) -> bool:
    """Flag findings that deserve a second look for usefulness/severity fit."""
    quality = finding.get("quality")
    if not isinstance(quality, dict):
        quality = evaluate_finding_practicality(finding)

    severity = str(finding.get("severity") or "").upper()
    if quality.get("severity_mismatch"):
        return True
    return bool(quality.get("likely_formal_only") and severity in HIGH_SEVERITY)


def enrich_findings(findings: list[dict]) -> dict:
    """Enrich findings in-place and return compact summary stats."""
    stats = {
        "total": len(findings),
        "high_relevance": 0,
        "medium_relevance": 0,
        "low_relevance": 0,
        "likely_formal_only": 0,
        "high_severity_formal_only": 0,
    }

    for finding in findings:
        quality = evaluate_finding_practicality(finding)
        finding["quality"] = quality

        relevance = quality["engineering_relevance"]
        stats[f"{relevance}_relevance"] += 1
        if quality["likely_formal_only"]:
            stats["likely_formal_only"] += 1
            if str(finding.get("severity") or "").upper() in HIGH_SEVERITY:
                stats["high_severity_formal_only"] += 1

    return stats


def enrich_findings_payload(payload: dict) -> dict:
    findings = payload.get("findings")
    if findings is None:
        findings = payload.get("items", [])
    if not isinstance(findings, list):
        findings = []

    stats = enrich_findings(findings)
    meta = payload.setdefault("meta", {})
    meta["quality_summary"] = stats
    return stats


def enrich_findings_file(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    stats = enrich_findings_payload(payload)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return stats
