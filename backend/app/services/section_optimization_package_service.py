"""Пакетное представление кандидата оптимизации для решения инженера.

Кандидат остаётся доказательным сигналом и не применяется автоматически. Этот
слой только собирает его в один пакет: что меняем, какой объём затронут,
какие проекты и строки входят в область, и почему денежный эффект пока нельзя
считать подтверждённым.
"""
from __future__ import annotations

from copy import deepcopy
from decimal import Decimal, InvalidOperation
import re
import unicodedata
from typing import Any


PACKAGE_DECISIONS = {"economic_yes", "economic_no", "needs_data"}


def _text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _decimal(value: Any) -> Decimal | None:
    text = _text(value).replace("\u00a0", "").replace(" ", "").replace(",", ".")
    if not text:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _number(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def _unit(value: Any) -> str:
    text = _text(value).lower().replace(".", "")
    compact = text.replace(" ", "")
    if compact in {"погм", "погонныйметр", "погонныхметров", "м", "метр", "метров"}:
        return "м"
    if compact in {"шт", "штук", "штука"}:
        return "шт"
    if compact in {"компл", "комплект", "комплектов"}:
        return "компл"
    return text or "единица неизвестна"


def _normalized_group_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", _text(value)).lower()
    # Варианты x/х, A/А и M/М не должны создавать ложные технические группы.
    return text.translate(str.maketrans({"х": "x", "×": "x", "а": "a", "м": "m"}))


def _technical_fingerprint(row: dict) -> dict:
    text = " ".join(
        _normalized_group_text(row.get(field))
        for field in ("name", "designation", "type_mark", "code", "note")
    )
    dimensions = sorted(set(re.findall(r"\d+(?:[.,]\d+)?\s*x\s*\d+(?:[.,]\d+)?(?:\s*x\s*\d+(?:[.,]\d+)?)?", text)))
    fire_classes = sorted(set(re.findall(r"\b(?:frhf|frls|hf|нг(?:\(a\)|a)?|ls)\b", text)))
    ip_values = sorted(set(re.findall(r"\bip\s*\d{2}\b", text)))
    voltages = sorted(set(re.findall(r"\b\d+(?:[.,]\d+)?\s*(?:v|в|kv|кв)\b", text)))
    cross_sections = sorted(set(re.findall(r"\b\d+(?:[.,]\d+)?\s*(?:mm2|мм2|мм²|мm²)\b", text)))
    diameters = sorted(set(re.findall(r"(?:ø|d\s*)\s*\d+(?:[.,]\d+)?", text)))
    return {
        "dimensions": dimensions,
        "fire_class": fire_classes,
        "ip": ip_values,
        "voltage": voltages,
        "cross_section": cross_sections,
        "diameter": diameters,
        "manufacturer": _normalized_group_text(row.get("manufacturer")),
        "unit": _unit(row.get("unit")),
    }


def _material_groups(rows: list[dict]) -> list[dict]:
    groups: dict[str, dict[str, Any]] = {}
    for row in rows:
        fingerprint = _technical_fingerprint(row)
        base = _normalized_group_text(
            row.get("canonical_key")
            or row.get("designation")
            or row.get("type_mark")
            or row.get("name")
            or "без обозначения"
        )
        key = "|".join((base, fingerprint["unit"], fingerprint["manufacturer"], repr(fingerprint)))
        group = groups.setdefault(key, {
            "group_key": key,
            "label": _text(row.get("type_mark") or row.get("designation") or row.get("name")) or "Без обозначения",
            "row_ids": [],
            "projects": {},
            "quantity_by_unit": {},
            "unknown_quantity_row_ids": [],
            "technical_fingerprint": fingerprint,
        })
        row_id = _text(row.get("row_id")) or "unknown"
        group["row_ids"].append(row_id)
        project_id = _text(row.get("project_id")) or "unknown"
        group["projects"][project_id] = _text(row.get("project_name")) or project_id
        quantity = _decimal(row.get("quantity"))
        raw_unit = _text(row.get("unit"))
        if quantity is None or not raw_unit:
            group["unknown_quantity_row_ids"].append(row_id)
        else:
            unit = _unit(raw_unit)
            group["quantity_by_unit"][unit] = _number(
                Decimal(str(group["quantity_by_unit"].get(unit, 0))) + quantity
            )
    result = []
    for group in groups.values():
        group["row_ids"] = sorted(set(group["row_ids"]))
        group["unknown_quantity_row_ids"] = sorted(set(group["unknown_quantity_row_ids"]))
        group["project_count"] = len(group.pop("projects"))
        group["quantity_by_unit"] = dict(sorted(group["quantity_by_unit"].items()))
        result.append(group)
    return sorted(result, key=lambda item: (-len(item["row_ids"]), item["label"]))


def _row_summary(rows: list[dict]) -> dict:
    quantities: dict[str, Decimal] = {}
    unknown_quantity_rows: list[str] = []
    projects: dict[str, dict[str, Any]] = {}
    variants: set[str] = set()
    row_ids: list[str] = []
    for row in rows:
        project_id = _text(row.get("project_id")) or "unknown"
        project = projects.setdefault(project_id, {
            "project_id": project_id,
            "project_name": _text(row.get("project_name")) or project_id,
            "version_id": _text(row.get("version_id")),
            "row_count": 0,
        })
        project["row_count"] += 1
        row_id = _text(row.get("row_id")) or "unknown"
        row_ids.append(row_id)
        quantity = _decimal(row.get("quantity"))
        raw_unit = _text(row.get("unit"))
        unit = _unit(raw_unit)
        if quantity is None or not raw_unit:
            unknown_quantity_rows.append(row_id)
        else:
            quantities[unit] = quantities.get(unit, Decimal("0")) + quantity
        variant = _text(row.get("type_mark") or row.get("designation") or row.get("name"))
        if variant:
            variants.add(variant)
    material_groups = _material_groups(rows)
    return {
        "row_count": len(rows),
        "row_ids": sorted(set(row_ids)),
        "project_count": len(projects),
        "projects": sorted(projects.values(), key=lambda item: item["project_id"]),
        "quantity_by_unit": {
            unit: _number(value) for unit, value in sorted(quantities.items())
        },
        "unknown_quantity_row_ids": sorted(set(unknown_quantity_rows)),
        "variant_count": len(variants),
        "variants": sorted(variants),
        "material_groups": material_groups,
        "material_group_count": len(material_groups),
    }


def _decision_plan(signal: dict, scope: dict, source_decisions: list[dict]) -> dict:
    """Explain the engineering work in the order it must be done."""
    kind = _text(signal.get("kind"))
    if kind == "type_size_reduction_opportunity":
        question = "Можно ли оставить один физический вариант без потери обязательных параметров?"
        steps = [
            "Отделить различия в написании от реальных различий размера, нагрузки и исполнения.",
            "Выбрать базовый типоразмер и перечислить варианты, которые останутся исключениями.",
            "Проверить нагрузку, пожарное исполнение, IP, условия среды и монтажные габариты.",
            "Сложить объём всех текущих вариантов и запросить цену единого варианта и переходных работ.",
        ]
    else:
        question = "Можно ли применить принятое решение ко всему найденному объёму раздела?"
        steps = [
            "Зафиксировать, что именно меняется: материал, марка, производитель или способ монтажа.",
            "Проверить по каждой группе пожарное исполнение, IP, сечение/размер, нагрузку и условия монтажа.",
            "Отдельно отметить проектные исключения; их не смешивать с общим объёмом.",
            "Сложить текущий объём по единицам измерения и сравнить цену текущего и предлагаемого варианта.",
        ]
    return {
        "question": question,
        "steps": steps,
        "required_inputs": [
            "Текущий объём по актуальным спецификациям и единицам измерения",
            "Предлагаемый стандарт/марка и список технических исключений",
            "Цена текущего и предлагаемого варианта в одной валюте и на одну базу",
            "Затраты на замену, монтаж, переделку и эксплуатацию",
        ],
        "decision_rule": {
            "economic_yes": "технические гейты закрыты, расчёт проверяемый, итоговая дельта меньше нуля",
            "economic_no": "проверка завершена, безопасного или выгодного общего варианта нет",
            "needs_data": "не хватает цены, объёма, технического подтверждения или графики",
        },
        "source_decisions": len(source_decisions),
        "scope_ready": bool(scope.get("row_count")),
    }


def _source_reference(source_decisions: list[dict]) -> dict | None:
    if not source_decisions:
        return None
    first = source_decisions[0]
    refs = sorted({
        _text(item.get("id") or item.get("source_ref"))
        for item in source_decisions
        if _text(item.get("id") or item.get("source_ref"))
    })
    projects = sorted({
        _text(item.get("project_name") or item.get("project_id"))
        for item in source_decisions
        if _text(item.get("project_name") or item.get("project_id"))
    })
    return {
        "refs": refs,
        "projects": projects,
        "version_ids": sorted({
            _text(item.get("version_id"))
            for item in source_decisions
            if _text(item.get("version_id"))
        }),
        "current": _text(first.get("current")),
        "proposed": _text(first.get("proposed")),
    }


def build_package_summary(
    signal: dict,
    rows: list[dict],
    source_decisions: list[dict] | None = None,
) -> dict:
    """Build a deterministic, section-wide summary for one candidate signal."""
    source_decisions = list(source_decisions or [])
    scope = _row_summary(rows)
    claimed = sorted({
        float(value)
        for item in source_decisions
        for value in [item.get("savings_pct")]
        if _decimal(value) is not None
    })
    blockers: list[str] = []
    if scope["unknown_quantity_row_ids"]:
        blockers.append("Не у всех строк указаны числовые количества и единицы измерения.")
    if not source_decisions:
        blockers.append("Для новой гипотезы нужно задать единый предлагаемый стандарт.")
    else:
        blockers.append("Нет сопоставимых цен или коммерческих предложений для всего объёма.")
    if signal.get("kind") == "type_size_reduction_opportunity":
        blockers.append("Для сокращения типоразмеров не задан единый предлагаемый стандарт.")
    if signal.get("graphics_recommended"):
        blockers.append("Техническую применимость нужно подтвердить по связанным листам.")
    economic_blockers = list(blockers[:1])
    if source_decisions:
        economic_blockers.append("Нет сопоставимых цен или коммерческих предложений для всего объёма.")
    elif signal.get("kind") == "type_size_reduction_opportunity":
        economic_blockers.append("Для новой гипотезы не задан единый предлагаемый стандарт.")
    technical_blockers = []
    if signal.get("graphics_recommended"):
        technical_blockers.append("Техническую применимость нужно подтвердить по связанным листам.")
    return {
        "package_version": 1,
        "action": _text(signal.get("representative_proposal") or signal.get("title")),
        "kind": _text(signal.get("kind")),
        "source_basis": _text(signal.get("match_basis") or signal.get("reason")),
        "source_reference": _source_reference(source_decisions),
        "scope": scope,
        "decision_plan": _decision_plan(signal, scope, source_decisions),
        "technical_review": {
            "status": "not_checked",
            "message": "Досье умного агента ещё не подготовлено.",
            "exceptions": [],
        },
        "claimed_savings_percent": claimed,
        "economic_status": "needs_data",
        "effect": {
            "status": "potential" if claimed else "unknown",
            "claimed_savings_percent": claimed,
            "baseline_cost": None,
            "proposal_cost": None,
            "delta": None,
            "currency": "",
            "source": "",
        },
        "blockers": blockers,
        "economic_blockers": economic_blockers,
        "technical_blockers": technical_blockers,
        "decision": None,
        "decision_note": "",
        "exceptions": [],
    }


def attach_package_summaries(snapshot: dict) -> dict:
    """Return a copy of a snapshot enriched with package summaries."""
    result = deepcopy(snapshot)
    rows_by_id = {
        _text(row.get("row_id")): row
        for row in result.get("specification_rows") or []
        if _text(row.get("row_id"))
    }
    accepted_by_ref = {
        _text(item.get("source_ref")): item
        for item in result.get("accepted_optimizations") or []
        if _text(item.get("source_ref"))
    }
    for signal in result.get("signals") or []:
        target_rows = [
            rows_by_id[row_id]
            for row_id in (_text(value) for value in signal.get("target_row_ids") or [])
            if row_id in rows_by_id
        ]
        source_rows = [
            rows_by_id[row_id]
            for row_id in (_text(value) for value in signal.get("source_row_ids") or [])
            if row_id in rows_by_id
        ]
        sources = [
            accepted_by_ref[source_ref]
            for source_ref in (_text(value) for value in signal.get("evidence_refs") or [])
            if source_ref in accepted_by_ref
        ]
        summary = build_package_summary(signal, target_rows, sources)
        if source_rows:
            summary["source_scope"] = _row_summary(source_rows)
        signal["package_summary"] = summary
    meta = result.setdefault("meta", {})
    meta["package_candidates"] = len(result.get("signals") or [])
    meta["package_decision_model"] = "economic_yes|economic_no|needs_data"
    return result


def package_summary_from_dossier(job: dict) -> dict:
    """Build the same summary for a stored legacy dossier."""
    dossier = job.get("dossier") or {}
    candidate = dict(dossier.get("candidate") or {})
    rows = [row for target in dossier.get("targets") or [] for row in target.get("rows") or []]
    summary = build_package_summary(candidate, rows, dossier.get("source_decisions") or [])
    target_project_ids = [str(value) for value in (job.get("target_project_ids") or []) if value]
    if target_project_ids and (
        summary["scope"].get("project_count") == 1
        and (summary["scope"].get("projects") or [{}])[0].get("project_id") == "unknown"
    ):
        summary["scope"]["project_count"] = len(target_project_ids)
        summary["scope"]["projects"] = [
            {"project_id": project_id, "project_name": project_id, "version_id": "", "row_count": 0}
            for project_id in target_project_ids
        ]
    assessments = list(job.get("agent_assessments") or [])
    open_requests = [item for item in (job.get("data_requests") or []) if item.get("status") == "open"]
    exceptions = [
        item for item in assessments
        if str(item.get("resolved_verdict") or item.get("verdict") or "")
        not in {"applicable", "applicable_with_conditions"}
    ]
    if not assessments:
        technical = {"status": "not_checked", "message": "Досье умного агента ещё не подготовлено.", "exceptions": []}
    elif (
        open_requests
        or (job.get("critic") or {}).get("status") == "blocked"
        or (job.get("graphics_required") and job.get("graphics_status") not in {"complete", "not_required", "skipped"})
    ):
        technical = {
            "status": "blocked",
            "message": "Есть незакрытые технические вопросы; сначала разберите их.",
            "exceptions": exceptions,
        }
    elif exceptions:
        technical = {
            "status": "exceptions",
            "message": "Есть проекты, для которых общее решение не подтверждено.",
            "exceptions": exceptions,
        }
    else:
        technical = {
            "status": "pass",
            "message": "Техническая применимость подтверждена по текущему досье.",
            "exceptions": [],
        }
    summary["technical_review"] = technical
    evaluation = job.get("alternative_evaluation") or dossier.get("alternative_evaluation") or {}
    monetary = evaluation.get("effect", {}).get("monetary") or {}
    delta = _decimal(monetary.get("delta"))
    if delta is not None:
        summary["economic_status"] = "positive" if delta < 0 else "negative"
        summary["effect"].update({
            "status": "calculated",
            "baseline_cost": monetary.get("baseline"),
            "proposal_cost": monetary.get("proposal"),
            "delta": monetary.get("delta"),
            "currency": _text(monetary.get("currency")),
            "source": _text(monetary.get("source")),
        })
        summary["blockers"] = [
            item for item in summary["blockers"]
            if "цен" not in item.lower() and "коммерчес" not in item.lower()
        ]
    summary["decision"] = ((job.get("package_decision") or {}).get("decision"))
    summary["decision_note"] = ((job.get("package_decision") or {}).get("note") or "")
    return summary


__all__ = [
    "PACKAGE_DECISIONS",
    "attach_package_summaries",
    "build_package_summary",
    "package_summary_from_dossier",
]
