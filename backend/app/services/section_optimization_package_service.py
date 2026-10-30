"""Пакетное представление кандидата оптимизации для решения инженера.

Кандидат остаётся доказательным сигналом и не применяется автоматически. Этот
слой только собирает его в один пакет: что меняем, какой объём затронут,
какие проекты и строки входят в область, и почему денежный эффект пока нельзя
считать подтверждённым.
"""
from __future__ import annotations

from copy import deepcopy
from decimal import Decimal, InvalidOperation
import hashlib
import re
import unicodedata
from typing import Any


PACKAGE_DECISIONS = {"economic_yes", "economic_no", "needs_data"}

_APPLICABLE_VERDICTS = {"applicable", "applicable_with_conditions"}
_EXCLUDED_VERDICTS = {"reject"}
_UNRESOLVED_VERDICTS = {"needs_data", "needs_graphics", ""}
_VERDICT_PRIORITY = {
    "applicable": 1,
    "applicable_with_conditions": 2,
    "reject": 3,
    "needs_graphics": 4,
    "needs_data": 5,
    "": 5,
}


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


def _assessment_verdict(assessment: dict) -> str:
    """Return the final verdict used to partition package rows.

    A graphics review may refine the text agent's verdict.  Older dossiers do
    not have ``resolved_verdict``; in that case the original assessment is the
    only available evidence and is used unchanged.
    """
    return _text(assessment.get("resolved_verdict") or assessment.get("verdict"))


def _scope_partition(rows: list[dict], assessments: list[dict]) -> dict:
    """Split the section-wide package into applicable, excluded and pending rows.

    The split is intentionally conservative.  A row is included in the
    applicable volume only when an assessment explicitly cites its row id and
    has an applicable verdict.  Uncited rows are therefore visible as
    ``unresolved`` instead of silently being counted as savings.
    """
    rows_by_id = {
        _text(row.get("row_id")): row
        for row in rows
        if _text(row.get("row_id"))
    }
    row_status: dict[str, str] = {}
    for assessment in assessments:
        verdict = _assessment_verdict(assessment)
        for row_id in assessment.get("target_row_ids") or []:
            key = _text(row_id)
            if key not in rows_by_id:
                continue
            current = row_status.get(key)
            if current is None or _VERDICT_PRIORITY.get(verdict, 5) > _VERDICT_PRIORITY.get(current, 5):
                row_status[key] = verdict

    applicable: list[dict] = []
    excluded: list[dict] = []
    unresolved: list[dict] = []
    for row in rows:
        row_id = _text(row.get("row_id"))
        verdict = row_status.get(row_id, "")
        if verdict in _APPLICABLE_VERDICTS:
            applicable.append(row)
        elif verdict in _EXCLUDED_VERDICTS:
            excluded.append(row)
        else:
            unresolved.append(row)

    exceptions: list[dict] = []
    seen_projects: set[str] = set()
    for assessment in assessments:
        verdict = _assessment_verdict(assessment)
        if verdict in _APPLICABLE_VERDICTS and not assessment.get("conditions"):
            continue
        project_id = _text(assessment.get("project_id")) or "unknown"
        exception = {
            "project_id": project_id,
            "project_name": _text(assessment.get("project_name")) or project_id,
            "version_id": _text(assessment.get("version_id")),
            "verdict": verdict or "needs_data",
            "row_ids": sorted({_text(value) for value in assessment.get("target_row_ids") or [] if _text(value)}),
            "reason": _text(assessment.get("reason")),
            "conditions": [
                _text(value)[:1500]
                for value in (assessment.get("conditions") or [])
                if _text(value)
            ],
            "missing_data": [
                _text(value)[:1500]
                for value in (assessment.get("missing_data") or [])
                if _text(value)
            ],
            "graphics_required": bool(assessment.get("graphics_required")),
        }
        # Keep one clear item per project/verdict; duplicate agent records do
        # not make the package look more uncertain than the evidence warrants.
        dedupe_key = f"{project_id}|{verdict}|{','.join(exception['row_ids'])}"
        if dedupe_key not in seen_projects:
            exceptions.append(exception)
            seen_projects.add(dedupe_key)

    return {
        "all": _row_summary(rows),
        "applicable": _row_summary(applicable),
        "excluded": _row_summary(excluded),
        "unresolved": _row_summary(unresolved),
        "coverage": {
            "total_rows": len(rows),
            "assessed_rows": len(applicable) + len(excluded),
            "applicable_rows": len(applicable),
            "excluded_rows": len(excluded),
            "unresolved_rows": len(unresolved),
            "uncovered_row_ids": sorted({
                _text(row.get("row_id"))
                for row in unresolved
                if _text(row.get("row_id"))
            }),
        },
        "exceptions": exceptions,
    }


def _unique_text(values: list[Any]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _decision_gate(summary: dict) -> dict:
    """Expose one package-level gate instead of requiring row decisions.

    The API still validates the same rules when saving a verdict.  This field
    is the explanation shown before the engineer clicks a single package
    decision button.
    """
    technical = summary.get("technical_review") or {}
    status = _text(technical.get("status")) or "not_checked"
    economic = _text(summary.get("economic_status")) or "needs_data"
    coverage = (summary.get("applicability") or {}).get("coverage") or {}
    reasons: list[str] = []
    if economic != "positive":
        reasons.append("Итоговая денежная дельта ещё не подтверждена.")
    if status not in {"pass", "pass_with_open_interfaces", "pass_with_exceptions"}:
        reasons.append(_text(technical.get("message")) or "Техническая применимость не подтверждена.")
    if coverage.get("unresolved_rows"):
        reasons.append(f"Не подтверждено строк: {coverage['unresolved_rows']}.")
    requires_note = bool(
        coverage.get("excluded_rows")
        or technical.get("conditions")
        or status == "pass_with_open_interfaces"
    )
    positive_allowed = (
        economic == "positive"
        and status in {"pass", "pass_with_open_interfaces", "pass_with_exceptions"}
        and not coverage.get("unresolved_rows")
        and bool((summary.get("applicability") or {}).get("applicable", {}).get("row_count"))
    )
    negative_allowed = economic == "negative" or status in {"exceptions", "pass_with_exceptions", "blocked"}
    return {
        "economic_yes": {
            "allowed": positive_allowed,
            "requires_note": requires_note,
            "reasons": [] if positive_allowed else _unique_text(reasons),
        },
        "economic_no": {
            "allowed": negative_allowed,
            "requires_note": True,
            "reasons": [] if negative_allowed else _unique_text(reasons),
        },
        "needs_data": {
            "allowed": True,
            "requires_note": True,
            "reasons": _unique_text(reasons),
        },
    }


def _build_section_package_portfolio(snapshot: dict) -> list[dict]:
    """Group overlapping candidates without merging different technical tasks.

    A row may be found by several lexical/accepted-decision signals.  Showing
    the overlap lets the UI warn that quantities must not be summed twice.  We
    only connect signals of the same kind; a type-size study and a brand
    substitution remain separate engineering packages even when they cite the
    same specification row.
    """
    signals = [item for item in snapshot.get("signals") or [] if item.get("signal_id")]
    parent = {str(item["signal_id"]): str(item["signal_id"]) for item in signals}

    def find(value: str) -> str:
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    by_kind_rows: dict[tuple[str, str], str] = {}
    for signal in signals:
        signal_id = str(signal["signal_id"])
        kind = _text(signal.get("kind"))
        for row_id in signal.get("target_row_ids") or []:
            key = (kind, _text(row_id))
            previous = by_kind_rows.get(key)
            if previous:
                left, right = find(signal_id), find(previous)
                if left != right:
                    parent[right] = left
            else:
                by_kind_rows[key] = signal_id

    rows_by_id = {
        _text(row.get("row_id")): row
        for row in snapshot.get("specification_rows") or []
        if _text(row.get("row_id"))
    }
    groups: dict[str, list[dict]] = {}
    for signal in signals:
        root = find(str(signal["signal_id"]))
        groups.setdefault(root, []).append(signal)
    portfolio: list[dict] = []
    for group in groups.values():
        row_ids = sorted({
            _text(row_id)
            for signal in group
            for row_id in (signal.get("target_row_ids") or [])
            if _text(row_id)
        })
        rows = [rows_by_id[row_id] for row_id in row_ids if row_id in rows_by_id]
        project_ids = sorted({
            _text(row.get("project_id"))
            for row in rows
            if _text(row.get("project_id"))
        })
        portfolio.append({
            "portfolio_id": "PORT-" + hashlib.sha1(
                "|".join(sorted(str(item["signal_id"]) for item in group)).encode("utf-8")
            ).hexdigest()[:12],
            "kind": _text(group[0].get("kind")),
            "signal_ids": sorted(str(item["signal_id"]) for item in group),
            "row_ids": row_ids,
            "project_ids": project_ids,
            "row_count": len(rows),
            "project_count": len(project_ids),
            "overlap": len(group) > 1,
            "warning": (
                "Сигналы пересекаются по строкам; объём считать один раз внутри этого пакета."
                if len(group) > 1 else ""
            ),
        })
    return sorted(portfolio, key=lambda item: (-item["row_count"], item["portfolio_id"]))


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
    applicability = _scope_partition(rows, [])
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
        "source_scope": None,
        "section_scope": scope,
        "applicability": applicability,
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
        "decision_gate": {
            "economic_yes": {
                "allowed": False,
                "requires_note": False,
                "reasons": ["Сначала нужна адресная проверка применимости по целевым строкам."],
            },
            "economic_no": {
                "allowed": False,
                "requires_note": True,
                "reasons": ["Сначала нужна адресная проверка применимости по целевым строкам."],
            },
            "needs_data": {
                "allowed": True,
                "requires_note": True,
                "reasons": list(blockers),
            },
        },
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
            combined = {
                _text(row.get("row_id")): row
                for row in [*source_rows, *target_rows]
                if _text(row.get("row_id"))
            }
            summary["section_scope"] = _row_summary(list(combined.values()))
        signal["package_summary"] = summary
    meta = result.setdefault("meta", {})
    meta["package_candidates"] = len(result.get("signals") or [])
    meta["package_decision_model"] = "economic_yes|economic_no|needs_data"
    meta["package_portfolio"] = _build_section_package_portfolio(result)
    return result


def package_summary_from_dossier(job: dict) -> dict:
    """Build the same summary for a stored legacy dossier."""
    dossier = job.get("dossier") or {}
    candidate = dict(dossier.get("candidate") or {})
    rows = [row for target in dossier.get("targets") or [] for row in target.get("rows") or []]
    summary = build_package_summary(candidate, rows, dossier.get("source_decisions") or [])
    source_rows = list(dossier.get("source_rows") or [])
    if source_rows:
        summary["source_scope"] = _row_summary(source_rows)
        combined = {
            _text(row.get("row_id")): row
            for row in [*source_rows, *rows]
            if _text(row.get("row_id"))
        }
        summary["section_scope"] = _row_summary(list(combined.values()))
        summary["applicability"]["source_accepted"] = summary["source_scope"]
        summary["applicability"]["coverage"]["source_rows"] = summary["source_scope"].get("row_count", 0)
        summary["applicability"]["coverage"]["section_total_rows"] = summary["section_scope"].get("row_count", 0)
    target_project_ids = [str(value) for value in (job.get("target_project_ids") or []) if value]
    known_scope_projects = [
        item for item in summary["scope"].get("projects") or []
        if _text(item.get("project_id")) and _text(item.get("project_id")) != "unknown"
    ]
    if target_project_ids and not known_scope_projects:
        summary["scope"]["project_count"] = len(target_project_ids)
        summary["scope"]["projects"] = [
            {"project_id": project_id, "project_name": project_id, "version_id": "", "row_count": 0}
            for project_id in target_project_ids
        ]
        if not dossier.get("source_rows"):
            summary["section_scope"] = summary["scope"]
    assessments = list(job.get("agent_assessments") or [])
    open_requests = [item for item in (job.get("data_requests") or []) if item.get("status") == "open"]
    dependency = job.get("dependency_assessment") or dossier.get("dependency_assessment") or {}
    dependency_unresolved = dependency.get("status") not in {"resolved", "not_required"}
    applicability = _scope_partition(rows, assessments)
    summary["applicability"] = applicability
    missing_project_ids = sorted(
        set(target_project_ids)
        - {
            _text(item.get("project_id"))
            for item in summary["scope"].get("projects") or []
            if _text(item.get("project_id")) and _text(item.get("project_id")) != "unknown"
        }
    )
    summary["scope"]["target_project_count"] = len(target_project_ids)
    summary["scope"]["missing_project_ids"] = missing_project_ids
    exceptions = list(applicability.get("exceptions") or [])
    unresolved_assessments = [
        item for item in assessments
        if _assessment_verdict(item) in _UNRESOLVED_VERDICTS
    ]
    rejected_assessments = [
        item for item in assessments
        if _assessment_verdict(item) in _EXCLUDED_VERDICTS
    ]
    conditional_assessments = [
        item for item in assessments
        if _assessment_verdict(item) == "applicable_with_conditions"
    ]
    conditions = _unique_text([
        condition
        for item in conditional_assessments
        for condition in (item.get("conditions") or [])
    ])
    technical_blockers: list[str] = []
    if open_requests:
        technical_blockers.append(f"Открытых запросов данных: {len(open_requests)}.")
    if (job.get("critic") or {}).get("status") == "blocked":
        technical_blockers.append("Critic оставил блокирующие вопросы по доказательствам.")
    if job.get("graphics_required") and job.get("graphics_status") not in {"complete", "not_required", "skipped"}:
        technical_blockers.append("Обязательная графическая проверка ещё не завершена.")
    if unresolved_assessments:
        technical_blockers.append(
            f"Не закрыта применимость по {len(unresolved_assessments)} проект(а/ам)."
        )
    if missing_project_ids:
        technical_blockers.append(
            "В выбранной области нет строк для проектов: " + ", ".join(missing_project_ids) + "."
        )
    if not assessments:
        technical = {
            "status": "not_checked",
            "message": "Досье умного агента ещё не подготовлено.",
            "exceptions": [],
            "conditions": [],
            "blockers": [],
            "dependency_status": dependency.get("status") or "not_checked",
        }
    elif (
        technical_blockers
    ):
        technical = {
            "status": "blocked",
            "message": "Есть незакрытые технические вопросы; сначала разберите их.",
            "exceptions": exceptions,
            "conditions": conditions,
            "blockers": _unique_text(technical_blockers),
            "dependency_status": dependency.get("status") or "not_checked",
        }
    elif rejected_assessments:
        technical = {
            "status": "pass_with_exceptions",
            "message": "Решение технически применимо к подтверждённой части объёма; исключения перечислены отдельно.",
            "exceptions": exceptions,
            "conditions": conditions,
            "blockers": [],
            "dependency_status": dependency.get("status") or "not_checked",
        }
    else:
        dependency_status = dependency.get("status") or "not_checked"
        technical = {
            "status": "pass_with_open_interfaces" if dependency_unresolved else "pass",
            "message": (
                "Техническая применимость подтверждена; карту инженерных интерфейсов приложите к пакетному решению."
                if dependency_unresolved
                else "Техническая применимость подтверждена по текущему досье."
            ),
            "exceptions": exceptions,
            "conditions": conditions,
            "blockers": [],
            "dependency_status": dependency_status,
        }
    summary["technical_review"] = technical
    summary["technical_blockers"] = _unique_text(technical_blockers)
    summary["exceptions"] = exceptions
    summary["blockers"] = _unique_text((summary.get("blockers") or []) + technical_blockers)
    evaluation = job.get("alternative_evaluation") or dossier.get("alternative_evaluation") or {}
    monetary = evaluation.get("effect", {}).get("monetary") or {}
    delta = _decimal(monetary.get("delta"))
    if delta is not None:
        delta_range = monetary.get("delta_range") or {}
        worst_delta = _decimal(delta_range.get("worst"))
        range_uncertain = worst_delta is not None and worst_delta >= 0
        summary["economic_status"] = "needs_data" if range_uncertain else ("positive" if delta < 0 else "negative")
        summary["effect"].update({
            "status": "uncertain" if range_uncertain else "calculated",
            "baseline_cost": monetary.get("baseline"),
            "proposal_cost": monetary.get("proposal"),
            "delta": monetary.get("delta"),
            "delta_range": delta_range,
            "currency": _text(monetary.get("currency")),
            "source": _text(monetary.get("source")),
        })
        summary["blockers"] = [
            item for item in summary["blockers"]
            if "цен" not in item.lower() and "коммерчес" not in item.lower()
        ]
        if range_uncertain:
            summary["blockers"].append(
                "Диапазон неопределённости пересекает нулевую экономию; нужен более точный расчёт."
            )
    applicable_scope = (summary.get("applicability") or {}).get("applicable") or {}
    coverage = (summary.get("applicability") or {}).get("coverage") or {}
    economic_blockers: list[str] = []
    if applicable_scope.get("unknown_quantity_row_ids"):
        economic_blockers.append(
            "В применимом объёме есть строки без количества или единицы измерения."
        )
    if not monetary:
        economic_blockers.append("Нет сопоставимой денежной оценки по всему применимому объёму.")
    if coverage.get("unresolved_rows"):
        economic_blockers.append(
            f"В расчёт нельзя включить неподтверждённые строки: {coverage['unresolved_rows']}."
        )
    if not applicable_scope.get("row_count") and summary["scope"].get("row_count"):
        economic_blockers.append("Нет подтверждённого применимого объёма для расчёта.")
    summary["economic_blockers"] = _unique_text(economic_blockers)
    summary["blockers"] = _unique_text(
        (summary.get("economic_blockers") or []) + (summary.get("technical_blockers") or [])
    )
    summary["decision_gate"] = _decision_gate(summary)
    saved_decision = ((job.get("package_decision") or {}).get("decision"))
    if saved_decision:
        summary["package_verdict"] = {
            "status": saved_decision,
            "label": {
                "economic_yes": "Экономически обосновано",
                "economic_no": "Экономически не обосновано",
                "needs_data": "Нужны данные",
            }.get(saved_decision, saved_decision),
            "reason": _text((job.get("package_decision") or {}).get("note")),
        }
    elif summary["decision_gate"]["economic_yes"]["allowed"]:
        summary["package_verdict"] = {
            "status": "ready",
            "label": "Можно принять пакетное решение",
            "reason": "Доказательства и денежная оценка собраны; инженер выбирает один вердикт.",
        }
    else:
        summary["package_verdict"] = {
            "status": "needs_data",
            "label": "Нужны данные",
            "reason": (summary["blockers"] or ["Пакет ещё не готов к решению."])[0],
        }
    summary["decision"] = ((job.get("package_decision") or {}).get("decision"))
    summary["decision_note"] = ((job.get("package_decision") or {}).get("note") or "")
    return summary


__all__ = [
    "PACKAGE_DECISIONS",
    "attach_package_summaries",
    "build_package_summary",
    "_build_section_package_portfolio",
    "package_summary_from_dossier",
]
