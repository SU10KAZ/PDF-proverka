"""Пакетное представление кандидата оптимизации для решения инженера.

Кандидат остаётся доказательным сигналом и не применяется автоматически. Этот
слой только собирает его в один пакет: что меняем, какой объём затронут,
какие проекты и строки входят в область, и почему денежный эффект пока нельзя
считать подтверждённым.
"""
from __future__ import annotations

from copy import deepcopy
from decimal import Decimal, InvalidOperation
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
        blockers.append("Нет принятого исходного решения с готовой ценой предлагаемого варианта.")
    else:
        blockers.append("Нет сопоставимых цен или коммерческих предложений для всего объёма.")
    if signal.get("kind") == "type_size_reduction_opportunity":
        blockers.append("Для сокращения типоразмеров не задан единый предлагаемый стандарт.")
    if signal.get("graphics_recommended"):
        blockers.append("Техническую применимость нужно подтвердить по связанным листам.")
    return {
        "package_version": 1,
        "action": _text(signal.get("representative_proposal") or signal.get("title")),
        "kind": _text(signal.get("kind")),
        "source_basis": _text(signal.get("match_basis") or signal.get("reason")),
        "scope": scope,
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
