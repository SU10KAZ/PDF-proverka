"""Воспроизводимое сравнение исходного и предлагаемого вариантов раздела."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any


ALTERNATIVE_EVALUATION_VERSION = 1


def _decimal(value: Any) -> Decimal | None:
    text = str(value or "").strip().replace("\u00a0", "").replace(" ", "").replace(",", ".")
    if not text:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _number(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def _current_metrics(targets: list[dict]) -> tuple[dict, list[str]]:
    rows = [row for target in targets for row in (target.get("rows") or [])]
    quantities: dict[str, Decimal] = {}
    unknown_quantity_rows: list[str] = []
    total_mass = Decimal("0")
    mass_complete = bool(rows)

    for row in rows:
        row_id = str(row.get("row_id") or "unknown")
        quantity = _decimal(row.get("quantity"))
        unit = str(row.get("unit") or "").strip()
        if quantity is None or not unit:
            unknown_quantity_rows.append(row_id)
        else:
            quantities[unit] = quantities.get(unit, Decimal("0")) + quantity

        row_total_mass = _decimal(row.get("total_mass"))
        if row_total_mass is None:
            unit_mass = _decimal(row.get("mass"))
            if unit_mass is not None and quantity is not None:
                row_total_mass = unit_mass * quantity
        if row_total_mass is None:
            mass_complete = False
        else:
            total_mass += row_total_mass

    type_marks = {
        str(row.get("type_mark") or row.get("designation") or "").strip()
        for row in rows
        if str(row.get("type_mark") or row.get("designation") or "").strip()
    }
    metrics = {
        "row_count": len(rows),
        "type_mark_count": len(type_marks),
        "quantity_by_unit": {unit: _number(value) for unit, value in sorted(quantities.items())},
        "total_mass_kg": _number(total_mass) if mass_complete else None,
    }
    return metrics, unknown_quantity_rows


def build_alternative_evaluation(
    candidate: dict,
    source_decisions: list[dict],
    targets: list[dict],
) -> dict:
    """Build a conservative comparison without inventing proposal inputs or prices."""
    current_metrics, unknown_quantity_rows = _current_metrics(targets)
    claimed_savings = sorted({
        float(value)
        for source in source_decisions
        if (value := _decimal(source.get("savings_pct"))) is not None
    })
    missing_inputs = [
        "Количественные показатели предлагаемого варианта",
        "Трудозатраты и число монтажных операций для обоих вариантов",
        "Цена, дата, валюта и состав затрат для денежной оценки",
    ]
    if unknown_quantity_rows:
        missing_inputs.insert(0, "Единица и числовое количество для всех исходных строк")
    if current_metrics["total_mass_kg"] is None:
        missing_inputs.append("Полная исходная масса или масса единицы и количество")

    return {
        "evaluation_version": ALTERNATIVE_EVALUATION_VERSION,
        "status": "requires_inputs",
        "baseline": {
            "alternative_id": "current",
            "title": "Сохранить текущее решение",
            "description": "; ".join(
                str(source.get("current") or "").strip()
                for source in source_decisions
                if str(source.get("current") or "").strip()
            ),
            "metrics": current_metrics,
        },
        "proposal": {
            "alternative_id": "proposal",
            "title": str(candidate.get("representative_proposal") or candidate.get("title") or "Предлагаемое решение"),
            "metrics": {
                "row_count": None,
                "type_mark_count": None,
                "quantity_by_unit": None,
                "total_mass_kg": None,
            },
            "claimed_savings_percent": claimed_savings,
        },
        "effect": {
            "natural": {
                "type_mark_delta": None,
                "mass_delta_kg": None,
                "operation_delta": None,
            },
            "monetary": None,
            "classification": "potential",
        },
        "missing_inputs": missing_inputs,
        "calculation_notes": [
            "Исходные количества суммируются раздельно по единицам измерения.",
            "Общая масса считается только при полноте исходных данных.",
            "Заявленная экономия источника не считается подтверждённым эффектом цели.",
            "Неизвестные цены и затраты не заменяются нулём.",
        ],
        "unknown_quantity_row_ids": unknown_quantity_rows,
    }


__all__ = ["ALTERNATIVE_EVALUATION_VERSION", "build_alternative_evaluation"]
