import pytest

from backend.app.services.section_optimization_alternative_service import (
    apply_alternative_inputs,
    build_alternative_evaluation,
)


def test_alternative_evaluation_sums_only_current_observable_metrics():
    result = build_alternative_evaluation(
        {"representative_proposal": "Применить типовой кронштейн"},
        [{"current": "Два типа крепления", "savings_pct": "12,5"}],
        [{"rows": [
            {"row_id": "R1", "type_mark": "A", "unit": "шт", "quantity": "2", "mass": "3,5"},
            {"row_id": "R2", "type_mark": "B", "unit": "шт", "quantity": "3", "total_mass": "12"},
        ]}],
    )

    assert result["baseline"]["metrics"] == {
        "row_count": 2,
        "type_mark_count": 2,
        "quantity_by_unit": {"шт": 5.0},
        "total_mass_kg": 19.0,
    }
    assert result["proposal"]["metrics"]["total_mass_kg"] is None
    assert result["proposal"]["claimed_savings_percent"] == [12.5]
    assert result["effect"]["natural"]["mass_delta_kg"] is None
    assert result["effect"]["monetary"] is None


def test_alternative_evaluation_exposes_incomplete_baseline_without_false_totals():
    result = build_alternative_evaluation(
        {"title": "Вариант"},
        [],
        [{"rows": [
            {"row_id": "R1", "unit": "шт", "quantity": "4", "mass": "2"},
            {"row_id": "R2", "quantity": "не указано"},
        ]}],
    )

    assert result["baseline"]["metrics"]["quantity_by_unit"] == {"шт": 4.0}
    assert result["baseline"]["metrics"]["total_mass_kg"] is None
    assert result["unknown_quantity_row_ids"] == ["R2"]
    assert "Единица и числовое количество для всех исходных строк" in result["missing_inputs"]


def test_alternative_inputs_calculate_natural_and_sourced_monetary_delta():
    evaluation = {"baseline": {"metrics": {"type_mark_count": 4, "total_mass_kg": 100}}, "proposal": {"metrics": {}}, "effect": {"natural": {}}}
    result = apply_alternative_inputs(evaluation, {
        "type_mark_count": 2, "total_mass_kg": 80,
        "baseline_cost": 1000, "proposal_cost": 850, "currency": "RUB",
        "price_source": "КП-17", "price_date": "2026-09-28", "cost_composition": "материал и монтаж",
        "cost_uncertainty_percent": 10,
    })
    assert result["effect"]["natural"]["type_mark_delta"] == -2
    assert result["effect"]["natural"]["mass_delta_kg"] == -20
    assert result["effect"]["monetary"]["delta"] == -150
    assert result["effect"]["monetary"]["delta_range"] == {"best": -335.0, "worst": 35.0}
    assert result["status"] == "calculated"


def test_monetary_inputs_require_traceable_basis():
    with pytest.raises(ValueError, match="источник"):
        apply_alternative_inputs({"baseline": {"metrics": {}}, "proposal": {"metrics": {}}}, {
            "baseline_cost": 100, "proposal_cost": 90, "currency": "RUB",
        })


def test_type_size_evaluation_requires_one_proposal_standard():
    evaluation = build_alternative_evaluation(
        {"kind": "type_size_reduction_opportunity", "representative_proposal": "Сократить варианты"},
        [],
        [{"rows": [{"row_id": "R1", "type_mark": "A", "unit": "шт", "quantity": "2"}]}],
    )
    assert "Единый предлагаемый типоразмер" in evaluation["missing_inputs"][0]
    with pytest.raises(ValueError, match="единый предлагаемый типоразмер"):
        apply_alternative_inputs(evaluation, {"baseline_cost": 100, "proposal_cost": 90})

    result = apply_alternative_inputs(evaluation, {
        "proposal_standard": "Тип A, IP44, 230 В",
        "baseline_operation_count": 10,
        "operation_count": 6,
        "baseline_cost": 100, "proposal_cost": 90,
        "currency": "RUB", "price_source": "КП-1", "price_date": "2026-10-30",
        "cost_composition": "материал",
    })
    assert result["proposal"]["standard"] == "Тип A, IP44, 230 В"
    assert result["effect"]["natural"]["operation_delta"] == -4.0
