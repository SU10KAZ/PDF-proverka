from backend.app.services.section_optimization_alternative_service import build_alternative_evaluation


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
