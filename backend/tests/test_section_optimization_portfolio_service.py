from backend.app.services.section_optimization_portfolio_service import build_section_optimization_portfolio


def _signal(signal_id, kind, title, row_id):
    return {
        "signal_id": signal_id,
        "kind": kind,
        "title": title,
        "target_row_ids": [row_id],
        "project_ids": ["P1"],
        "package_summary": {"source_reference": None},
    }


def test_portfolio_keeps_both_candidate_kinds_and_null_source_reference():
    snapshot = {
        "meta": {"generated_at": "2026-10-30T00:00:00Z"},
        "signals": [
            _signal("R1", "replicate_accepted_optimization", "Кабель FRHF", "row-1"),
            _signal("T1", "type_size_reduction_opportunity", "Типоразмер кабеля", "row-2"),
        ],
        "specification_rows": [
            {"row_id": "row-1", "project_id": "P1", "project_name": "Проект 1", "quantity": 2, "unit": "шт"},
            {"row_id": "row-2", "project_id": "P1", "project_name": "Проект 1", "quantity": 3, "unit": "шт"},
        ],
    }
    packages = build_section_optimization_portfolio(snapshot, [])
    assert {next(iter(item["kind_counts"])) for item in packages} == {
        "replicate_accepted_optimization", "type_size_reduction_opportunity",
    }
    assert len(packages) == 2
    assert all(item["economic_status"] == "needs_data" for item in packages)


def test_portfolio_does_not_double_count_overlapping_signals():
    first = _signal("R1", "replicate_accepted_optimization", "Кабель", "row-1")
    second = _signal("R2", "replicate_accepted_optimization", "Кабель", "row-1")
    snapshot = {
        "meta": {}, "signals": [first, second],
        "specification_rows": [{"row_id": "row-1", "project_id": "P1", "quantity": 1, "unit": "м"}],
    }
    packages = build_section_optimization_portfolio(snapshot, [])
    assert len(packages) == 2
    assert all(item["scope"]["row_count"] == 1 for item in packages)
    assert all(item["economic_status"] == "needs_data" for item in packages)
