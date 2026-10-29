from backend.app.services.section_optimization_package_service import (
    attach_package_summaries,
    build_package_summary,
    package_summary_from_dossier,
)


def _row(row_id, project_id, quantity, unit="шт", type_mark="A"):
    return {
        "row_id": row_id,
        "project_id": project_id,
        "project_name": project_id,
        "version_id": "v002",
        "quantity": quantity,
        "unit": unit,
        "type_mark": type_mark,
    }


def test_package_summary_aggregates_scope_without_inventing_unknown_units():
    signal = {
        "signal_id": "DISC-1",
        "kind": "type_size_reduction_opportunity",
        "representative_proposal": "Сократить типоразмеры",
        "match_basis": "семейство",
    }
    summary = build_package_summary(signal, [
        _row("r1", "P1", "10", "шт", "A"),
        _row("r2", "P2", "2", "шт", "B"),
        _row("r3", "P2", "4", "", "C"),
    ])
    assert summary["scope"]["project_count"] == 2
    assert summary["scope"]["row_count"] == 3
    assert summary["scope"]["quantity_by_unit"] == {"шт": 12.0}
    assert summary["scope"]["unknown_quantity_row_ids"] == ["r3"]
    assert summary["economic_status"] == "needs_data"
    assert summary["blockers"]


def test_attach_package_summaries_keeps_all_candidates_traceable():
    snapshot = {
        "meta": {"signals": 1},
        "specification_rows": [_row("r1", "P1", "3")],
        "accepted_optimizations": [],
        "signals": [{
            "signal_id": "DISC-1",
            "kind": "type_size_reduction_opportunity",
            "target_row_ids": ["r1"],
            "representative_proposal": "Проверить варианты",
        }],
    }
    result = attach_package_summaries(snapshot)
    assert result["meta"]["package_candidates"] == 1
    assert result["signals"][0]["package_summary"]["scope"]["row_ids"] == ["r1"]
    assert "package_summary" not in snapshot["signals"][0]


def test_dossier_package_summary_uses_calculated_monetary_effect():
    job = {
        "dossier": {
            "candidate": {"kind": "replicate_accepted_optimization", "representative_proposal": "Аналог"},
            "targets": [{"project_id": "P1", "rows": [_row("r1", "P1", "3")]}],
            "source_decisions": [],
            "alternative_evaluation": {
                "effect": {"monetary": {"delta": -10, "baseline": 100, "proposal": 90, "currency": "RUB", "source": "КП"}},
            },
        },
        "alternative_evaluation": {
            "effect": {"monetary": {"delta": -10, "baseline": 100, "proposal": 90, "currency": "RUB", "source": "КП"}},
        },
    }
    summary = package_summary_from_dossier(job)
    assert summary["economic_status"] == "positive"
    assert summary["effect"]["delta"] == -10
