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
    assert summary["scope"]["material_group_count"] == 3
    assert all(group["project_count"] == 1 for group in summary["scope"]["material_groups"])


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


def test_attach_package_summaries_groups_overlapping_signals_without_cross_kind_merge():
    snapshot = {
        "meta": {},
        "specification_rows": [
            _row("r1", "P1", "3"),
            _row("r2", "P2", "4"),
        ],
        "accepted_optimizations": [],
        "signals": [
            {"signal_id": "REPL-1", "kind": "replicate_accepted_optimization", "target_row_ids": ["r1"]},
            {"signal_id": "REPL-2", "kind": "replicate_accepted_optimization", "target_row_ids": ["r1", "r2"]},
            {"signal_id": "DISC-1", "kind": "type_size_reduction_opportunity", "target_row_ids": ["r1"]},
        ],
    }
    result = attach_package_summaries(snapshot)
    portfolio = result["meta"]["package_portfolio"]
    assert any(item["overlap"] and item["signal_ids"] == ["REPL-1", "REPL-2"] for item in portfolio)
    assert all(item["kind"] == "replicate_accepted_optimization" or item["signal_ids"] == ["DISC-1"] for item in portfolio)


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


def test_dossier_scope_falls_back_to_target_projects_for_legacy_rows():
    summary = package_summary_from_dossier({
        "target_project_ids": ["P1", "P2", "P3"],
        "dossier": {
            "candidate": {"kind": "replicate_accepted_optimization"},
            "targets": [{"rows": [{"row_id": "r1", "quantity": "3", "unit": "шт"}]}],
        },
    })
    assert summary["scope"]["project_count"] == 3
    assert [item["project_id"] for item in summary["scope"]["projects"]] == ["P1", "P2", "P3"]


def test_dossier_partition_exposes_applicable_excluded_and_unresolved_volume():
    rows = [
        _row("r1", "P1", "10", "шт", "A"),
        _row("r2", "P1", "4", "шт", "B"),
        _row("r3", "P2", "7", "шт", "C"),
    ]
    summary = package_summary_from_dossier({
        "target_project_ids": ["P1", "P2"],
        "dossier": {
            "candidate": {"kind": "replicate_accepted_optimization"},
            "targets": [
                {"project_id": "P1", "rows": rows[:2]},
                {"project_id": "P2", "rows": rows[2:]},
            ],
        },
        "agent_assessments": [
            {
                "project_id": "P1", "verdict": "applicable_with_conditions",
                "target_row_ids": ["r1"], "conditions": ["Сохранить IP44"],
                "reason": "Параметры совпадают",
            },
            {
                "project_id": "P1", "verdict": "reject",
                "target_row_ids": ["r2"], "reason": "Размер отличается",
            },
            {
                "project_id": "P2", "verdict": "needs_data",
                "target_row_ids": ["r3"], "missing_data": ["Нужна нагрузка"],
                "reason": "Нет исходной нагрузки",
            },
        ],
        "critic": {"status": "pass"},
    })

    applicability = summary["applicability"]
    assert applicability["coverage"] == {
        "total_rows": 3,
        "assessed_rows": 2,
        "applicable_rows": 1,
        "excluded_rows": 1,
        "unresolved_rows": 1,
        "uncovered_row_ids": ["r3"],
    }
    assert applicability["applicable"]["quantity_by_unit"] == {"шт": 10.0}
    assert applicability["excluded"]["quantity_by_unit"] == {"шт": 4.0}
    assert applicability["unresolved"]["quantity_by_unit"] == {"шт": 7.0}
    assert summary["technical_review"]["status"] == "blocked"
    assert summary["decision_gate"]["economic_yes"]["allowed"] is False
    assert summary["package_verdict"]["status"] == "needs_data"


def test_package_gate_allows_one_verdict_after_complete_applicability_and_prices():
    summary = package_summary_from_dossier({
        "target_project_ids": ["P1"],
        "dossier": {
            "candidate": {"kind": "replicate_accepted_optimization"},
            "targets": [{"project_id": "P1", "rows": [_row("r1", "P1", "10")]}],
        },
        "agent_assessments": [{
            "project_id": "P1", "verdict": "applicable_with_conditions",
            "target_row_ids": ["r1"], "conditions": ["Сохранить IP44"],
            "reason": "Параметры совпадают",
        }],
        "critic": {"status": "pass"},
        "graphics_status": "not_required",
        "dependency_assessment": {"status": "resolved"},
        "alternative_evaluation": {
            "effect": {"monetary": {
                "delta": -100, "baseline": 1000, "proposal": 900,
                "currency": "RUB", "source": "КП", "delta_range": {"best": -100, "worst": -100},
            }},
        },
    })
    assert summary["technical_review"]["status"] == "pass"
    assert summary["decision_gate"]["economic_yes"]["allowed"] is True
    assert summary["decision_gate"]["economic_yes"]["requires_note"] is True
    assert summary["package_verdict"]["status"] == "ready"
