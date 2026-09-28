from backend.app.services.section_optimization_critic_service import review_replication_dossier


def _dossier():
    return {
        "source_decisions": [{"source_ref": "P1:OPT-1", "proposed": "Типовое решение"}],
        "targets": [{
            "project_id": "P2",
            "rows": [{"row_id": "ROW-2", "name": "Целевая позиция"}],
        }],
    }


def test_critic_passes_addressed_complete_assessment():
    result = review_replication_dossier(_dossier(), [{
        "project_id": "P2",
        "verdict": "applicable_with_conditions",
        "resolved_verdict": "applicable_with_conditions",
        "reason": "Параметры позиции совпадают.",
        "target_row_ids": ["ROW-2"],
        "conditions": ["Сохранить расчётную нагрузку"],
        "missing_data": [],
        "graphics_required": False,
    }])

    assert result["status"] == "pass"
    assert result["counts"] == {"pass": 1, "blocked": 0}
    assert "accepted_with_conditions" in result["target_reviews"][0]["allowed_expert_decisions"]


def test_critic_blocks_positive_decision_with_missing_data_and_bad_reference():
    result = review_replication_dossier(_dossier(), [{
        "project_id": "P2",
        "verdict": "applicable",
        "reason": "Предположительно применимо.",
        "target_row_ids": ["HALLUCINATED"],
        "conditions": [],
        "missing_data": ["Неизвестна нагрузка"],
        "graphics_required": False,
    }])

    review = result["target_reviews"][0]
    assert result["status"] == "blocked"
    assert {item["code"] for item in review["findings"]} == {
        "target_evidence_missing", "missing_data",
    }
    assert review["allowed_expert_decisions"] == ["rejected", "returned"]


def test_critic_requires_target_graphics_evidence_when_requested():
    result = review_replication_dossier(_dossier(), [{
        "project_id": "P2",
        "verdict": "needs_graphics",
        "resolved_verdict": "applicable_with_conditions",
        "reason": "Нужна схема подключения.",
        "target_row_ids": ["ROW-2"],
        "conditions": ["Сохранить подключение"],
        "missing_data": [],
        "graphics_required": True,
        "graphics_review": {
            "conclusion": "supports_replication",
            "evidence": [],
        },
    }])

    review = result["target_reviews"][0]
    assert review["status"] == "blocked"
    assert review["findings"][0]["code"] == "graphics_evidence_missing"
