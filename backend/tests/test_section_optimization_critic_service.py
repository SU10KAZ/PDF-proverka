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


def test_critic_closes_only_matching_missing_data_with_evidenced_answer():
    assessment = {
        "project_id": "P2", "verdict": "applicable", "reason": "Параметры совпадают.",
        "target_row_ids": ["ROW-2"], "missing_data": ["Неизвестна нагрузка"],
        "graphics_required": False,
    }
    request = {
        "project_id": "P2", "kind": "missing_data", "answer": "100 кВт",
        "evidence_refs": ["P2:v3:лист ЭОМ-7"],
    }

    assert review_replication_dossier(_dossier(), [assessment], [request])["status"] == "pass"
    request["evidence_refs"] = []
    assert review_replication_dossier(_dossier(), [assessment], [request])["status"] == "blocked"


def test_replication_still_requires_accepted_source():
    dossier = _dossier()
    dossier["source_decisions"] = []
    result = review_replication_dossier(dossier, [{"project_id": "P2"}])
    assert "source_decision_missing" in {f["code"] for f in result["target_reviews"][0]["findings"]}


def test_discovery_cannot_be_accepted_without_concrete_comparison():
    from copy import deepcopy
    dossier = _dossier()
    dossier.update(candidate={"kind": "type_size_reduction_opportunity"}, source_decisions=[])
    dossier["targets"][0]["rows"][0]["type_mark"] = "A"
    dossier["targets"].append({"project_id": "P3", "rows": [{"row_id": "ROW-3", "type_mark": "B"}]})
    assessment = {
        "project_id": "P2", "verdict": "applicable", "reason": "Совпадают параметры подключения",
        "target_row_ids": ["ROW-2"], "proposed_action": "Заменить A на B",
        "comparison_row_ids": ["ROW-2", "ROW-3"], "graphics_required": False,
    }
    def target_review(a, d=dossier):
        return next(r for r in review_replication_dossier(d, [a])["target_reviews"] if r["project_id"] == "P2")
    assert target_review(assessment)["status"] == "pass"
    for patch in [{"proposed_action": ""}, {"comparison_row_ids": ["ROW-2"]},
                  {"comparison_row_ids": ["ROW-2", "INVENTED"]}, {"comparison_row_ids": ["ROW-3"]}]:
        result = target_review({**assessment, **patch})
        assert result["status"] == "blocked"
        assert "accepted" not in result["allowed_expert_decisions"]
    same_variant = deepcopy(dossier)
    same_variant["targets"][1]["rows"][0]["type_mark"] = "A"
    assert target_review(assessment, same_variant)["status"] == "blocked"
