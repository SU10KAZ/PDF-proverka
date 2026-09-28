from backend.app.services.section_optimization_library_service import build_solution_library


def test_library_preserves_decision_scope_and_implementation_evidence():
    result = build_solution_library([{
        "replication_id": "R1", "signal_id": "S1", "title": "Типовой кронштейн",
        "engineering_passport": {"action": "Заменить крепление", "subjects": [{"name": "Кронштейн"}]},
        "expert_decisions": [{
            "decision_id": "D1", "project_id": "P2", "decision": "accepted_with_conditions",
            "conditions": ["Проверить основание"], "reviewer": "Иван", "decided_at": "2026-09-28",
        }],
        "implementation_checks": [{
            "project_id": "P2", "status": "implemented", "evidence_refs": ["P2:v3:лист 7"],
        }],
    }])
    entry = result["entries"][0]
    assert entry["project_id"] == "P2"
    assert entry["implementation_status"] == "implemented"
    assert entry["implementation_evidence_refs"] == ["P2:v3:лист 7"]
    assert "отдельной проверки" in entry["reuse_policy"]
    assert result["counts"]["implemented"] == 1
