from backend.app.services.section_optimization_readiness_service import build_pilot_readiness


def test_readiness_reports_exact_unclosed_gate():
    result = build_pilot_readiness({"specification_rows": [{"row_id": "R", "project_id": "P", "version_id": "v1"}]}, [{"input_stale": False, "critic": {"status": "pass"}, "expert_decisions": []}])
    assert [item["key"] for item in result["blockers"]] == ["expert_decisions"]


def test_readiness_never_hides_stale_expert_decision():
    result = build_pilot_readiness({"specification_rows": [{"row_id": "R", "project_id": "P", "version_id": "v1"}]}, [
        {"input_stale": False, "critic": {"status": "pass"}, "expert_decisions": [{"decision": "accepted"}]},
        {"input_stale": True, "expert_decisions": [{"decision": "accepted"}]},
    ])
    assert "no_stale_decisions" in {item["key"] for item in result["blockers"]}
