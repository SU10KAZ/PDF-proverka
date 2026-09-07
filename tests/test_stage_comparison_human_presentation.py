from copy import deepcopy

import pytest

from backend.app.services.stage_comparison import human_presentation as hp
from backend.app.services.stage_comparison import domain_keys as dk
from backend.app.services.stage_comparison import production_orchestrator as production
from backend.app.services.stage_comparison import production_store, store
from backend.app.services.stage_comparison.decision_registry import DecisionRegistry
from test_stage_comparison_human_contour_v1 import (
    answer, artifact, build_view, contour_flags, document, question_key, question_row,
)


def source(indices=(1, 2, 3), prefix="a"):
    result = artifact()
    result["questions"] = [question_row(question_key(target=dk.sheet_relation(
        document("left").key, document("right").key,
        [dk.sheet(document("left").key, index).key],
        [dk.sheet(document("right").key, index).key], "CANDIDATE")),
        question_id=f"{prefix}-{index}") for index in indices]
    return result


def project(raw, registry):
    return hp.questions(build_view(raw, registry), identity="left@v1|right@v1")


def test_group_roundtrip_rerun_and_membership_change(contour_flags, tmp_path):
    registry = DecisionRegistry(tmp_path / "decisions.sqlite")
    raw = source()
    before = project(raw, registry)
    group = before["presentation"]["items"][0]
    assert group["child_count"] == 3
    assert group["decision_policy"] == "DISPLAY_ONLY_GROUP"
    assert group["atomic_question_domain_keys"] == sorted(q["domain_key"] for q in raw["questions"])
    assert group["status_summary"]["status"] == "ALL_PENDING"
    answer(raw, registry)
    after = project(raw, registry)
    assert [q["question_state"] for q in after["questions"]] == ["RESOLVED", "ACTIONABLE", "ACTIONABLE"]
    assert after["questions"][1:] == before["questions"][1:]
    assert after["presentation"]["items"][0]["status_summary"]["status"] == "PARTIALLY_RESOLVED"
    rerun = project(source(prefix="different-runtime"), registry)
    assert rerun["presentation"]["items"][0]["group_id"] == group["group_id"]
    assert rerun["questions"][0]["human_answer"]["answer"] == "YES"
    changed = project(source((1, 3, 4), prefix="run-c"), registry)
    assert [q["question_state"] for q in changed["questions"]] == ["RESOLVED", "ACTIONABLE", "ACTIONABLE"]
    assert changed["questions"][-1]["decision_history"] == []


@pytest.mark.parametrize("states,expected", [
    (["ACTIONABLE", "ACTIONABLE"], "ALL_PENDING"),
    (["ACTIONABLE", "RESOLVED"], "PARTIALLY_RESOLVED"),
    (["RESOLVED", "LOCKED", "ACTIVE"], "ALL_RESOLVED"),
    (["STALE", "ACTIONABLE"], "HAS_STALE"),
    (["REQUIRES_REVALIDATION", "LOCKED"], "HAS_REVALIDATION_REQUIRED"),
    (["UNAVAILABLE", "ACTIONABLE"], "HAS_UNAVAILABLE"),
    (["SUPERSEDED"], "HAS_UNAVAILABLE"),
])
def test_child_only_status(states, expected):
    rows = [{"question_state": state} for state in states]
    before = deepcopy(rows)
    assert hp.status_summary(rows)["status"] == expected
    assert rows == before


def test_standalone_historical_and_read_only_projection(contour_flags, tmp_path):
    registry = DecisionRegistry(tmp_path / "decisions.sqlite")
    raw = source()
    raw["questions"][-1]["question_type"] = "CONFLICT"
    raw.pop("stable_domain_keys")
    for row in raw["questions"]:
        row.pop("domain_key")
    before = deepcopy(raw)
    result = project(raw, registry)
    assert result["presentation"]["metrics"]["groups"] == 1
    assert result["presentation"]["metrics"]["standalone"] == 1
    assert result["presentation"]["metrics"]["unavailable_historical"] == 3
    assert all(not q["answerable"] for q in result["questions"])
    assert raw == before and registry.revision() == 0


def test_flag_requires_human_contour(monkeypatch):
    monkeypatch.setenv(hp.FEATURE_FLAG, "true")
    monkeypatch.setenv("HUMAN_CONTOUR_V1_ENABLED", "false")
    assert not hp.enabled()


def test_off_adapter_preserves_response_and_does_no_extra_reads(monkeypatch):
    monkeypatch.delenv(hp.FEATURE_FLAG, raising=False)
    response = {"questions": [{"question_state": "UNAVAILABLE"}]}
    monkeypatch.setattr(production, "get_review_questions", lambda *args: response)
    monkeypatch.setattr(hp, "_generation", lambda *args: pytest.fail("flag-off extra read"))
    assert hp.get_review_questions("session", "pair") is response


def test_generation_race_is_rejected(contour_flags, tmp_path, monkeypatch):
    monkeypatch.setenv(hp.FEATURE_FLAG, "true")
    registry = DecisionRegistry(tmp_path / "decisions.sqlite")
    response = build_view(source(), registry)
    monkeypatch.setattr(production, "get_review_questions", lambda *args: response)
    monkeypatch.setattr(store, "get_pair_for_production", lambda *args: {})
    states = iter([{"run_id": "a"}, {"run_id": "b"}])
    monkeypatch.setattr(hp, "_generation", lambda *args: next(states))
    with pytest.raises(production_store.ProductionConflictError):
        hp.get_review_questions("session", "pair")


def test_group_id_cannot_be_answer_target(contour_flags, tmp_path):
    registry = DecisionRegistry(tmp_path / "decisions.sqlite")
    raw = source()
    group_id = project(raw, registry)["presentation"]["items"][0]["group_id"]
    forged = deepcopy(raw)
    forged["questions"][0]["domain_key"] = group_id
    from backend.app.services.stage_comparison import human_contour
    with pytest.raises(ValueError):
        human_contour.append_answers(
            questions_artifact=raw, state={"stale": False},
            answers=[{"domain_key": group_id, "answer": "YES"}],
            author="engineer", expected_revision=0,
            expected_input_signature="input-a", session_id="new", pair_id="new",
            decision_registry=registry,
        )
    assert registry.revision() == 0
