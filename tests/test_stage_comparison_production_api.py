from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routers import stage_comparison as router_mod
from backend.app.services.stage_comparison import production_orchestrator


BASE = "/api/stage-comparison/sessions/session-1/pairs/pair-1/production"


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(router_mod.router)
    return TestClient(app)


def test_run_endpoint_is_thin_and_rejects_client_paths_geometry(monkeypatch):
    captured = {}

    def run(session_id, pair_id, **request):
        captured.update(session_id=session_id, pair_id=pair_id, request=request)
        return {"status": "COMPLETED"}

    monkeypatch.setattr(router_mod.production, "run_production_comparison", run)
    client = _client()
    response = client.post(
        f"{BASE}/run",
        json={
            "input_mode": "PAGE",
            "left_pages": [10],
            "right_pages": [24],
            "left_block_ids": ["left-block"],
            "right_block_ids": ["right-block"],
        },
    )

    assert response.status_code == 200
    assert response.json() == {"status": "COMPLETED"}
    assert captured == {
        "session_id": "session-1",
        "pair_id": "pair-1",
        "request": {
            "input_mode": "PAGE",
            "left_pages": [10],
            "right_pages": [24],
            "left_block_ids": ["left-block"],
            "right_block_ids": ["right-block"],
        },
    }
    for forbidden in (
        {"pdf_path": "/srv/private.pdf"},
        {"bbox": [0, 0, 1, 1]},
        {"document": {"document_code": "forged"}},
    ):
        invalid = client.post(
            f"{BASE}/run",
            json={
                "input_mode": "PAGE",
                "left_pages": [10],
                "right_pages": [24],
                **forbidden,
            },
        )
        assert invalid.status_code == 422


def test_get_endpoints_never_trigger_run_and_keep_wrappers(monkeypatch):
    calls = []
    monkeypatch.setattr(
        router_mod.production,
        "run_production_comparison",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("GET must not start producer")
        ),
    )
    monkeypatch.setattr(
        router_mod.production,
        "get_production_state",
        lambda *_: calls.append("state") or {"status": "COMPLETED"},
    )
    monkeypatch.setattr(
        router_mod.production,
        "get_production_changes",
        lambda *_: calls.append("changes") or {"rows": [], "summary": {"total": 0}},
    )
    monkeypatch.setattr(
        router_mod.production,
        "get_review_questions",
        lambda *_: calls.append("questions") or {"questions": [], "counts": {"total": 0}},
    )
    monkeypatch.setattr(
        router_mod.production,
        "get_final_report",
        lambda *_: calls.append("final") or {"approved_atomic_changes": []},
    )
    client = _client()

    assert client.get(f"{BASE}/state").json()["status"] == "COMPLETED"
    assert client.get(f"{BASE}/changes").json()["rows"] == []
    assert client.get(f"{BASE}/questions").json()["questions"] == []
    assert client.get(f"{BASE}/final-report").json()["approved_atomic_changes"] == []
    assert calls == ["state", "changes", "questions", "final"]


def test_changes_get_exposes_published_artifact_conflict_as_409(monkeypatch):
    monkeypatch.setattr(
        router_mod.production,
        "get_production_changes",
        lambda *_: (_ for _ in ()).throw(
            production_orchestrator.ProductionStateConflictError(
                "engineer decisions digest does not match state"
            )
        ),
    )

    response = _client().get(f"{BASE}/changes")

    assert response.status_code == 409
    assert "digest does not match state" in response.json()["detail"]


def test_decision_author_is_server_owned_and_conflicts_are_409(monkeypatch):
    captured = {}
    monkeypatch.setattr(router_mod, "_engineer_author", lambda _request: "server-user")

    def update(session_id, pair_id, **kwargs):
        captured.update(session_id=session_id, pair_id=pair_id, **kwargs)
        return {"rows": [{"target_id": "change-1"}]}

    monkeypatch.setattr(router_mod.production, "update_engineer_decisions", update)
    client = _client()
    response = client.put(
        f"{BASE}/decisions",
        json={
            "updates": [{
                "target_id": "change-1",
                "decision": "APPROVED",
                "author": "client-forgery",
                "comment": "checked",
            }],
            "expected_input_signature": "input-1",
            "expected_revision": 7,
        },
    )

    assert response.status_code == 200
    assert captured["author"] == "server-user"
    assert "author" not in captured["updates"][0]
    assert captured["expected_input_signature"] == "input-1"
    assert captured["expected_revision"] == 7

    def conflict(*_args, **_kwargs):
        raise production_orchestrator.ProductionStateConflictError("stale")

    monkeypatch.setattr(router_mod.production, "update_engineer_decisions", conflict)
    missing_guard = client.put(
        f"{BASE}/decisions",
        json={"updates": [], "expected_input_signature": "old"},
    )
    assert missing_guard.status_code == 422
    stale = client.put(
        f"{BASE}/decisions",
        json={
            "updates": [],
            "expected_input_signature": "old",
            "expected_revision": 7,
        },
    )
    assert stale.status_code == 409


def test_review_answer_author_is_server_owned_and_revision_conflict_is_409(monkeypatch):
    captured = {}
    monkeypatch.setattr(router_mod, "_engineer_author", lambda _request: "server-user")

    def update(session_id, pair_id, **kwargs):
        captured.update(session_id=session_id, pair_id=pair_id, **kwargs)
        return {"questions": [], "counts": {"total": 0}}

    monkeypatch.setattr(router_mod.production, "update_review_answers", update)
    client = _client()
    response = client.put(
        f"{BASE}/answers",
        json={
            "answers": [{
                "question_id": "question-1",
                "answer": "YES",
                "author": "client-forgery",
            }],
            "expected_input_signature": "queue-1",
            "expected_revision": 3,
        },
    )

    assert response.status_code == 200
    assert captured["author"] == "server-user"
    assert "author" not in captured["answers"][0]
    assert captured["expected_revision"] == 3

    monkeypatch.setattr(
        router_mod.production,
        "update_review_answers",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            production_orchestrator.ProductionStateConflictError("revision changed")
        ),
    )
    conflict = client.put(
        f"{BASE}/answers",
        json={
            "answers": [],
            "expected_input_signature": "queue-1",
            "expected_revision": 2,
        },
    )
    assert conflict.status_code == 409


def test_review_answer_contract_preserves_explicit_and_typed_resolution(monkeypatch):
    captured = {}
    monkeypatch.setattr(router_mod, "_engineer_author", lambda _request: "server-user")

    def update(_session_id, _pair_id, **kwargs):
        captured.update(kwargs)
        return {"questions": [], "counts": {"total": 0}}

    monkeypatch.setattr(router_mod.production, "update_review_answers", update)
    response = _client().put(
        f"{BASE}/answers",
        json={
            "answers": [{
                "question_id": "question-1",
                "answer": "OTHER",
                "selected_refs": ["right:panel-2"],
                "explicit_candidate": {
                    "right_entity_ref": "right:panel-2",
                    "project_entity_ref": "project:panel-2",
                },
                "typed_resolution": {
                    "dimension": "PARAMETER",
                    "subject_ref": "panel-2",
                    "project_entity_ref": "project:panel-2",
                    "direction": "ALTERED",
                    "outcome": "MATERIAL_CHANGE",
                    "before_value": "220",
                    "after_value": "380",
                },
            }],
            "expected_input_signature": "queue-1",
            "expected_revision": 0,
        },
    )

    assert response.status_code == 200
    answer = captured["answers"][0]
    assert answer["selected_refs"] == ["right:panel-2"]
    assert answer["explicit_candidate"]["right_entity_ref"] == "right:panel-2"
    assert answer["typed_resolution"]["dimension"] == "PARAMETER"
    assert "selected_change_ids" not in answer["typed_resolution"]
    assert "author" not in answer


def test_review_answer_api_rejects_open_typed_enums_before_persistence(monkeypatch):
    called = False

    def update(*_args, **_kwargs):
        nonlocal called
        called = True
        return {}

    monkeypatch.setattr(router_mod.production, "update_review_answers", update)
    response = _client().put(
        f"{BASE}/answers",
        json={
            "answers": [{
                "question_id": "question-1",
                "answer": "OTHER",
                "typed_resolution": {
                    "dimension": "PARAMETER",
                    "direction": "SIDEWAYS",
                    "outcome": "REVIEW_REQUIRED",
                },
            }],
            "expected_input_signature": "queue-1",
            "expected_revision": 0,
        },
    )

    assert response.status_code == 422
    assert called is False


@pytest.mark.parametrize(
    "answer",
    [
        {"question_id": "question-1", "answer": "OTHER", "typed_resolution": {}},
        {
            "question_id": "question-1",
            "answer": "OTHER",
            "typed_resolution": {"selected_change_ids": []},
        },
        {
            "question_id": "question-1",
            "answer": "OTHER",
            "explicit_candidate": {
                "left_pages": [1, 2],
                "right_pages": [3, 4],
                "relation_type": "UNCERTAIN",
            },
        },
    ],
)
def test_review_answer_api_rejects_empty_or_uncertain_resolution(
    monkeypatch, answer
):
    called = False

    def update(*_args, **_kwargs):
        nonlocal called
        called = True
        return {}

    monkeypatch.setattr(router_mod.production, "update_review_answers", update)
    response = _client().put(
        f"{BASE}/answers",
        json={
            "answers": [answer],
            "expected_input_signature": "queue-1",
            "expected_revision": 0,
        },
    )

    assert response.status_code == 422
    assert called is False


def test_evidence_endpoint_only_delegates_target_id(monkeypatch):
    captured = []

    def evidence(session_id, pair_id, target_id):
        captured.append((session_id, pair_id, target_id))
        return {"target_id": target_id, "layout": "SIDE_BY_SIDE", "sides": {}}

    monkeypatch.setattr(router_mod.production, "get_change_evidence", evidence)
    response = _client().get(f"{BASE}/changes/change-1/evidence")

    assert response.status_code == 200
    assert response.json()["target_id"] == "change-1"
    assert captured == [("session-1", "pair-1", "change-1")]
