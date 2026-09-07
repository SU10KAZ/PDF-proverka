from __future__ import annotations

from copy import deepcopy
import json
import sqlite3
import time

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from backend.app.api.routers import stage_comparison as router
from backend.app.core.portal_auth import PortalSettings
from backend.app.services.stage_comparison import domain_keys as dk
from backend.app.services.stage_comparison.decision_registry import (
    DecisionRegistry,
    RegistryConflict,
)
from backend.app.services.stage_comparison import human_contour
from backend.app.services.stage_comparison import production_orchestrator
from backend.app.services.stage_comparison.production_artifacts import content_signature


def document(document_id="doc", version_id="v1"):
    return dk.document_version(
        document_id,
        version_id,
        [{"kind": "pdf", "sha256": "1" * 64, "size": 10, "missing": False}],
    )


def question_key(*, left=None, right=None, target=None, question_class="SHEET_MATCHING"):
    left = left or document("left")
    right = right or document("right")
    target = target or dk.sheet_relation(
        left.key,
        right.key,
        [dk.sheet(left.key, 1).key],
        [dk.sheet(right.key, 2).key],
        "CANDIDATE",
    )
    return dk.question(left.key, right.key, question_class, [target.key], [])


def question_row(key, *, question_id="legacy-a", question_type="SHEET_RELATION"):
    return {
        "question_id": question_id,
        "legacy_id": question_id,
        **key.fields(),
        "category": "SHEET",
        "question_type": question_type,
        "prompt": "Это один и тот же лист?",
        "answer_options": [
            {"code": "YES", "label": "Да"},
            {"code": "NO", "label": "Нет"},
        ],
        "dependencies": [{"kind": "SHEET_RELATION", "ref": "relation-a"}],
        "context": {
            "left_pages": [1],
            "right_pages": [2],
            "left_sheets": [{"page": 1, "title": "План", "sheet_no": "1"}],
            "right_sheets": [{"page": 2, "title": "План", "sheet_no": "1"}],
            "why_proposed": ["совпадает назначение листа"],
        },
        "input_signature": content_signature({"semantic-question": key.key}),
        "status": "PENDING",
    }


def artifact(
    key=None,
    *,
    question_id="legacy-a",
    input_signature="input-a",
    algorithm_signature="algorithm-a",
    question_type="SHEET_RELATION",
):
    key = key or question_key()
    row = question_row(key, question_id=question_id, question_type=question_type)
    return {
        "kind": "stage_comparison_human_review_questions",
        "schema_version": "human-review-queue.v3",
        "input_signature": "legacy-queue-signature",
        "questions": [row],
        "counts": {"SHEET": 1, "ENTITY": 0, "CHANGE": 0, "total": 1},
        "stable_domain_keys": {
            "schema": "stable-domain-materialization.v1",
            "domain_key_version": "v1",
            "input_content_signature": input_signature,
            "algorithm_signature": algorithm_signature,
            "result_signature": "result-a",
            "run_instance_signature": "runtime-only",
            "document_versions": {
                "left": {**document("left").fields(), "document_id": "left", "version_id": "v1"},
                "right": {**document("right").fields(), "document_id": "right", "version_id": "v1"},
            },
        },
    }


def state(*, run_id="run-a", stale=False):
    return {"run_id": run_id, "stale": stale, "status": "PARTIAL"}


@pytest.fixture
def contour_flags(monkeypatch, tmp_path):
    monkeypatch.setenv(human_contour.HUMAN_CONTOUR_FLAG, "true")
    monkeypatch.setenv(human_contour.QUESTION_VISIBILITY_FLAG, "true")
    monkeypatch.setenv(
        human_contour.REGISTRY_PATH_ENV, str(tmp_path / "decision-registry.sqlite")
    )


def build_view(source, registry, *, current_state=None):
    return human_contour.build_read_model(
        source,
        state=current_state or state(),
        materialized=source,
        decision_registry=registry,
    )


def answer(source, registry, *, expected_revision=0, run_id="run-a", value="YES", lock=False):
    row = source["questions"][0]
    return human_contour.append_answers(
        questions_artifact=source,
        state=state(run_id=run_id),
        answers=[
            {
                "question_id": row["question_id"],
                "domain_key": row["domain_key"],
                "answer": value,
                "comment": "Проверено по листам",
                "lock": lock,
            }
        ],
        author="engineer-1",
        expected_input_signature=source["stable_domain_keys"]["input_content_signature"],
        expected_revision=expected_revision,
        session_id="session-a",
        pair_id="pair-a",
        decision_registry=registry,
    )


def test_visibility_has_explicit_actionable_and_stale_states(monkeypatch, tmp_path):
    monkeypatch.setenv(human_contour.QUESTION_VISIBILITY_FLAG, "true")
    monkeypatch.delenv(human_contour.HUMAN_CONTOUR_FLAG, raising=False)
    source = artifact()
    actionable = human_contour.build_read_model(
        source, state=state(), materialized=source
    )
    assert actionable["questions"][0]["question_state"] == "ACTIONABLE"
    assert actionable["questions"][0]["visible"] is True
    assert actionable["contour_state_counts"] == {
        "atomic_questions_total": 1,
        "actionable": 1,
        "visible": 1,
        "stale": 0,
        "resolved": 0,
        "superseded": 0,
        "locked": 0,
        "requires_revalidation": 0,
        "unavailable": 0,
        "hidden_healthy": 0,
    }
    stale = human_contour.build_read_model(
        source, state=state(stale=True), materialized=source
    )
    assert stale["questions"][0]["question_state"] == "STALE"
    assert stale["questions"][0]["actionable"] is False
    assert stale["questions"][0]["visible"] is True


def test_flags_off_preserve_question_response_without_storage_reads(monkeypatch):
    monkeypatch.delenv(human_contour.QUESTION_VISIBILITY_FLAG, raising=False)
    monkeypatch.delenv(human_contour.HUMAN_CONTOUR_FLAG, raising=False)
    monkeypatch.setattr(
        production_orchestrator.production_store,
        "load_artifact",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected read")),
    )
    legacy = {"questions": [{"question_id": "legacy"}], "available": True}
    assert production_orchestrator._decorate_human_contour_questions(
        "session", "pair", {"stale": False}, legacy
    ) == legacy


def test_contour_fails_closed_without_stable_materialization(contour_flags, tmp_path):
    registry = DecisionRegistry(tmp_path / "decision-registry.sqlite")
    source = artifact()
    missing = deepcopy(source)
    missing.pop("stable_domain_keys")
    view = human_contour.build_read_model(
        source, state=state(), materialized=missing, decision_registry=registry
    )
    assert view["questions"][0]["question_state"] == "UNAVAILABLE"
    assert view["questions"][0]["actionable"] is False
    with pytest.raises(human_contour.HumanContourUnavailable):
        human_contour.append_answers(
            questions_artifact=missing,
            state=state(),
            answers=[],
            author="engineer",
            expected_input_signature="legacy-queue-signature",
            expected_revision=0,
            session_id="session",
            pair_id="pair",
            decision_registry=registry,
        )


def test_domain_answer_survives_new_session_pair_run_and_store_reopen(
    contour_flags, tmp_path
):
    source_a = artifact(question_id="legacy-run-a")
    path = tmp_path / "decision-registry.sqlite"
    first_registry = DecisionRegistry(path)
    receipt = answer(source_a, first_registry)
    assert receipt["receipts"][0]["domain_key"] == source_a["questions"][0]["domain_key"]

    # Simulate backend restart plus a new session/pair/run and a new transient ID.
    second_registry = DecisionRegistry(path)
    source_b = artifact(question_id="legacy-run-b")
    view = human_contour.build_read_model(
        source_b,
        state=state(run_id="run-b"),
        materialized=source_b,
        decision_registry=second_registry,
    )
    row = view["questions"][0]
    assert row["domain_key"] == source_a["questions"][0]["domain_key"]
    assert row["question_id"] != source_a["questions"][0]["question_id"]
    assert row["question_state"] == "RESOLVED"
    assert row["human_answer"]["answer"] == "YES"
    assert row["human_answer"]["author"] == "engineer-1"
    assert view["contour_state_counts"]["actionable"] == 0


def test_false_reuse_is_zero_for_semantic_question_changes(contour_flags, tmp_path):
    registry = DecisionRegistry(tmp_path / "decision-registry.sqlite")
    original = artifact()
    answer(original, registry)
    left, right = document("left"), document("right")
    variants = [
        question_key(
            left=left,
            right=right,
            target=dk.sheet_relation(
                left.key,
                right.key,
                [dk.sheet(left.key, 3).key],
                [dk.sheet(right.key, 4).key],
                "CANDIDATE",
            ),
        ),
        question_key(
            left=left,
            right=right,
            target=dk.atomic_change(
                dk.Claim(
                    left.key,
                    right.key,
                    "owner",
                    "facet",
                    "PARAMETER",
                    "ALTERED",
                    "MATERIAL_CHANGE",
                    dk.typed("10"),
                    dk.typed("20"),
                ),
                field="pressure",
            ),
            question_class="CHANGE_CONFIRMATION",
        ),
        question_key(left=document("left", "v2"), right=right),
        question_key(
            left=left,
            right=right,
            target=dk.atomic_change(
                dk.Claim(
                    left.key,
                    right.key,
                    "owner",
                    "facet",
                    "PARAMETER",
                    "ALTERED",
                    "MATERIAL_CHANGE",
                    dk.typed("11"),
                    dk.typed("21"),
                ),
                field="temperature",
            ),
            question_class="CHANGE_CONFIRMATION",
        ),
        question_key(left=left, right=right, question_class="CONFLICT"),
    ]
    assert len({value.key for value in variants}) == len(variants)
    false_reuse = 0
    for index, variant in enumerate(variants):
        changed = artifact(
            variant,
            question_id=f"changed-{index}",
            input_signature=f"changed-input-{index}",
        )
        view = build_view(changed, registry)
        row = view["questions"][0]
        false_reuse += int(row["human_answer"] is not None)
        assert row["question_state"] == "ACTIONABLE"
    assert false_reuse == 0


def test_same_domain_document_scope_change_is_stale(contour_flags, tmp_path):
    registry = DecisionRegistry(tmp_path / "decision-registry.sqlite")
    source = artifact()
    answer(source, registry)
    changed = deepcopy(source)
    changed["stable_domain_keys"]["input_content_signature"] = "new-document-scope"
    view = build_view(changed, registry)
    assert view["questions"][0]["question_state"] == "STALE"
    assert view["questions"][0]["actionable"] is False
    assert view["questions"][0]["human_answer"] is None
    assert view["questions"][0]["prior_human_answer"]["answer"] == "YES"


def test_algorithm_change_requires_revalidation_and_explicit_policy_reuses(
    contour_flags, tmp_path
):
    registry = DecisionRegistry(tmp_path / "decision-registry.sqlite")
    source = artifact()
    answer(source, registry)
    key = source["questions"][0]["domain_key"]
    result = registry.resolve(key, "ATOMIC", "input-a", "algorithm-b")
    assert result["state"] == "REQUIRES_REVALIDATION"
    assert result["decision"] is None
    changed = deepcopy(source)
    changed["stable_domain_keys"]["algorithm_signature"] = "algorithm-b"
    changed_view = build_view(changed, registry)["questions"][0]
    assert changed_view["question_state"] == "REQUIRES_REVALIDATION"
    assert changed_view["human_answer"] is None
    assert changed_view["prior_human_answer"]["answer"] == "YES"
    compatible = registry.resolve(
        key,
        "ATOMIC",
        "input-a",
        "algorithm-b",
        compatibility_policy={
            "from_algorithm_signature": "algorithm-a",
            "to_algorithm_signature": "algorithm-b",
            "change_class": "ID_ONLY",
            "decision_policy": "REUSE",
            "approved_by": "release-owner",
        },
    )
    assert compatible["decision"] == "YES"
    answer(changed, registry, expected_revision=1, run_id="run-b")
    assert registry.raw_history(key, "ATOMIC")[-1]["audit_event"] == "REVALIDATED"
    assert registry.resolve(key, "ATOMIC", "input-a", "algorithm-b")["decision"] == "YES"


def test_locked_human_authority_and_append_only_supersession(contour_flags, tmp_path):
    registry = DecisionRegistry(tmp_path / "decision-registry.sqlite")
    source = artifact()
    first = answer(source, registry, lock=True)["receipts"][0]
    row = source["questions"][0]
    lower = {
        "domain_key": row["domain_key"],
        "domain_key_version": "v1",
        "target_kind": "ATOMIC_QUESTION",
        "decision_scope": "ATOMIC",
        "decision": "NO",
        "authority": "AI",
        "based_on_input_content_signature": "input-a",
        "based_on_algorithm_signature": "algorithm-a",
        "supersedes_decision_id": first["decision_id"],
    }
    with pytest.raises(RegistryConflict):
        registry.append(
            lower,
            expected_revision=1,
            current_input_signature="input-a",
        )
    second = answer(source, registry, expected_revision=1, value="NO")["receipts"][0]
    history = registry.effective_history(row["domain_key"], "ATOMIC")
    assert len(history) == 2
    assert history[0]["state"] == "SUPERSEDED"
    assert history[1]["state"] == "ACTIVE"
    assert history[1]["supersedes_decision_id"] == first["decision_id"]
    assert second["decision_id"] != first["decision_id"]
    assert history[0]["audit_event"] == "CREATED"
    assert history[1]["audit_event"] == "CHANGED"


def test_existing_m1_registry_schema_is_upgraded_without_rewriting_payload(tmp_path):
    path = tmp_path / "m1-registry.sqlite"
    original = {
        "decision_id": "dec_existing",
        "domain_key": question_key().key,
        "domain_key_version": "v1",
        "target_kind": "ATOMIC_QUESTION",
        "decision_scope": "ATOMIC",
        "decision": "YES",
        "authority": "HUMAN",
        "author": "engineer",
        "based_on_input_content_signature": "input-a",
        "based_on_algorithm_signature": "algorithm-a",
        "state": "ACTIVE",
    }
    encoded = json.dumps(original, ensure_ascii=False)
    with sqlite3.connect(path) as db:
        db.execute(
            "CREATE TABLE events (revision INTEGER PRIMARY KEY AUTOINCREMENT, "
            "decision_id TEXT UNIQUE NOT NULL, domain_key TEXT NOT NULL, "
            "scope TEXT NOT NULL, payload TEXT NOT NULL)"
        )
        db.execute(
            "INSERT INTO events(decision_id,domain_key,scope,payload) VALUES(?,?,?,?)",
            (original["decision_id"], original["domain_key"], "ATOMIC", encoded),
        )
    registry = DecisionRegistry(path)
    assert registry.raw_history(original["domain_key"], "ATOMIC") == [original]
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT payload FROM events").fetchone()[0] == encoded


def test_answer_is_atomic_only_and_preserves_evidence(contour_flags, tmp_path):
    registry = DecisionRegistry(tmp_path / "decision-registry.sqlite")
    source = artifact()
    source["questions"][0]["provenance_keys"] = ["dk1_evidence_provenance_deadbeef"]
    answer(source, registry)
    view = build_view(source, registry)
    assert view["resolution_semantics"] == {
        "durable_target": "AtomicQuestion DomainKey",
        "propagation": "ATOMIC_ONLY",
        "atomic_review_item": "UNCHANGED",
        "sheet_relation": "UNCHANGED",
        "entity_relation": "UNCHANGED",
        "atomic_change": "UNCHANGED",
        "presentation_group": "DISPLAY_ONLY",
    }
    row = view["questions"][0]
    assert row["evidence"]["left_sheets"][0]["page"] == 1
    assert row["evidence"]["document_versions"]["left"]["version_id"] == "v1"
    stored = registry.raw_history(row["domain_key"], "ATOMIC")[0]
    assert stored["provenance"]["matcher_artifacts_mutated"] is False
    assert stored["evidence_provenance_keys"] == ["dk1_evidence_provenance_deadbeef"]


def test_optimistic_revision_and_domain_legacy_cross_check(contour_flags, tmp_path):
    registry = DecisionRegistry(tmp_path / "decision-registry.sqlite")
    source = artifact()
    answer(source, registry)
    with pytest.raises(RegistryConflict):
        answer(source, registry, expected_revision=0)
    bad = deepcopy(source)
    with pytest.raises(ValueError, match="legacy question_id"):
        human_contour.append_answers(
            questions_artifact=bad,
            state=state(),
            answers=[
                {
                    "question_id": "another-transient-id",
                    "domain_key": bad["questions"][0]["domain_key"],
                    "answer": "YES",
                }
            ],
            author="engineer-1",
            expected_input_signature="input-a",
            expected_revision=1,
            session_id="session",
            pair_id="pair",
            decision_registry=registry,
        )


def test_question_types_include_conflict_and_missing_data(contour_flags, tmp_path):
    registry = DecisionRegistry(tmp_path / "decision-registry.sqlite")
    conflict = artifact(question_type="CHANGE_CONTESTED")
    conflict["questions"][0]["category"] = "CHANGE"
    missing = artifact(question_type="CHANGE_MISSING_DATA")
    missing["questions"][0]["category"] = "CHANGE"
    assert build_view(conflict, registry)["questions"][0]["question_class"] == "CONFLICT"
    assert build_view(missing, registry)["questions"][0]["question_class"] == "MISSING_DATA"


def test_authorization_requires_portal_session_and_employee_role(monkeypatch):
    settings = PortalSettings(
        enabled=True,
        users={"alice": "hash"},
        secret="secret",
        ttl_seconds=3600,
        cookie_secure_mode="false",
        cookie_name="portal_session",
    )
    request = Request({"type": "http", "method": "PUT", "path": "/", "headers": []})
    monkeypatch.setattr(router.portal_auth, "get_settings", lambda: settings)
    monkeypatch.setattr(router.portal_auth, "request_username", lambda *_: "alice")
    monkeypatch.setattr(
        router.user_service,
        "get_user_by_login",
        lambda login: {"id": "employee-7", "login": login, "role": "expert"},
    )
    assert router._authorized_human_contour_author(request) == "employee-7"
    monkeypatch.setattr(router.portal_auth, "request_username", lambda *_: None)
    with pytest.raises(HTTPException) as denied:
        router._authorized_human_contour_author(request)
    assert denied.value.status_code == 401
    monkeypatch.setattr(router.portal_auth, "request_username", lambda *_: "alice")
    monkeypatch.setattr(
        router.user_service,
        "get_user_by_login",
        lambda login: {"id": "employee-7", "login": login, "role": "viewer"},
    )
    with pytest.raises(HTTPException) as forbidden:
        router._authorized_human_contour_author(request)
    assert forbidden.value.status_code == 403
    monkeypatch.setattr(
        router.portal_auth,
        "get_settings",
        lambda: PortalSettings(
            enabled=False,
            users={},
            secret="secret",
            ttl_seconds=3600,
            cookie_secure_mode="false",
            cookie_name="portal_session",
        ),
    )
    with pytest.raises(HTTPException) as unavailable:
        router._authorized_human_contour_author(request)
    assert unavailable.value.status_code == 503


def test_121_question_read_and_lookup_are_cheap(contour_flags, tmp_path):
    registry = DecisionRegistry(tmp_path / "decision-registry.sqlite")
    left, right = document("left"), document("right")
    source = artifact()
    rows = []
    for index in range(121):
        target = dk.sheet_relation(
            left.key,
            right.key,
            [dk.sheet(left.key, index + 1).key],
            [dk.sheet(right.key, index + 1).key],
            "CANDIDATE",
        )
        key = question_key(left=left, right=right, target=target)
        rows.append(question_row(key, question_id=f"question-{index}"))
    source["questions"] = rows
    source["counts"] = {"SHEET": 121, "ENTITY": 0, "CHANGE": 0, "total": 121}
    started = time.perf_counter()
    view = build_view(source, registry)
    read_seconds = time.perf_counter() - started
    started = time.perf_counter()
    for row in rows:
        registry.resolve(row["domain_key"], "ATOMIC", "input-a", "algorithm-a")
    lookup_seconds = time.perf_counter() - started
    assert view["contour_state_counts"]["visible"] == 121
    assert view["contour_state_counts"]["hidden_healthy"] == 0
    assert read_seconds < 1.0
    assert lookup_seconds < 1.0


def test_frontend_uses_explicit_visibility_and_domain_key_payload():
    root = __import__("pathlib").Path(__file__).parents[1]
    index = (root / "frontend/index.html").read_text(encoding="utf-8")
    app = (root / "frontend/static/js/app.js").read_text(encoding="utf-8")
    review = (root / "frontend/static/js/stage-comparison-review.js").read_text(
        encoding="utf-8"
    )
    assert 'v-if="scProductionQuestionSectionVisible"' in index
    assert '!scHumanReview.available\n                                      &&' not in index
    assert "questions.question_visibility_v1_enabled === true" in app
    assert "domain_key: row.domain_key || undefined" in app
    assert "actionable: question.actionable !== false" in review
