from __future__ import annotations

import copy

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routers import stage_comparison as router_mod
from backend.app.services.stage_comparison import production_orchestrator
from backend.app.services.stage_comparison.production_artifacts import (
    content_signature,
)
from backend.app.services.stage_comparison.production_text_evidence import (
    ProductionTextEvidenceConflictError,
    build_production_text_evidence,
)
from backend.app.services.stage_comparison.text_semantic_validation import (
    stage3_content_signature,
)


BASE = "/api/stage-comparison/sessions/session-1/pairs/pair-1/production"


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(router_mod.router)
    return TestClient(app)


def _anchor(fragment_id: str, page: int, x: float) -> dict:
    return {
        "fragment_id": fragment_id,
        "page": page,
        "bboxes": [{"x": x, "y": 0.2, "width": 0.3, "height": 0.04}],
    }


def _artifacts(*, stale: bool = False, review_required: bool = False):
    changed = {
        "before": "Напряжение 220 В",
        "after": "Напряжение 380 В",
        "left_fragment_ids": ["left-change"],
        "right_fragment_ids": ["right-change"],
        "left_pages": [7],
        "right_pages": [16],
        "left_anchors": [_anchor("left-change", 7, 0.2)],
        "right_anchors": [_anchor("right-change", 16, 0.22)],
    }
    same = {
        "before": "Одинаковый заголовок",
        "after": "Одинаковый заголовок",
        "left_fragment_ids": ["left-same"],
        "right_fragment_ids": ["right-same"],
        "left_pages": [7],
        "right_pages": [16],
        "left_anchors": [_anchor("left-same", 7, 0.1)],
        "right_anchors": [_anchor("right-same", 16, 0.12)],
    }
    differences = {
        "kind": "stage_comparison_text_differences",
        "version": 1,
        "pair_id": "pair-1",
        "algorithm": "production_scope_deterministic_text_differences_v1_5",
        "source_signature": "stage3-source-current",
        "sheet_groups": [{
            "id": "group-1",
            "left_pages": [7],
            "right_pages": [16],
            "relation_type": "MATCHED",
            "relation_status": "CONFIRMED",
            "changed": [changed],
            "removed": [],
            "added": [],
            "deterministic_same": [same],
        }],
    }
    atom = {
        "atom_id": "atom-1",
        "before_value": "220 В",
        "after_value": "380 В",
        "review_status": "REVIEW_REQUIRED" if review_required else "CONFIRMED",
        "provenance": {
            "locations": {
                "LEFT": [_anchor("left-change", 7, 0.2)],
                "RIGHT": [_anchor("right-change", 16, 0.22)],
            },
        },
    }
    text_artifact = {
        "kind": "stage_comparison_text_atoms",
        "schema_version": "text-atoms.v1",
        "version": 1,
        "input_signature": "atoms-current",
        "atoms": [atom],
        "provenance": {
            "stage3_signature": stage3_content_signature(differences),
        },
    }
    source_snapshot = {
        "run_id": "run-current",
        "generation_input_signature": "generation-current",
        "text": {
            "source_state": "VALID",
            "content_digest": content_signature(text_artifact),
            "artifact": text_artifact,
        },
    }
    state = {
        "status": "COMPLETED",
        "stale": stale,
        "run_id": "run-current",
        "input_signature": "generation-current",
        "revision": 11,
        "stages": {
            "text": {
                "status": "COMPLETED",
                "source_state": "VALID",
                # Deliberately differ from the projected list lengths: summary
                # is a state projection, never an artifact recount.
                "atoms": 17,
                "deltas": 9,
                "review_required": 4,
                "input_signature": "atoms-current",
                "preparation": {"fragments": 31},
                "deterministic_diff": {
                    "changed": 5,
                    "removed": 3,
                    "added": 1,
                    "source_signature": "stage3-source-current",
                },
                "text_atoms": {"input_signature": "atoms-current"},
            },
        },
    }
    target_id = "review-1" if review_required else "change-1"
    target = {
        "review_evidence_id" if review_required else "change_id": target_id,
        "atom_id": "atom-1",
        "source": "TEXT",
        "review_status": "REVIEW_REQUIRED" if review_required else "CONFIRMED",
        "evidence_refs": [{"source": "TEXT", "atom_id": "atom-1"}],
    }
    synthesis = {
        "changes": [] if review_required else [target],
        "review_items": [target] if review_required else [],
    }
    return state, source_snapshot, differences, synthesis


def test_projection_uses_exact_persisted_anchors_and_state_only_summary():
    state, snapshot, differences, synthesis = _artifacts(stale=True)

    payload = build_production_text_evidence(
        state=state,
        source_snapshot=snapshot,
        text_differences=differences,
        synthesis=synthesis,
        synthesis_input_signature="synthesis-current",
    )

    assert payload["available"] is True
    assert payload["stale"] is True
    assert payload["generation_run_id"] == "run-current"
    assert payload["generation_revision"] == 11
    assert payload["input_signature"] == "generation-current"
    assert payload["synthesis_input_signature"] == "synthesis-current"
    assert payload["summary"] == {
        "matched_fragments": None,
        "changed_fragments": 9,
        "changed": 5,
        "removed": 3,
        "added": 1,
        "review_required": 4,
        "prepared_fragments": 31,
        "text_atoms": 17,
    }
    assert payload["available_match_pairs"] == 1
    match = payload["matches"][0]
    assert match["sides"]["LEFT"][0]["fragment_id"] == "left-same"
    assert match["sides"]["RIGHT"][0]["fragment_id"] == "right-same"
    assert match["sides"]["LEFT"][0]["highlight"] == {
        "kind": "BBOX_SET",
        "bboxes": [{"x": 0.1, "y": 0.2, "width": 0.3, "height": 0.04}],
    }
    change = payload["changes"][0]
    assert change["evidence_id"] == "atom-1"
    assert change["target_id"] == "change-1"
    assert change["review_required"] is False
    assert change["sides"]["RIGHT"][0]["coordinate_space"] == (
        "NORMALIZED_PAGE_TOP_LEFT"
    )
    assert payload["constraints"]["read_only"] is True
    assert payload["constraints"]["matching_recomputed"] is False
    assert payload["constraints"]["exact_match_coverage"] == (
        "PERSISTED_DELTA_GROUPS_ONLY"
    )


def test_review_atom_maps_only_to_current_published_review_target():
    state, snapshot, differences, synthesis = _artifacts(review_required=True)

    payload = build_production_text_evidence(
        state=state,
        source_snapshot=snapshot,
        text_differences=differences,
        synthesis=synthesis,
        synthesis_input_signature="synthesis-current",
    )

    assert payload["changes"][0]["review_required"] is True
    assert payload["changes"][0]["review_status"] == "REVIEW_REQUIRED"
    assert payload["changes"][0]["target_id"] == "review-1"
    assert payload["changes"][0]["target_kind"] == "REVIEW_EVIDENCE"


def test_match_without_drawable_coordinates_on_both_sides_is_not_advertised():
    state, snapshot, differences, synthesis = _artifacts()
    same = differences["sheet_groups"][0]["deterministic_same"][0]
    same["right_anchors"][0]["bboxes"] = [
        {"x": 0.9, "y": 0.2, "width": 0.3, "height": 0.04}
    ]
    snapshot["text"]["artifact"]["provenance"]["stage3_signature"] = (
        stage3_content_signature(differences)
    )
    snapshot["text"]["content_digest"] = content_signature(
        snapshot["text"]["artifact"]
    )

    payload = build_production_text_evidence(
        state=state,
        source_snapshot=snapshot,
        text_differences=differences,
        synthesis=synthesis,
        synthesis_input_signature="synthesis-current",
    )

    assert payload["available_match_pairs"] == 0
    assert payload["matches"] == []
    assert payload["constraints"]["match_coordinates_required"] == (
        "BOTH_SIDES"
    )


def test_effective_target_status_controls_the_current_overlay_warning():
    state, snapshot, differences, synthesis = _artifacts(review_required=True)
    target = synthesis["review_items"].pop()
    target["change_id"] = "change-resolved"
    target["review_status"] = "CONFIRMED"
    synthesis["changes"] = [target]

    payload = build_production_text_evidence(
        state=state,
        source_snapshot=snapshot,
        text_differences=differences,
        synthesis=synthesis,
        synthesis_input_signature="synthesis-current",
    )

    change = payload["changes"][0]
    assert change["source_review_status"] == "REVIEW_REQUIRED"
    assert change["review_status"] == "CONFIRMED"
    assert change["review_required"] is False
    assert change["target_id"] == "change-resolved"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda state, snapshot, differences: snapshot.update(run_id="other-run"),
        lambda state, snapshot, differences: state["stages"]["text"].update(
            input_signature="other-atoms"
        ),
        lambda state, snapshot, differences: differences.update(
            source_signature="other-stage3"
        ),
        lambda state, snapshot, differences: snapshot["text"]["artifact"][
            "provenance"
        ].update(stage3_signature="other-stage3-content"),
    ],
)
def test_generation_mismatch_fails_closed(mutate):
    state, snapshot, differences, synthesis = _artifacts()
    mutate(state, snapshot, differences)

    with pytest.raises(ProductionTextEvidenceConflictError):
        build_production_text_evidence(
            state=state,
            source_snapshot=snapshot,
            text_differences=differences,
            synthesis=synthesis,
            synthesis_input_signature="synthesis-current",
        )


@pytest.mark.parametrize("status", ["NOT_STARTED", "RUNNING", "CHECK_BLOCKED"])
def test_unpublished_or_blocked_text_is_empty_without_artifact_reads(
    monkeypatch, status
):
    state, _snapshot, _differences, _synthesis = _artifacts()
    if status in {"NOT_STARTED", "RUNNING"}:
        state["status"] = status
    else:
        state["status"] = "PARTIAL"
        state["stages"]["text"]["status"] = "CHECK_BLOCKED"
        state["stages"]["text"]["source_state"] = "CHECK_BLOCKED"
    monkeypatch.setattr(
        production_orchestrator,
        "get_production_state",
        lambda *_: state,
    )
    monkeypatch.setattr(
        production_orchestrator,
        "_load_published_source_snapshot",
        lambda *_: (_ for _ in ()).throw(AssertionError("snapshot read")),
    )
    monkeypatch.setattr(
        production_orchestrator,
        "_published_synthesis",
        lambda *_: (_ for _ in ()).throw(AssertionError("synthesis read")),
    )
    monkeypatch.setattr(
        production_orchestrator.production_store,
        "load_artifact",
        lambda *_: (_ for _ in ()).throw(AssertionError("artifact read")),
    )

    payload = production_orchestrator.get_production_text_evidence(
        "session-1", "pair-1"
    )

    assert payload["available"] is False
    assert payload["matches"] == []
    assert payload["changes"] == []
    assert payload["stale"] is state["stale"]
    assert payload["generation_revision"] == state["revision"]
    assert payload["synthesis_input_signature"] is None


def test_get_projection_never_starts_a_producer_or_writes(monkeypatch):
    state, snapshot, differences, synthesis = _artifacts()
    reads = []
    monkeypatch.setattr(
        production_orchestrator,
        "get_production_state",
        lambda *_: copy.deepcopy(state),
    )
    monkeypatch.setattr(
        production_orchestrator,
        "_load_published_source_snapshot",
        lambda *_: copy.deepcopy(snapshot),
    )
    monkeypatch.setattr(
        production_orchestrator,
        "_published_synthesis",
        lambda *_: copy.deepcopy(synthesis),
    )
    monkeypatch.setattr(
        production_orchestrator,
        "canonical_synthesis_digest",
        lambda _synthesis: "synthesis-current",
    )

    def load(_session_id, _pair_id, name):
        reads.append(name)
        return copy.deepcopy(differences if name == "text_differences" else state)

    monkeypatch.setattr(
        production_orchestrator.production_store, "load_artifact", load
    )
    for name in ("save_artifact", "mutate_artifact"):
        monkeypatch.setattr(
            production_orchestrator.production_store,
            name,
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("GET attempted a write")
            ),
        )
    monkeypatch.setattr(
        production_orchestrator,
        "run_production_comparison",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("GET attempted to start a producer")
        ),
    )

    payload = production_orchestrator.get_production_text_evidence(
        "session-1", "pair-1"
    )

    assert payload["available"] is True
    assert payload["generation_revision"] == state["revision"]
    assert payload["synthesis_input_signature"] == "synthesis-current"
    assert reads == ["text_differences", "state"]


def test_get_projection_fails_closed_when_revision_changes_during_read(
    monkeypatch,
):
    state, snapshot, differences, synthesis = _artifacts()
    latest = copy.deepcopy(state)
    latest["revision"] += 1
    monkeypatch.setattr(
        production_orchestrator,
        "get_production_state",
        lambda *_: copy.deepcopy(state),
    )
    monkeypatch.setattr(
        production_orchestrator,
        "_load_published_source_snapshot",
        lambda *_: copy.deepcopy(snapshot),
    )
    monkeypatch.setattr(
        production_orchestrator,
        "_published_synthesis",
        lambda *_: copy.deepcopy(synthesis),
    )
    monkeypatch.setattr(
        production_orchestrator,
        "canonical_synthesis_digest",
        lambda _synthesis: "synthesis-current",
    )

    def load(_session_id, _pair_id, name):
        return copy.deepcopy(
            differences if name == "text_differences" else latest
        )

    monkeypatch.setattr(
        production_orchestrator.production_store, "load_artifact", load
    )

    with pytest.raises(
        production_orchestrator.ProductionStateConflictError,
        match="generation changed",
    ):
        production_orchestrator.get_production_text_evidence(
            "session-1", "pair-1"
        )


def test_text_evidence_api_is_thin_read_only_and_conflicts_are_409(monkeypatch):
    calls = []
    monkeypatch.setattr(
        router_mod.production,
        "run_production_comparison",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("GET attempted to start a producer")
        ),
    )
    monkeypatch.setattr(
        router_mod.production,
        "get_production_text_evidence",
        lambda session_id, pair_id: calls.append((session_id, pair_id)) or {
            "kind": "stage_comparison_production_text_evidence",
            "available": False,
            "matches": [],
            "changes": [],
        },
    )

    response = _client().get(f"{BASE}/text-evidence")

    assert response.status_code == 200
    assert response.json()["available"] is False
    assert calls == [("session-1", "pair-1")]

    monkeypatch.setattr(
        router_mod.production,
        "get_production_text_evidence",
        lambda *_: (_ for _ in ()).throw(
            production_orchestrator.ProductionStateConflictError(
                "Stage 3 generation mismatch"
            )
        ),
    )
    conflict = _client().get(f"{BASE}/text-evidence")
    assert conflict.status_code == 409
    assert "generation mismatch" in conflict.json()["detail"]
