from __future__ import annotations

import copy
import json
import os
import subprocess
import sys

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routers import stage_comparison as router_mod
from backend.app.services.stage_comparison import production_orchestrator
from backend.app.services.stage_comparison.production_artifacts import (
    content_signature,
)
from backend.app.services.stage_comparison.production_text_evidence import (
    MATCH_EVIDENCE_LEGACY,
    MATCH_EVIDENCE_VERIFIED,
    ProductionTextEvidenceConflictError,
    build_production_text_evidence,
    empty_production_text_evidence,
)
from backend.app.services.stage_comparison.text_semantic_validation import (
    STAGE3_FULL_DIGEST_VERSION,
    stage3_content_signature,
    stage3_full_content_signature,
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


def _atom_locations() -> dict:
    return {
        "LEFT": [_anchor("left-change", 7, 0.2)],
        "RIGHT": [_anchor("right-change", 16, 0.22)],
    }


def _atom_provenance() -> dict:
    return {"producer": "text-atom-builder-v1", "locations": _atom_locations()}


def _change(
    *,
    change_id: str = "change-1",
    before: str = "220 В",
    after: str = "380 В",
    review_status: str = "CONFIRMED",
    atom_id: str = "atom-1",
) -> dict:
    return {
        "change_id": change_id,
        "source_mode": "TEXT",
        "outcome": "ALTERED",
        "before_value": before,
        "after_value": after,
        "review_status": review_status,
        "evidence_refs": [{"source": "TEXT", "atom_id": atom_id}],
        "provenance": {
            "source_atoms": [
                {
                    "atom_id": atom_id,
                    "source": "TEXT",
                    "provenance": _atom_provenance(),
                }
            ],
            "synthesis": "UNION_SINGLE_SOURCE",
        },
    }


def _review_item(
    *,
    review_id: str = "review-1",
    atom_id: str = "atom-1",
    before: str = "220 В",
    after: str = "380 В",
) -> dict:
    return {
        "review_evidence_id": review_id,
        "atom_id": atom_id,
        "source": "TEXT",
        "outcome": "REVIEW_REQUIRED",
        "review_status": "REVIEW_REQUIRED",
        "before_value": before,
        "after_value": after,
        "evidence_refs": [{"source": "TEXT", "atom_id": atom_id}],
        "provenance": {
            "source_atom": _atom_provenance(),
            "source_atom_outcome": "REVIEW_REQUIRED",
            "synthesis": "REVIEW_EVIDENCE_PRESERVED",
        },
    }


def _seal(snapshot: dict, differences: dict, *, legacy: bool = False) -> None:
    """Re-bind the frozen snapshot to the (possibly edited) Stage 3 payload."""
    provenance = snapshot["text"]["artifact"]["provenance"]
    provenance["stage3_signature"] = stage3_content_signature(differences)
    if legacy:
        provenance.pop("stage3_full_signature", None)
        provenance.pop("stage3_full_signature_version", None)
    else:
        provenance["stage3_full_signature"] = stage3_full_content_signature(
            differences
        )
        provenance["stage3_full_signature_version"] = STAGE3_FULL_DIGEST_VERSION
    snapshot["text"]["content_digest"] = content_signature(
        snapshot["text"]["artifact"]
    )


def _artifacts(
    *,
    stale: bool = False,
    review_required: bool = False,
    legacy_same_signature: bool = False,
):
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
        "provenance": _atom_provenance(),
    }
    text_artifact = {
        "kind": "stage_comparison_text_atoms",
        "schema_version": "text-atoms.v1",
        "version": 1,
        "input_signature": "atoms-current",
        "atoms": [atom],
        "provenance": {},
    }
    source_snapshot = {
        "run_id": "run-current",
        "generation_input_signature": "generation-current",
        "text": {
            "source_state": "VALID",
            "content_digest": None,
            "artifact": text_artifact,
        },
    }
    _seal(source_snapshot, differences, legacy=legacy_same_signature)
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
    synthesis = {
        "changes": [] if review_required else [_change()],
        "review_items": [_review_item()] if review_required else [],
    }
    return state, source_snapshot, differences, synthesis


def _build(state, snapshot, differences, synthesis):
    return build_production_text_evidence(
        state=state,
        source_snapshot=snapshot,
        text_differences=differences,
        synthesis=synthesis,
        synthesis_input_signature="synthesis-current",
    )


def test_projection_uses_exact_persisted_anchors_and_state_only_summary():
    state, snapshot, differences, synthesis = _artifacts(stale=True)

    payload = _build(state, snapshot, differences, synthesis)

    assert payload["available"] is True
    assert payload["stale"] is True
    assert payload["text_result_state"] == "PUBLISHED"
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
    assert payload["match_evidence_state"] == MATCH_EVIDENCE_VERIFIED
    assert payload["available_match_pairs"] == 1
    match = payload["matches"][0]
    assert match["sides"]["LEFT"][0]["fragment_id"] == "left-same"
    assert match["sides"]["RIGHT"][0]["fragment_id"] == "right-same"
    assert match["sides"]["LEFT"][0]["highlight"] == {
        "kind": "BBOX_SET",
        "bboxes": [{"x": 0.1, "y": 0.2, "width": 0.3, "height": 0.04}],
    }
    change = payload["changes"][0]
    assert change["evidence_id"] == "change-1"
    assert change["target_id"] == "change-1"
    assert change["target_kind"] == "CHANGE"
    assert change["review_required"] is False
    assert change["atom_ids"] == ["atom-1"]
    assert change["sides"]["RIGHT"][0]["coordinate_space"] == (
        "NORMALIZED_PAGE_TOP_LEFT"
    )
    assert payload["change_items"] == 1
    assert payload["available_change_items"] == 1
    assert payload["constraints"]["read_only"] is True
    assert payload["constraints"]["matching_recomputed"] is False
    assert payload["constraints"]["changes_source"] == (
        "effective_unified_synthesis"
    )
    assert payload["constraints"]["exact_match_coverage"] == (
        "PERSISTED_DELTA_GROUPS_ONLY"
    )


def test_review_atom_maps_only_to_current_published_review_target():
    state, snapshot, differences, synthesis = _artifacts(review_required=True)

    payload = _build(state, snapshot, differences, synthesis)

    change = payload["changes"][0]
    assert change["review_required"] is True
    assert change["review_status"] == "REVIEW_REQUIRED"
    assert change["evidence_id"] == "review-1"
    assert change["target_id"] == "review-1"
    assert change["target_kind"] == "REVIEW_EVIDENCE"


def test_match_without_drawable_coordinates_on_both_sides_is_not_advertised():
    state, snapshot, differences, synthesis = _artifacts()
    same = differences["sheet_groups"][0]["deterministic_same"][0]
    same["right_anchors"][0]["bboxes"] = [
        {"x": 0.9, "y": 0.2, "width": 0.3, "height": 0.04}
    ]
    _seal(snapshot, differences)

    payload = _build(state, snapshot, differences, synthesis)

    assert payload["available_match_pairs"] == 0
    assert payload["matches"] == []
    assert payload["constraints"]["match_coordinates_required"] == (
        "BOTH_SIDES"
    )


def test_match_sides_keep_their_own_document_anchors():
    state, snapshot, differences, synthesis = _artifacts()

    match = _build(state, snapshot, differences, synthesis)["matches"][0]

    assert [item["document_ref"] for item in match["sides"]["LEFT"]] == ["LEFT"]
    assert [item["document_ref"] for item in match["sides"]["RIGHT"]] == ["RIGHT"]
    assert match["sides"]["LEFT"][0]["page"] == 7
    assert match["sides"]["RIGHT"][0]["page"] == 16
    assert match["review_status"] == "CONFIRMED"


# --- Human review regression scenarios (source of truth) --------------------


def test_corrected_values_come_from_the_effective_synthesis():
    """A: automatic 220 → 380, resolved to 220 → 400."""
    state, snapshot, differences, synthesis = _artifacts()
    synthesis["changes"] = [_change(after="400 В")]

    change = _build(state, snapshot, differences, synthesis)["changes"][0]

    assert (change["before"], change["after"]) == ("220 В", "400 В")
    assert change["title"] == "220 В → 400 В"
    # The frozen snapshot still says 380 В and is used for coordinates only.
    assert snapshot["text"]["artifact"]["atoms"][0]["after_value"] == "380 В"
    assert change["sides"]["LEFT"][0]["page"] == 7
    assert change["sides"]["RIGHT"][0]["page"] == 16


def test_rejected_atom_is_absent_from_the_current_projection():
    """B: the atom exists in the snapshot but has no current target."""
    state, snapshot, differences, synthesis = _artifacts()
    synthesis["changes"] = []

    payload = _build(state, snapshot, differences, synthesis)

    assert payload["changes"] == []
    assert payload["change_items"] == 0
    assert payload["available_change_items"] == 0
    assert snapshot["text"]["artifact"]["atoms"][0]["atom_id"] == "atom-1"


def test_review_required_target_keeps_its_warning_state():
    """C: a still-unresolved item is never presented as confirmed."""
    state, snapshot, differences, synthesis = _artifacts(review_required=True)

    change = _build(state, snapshot, differences, synthesis)["changes"][0]

    assert change["review_required"] is True
    assert change["review_status"] == "REVIEW_REQUIRED"
    assert change["outcome"] == "REVIEW_REQUIRED"
    assert change["target_kind"] == "REVIEW_EVIDENCE"


def test_only_the_selected_contested_change_survives_projection():
    """D: an unselected contested change no longer has a target."""
    state, snapshot, differences, synthesis = _artifacts()
    synthesis["changes"] = [_change(change_id="change-kept")]

    payload = _build(state, snapshot, differences, synthesis)

    assert [item["target_id"] for item in payload["changes"]] == ["change-kept"]
    assert all(item["target_id"] for item in payload["changes"])


def test_effective_synthesis_wins_over_a_stale_source_snapshot():
    """E: snapshot status REVIEW_REQUIRED, effective status CONFIRMED."""
    state, snapshot, differences, synthesis = _artifacts(review_required=True)
    synthesis["review_items"] = []
    synthesis["changes"] = [_change(change_id="change-resolved", after="400 В")]

    change = _build(state, snapshot, differences, synthesis)["changes"][0]

    assert change["source_review_status"] == "REVIEW_REQUIRED"
    assert change["review_status"] == "CONFIRMED"
    assert change["review_required"] is False
    assert change["target_id"] == "change-resolved"
    assert change["after"] == "400 В"


def test_graphic_only_change_is_not_text_viewer_evidence():
    state, snapshot, differences, synthesis = _artifacts()
    graphic = _change(change_id="change-graphic")
    graphic["source_mode"] = "GRAPHIC"
    graphic["evidence_refs"] = [{"source": "GRAPHIC", "atom_id": "gatom-1"}]
    graphic["provenance"]["source_atoms"] = [
        {"atom_id": "gatom-1", "source": "GRAPHIC", "provenance": {}}
    ]
    synthesis["changes"].append(graphic)

    payload = _build(state, snapshot, differences, synthesis)

    assert [item["target_id"] for item in payload["changes"]] == ["change-1"]


def test_change_without_persisted_coordinates_is_honest_about_it():
    state, snapshot, differences, synthesis = _artifacts()
    change = _change()
    change["provenance"]["source_atoms"][0]["provenance"] = {
        "locations": {"LEFT": [], "RIGHT": []}
    }
    synthesis["changes"] = [change]
    snapshot["text"]["artifact"]["atoms"][0]["provenance"] = {
        "locations": {"LEFT": [], "RIGHT": []}
    }
    _seal(snapshot, differences)

    payload = _build(state, snapshot, differences, synthesis)

    projected = payload["changes"][0]
    assert projected["sides"] == {"LEFT": [], "RIGHT": []}
    assert projected["coordinates_available"] is False
    assert payload["change_items"] == 1
    assert payload["available_change_items"] == 0


def test_snapshot_provides_coordinates_when_synthesis_carries_none():
    state, snapshot, differences, synthesis = _artifacts()
    change = _change()
    change["provenance"] = {"synthesis": "UNION_SINGLE_SOURCE"}
    synthesis["changes"] = [change]

    projected = _build(state, snapshot, differences, synthesis)["changes"][0]

    assert projected["sides"]["LEFT"][0]["fragment_id"] == "left-change"
    assert projected["sides"]["RIGHT"][0]["fragment_id"] == "right-change"


# --- deterministic_same stale / tamper protection ---------------------------


def test_valid_deterministic_same_passes_projection():
    """A: an untouched exact pair projects normally."""
    state, snapshot, differences, synthesis = _artifacts()

    payload = _build(state, snapshot, differences, synthesis)

    assert payload["match_evidence_state"] == MATCH_EVIDENCE_VERIFIED
    assert payload["available_match_pairs"] == 1


@pytest.mark.parametrize(
    "tamper",
    [
        pytest.param(
            lambda same: same.update(after="Подменённый заголовок"),
            id="same_text",
        ),
        pytest.param(
            lambda same: same.update(before="Подменённый исходный текст"),
            id="same_before_text",
        ),
        pytest.param(
            lambda same: same.update(right_pages=[17]),
            id="page",
        ),
        pytest.param(
            lambda same: same["left_anchors"][0].update(page=9),
            id="anchor_page",
        ),
        pytest.param(
            lambda same: same["right_anchors"][0]["bboxes"][0].update(x=0.55),
            id="anchor_bbox",
        ),
        pytest.param(
            lambda same: same["left_anchors"][0].update(fragment_id="other"),
            id="anchor_fragment_identity",
        ),
    ],
)
def test_tampered_deterministic_same_is_refused_under_the_old_signature(tamper):
    """B/C/D: content edits kept under a stale signature fail closed."""
    state, snapshot, differences, synthesis = _artifacts()
    tamper(differences["sheet_groups"][0]["deterministic_same"][0])
    # Only the Stage 4 semantic binding is refreshed, exactly as a tamper
    # attempt would do before this projection learned to cover exact pairs.
    snapshot["text"]["artifact"]["provenance"]["stage3_signature"] = (
        stage3_content_signature(differences)
    )
    snapshot["text"]["content_digest"] = content_signature(
        snapshot["text"]["artifact"]
    )

    with pytest.raises(
        ProductionTextEvidenceConflictError, match="exact pairs"
    ):
        _build(state, snapshot, differences, synthesis)


def test_json_key_order_does_not_move_the_stage3_signature():
    """E: reordering keys without changing content keeps the digest."""
    _state, _snapshot, differences, _synthesis = _artifacts()
    reordered = json.loads(
        json.dumps(differences),
        object_pairs_hook=lambda pairs: dict(reversed(pairs)),
    )

    assert list(reordered) != list(differences)
    assert stage3_full_content_signature(reordered) == (
        stage3_full_content_signature(differences)
    )


def test_stage3_signature_is_stable_across_python_hash_seeds():
    """F: the digest never depends on PYTHONHASHSEED."""
    _state, _snapshot, differences, _synthesis = _artifacts()
    program = (
        "import json, sys;"
        "from backend.app.services.stage_comparison.text_semantic_validation"
        " import stage3_full_content_signature;"
        "print(stage3_full_content_signature(json.loads(sys.argv[1])))"
    )
    digests = set()
    for seed in ("0", "1", "12345"):
        env = {**os.environ, "PYTHONHASHSEED": seed}
        digests.add(subprocess.run(
            [sys.executable, "-c", program, json.dumps(differences)],
            capture_output=True, text=True, check=True, env=env,
        ).stdout.strip())

    assert digests == {stage3_full_content_signature(differences)}


def test_semantic_signature_still_ignores_deterministic_same():
    """The Stage 4 binding keeps its own, unchanged v1 contract."""
    _state, _snapshot, differences, _synthesis = _artifacts()
    before = stage3_content_signature(differences)
    differences["sheet_groups"][0]["deterministic_same"][0]["after"] = "иной"

    assert stage3_content_signature(differences) == before
    assert stage3_full_content_signature(differences) != before


def test_legacy_generation_reports_unknown_matches_instead_of_zero():
    state, snapshot, differences, synthesis = _artifacts(
        legacy_same_signature=True
    )

    payload = _build(state, snapshot, differences, synthesis)

    assert payload["available"] is True
    assert payload["match_evidence_state"] == MATCH_EVIDENCE_LEGACY
    assert payload["matches"] == []
    assert payload["available_match_pairs"] is None
    # Changes stay fully available: they were covered by the old signature.
    assert payload["changes"][0]["target_id"] == "change-1"


def test_unsupported_same_signature_version_fails_closed():
    state, snapshot, differences, synthesis = _artifacts()
    snapshot["text"]["artifact"]["provenance"][
        "stage3_full_signature_version"
    ] = "stage3-content-v99"
    snapshot["text"]["content_digest"] = content_signature(
        snapshot["text"]["artifact"]
    )

    with pytest.raises(
        ProductionTextEvidenceConflictError, match="unsupported version"
    ):
        _build(state, snapshot, differences, synthesis)


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
        _build(state, snapshot, differences, synthesis)


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
    assert payload["text_result_state"] == (
        "BLOCKED" if status == "CHECK_BLOCKED" else "NOT_PRODUCED"
    )
    # Unknown, never a checked zero.
    assert payload["available_match_pairs"] is None
    assert payload["change_items"] is None
    assert set(payload["summary"].values()) == {None}


def test_blocked_text_publishes_its_reason_instead_of_zero_counters():
    state, _snapshot, _differences, _synthesis = _artifacts()
    state["status"] = "PARTIAL"
    state["stages"]["text"] = {
        "status": "CHECK_BLOCKED",
        "source_state": "CHECK_BLOCKED",
        "atoms": 0,
        "deltas": 0,
        "review_required": 0,
        "reason_code": "TEXT_SOURCE_MISSING",
        "error_type": "FileNotFoundError",
    }

    payload = empty_production_text_evidence(state)

    assert payload["text_result_state"] == "BLOCKED"
    assert payload["text_blocked_reason"] == "TEXT_SOURCE_MISSING"
    assert payload["text_blocked_error"] == "FileNotFoundError"
    # The stage wrote zeros for an aborted run; they must not surface as
    # "checked and found nothing".
    assert payload["summary"]["text_atoms"] is None
    assert payload["summary"]["changed_fragments"] is None
    assert payload["summary"]["review_required"] is None
    assert payload["available_match_pairs"] is None
    assert payload["match_evidence_state"] == "UNKNOWN"


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
