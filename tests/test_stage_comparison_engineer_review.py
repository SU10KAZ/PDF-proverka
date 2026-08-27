from __future__ import annotations

import pytest

from backend.app.services.stage_comparison.engineer_review import (
    build_engineer_decisions,
    build_final_report,
    review_rows,
)
from backend.app.services.stage_comparison.unified_change_synthesizer import (
    synthesize_unified_changes,
)


def _atom(atom_id, facet, before, after, artifact_ref="artifact:text"):
    return {
        "atom_id": atom_id,
        "source": "TEXT",
        "scope_ref": "scope-1",
        "subject_ref": "equipment:panel-1",
        "project_entity_ref": "project:panel-1",
        "dimension": "PARAMETER",
        "direction": "ALTERED",
        "outcome": "MATERIAL_CHANGE",
        "confidence": "HIGH",
        "evidence_ref": f"evidence:{atom_id}",
        "source_artifact": {
            "kind": "stage_comparison_text_atoms",
            "schema_version": "text-atoms.v1",
            "artifact_ref": artifact_ref,
        },
        "provenance": {"producer": "test"},
        "facet_ref": facet,
        "before_value": before,
        "after_value": after,
    }


def _synthesis(artifact_ref="artifact:text"):
    return synthesize_unified_changes(text_atoms=[
        _atom("voltage", "voltage", "220 В", "380 В", artifact_ref),
        _atom("temperature", "temperature_range", "-10…+40", "-25…+50", artifact_ref),
    ])


def test_approved_only_enters_final_report():
    synthesis = _synthesis()
    ids = [change["change_id"] for change in synthesis["changes"]]
    state = build_engineer_decisions(synthesis, updates=[
        {"target_id": ids[0], "decision": "APPROVED", "author": "engineer", "comment": "ok"},
        {"target_id": ids[1], "decision": "REJECTED", "author": "engineer", "reason_code": "not_a_change"},
    ], generated_at="fixed")

    report = build_final_report(synthesis, state, generated_at="fixed")

    assert [item["change_id"] for item in report["approved_atomic_changes"]] == [ids[0]]
    assert report["constraints"]["rejected_included"] is False
    assert state["counts"] == {"APPROVED": 1, "PENDING_REVIEW": 0, "REJECTED": 1}


def test_pending_is_not_in_final_report_and_rejected_stays_feedback():
    synthesis = _synthesis()
    target = synthesis["changes"][0]["change_id"]
    initial = build_engineer_decisions(synthesis, generated_at="t1")
    rejected = build_engineer_decisions(synthesis, existing=initial, updates=[{
        "target_id": target,
        "decision": "REJECTED",
        "author": "engineer",
        "comment": "OCR artifact",
    }], generated_at="t2")

    report = build_final_report(synthesis, rejected)

    assert report["approved_atomic_changes"] == []
    assert any(row["target_id"] == target for row in rejected["decisions"])
    assert rejected["decisions"][0]["finding_snapshot"]
    assert rejected["history"]  # pending state is retained, not deleted


def test_review_evidence_cannot_be_approved_as_an_atomic_change():
    unresolved = _atom("unresolved", "voltage", "220 В", "380 В")
    unresolved["project_entity_ref"] = None
    synthesis = synthesize_unified_changes(text_atoms=[unresolved])
    target_id = synthesis["review_items"][0]["review_evidence_id"]

    with pytest.raises(ValueError, match="resolved into an atomic change"):
        build_engineer_decisions(
            synthesis,
            updates=[{
                "target_id": target_id,
                "decision": "APPROVED",
                "author": "engineer",
            }],
            generated_at="fixed",
        )


def test_presentation_group_does_not_replace_row_decisions():
    synthesis = _synthesis()
    rows = review_rows(synthesis, None)

    assert len(synthesis["presentation_groups"]) == 1
    assert len(rows) == 2
    assert len({row["target_id"] for row in rows}) == 2
    assert len({row["presentation_group_id"] for row in rows}) == 1
    assert all(row["engineer_decision"]["decision"] == "PENDING_REVIEW" for row in rows)


def test_changed_input_marks_old_decision_stale_and_pending():
    synthesis = _synthesis()
    target = synthesis["changes"][0]["change_id"]
    approved = build_engineer_decisions(synthesis, updates=[{
        "target_id": target, "decision": "APPROVED", "author": "engineer",
    }])
    changed = _synthesis("artifact:text:v2")

    refreshed = build_engineer_decisions(changed, existing=approved)

    assert next(row for row in refreshed["decisions"] if row["target_id"] == target)["decision"] == "PENDING_REVIEW"
    assert any(row.get("stale") is True for row in refreshed["history"] if row["target_id"] == target)


def test_identical_retry_is_idempotent_for_row_revision():
    synthesis = _synthesis()
    target = synthesis["changes"][0]["change_id"]
    update = {"target_id": target, "decision": "APPROVED", "author": "engineer"}
    first = build_engineer_decisions(synthesis, updates=[update], generated_at="t1")
    second = build_engineer_decisions(
        synthesis, existing=first, updates=[update], generated_at="t2",
    )

    first_row = next(row for row in first["decisions"] if row["target_id"] == target)
    second_row = next(row for row in second["decisions"] if row["target_id"] == target)
    assert second_row["revision"] == first_row["revision"]
    assert second_row["updated_at"] == first_row["updated_at"]
