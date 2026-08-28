"""One change, one human action: Stage 5 exceptions vs Stage 7 review."""
from __future__ import annotations

import pytest

from backend.app.services.stage_comparison.engineer_review import (
    build_engineer_decisions,
    build_final_report,
    review_rows,
)
from backend.app.services.stage_comparison.review_presentation import (
    is_presentable_review_item,
    review_finding_presentation,
)
from backend.app.services.stage_comparison.review_queue import (
    build_review_queue,
    mint_project_entity_ref,
)
from backend.app.services.stage_comparison.unified_change_synthesizer import (
    synthesize_unified_changes,
)


def _atom(
    atom_id: str = "atom_1",
    *,
    before: str | None = "EI 60",
    after: str | None = "EI 90",
    locations: dict | None = None,
) -> dict:
    """An unresolved TEXT atom: a proven value change with no known dimension."""
    return {
        "atom_id": atom_id,
        "source": "TEXT",
        "scope_ref": "text_scope_1",
        "subject_ref": None,
        "project_entity_ref": None,
        "facet_ref": None,
        "dimension": "UNKNOWN_DIMENSION",
        "direction": "ALTERED",
        "outcome": "REVIEW_REQUIRED",
        "confidence": "UNKNOWN",
        "before_value": before,
        "after_value": after,
        "evidence_ref": f"evidence:{atom_id}",
        "source_artifact": {
            "kind": "stage_comparison_text_atoms",
            "schema_version": "text-atoms.v1",
            "artifact_ref": "artifact:text",
        },
        "provenance": {
            "producer": "test",
            "locations": locations
            if locations is not None
            else {
                "LEFT": [{"page": 29, "fragment_id": "txt_l", "bboxes": []}],
                "RIGHT": [{"page": 8, "fragment_id": "txt_r", "bboxes": []}],
            },
        },
    }


def _synthesis(*atoms: dict) -> dict:
    return synthesize_unified_changes(text_atoms=list(atoms) or [_atom()])


def _review_id(synthesis: dict, index: int = 0) -> str:
    return synthesis["review_items"][index]["review_evidence_id"]


# --- A / B: a review item is a Stage 7 row, not a Stage 5 question ----------


def test_a_presentable_review_item_asks_the_engineer_nothing():
    queue = build_review_queue(synthesis=_synthesis(), generated_at="fixed")

    assert queue["counts"]["CHANGE"] == 0
    diagnostics = queue["diagnostics"]
    assert diagnostics["suppressed_change_questions"] == 1
    assert diagnostics["engineer_review_targets_suppressed"] == 1
    assert diagnostics["suppressed_change_question_reasons"] == {
        "presentable_in_engineer_review": 1
    }


def test_the_same_review_item_is_a_row_in_engineer_review():
    synthesis = _synthesis()

    rows = review_rows(synthesis, None)

    assert [row["target_id"] for row in rows] == [_review_id(synthesis)]
    row = rows[0]
    assert row["target_kind"] == "REVIEW_EVIDENCE"
    assert row["change"]["before_value"] == "EI 60"
    assert row["change"]["after_value"] == "EI 90"
    assert row["change"]["dimension"] == "UNKNOWN_DIMENSION"
    assert row["presentation"]["presentable"] is True
    assert row["presentation"]["left_pages"] == [29]
    assert row["presentation"]["right_pages"] == [8]
    assert row["engineer_decision"]["decision"] == "PENDING_REVIEW"


# --- C: one finding, one engineer decision ---------------------------------


def test_one_finding_costs_the_engineer_exactly_one_decision():
    synthesis = _synthesis()
    queue = build_review_queue(synthesis=synthesis, generated_at="fixed")
    decisions = build_engineer_decisions(synthesis, generated_at="fixed")

    question_targets = {
        question["context"].get("review_evidence_id")
        for question in queue["questions"]
        if question["category"] == "CHANGE"
    }
    decision_targets = {row["target_id"] for row in decisions["decisions"]}

    assert decision_targets == {_review_id(synthesis)}
    assert question_targets & decision_targets == set()


def test_an_engineer_can_approve_a_finding_whose_dimension_stayed_unknown():
    synthesis = _synthesis()

    decisions = build_engineer_decisions(
        synthesis,
        updates=[{
            "target_id": _review_id(synthesis),
            "decision": "APPROVED",
            "author": "Андрей Иванович",
        }],
        generated_at="fixed",
    )

    row = decisions["decisions"][0]
    assert row["decision"] == "APPROVED"
    assert row["presentable"] is True
    assert decisions["counts"]["APPROVED"] == 1


def test_an_approved_finding_reaches_the_report_and_a_pending_one_does_not():
    synthesis = _synthesis(
        _atom("atom_1"), _atom("atom_2", before="R 45", after="R 90"),
    )
    approved_id = _review_id(synthesis)
    decisions = build_engineer_decisions(
        synthesis,
        updates=[{
            "target_id": approved_id,
            "decision": "APPROVED",
            "author": "Андрей Иванович",
        }],
        generated_at="fixed",
    )

    report = build_final_report(synthesis, decisions, generated_at="fixed")

    assert len(synthesis["review_items"]) == 2
    assert [
        item["review_evidence_id"] for item in report["approved_review_findings"]
    ] == [approved_id]
    assert report["approved_review_findings"][0]["left_pages"] == [29]
    assert report["constraints"]["approved_only"] is True
    assert report["constraints"]["pending_included"] is False
    # The atomic-change contract is untouched.
    assert report["approved_atomic_changes"] == []
    assert report["summary"] == {"approved": 0, "approved_review_findings": 1}


def test_a_rejected_finding_stays_out_of_the_report():
    synthesis = _synthesis()
    decisions = build_engineer_decisions(
        synthesis,
        updates=[{
            "target_id": _review_id(synthesis),
            "decision": "REJECTED",
            "author": "Андрей Иванович",
        }],
        generated_at="fixed",
    )

    report = build_final_report(synthesis, decisions, generated_at="fixed")

    assert report["approved_review_findings"] == []


# --- E: a genuine exception still reaches Stage 5 ---------------------------


def test_a_finding_with_nothing_to_show_is_still_an_exception():
    synthesis = _synthesis(_atom(locations={"LEFT": [], "RIGHT": []}))

    queue = build_review_queue(synthesis=synthesis, generated_at="fixed")

    assert queue["counts"]["CHANGE"] == 1
    item = synthesis["review_items"][0]
    assert is_presentable_review_item(item) is False
    assert review_finding_presentation(item)["missing_for_presentation"] == ["location"]


def test_a_finding_that_cannot_be_shown_cannot_be_approved_either():
    synthesis = _synthesis(_atom(locations={"LEFT": [], "RIGHT": []}))

    with pytest.raises(ValueError, match="resolved into an atomic change"):
        build_engineer_decisions(
            synthesis,
            updates=[{
                "target_id": _review_id(synthesis),
                "decision": "APPROVED",
                "author": "Андрей Иванович",
            }],
            generated_at="fixed",
        )


# --- F / G: what the engineer is actually asked ----------------------------


def test_the_remaining_question_describes_the_change_not_its_identifier():
    synthesis = _synthesis(_atom(locations={"LEFT": [], "RIGHT": []}))

    queue = build_review_queue(synthesis=synthesis, generated_at="fixed")
    prompt = queue["questions"][0]["prompt"]

    assert "ureview_" not in prompt
    assert "UNKNOWN_DIMENSION" not in prompt
    assert "EI 60" in prompt and "EI 90" in prompt


def test_the_engineer_is_asked_for_an_object_name_not_for_a_stable_id():
    synthesis = _synthesis(_atom(locations={"LEFT": [], "RIGHT": []}))

    queue = build_review_queue(synthesis=synthesis, generated_at="fixed")
    contract = queue["questions"][0]["context"]["typed_resolution_contract"]

    assert "project_entity_ref" not in contract["required_fields"]
    assert "object_label" in contract["required_fields"]


def test_the_backend_mints_the_entity_ref_from_the_human_label():
    minted = mint_project_entity_ref("Помещение 24.5")
    again = mint_project_entity_ref("  помещение   24.5 ")

    assert minted == again
    assert minted["project_entity_ref"].startswith("project_text_entity_")
    assert minted["subject_ref"] == "text_entity:помещение 24.5"
    assert mint_project_entity_ref("Кровля К5") != minted

    with pytest.raises(ValueError):
        mint_project_entity_ref("   ")


# --- H: the report is still approved-only ----------------------------------


def test_the_report_never_carries_a_finding_the_engineer_did_not_approve():
    synthesis = _synthesis(
        _atom("atom_1"), _atom("atom_2", before="R 45", after="R 90"),
    )
    decisions = build_engineer_decisions(synthesis, generated_at="fixed")

    report = build_final_report(synthesis, decisions, generated_at="fixed")

    assert report["approved_atomic_changes"] == []
    assert report["approved_review_findings"] == []
    assert report["constraints"] == {
        "approved_only": True,
        "pending_included": False,
        "rejected_included": False,
        "uses_llm_summary": False,
    }


def test_a_changed_finding_invalidates_the_approval_it_carried():
    synthesis = _synthesis()
    approved = build_engineer_decisions(
        synthesis,
        updates=[{
            "target_id": _review_id(synthesis),
            "decision": "APPROVED",
            "author": "Андрей Иванович",
        }],
        generated_at="fixed",
    )
    moved = _synthesis(_atom(after="EI 120"))

    report = build_final_report(moved, approved, generated_at="fixed")

    assert report["approved_review_findings"] == []


# --- D: an entity question only where the identity is genuinely open --------


def _entity_relations(*relations: dict) -> dict:
    return {
        "kind": "stage_comparison_entity_relations",
        "relations": list(relations),
    }


def _entity(left: str, right: str, relation_id: str) -> dict:
    return {
        "relation_id": relation_id,
        "left_entity_ref": left,
        "right_entity_ref": right,
        "relation": "POSSIBLE_ENTITY",
        "review_required": True,
        "confidence": "MEDIUM",
        "score": 0.75,
    }


def test_an_object_is_not_asked_to_confirm_it_is_itself():
    queue = build_review_queue(
        entity_relations=_entity_relations(
            _entity("text_entity:24_5", "text_entity:24_5", "erel_same"),
        ),
        generated_at="fixed",
    )

    assert queue["counts"]["ENTITY"] == 0
    assert queue["diagnostics"]["suppressed_entity_questions"] == 1
    assert queue["diagnostics"]["suppressed_entity_question_reasons"] == {
        "identical_entity_designation": 1
    }


def test_two_different_designations_are_still_a_real_question():
    queue = build_review_queue(
        entity_relations=_entity_relations(
            _entity("text_entity:24_5", "text_entity:24_6", "erel_moved"),
        ),
        generated_at="fixed",
    )

    assert queue["counts"]["ENTITY"] == 1
    prompt = queue["questions"][0]["prompt"]
    assert "text_entity:" not in prompt
    assert "«24.5»" in prompt and "«24.6»" in prompt


def test_a_choice_between_candidates_names_them_in_the_options():
    queue = build_review_queue(
        entity_relations=_entity_relations(
            _entity("text_entity:24_5", "text_entity:24_6", "erel_a"),
            _entity("text_entity:24_5", "text_entity:24_7", "erel_b"),
        ),
        generated_at="fixed",
    )

    question = queue["questions"][0]
    assert question["question_type"] == "ENTITY_CANDIDATE_SELECTION"
    assert "text_entity:" not in question["prompt"]
    labels = [option["label"] for option in question["answer_options"]]
    assert "«24.6» справа" in labels and "«24.7» справа" in labels
