"""Atomic audit stays intact while the engineer sees only real questions."""
from __future__ import annotations

from copy import deepcopy

import pytest

from backend.app.services.stage_comparison.ai_v2 import inventory
from backend.app.services.stage_comparison.ai_v2.materialization import (
    _annotate_relation_atoms,
    _identity_owned_finding_ids,
)
from backend.app.services.stage_comparison.human_review_orchestrator import (
    ACTIONABLE_ENGINEERING_DECISION,
    DOCUMENT_METADATA_CHANGE,
    MISSING_EVIDENCE,
    PROVEN_CHANGE,
    ROOT_CAUSE_GROUP_MEMBER,
    TEXT_REQUIREMENT_CHANGE,
    build_human_review_plan,
    materialize_group_decision,
)
from backend.app.services.stage_comparison.preliminary_report import (
    STATUS_AUTOMATIC,
    build_preliminary_report,
)


def _change(target_id: str, *, review: bool) -> dict:
    return {
        "change_id": target_id,
        "review_status": "REVIEW_REQUIRED" if review else "CONFIRMED",
        "outcome": "REVIEW_REQUIRED" if review else "MATERIAL_CHANGE",
        "before_value": 10,
        "after_value": 20,
        "evidence_refs": [{"evidence_ref": f"e:{target_id}"}],
        "provenance": {"source_atoms": []},
    }


def _text_target(target_id: str, text: str, fragment_id: str) -> dict:
    return {
        "review_evidence_id": target_id,
        "review_status": "REVIEW_REQUIRED",
        "outcome": "REVIEW_REQUIRED",
        "dimension": "UNKNOWN_DIMENSION",
        "before_value": None,
        "after_value": text,
        "evidence_refs": [{"evidence_ref": f"e:{target_id}"}],
        "provenance": {
            "source_atom": {
                "locations": {
                    "LEFT": [],
                    "RIGHT": [{"page": 1, "fragment_id": fragment_id}],
                }
            }
        },
    }


def _text_preparation() -> dict:
    return {
        "fragments": {
            "left": [{
                "id": "left-full",
                "pdf_page": 1,
                "text": "Независимое содержание левого листа " * 30,
                "source_block_id": "left",
                "source_kind": "paragraph",
                "bboxes": [{"x": 0.1, "y": 0.2, "width": 0.2, "height": 0.01}],
            }],
            "right": [
                {
                    "id": "stamp-fragment",
                    "pdf_page": 1,
                    "text": "ГИП Иванов 02.26",
                    "source_block_id": "title",
                    "source_kind": "table_row",
                    "bboxes": [{"x": 0.86, "y": 0.94, "width": 0.05, "height": 0.01}],
                },
                {
                    "id": "note-fragment",
                    "pdf_page": 1,
                    "text": "1. Щиты выполнить со степенью защиты не ниже IP31.",
                    "source_block_id": "notes",
                    "source_kind": "paragraph",
                    "bboxes": [{"x": 0.04, "y": 0.86, "width": 0.25, "height": 0.01}],
                },
            ],
        },
        "recognition_index": {
            "LEFT": {"1": {"char_count": 1600, "has_text_layer": True, "truncated": False}},
            "RIGHT": {"1": {"char_count": 1600, "has_text_layer": True, "truncated": False}},
        },
        "extraction": {"selected_pages": {"LEFT": [1], "RIGHT": [1]}},
    }


def _mode(subject: str, facet: str) -> dict:
    return {
        "reason": "mode_label_mismatch",
        "match_id": "same-document-root",
        "subject": subject,
        "facet_ref": facet,
        "left_modes": ["Рабочий", "Пожарный"],
        "right_modes": ["Аварийный", "ПП"],
        "evidence": {"LEFT": [{"raw": "10/20"}], "RIGHT": [{"raw": "11/22"}]},
        "summary": f"{subject}: режимы различаются",
    }


def _inputs() -> tuple[dict, dict, dict]:
    synthesis = {
        "changes": [_change("confirmed", review=False), _change("engineering", review=True)],
        "review_items": [
            _text_target("metadata", "ГИП Иванов 02.26", "stamp-fragment"),
            _text_target(
                "requirement",
                "1. Щиты выполнить со степенью защиты не ниже IP31.",
                "note-fragment",
            ),
        ],
    }
    electrical = {
        "blocked": [_mode("ВРУ1", "power"), _mode("ВРУ2", "current")],
        "unproven": [
            {
                "side": "LEFT", "row_id": "r1", "subject": "ВРУ3", "section_ref": "РП1",
                "summary": "ВРУ3 не имеет доказанной пары.",
            },
            {
                "side": "LEFT", "row_id": "r2", "subject": "ВРУ3", "section_ref": "РП1",
                "summary": "ВРУ3: другая атомарная строка не имеет пары.",
            },
        ],
    }
    return synthesis, electrical, _text_preparation()


def _plan() -> dict:
    synthesis, electrical, text = _inputs()
    return build_human_review_plan(
        pair_id="pair",
        synthesis=synthesis,
        electrical_table_changes=electrical,
        text_preparation=text,
        document_inconsistencies={
            "items": [{"inconsistency_id": "confirmed-doc-error", "verdict": "CONFIRMED"}]
        },
        generated_at="fixed",
    )


def test_existing_fast_finding_keeps_fast_primary_provenance_and_ai_is_support_only():
    atom = {
        "atom_id": "graphic:count-change",
        "provenance": {
            "producer": "graphic-change-ledger-adapter-v2",
            "structured": {"left_nodes": ["L"], "right_nodes": ["R"]},
        },
    }
    relation = {
        "resolution_id": "resolution",
        "task_id": "identity",
        "left_entity_ref": "LEFT:NODE:L",
        "right_entity_ref": "RIGHT:NODE:R",
    }
    annotated = _annotate_relation_atoms(
        [atom], [relation], baseline_atom_ids=["graphic:count-change"]
    )[0]
    assert annotated["provenance"]["producer"] == "graphic-change-ledger-adapter-v2"
    assert "ai_verified_relation" not in annotated["provenance"]
    assert annotated["provenance"]["supporting_resolution"][0]["task_id"] == "identity"


def test_unrelated_existing_fast_finding_cannot_satisfy_ai_task():
    finding = {
        "change_id": "fast-finding",
        "review_status": "CONFIRMED",
        "provenance": {"source_atoms": [{
            "source": "GRAPHIC",
            "provenance": {"producer": "FAST", "supporting_resolution": [{"task_id": "identity"}]},
        }]},
    }
    owned = _identity_owned_finding_ids(
        task_id="identity",
        candidate_ids=["fast-finding"],
        before={"fast-finding": deepcopy(finding)},
        after={"fast-finding": finding},
    )
    assert owned == []


def test_ai_identity_can_own_exact_review_to_confirmed_promotion():
    relation = {
        "resolution_id": "resolution",
        "task_id": "identity",
        "left_entity_ref": "LEFT:NODE:L",
        "right_entity_ref": "RIGHT:NODE:R",
    }
    baseline_atom = {
        "atom_id": "graphic:promoted",
        "review_status": "REVIEW_REQUIRED",
        "provenance": {
            "producer": "FAST",
            "structured": {"left_nodes": ["L"], "right_nodes": ["R"]},
        },
    }
    current_atom = {**baseline_atom, "review_status": "CONFIRMED"}
    annotated = _annotate_relation_atoms(
        [current_atom], [relation], baseline_atom_ids=[baseline_atom]
    )[0]
    finding = {
        "review_status": "CONFIRMED",
        "provenance": {"source_atoms": [{
            "source": "GRAPHIC", "provenance": annotated["provenance"],
        }]},
    }
    owned = _identity_owned_finding_ids(
        task_id="identity",
        candidate_ids=["finding"],
        before={"finding": {"review_status": "REVIEW_REQUIRED"}},
        after={"finding": finding},
    )
    assert owned == ["finding"]
    assert annotated["provenance"]["ai_verified_relation"][0]["task_id"] == "identity"


def test_missing_evidence_is_visible_information_not_a_human_question():
    plan = _plan()
    assert plan["summary"]["unproven_items"] == 1
    assert plan["summary"]["unproven_atomic_targets"] == 2
    assert len(plan["missing_evidence"]) == 1
    assert plan["missing_evidence"][0]["affected_target_ids"]


def test_metadata_is_not_an_engineer_question():
    plan = _plan()
    row = next(row for row in plan["atomic_target_mapping"] if row["target_id"] == "metadata")
    assert row["new_category"] == DOCUMENT_METADATA_CHANGE
    assert row["human_action_required"] is False
    assert row["source_region"]["region"] == "TITLE_BLOCK"


def test_technical_note_is_distinct_from_metadata_and_bounded_absence_is_required():
    plan = _plan()
    row = next(row for row in plan["atomic_target_mapping"] if row["target_id"] == "requirement")
    assert row["new_category"] == TEXT_REQUIREMENT_CHANGE
    assert row["subtype"] == "TEXT_REQUIREMENT_ADDED"
    assert row["bounded_absence"]["proven"] is True
    assert row["source_region"]["region"] == "NOTE_BLOCK"


def test_technical_note_without_full_searchable_counterpart_stays_missing_evidence():
    synthesis, electrical, text = _inputs()
    text["recognition_index"]["LEFT"]["1"]["truncated"] = True
    plan = build_human_review_plan(
        pair_id="pair", synthesis=synthesis,
        electrical_table_changes=electrical, text_preparation=text,
    )
    row = next(row for row in plan["atomic_target_mapping"] if row["target_id"] == "requirement")
    assert row["new_category"] == MISSING_EVIDENCE
    assert row["human_action_required"] is False


def test_bounded_absence_rejects_requirement_present_in_full_opposite_page_text():
    synthesis, electrical, text = _inputs()
    text["fragments"]["left"] = [
        {**text["fragments"]["left"][0], "text": "1. Щиты выполнить со степенью"},
        {
            **text["fragments"]["left"][0],
            "id": "left-continuation",
            "text": "защиты не ниже IP31.",
        },
    ]
    plan = build_human_review_plan(
        pair_id="pair", synthesis=synthesis,
        electrical_table_changes=electrical, text_preparation=text,
    )
    row = next(row for row in plan["atomic_target_mapping"] if row["target_id"] == "requirement")
    assert row["new_category"] == ACTIONABLE_ENGINEERING_DECISION
    assert row["human_action_required"] is True
    assert row["bounded_absence"]["normalized_match"] is not None


def test_semantic_candidate_in_drawing_blocks_bounded_absence_and_becomes_question():
    synthesis, electrical, text = _inputs()
    synthesis["review_items"][1]["after_value"] = (
        "5. Для контроля качества электроэнергии применить "
        "многофункциональные измерительные приборы."
    )
    text["fragments"]["right"][1]["text"] = synthesis["review_items"][1]["after_value"]
    text["fragments"]["left"].append({
        **text["fragments"]["left"][0],
        "id": "left-meter",
        "text": "Мультиметр PW1",
    })
    plan = build_human_review_plan(
        pair_id="pair", synthesis=synthesis,
        electrical_table_changes=electrical, text_preparation=text,
    )
    row = next(row for row in plan["atomic_target_mapping"] if row["target_id"] == "requirement")
    assert row["new_category"] == ACTIONABLE_ENGINEERING_DECISION
    assert row["bounded_absence"]["strong_semantic_candidate"] == "Мультиметр PW1"
    question = next(
        value for value in plan["standalone_questions"]
        if value["affected_target_ids"] == ["requirement"]
    )
    assert question["decision_type"] == "TEXT_REQUIREMENT_EQUIVALENCE"


def test_bus_label_candidate_blocks_absence_for_n_pe_requirement():
    synthesis, electrical, text = _inputs()
    synthesis["review_items"][1]["after_value"] = "3. В панелях предусмотреть шины N и РЕ."
    text["fragments"]["right"][1]["text"] = synthesis["review_items"][1]["after_value"]
    text["fragments"]["left"].append({
        **text["fragments"]["left"][0],
        "id": "left-pe-bus",
        "text": "К РЕ-шине ГРЩ2",
    })
    plan = build_human_review_plan(
        pair_id="pair", synthesis=synthesis,
        electrical_table_changes=electrical, text_preparation=text,
    )
    row = next(row for row in plan["atomic_target_mapping"] if row["target_id"] == "requirement")
    assert row["new_category"] == ACTIONABLE_ENGINEERING_DECISION
    assert row["bounded_absence"]["strong_semantic_candidate"] == "К РЕ-шине ГРЩ2"


def test_mode_atoms_are_one_root_cause_group_and_keep_atomic_ids():
    plan = _plan()
    assert plan["summary"]["mode_atoms"] == 2
    assert plan["summary"]["mode_groups"] == 1
    group = plan["groups"][0]
    rows = [row for row in plan["atomic_target_mapping"] if row["group_id"] == group["group_id"]]
    assert len(rows) == 2
    assert {row["new_category"] for row in rows} == {ROOT_CAUSE_GROUP_MEMBER}
    assert len(set(group["affected_target_ids"])) == 2


def test_one_group_decision_maps_to_many_atoms_and_preserves_audit():
    plan = _plan()
    group = plan["groups"][0]
    result = materialize_group_decision(
        plan,
        group_id=group["group_id"],
        answer={"answer_id": "NOT_COMPARABLE"},
        author="engineer",
        generated_at="fixed",
    )
    assert len(result["atomic_resolutions"]) == 2
    assert {row["target_id"] for row in result["atomic_resolutions"]} == set(group["affected_target_ids"])
    assert all(row["evidence_refs"] for row in result["atomic_resolutions"])
    assert result["constraints"]["atomic_audit_preserved"] is True


def test_human_override_per_atom_has_priority_over_group_answer():
    plan = _plan()
    group = plan["groups"][0]
    target_id = group["affected_target_ids"][0]
    result = materialize_group_decision(
        plan,
        group_id=group["group_id"],
        answer={"answer_id": "NOT_COMPARABLE"},
        author="engineer",
        overrides=[{
            "target_id": target_id,
            "answer": {"answer_id": "ADDITIONAL_DOCUMENT_REQUIRED"},
            "comment": "Нужен расчётный лист",
        }],
    )
    overridden = next(row for row in result["atomic_resolutions"] if row["target_id"] == target_id)
    assert overridden["decision_source"] == "HUMAN_ATOM_OVERRIDE"
    assert overridden["answer"]["answer_id"] == "ADDITIONAL_DOCUMENT_REQUIRED"


def test_group_mapping_rejects_modes_not_present_in_evidence():
    plan = _plan()
    group = plan["groups"][0]
    with pytest.raises(ValueError, match="outside the allowed evidence"):
        materialize_group_decision(
            plan,
            group_id=group["group_id"],
            answer={
                "answer_id": "DECLARE_MODE_MAPPING",
                "mapping": {"Рабочий": "Выдуманный режим"},
            },
            author="engineer",
        )


def test_review_plan_exact_accounting_and_no_silent_stage7_drop():
    plan = _plan()
    assert plan["summary"]["atomic_stage7_targets"] == 4
    stage7 = [row for row in plan["atomic_target_mapping"] if row["origin"] == "STAGE7"]
    assert {row["target_id"] for row in stage7} == {
        "confirmed", "engineering", "metadata", "requirement",
    }
    assert plan["constraints"]["no_silent_drop"] is True
    assert next(row for row in stage7 if row["target_id"] == "confirmed")["new_category"] == PROVEN_CHANGE


def test_document_inconsistency_is_not_duplicated_as_ab_target():
    plan = _plan()
    assert "confirmed-doc-error" not in {
        row["target_id"] for row in plan["atomic_target_mapping"]
    }
    assert plan["constraints"]["document_inconsistency_not_duplicated"] is True


def test_preliminary_report_moves_metadata_requirements_and_limitations_out_of_review():
    synthesis, electrical, _text = _inputs()
    plan = _plan()
    report = build_preliminary_report(
        pair_id="pair",
        synthesis=synthesis,
        electrical_table_changes=electrical,
        human_review_plan=plan,
    )
    counts = report["summary"]["counts"]
    assert counts["review"] == 2  # one group plus one standalone question
    assert counts["metadata"] == 1
    assert counts["text_requirements"] == 1
    sections = {section["section_id"]: section for section in report["sections"]}
    assert sections["metadata_changes"]["collapsed"] is True
    assert sections["text_requirements"]["items"][0]["status"] == STATUS_AUTOMATIC
    assert len(sections["unproven"]["items"]) == 1


def test_ai_routing_excludes_metadata_but_keeps_actionable_problem():
    plan = _plan()
    legacy = {
        "items": [
            {
                "item_id": "metadata", "kind": "TEXT_REVIEW",
                "decision": "AI_ELIGIBLE", "unresolved": True,
            },
            {
                "item_id": "engineering", "kind": "CHANGE_INCOMPLETE_EVIDENCE",
                "decision": "AI_ELIGIBLE", "unresolved": True,
            },
        ]
    }
    result = inventory.build_inventory(
        legacy_inventory=legacy,
        direct_page={},
        human_review_plan=plan,
        pair_id="pair",
    )
    by_id = {row["task_id"]: row for row in result["items"]}
    assert by_id["metadata"]["routed_to_ai"] is False
    assert by_id["metadata"]["pre_ai_classification"] == DOCUMENT_METADATA_CHANGE
    assert by_id["engineering"]["routed_to_ai"] is True
    assert by_id["engineering"]["decision"] == inventory.ELIGIBLE


def test_current_review_rows_have_exact_yes_no_accounting():
    plan = _plan()
    rows = plan["review_item_classification"]
    assert len(rows) == 5  # two modes, two text rows, one engineering change
    assert sum(row["human_action_required"] == "YES" for row in rows) == 1
    assert sum(row["human_action_required"] == "NO" for row in rows) == 4
    assert plan["summary"]["mandatory_human_interactions"] == 2
