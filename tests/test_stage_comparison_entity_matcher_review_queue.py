from __future__ import annotations

from copy import deepcopy

from backend.app.services.stage_comparison.entity_matcher import (
    build_early_entity_candidates,
    build_text_graphic_synthesis_candidates,
    match_entities,
)
from backend.app.services.stage_comparison.review_queue import (
    apply_human_decisions,
    build_human_decisions,
    build_review_queue,
    human_decisions_are_stale,
)
from backend.app.services.stage_comparison.unified_change_synthesizer import (
    synthesize_unified_changes,
)


def _entity(entity_id: str, name: str, **facts):
    return {"id": entity_id, "name": name, **facts}


def _panel(entity_id: str, name: str, **facts):
    defaults = {
        "functional_role": "distribution panel",
        "fed_by": ["input-1"],
        "feeds": ["load-a", "load-b"],
        "location": "electric room 1",
        "relationships": ["input-1 -> panel -> load-a", "panel -> load-b"],
    }
    defaults.update(facts)
    return _entity(entity_id, name, **defaults)


def _atom(atom_id: str, source: str, project_ref: str, **overrides):
    value = {
        "atom_id": atom_id,
        "source": source,
        "scope_ref": "scope-1",
        "subject_ref": project_ref,
        "project_entity_ref": project_ref,
        "dimension": "TYPE",
        "direction": "REPLACED",
        "outcome": "MATERIAL_CHANGE",
        "confidence": "HIGH",
        "evidence_ref": f"evidence:{atom_id}",
        "source_artifact": {
            "kind": "text-atoms" if source == "TEXT" else "graphic-ledger",
            "schema_version": "1",
            "artifact_ref": f"artifact:{source.lower()}",
        },
        "provenance": {"producer": "test"},
        "facet_ref": "device_type",
        "before_value": "QS1",
        "after_value": "QF3",
    }
    value.update(overrides)
    return value


def _ambiguous_entity_artifact():
    return match_entities(
        [_panel("left-panel", "Щит распределительный ЩР-1")],
        [
            _panel("right-panel-1", "Шкаф распределительный ШР-1"),
            _panel("right-panel-2", "Шкаф распределительный ШР-2"),
        ],
        generated_at="fixed",
    )


def _sheet_artifact():
    return {
        "kind": "stage_comparison_sheet_relations",
        "schema_version": "sheet-relations.v1",
        "input_signature": "sheet-input",
        "relations": [
            {
                "relation_id": "sheet-split-1",
                "left_pages": [10],
                "right_pages": [24, 25],
                "relation_type": "SPLIT",
                "status": "POSSIBLE",
            }
        ],
    }


def _synthesis_review_artifact():
    review = {
        "review_evidence_id": "review-1",
        "atom_id": "text-unknown",
        "dimension": "UNKNOWN_DIMENSION",
        "reason_codes": ["dimension_unknown"],
    }
    return {
        "kind": "stage_comparison_unified_changes",
        "provenance": {"input_signature": "synthesis-input"},
        "review_items": [review, deepcopy(review)],
        "contested_groups": [
            {
                "group_id": "contest-1",
                "change_ids": ["change-a", "change-b"],
                "reason_codes": ["directions_contradict"],
            }
        ],
    }


def test_early_candidates_can_never_assert_same_entity():
    result = build_early_entity_candidates(
        [_panel("left-panel", "ЩР-1")],
        [_panel("right-panel", "ЩР-1")],
        generated_at="fixed",
    )

    assert result["candidates"][0]["relation"] == "POSSIBLE_ENTITY"
    assert result["candidates"][0]["same_entity_allowed"] is False
    assert result["diagnostics"]["same_entity_emitted"] is False
    assert result["diagnostics"]["uses_model"] is False


def test_f_changed_name_is_same_when_independent_functional_facts_agree():
    result = match_entities(
        [_panel("left-panel", "Щит распределительный ЩР-1")],
        [_panel("right-panel", "Шкаф распределительный ШР-1")],
        generated_at="fixed",
    )

    relation = result["relations"][0]
    assert relation["relation"] == "SAME_ENTITY"
    assert relation["unique_candidate"] is True
    assert len(relation["strong_signals"]) >= 3
    assert relation["provenance"]["name_is_primary"] is False
    assert relation["project_entity_ref"].startswith("project_entity_")


def test_g_multiple_strong_candidates_stay_possible_and_create_one_question():
    entity_relations = _ambiguous_entity_artifact()

    assert {item["relation"] for item in entity_relations["relations"]} == {
        "POSSIBLE_ENTITY"
    }
    assert all(item["review_required"] for item in entity_relations["relations"])

    queue = build_review_queue(
        entity_relations=entity_relations, generated_at="fixed"
    )
    assert queue["counts"]["ENTITY"] == 1
    question = queue["questions"][0]
    assert question["question_type"] == "ENTITY_CANDIDATE_SELECTION"
    assert {option["code"] for option in question["answer_options"]} >= {
        "SELECT_RIGHT:right-panel-1",
        "SELECT_RIGHT:right-panel-2",
        "NO",
        "OTHER",
        "UNSURE",
    }


def test_explicit_role_conflict_is_different_and_missing_facts_are_unknown():
    result = match_entities(
        [
            _entity("left-a", "A", functional_role="supply fan"),
            _entity("left-unknown", "Unknown"),
        ],
        [
            _entity("right-a", "B", functional_role="fire pump"),
            _entity("right-unknown", "Other"),
        ],
        generated_at="fixed",
    )

    by_pair = {
        (item["left_entity_ref"], item["right_entity_ref"]): item
        for item in result["relations"]
    }
    assert by_pair[("left-a", "right-a")]["relation"] == "DIFFERENT_ENTITY"
    assert by_pair[("left-unknown", "right-unknown")]["relation"] == "UNKNOWN"


def test_entity_artifact_ids_and_signature_are_order_independent():
    left = [
        _panel("left-1", "ЩР-1", location="room 1"),
        _panel("left-2", "ЩР-2", location="room 2"),
    ]
    right = [
        _panel("right-1", "ШР-1", location="room 1"),
        _panel("right-2", "ШР-2", location="room 2"),
    ]

    first = match_entities(left, right, generated_at="fixed")
    second = match_entities(reversed(left), reversed(right), generated_at="fixed")

    assert first["input_signature"] == second["input_signature"]
    assert [item["relation_id"] for item in first["relations"]] == [
        item["relation_id"] for item in second["relations"]
    ]


def test_g246_candidate_has_exact_contract_and_keeps_missing_m_gates_fail_closed():
    project_ref = "project:panel-1"
    entities = match_entities(
        [_panel("left-panel", "ЩР-1", project_entity_ref=project_ref)],
        [_panel("right-panel", "ШР-1", project_entity_ref=project_ref)],
        generated_at="fixed",
    )
    text = _atom("text-type", "TEXT", project_ref)
    graphic = _atom("graphic-type", "GRAPHIC", project_ref)

    artifact = build_text_graphic_synthesis_candidates(
        [text], [graphic], entities, generated_at="fixed"
    )

    assert len(artifact["candidates"]) == 1
    candidate = artifact["candidates"][0]
    assert set(candidate) == {
        "candidate_id",
        "text_atom_id",
        "graphic_atom_id",
        "subject_relation",
        "links_by_side",
        "source_valid",
        "coverage_by_side",
        "document_binding_state",
        "text_count",
        "graphic_count",
        "subject_identity_provenance",
    }
    assert candidate["source_valid"] is False
    assert candidate["coverage_by_side"] == {
        "LEFT": "NOT_CHECKED",
        "RIGHT": "NOT_CHECKED",
    }
    assert candidate["document_binding_state"] == "DOCUMENT_BINDING_UNKNOWN"

    result = synthesize_unified_changes(
        text_atoms=[text], graphic_atoms=[graphic], candidates=[candidate]
    )
    assert len(result["changes"]) == 2
    evaluation = result["diagnostics"]["candidate_evaluations"][0]
    assert evaluation["merge_allowed"] is False
    assert evaluation["gates"]["M7"]["state"] != "PASS"


def test_g246_candidate_requires_unique_atom_pair_and_proven_relation():
    ref = "project:panel-1"
    possible_relations = _ambiguous_entity_artifact()
    text = _atom("text-type", "TEXT", ref)
    graphic_a = _atom("graphic-a", "GRAPHIC", ref)
    graphic_b = _atom("graphic-b", "GRAPHIC", ref)

    unproven = build_text_graphic_synthesis_candidates(
        [text], [graphic_a], possible_relations, generated_at="fixed"
    )
    ambiguous = build_text_graphic_synthesis_candidates(
        [text], [graphic_a, graphic_b], generated_at="fixed"
    )

    assert unproven["candidates"] == []
    assert unproven["diagnostics"]["unproven_pairs"]
    assert ambiguous["candidates"] == []
    assert len(ambiguous["diagnostics"]["ambiguous_pairs"]) == 2


def test_review_queue_consolidates_categories_counts_and_exact_duplicates():
    queue = build_review_queue(
        _sheet_artifact(),
        _ambiguous_entity_artifact(),
        _synthesis_review_artifact(),
        generated_at="fixed",
    )

    assert queue["counts"]["by_category"] == {
        "SHEET": 1,
        "ENTITY": 1,
        "CHANGE": 2,
    }
    assert queue["counts"]["total"] == 4
    assert len({item["question_id"] for item in queue["questions"]}) == 4
    sheet = next(item for item in queue["questions"] if item["category"] == "SHEET")
    assert sheet["question_type"] == "SHEET_SPLIT"
    assert {item["code"] for item in sheet["answer_options"]} >= {
        "YES",
        "SELECT_RIGHT:24",
        "SELECT_RIGHT:25",
        "NO",
        "OTHER",
        "UNSURE",
    }


def test_v_human_candidate_choice_updates_only_dependent_relations_without_rerun():
    entities = _ambiguous_entity_artifact()
    queue = build_review_queue(entity_relations=entities, generated_at="fixed")
    question = queue["questions"][0]
    decisions = build_human_decisions(
        queue,
        {question["question_id"]: "SELECT_RIGHT:right-panel-1"},
        author="engineer",
        generated_at="fixed",
    )

    applied = apply_human_decisions(
        queue,
        decisions,
        entity_relations=entities,
        generated_at="fixed",
    )

    effective = {
        item["right_entity_ref"]: item
        for item in applied["effective_entity_relations"]["relations"]
    }
    assert effective["right-panel-1"]["relation"] == "SAME_ENTITY"
    assert effective["right-panel-2"]["relation"] == "DIFFERENT_ENTITY"
    assert all(item["confidence"] == "HUMAN" for item in effective.values())
    assert {item["relation"] for item in entities["relations"]} == {
        "POSSIBLE_ENTITY"
    }
    assert applied["diagnostics"]["pipeline_rerun"] is False
    assert applied["diagnostics"]["automatic_artifacts_mutated"] is False


def test_unchanged_answer_is_not_reasked_but_changed_dependency_makes_it_stale():
    entities = _ambiguous_entity_artifact()
    queue = build_review_queue(entity_relations=entities, generated_at="fixed")
    question = queue["questions"][0]
    decisions = build_human_decisions(
        queue,
        {question["question_id"]: "UNSURE"},
        generated_at="fixed",
    )

    unchanged = build_review_queue(
        entity_relations=entities,
        human_decisions=decisions,
        generated_at="fixed",
    )
    assert unchanged["questions"] == []
    assert unchanged["counts"]["resolved_unchanged"] == 1
    assert human_decisions_are_stale(decisions, unchanged) is False

    repeated = build_human_decisions(
        queue,
        {question["question_id"]: "UNSURE"},
        previous=decisions,
        generated_at="later",
    )
    assert len(repeated["decisions"]) == 1
    assert repeated["decisions"][0]["decision_id"] == decisions["decisions"][0][
        "decision_id"
    ]
    assert repeated["input_signature"] == decisions["input_signature"]
    applied_from_filtered_queue = apply_human_decisions(
        unchanged,
        repeated,
        entity_relations=entities,
        generated_at="fixed",
    )
    assert applied_from_filtered_queue["applied_decision_ids"] == [
        decisions["decisions"][0]["decision_id"]
    ]

    changed_entities = deepcopy(entities)
    changed_entities["relations"][0]["score"] = 0.71
    changed = build_review_queue(
        entity_relations=changed_entities,
        human_decisions=decisions,
        generated_at="fixed",
    )
    assert len(changed["questions"]) == 1
    assert changed["counts"]["stale_decisions"] == 1
    assert human_decisions_are_stale(decisions, changed) is True


def test_review_question_and_decision_ids_are_order_independent():
    first_entities = _ambiguous_entity_artifact()
    second_entities = deepcopy(first_entities)
    second_entities["relations"].reverse()
    synthesis_a = _synthesis_review_artifact()
    synthesis_b = deepcopy(synthesis_a)
    synthesis_b["review_items"].reverse()
    synthesis_b["contested_groups"].reverse()

    first = build_review_queue(
        _sheet_artifact(), first_entities, synthesis_a, generated_at="fixed"
    )
    second = build_review_queue(
        _sheet_artifact(), second_entities, synthesis_b, generated_at="fixed"
    )

    assert first["input_signature"] == second["input_signature"]
    assert [item["question_id"] for item in first["questions"]] == [
        item["question_id"] for item in second["questions"]
    ]

    answers_first = {
        item["question_id"]: "UNSURE" for item in reversed(first["questions"])
    }
    answers_second = {
        item["question_id"]: "UNSURE" for item in second["questions"]
    }
    decisions_first = build_human_decisions(
        first, answers_first, generated_at="fixed"
    )
    decisions_second = build_human_decisions(
        second, answers_second, generated_at="fixed"
    )
    assert decisions_first["input_signature"] == decisions_second["input_signature"]
    assert [item["decision_id"] for item in decisions_first["decisions"]] == [
        item["decision_id"] for item in decisions_second["decisions"]
    ]
