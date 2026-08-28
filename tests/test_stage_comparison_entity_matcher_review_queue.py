from __future__ import annotations

from copy import deepcopy

import pytest

from backend.app.services.stage_comparison.entity_matcher import (
    bind_atoms_to_entity_relations,
    build_early_entity_candidates,
    build_text_graphic_synthesis_candidates,
    match_entities,
    normalize_entity,
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
        "outcome": "REVIEW_REQUIRED",
        "reason_codes": ["dimension_unknown"],
        "provenance": {"source_atom_outcome": "REVIEW_REQUIRED"},
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


def test_fallback_entity_ref_uses_canonical_nested_semantic_fact_sets():
    first = {
        "name": " Щит   ЩР-1 ",
        "facts": {
            "parameters": [
                {"name": "current", "values": [20, 10]},
                {"name": "voltage", "values": [400, 380]},
            ],
            "relationships": [
                {"from": "input-1", "to": ["load-b", "load-a"]},
                {"from": "input-2", "to": ["load-c"]},
            ],
        },
    }
    second = {
        "facts": {
            "relationships": [
                {"to": ["load-c"], "from": "input-2"},
                {"to": ["load-a", "load-b"], "from": "input-1"},
            ],
            "parameters": [
                {"values": [380, 400], "name": "voltage"},
                {"values": [10, 20], "name": "current"},
            ],
        },
        "name": "Щит ЩР-1",
    }

    normalized_first = normalize_entity(first, side="LEFT")
    normalized_second = normalize_entity(second, side="LEFT")

    assert normalized_first["entity_ref"] == normalized_second["entity_ref"]
    assert normalized_first["signals"] == normalized_second["signals"]


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


def test_exact_relation_binds_side_specific_atoms_before_g246_without_fuzzy_match():
    relations = match_entities(
        [_panel("left-panel", "ЩР-1")],
        [_panel("right-panel", "ШР-1")],
        generated_at="fixed",
    )
    text = _atom(
        "text-type",
        "TEXT",
        None,
        subject_ref="left-panel",
        project_entity_ref=None,
    )
    graphic = _atom(
        "graphic-type",
        "GRAPHIC",
        None,
        subject_ref="right-panel",
        project_entity_ref=None,
    )

    bound = bind_atoms_to_entity_relations(
        [text], [graphic], relations, generated_at="fixed"
    )
    shared_ref = relations["relations"][0]["project_entity_ref"]
    assert text["project_entity_ref"] is None
    assert graphic["project_entity_ref"] is None
    assert bound["text_atoms"][0]["project_entity_ref"] == shared_ref
    assert bound["graphic_atoms"][0]["project_entity_ref"] == shared_ref
    assert bound["text_atoms"][0]["subject_ref"] == shared_ref
    assert bound["graphic_atoms"][0]["subject_ref"] == shared_ref
    assert bound["entity_relations_binding"]["proof_signature"]
    rebound = bind_atoms_to_entity_relations(
        bound["text_atoms"],
        bound["graphic_atoms"],
        relations,
        generated_at="later",
    )
    assert rebound["input_signature"] == bound["input_signature"]

    candidates = build_text_graphic_synthesis_candidates(
        [text], [graphic], relations, generated_at="fixed"
    )
    assert len(candidates["candidates"]) == 1
    provenance = candidates["candidates"][0]["subject_identity_provenance"]
    assert provenance["entity_relations_binding"]["binding_signature"]
    assert provenance["entity_relation_ids"] == [
        relations["relations"][0]["relation_id"]
    ]
    synthesis = synthesize_unified_changes(
        text_atoms=bound["text_atoms"],
        graphic_atoms=bound["graphic_atoms"],
        candidates=candidates["candidates"],
    )
    assert len(synthesis["changes"]) == 2
    assert synthesis["review_items"] == []
    assert {
        change["project_entity_ref"] for change in synthesis["changes"]
    } == {shared_ref}

    fuzzy = deepcopy(graphic)
    fuzzy["subject_ref"] = "right-pane"
    rejected = build_text_graphic_synthesis_candidates(
        [text], [fuzzy], relations, generated_at="fixed"
    )
    assert rejected["candidates"] == []
    assert rejected["diagnostics"]["unproven_pairs"] == [
        ["text-type", "graphic-type"]
    ]

    unresolved_same = deepcopy(relations)
    unresolved_same["relations"][0]["review_required"] = True
    unresolved = build_text_graphic_synthesis_candidates(
        [text], [graphic], unresolved_same, generated_at="fixed"
    )
    assert unresolved["candidates"] == []


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


def test_other_without_ref_stays_pending_but_explicit_ref_creates_effective_relation():
    entities = _ambiguous_entity_artifact()
    queue = build_review_queue(entity_relations=entities, generated_at="fixed")
    question = queue["questions"][0]
    unresolved = build_human_decisions(
        queue,
        {question["question_id"]: "OTHER"},
        generated_at="fixed",
    )

    still_pending = build_review_queue(
        entity_relations=entities,
        human_decisions=unresolved,
        generated_at="fixed",
    )
    assert still_pending["counts"]["ENTITY"] == 1
    assert still_pending["counts"]["unresolved_decisions"] == 1
    unresolved_application = apply_human_decisions(
        queue,
        unresolved,
        entity_relations=entities,
        generated_at="fixed",
    )
    assert {
        item["relation"]
        for item in unresolved_application["effective_entity_relations"][
            "relations"
        ]
    } == {"POSSIBLE_ENTITY"}
    assert all(
        item["review_required"]
        for item in unresolved_application["effective_entity_relations"][
            "relations"
        ]
    )

    explicit = build_human_decisions(
        queue,
        {
            question["question_id"]: {
                "answer": "OTHER",
                "explicit_candidate": {
                    "right_entity_ref": "right-panel-3",
                },
            }
        },
        generated_at="fixed",
    )
    resolved_queue = build_review_queue(
        entity_relations=entities,
        human_decisions=explicit,
        generated_at="fixed",
    )
    assert resolved_queue["counts"]["ENTITY"] == 0
    effective = apply_human_decisions(
        queue,
        explicit,
        entity_relations=entities,
        generated_at="fixed",
    )["effective_entity_relations"]
    by_right = {
        item["right_entity_ref"]: item for item in effective["relations"]
    }
    assert by_right["right-panel-3"]["relation"] == "SAME_ENTITY"
    assert by_right["right-panel-3"]["confidence"] == "HUMAN"
    assert by_right["right-panel-3"]["review_required"] is False
    assert by_right["right-panel-1"]["relation"] == "DIFFERENT_ENTITY"
    assert by_right["right-panel-2"]["relation"] == "DIFFERENT_ENTITY"
    assert {item["relation"] for item in entities["relations"]} == {
        "POSSIBLE_ENTITY"
    }

    text = _atom(
        "text-explicit",
        "TEXT",
        None,
        subject_ref="left-panel",
        project_entity_ref=None,
    )
    graphic = _atom(
        "graphic-explicit",
        "GRAPHIC",
        None,
        subject_ref="right-panel-3",
        project_entity_ref=None,
    )
    bound = bind_atoms_to_entity_relations(
        [text], [graphic], effective, generated_at="fixed"
    )
    assert bound["text_atoms"][0]["project_entity_ref"] == by_right[
        "right-panel-3"
    ]["project_entity_ref"]
    assert bound["graphic_atoms"][0]["project_entity_ref"] == by_right[
        "right-panel-3"
    ]["project_entity_ref"]


def test_generic_yes_cannot_resolve_unknown_change_without_typed_identity():
    synthesis = _synthesis_review_artifact()
    queue = build_review_queue(synthesis=synthesis, generated_at="fixed")
    question = next(
        item
        for item in queue["questions"]
        if item["question_type"] == "CHANGE_REVIEW_EVIDENCE"
    )
    generic = build_human_decisions(
        queue,
        {question["question_id"]: "YES"},
        generated_at="fixed",
    )

    still_pending = build_review_queue(
        synthesis=synthesis,
        human_decisions=generic,
        generated_at="fixed",
    )
    assert question["question_id"] in {
        item["question_id"] for item in still_pending["questions"]
    }
    application = apply_human_decisions(
        queue,
        generic,
        synthesis=synthesis,
        generated_at="fixed",
    )
    resolution = application["change_resolutions"][0]
    assert resolution["resolution"] == "REVIEW_REQUIRED"
    assert resolution["resolution_complete"] is False
    assert resolution["missing_typed_fields"] == [
        "dimension",
        "outcome",
        "project_entity_ref",
    ]
    assert question["context"]["typed_resolution_contract"] == {
        "version": "change-typed-resolution.v1",
        "generic_yes_allowed": False,
        "required_fields": ["dimension", "project_entity_ref", "outcome"],
        "accepted_fields": sorted(
            {
                "dimension",
                "subject_ref",
                "project_entity_ref",
                "facet_ref",
                "direction",
                "outcome",
                "before_value",
                "after_value",
            }
        ),
    }

    typed = build_human_decisions(
        queue,
        {
            question["question_id"]: {
                "answer": "YES",
                "typed_resolution": {
                    "dimension": "TYPE",
                    "project_entity_ref": "project:panel-1",
                    "outcome": "MATERIAL_CHANGE",
                },
            }
        },
        generated_at="fixed",
    )
    resolved = build_review_queue(
        synthesis=synthesis,
        human_decisions=typed,
        generated_at="fixed",
    )
    assert question["question_id"] not in {
        item["question_id"] for item in resolved["questions"]
    }
    typed_application = apply_human_decisions(
        queue,
        typed,
        synthesis=synthesis,
        generated_at="fixed",
    )
    typed_resolution = typed_application["change_resolutions"][0]
    assert typed_resolution["resolution"] == "CONFIRMED"
    assert typed_resolution["resolution_complete"] is True
    assert typed_resolution["typed_resolution"]["dimension"] == "TYPE"


def test_human_answer_contract_rejects_open_enums_invalid_selection_and_cardinality():
    synthesis = _synthesis_review_artifact()
    queue = build_review_queue(
        sheet_relations=_sheet_artifact(),
        synthesis=synthesis,
        generated_at="fixed",
    )
    change_question = next(
        item for item in queue["questions"]
        if item["question_type"] == "CHANGE_REVIEW_EVIDENCE"
    )
    contested_question = next(
        item for item in queue["questions"]
        if item["question_type"] == "CHANGE_CONTESTED"
    )
    sheet_question = next(
        item for item in queue["questions"]
        if item["question_type"] == "SHEET_SPLIT"
    )

    with pytest.raises(ValueError, match="direction unsupported"):
        build_human_decisions(
            queue,
            {change_question["question_id"]: {
                "answer": "OTHER",
                "typed_resolution": {"direction": "SIDEWAYS"},
            }},
        )
    with pytest.raises(ValueError, match="resolve rather than preserve review"):
        build_human_decisions(
            queue,
            {change_question["question_id"]: {
                "answer": "OTHER",
                "typed_resolution": {"outcome": "REVIEW_REQUIRED"},
            }},
        )
    with pytest.raises(ValueError, match="must select offered changes"):
        build_human_decisions(
            queue,
            {contested_question["question_id"]: {
                "answer": "YES",
                "typed_resolution": {"selected_change_ids": ["not-offered"]},
            }},
        )
    with pytest.raises(ValueError, match="conflicts with cardinality"):
        build_human_decisions(
            queue,
            {sheet_question["question_id"]: {
                "answer": "OTHER",
                "explicit_candidate": {
                    "left_pages": [10],
                    "right_pages": [24, 25],
                    "relation_type": "MATCHED",
                },
            }},
        )

    legacy_invalid = build_human_decisions(
        queue,
        {change_question["question_id"]: {
            "answer": "OTHER",
            "typed_resolution": {
                "dimension": "TYPE",
                "project_entity_ref": "project:panel-1",
                "outcome": "MATERIAL_CHANGE",
            },
        }},
    )
    legacy_invalid["decisions"][0]["typed_resolution"]["outcome"] = (
        "REVIEW_REQUIRED"
    )
    pending = build_review_queue(
        synthesis=synthesis,
        human_decisions=legacy_invalid,
        generated_at="fixed",
    )
    assert change_question["question_id"] in {
        item["question_id"] for item in pending["questions"]
    }


def test_typed_answer_fields_are_scoped_to_the_exact_change_question():
    synthesis = _synthesis_review_artifact()
    queue = build_review_queue(synthesis=synthesis, generated_at="fixed")
    review_question = next(
        item for item in queue["questions"]
        if item["question_type"] == "CHANGE_REVIEW_EVIDENCE"
    )
    contested_question = next(
        item for item in queue["questions"]
        if item["question_type"] == "CHANGE_CONTESTED"
    )

    assert contested_question["context"]["typed_resolution_contract"] == {
        "version": "change-typed-resolution.v1",
        "generic_yes_allowed": False,
        "required_fields": ["selected_change_ids"],
        "accepted_fields": ["selected_change_ids"],
    }
    with pytest.raises(ValueError, match="unsupported fields"):
        build_human_decisions(
            queue,
            {review_question["question_id"]: {
                "answer": "OTHER",
                "typed_resolution": {"selected_change_ids": ["change-a"]},
            }},
        )
    with pytest.raises(ValueError, match="selected_refs is unsupported"):
        build_human_decisions(
            queue,
            {review_question["question_id"]: {
                "answer": "YES",
                "selected_refs": ["change-a"],
            }},
        )
    with pytest.raises(ValueError, match="semantically empty"):
        build_human_decisions(
            queue,
            {review_question["question_id"]: {
                "answer": "OTHER",
                "typed_resolution": {},
            }},
        )
    with pytest.raises(ValueError, match="unsupported fields"):
        build_human_decisions(
            queue,
            {contested_question["question_id"]: {
                "answer": "OTHER",
                "typed_resolution": {"dimension": "TYPE"},
            }},
        )

    valid = build_human_decisions(
        queue,
        {review_question["question_id"]: {
            "answer": "OTHER",
            "typed_resolution": {
                "dimension": "TYPE",
                "project_entity_ref": "project:panel-1",
                "outcome": "MATERIAL_CHANGE",
            },
        }},
    )
    legacy_foreign = deepcopy(valid)
    legacy_foreign["decisions"][0]["typed_resolution"][
        "selected_change_ids"
    ] = ["change-a"]
    pending = build_review_queue(
        synthesis=synthesis,
        human_decisions=legacy_foreign,
        generated_at="fixed",
    )
    assert review_question["question_id"] in {
        item["question_id"] for item in pending["questions"]
    }

    # The short-lived API contract used to append an empty selection to an
    # otherwise valid atom resolution.  Preserve that valid legacy shape.
    legacy_synthetic_empty = deepcopy(valid)
    legacy_synthetic_empty["decisions"][0]["typed_resolution"][
        "selected_change_ids"
    ] = []
    resolved = build_review_queue(
        synthesis=synthesis,
        human_decisions=legacy_synthetic_empty,
        generated_at="fixed",
    )
    assert review_question["question_id"] not in {
        item["question_id"] for item in resolved["questions"]
    }

    legacy_resolution_alias = build_human_decisions(
        queue,
        {review_question["question_id"]: {
            "answer": "OTHER",
            "resolution": {
                "dimension": "TYPE",
                "project_entity_ref": "project:panel-1",
                "outcome": "MATERIAL_CHANGE",
            },
        }},
    )
    assert legacy_resolution_alias["decisions"][0]["typed_resolution"] == {
        "dimension": "TYPE",
        "project_entity_ref": "project:panel-1",
        "outcome": "MATERIAL_CHANGE",
    }


@pytest.mark.parametrize(
    ("answer_code", "expected_resolution"),
    [("YES", "CONFIRMED"), ("OTHER", "TYPED_RESOLUTION")],
)
def test_contested_change_accepts_validated_legacy_selected_refs(
    answer_code, expected_resolution
):
    synthesis = _synthesis_review_artifact()
    queue = build_review_queue(synthesis=synthesis, generated_at="fixed")
    question = next(
        item for item in queue["questions"]
        if item["question_type"] == "CHANGE_CONTESTED"
    )

    decisions = build_human_decisions(
        queue,
        {question["question_id"]: {
            "answer": answer_code,
            "selected_refs": ["change-a"],
        }},
        generated_at="fixed",
    )

    decision = decisions["decisions"][0]
    assert decision["selected_refs"] == ["change-a"]
    assert decision["typed_resolution"] is None
    pending = build_review_queue(
        synthesis=synthesis,
        human_decisions=decisions,
        generated_at="fixed",
    )
    assert question["question_id"] not in {
        item["question_id"] for item in pending["questions"]
    }
    resolution = apply_human_decisions(
        queue,
        decisions,
        synthesis=synthesis,
        generated_at="fixed",
    )["change_resolutions"][0]
    assert resolution["resolution"] == expected_resolution
    assert resolution["resolution_complete"] is True

    with pytest.raises(ValueError, match="must select offered changes"):
        build_human_decisions(
            queue,
            {question["question_id"]: {
                "answer": "YES",
                "selected_refs": ["not-offered"],
            }},
        )
    for selection_payload in (
        {"selected_refs": ["change-a", "change-b"]},
        {
            "typed_resolution": {
                "selected_change_ids": ["change-a", "change-b"],
            }
        },
    ):
        with pytest.raises(ValueError, match="proper subset"):
            build_human_decisions(
                queue,
                {question["question_id"]: {
                    "answer": "YES",
                    **selection_payload,
                }},
            )
    with pytest.raises(ValueError, match="conflicts with selected_refs"):
        build_human_decisions(
            queue,
            {question["question_id"]: {
                "answer": "YES",
                "selected_refs": ["change-a"],
                "typed_resolution": {
                    "selected_change_ids": ["change-b"],
                },
            }},
        )

    typed = build_human_decisions(
        queue,
        {question["question_id"]: {
            "answer": "YES",
            "typed_resolution": {
                "selected_change_ids": ["change-b"],
            },
        }},
        generated_at="fixed",
    )
    assert typed["decisions"][0]["selected_refs"] == []
    assert typed["decisions"][0]["typed_resolution"] == {
        "selected_change_ids": ["change-b"]
    }
    typed_pending = build_review_queue(
        synthesis=synthesis,
        human_decisions=typed,
        generated_at="fixed",
    )
    assert question["question_id"] not in {
        item["question_id"] for item in typed_pending["questions"]
    }

    legacy_all = deepcopy(decisions)
    legacy_all["decisions"][0]["selected_refs"] = ["change-a", "change-b"]
    unresolved = build_review_queue(
        synthesis=synthesis,
        human_decisions=legacy_all,
        generated_at="fixed",
    )
    assert question["question_id"] in {
        item["question_id"] for item in unresolved["questions"]
    }


def test_uncertain_explicit_sheet_other_cannot_resolve_review():
    queue = build_review_queue(
        sheet_relations=_sheet_artifact(), generated_at="fixed"
    )
    question = queue["questions"][0]

    with pytest.raises(ValueError, match="relation_type unsupported"):
        build_human_decisions(
            queue,
            {question["question_id"]: {
                "answer": "OTHER",
                "explicit_candidate": {
                    "left_pages": [10, 11],
                    "right_pages": [24, 25],
                    "relation_type": "UNCERTAIN",
                },
            }},
        )

    valid = build_human_decisions(
        queue,
        {question["question_id"]: {
            "answer": "OTHER",
            "explicit_candidate": {
                "left_pages": [10],
                "right_pages": [24, 25],
                "relation_type": "SPLIT",
            },
        }},
        generated_at="fixed",
    )
    legacy_uncertain = deepcopy(valid)
    legacy_uncertain["decisions"][0]["explicit_candidate"] = {
        "left_pages": [10, 11],
        "right_pages": [24, 25],
        "relation_type": "UNCERTAIN",
    }
    pending = build_review_queue(
        sheet_relations=_sheet_artifact(),
        human_decisions=legacy_uncertain,
        generated_at="fixed",
    )
    assert question["question_id"] in {
        item["question_id"] for item in pending["questions"]
    }
    effective = apply_human_decisions(
        queue,
        legacy_uncertain,
        sheet_relations=_sheet_artifact(),
        generated_at="fixed",
    )["effective_sheet_relations"]["relations"][0]
    assert effective["status"] == "POSSIBLE"
    assert effective["relation_type"] == "UNCERTAIN"
    assert effective["review_required"] is True


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


def test_sheet_queue_asks_only_substantive_possible_components():
    relations = {
        "kind": "stage_comparison_sheet_relations",
        "input_signature": "sheet-filtering",
        "relations": [
            {
                "relation_id": "high-split",
                "left_pages": [1],
                "right_pages": [10, 11],
                "relation_type": "SPLIT",
                "status": "HIGH",
            },
            {
                "relation_id": "no-match",
                "left_pages": [2],
                "right_pages": [],
                "relation_type": "NO_MATCH",
                "status": "NO_MATCH",
            },
            {
                "relation_id": "empty-unknown",
                "left_pages": [3],
                "right_pages": [],
                "relation_type": "UNCERTAIN",
                "status": "UNKNOWN",
                "candidate_edges": [{
                    "left_page": 3,
                    "right_page": 12,
                    "status": "UNKNOWN",
                    "substantive_signals": [],
                }],
            },
            {
                "relation_id": "possible",
                "left_pages": [4],
                "right_pages": [13],
                "relation_type": "MATCHED",
                "status": "POSSIBLE",
            },
        ],
    }

    queue = build_review_queue(sheet_relations=relations, generated_at="fixed")

    assert queue["counts"]["SHEET"] == 1
    assert queue["questions"][0]["context"]["candidate_relation_ids"] == [
        "possible"
    ]


def test_sheet_candidate_component_is_one_question_and_one_materialized_relation():
    relations = {
        "kind": "stage_comparison_sheet_relations",
        "input_signature": "sheet-components",
        "relations": [
            {
                "relation_id": "canonical",
                "left_pages": [1],
                "right_pages": [10],
                "relation_type": "MATCHED",
                "status": "POSSIBLE",
                "automatic_scope": True,
                "confidence": 0.4,
                "candidate_edges": [{
                    "left_page": 1,
                    "right_page": 10,
                    "status": "POSSIBLE",
                    "substantive_signals": ["functional"],
                }],
            },
            {
                "relation_id": "alternative",
                "left_pages": [],
                "right_pages": [11],
                "relation_type": "UNCERTAIN",
                "status": "POSSIBLE",
                "candidate_edges": [{
                    "left_page": 1,
                    "right_page": 11,
                    "status": "POSSIBLE",
                    "substantive_signals": ["functional"],
                }],
            },
        ],
    }
    queue = build_review_queue(sheet_relations=relations, generated_at="fixed")

    assert queue["counts"]["SHEET"] == 1
    question = queue["questions"][0]
    assert question["question_type"] == "SHEET_SPLIT"
    assert question["context"]["left_pages"] == [1]
    assert question["context"]["right_pages"] == [10, 11]
    assert question["context"]["materialization_relation_id"] == "canonical"
    assert len(question["dependencies"]) == 2

    decisions = build_human_decisions(
        queue,
        {question["question_id"]: "YES"},
        generated_at="fixed",
    )
    effective = apply_human_decisions(
        queue,
        decisions,
        sheet_relations=relations,
        generated_at="fixed",
    )["effective_sheet_relations"]["relations"]

    assert sum(item["status"] == "HIGH" for item in effective) == 1
    canonical = next(item for item in effective if item["relation_id"] == "canonical")
    alternative = next(
        item for item in effective if item["relation_id"] == "alternative"
    )
    assert canonical["left_pages"] == [1]
    assert canonical["right_pages"] == [10, 11]
    assert canonical["relation_type"] == "SPLIT"
    assert alternative["status"] == "CANDIDATE_SUPERSEDED"
    assert alternative["superseded_by_relation_id"] == "canonical"


def test_many_to_many_sheet_component_remains_one_fail_closed_question():
    relations = {
        "kind": "stage_comparison_sheet_relations",
        "input_signature": "sheet-many-many",
        "relations": [
            {
                "relation_id": "edge-a",
                "left_pages": [1],
                "right_pages": [10],
                "relation_type": "MATCHED",
                "status": "POSSIBLE",
                "candidate_edges": [{
                    "left_page": 1,
                    "right_page": 10,
                    "status": "POSSIBLE",
                    "substantive_signals": ["title"],
                }, {
                    "left_page": 2,
                    "right_page": 10,
                    "status": "POSSIBLE",
                    "substantive_signals": ["title"],
                }],
            },
            {
                "relation_id": "edge-b",
                "left_pages": [],
                "right_pages": [11],
                "relation_type": "UNCERTAIN",
                "status": "POSSIBLE",
                "candidate_edges": [{
                    "left_page": 2,
                    "right_page": 11,
                    "status": "POSSIBLE",
                    "substantive_signals": ["title"],
                }],
            },
        ],
    }

    queue = build_review_queue(sheet_relations=relations, generated_at="fixed")

    assert queue["counts"]["SHEET"] == 1
    question = queue["questions"][0]
    assert question["question_type"] == "SHEET_CANDIDATE_GROUP"
    assert {option["code"] for option in question["answer_options"]} == {
        "NO", "OTHER", "UNSURE"
    }


def test_non_actionable_text_fact_reviews_leave_only_the_sheet_question():
    sheet_relations = {
        "kind": "stage_comparison_sheet_relations",
        "input_signature": "sheet-blocker",
        "relations": [{
            "relation_id": "possible-sheet",
            "left_pages": [4],
            "right_pages": [13],
            "relation_type": "MATCHED",
            "status": "POSSIBLE",
        }],
    }
    text_atoms = [
        _atom(
            "sheet-blocked",
            "TEXT",
            None,
            subject_ref="text_entity:PANEL_1",
            project_entity_ref=None,
            outcome="REVIEW_REQUIRED",
            provenance={
                "producer": "deterministic-text-fact-producer-v1",
                "review_requirement": {
                    "reason_codes": ["sheet_relation_unconfirmed"],
                    "only_upstream_relation_blocker": True,
                    "per_atom_question_actionable": False,
                },
            },
        ),
        _atom(
            "coverage-blocked",
            "TEXT",
            None,
            subject_ref="text_entity:PANEL_2",
            project_entity_ref=None,
            outcome="REVIEW_REQUIRED",
            provenance={
                "producer": "deterministic-text-fact-producer-v1",
                "review_requirement": {
                    "reason_codes": [
                        "sheet_relation_unconfirmed",
                        "opposite_side_structured_coverage_incomplete",
                    ],
                    "only_upstream_relation_blocker": False,
                    "per_atom_question_actionable": False,
                },
            },
        ),
    ]
    synthesis = synthesize_unified_changes(text_atoms=text_atoms)
    original_review_items = deepcopy(synthesis["review_items"])

    queue = build_review_queue(
        sheet_relations=sheet_relations,
        synthesis=synthesis,
        generated_at="fixed",
    )

    assert len(synthesis["review_items"]) == 2
    assert synthesis["review_items"] == original_review_items
    assert queue["counts"]["by_category"] == {
        "SHEET": 1,
        "ENTITY": 0,
        "CHANGE": 0,
    }
    assert [item["category"] for item in queue["questions"]] == ["SHEET"]
    diagnostics = queue["diagnostics"]
    assert diagnostics["suppressed_change_questions"] == 2
    assert diagnostics["suppressed_change_question_reasons"] == {
        "opposite_side_structured_coverage_incomplete": 1,
        "sheet_relation_unconfirmed": 2,
    }
    assert diagnostics["upstream_sheet_relation_review_items_suppressed"] == 1
    assert diagnostics["opposite_coverage_gap_review_items_suppressed"] == 1
    assert diagnostics["suppressed_change_review_item_refs"] == sorted(
        item["review_evidence_id"] for item in synthesis["review_items"]
    )


def test_change_question_policy_supports_nested_provenance_and_fails_closed():
    base = {
        "review_evidence_id": "nested-review",
        "atom_id": "nested-atom",
        "dimension": "UNKNOWN_DIMENSION",
        "project_entity_ref": None,
        "outcome": "REVIEW_REQUIRED",
        "reason_codes": ["dimension_unknown"],
        "provenance": {
            "source_atom_outcome": "REVIEW_REQUIRED",
            "source_atom": {
                "provenance": {
                    "review_requirement": {
                        "reason_codes": [
                            "opposite_side_structured_coverage_incomplete"
                        ],
                        "per_atom_question_actionable": False,
                    }
                }
            },
        },
    }
    suppressed = build_review_queue(
        synthesis={"kind": "synthesis", "review_items": [base]},
        generated_at="fixed",
    )

    assert suppressed["counts"]["CHANGE"] == 0
    assert suppressed["diagnostics"]["suppressed_change_questions"] == 1

    conflicting = deepcopy(base)
    conflicting["review_evidence_id"] = "conflicting-review"
    conflicting["provenance"]["review_requirement"] = {
        "per_atom_question_actionable": True,
    }
    actionable = build_review_queue(
        synthesis={"kind": "synthesis", "review_items": [conflicting]},
        generated_at="fixed",
    )

    assert actionable["counts"]["CHANGE"] == 1
    assert actionable["diagnostics"]["suppressed_change_questions"] == 0
    contract = actionable["questions"][0]["context"][
        "typed_resolution_contract"
    ]
    assert contract["required_fields"] == [
        "dimension", "project_entity_ref", "outcome"
    ]
