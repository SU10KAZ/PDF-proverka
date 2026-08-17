from __future__ import annotations

import copy

from backend.app.services.stage_comparison import semantic_diff


def _entry(number, change="changed", before=None, after=None, kind="text", bbox=None):
    value = {
        "evidence_id": f"raw_{number:04d}", "kind": kind, "change": change,
        "bbox": bbox or [10 + number, 10, 20 + number, 20],
    }
    if before is not None:
        value.update({"left_value": before, "left_ref": f"word_l_{number}"})
    if after is not None:
        value.update({"right_value": after, "right_ref": f"word_r_{number}"})
    return value


def _analyze(items, **kwargs):
    return semantic_diff.analyze_local_evidence(items, left_page=8, right_page=7, **kwargs)


def test_changed_number_is_not_lost():
    result = _analyze([_entry(1, before="QF1 25A", after="QF1 32A")])
    assert result["before"] == "QF1 25A"
    assert result["after"] == "QF1 32A"
    assert result["confidence"] == .96


def test_changed_word_is_reported_exactly():
    result = _analyze([_entry(1, before="существующий", after="проектируемый")])
    assert "существующий → проектируемый" in result["change_summary"]


def test_added_text_has_absent_before_side():
    result = _analyze([_entry(1, change="added", after="Ограждение Н=1200")])
    assert result["change_kind"] == "added"
    assert result["before"] == "отсутствует"


def test_removed_text_has_absent_after_side():
    result = _analyze([_entry(1, change="removed", before="Люк Л1")])
    assert result["change_kind"] == "removed"
    assert result["after"] == "отсутствует"


def test_local_entity_change_is_preserved_as_structured_match():
    result = _analyze([], left_entities=["ВВГнг-LS 5×16"], right_entities=["ВВГнг-LS 5×25"])
    assert result["source"] == "graphic_description"
    assert result["entity_matches"][0]["status"] == "same_entity_changed"


def test_table_row_keeps_old_and_new_value():
    result = _analyze([_entry(1, before="Поз. 12 | 4 шт.", after="Поз. 12 | 6 шт.")], semantic_types=["table"])
    assert result["semantic_types"] == ["table"]
    assert "4 шт." in result["before"] and "6 шт." in result["after"]


def test_multiple_values_of_one_entity_are_not_collapsed():
    result = _analyze([
        _entry(1, before="D=100", after="D=150"),
        _entry(2, before="L=2400", after="L=2600"),
    ])
    assert len(result["structured_changes"]) == 2
    assert "D=100" in result["before"] and "L=2400" in result["before"]


def test_graphic_block_can_use_already_prepared_entities():
    block = {"block_id": "plan", "bbox_pdf_visual": [0, 0, 1000, 1000], "type": "image",
             "semantic_type": "plan", "entities": "Ось А, 6000, Узел К"}
    context = semantic_diff._block_context(block, [100, 100, 120, 120], local_text="6000")
    assert context["localized_entities"] == ["6000"]
    assert context["whole_block_change_not_assumed"] is True


def test_small_group_does_not_turn_whole_large_block_into_changed_context():
    block = {"block_id": "large", "bbox_pdf_visual": [0, 0, 1000, 1000], "entities": "A, B, C"}
    context = semantic_diff._block_context(block, [10, 10, 20, 20], local_text="A")
    assert context["group_overlap_of_block"] == .0001
    assert context["localized_entities"] == ["A"]


def test_missing_evidence_is_uncertain():
    result = _analyze([])
    assert result["change_kind"] == "uncertain"
    assert result["requires_human_review"] is True
    assert result["confidence"] == 0


def test_added_image_is_not_described_as_existing_before():
    result = _analyze([_entry(1, change="added", kind="image")])
    assert result["change_kind"] == "added"
    assert result["before"] == "отсутствует"
    assert result["requires_human_review"] is True


def test_llm_cannot_reference_evidence_outside_change_group():
    context = {"evidence": [_entry(1, before="16", after="17")]}
    candidate = {"before": "16", "after": "18", "change_summary": "изменено",
                 "change_kind": "changed", "confidence": .9,
                 "before_evidence_ids": ["raw_0001"], "after_evidence_ids": ["raw_9999"]}
    valid, reason = semantic_diff.validate_llm_result(candidate, context)
    assert valid is False
    assert reason == "evidence_outside_change_group"


def test_llm_cannot_claim_changed_without_both_sides_of_local_evidence():
    context = {"evidence": [_entry(1, change="added", after="17")]}
    candidate = {"before": "16", "after": "17", "change_summary": "изменено",
                 "change_kind": "changed", "confidence": .9,
                 "before_evidence_ids": [], "after_evidence_ids": ["raw_0001"]}
    assert semantic_diff.validate_llm_result(candidate, context) == (False, "before_has_no_local_evidence")


def test_deterministic_path_is_repeatable_and_does_not_mutate_input():
    evidence = [_entry(1, before="16 этажей", after="17 этажей")]
    before = copy.deepcopy(evidence)
    assert _analyze(evidence) == _analyze(evidence)
    assert evidence == before
