"""Stamp identity in the production Sheet Matcher, and the end of greedy 1:1."""
from __future__ import annotations

from backend.app.services.stage_comparison.sheet_identity import parse_stamp_title
from backend.app.services.stage_comparison.sheet_matcher import match_sheets


def _sheet(page: int, stamp: str | None = None, **facts):
    record: dict = {"pdf_page": page, "title": "Часть 1. Архитектурные решения. Планы"}
    record.update(facts)
    if stamp is not None:
        identity = parse_stamp_title(stamp, page=page)
        assert identity is not None, stamp
        record["sheet_identity"] = identity.to_dict()
    return record


def _two_sided(result):
    return {
        (tuple(item["left_pages"]), tuple(item["right_pages"])): item
        for item in result["relations"]
        if item["left_pages"] and item["right_pages"]
    }


def test_equal_stamp_keys_pair_the_sheets_whatever_their_page_numbers_are():
    result = match_sheets(
        [
            _sheet(28, "Корпуса 1, 2. План 2 этажа"),
            _sheet(29, "Корпуса 1, 2. План 3 этажа"),
            _sheet(30, "Корпуса 1, 2. План 4 этажа"),
        ],
        [
            _sheet(7, "Корпуса 1, 2. План 2 этажа"),
            _sheet(8, "Корпуса 1, 2. План 3 этажа"),
            _sheet(9, "Корпуса 1, 2. План 4 этажа"),
        ],
    )

    pairs = _two_sided(result)
    assert set(pairs) == {((28,), (7,)), ((29,), (8,)), ((30,), (9,))}
    for relation in pairs.values():
        assert relation["relation_type"] == "MATCHED"
        assert relation["status"] == "HIGH"
        assert relation["primary_source"] == "STAMP_EXACT"
        assert relation["confidence"] == 1.0
        assert "stamp_key_exact" in relation["reason_codes"]
        assert relation["provenance"]["page_number_is_primary"] is False
    assert result["diagnostics"]["stamp_exact_relations"] == 3
    assert result["diagnostics"]["greedy_assignment_used"] is False
    assert result["diagnostics"]["uses_model"] is False


def test_page_proximity_never_outvotes_a_proven_different_stamp():
    # Identical vocabulary on both floors; only the stamp tells them apart.
    rooms = {"functional_content": ["холл", "спальня", "с/у"]}
    result = match_sheets(
        [_sheet(29, "Корпуса 1, 2. План 3 этажа", **rooms)],
        [
            _sheet(29, "Корпуса 1, 2. План 4 этажа", **rooms),
            _sheet(8, "Корпуса 1, 2. План 3 этажа", **rooms),
        ],
    )

    pairs = _two_sided(result)
    assert set(pairs) == {((29,), (8,))}
    conflicting = next(
        candidate
        for candidate in result["candidate_search"][0]["deep_candidates"]
        if candidate["right_page"] == 29
    )
    assert conflicting["status"] == "NO_MATCH"
    assert "stamp_key_conflict" in conflicting["reason_codes"]


def test_two_sheets_with_one_key_become_a_question_not_a_first_match():
    result = match_sheets(
        [
            _sheet(3, "План первого подземного этажа"),
            _sheet(25, "План первого подземного этажа"),
        ],
        [_sheet(4, "План первого подземного этажа")],
    )

    ambiguous = [
        item for item in result["relations"]
        if "stamp_key_ambiguous" in item["reason_codes"]
    ]
    assert len(ambiguous) == 1
    relation = ambiguous[0]
    assert relation["relation_type"] == "UNCERTAIN"
    assert relation["status"] == "UNKNOWN"
    assert relation["automatic_scope"] is False
    assert relation["left_pages"] == [3, 25]
    assert relation["right_pages"] == [4]
    # Nothing was silently paired off.
    assert not _two_sided(result) or set(_two_sided(result)) == {((3, 25), (4,))}
    assert result["diagnostics"]["stamp_ambiguous_keys"] == [
        {"stamp_key": relation["evidence"][-1]["stamp_key"],
         "left_pages": [3, 25], "right_pages": [4]}
    ]


def test_a_floor_range_sheet_merges_its_floors_instead_of_pairing_with_one():
    result = match_sheets(
        [
            _sheet(41, "Корпус 4. План 7 этажа"),
            _sheet(42, "Корпус 4. План 8 этажа"),
        ],
        [_sheet(21, "Корпус 4. План 3-15 этажей")],
    )

    pairs = _two_sided(result)
    assert set(pairs) == {((41, 42), (21,))}
    relation = pairs[((41, 42), (21,))]
    assert relation["relation_type"] == "MERGED"
    assert relation["primary_source"] == "STAMP_GROUP"
    assert relation["status"] == "POSSIBLE"
    assert "stamp_floor_range_covers" in relation["reason_codes"]
    assert result["diagnostics"]["stamp_group_relations"] == 1


def test_sheets_without_a_stamp_fall_back_to_the_content_signals():
    facts = {
        "functional_content": ["щит освещения", "групповая сеть"],
        "main_entities": ["ЩО-1", "ЩО-2"],
        "relationships": ["ЩО-1 -> ЩО-2"],
    }
    result = match_sheets([_sheet(3, None, **facts)], [_sheet(9, None, **facts)])

    pairs = _two_sided(result)
    assert set(pairs) == {((3,), (9,))}
    relation = pairs[((3,), (9,))]
    assert relation["primary_source"] == "CONTENT"
    assert relation["status"] == "HIGH"
    assert result["diagnostics"]["left_pages_with_stamp_identity"] == 0
    assert result["diagnostics"]["right_pages_with_stamp_identity"] == 0


def test_a_strong_pair_is_not_lost_to_a_weaker_one_that_walked_in_first():
    # LEFT 34 scores 0.53 against RIGHT 3; LEFT 41 scores much higher against
    # the same page.  Walking LEFT in page order used to hand RIGHT 3 to 34.
    strong = {
        "functional_content": ["распределительный пункт", "вводная панель", "секция шин"],
        "main_entities": ["ВРУ-1", "ЩР-4", "QF12"],
        "relationships": ["ВРУ-1 -> ЩР-4", "ЩР-4 -> QF12"],
    }
    weak = {
        "functional_content": ["распределительный пункт", "перемычка"],
        "main_entities": ["ЩР-9"],
        "relationships": ["перемычка -> ЩР-9"],
    }
    result = match_sheets(
        [_sheet(34, None, **weak), _sheet(41, None, **strong)],
        [_sheet(3, None, **strong)],
    )

    pairs = _two_sided(result)
    assert set(pairs) == {((41,), (3,))}
    assert 34 in result["unmatched_left_pages"]


def test_a_displaced_high_candidate_is_reported_rather_than_dropped():
    shared = {
        "functional_content": ["распределительный пункт", "вводная панель", "секция шин"],
        "main_entities": ["ВРУ-1", "ЩР-4", "QF12"],
        "relationships": ["ВРУ-1 -> ЩР-4", "ЩР-4 -> QF12"],
    }
    result = match_sheets(
        [_sheet(10, None, **shared), _sheet(11, None, **shared)],
        [_sheet(20, None, **shared)],
    )

    displaced = result["diagnostics"]["displaced_high_candidates"]
    assert displaced, "a HIGH candidate that lost its page must stay visible"
    assert {item["reason_code"] for item in displaced} == {"high_candidate_displaced"}
    loser = next(
        item for item in result["relations"]
        if not item["right_pages"] and "high_candidate_displaced" in item["reason_codes"]
    )
    assert loser["conflicting_evidence"]
    assert loser["automatic_scope"] is False


def test_the_relation_carries_its_stamp_evidence_for_the_question_card():
    result = match_sheets(
        [_sheet(33, "Корпуса 1, 2. План кровли")],
        [_sheet(12, "Корпуса 1, 2. План кровли")],
    )

    relation = _two_sided(result)[((33,), (12,))]
    stamp = next(item for item in relation["evidence"] if item.get("kind") == "STAMP_IDENTITY")
    assert stamp["sheet_kind"] == "ROOF"
    assert stamp["buildings"] == ["1", "2"]
    assert stamp["left_stamp_text"] == "Корпуса 1, 2. План кровли"
    assert stamp["right_stamp_text"] == "Корпуса 1, 2. План кровли"
    assert stamp["title_used"] is False
    assert stamp["page_proximity_used"] is False


def test_identity_page_must_agree_with_the_record_it_travels_on():
    record = _sheet(29, "Корпуса 1, 2. План 3 этажа")
    record["sheet_identity"]["page"] = 30

    try:
        match_sheets([record], [_sheet(8, "Корпуса 1, 2. План 3 этажа")])
    except ValueError as error:
        assert "identity page mismatch" in str(error)
    else:  # pragma: no cover - the guard must fire
        raise AssertionError("a mislabelled identity must not be accepted")


def test_page_mode_selection_is_never_gated_by_the_stamp_matcher():
    from backend.app.services.stage_comparison.sheet_matcher import (
        page_selection_suggestions,
    )

    relations = match_sheets(
        [_sheet(29, "Корпуса 1, 2. План 3 этажа")],
        [_sheet(8, "Корпуса 1, 2. План 3 этажа")],
    )

    # The user picked two pages the stamp says are different sheets.
    result = page_selection_suggestions([29], [12], relations)

    assert result["selection_preserved"] is True
    assert result["sheet_matcher_is_gate"] is False
    assert result["selected_scope"] == {"left_pages": [29], "right_pages": [12]}
    # The proven pair is offered as advice, not imposed.
    assert [item["suggested_right_pages"] for item in result["suggestions"]] == [[8]]
    assert all(item["applied"] is False for item in result["suggestions"])


def test_a_sheet_question_names_the_sheets_it_asks_about():
    from backend.app.services.stage_comparison.review_queue import build_review_queue

    relations = match_sheets(
        [
            _sheet(28, "Корпуса 1, 2. План 2 этажа",
                   functional_content=["холл", "спальня"]),
            _sheet(42, "Корпус 4. План 3-15 этажей",
                   functional_content=["холл", "спальня"]),
        ],
        [_sheet(9, functional_content=["холл", "спальня"])],
    )
    queue = build_review_queue(sheet_relations=relations, generated_at="fixed")
    question = next(
        item for item in queue["questions"] if item["category"] == "SHEET"
    )

    assert "Корпуса 1, 2. План 2 этажа" in question["prompt"]
    assert "стр. 28" in question["prompt"]
    assert "LEFT" not in question["prompt"] and "RIGHT" not in question["prompt"]
    assert "srel_" not in question["prompt"]
    assert question["context"]["left_sheets"] == [
        {"page": 28, "label": "Корпуса 1, 2. План 2 этажа"},
        {"page": 42, "label": "Корпус 4. План 3-15 этажей"},
    ]
    assert question["context"]["right_sheets"] == [{"page": 9, "label": "Часть 1. Архитектурные решения. Планы"}]
    labels = [option["label"] for option in question["answer_options"]]
    assert not any("N→1" in label or "1→N" in label for label in labels)


def test_a_sheet_question_says_why_the_link_was_proposed():
    from backend.app.services.stage_comparison.review_queue import build_review_queue

    shared = {"functional_content": ["план этажа", "экспликация помещений"]}
    relations = match_sheets(
        [
            _sheet(28, "Корпуса 1, 2. План 2 этажа", **shared),
            _sheet(42, "Корпус 4. План 3-15 этажей", **shared),
        ],
        [_sheet(9, None, **shared)],
    )
    queue = build_review_queue(sheet_relations=relations, generated_at="fixed")
    question = next(item for item in queue["questions"] if item["category"] == "SHEET")

    why = question["context"]["why_proposed"]
    assert why
    assert all(isinstance(item, str) and item for item in why)
    # Reasons are sentences, not codes.
    assert not any("_" in item for item in why)
    assert "совпадает назначение листа" in why
