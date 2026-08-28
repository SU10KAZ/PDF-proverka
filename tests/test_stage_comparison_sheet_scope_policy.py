"""Только доказанная пара листов попадает в сравнение.

Тесты закрывают правило целиком: что считается доказанным, что уходит в
вопрос инженеру, и что ответ инженера действительно открывает область.
"""
from __future__ import annotations

import pytest

from backend.app.services.stage_comparison import sheet_scope_policy as policy


def _relation(**kw):
    base = {
        "relation_id": "srel_x",
        "status": "POSSIBLE",
        "relation_type": "MATCHED",
        "left_pages": [1],
        "right_pages": [2],
    }
    base.update(kw)
    return base


@pytest.mark.parametrize("status", ["HIGH", "USER_CONFIRMED", "CONFIRMED"])
def test_proven_status_is_effective(status):
    assert policy.is_effective(_relation(status=status)) is True
    assert policy.is_pending_confirmation(_relation(status=status)) is False


@pytest.mark.parametrize("status", ["POSSIBLE", "UNKNOWN", "MEDIUM", "LOW"])
def test_unproven_status_waits_for_the_engineer(status):
    relation = _relation(status=status)
    assert policy.is_effective(relation) is False
    assert policy.is_pending_confirmation(relation) is True


def test_stamp_exact_publishes_high_and_is_therefore_effective():
    # sheet_matcher publishes STAMP_EXACT as status HIGH with confidence 1.0.
    relation = _relation(
        status="HIGH", confidence=1.0, primary_source="STAMP_EXACT",
        reason_codes=["stamp_key_exact"],
    )
    assert policy.is_effective(relation) is True


def test_stamp_group_publishes_possible_and_is_therefore_held_back():
    # «План 3-15 этажей» covering «План 7 этажа» is a scope guess, not a read.
    relation = _relation(
        status="POSSIBLE", relation_type="MERGED", left_pages=[3, 4],
        primary_source="STAMP_GROUP", reason_codes=["stamp_floor_range_covers"],
    )
    assert policy.is_effective(relation) is False
    assert policy.is_pending_confirmation(relation) is True


def test_answered_relation_becomes_effective():
    relation = _relation(
        status="HIGH",
        human_decision={"decision_id": "hdecision_1", "answer": "YES"},
        review_required=False,
    )
    assert policy.has_resolving_human_decision(relation) is True
    assert policy.is_effective(relation) is True


def test_unsure_answer_does_not_open_the_scope():
    relation = _relation(
        status="POSSIBLE",
        human_decision={"decision_id": "hdecision_1", "answer": "UNSURE"},
        review_required=True,
    )
    assert policy.has_resolving_human_decision(relation) is False
    assert policy.is_effective(relation) is False


@pytest.mark.parametrize(
    "relation",
    [
        _relation(status="NO_MATCH"),
        _relation(status="CANDIDATE_SUPERSEDED"),
        _relation(relation_type="NO_MATCH"),
    ],
)
def test_rejected_relations_are_neither_compared_nor_asked_about(relation):
    assert policy.is_rejected(relation) is True
    assert policy.is_effective(relation) is False
    assert policy.is_pending_confirmation(relation) is False


def test_one_sided_relation_is_not_a_pair():
    relation = _relation(status="HIGH", right_pages=[])
    assert policy.is_effective(relation) is False
    assert policy.is_pending_confirmation(relation) is False


def test_pending_relations_report_pages_and_reason():
    pending = policy.pending_relations([
        _relation(relation_id="b", status="POSSIBLE", left_pages=[5], right_pages=[6]),
        _relation(relation_id="a", status="HIGH"),
        _relation(relation_id="c", status="NO_MATCH"),
    ])
    assert [item["relation_id"] for item in pending] == ["b"]
    assert pending[0]["left_pages"] == [5]
    assert pending[0]["right_pages"] == [6]
    assert pending[0]["reason_code"] == "sheet_relation_unconfirmed"


def test_eom_pair_keeps_the_one_proven_relation_and_drops_the_guesses():
    """Живая запись пары ЭОМ p19cd7f695a: 0.70 остаётся, 0.29-0.34 уходят."""
    relations = [
        _relation(relation_id="srel_5390618337efefdd8d86", status="HIGH",
                  confidence=0.703282, left_pages=[37], right_pages=[45]),
        _relation(relation_id="srel_02390058842b133d96eb", status="POSSIBLE",
                  confidence=0.288507, left_pages=[24], right_pages=[29]),
        _relation(relation_id="srel_a8df462d23a0db974354", status="POSSIBLE",
                  confidence=0.297754, left_pages=[25], right_pages=[40]),
        _relation(relation_id="srel_26e11039f77b5928c359", status="POSSIBLE",
                  confidence=0.340175, left_pages=[26], right_pages=[24]),
        _relation(relation_id="srel_b289f9fdf78efb2cec39", status="POSSIBLE",
                  confidence=0.309357, left_pages=[51], right_pages=[32]),
    ]
    effective = [item["relation_id"] for item in relations if policy.is_effective(item)]
    assert effective == ["srel_5390618337efefdd8d86"]
    assert len(policy.pending_relations(relations)) == 4
