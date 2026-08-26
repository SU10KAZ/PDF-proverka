"""Structured replay of the frozen G2.4.5 A1–A19/B1–B9 reference."""
from __future__ import annotations

from collections import Counter
from pathlib import Path

import pytest

from backend.app.services.stage_comparison.unified_change_policy import (
    evaluate_source_relation,
)


ROOT = Path(__file__).resolve().parents[1]
FROZEN_CORPUS = ROOT / "docs/research/g2_4_5_policy_corpus.md"
CHECKED_BOTH = {"LEFT": "CHECKED", "RIGHT": "CHECKED"}


def _both(**overrides):
    values = {
        "text_state": "VALID",
        "graphic_state": "VALID",
        "text_dimension": "PARAMETER",
        "graphic_dimension": "PARAMETER",
        "coverage_by_side": CHECKED_BOTH,
    }
    values.update(overrides)
    return values


CASES = [
    ("A1", _both(text_dimension="STRUCTURE", graphic_dimension="QUANTITY"), "COMPLEMENTARY"),
    ("A2", {"text_state": "ABSENT", "graphic_state": "VALID"}, "SINGLE_SOURCE"),
    ("A3", {"text_state": "VALID", "graphic_state": "NOT_CHECKED"}, "SINGLE_SOURCE"),
    ("A4", {"text_state": "VALID", "graphic_state": "NOT_APPLICABLE"}, "SINGLE_SOURCE"),
    ("A5", {"text_state": "ABSENT", "graphic_state": "REVIEW_REQUIRED"}, "REVIEW_REQUIRED"),
    ("A6", _both(text_count=1, graphic_count=2), "REVIEW_REQUIRED"),
    ("A7", _both(subject_relation="DIFFERENT_ENTITY"), "UNRELATED"),
    ("A8", _both(scope_compatible=False), "UNRELATED"),
    ("A9", _both(subject_relation="UNKNOWN"), "REVIEW_REQUIRED"),
    ("A10", _both(subject_relation="DIFFERENT_ENTITY"), "UNRELATED"),
    ("A11", {"text_state": "ABSENT", "graphic_state": "REVIEW_REQUIRED"}, "REVIEW_REQUIRED"),
    ("A12", {"text_state": "ABSENT", "graphic_state": "REVIEW_REQUIRED"}, "REVIEW_REQUIRED"),
    ("A13", {"text_state": "ABSENT", "graphic_state": "VALID"}, "SINGLE_SOURCE"),
    ("A14", _both(subject_relation="DIFFERENT_ENTITY"), "UNRELATED"),
    ("A15", _both(subject_relation="RELATED_ENTITY"), "COMPLEMENTARY"),
    ("A18", _both(text_count=1, graphic_count=6), "REVIEW_REQUIRED"),
    ("A19", _both(subject_relation="RELATED_ENTITY"), "COMPLEMENTARY"),
    ("B1", {"text_state": "VALID", "graphic_state": "ABSENT"}, "SINGLE_SOURCE"),
    ("B2", {"text_state": "VALID", "graphic_state": "ABSENT"}, "SINGLE_SOURCE"),
    (
        "B3",
        {"text_state": "VALID", "graphic_state": "ABSENT", "text_outcome": "DETAIL_ONLY"},
        "SINGLE_SOURCE",
    ),
    ("B4", {"text_state": "REVIEW_REQUIRED", "graphic_state": "ABSENT"}, "REVIEW_REQUIRED"),
    ("B5", {"text_state": "REVIEW_REQUIRED", "graphic_state": "ABSENT"}, "REVIEW_REQUIRED"),
    ("B6", {"text_state": "VALID", "graphic_state": "ABSENT"}, "SINGLE_SOURCE"),
    ("B7", {"text_state": "VALID", "graphic_state": "ABSENT"}, "SINGLE_SOURCE"),
    ("B8", {"text_state": "VALID", "graphic_state": "ABSENT"}, "SINGLE_SOURCE"),
    ("B9", {"text_state": "REVIEW_REQUIRED", "graphic_state": "ABSENT"}, "REVIEW_REQUIRED"),
]


def _verdict(result):
    return result["relation_status"] or result["outcome"]


def test_frozen_reference_declares_the_same_case_summary():
    text = FROZEN_CORPUS.read_text(encoding="utf-8")
    for expected in (
        "| `MERGE` | **0** |",
        "| `COMPLEMENTARY` | 3 |",
        "| `CONTRADICTORY` | **0** |",
        "| `REVIEW_REQUIRED` | 9 |",
        "| `UNRELATED` | 4 |",
        "| `SINGLE_SOURCE` | 10 |",
    ):
        assert expected in text


@pytest.mark.parametrize(("case_id", "facts", "expected"), CASES)
def test_frozen_corpus_case(case_id, facts, expected):
    result = evaluate_source_relation(**facts)
    assert _verdict(result) == expected, case_id
    assert "merge" not in result


def test_frozen_corpus_aggregate_has_no_merge_or_contradiction():
    results = [_verdict(evaluate_source_relation(**facts)) for _, facts, _ in CASES]
    assert Counter(results) == {
        "COMPLEMENTARY": 3,
        "REVIEW_REQUIRED": 9,
        "UNRELATED": 4,
        "SINGLE_SOURCE": 10,
    }
    assert "CONTRADICTORY" not in results
    assert all("merge" not in evaluate_source_relation(**facts) for _, facts, _ in CASES)
