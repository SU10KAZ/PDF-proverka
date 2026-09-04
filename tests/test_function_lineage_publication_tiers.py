"""Product safety tiers: nothing is auto-published until it earns eligibility."""
from __future__ import annotations

import pytest

from backend.app.services.stage_comparison import function_lineage_shadow as lineage


def test_nothing_is_auto_eligible_by_default():
    """No class has passed an independent acceptance holdout yet."""
    assert lineage.AUTO_ELIGIBLE_RELATIONS == frozenset()

    for relation in (
        "CONTINUED_1_TO_1", "MERGED_N_TO_1", "SPLIT_1_TO_N",
        "FUNCTION_DISTRIBUTED", "MIXED_RELATION", "UNRESOLVED_OR_INVALID",
    ):
        assert lineage.publication_tier(relation) == lineage.TIER_REVIEW


def test_need_more_evidence_is_never_an_auto_match():
    for proven in (frozenset(), frozenset({"CONTINUED_1_TO_1", "MERGED_N_TO_1"})):
        assert lineage.publication_tier(
            lineage.NEED_MORE_EVIDENCE, auto_eligible_relations=proven
        ) == lineage.TIER_REVIEW


def test_a_class_becomes_auto_only_when_explicitly_proven():
    proven = frozenset({"CONTINUED_1_TO_1"})

    assert lineage.publication_tier(
        "CONTINUED_1_TO_1", auto_eligible_relations=proven
    ) == lineage.TIER_AUTO
    # MERGED must earn eligibility on its own data.
    assert lineage.publication_tier(
        "MERGED_N_TO_1", auto_eligible_relations=proven
    ) == lineage.TIER_REVIEW


@pytest.mark.parametrize("relation", [
    "SPLIT_1_TO_N", "FUNCTION_DISTRIBUTED", "MIXED_RELATION",
    "UNRESOLVED_OR_INVALID", "RENAMED_FUNCTION", "FUNCTION_EXPANDED",
])
def test_group_and_mixed_classes_are_never_auto_eligible_implicitly(relation: str):
    """Proving 1:1 and N:1 must not silently admit anything else."""
    proven = frozenset({"CONTINUED_1_TO_1", "MERGED_N_TO_1"})

    assert lineage.publication_tier(
        relation, auto_eligible_relations=proven
    ) == lineage.TIER_REVIEW


def test_eligibility_candidates_are_documented_but_grant_nothing():
    assert lineage.ELIGIBILITY_CANDIDATE_RELATIONS == frozenset({
        "CONTINUED_1_TO_1", "MERGED_N_TO_1",
    })
    assert not lineage.ELIGIBILITY_CANDIDATE_RELATIONS & lineage.AUTO_ELIGIBLE_RELATIONS


def test_unknown_relation_falls_to_review():
    assert lineage.publication_tier(None) == lineage.TIER_REVIEW
    assert lineage.publication_tier("") == lineage.TIER_REVIEW
    assert lineage.publication_tier("SOMETHING_NEW") == lineage.TIER_REVIEW
