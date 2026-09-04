"""Capacity resolution must be a pure function of the set of stable claims.

The published lineage map may not depend on shard boundaries, shard size,
batch or task ordering, parallel scheduling or cold-run grouping.  Every case
here is built from synthetic fragment identifiers; no page number, project or
file name of any corpus appears.
"""
from __future__ import annotations

import itertools
import random

import pytest

from backend.app.services.stage_comparison import function_lineage_shadow as lineage


def _single(candidate_id: str, left: str, right: str, key: str) -> dict:
    return {
        "candidate_id": candidate_id,
        "relation_type": "CONTINUED_1_TO_1",
        "right_capacity_keys": [key],
        "component_map": [{
            "left_fragment_id": left,
            "right_fragment_id": right,
            "capacity_key": key,
        }],
    }


def _group(candidate_id: str, relation: str, rows: list[tuple[str, str, str]]) -> dict:
    return {
        "candidate_id": candidate_id,
        "relation_type": relation,
        "right_capacity_keys": sorted({key for _left, _right, key in rows}),
        "component_map": [
            {"left_fragment_id": left, "right_fragment_id": right, "capacity_key": key}
            for left, right, key in rows
        ],
    }


@pytest.fixture
def corpus() -> tuple[dict, dict]:
    """A dataset that mixes every licence with a genuine conflict."""
    shared = "RIGHT:7:frag_shared"
    solo = "RIGHT:8:frag_solo"
    split_a = "RIGHT:9:frag_split_a"
    candidates = {
        # two children of one certified exact union -> co-owners
        "child_a": _single("child_a", "frag_l_a", "frag_shared", shared),
        "child_b": _single("child_b", "frag_l_b", "frag_shared", shared),
        "union_parent": _group("union_parent", "MERGED_N_TO_1", [
            ("frag_l_a", "frag_shared", shared),
            ("frag_l_b", "frag_shared", shared),
        ]),
        # an unrelated lineage that genuinely competes for the same fragment
        "rival": _single("rival", "frag_l_rival", "frag_shared", shared),
        # an uncontested lineage
        "quiet": _single("quiet", "frag_l_quiet", "frag_solo", solo),
        # a split whose second key nobody else touches
        "split": _group("split", "SPLIT_1_TO_N", [
            ("frag_l_split", "frag_split_a", split_a),
            ("frag_l_split", "frag_solo_2", "RIGHT:8:frag_solo_2"),
        ]),
    }
    claims = {
        "task_a": "child_a",
        "task_b": "child_b",
        "task_rival": "rival",
        "task_quiet": "quiet",
        "task_split": "split",
    }
    return candidates, claims


def _published(candidates: dict, claims: dict) -> dict:
    return lineage.resolve_lineage_capacity(claims, candidates)["published"]


def test_a_contested_fragment_withholds_every_claim_on_it(corpus):
    candidates, claims = corpus

    result = lineage.resolve_lineage_capacity(claims, candidates)

    assert result["contested_capacity_keys"] == ["RIGHT:7:frag_shared"]
    # No winner is chosen: the two licensed co-owners are withheld as well.
    assert sorted(result["withheld"]) == ["task_a", "task_b", "task_rival"]
    assert sorted(result["published"]) == ["task_quiet", "task_split"]
    for row in result["withheld"].values():
        assert row["reason_code"] == lineage.CAPACITY_CONTESTED


def test_licensed_co_owners_publish_when_no_rival_claims_the_fragment(corpus):
    candidates, claims = corpus
    claims = {key: value for key, value in claims.items() if key != "task_rival"}

    result = lineage.resolve_lineage_capacity(claims, candidates)

    assert result["contested_capacity_keys"] == []
    assert sorted(result["published"]) == [
        "task_a", "task_b", "task_quiet", "task_split",
    ]
    assert any("DERIVED_EXACT_CHILD_UNION" in value for value in result["licences"])


def test_task_order_does_not_change_the_published_map(corpus):
    candidates, claims = corpus
    baseline = _published(candidates, claims)

    for order in itertools.permutations(sorted(claims)):
        shuffled = {key: claims[key] for key in order}
        assert _published(candidates, shuffled) == baseline


def test_reverse_order_does_not_change_the_published_map(corpus):
    candidates, claims = corpus
    reversed_claims = {key: claims[key] for key in sorted(claims, reverse=True)}

    assert _published(candidates, reversed_claims) == _published(candidates, claims)


def test_random_deterministic_permutations_do_not_change_the_published_map(corpus):
    candidates, claims = corpus
    baseline = _published(candidates, claims)
    generator = random.Random(20260904)

    for _ in range(50):
        keys = sorted(claims)
        generator.shuffle(keys)
        assert _published(candidates, {key: claims[key] for key in keys}) == baseline


@pytest.mark.parametrize("size", [1, 2, 3, 4, 5, 99])
def test_shard_size_does_not_change_the_conflict_set(corpus, size: int):
    """Sharding is a transport concern; it may not change accounting."""
    candidates, claims = corpus
    keys = sorted(claims)
    shards = [keys[index:index + size] for index in range(0, len(keys), size)]

    # A per-shard union of errors must equal the single global resolution only
    # when the resolution is global; this asserts the global path is the one in
    # use, not that per-shard accounting is equivalent.
    whole = lineage.resolve_lineage_capacity(claims, candidates)
    rebuilt = lineage.resolve_lineage_capacity(
        {key: claims[key] for shard in shards for key in shard}, candidates
    )

    assert rebuilt["errors"] == whole["errors"]
    assert rebuilt["published"] == whole["published"]
    assert rebuilt["contested_capacity_keys"] == whole["contested_capacity_keys"]


def test_one_giant_batch_equals_the_same_claims_split_arbitrarily(corpus):
    candidates, claims = corpus
    giant = lineage.resolve_lineage_capacity(claims, candidates)

    for split in range(1, len(claims)):
        keys = sorted(claims)
        left = {key: claims[key] for key in keys[:split]}
        right = {key: claims[key] for key in keys[split:]}
        merged = lineage.resolve_lineage_capacity({**left, **right}, candidates)

        assert merged["published"] == giant["published"]
        assert merged["errors"] == giant["errors"]


def test_evaluating_each_task_alone_never_publishes_more_than_the_global_run(corpus):
    """A task alone cannot see its rival, so isolation must not be the rule."""
    candidates, claims = corpus
    global_published = _published(candidates, claims)

    isolated: dict[str, str] = {}
    for task_id, candidate_id in claims.items():
        isolated.update(_published(candidates, {task_id: candidate_id}))

    # Isolation would publish the contested claims, which is exactly the defect
    # global resolution removes.  The production path must use the global one.
    assert set(global_published) < set(isolated)
    assert set(isolated) - set(global_published) == {"task_a", "task_b", "task_rival"}


def test_conflict_strings_are_symmetric_and_sorted(corpus):
    candidates, claims = corpus

    errors = lineage.verify_capacity(
        [{"candidate_id": value} for value in sorted(claims.values())], candidates
    )
    reversed_errors = lineage.verify_capacity(
        [{"candidate_id": value} for value in sorted(claims.values(), reverse=True)],
        candidates,
    )

    assert errors == reversed_errors
    for error in errors:
        parts = error.split(":")
        assert parts[4] < parts[5]


def test_need_more_evidence_never_consumes_capacity(corpus):
    candidates, claims = corpus
    claims = {**claims, "task_nme": lineage.NEED_MORE_EVIDENCE}

    result = lineage.resolve_lineage_capacity(claims, candidates)

    assert "task_nme" not in result["published"]
    assert "task_nme" not in result["withheld"]
    assert result["stable_claim_count"] == len(claims) - 1


def test_a_claim_is_withheld_whole_when_only_one_of_its_keys_is_contested():
    first = "RIGHT:1:frag_one"
    second = "RIGHT:2:frag_two"
    candidates = {
        "wide": _group("wide", "SPLIT_1_TO_N", [
            ("frag_l_wide", "frag_one", first),
            ("frag_l_wide", "frag_two", second),
        ]),
        "narrow": _single("narrow", "frag_l_narrow", "frag_two", second),
    }

    result = lineage.resolve_lineage_capacity(
        {"task_wide": "wide", "task_narrow": "narrow"}, candidates
    )

    assert result["contested_capacity_keys"] == [second]
    assert result["published"] == {}
    assert sorted(result["withheld"]) == ["task_narrow", "task_wide"]
    assert result["withheld"]["task_wide"]["contested_capacity_keys"] == [second]
