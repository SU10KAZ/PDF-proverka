"""Fragment-level capacity ownership: licences, fail-closed defaults, arity.

The suite is built from synthetic fragment identifiers only.  No page number,
project name or file name of any research corpus appears here.
"""
from __future__ import annotations

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


def _merged_family() -> dict[str, dict]:
    key = "RIGHT:7:frag_target"
    return {
        "child_a": _single("child_a", "frag_left_a", "frag_target", key),
        "child_b": _single("child_b", "frag_left_b", "frag_target", key),
        "parent": _group("parent", "MERGED_N_TO_1", [
            ("frag_left_a", "frag_target", key),
            ("frag_left_b", "frag_target", key),
        ]),
    }


def test_page_reuse_through_distinct_fragments_stays_free():
    candidates = {
        "a": _single("a", "frag_left_a", "frag_one", "RIGHT:9:frag_one"),
        "b": _single("b", "frag_left_b", "frag_two", "RIGHT:9:frag_two"),
    }

    assert lineage.verify_capacity(
        [{"candidate_id": "a"}, {"candidate_id": "b"}], candidates
    ) == []


def test_two_unrelated_lineages_cannot_reuse_one_exact_fragment():
    key = "RIGHT:9:frag_one"
    candidates = {
        "a": _single("a", "frag_left_a", "frag_one", key),
        "b": _single("b", "frag_left_b", "frag_one", key),
    }

    errors = lineage.verify_capacity(
        [{"candidate_id": "a"}, {"candidate_id": "b"}], candidates
    )

    assert errors == [f"FUNCTION_FRAGMENT_CONFLICT:{key}:a:b"]


def test_missing_component_map_is_never_a_licence():
    """Unknown ownership must stay fail-closed, exactly as before."""
    candidates = {
        "a": {"right_capacity_keys": ["RIGHT:9:frag_one"]},
        "b": {"right_capacity_keys": ["RIGHT:9:frag_one"]},
    }

    errors = lineage.verify_capacity(
        [{"candidate_id": "a"}, {"candidate_id": "b"}], candidates
    )

    assert len(errors) == 1
    assert errors[0].startswith("FUNCTION_FRAGMENT_CONFLICT:RIGHT:9:frag_one")


def test_certified_exact_child_union_children_are_co_owners():
    candidates = _merged_family()
    index = lineage.group_derivability_index(candidates)

    assert index["parent"]["classification"] == "EXACT_CHILD_UNION"
    assert index["parent"]["child_candidate_ids"] == ["child_a", "child_b"]

    report = lineage.capacity_ownership(
        [{"candidate_id": "child_a"}, {"candidate_id": "child_b"}], candidates
    )

    assert report["errors"] == []
    assert any("DERIVED_EXACT_CHILD_UNION" in value for value in report["licences"])


def test_derived_union_does_not_double_consume_with_its_parent():
    candidates = _merged_family()

    report = lineage.capacity_ownership([
        {"candidate_id": "child_a"},
        {"candidate_id": "parent"},
    ], candidates)

    assert report["errors"] == []
    assert any("DERIVED_COMPOSITE_OWNERSHIP" in value for value in report["licences"])


def test_partial_child_union_is_not_a_licence():
    key = "RIGHT:7:frag_target"
    other = "RIGHT:8:frag_other"
    candidates = {
        "child_a": _single("child_a", "frag_left_a", "frag_target", key),
        "rival": _single("rival", "frag_left_c", "frag_target", key),
        "parent": _group("parent", "MERGED_N_TO_1", [
            ("frag_left_a", "frag_target", key),
            ("frag_left_b", "frag_other", other),
        ]),
    }
    index = lineage.group_derivability_index(candidates)

    assert index["parent"]["classification"] == "PARTIAL_CHILD_UNION"
    assert lineage.verify_capacity(
        [{"candidate_id": "child_a"}, {"candidate_id": "rival"}], candidates
    ) == [f"FUNCTION_FRAGMENT_CONFLICT:{key}:child_a:rival"]


def test_non_decomposable_group_keeps_exclusive_capacity():
    key = "RIGHT:7:frag_target"
    candidates = {
        "split_a": _group("split_a", "SPLIT_1_TO_N", [
            ("frag_left_a", "frag_target", key),
            ("frag_left_a", "frag_next", "RIGHT:8:frag_next"),
        ]),
        "split_b": _group("split_b", "SPLIT_1_TO_N", [
            ("frag_left_b", "frag_target", key),
            ("frag_left_b", "frag_next", "RIGHT:8:frag_next"),
        ]),
    }
    index = lineage.group_derivability_index(candidates)

    assert index["split_a"]["classification"] == "NON_DECOMPOSABLE_GROUP"
    errors = lineage.verify_capacity(
        [{"candidate_id": "split_a"}, {"candidate_id": "split_b"}], candidates
    )

    assert len(errors) == 2
    assert all(value.startswith("FUNCTION_FRAGMENT_CONFLICT:") for value in errors)


def test_incompatible_merge_arity_is_rejected():
    """{A,B} -> R and {B,C} -> R are contradictory assertions, not a licence."""
    key = "RIGHT:7:frag_target"
    candidates = {
        "ab": _group("ab", "MERGED_N_TO_1", [
            ("frag_left_a", "frag_target", key),
            ("frag_left_b", "frag_target", key),
        ]),
        "bc": _group("bc", "MERGED_N_TO_1", [
            ("frag_left_b", "frag_target", key),
            ("frag_left_c", "frag_target", key),
        ]),
    }

    assert lineage.verify_capacity(
        [{"candidate_id": "ab"}, {"candidate_id": "bc"}], candidates
    ) == [f"FUNCTION_FRAGMENT_CONFLICT:{key}:ab:bc"]


def test_same_atomic_ownership_expressed_by_two_candidates_is_not_a_conflict():
    key = "RIGHT:7:frag_target"
    candidates = {
        "single": _single("single", "frag_left_a", "frag_target", key),
        "split": _group("split", "SPLIT_1_TO_N", [
            ("frag_left_a", "frag_target", key),
            ("frag_left_a", "frag_next", "RIGHT:8:frag_next"),
        ]),
    }

    report = lineage.capacity_ownership([
        {"candidate_id": "single"}, {"candidate_id": "split"},
    ], candidates)

    assert report["errors"] == []
    assert any("SAME_ATOMIC_OWNERSHIP" in value for value in report["licences"])


def test_capacity_identity_is_never_page_global():
    report = lineage.capacity_ownership([], {})

    assert report["capacity_identity"] == (
        "RIGHT physical_page + exact function_fragment_id"
    )


def test_licences_are_deterministic_and_order_independent():
    candidates = _merged_family()
    forward = lineage.capacity_ownership(
        [{"candidate_id": "child_a"}, {"candidate_id": "child_b"}], candidates
    )
    backward = lineage.capacity_ownership(
        [{"candidate_id": "child_b"}, {"candidate_id": "child_a"}], candidates
    )

    assert forward["licences"] == backward["licences"]
    assert forward["errors"] == backward["errors"] == []
