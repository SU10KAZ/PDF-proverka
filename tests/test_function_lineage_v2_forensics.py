from __future__ import annotations

import copy
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.function_lineage_v2 import forensics


def _component(left: str, right: str, *, page: int = 1):
    return {
        "left_fragment_id": f"left-{left}",
        "left_function_id": f"left-function-{left}",
        "component_role": f"role-{left}",
        "left_physical_page": page,
        "right_fragment_id": f"right-{right}",
        "right_function_id": f"right-function-{right}",
        "right_physical_page": page,
        "capacity_key": f"RIGHT:{page}:right-{right}",
    }


def _candidate(candidate_id: str, *components, **extra):
    return {
        "candidate_id": candidate_id,
        "component_map": list(components),
        "explicit_contradictions": [],
        **extra,
    }


def test_exact_subset_is_recognized():
    component_a = _component("a", "a")
    component_b = _component("b", "b")
    assert forensics.classify_candidate_relation(
        _candidate("a", component_a),
        _candidate("b", component_a, component_b),
    ) == "STRICT_SUBSET"


def test_overlap_is_not_subset():
    shared = _component("shared", "shared")
    first = _candidate("a", shared, _component("a", "a"))
    second = _candidate("b", shared, _component("b", "b"))
    assert forensics.classify_candidate_relation(first, second) == "OVERLAP"


def test_different_fragments_on_same_page_are_allowed():
    first = _candidate("a", _component("a", "a", page=17))
    second = _candidate("b", _component("b", "b", page=17))
    assert forensics.classify_candidate_relation(first, second) == "DISJOINT"


def test_group_atomicity_is_preserved():
    component_a = _component("a", "a")
    group = _candidate("group", component_a, _component("b", "b"))
    before = copy.deepcopy(group)
    assert forensics.classify_candidate_relation(
        group, _candidate("single", component_a)
    ) == "STRICT_SUPERSET"
    assert group == before
    assert len(group["component_map"]) == 2


def test_rank_and_score_do_not_determine_classification():
    component_a = _component("a", "a")
    component_b = _component("b", "b")
    low = _candidate("low", component_a, rank=99, source_score=-1000)
    high = _candidate("high", component_a, component_b, rank=1, source_score=1000)
    assert forensics.classify_candidate_relation(low, high) == "STRICT_SUBSET"
    low["rank"], high["rank"] = high["rank"], low["rank"]
    low["source_score"], high["source_score"] = (
        high["source_score"], low["source_score"]
    )
    assert forensics.classify_candidate_relation(low, high) == "STRICT_SUBSET"


def test_unknown_fields_do_not_become_evidence():
    unknown = _candidate(
        "unknown",
        {
            "left_fragment_id": None,
            "left_function_id": "left-function",
            "component_role": "role",
            "right_fragment_id": "right-fragment",
            "right_function_id": "right-function",
        },
    )
    known = _candidate("known", _component("a", "a"))
    assert forensics.classify_candidate_relation(unknown, known) == "UNKNOWN"


def test_classifier_has_no_page_specific_behavior():
    def result(page: int):
        component_a = _component("a", "a", page=page)
        return forensics.classify_candidate_relation(
            _candidate("single", component_a),
            _candidate("group", component_a, _component("b", "b", page=page)),
        )

    assert result(20) == result(987) == "STRICT_SUBSET"


def test_coverage_requires_an_explicit_scope():
    component_a = _component("a", "a")
    component_b = _component("b", "b")
    single = _candidate("single", component_a)
    required = forensics.left_component_set(
        _candidate("group", component_a, component_b)
    )
    assert forensics.coverage_scope(single, required) == "PARTIAL"
    assert forensics.coverage_scope(single, []) == "UNKNOWN"


def test_deterministic_replay_is_byte_identical():
    assert forensics.build_artifacts() == forensics.build_artifacts()
