from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.function_lineage_v2 import scope_graph


def _mapping(
    left: str,
    right: str,
    *,
    left_page: int = 1,
    right_page: int = 2,
):
    return {
        "left_fragment_id": f"left-fragment-{left}",
        "left_function_id": f"left-function-{left}",
        "component_role": f"ROLE_{left.upper()}",
        "left_physical_page": left_page,
        "right_fragment_id": f"right-fragment-{right}",
        "right_function_id": f"right-function-{right}",
        "right_physical_page": right_page,
        "capacity_key": f"RIGHT:{right_page}:right-fragment-{right}",
    }


def _candidate(candidate_id: str, relation_type: str, *mappings, **extra):
    return {
        "candidate_id": candidate_id,
        "relation_type": relation_type,
        "component_map": list(mappings),
        "right_capacity_keys": sorted({row["capacity_key"] for row in mappings}),
        "evidence_refs": [],
        "explicit_contradictions": [],
        **extra,
    }


def _dataset(*candidates):
    mappings = [row for candidate in candidates for row in candidate["component_map"]]
    sources = {
        (
            row["left_fragment_id"],
            row["left_function_id"],
            row["component_role"],
            row["left_physical_page"],
        )
        for row in mappings
        if row.get("left_fragment_id")
        and row.get("left_function_id")
        and row.get("component_role")
    }
    fragments = {
        fragment_id: {
            "fragment_id": fragment_id,
            "function_id": function_id,
            "component_role": role,
            "function_class": role,
            "document_role": "GRAPHIC_SHEET",
            "physical_page": page,
            "evidence_refs": [],
        }
        for fragment_id, function_id, role, page in sources
    }
    passports = {
        function_id: {
            "function_id": function_id,
            "component_role": role,
            "function_class": role,
            "document_role": "GRAPHIC_SHEET",
            "evidence_refs": [],
        }
        for _, function_id, role, _ in sources
    }
    tasks = [
        {
            "task_id": f"task-{index}",
            "left_fragment_id": fragment_id,
            "left_function_id": function_id,
            "left_physical_page": page,
            "candidate_ids": [
                candidate["candidate_id"]
                for candidate in candidates
                if any(
                    row.get("left_fragment_id") == fragment_id
                    for row in candidate["component_map"]
                )
            ],
            "candidate_ranks": {
                candidate["candidate_id"]: rank
                for rank, candidate in enumerate(candidates, start=1)
                if any(
                    row.get("left_fragment_id") == fragment_id
                    for row in candidate["component_map"]
                )
            },
        }
        for index, (fragment_id, function_id, _, page) in enumerate(sorted(sources))
    ]
    return {
        "pair_id": "pe336037597",
        "function_fragments": {"LEFT": fragments, "RIGHT": {}},
        "function_passports": {"LEFT": passports, "RIGHT": {}},
        "functional_candidates": list(candidates),
        "candidate_tasks": tasks,
        "diagnostics": {"search_failures": [], "group_generation_failures": []},
    }


def test_atomic_scope_and_singleton_are_exact():
    assert scope_graph.classify_scope_relation(["component-a"], ["component-a"]) == "EXACT_SCOPE"
    assert scope_graph.selector_eligible("EXACT_SCOPE") is True


def test_composite_scope_is_an_exact_child_union():
    a = _mapping("a", "a")
    b = _mapping("b", "b")
    group = _candidate("group", "FUNCTION_DISTRIBUTED", a, b)
    children = [
        _candidate("child-a", "CONTINUED_1_TO_1", a),
        _candidate("child-b", "CONTINUED_1_TO_1", b),
    ]
    classification, child_ids = scope_graph.classify_group_derivability(group, children)
    assert classification == "EXACT_CHILD_UNION"
    assert child_ids == ["child-a", "child-b"]


def test_singleton_is_exact_for_child_and_subset_of_parent():
    assert scope_graph.classify_scope_relation(["a"], ["a"]) == "EXACT_SCOPE"
    assert scope_graph.classify_scope_relation(["a"], ["a", "b"]) == "STRICT_SUBSET"
    assert scope_graph.selector_eligible("STRICT_SUBSET") is False


def test_coverage_has_no_global_full_or_partial_label():
    candidate = ["domestic"]
    relations = {
        "child": scope_graph.classify_scope_relation(candidate, ["domestic"]),
        "parent": scope_graph.classify_scope_relation(
            candidate, ["domestic", "fire", "metering"]
        ),
    }
    assert relations == {"child": "EXACT_SCOPE", "parent": "STRICT_SUBSET"}


def test_one_to_many_is_one_source_component_scope():
    first = _mapping("a", "right-a")
    second = _mapping("a", "right-b", right_page=3)
    split = _candidate("split", "SPLIT_1_TO_N", first, second)
    model = scope_graph.build_scope_model(_dataset(split))
    assert len(scope_graph.candidate_source_keys(split)) == 1
    assert {row["scope_kind"] for row in model["scopes"]} == {"COMPONENT"}
    assert scope_graph.classify_group_derivability(split, [])[0] == "NON_DECOMPOSABLE_GROUP"


def test_many_to_one_remains_a_legitimate_composite():
    a = _mapping("a", "shared", right_page=7)
    b = _mapping("b", "shared", right_page=7)
    # Exact fragment capacity may intentionally be shared inside MERGED_N_TO_1.
    b["right_fragment_id"] = a["right_fragment_id"]
    b["right_function_id"] = a["right_function_id"]
    b["capacity_key"] = a["capacity_key"]
    merged = _candidate("merged", "MERGED_N_TO_1", a, b)
    child_a = _candidate("a", "CONTINUED_1_TO_1", a)
    child_b = _candidate("b", "CONTINUED_1_TO_1", b)
    model = scope_graph.build_scope_model(_dataset(merged, child_a, child_b))
    assert any(row["scope_kind"] == "COMPOSITE" for row in model["scopes"])
    assert scope_graph.classify_group_derivability(
        merged, [child_a, child_b]
    )[0] == "EXACT_CHILD_UNION"


def test_same_right_page_different_fragments_have_distinct_capacity():
    first = _mapping("a", "a", right_page=17)
    second = _mapping("b", "b", right_page=17)
    assert first["right_physical_page"] == second["right_physical_page"]
    assert first["capacity_key"] != second["capacity_key"]


@pytest.fixture(scope="session")
def frozen_payloads():
    payloads, objects = scope_graph._build_payloads()
    return payloads, objects


def test_left19_same_scope_ambiguity_is_preserved(frozen_payloads):
    _, objects = frozen_payloads
    value = objects["ios21_scope_forensics.json"]["LEFT19"]
    assert value["r30_r25_same_scope"] is True
    assert value["both_selector_eligible"] is True
    assert value["ambiguity_remains"] is True
    assert value["frozen_6_of_6_preference_used_as_truth"] is False


def test_unknown_scope_fails_closed():
    assert scope_graph.classify_scope_relation(None, ["known"]) == "UNKNOWN"
    assert scope_graph.classify_scope_relation([], ["known"]) == "UNKNOWN"
    assert scope_graph.selector_eligible("UNKNOWN") is False
    invalid = _mapping("unknown", "right")
    invalid["left_fragment_id"] = None
    candidate = _candidate("unknown", "CONTINUED_1_TO_1", invalid)
    dataset = _dataset(candidate)
    model = scope_graph.build_scope_model(dataset)
    artifact = scope_graph.build_candidate_memberships(
        {dataset["pair_id"]: dataset}, {dataset["pair_id"]: model}
    )
    assert [row["scope_kind"] for row in model["scopes"]] == ["UNKNOWN"]
    assert artifact["memberships"][0]["scope_relation"] == "UNKNOWN"
    assert artifact["memberships"][0]["selector_eligible"] is False


def test_component_identity_and_relation_have_no_page_rule():
    fragment = {
        "fragment_id": "fragment-a",
        "function_id": "function-a",
        "function_class": "CLASS_A",
        "component_role": "ROLE_A",
        "document_role": "GRAPHIC_SHEET",
        "evidence_refs": ["evidence-a"],
        "physical_page": 20,
    }
    passport = {
        "function_class": "CLASS_A",
        "component_role": "ROLE_A",
        "evidence_refs": ["evidence-a"],
    }
    first = scope_graph._component_identity_payload("pair", fragment, passport)
    fragment["physical_page"] = 987
    second = scope_graph._component_identity_payload("pair", fragment, passport)
    assert first == second
    assert scope_graph.classify_scope_relation(["a"], ["a", "b"]) == "STRICT_SUBSET"


def test_score_and_rank_do_not_define_scope():
    low = _candidate("low", "CONTINUED_1_TO_1", _mapping("a", "a"), source_score=-100, single_rank=99)
    high = _candidate("high", "FUNCTION_DISTRIBUTED", _mapping("a", "a"), _mapping("b", "b"), source_score=100, single_rank=1)
    assert scope_graph.classify_scope_relation(
        scope_graph.candidate_source_keys(low),
        scope_graph.candidate_source_keys(high),
    ) == "STRICT_SUBSET"
    low["source_score"], high["source_score"] = high["source_score"], low["source_score"]
    low["single_rank"], high["single_rank"] = high["single_rank"], low["single_rank"]
    assert scope_graph.classify_scope_relation(
        scope_graph.candidate_source_keys(low),
        scope_graph.candidate_source_keys(high),
    ) == "STRICT_SUBSET"


def test_deterministic_replay_is_byte_identical(frozen_payloads):
    first = scope_graph.build_artifacts()
    second = scope_graph.build_artifacts()
    assert first == second
    assert {
        name: scope_graph.hashlib.sha256(payload).hexdigest()
        for name, payload in first.items()
    } == {
        name: scope_graph.hashlib.sha256(payload).hexdigest()
        for name, payload in second.items()
    }


def test_frozen_controls_and_safety_metrics(frozen_payloads):
    _, objects = frozen_payloads
    controls = objects["ios21_scope_forensics.json"]
    metrics = objects["scope_metrics.json"]
    selector = objects["selector_tasks_scoped.json"]
    assert controls["LEFT17"]["eligible"] is True
    assert controls["LEFT18"]["eligible"] is True
    assert controls["LEFT20"]["singletons"]["R26"]["relation_to_parent"] == "STRICT_SUBSET"
    assert controls["LEFT20"]["distributed_candidate"]["exact_child_union"] is True
    assert selector["cross_granularity_competition"]["after"]["task_count"] == 0
    assert metrics["safety"]["candidate_loss_count"] == 0
    assert metrics["safety"]["RIGHT_MAP_CONFLICT"] == 0
    assert metrics["safety"]["model_calls"] == 0
