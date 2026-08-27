"""Replay the tracked G2.4.5 policy corpus v2 without historical v1."""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

import pytest

from backend.app.services.stage_comparison.unified_change_policy import (
    check_cardinality_safety,
    check_source_validity,
    evaluate_source_relation,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_PATH = ROOT / "tests/fixtures/g2_4_5_policy_cases_v2.json"
FIXTURE = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
CORPUS_V2 = ROOT / FIXTURE["corpus_path"]
FUNCTIONS = {
    "check_cardinality_safety": check_cardinality_safety,
    "check_source_validity": check_source_validity,
    "evaluate_source_relation": evaluate_source_relation,
}
ALLOWED_REPRESENTABILITY = {
    "REPRESENTABLE_IN_V1",
    "POLICY_FACTS_ONLY_IN_V1",
    "NOT_REPRESENTABLE_IN_V1",
    "NEGATIVE_CONTROL",
}
EXPECTED_CASE_IDS = [
    *[f"A{index}" for index in range(1, 20)],
    *[f"B{index}" for index in range(1, 10)],
]
CALLS = [
    (case, index, call)
    for case in FIXTURE["cases"]
    for index, call in enumerate(case["policy_calls"])
]


def _all_strings(value: Any) -> set[str]:
    if isinstance(value, str):
        return {value}
    if isinstance(value, dict):
        return set().union(*(_all_strings(item) for item in value.values()), set())
    if isinstance(value, list):
        return set().union(*(_all_strings(item) for item in value), set())
    return set()


def _verdict(result: dict[str, Any]) -> str:
    return result["relation_status"] or result["outcome"]


def test_v2_fixture_has_closed_traceable_schema():
    assert set(FIXTURE) == {
        "schema_version",
        "policy_contract",
        "corpus_path",
        "graphic_truth_root",
        "expected_verdict_distribution",
        "cases",
    }
    assert FIXTURE["schema_version"] == "g2.4.5-policy-cases-v2"
    assert FIXTURE["policy_contract"] == "unified-change-policy-v1"
    assert [case["case_id"] for case in FIXTURE["cases"]] == EXPECTED_CASE_IDS
    assert len({case["case_id"] for case in FIXTURE["cases"]}) == 28

    expected_fields = {
        "case_id",
        "title",
        "source_references",
        "observed_facts",
        "policy_calls",
        "expected_policy_facts",
        "reason",
        "representability",
    }
    for case in FIXTURE["cases"]:
        assert set(case) == expected_fields, case["case_id"]
        assert case["representability"] in ALLOWED_REPRESENTABILITY
        assert case["source_references"]
        assert case["expected_policy_facts"]
        assert case["reason"]
        if case["representability"] == "REPRESENTABLE_IN_V1":
            assert len(case["policy_calls"]) == 1
            assert case["policy_calls"][0]["function"] == "evaluate_source_relation"
        elif case["representability"] == "POLICY_FACTS_ONLY_IN_V1":
            assert case["policy_calls"]
            assert all(
                call["function"] != "evaluate_source_relation"
                for call in case["policy_calls"]
            )
        else:
            assert case["policy_calls"] == []


def test_corpus_v2_contains_every_machine_case_marker():
    text = CORPUS_V2.read_text(encoding="utf-8")

    assert "tests/fixtures/g2_4_5_policy_cases_v2.json" in text
    for case_id in EXPECTED_CASE_IDS:
        assert f"<!-- policy-case:{case_id} -->" in text


@pytest.mark.parametrize("case", FIXTURE["cases"], ids=lambda case: case["case_id"])
def test_case_source_references_exist_and_ids_are_real(case):
    for source in case["source_references"]:
        assert set(source) == {"path", "ids"}
        path = ROOT / source["path"]
        assert path.is_file(), (case["case_id"], source["path"])
        if not source["ids"]:
            continue
        values = _all_strings(json.loads(path.read_text(encoding="utf-8")))
        for source_id in source["ids"]:
            assert source_id in values, (case["case_id"], source_id)


@pytest.mark.parametrize(
    ("case", "call_index", "call"),
    CALLS,
    ids=[
        f"{case['case_id']}:{call['function']}:{index}"
        for case, index, call in CALLS
    ],
)
def test_v2_case_calls_production_policy_functions(case, call_index, call):
    assert set(call) == {"function", "inputs", "expected"}
    result = FUNCTIONS[call["function"]](**call["inputs"])

    for key, expected in call["expected"].items():
        assert result[key] == expected, (case["case_id"], call_index, key)


def test_v2_derived_verdict_distribution_is_not_padded_by_unrepresentable_cases():
    results = []
    for case in FIXTURE["cases"]:
        if case["representability"] != "REPRESENTABLE_IN_V1":
            continue
        call = case["policy_calls"][0]
        results.append(_verdict(FUNCTIONS[call["function"]](**call["inputs"])))

    assert Counter(results) == FIXTURE["expected_verdict_distribution"]
    assert "CORROBORATING" not in results
    assert "CONTRADICTORY" not in results


def test_correct_sides_graphic_truth_matches_corpus_v2_pilot():
    root = ROOT / FIXTURE["graphic_truth_root"]
    left = json.loads((root / "left_system_graph.json").read_text(encoding="utf-8"))
    right = json.loads((root / "right_system_graph.json").read_text(encoding="utf-8"))
    comparison = json.loads(
        (root / "comparison_result.json").read_text(encoding="utf-8")
    )

    assert comparison["left_graph"]["block_id"] == (
        "blk_2d72a6705eaf4d8c9ee1d6ff459b15a6"
    )
    assert comparison["right_graph"]["block_id"] == (
        "blk_039909ec039649a1b8209f059c95167b"
    )
    assert (len(left["nodes"]), len(left["edges"])) == (73, 99)
    assert (len(right["nodes"]), len(right["edges"])) == (82, 111)
    assert left["quality"]["outgoing_devices"] == 27
    assert right["quality"]["outgoing_devices"] == 30
    assert comparison["summary"]["changes_total"] == 4

    changes = {item["type"]: item for item in comparison["changes"]}
    assert changes["GROUP_COUNT_CHANGED"]["summary"].endswith("27 → 30.")
    assert "QS1 (SWITCH_DISCONNECTOR) → QF3 (CIRCUIT_BREAKER)" in changes[
        "NODE_TYPE_CHANGED"
    ]["summary"]
    assert not {
        "DETAIL_LEVEL_INCREASED",
        "NODE_ADDED",
        "NODE_REMOVED",
    }.intersection(item["type"] for item in comparison["changes"])
