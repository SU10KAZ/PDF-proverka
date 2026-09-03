from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.app.services.stage_comparison import function_lineage_shadow as lineage
from experiments.function_lineage_v2 import scoped_transport


@pytest.fixture(scope="session")
def frozen_build():
    metrics, shards, values = scoped_transport._build_objects()
    return metrics, shards, values["critical"], values["metadata"]


def test_every_task_is_one_exact_scope_and_candidates_partition_raw(frozen_build):
    metrics, _, _, metadata = frozen_build
    contexts = metadata["contexts"]
    candidate_ids = [
        candidate["candidate_id"]
        for context in contexts
        for candidate in context["functional_candidates"]
    ]
    assert len(contexts) == 213
    assert len(candidate_ids) == 1461
    assert Counter(candidate_ids) == Counter(
        {candidate_id: 1 for candidate_id in metadata["raw_candidate_ids"]}
    )
    assert all(
        context["scope_id"] == context["function_scope_core"]["scope_id"]
        and context["scope_policy"]["task_identity"]
        == "ONE_EXACT_FUNCTION_SCOPE"
        and all(
            candidate["scope_relation"] == "EXACT_SCOPE"
            for candidate in context["functional_candidates"]
        )
        for context in contexts
    )
    assert metrics["forensically_preserved_raw_candidate_count"] == 1461
    assert metrics["candidate_partition_exact"] is True


def test_cross_granularity_candidates_are_not_selectable(frozen_build):
    metrics, _, _, metadata = frozen_build
    tasks = metadata["inputs"]["selector_tasks"]["tasks"]
    contexts = {value["task_id"]: value for value in metadata["contexts"]}
    for task in tasks:
        selectable = {
            value["candidate_id"]
            for value in contexts[task["scoped_task_id"]]["functional_candidates"]
        }
        related = {
            candidate_id
            for values in task["non_selectable_related_candidate_ids"].values()
            for candidate_id in values
        }
        assert selectable.isdisjoint(related)
    assert metrics["safety"]["cross_granularity_selectable_competition"] == 0


def test_ios21_critical_scopes(frozen_build):
    metrics, _, critical, _ = frozen_build
    tasks = {value["label"]: value for value in critical["tasks"]}
    assert list(tasks) == [
        "LEFT17",
        "LEFT18",
        "LEFT19",
        "LEFT20 DOMESTIC child",
        "LEFT20 FIRE child",
        "LEFT20 METERING child",
        "LEFT20 composite parent",
    ]
    assert any(
        pages == [27]
        for pages in tasks["LEFT17"]["candidate_right_pages"].values()
    )
    assert any(
        pages == [24]
        for pages in tasks["LEFT18"]["candidate_right_pages"].values()
    )
    left19_pages = list(tasks["LEFT19"]["candidate_right_pages"].values())
    assert [30] in left19_pages and [25] in left19_pages
    assert any(
        pages == [26]
        for pages in tasks["LEFT20 DOMESTIC child"][
            "candidate_right_pages"
        ].values()
    )
    assert any(
        pages == [28]
        for pages in tasks["LEFT20 FIRE child"]["candidate_right_pages"].values()
    )
    assert any(
        pages == [29]
        for pages in tasks["LEFT20 METERING child"][
            "candidate_right_pages"
        ].values()
    )
    parent = tasks["LEFT20 composite parent"]
    assert [26, 28, 29] in parent["candidate_right_pages"].values()
    assert all(
        pages not in ([26], [28], [29])
        for pages in parent["candidate_right_pages"].values()
    )
    assert metrics["ios21_controls"]["all_pass"] is True


def test_bounded_atomic_transport_and_safety(frozen_build):
    metrics, shards, _, _ = frozen_build
    assert metrics["payloads_over_target"] == 0
    assert metrics["safety"]["payloads_over_hard_gate"] == 0
    assert metrics["safety"]["oversized_task_count"] == 0
    assert metrics["safety"]["task_atomicity_defect_count"] == 0
    assert metrics["safety"]["candidate_list_truncations"] == 0
    assert metrics["safety"]["silent_truncations"] == 0
    assert all(
        shard["prompt_characters"] <= scoped_transport.TARGET_CHARACTERS
        for shard in shards
    )
    assert metrics["safety"]["RIGHT_MAP_CONFLICT"] == 0
    assert metrics["safety"]["capacity_defect_count"] == 0
    assert metrics["safety"]["search_failure_count"] == 0


def test_oversized_scope_fails_without_splitting_or_candidate_truncation():
    context = {
        "task_id": "fstask_oversized",
        "scope_id": "fscope_oversized",
        "functional_candidates": [{"candidate_id": "candidate_kept"}],
        "local_evidence": {"evidence": {"normalized_value": "x" * 360_000}},
        "allowed_decisions": ["candidate_kept", lineage.NEED_MORE_EVIDENCE],
    }
    shards, oversized = scoped_transport.shard_scoped_task_contexts(
        "pair", "signature", [context]
    )
    assert shards == []
    assert oversized == [
        {
            "pair_id": "pair",
            "task_id": "fstask_oversized",
            "scope_id": "fscope_oversized",
            "reason_code": "ATOMIC_SCOPE_CONTEXT_EXCEEDS_HARD_GATE",
            "characters": oversized[0]["characters"],
            "hard_gate": 350_000,
            "candidate_count": 1,
            "candidate_list_truncated": False,
            "task_split": False,
        }
    ]


def test_provider_safe_schema_and_parser_from_0655372c(frozen_build):
    metrics, shards, _, _ = frozen_build
    assert metrics["safety"]["provider_schema_problem_count"] == 0
    assert metrics["safety"]["provider_schema_contains_oneOf"] is False
    assert metrics["safety"]["provider_safe_parser"]["fail_closed"] is True
    assert all("oneOf" not in json.dumps(shard["output_schema"]) for shard in shards)
    shard = next(value for value in shards if len(value["task_ids"]) > 1)
    payload = shard["model_payload"]
    first, second = payload["task_contexts"][:2]
    foreign = first["functional_candidates"][0]["candidate_id"]
    if foreign in second["allowed_decisions"]:
        pytest.skip("first two frozen tasks happen to share a selectable candidate")
    response = {
        "results": [
            {
                "task_id": context["task_id"],
                "decision": (
                    foreign
                    if context["task_id"] == second["task_id"]
                    else lineage.NEED_MORE_EVIDENCE
                ),
            }
            for context in payload["task_contexts"]
        ]
    }
    checked = scoped_transport.verify_scoped_transport_response(payload, response)
    assert checked["ok"] is False
    assert checked["task_results"][second["task_id"]]["errors"] == [
        "CANDIDATE_ID_NOT_ALLOWED_FOR_TASK"
    ]


def test_raw_and_scope_eligible_recall_do_not_regress(frozen_build):
    metrics, _, _, _ = frozen_build
    assert metrics["recall"]["raw_candidate_recall"] == {
        "recall_at_1": 0.578947,
        "recall_at_3": 0.684211,
        "recall_at_5": 0.842105,
        "recall_at_10": 0.947368,
    }
    assert metrics["recall"]["scope_eligible_recall"] == {
        "recall_at_1": 0.789474,
        "recall_at_3": 0.842105,
        "recall_at_5": 0.894737,
        "recall_at_10": 0.947368,
    }
    assert metrics["recall"]["raw_no_regression"] is True
    assert metrics["recall"]["scope_eligible_no_regression"] is True


def test_pre_scope_shards_are_not_an_input(frozen_build):
    metrics, _, _, _ = frozen_build
    assert metrics["pre_scope_shards_used_as_ai_input"] is False
    assert metrics["model_calls"] == 0
    assert metrics["deploy"] is False
    assert metrics["shadow_enabled"] is False
    assert metrics["materialization"] is False
    assert metrics["vision"] is False


def test_two_full_replays_are_byte_identical(tmp_path: Path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    first_hashes = scoped_transport.build_artifacts(first)
    second_hashes = scoped_transport.build_artifacts(second)
    assert first_hashes == second_hashes
    assert set(first_hashes) == set(scoped_transport.ARTIFACT_NAMES)
    for name in scoped_transport.ARTIFACT_NAMES:
        assert (first / name).read_bytes() == (second / name).read_bytes()
