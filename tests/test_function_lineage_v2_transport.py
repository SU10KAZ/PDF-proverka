from __future__ import annotations

import copy
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.app.services.stage_comparison import function_lineage_shadow as lineage
from experiments.function_lineage_v2 import transport


@pytest.fixture(scope="session")
def projected():
    artifacts = transport.load_source_artifacts()
    contexts = {}
    shards = {}
    for pair_id, artifact in artifacts.items():
        contexts[pair_id], errors = transport.project_tasks(artifact)
        assert errors == []
        shards[pair_id], oversized = transport.shard_task_contexts(
            pair_id, artifact["input_signature"], contexts[pair_id],
        )
        assert oversized == []
    return artifacts, contexts, shards


def _contexts_by_task(values):
    return {str(value["task_id"]): value for value in values}


def test_candidate_absent_from_sheet_matcher_edges_is_in_task_context(projected):
    artifacts, contexts, _ = projected
    for pair_id, artifact in artifacts.items():
        by_task = _contexts_by_task(contexts[pair_id])
        candidates = {
            value["candidate_id"]: value
            for value in artifact["functional_candidates"]
        }
        for task in artifact["candidate_tasks"]:
            for candidate_id in task["candidate_ids"]:
                document = candidates[candidate_id].get("document_context") or {}
                if not document.get("sheet_matcher_edge_present"):
                    projected_ids = {
                        value["candidate_id"]
                        for value in by_task[task["task_id"]]["functional_candidates"]
                    }
                    assert candidate_id in projected_ids
                    return
    raise AssertionError("corpus has no candidate outside Sheet Matcher edges")


def test_all_generator_candidates_and_task_edges_survive_sharding(projected):
    artifacts, contexts, shards = projected
    for pair_id, artifact in artifacts.items():
        by_task = _contexts_by_task(contexts[pair_id])
        source_candidates = {
            value["candidate_id"] for value in artifact["functional_candidates"]
        }
        projected_candidates = {
            value["candidate_id"]
            for context in contexts[pair_id]
            for value in context["functional_candidates"]
        }
        assert projected_candidates == source_candidates
        for task in artifact["candidate_tasks"]:
            assert [
                value["candidate_id"]
                for value in by_task[task["task_id"]]["functional_candidates"]
            ] == task["candidate_ids"]
        sharded_tasks = [task_id for shard in shards[pair_id] for task_id in shard["task_ids"]]
        assert sorted(sharded_tasks) == sorted(by_task)
        assert len(sharded_tasks) == len(set(sharded_tasks))


def test_ios21_left19_r25_and_r30_remain_in_one_task(projected):
    artifacts, contexts, shards = projected
    controls = transport._ios21_controls(
        artifacts[transport.IOS21_PAIR_ID],
        contexts[transport.IOS21_PAIR_ID],
        shards[transport.IOS21_PAIR_ID],
    )
    value = controls["LEFT19_R30_R25"]
    assert value["present_together"] is True
    assert value["R30_rank"] == 1
    assert value["R25_rank"] == 2
    assert len(value["task_contexts"]) == 1


def test_ios21_left20_distributed_candidate_is_atomic(projected):
    artifacts, contexts, shards = projected
    controls = transport._ios21_controls(
        artifacts[transport.IOS21_PAIR_ID],
        contexts[transport.IOS21_PAIR_ID],
        shards[transport.IOS21_PAIR_ID],
    )
    value = controls["LEFT20_DISTRIBUTED"]
    assert value["candidate_id"] == "lcand_9c617494b14c2b922d3f"
    assert value["right_pages"] == [26, 28, 29]
    assert value["intact_in_every_task_context"] is True
    assert value["task_atomic_sharding"] is True
    assert all(len(row["component_map"]) == 3 for row in value["task_context_occurrences"])


def test_distinct_fragments_on_one_right_page_are_capacity_compatible(projected):
    artifacts, _, _ = projected
    for artifact in artifacts.values():
        candidates = artifact["functional_candidates"]
        for index, left in enumerate(candidates):
            for right in candidates[index + 1:]:
                if left["candidate_id"] == right["candidate_id"]:
                    continue
                shared_pages = set(left["right_pages"]) & set(right["right_pages"])
                if not shared_pages:
                    continue
                if set(left["right_capacity_keys"]) & set(right["right_capacity_keys"]):
                    continue
                mapped = {
                    left["candidate_id"]: left,
                    right["candidate_id"]: right,
                }
                assert lineage.verify_capacity([
                    {"candidate_id": left["candidate_id"]},
                    {"candidate_id": right["candidate_id"]},
                ], mapped) == []
                return
    raise AssertionError("no distinct-fragment same-page pair found")


def test_other_right_fragment_evidence_does_not_leak(projected):
    artifacts, contexts, _ = projected
    for pair_id, artifact in artifacts.items():
        catalog = artifact["evidence_catalog"]
        right_fragments = artifact["function_fragments"]["RIGHT"]
        for context in contexts[pair_id]:
            included_fragments = {
                fragment_id
                for candidate in context["functional_candidates"]
                for fragment_id in candidate["right_fragment_ids"]
            }
            included_pages = {
                page
                for candidate in context["functional_candidates"]
                for page in candidate["right_physical_pages"]
            }
            for fragment_id, fragment in right_fragments.items():
                if fragment_id in included_fragments or fragment["physical_page"] not in included_pages:
                    continue
                foreign = {
                    evidence_id for evidence_id in fragment["evidence_refs"]
                    if catalog[evidence_id]["provenance_type"] == lineage.FRAGMENT_OWNED_EVIDENCE
                    and catalog[evidence_id]["owner_fragment_id"] == fragment_id
                }
                if foreign:
                    assert foreign.isdisjoint(context["local_evidence"])
                    return
    raise AssertionError("no same-page foreign RIGHT fragment evidence found")


def test_sheet_shared_evidence_keeps_null_owners(projected):
    _, contexts, _ = projected
    facts = [
        fact
        for values in contexts.values()
        for context in values
        for fact in context["local_evidence"].values()
        if fact["provenance_type"] == lineage.SHEET_SHARED_EVIDENCE
    ]
    assert facts
    assert all(value["owner_function_id"] is None for value in facts)
    assert all(value["owner_fragment_id"] is None for value in facts)


def test_unknown_candidate_response_fails_closed(projected):
    _, _, shards = projected
    payload = next(iter(shards.values()))[0]["model_payload"]
    response = {
        "results": [
            {
                "task_id": context["task_id"],
                "decision": (
                    "lcand_unknown" if index == 0 else lineage.NEED_MORE_EVIDENCE
                ),
            }
            for index, context in enumerate(payload["task_contexts"])
        ],
    }
    verified = transport.verify_transport_response(payload, response)
    assert verified["ok"] is False
    first_task = payload["task_contexts"][0]["task_id"]
    assert verified["task_results"][first_task]["errors"] == [
        "UNKNOWN_CANDIDATE_ID"
    ]


def test_oversized_task_fails_without_candidate_truncation():
    context = {
        "task_id": "task_oversized",
        "left_function_core": {"task_id": "task_oversized"},
        "functional_candidates": [{
            "candidate_id": "candidate_kept",
            "evidence_ids": ["evidence_kept"],
        }],
        "local_evidence": {"evidence_kept": {"normalized_value": "x" * 360_000}},
        "allowed_decisions": ["candidate_kept", lineage.NEED_MORE_EVIDENCE],
    }
    shards, oversized = transport.shard_task_contexts(
        "pair", "signature", [context],
    )
    assert shards == []
    assert oversized[0]["reason_code"] == "TASK_CONTEXT_EXCEEDS_HARD_GATE"
    assert oversized[0]["candidate_count"] == 1
    assert oversized[0]["candidate_list_truncated"] is False


def test_projection_replay_is_byte_identical(tmp_path: Path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    transport.build_artifacts(first)
    transport.build_artifacts(second)
    for name in (
        "selector_transport_manifest.json",
        "selector_shards.jsonl",
        "selector_transport_metrics.json",
        "report.md",
    ):
        assert (first / name).read_bytes() == (second / name).read_bytes()
