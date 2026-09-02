from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.ai_sheet_matcher.core import PROJECT_CONFIG
from experiments.function_lineage_v1.core import (
    PASSPORT_FIELDS,
    RELATION_DOCUMENT_LINK,
    RELATION_FUNCTIONAL_ANALOGUE,
    build_function_lineage_dataset,
    build_selector_prompt,
    stable_consensus,
    verify_capacity,
    verify_selector_response,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def datasets():
    return {
        pair_id: build_function_lineage_dataset(REPO_ROOT, pair_id)
        for pair_id in PROJECT_CONFIG
    }


def _task(dataset, left_page: int):
    return next(value for value in dataset.tasks if value["left_physical_page"] == left_page)


def _candidate(dataset, left_page: int, right_pages: list[int], relation: str | None = None):
    return next(
        dataset.candidates[candidate_id]
        for candidate_id in _task(dataset, left_page)["candidate_ids"]
        if dataset.candidates[candidate_id]["right_pages"] == right_pages
        and (relation is None or dataset.candidates[candidate_id]["relation_type"] == relation)
    )


def test_document_link_and_functional_analogue_are_independent(datasets) -> None:
    dataset = datasets["pe336037597"]
    document = next(
        value for value in dataset.document_links
        if value["left_physical_page"] == 17 and value["right_physical_page"] == 7
    )
    functional = _candidate(dataset, 17, [27], "CONTINUED_1_TO_1")

    assert document["relation_namespace"] == RELATION_DOCUMENT_LINK
    assert document["functional_score_contribution"] == 0
    assert functional["relation_namespace"] == RELATION_FUNCTIONAL_ANALOGUE
    assert functional["candidate_id"] != document["document_link_id"]


def test_change_register_never_overrides_functional_candidate(datasets) -> None:
    dataset = datasets["pe336037597"]
    for left_page, register_page, graphic_page in ((17, 7, 27), (18, 8, 24), (19, 9, 25)):
        task = _task(dataset, left_page)
        assert all(register_page not in dataset.candidates[value]["right_pages"] for value in task["candidate_ids"])
        assert any(graphic_page in dataset.candidates[value]["right_pages"] for value in task["candidate_ids"])


def test_function_passport_v2_has_all_fields_and_provenance(datasets) -> None:
    dataset = datasets["pe336037597"]
    for side in ("LEFT", "RIGHT"):
        for passport in dataset.function_passports[side].values():
            assert set(PASSPORT_FIELDS) <= set(passport)
            assert set(PASSPORT_FIELDS) == set(passport["provenance"])
            assert passport["evidence_refs"]


def test_one_right_sheet_contains_multiple_atomic_fragments(datasets) -> None:
    dataset = datasets["pe336037597"]
    page29 = [
        value for value in dataset.function_fragments["RIGHT"].values()
        if value["physical_page"] == 29
    ]
    assert len(page29) >= 2
    assert {value["function_class"] for value in page29} >= {"METERING", "INCOMING_METERING"}
    assert len({value["capacity_key"] for value in page29}) == len(page29)


def test_same_right_page_can_participate_in_independent_lineages() -> None:
    candidates = {
        "a": {"right_capacity_keys": ["RIGHT:29:frag_meter"]},
        "b": {"right_capacity_keys": ["RIGHT:29:frag_incoming"]},
    }
    assert verify_capacity([
        {"candidate_id": "a"}, {"candidate_id": "b"},
    ], candidates) == []


def test_same_function_fragment_conflict_is_blocked() -> None:
    candidates = {
        "a": {"right_capacity_keys": ["RIGHT:29:frag_meter"]},
        "b": {"right_capacity_keys": ["RIGHT:29:frag_meter"]},
    }
    errors = verify_capacity([
        {"candidate_id": "a"}, {"candidate_id": "b"},
    ], candidates)
    assert len(errors) == 1
    assert errors[0].startswith("FUNCTION_FRAGMENT_CONFLICT:RIGHT:29:frag_meter")


def test_lineage_shapes_include_one_to_one_one_to_many_many_to_one_and_distributed(datasets) -> None:
    relations = {
        value["relation_type"]
        for dataset in datasets.values() for value in dataset.candidates.values()
    }
    assert {
        "CONTINUED_1_TO_1", "SPLIT_1_TO_N", "MERGED_N_TO_1", "FUNCTION_DISTRIBUTED",
    } <= relations


def test_ios21_old_graphic_sheet_5_uses_exact_component_fragments(datasets) -> None:
    dataset = datasets["pe336037597"]
    target = _candidate(dataset, 20, [26, 28, 29], "FUNCTION_DISTRIBUTED")
    mapping = {
        value["component_role"]: value["right_physical_page"]
        for value in target["component_map"]
    }
    assert mapping == {
        "DOMESTIC_PRESSURE_BOOST": 26,
        "FIRE_PRESSURE_BOOST": 28,
        "INCOMING_METERING": 29,
    }
    assert len(target["right_capacity_keys"]) == 3


def test_new_sheet_and_removed_sheet_do_not_imply_function_state(datasets) -> None:
    dataset = datasets["pe336037597"]
    _, payload = build_selector_prompt(dataset)
    response = {
        "payload_signature": payload["payload_signature"],
        "selections": [
            {
                "task_id": task["task_id"],
                "candidate_id": "FUNCTION_REMOVED" if index == 0 else "NEED_MORE_EVIDENCE",
            }
            for index, task in enumerate(dataset.tasks)
        ],
    }
    verified = verify_selector_response(dataset, payload["payload_signature"], response)
    first = verified["task_results"][dataset.tasks[0]["task_id"]]
    assert "FUNCTION_REMOVED_WITHOUT_EXHAUSTIVE_EVIDENCE" in first["errors"]
    assert all(value["relation_type"] != "NEW_FUNCTION" for value in dataset.candidates.values())


def test_ai_invented_fragment_and_evidence_are_rejected(datasets) -> None:
    dataset = datasets["pe336037597"]
    _, payload = build_selector_prompt(dataset)
    response = {
        "payload_signature": payload["payload_signature"],
        "selections": [
            {
                "task_id": task["task_id"],
                "candidate_id": "NEED_MORE_EVIDENCE",
                **({"fragment_ids": ["invented"], "evidence_refs": ["invented"]} if index == 0 else {}),
            }
            for index, task in enumerate(dataset.tasks)
        ],
    }
    verified = verify_selector_response(dataset, payload["payload_signature"], response)
    errors = verified["task_results"][dataset.tasks[0]["task_id"]]["errors"]
    assert "AI_INVENTED_FRAGMENT" in errors
    assert "AI_INVENTED_EVIDENCE" in errors


def test_incomplete_many_to_one_group_fails_closed(datasets) -> None:
    dataset = datasets["p19cd7f695a"]
    merge = next(
        value for value in dataset.candidates.values()
        if value["relation_type"] == "MERGED_N_TO_1"
        and all(page in {task["left_physical_page"] for task in dataset.tasks} for page in value["left_pages"])
    )
    _, payload = build_selector_prompt(dataset)
    selected_once = False
    response = {"payload_signature": payload["payload_signature"], "selections": []}
    for task in dataset.tasks:
        candidate_id = "NEED_MORE_EVIDENCE"
        if not selected_once and task["left_physical_page"] in merge["left_pages"]:
            candidate_id = merge["candidate_id"]
            selected_once = True
        response["selections"].append({"task_id": task["task_id"], "candidate_id": candidate_id})
    verified = verify_selector_response(dataset, payload["payload_signature"], response)
    assert f"INCOMPLETE_MERGE_GROUP:{merge['candidate_id']}" in verified["global_errors"]


def _records(dataset, choices: dict[str, str], *, failed: set[tuple[int, str]] | None = None):
    rows = []
    failed = failed or set()
    for cold_run in (1, 2, 3):
        for pass_name in ("A", "B"):
            is_failed = (cold_run, pass_name) in failed
            rows.append({
                "cold_run": cold_run,
                "pass_name": pass_name,
                "model_call": {"ok": not is_failed},
                "verification": {
                    "task_results": {
                        task["task_id"]: {
                            "ok": not is_failed,
                            "candidate_id": choices[task["task_id"]],
                        }
                        for task in dataset.tasks
                    },
                },
            })
    return rows


def test_two_pass_disagreement_fails_closed(datasets) -> None:
    dataset = datasets["pe336037597"]
    choices = {task["task_id"]: "NEED_MORE_EVIDENCE" for task in dataset.tasks}
    records = _records(dataset, choices)
    first_task = dataset.tasks[0]["task_id"]
    records[1]["verification"]["task_results"][first_task]["candidate_id"] = dataset.tasks[0]["candidate_ids"][0]
    decisions = stable_consensus(dataset, records)
    first = next(value for value in decisions if value["task_id"] == first_task)
    assert first["stable"] is False
    assert first["selected_candidate_id"] == "NEED_MORE_EVIDENCE"


def test_model_failure_fails_closed(datasets) -> None:
    dataset = datasets["pe336037597"]
    choices = {task["task_id"]: "NEED_MORE_EVIDENCE" for task in dataset.tasks}
    decisions = stable_consensus(dataset, _records(dataset, choices, failed={(2, "B")}))
    assert all(value["stable"] is False for value in decisions)


def test_deterministic_repeatability(datasets) -> None:
    first = datasets["pe336037597"]
    second = build_function_lineage_dataset(REPO_ROOT, "pe336037597")
    assert first.input_signature == second.input_signature
    assert first.document_links == second.document_links
    assert first.candidates == second.candidates
    assert first.tasks == second.tasks


def test_production_artifacts_unchanged() -> None:
    paths = [
        REPO_ROOT / "backend/app/services/stage_comparison/sheet_matcher.py",
        REPO_ROOT / "backend/app/services/stage_comparison/production_orchestrator.py",
    ]
    before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    build_function_lineage_dataset(REPO_ROOT, "pe336037597")
    after = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    assert before == after
