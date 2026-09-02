from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.ai_sheet_matcher.core import PROJECT_CONFIG
from experiments.ai_sheet_matcher_v4.core import (
    GROUP_SHORTLIST_LIMIT,
    build_group_audit,
    build_selector_payload,
    build_v4_selector_dataset,
    subset_selector_dataset,
    verify_v4_selector_response,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def datasets():
    return {
        pair_id: build_v4_selector_dataset(REPO_ROOT, pair_id)
        for pair_id in PROJECT_CONFIG
    }


def test_only_v4_candidate_and_group_ids_are_exposed(datasets) -> None:
    for dataset in datasets.values():
        for task in dataset.selector.tasks:
            assert len(task["page_candidate_ids"]) == 10
            assert len(task["group_candidate_ids"]) <= GROUP_SHORTLIST_LIMIT
            assert all(value.startswith("vcand_") for value in task["page_candidate_ids"])
            assert all(value.startswith("fcand_") for value in task["group_candidate_ids"])
            assert task["option_ids"][-2:] == ["NO_ANALOG", "NEED_MORE_EVIDENCE"]


def test_group_shortlist_recall_is_100_percent_without_reference_ranking(datasets) -> None:
    audit = build_group_audit(list(datasets.values()))

    assert audit["reference_used_for_shortlist"] is False
    assert audit["summary"]["group_recall_after_shortlist"] == 1.0
    assert audit["summary"]["evaluation_hits_after_shortlist"] == 7
    ios21 = datasets["pe336037597"]
    target = next(
        row for row in ios21.group_shortlists[20]
        if row["candidate_group_id"] == "fcand_6294159aac7851a636dd"
    )
    assert target["right_pages"] == [26, 28, 29]
    assert target["shortlist_rank"] <= GROUP_SHORTLIST_LIMIT


def test_payload_has_required_v4_evidence_and_omits_page_proximity(datasets) -> None:
    dataset = datasets["pe336037597"]
    payload = build_selector_payload(dataset)
    candidate = next(
        row for row in payload["page_candidates"] if row["left_physical_page"] == 17
    )

    assert payload["candidate_generator"] == "research-candidate-generator.v4"
    assert set(candidate["retrieval_channel_evidence"]) == {
        "FUNCTION", "ENTITY", "OBJECT_ZONE", "TOPOLOGY", "TITLE_STAMP", "NEIGHBOR_TOC",
    }
    assert set(candidate["field_evidence_refs"]) == {
        "sheet_sides", "entity_evidence", "object_zone_evidence", "topology_evidence",
        "stamp_title_evidence", "neighbor_toc_evidence",
    }
    assert "page_proximity_signal" not in str(payload)
    assert "reference_cases" not in payload
    assert "human_by_left" not in payload


def test_v4_verifier_accepts_real_group_and_rejects_wrong_evidence_page(datasets) -> None:
    dataset = datasets["pe336037597"]
    payload = build_selector_payload(dataset)
    target_id = "fcand_6294159aac7851a636dd"
    response = {
        "payload_signature": payload["payload_signature"],
        "selections": [
            {
                "task_id": task["task_id"],
                "local_option_id": target_id if task["left_page"] == 20 else "NEED_MORE_EVIDENCE",
                "map_option_id": target_id if task["left_page"] == 20 else "NEED_MORE_EVIDENCE",
            }
            for task in dataset.selector.tasks
        ],
    }

    assert verify_v4_selector_response(dataset, payload["payload_signature"], response)["ok"] is True

    evidence_id = dataset.selector.options[target_id]["evidence_refs"][0]
    original_page = dataset.selector.evidence_catalog[evidence_id]["physical_page"]
    try:
        dataset.selector.evidence_catalog[evidence_id]["physical_page"] = 999
        verified = verify_v4_selector_response(dataset, payload["payload_signature"], response)
        task_id = next(task["task_id"] for task in dataset.selector.tasks if task["left_page"] == 20)
        assert "EVIDENCE_PAGE_MISMATCH" in verified["task_results"][task_id]["errors"]
    finally:
        dataset.selector.evidence_catalog[evidence_id]["physical_page"] = original_page


def test_vision_subset_does_not_reopen_non_fallback_text_tasks(datasets) -> None:
    dataset = datasets["p19cd7f695a"]
    subset = subset_selector_dataset(dataset, [27])

    assert [task["left_page"] for task in subset.selector.tasks] == [27]
    task = subset.selector.tasks[0]
    assert all(
        set(subset.selector.options[option_id]["left_pages"]) <= {27}
        for option_id in task["option_ids"]
        if option_id not in {"NO_ANALOG", "NEED_MORE_EVIDENCE"}
    )
