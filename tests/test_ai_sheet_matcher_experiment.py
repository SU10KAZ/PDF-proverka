from __future__ import annotations

import copy
import hashlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.ai_sheet_matcher.core import (
    ProjectDataset,
    aggregate_decisions,
    build_selector_prompt,
    production_sources_unchanged,
    verify_selector_response,
)


def _passport(side: str, page: int) -> dict:
    return {
        "side": side,
        "pdf_page": page,
        "sheet_number": str(page),
        "sheet_title": f"sheet {page}",
        "sheet_types": ["scheme"],
        "large_headings": [],
        "systems": [],
        "served_object_or_zone": [],
        "floor_or_level": [],
        "characteristic_rooms": [],
        "equipment": [],
        "entities": [],
        "incoming_connections": [],
        "outgoing_connections": [],
        "functional_signals": [f"function {page}"],
        "topology": [],
        "stamp_text": f"Sheet: {page}",
        "page_text_excerpt": f"literal page {page}",
        "page_text_source_length": 14,
        "page_text_truncated": False,
        "neighbor_pages": [],
        "evidence_refs": [f"ev_{side}_{page}"],
        "field_evidence_refs": {},
    }


def _option(
    option_id: str,
    decision_type: str,
    left_pages: list[int],
    right_pages: list[int],
) -> dict:
    return {
        "option_id": option_id,
        "pair_id": "pair",
        "decision_type": decision_type,
        "left_pages": left_pages,
        "right_pages": right_pages,
        "evidence_refs": ["ev"],
        "deterministic_evidence": {"fixture": True},
    }


def _dataset(tmp_path: Path, *, two_tasks: bool = True) -> ProjectDataset:
    pair_dir = tmp_path / "pair"
    (pair_dir / "production").mkdir(parents=True)
    source_files = {
        "pair.json": b"{}",
        "sheet_links.json": b"{}",
        "production/state.json": b"{}",
        "production/sheet_relations.json": b"{}",
    }
    for relative, content in source_files.items():
        path = pair_dir / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    source_hashes = {
        relative: hashlib.sha256(content).hexdigest()
        for relative, content in source_files.items()
    }
    options = {
        "cand_match_1": _option("cand_match_1", "MATCH_1_TO_1", [1], [1]),
        "cand_match_2": _option("cand_match_2", "MATCH_1_TO_1", [1], [2]),
        "cand_match_task_2": _option("cand_match_task_2", "MATCH_1_TO_1", [2], [3]),
        "cand_split": _option("cand_split", "SPLIT_1_TO_N", [1], [1, 2]),
        "cand_merge": _option("cand_merge", "MERGED_N_TO_1", [1, 2], [1]),
        "cand_distributed": _option("cand_distributed", "FUNCTION_DISTRIBUTED", [1, 2], [1, 2]),
    }
    tasks = [{
        "task_id": "task_1",
        "left_page": 1,
        "option_ids": [
            "cand_match_1", "cand_match_2", "cand_split", "cand_merge",
            "cand_distributed", "NO_ANALOG", "NEED_MORE_EVIDENCE",
        ],
    }]
    if two_tasks:
        tasks.append({
            "task_id": "task_2",
            "left_page": 2,
            "option_ids": [
                "cand_match_task_2", "cand_merge", "cand_distributed",
                "NO_ANALOG", "NEED_MORE_EVIDENCE",
            ],
        })
    return ProjectDataset(
        pair_id="pair",
        project="fixture",
        run_id="run",
        pair_dir=pair_dir,
        pair={"left": {"pdf_path": "left.pdf"}, "right": {"pdf_path": "right.pdf"}},
        baseline={"HIGH": 0, "POSSIBLE": 0, "UNKNOWN": 2},
        page_counts={"LEFT": 2, "RIGHT": 4},
        passports={
            "LEFT": {1: _passport("LEFT", 1), 2: _passport("LEFT", 2)},
            "RIGHT": {page: _passport("RIGHT", page) for page in range(1, 5)},
        },
        evidence_catalog={"ev": {"evidence_id": "ev"}},
        frozen_top5={},
        top10={},
        deep_top10={},
        human_links=[],
        human_by_left={
            1: {"left_pages": [1], "right_pages": [1], "decision_type": "MATCH_1_TO_1"},
            2: {"left_pages": [2], "right_pages": [3], "decision_type": "MATCH_1_TO_1"},
        },
        reference_cases=[],
        tasks=tasks,
        options=options,
        contents_context={"LEFT": [], "RIGHT": []},
        source_hashes=source_hashes,
        input_signature="dataset-signature",
    )


def _response(*, first: str = "cand_match_1", second: str = "cand_match_task_2") -> dict:
    return {
        "payload_signature": "payload",
        "selections": [
            {"task_id": "task_1", "local_option_id": first, "map_option_id": first},
            {"task_id": "task_2", "local_option_id": second, "map_option_id": second},
        ],
    }


def _records(dataset: ProjectDataset, responses: list[dict | None], mode: str = "TEXT") -> list[dict]:
    rows = []
    for index, response in enumerate(responses):
        cold_run = index // 2 + 1
        pass_name = "A" if index % 2 == 0 else "B"
        rows.append({
            "pair_id": dataset.pair_id,
            "mode": mode,
            "cold_run": cold_run,
            "pass_name": pass_name,
            "verification": verify_selector_response(dataset, "payload", response),
        })
    return rows


def test_bounded_output_accepts_only_task_bound_ids(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    good = verify_selector_response(dataset, "payload", _response())
    invented = _response(first="cand_invented")
    bad = verify_selector_response(dataset, "payload", invented)

    assert good["ok"] is True
    assert bad["ok"] is False
    assert "LOCAL_OPTION_ID_NOT_BOUND_TO_TASK" in bad["task_results"]["task_1"]["errors"]


def test_invalid_page_is_rejected(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    dataset.options["cand_match_1"]["right_pages"] = [99]

    result = verify_selector_response(dataset, "payload", _response())

    assert result["ok"] is False
    assert "INVALID_RIGHT_PAGE" in result["task_results"]["task_1"]["errors"]


def test_model_cannot_add_invented_evidence(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    response = _response()
    response["evidence"] = ["invented"]

    result = verify_selector_response(dataset, "payload", response)

    assert result["ok"] is False
    assert result["global_errors"] == ["UNEXPECTED_RESPONSE_FIELD"]


def test_candidate_evidence_must_belong_to_selected_pages(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    dataset.evidence_catalog["ev"] = {
        "evidence_id": "ev", "side": "RIGHT", "page": 4,
    }

    result = verify_selector_response(dataset, "payload", _response())

    assert result["ok"] is False
    assert "EVIDENCE_PAGE_MISMATCH" in result["task_results"]["task_1"]["errors"]


def test_disagreement_fails_closed(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    responses = [_response(), _response(first="cand_match_2"), *([_response()] * 4)]

    decisions, _stability = aggregate_decisions(
        dataset, mode="TEXT", run_records=_records(dataset, responses),
    )

    first = next(item for item in decisions if item["left_page"] == 1)
    assert first["final_status"] == "UNRESOLVED"
    assert first["materialization_allowed"] is False


def test_model_failure_fails_closed(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)

    decisions, stability = aggregate_decisions(
        dataset, mode="TEXT", run_records=_records(dataset, [None] * 6),
    )

    assert all(item["materialization_allowed"] is False for item in decisions)
    assert stability["stable_task_count"] == 0


def test_split_cardinality_is_validated(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path, two_tasks=False)
    response = {
        "payload_signature": "payload",
        "selections": [{"task_id": "task_1", "local_option_id": "cand_split", "map_option_id": "cand_split"}],
    }

    assert verify_selector_response(dataset, "payload", response)["ok"] is True

    dataset.options["cand_split"]["right_pages"] = [1]
    invalid = verify_selector_response(dataset, "payload", response)
    assert "INVALID_CARDINALITY" in invalid["task_results"]["task_1"]["errors"]


def test_merge_requires_all_constituent_tasks(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    complete = _response(first="cand_merge", second="cand_merge")
    incomplete = _response(first="cand_merge", second="NEED_MORE_EVIDENCE")

    assert verify_selector_response(dataset, "payload", complete)["ok"] is True
    rejected = verify_selector_response(dataset, "payload", incomplete)
    assert rejected["ok"] is False
    assert "INCOMPLETE_GROUP_SELECTION" in rejected["task_results"]["task_1"]["errors"]


def test_deterministic_verifier_repeats_identically(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)

    first = verify_selector_response(dataset, "payload", _response())
    second = verify_selector_response(dataset, "payload", copy.deepcopy(_response()))

    assert first == second


def test_human_decision_overrides_stable_model_choice(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    conflict = _response(first="cand_match_2")

    decisions, _stability = aggregate_decisions(
        dataset, mode="TEXT", run_records=_records(dataset, [conflict] * 6),
    )

    first = next(item for item in decisions if item["left_page"] == 1)
    assert first["final_status"] == "HUMAN_REVIEW"
    assert first["verifier_status"] == "BLOCKED_HUMAN_DECISION_CONFLICT"
    assert first["materialization_allowed"] is False


def test_repeated_run_unanimity_can_pass_human_priority(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)

    decisions, stability = aggregate_decisions(
        dataset, mode="TEXT", run_records=_records(dataset, [_response()] * 6),
    )

    assert stability["stable_task_count"] == 2
    assert all(item["final_status"] == "STABLE_AUTO" for item in decisions)
    assert all(item["materialization_allowed"] is True for item in decisions)


def test_experiment_does_not_mutate_production_sources(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)
    before = copy.deepcopy(dataset.source_hashes)

    verify_selector_response(dataset, "payload", _response())

    assert production_sources_unchanged(dataset) is True
    assert dataset.source_hashes == before


def test_selector_prompt_excludes_human_ground_truth(tmp_path: Path) -> None:
    dataset = _dataset(tmp_path)

    prompt, _payload = build_selector_prompt(dataset, mode="TEXT")

    assert "human_by_left" not in prompt
    assert "saved engineer" not in prompt
