"""G2.4 Mode 2 adapter, validation, and real GRSh acceptance tests."""
from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison.graphic_comparison.confidence_policy import (
    MODE2_CONFIDENCE_POLICY_V1,
)
from backend.app.services.stage_comparison.graphic_comparison.contract import (
    MODE1_SCHEMA_VERSION,
    MODE2_SCHEMA_VERSION,
    LedgerValidationError,
    schema_path,
    validate_ledger,
)
from backend.app.services.stage_comparison.graphic_comparison import (
    graphic_change_ledger_adapter as ledger_adapter,
)


LedgerAdapterError = ledger_adapter.LedgerAdapterError
adapt_system_graph_comparison_to_ledger = (
    ledger_adapter.adapt_system_graph_comparison_to_ledger
)


ROOT = Path(__file__).resolve().parents[1]
GRAPH_DIR = ROOT / "experiments/g2_dense_sectioned_board"
COMPARISON_PATH = ROOT / "experiments/g2_system_graph_comparator/comparison_result.json"


@pytest.fixture(scope="module")
def grsh_sources() -> tuple[dict, dict, dict]:
    left = json.loads((GRAPH_DIR / "left_system_graph.json").read_text(encoding="utf-8"))
    right = json.loads((GRAPH_DIR / "right_system_graph.json").read_text(encoding="utf-8"))
    comparison = json.loads(COMPARISON_PATH.read_text(encoding="utf-8"))
    return left, right, comparison


@pytest.fixture(scope="module")
def compact_mode2_ledger(grsh_sources: tuple[dict, dict, dict]) -> dict:
    left, right, comparison = grsh_sources
    comparison = copy.deepcopy(comparison)
    comparison["changes"] = [
        change
        for change in comparison["changes"]
        if change["type"] == "NODE_TYPE_CHANGED"
    ]
    return adapt_system_graph_comparison_to_ledger(comparison, left, right)


def _old_mode1_ledger() -> dict:
    return {
        "schema_version": MODE1_SCHEMA_VERSION,
        "comparison_scope": {
            "left_blocks": [
                {
                    "block_id": "left",
                    "page_index": 0,
                    "block_type": "image",
                    "bbox_visual_pt": [0, 0, 100, 100],
                }
            ],
            "right_blocks": [
                {
                    "block_id": "right",
                    "page_index": 0,
                    "block_type": "image",
                    "bbox_visual_pt": [0, 0, 100, 100],
                }
            ],
        },
        "route": "MODE_1_APPLICABLE",
        "mode": "MODE_1",
        "policy": {"version": "legacy-g1"},
        "quality": {},
        "changes": [
            {
                "change_id": "legacy-change",
                "type": "GEOMETRY_CHANGED",
                "left_region": {
                    "block_id": "left",
                    "page_index": 0,
                    "bbox_visual_pt": [10, 10, 20, 20],
                },
                "right_region": {
                    "block_id": "right",
                    "page_index": 0,
                    "bbox_visual_pt": [11, 11, 21, 21],
                },
                "evidence": [{"kind": "VECTOR_LOCAL_DIFF"}],
                "address_hints": [],
                "confidence": "HIGH",
                "provenance": ["VECTOR"],
            }
        ],
        "diagnostics": {},
    }


def test_old_mode1_ledger_remains_valid_without_migration():
    ledger = _old_mode1_ledger()

    assert validate_ledger(ledger) is ledger
    assert ledger["schema_version"] == "graphic-change-ledger.v1"


def test_mode2_schema_and_structural_result_validate(compact_mode2_ledger: dict):
    schema = json.loads(schema_path(MODE2_SCHEMA_VERSION).read_text(encoding="utf-8"))

    assert schema["title"] == "GraphicChangeLedger Mode 2"
    assert validate_ledger(compact_mode2_ledger) is compact_mode2_ledger
    change = compact_mode2_ledger["changes"][0]
    assert change["mode"] == "MODE_2"
    assert change["type"] == "NODE_TYPE_CHANGED"
    assert change["raw_confidence"] == 0.92
    assert change["mapped_confidence"] == change["confidence"] == "HIGH"
    assert change["structural"]["level"] == "NODE"


def test_mode2_missing_evidence_is_rejected(compact_mode2_ledger: dict):
    ledger = copy.deepcopy(compact_mode2_ledger)
    ledger["changes"][0]["evidence"] = []

    with pytest.raises(LedgerValidationError, match="evidence"):
        validate_ledger(ledger)


def test_mode2_unknown_change_type_is_rejected(compact_mode2_ledger: dict):
    ledger = copy.deepcopy(compact_mode2_ledger)
    ledger["changes"][0]["type"] = "UNKNOWN_CHANGE"

    with pytest.raises(LedgerValidationError, match="type"):
        validate_ledger(ledger)


def test_mode2_bad_confidence_is_rejected(compact_mode2_ledger: dict):
    ledger = copy.deepcopy(compact_mode2_ledger)
    ledger["changes"][0]["confidence"] = "CERTAIN"

    with pytest.raises(LedgerValidationError, match="confidence"):
        validate_ledger(ledger)


def test_mode2_duplicate_change_id_is_rejected(compact_mode2_ledger: dict):
    ledger = copy.deepcopy(compact_mode2_ledger)
    duplicate = copy.deepcopy(ledger["changes"][0])
    ledger["changes"].append(duplicate)

    with pytest.raises(LedgerValidationError, match="duplicate"):
        validate_ledger(ledger)


def test_mode2_invalid_structural_payload_is_rejected(compact_mode2_ledger: dict):
    ledger = copy.deepcopy(compact_mode2_ledger)
    ledger["changes"][0]["structural"]["level"] = "GROUP"

    with pytest.raises(LedgerValidationError, match="structural.level"):
        validate_ledger(ledger)


def test_mode2_conflicting_node_claims_are_rejected(compact_mode2_ledger: dict):
    ledger = copy.deepcopy(compact_mode2_ledger)
    type_change = ledger["changes"][0]
    added = copy.deepcopy(type_change)
    added["change_id"] = "conflicting-added-node"
    added["type"] = "NODE_ADDED"
    added["summary"] = "Conflicting added-node claim"
    added["left_region"] = None
    added["evidence"] = [
        item for item in added["evidence"] if item["source_graph"]["side"] == "RIGHT"
    ]
    added["address_hints"] = [
        item for item in added["address_hints"] if item["side"] == "RIGHT"
    ]
    added["structural"]["left_nodes"] = []
    added["structural"]["left_edges"] = []
    added["structural"]["relation"] = {}
    ledger["changes"].append(added)

    with pytest.raises(LedgerValidationError, match="conflicting"):
        validate_ledger(ledger)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(0.85, "HIGH"), (0.849, "MEDIUM"), (0.60, "MEDIUM"), (0.599, "LOW")],
)
def test_confidence_mapping_boundaries_are_versioned(raw: float, expected: str):
    assert MODE2_CONFIDENCE_POLICY_V1.map(raw) == expected
    assert MODE2_CONFIDENCE_POLICY_V1.public_dict()["policy_id"].endswith("-v1")


def test_adapter_uses_graph_evidence_for_addresses_not_comparator_grounding(
    grsh_sources: tuple[dict, dict, dict],
):
    left, right, comparison = grsh_sources
    comparison = copy.deepcopy(comparison)
    comparison["changes"] = [
        change
        for change in comparison["changes"]
        if change["type"] == "NODE_TYPE_CHANGED"
    ]
    for side in ("left", "right"):
        comparison["changes"][0]["evidence"][side]["block_id"] = "not-a-real-block"
        comparison["changes"][0]["evidence"][side]["source_tokens"] = ["not-real"]
        comparison["changes"][0]["evidence"][side]["confidence"] = 0.0

    ledger = adapt_system_graph_comparison_to_ledger(comparison, left, right)
    change = ledger["changes"][0]
    by_side = {item["source_graph"]["side"]: item for item in change["evidence"]}

    assert by_side["LEFT"]["source_graph"]["block_id"] == left["block"]["block_id"]
    assert by_side["RIGHT"]["source_graph"]["block_id"] == right["block"]["block_id"]
    assert "QF3" in by_side["LEFT"]["source_tokens"]
    assert "QS1" in by_side["RIGHT"]["source_tokens"]
    assert "not-real" not in by_side["LEFT"]["source_tokens"]


def test_adapter_fails_closed_for_wrong_source_graph(
    grsh_sources: tuple[dict, dict, dict],
):
    left, right, comparison = grsh_sources
    wrong_left = copy.deepcopy(left)
    wrong_left["block"]["block_id"] = "wrong-block"

    with pytest.raises(LedgerAdapterError, match="source mismatch"):
        adapt_system_graph_comparison_to_ledger(comparison, wrong_left, right)


def test_real_grsh_result_is_preserved_in_mode2_ledger(
    grsh_sources: tuple[dict, dict, dict],
):
    left, right, comparison = grsh_sources
    ledger = adapt_system_graph_comparison_to_ledger(comparison, left, right)
    types = [change["type"] for change in ledger["changes"]]

    assert validate_ledger(ledger) is ledger
    assert ledger["mode"] == "MODE_2"
    assert ledger["diagnostics"]["structural_status"]["backbone"] == "BACKBONE_PRESERVED"
    assert types.count("DETAIL_LEVEL_INCREASED") == 2
    assert types.count("NODE_TYPE_CHANGED") == 1
    assert types.count("GROUP_COUNT_CHANGED") == 1
    assert types.count("UNCERTAIN_STRUCTURAL_CHANGE") >= 1
    assert "NODE_ADDED" not in types
    assert "NODE_REMOVED" not in types

    details = [
        change for change in ledger["changes"] if change["type"] == "DETAIL_LEVEL_INCREASED"
    ]
    assert all(
        change["structural"]["equivalence"] == "representation_expansion"
        for change in details
    )
    type_change = next(
        change for change in ledger["changes"] if change["type"] == "NODE_TYPE_CHANGED"
    )
    tokens_by_side = {
        item["source_graph"]["side"]: item["source_tokens"]
        for item in type_change["evidence"]
    }
    assert "QF3" in tokens_by_side["LEFT"]
    assert "QS1" in tokens_by_side["RIGHT"]
    count_change = next(
        change for change in ledger["changes"] if change["type"] == "GROUP_COUNT_CHANGED"
    )
    assert count_change["structural"]["relation"]["left_count"] == 30
    assert count_change["structural"]["relation"]["right_count"] == 27
