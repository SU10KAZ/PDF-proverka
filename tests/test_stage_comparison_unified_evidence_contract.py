"""G2.4.1 contract-only checks for future TEXT/GRAPHIC unification."""
from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import high_level_project_changes as high
from backend.app.services.stage_comparison.graphic_comparison import (
    adapt_system_graph_comparison_to_ledger,
)
from backend.app.services.stage_comparison.graphic_comparison.contract import (
    validate_ledger,
)
from backend.app.services.stage_comparison.unified_evidence_contract import (
    KIND,
    SCHEMA_VERSION,
    UnifiedEvidenceValidationError,
    schema_path,
    validate_unified_evidence_bundle,
)


ROOT = Path(__file__).resolve().parents[1]


def _text_evidence(
    *,
    evidence_id: str = "ue_text_reservation",
    source_change_id: str = "hlc_reservation",
    source_type: str = "SYSTEM_OPERATION_CHANGED",
) -> dict:
    return {
        "evidence_id": evidence_id,
        "evidence_source": "TEXT",
        "source_artifact": {
            "kind": "stage_comparison_high_level_project_changes",
            "schema_version": "1.0",
        },
        "source_change_id": source_change_id,
        "provenance": {
            "source_type": source_type,
            "source_signature": "stage5.3-signature",
            "sheet_groups": ["sheet-eom-1"],
        },
        "locations": [
            {
                "kind": "TEXT_FRAGMENT",
                "side": "LEFT",
                "sheet": "ЭОМ-1",
                "page": 1,
                "fragment_ids": ["fragment-left"],
            },
            {
                "kind": "TEXT_FRAGMENT",
                "side": "RIGHT",
                "sheet": "ЭОМ-1",
                "page": 2,
                "fragment_ids": ["fragment-right"],
            },
        ],
        "source_ids": ["text-evidence-1", "fragment-left", "fragment-right"],
        "confidence": {
            "level": "HIGH",
            "raw": None,
            "source_scale": "stage5.3-high-level-v1",
        },
    }


def _graphic_evidence(
    *,
    evidence_id: str = "ue_graphic_sectioning",
    source_change_id: str = "chg_sectioning",
    source_type: str = "SYSTEM_BACKBONE_CHANGED",
) -> dict:
    return {
        "evidence_id": evidence_id,
        "evidence_source": "GRAPHIC",
        "source_artifact": {
            "kind": "graphic_change_ledger",
            "schema_version": "graphic-change-ledger.v2",
        },
        "source_change_id": source_change_id,
        "provenance": {
            "source_type": source_type,
            "adapter_version": "system-graph-ledger-adapter-v1",
            "profile_version": "dense-sectioned-board-v1",
        },
        "locations": [
            {
                "kind": "SYSTEM_GRAPH_REGION",
                "side": "LEFT",
                "block_id": "left-block",
                "page": 0,
                "node_ids": ["BUS1", "BUS2"],
                "edge_ids": ["TIES:BUS1->BUS2"],
                "bbox_visual_pt": [10, 20, 100, 120],
            },
            {
                "kind": "SYSTEM_GRAPH_REGION",
                "side": "RIGHT",
                "block_id": "right-block",
                "page": 0,
                "node_ids": ["BUS1", "BUS2"],
                "edge_ids": ["TIES:BUS1->BUS2"],
                "bbox_visual_pt": [15, 25, 105, 125],
            },
        ],
        "source_ids": ["BUS1", "BUS2", "TIES:BUS1->BUS2"],
        "confidence": {
            "level": "HIGH",
            "raw": 0.92,
            "source_scale": "system-graph-ledger-confidence-v1",
        },
    }


def _bundle(source: str, evidence: list[dict], change_id: str = "upc_shared") -> dict:
    actual = {item["evidence_source"] for item in evidence}
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": KIND,
        "change_id": change_id,
        "source": source,
        "evidence_sources": [
            candidate for candidate in ("TEXT", "GRAPHIC") if candidate in actual
        ],
        "evidence": evidence,
    }


def test_unified_evidence_schema_is_versioned_and_parseable():
    schema = json.loads(schema_path().read_text(encoding="utf-8"))

    assert schema["title"] == "Unified TEXT/GRAPHIC Change Evidence"
    assert schema["properties"]["schema_version"]["const"] == SCHEMA_VERSION


def test_old_stage53_artifact_still_opens_without_rewrite():
    old_artifact = {
        "version": 1,
        "schema_version": "1.0",
        "kind": high.KIND,
        "source_signature": "old-signature",
        "evidence_sources": ["TEXT"],
        "high_level_changes": [],
    }
    original = copy.deepcopy(old_artifact)

    public = high.public_view(old_artifact)

    assert public is not None
    assert public["schema_version"] == "1.0"
    assert old_artifact == original


def test_old_ledger_v2_still_opens(grsh_mode2_ledger: dict):
    stored = json.loads(json.dumps(grsh_mode2_ledger, ensure_ascii=False))

    assert validate_ledger(stored) is stored


@pytest.fixture(scope="module")
def grsh_mode2_ledger() -> dict:
    left = json.loads(
        (ROOT / "experiments/g2_dense_sectioned_board/left_system_graph.json").read_text(
            encoding="utf-8"
        )
    )
    right = json.loads(
        (ROOT / "experiments/g2_dense_sectioned_board/right_system_graph.json").read_text(
            encoding="utf-8"
        )
    )
    comparison = json.loads(
        (ROOT / "experiments/g2_system_graph_comparator/comparison_result.json").read_text(
            encoding="utf-8"
        )
    )
    return adapt_system_graph_comparison_to_ledger(comparison, left, right)


def test_text_source_bundle_validates():
    bundle = _bundle("TEXT", [_text_evidence()], "upc_text_only")

    assert validate_unified_evidence_bundle(bundle) is bundle


def test_graphic_source_bundle_validates():
    bundle = _bundle("GRAPHIC", [_graphic_evidence()], "upc_graphic_only")

    assert validate_unified_evidence_bundle(bundle) is bundle


@pytest.mark.parametrize(
    ("change_id", "text_type", "graphic_type"),
    [
        (
            "upc_reservation",
            "SYSTEM_OPERATION_CHANGED",
            "SYSTEM_BACKBONE_CHANGED",
        ),
        (
            "upc_added_object",
            "SYSTEM_STRUCTURE_CHANGED",
            "NODE_ADDED",
        ),
        (
            "upc_source_detail",
            "DETAIL_LEVEL_INCREASED",
            "DETAIL_LEVEL_INCREASED",
        ),
    ],
)
def test_both_sources_share_one_unified_change_id(
    change_id: str,
    text_type: str,
    graphic_type: str,
):
    bundle = _bundle(
        "BOTH",
        [
            _text_evidence(source_type=text_type),
            _graphic_evidence(source_type=graphic_type),
        ],
        change_id,
    )

    assert validate_unified_evidence_bundle(bundle) is bundle
    assert bundle["change_id"] == change_id
    assert bundle["evidence_sources"] == ["TEXT", "GRAPHIC"]
    assert {item["source_change_id"] for item in bundle["evidence"]} == {
        "hlc_reservation",
        "chg_sectioning",
    }


def test_source_label_must_match_actual_evidence():
    bundle = _bundle("BOTH", [_text_evidence()], "upc_bad_source")

    with pytest.raises(UnifiedEvidenceValidationError, match="bundle.source"):
        validate_unified_evidence_bundle(bundle)


def test_source_artifact_cannot_cross_modalities():
    item = _graphic_evidence()
    item["source_artifact"] = {
        "kind": "stage_comparison_high_level_project_changes",
        "schema_version": "1.0",
    }
    bundle = _bundle("GRAPHIC", [item], "upc_bad_artifact")

    with pytest.raises(UnifiedEvidenceValidationError, match="source mismatch"):
        validate_unified_evidence_bundle(bundle)


def test_source_change_cannot_be_counted_twice():
    first = _text_evidence()
    duplicate = _text_evidence(evidence_id="ue_text_duplicate")
    bundle = _bundle("TEXT", [first, duplicate], "upc_duplicate_source_link")

    with pytest.raises(UnifiedEvidenceValidationError, match="duplicate source link"):
        validate_unified_evidence_bundle(bundle)
