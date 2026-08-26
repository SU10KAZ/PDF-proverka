"""Production direct PAGE ↔ PAGE MODE 2 orchestration."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison.graphic_comparison import (
    DirectPageComparisonError,
    compare_selected_pages,
    resolve_selected_page_source,
    validate_direct_page_comparison_result,
)
from backend.app.services.stage_comparison.graphic_comparison.mode2 import schema_path
from backend.app.services.stage_comparison.unified_entity_bridge.document_binding import (
    PROVENANCE_ARTIFACT,
)


ROOT = Path(__file__).resolve().parents[1]
STORE = ROOT / "projects_v2/objects/272_Sadovnicheskaya_76_Balchug_Esteyt/comparison"
LEFT_DOCUMENT = STORE / "stage_1/documents/Страница_52_из_АА_БЭ-03-ДС3-ИОС1.1"
RIGHT_DOCUMENT = (
    STORE / "stage_2/documents/Страница_21_из_АА-БЭ-03-ДС3-ИОС1.1_—_копия"
)
LEFT_BLOCK = "blk_2d72a6705eaf4d8c9ee1d6ff459b15a6"
RIGHT_BLOCK = "blk_039909ec039649a1b8209f059c95167b"


def _document(code: str, pdf_path: Path) -> dict:
    return {
        "document_code": code,
        "version_id": "v001",
        "storage_identity": None,
        "source_path": str(pdf_path),
        "provenance": PROVENANCE_ARTIFACT,
    }


def _source(document_dir: Path, block_id: str) -> dict:
    work = document_dir / "versions/v001/02_work"
    pdf_path = work / "document.pdf"
    return {
        "document": _document(document_dir.name, pdf_path),
        "pdf_path": pdf_path,
        "blocks_path": work / "blocks.json",
        "page_index": 0,
        "block_id": block_id,
    }


def _write_blocks(path: Path, blocks: list[dict]) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "coordinate_space": "normalized_page_top_left",
                "pages": [{"page_index": 0, "rotation": 0}],
                "blocks": blocks,
            }
        ),
        encoding="utf-8",
    )


def test_page_source_auto_selection_fails_closed_when_graphic_is_ambiguous(tmp_path):
    pdf_path = tmp_path / "document.pdf"
    blocks_path = tmp_path / "blocks.json"
    pdf_path.write_bytes(b"not opened by source resolution")
    records = [
        {
            "block_id": block_id,
            "page_index": 0,
            "block_type": "image",
            "coords_norm": [0.1, 0.1, 0.4, 0.4],
        }
        for block_id in ("graphic-a", "graphic-b")
    ]
    _write_blocks(blocks_path, records)
    source = {
        "document": _document("DOC", pdf_path),
        "pdf_path": pdf_path,
        "blocks_path": blocks_path,
        "page_index": 0,
    }

    with pytest.raises(DirectPageComparisonError, match="block_id required"):
        resolve_selected_page_source(source, "LEFT")

    resolved = resolve_selected_page_source(
        {**source, "block_id": "graphic-b"}, "LEFT"
    )
    assert resolved["reference"]["block_id"] == "graphic-b"
    assert resolved["reference"]["selection_kind"] == "PAGE"


def test_direct_page_contract_has_explicit_schema_version():
    schema = json.loads(schema_path().read_text(encoding="utf-8"))

    assert schema["properties"]["schema_version"]["const"] == (
        "direct-page-system-graph-comparison.v1"
    )


def test_page_source_rejects_block_from_another_local_page(tmp_path):
    pdf_path = tmp_path / "document.pdf"
    blocks_path = tmp_path / "blocks.json"
    pdf_path.write_bytes(b"not opened by source resolution")
    _write_blocks(
        blocks_path,
        [
            {
                "block_id": "other-page",
                "page_index": 1,
                "block_type": "image",
                "coords_norm": [0.1, 0.1, 0.4, 0.4],
            }
        ],
    )
    source = {
        "document": _document("DOC", pdf_path),
        "pdf_path": pdf_path,
        "blocks_path": blocks_path,
        "page_index": 0,
        "block_id": "other-page",
    }

    with pytest.raises(DirectPageComparisonError, match="not on selected page"):
        resolve_selected_page_source(source, "LEFT")


@pytest.fixture(scope="module")
def pilot_result():
    left = _source(LEFT_DOCUMENT, LEFT_BLOCK)
    right = _source(RIGHT_DOCUMENT, RIGHT_BLOCK)
    if not Path(left["pdf_path"]).is_file() or not Path(right["pdf_path"]).is_file():
        pytest.skip("G2.4.4.3 pilot corpus is not installed")
    return compare_selected_pages(left, right)


def test_production_mode2_reproduces_g2443_pilot(pilot_result):
    result = validate_direct_page_comparison_result(pilot_result)
    left_graph = result["left_graph"]
    right_graph = result["right_graph"]
    comparison = result["comparison_result"]
    ledger = result["graphic_change_ledger"]

    assert (len(left_graph["nodes"]), len(left_graph["edges"])) == (73, 99)
    assert (len(right_graph["nodes"]), len(right_graph["edges"])) == (82, 111)
    assert comparison["summary"]["by_type"] == {
        "UNCERTAIN_STRUCTURAL_CHANGE": 2,
        "GROUP_COUNT_CHANGED": 1,
        "NODE_TYPE_CHANGED": 1,
    }
    group = next(item for item in ledger["changes"] if item["type"] == "GROUP_COUNT_CHANGED")
    typed = next(item for item in ledger["changes"] if item["type"] == "NODE_TYPE_CHANGED")
    assert group["structural"]["relation"]["left_count"] == 27
    assert group["structural"]["relation"]["right_count"] == 30
    assert "QS1" in typed["summary"] and "QF3" in typed["summary"]
    assert not {"NODE_ADDED", "NODE_REMOVED"} & set(comparison["summary"]["by_type"])


def test_direct_page_result_carries_full_left_right_provenance(pilot_result):
    result = pilot_result
    assert result["direction"] == "LEFT_TO_RIGHT"
    assert result["sources"]["LEFT"]["block_id"] == LEFT_BLOCK
    assert result["sources"]["RIGHT"]["block_id"] == RIGHT_BLOCK
    assert result["sources"]["LEFT"]["document"]["version_id"] == "v001"
    assert (
        result["comparison_result"]["left_graph"]["source_reference"]
        == result["sources"]["LEFT"]
    )
    assert (
        result["graphic_change_ledger"]["comparison_scope"]["right_blocks"][0]
        ["source"]["selected_source"]
        == result["sources"]["RIGHT"]
    )
    assert result["graphic_change_ledger"]["diagnostics"]["direct_page_comparison"] == {
        "direction": "LEFT_TO_RIGHT",
        "parent_relation_required": False,
    }


def test_rotate_270_uses_production_vector_extractor(pilot_result):
    provenance = pilot_result["diagnostics"]["LEFT"]["vector_evidence"]["provenance"]
    assert provenance["rotation_degrees"] == 270
    assert provenance["rotation_applied"] is True


def test_direct_page_mode2_is_deterministic(pilot_result):
    second = compare_selected_pages(
        _source(LEFT_DOCUMENT, LEFT_BLOCK),
        _source(RIGHT_DOCUMENT, RIGHT_BLOCK),
    )
    assert second == pilot_result
