"""Frozen-V3 packaging contract and Human Mapping semantic membership."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

from backend.app.services.project_change_v3 import hm_builder, provider, source_prep

REPO = Path(__file__).resolve().parents[3]
FROZEN = Path("/home/coder/auditmanager/corpus-audits/20260914_project_change_272/ai_first_semantic_mapping_projectchange_v3")


def _png(path: Path, shade: int) -> Path:
    import fitz

    pix = fitz.Pixmap(fitz.csRGB, fitz.IRect(0, 0, 6, 6), 0)
    pix.clear_with(shade)
    pix.save(path)
    return path


def test_payload_identifies_images_by_content(tmp_path):
    a, b, c = _png(tmp_path / "a.png", 10), _png(tmp_path / "b.png", 10), _png(tmp_path / "c.png", 200)
    images = [{"path": str(p), "label": {"block_id": p.stem}} for p in (a, b, c, a)]
    payload, paths, labels = provider.build_codex_payload("PROMPT", {"x": 1}, images)
    assert paths == [str(a), str(c)]
    assert labels == [{"block_id": "a", "image": 1}, {"block_id": "c", "image": 2}]
    assert payload == 'PROMPT\nIMAGES:\n' + json.dumps(labels, ensure_ascii=False) + '\nSOURCE DATA:\n{"x": 1}'


def test_payload_rejects_image_without_path():
    with pytest.raises(provider.ProviderError):
        provider.build_codex_payload("P", {}, [{"label": {}}])


@pytest.mark.parametrize("block_type,tables,expected", [
    ("text", [], "TEXT"), ("text", ["|a|"], "TABLE"), ("stamp", [], "TEXT"),
    ("stamp", ["|a|"], "TABLE"), ("image", [], "GRAPHIC"), ("image", ["|a|"], "GRAPHIC"),
])
def test_frozen_block_type_table(block_type, tables, expected):
    assert source_prep._modality(block_type, tables) == expected


@pytest.mark.parametrize("block_type", ["table", "graphic", "chart", "", None, "TEXT"])
def test_unknown_block_type_fails_closed(block_type):
    with pytest.raises(source_prep.SourcePreparationError):
        source_prep._modality(block_type, [])


@pytest.mark.parametrize("block", [
    {"block_id": "b"}, {"block_id": "b", "coords_norm": None}, {"block_id": "b", "coords_norm": [0, 0, 1]},
    {"block_id": "b", "coords_norm": [0, 0, 1, "x"]}, {"block_id": "b", "coords_norm": [True, 0, 1, 1]},
])
def test_missing_or_invalid_bbox_fails_closed(block):
    with pytest.raises(source_prep.SourcePreparationError):
        source_prep._block_bbox(block)


@pytest.mark.parametrize("value", [None, -1, "0", 1.5, True])
def test_missing_or_invalid_page_index_fails_closed(value):
    block = {"block_id": "b"} if value is None else {"block_id": "b", "page_index": value}
    with pytest.raises(source_prep.SourcePreparationError):
        source_prep._block_page(block)


def _write_page(work: Path, side: str, page: int, blocks: list[dict]) -> None:
    d = work / "source" / side.lower() / f"p{page:03d}"
    d.mkdir(parents=True, exist_ok=True)
    (d / "page.json").write_text(json.dumps({"side": side, "physical_page": page, "blocks": blocks}), encoding="utf-8")


def _blk(block_id: str, modality: str = "TEXT", crop: str = "") -> dict:
    return {"block_id": block_id, "modality": modality, "bbox": [0, 0, 1, 1], "structured_md": block_id,
            "tables": [], "graphic_crop_ref": crop}


def _ref(side, page, block_id, block_type="TEXT"):
    return {"side": side, "physical_page": page, "block_id": block_id, "block_type": block_type, "relevance": "r"}


def test_membership_is_mapper_membership_not_page_blocks(tmp_path):
    _write_page(tmp_path, "OLD", 1, [_blk("o1"), _blk("o2"), _blk("o3", "GRAPHIC", "/x/o3.png")])
    _write_page(tmp_path, "OLD", 2, [_blk("o4")])
    _write_page(tmp_path, "NEW", 1, [_blk("n1"), _blk("n2")])
    region = {"region_id": "R1", "old_pages": [1, 2], "new_pages": [1],
              "important_text_blocks": [_ref("OLD", 1, "o2"), _ref("NEW", 1, "n1")],
              "important_table_blocks": [], "important_graphic_blocks": [_ref("OLD", 1, "o3", "GRAPHIC")]}
    out = hm_builder.build_region(region, tmp_path)
    assert [b["id"] for b in out["old_blocks"]] == ["o2", "o3"]
    assert [b["id"] for b in out["new_blocks"]] == ["n1"]
    assert out["old_blocks"][1]["crop"] == "assets/old/p001/o3.png"
    assert {k for k in out["old_blocks"][0]} == {"id", "side", "type", "page", "bbox", "structured_md",
                                                 "tables", "crop", "relevance"}
    # Page context: only pages hosting members, with ALL their blocks.
    assert [p["page"] for p in out["pages"]["OLD"]] == [1]
    assert [b["id"] for b in out["pages"]["OLD"][0]["blocks"]] == ["o1", "o2", "o3"]
    assert out["mapping_state"] == "MAPPED"


def test_empty_side_stays_empty_and_is_review(tmp_path):
    _write_page(tmp_path, "OLD", 1, [_blk("o1")])
    _write_page(tmp_path, "NEW", 3, [_blk("n1"), _blk("n2")])
    region = {"region_id": "R1", "old_pages": [1], "new_pages": [3],
              "important_text_blocks": [_ref("OLD", 1, "o1")],
              "important_table_blocks": [], "important_graphic_blocks": []}
    out = hm_builder.build_region(region, tmp_path)
    assert out["new_blocks"] == []
    assert out["membership_state"] == {"OLD": "MAPPED", "NEW": "EMPTY"}
    assert out["mapping_state"] == "REVIEW_INSUFFICIENT_MAPPING"
    assert [b["id"] for b in out["pages"]["NEW"][0]["blocks"]] == ["n1", "n2"]
    from backend.app.services.human_mapping_production.validation import allowed_block_ids

    assert allowed_block_ids(out, "NEW") == {"n1", "n2"}
    assert allowed_block_ids(out, "OLD") == {"o1"}


def test_invalid_refs_are_explicit_review(tmp_path):
    _write_page(tmp_path, "OLD", 1, [_blk("o1")])
    _write_page(tmp_path, "NEW", 1, [_blk("n1")])
    region = {"region_id": "R1", "old_pages": [1], "new_pages": [1],
              "important_text_blocks": [_ref("OLD", 1, "o1"), _ref("NEW", 1, "ghost"), _ref("NEW", 1, "n1")],
              "important_table_blocks": [], "important_graphic_blocks": []}
    out = hm_builder.build_region(region, tmp_path)
    assert [b["id"] for b in out["new_blocks"]] == ["n1"]
    assert out["mapping_state"] == "REVIEW_INVALID_MEMBERSHIP_REFS"
    assert out["invalid_membership_refs"][0]["block_id"] == "ghost"


def test_legacy_fixture_regions_keep_v124_rule():
    from backend.app.services.human_mapping_production.validation import allowed_block_ids

    fixture = json.loads((REPO / "backend/app/data/human_mapping_fixtures/UI_DATA_PAIR_A.json").read_text(encoding="utf-8"))
    region = fixture["regions"][1]
    assert allowed_block_ids(region, "OLD") == {b["id"] for b in region["old_blocks"]}
    assert allowed_block_ids({"id": "x", "old_blocks": [], "pages": {"OLD": [{"blocks": [{"id": "p"}]}]}}, "OLD") == set()


@pytest.mark.skipif(not FROZEN.is_dir(), reason="frozen V3 experiment not on this host")
def test_hm_membership_parity_with_sealed_ab(tmp_path):
    spec = importlib.util.spec_from_file_location(
        "hm_parity", REPO / "scripts/audit_projectchange_v3_hm_membership_parity.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    for pair, regions in (("A", 25), ("B", 13)):
        result = module.audit_pair(pair, tmp_path)
        assert result["fixture_regions"] == result["production_regions"] == regions
        assert result["membership_match"] == regions
        assert result["full_match"] == regions
