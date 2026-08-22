from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

import fitz
import pytest

from .benchmark_data import benchmark_manifest
from .comparator import compare_graphic_scopes
from .controlled_falsifiers import run_controlled_falsifiers
from .input_contract import PreparedBlockError, resolve_prepared_block
from .objects import _clip_rect, _invisible_fill_only, _spatial_cap, build_graphic_block_description
from .page_cache import PageDrawingCache


HERE = Path(__file__).resolve().parent


def _pair(pair_id: str) -> dict:
    return next(row for row in benchmark_manifest()["pairs"] if row["pair_id"] == pair_id)


def test_benchmark_uses_only_real_prepared_references() -> None:
    manifest = benchmark_manifest()
    assert len(manifest["pairs"]) == 38
    for pair in manifest["pairs"]:
        for key in ("left_blocks", "right_blocks"):
            reference = pair["scope"][key][0]
            assert set(reference) == {"blocks_json", "block_id", "block_group_id"}
            assert resolve_prepared_block(reference)["block_id"] == reference["block_id"]


def test_resolver_rejects_benchmark_bbox_duplication() -> None:
    reference = dict(_pair("ss_simple_node")["scope"]["left_blocks"][0]); reference["coords_norm"] = [0, 0, 1, 1]
    with pytest.raises(PreparedBlockError, match="must not duplicate"):
        resolve_prepared_block(reference)


def test_rotation_coordinate_path_retains_visible_geometry(tmp_path: Path) -> None:
    pdf = tmp_path / "rotation.pdf"; document = fitz.open()
    for rotation in (0, 90, 180, 270):
        page = document.new_page(width=200, height=100); page.draw_rect(fitz.Rect(20, 20, 80, 60), color=(0, 0, 0), width=1); page.set_rotation(rotation)
    document.save(pdf); document.close(); cache = PageDrawingCache(tmp_path / "cache")
    counts = []
    for page_index, rotation in enumerate((0, 90, 180, 270)):
        block = {"comparison_unit": "ALREADY_PREPARED_GRAPHIC_BLOCK", "block_id": str(rotation), "blocks_json": "synthetic/blocks.json", "source_pdf": "synthetic/document.pdf", "source_pdf_path": str(pdf), "page_index": page_index, "coords_norm": [0, 0, 1, 1], "polygon_points_norm": None, "block_type": "image", "graphic_applicability": "GRAPHIC_APPLICABLE", "prepared_text_metadata": []}
        description = build_graphic_block_description(block, cache)
        counts.append((len(description["objects"]), description["visible_geometry_summary"]["segments"]))
        assert description["visible_geometry_summary"]["extraction"]["coordinate_method"].startswith("upstream visual coords")
    assert counts == [(1, 4)] * 4


def test_strict_clip_and_invisible_white_fill_only() -> None:
    clipped = _clip_rect((-1, .5), (2, .5), (0, 0, 1, 1))
    assert clipped == ([0.0, .5], [1.0, .5])
    assert _invisible_fill_only({"stroke": None, "fill": [1, 1, 1], "fill_opacity": 1})
    assert not _invisible_fill_only({"stroke": [1, 1, 1], "fill": [1, 1, 1], "fill_opacity": 1})


def test_spatial_cap_preserves_occupied_cells() -> None:
    atoms = []
    for index, (x, y) in enumerate(((4.1, 4.1), (20.1, 4.1), (4.1, 20.1), (20.1, 20.1), (4.4, 4.4), (20.4, 4.4), (4.4, 20.4), (20.4, 20.4))):
        atoms.append({"atom_id": str(index), "segments": [[[x, y], [x + .1, y + .1]]], "bbox": [x, y, x + .1, y + .1]})
    retained, info = _spatial_cap(atoms, 4, 32, 32)
    assert sum(len(row["segments"]) for row in retained) == 4
    assert info["occupied_cells_before"] == info["occupied_cells_after"] == 4
    assert info["policy"].endswith("never longest-lines-only")


def test_controlled_mechanisms_and_known_identity_failure() -> None:
    result = run_controlled_falsifiers(); rows = {row["case_id"]: row for row in result["cases"]}
    assert result["passed"] == 8 and result["total"] == 9
    assert rows["local_removal_among_50000_lines"]["passed"]
    assert rows["same_rectangle_repacked"]["passed"]
    assert rows["text_changed_graphics_same"]["passed"]
    assert rows["table_content_changed"]["passed"]
    assert not rows["similar_objects_swapped"]["passed"]


def test_real_table_is_not_graphically_compared() -> None:
    pair = _pair("ss_table_page19")
    descriptions = []
    cache = PageDrawingCache(HERE / ".page_cache")
    for key in ("left_blocks", "right_blocks"):
        descriptions.append([build_graphic_block_description(resolve_prepared_block(pair["scope"][key][0]), cache)])
    ledger = compare_graphic_scopes(pair["pair_id"], *descriptions)
    assert ledger["applicability"] == "GRAPHIC_NOT_APPLICABLE"
    assert ledger["changes"] == []


def test_lossless_artifact_hash_and_schemas() -> None:
    cases = [
        (HERE / "artifacts/object_descriptions/ss_simple_node/left-1.json", HERE / "graphic_block_description.schema.json"),
        (HERE / "artifacts/object_comparisons/ss_simple_node/graphic_change_ledger.json", HERE / "graphic_change_ledger.schema.json"),
    ]
    for index_path, schema_path in cases:
        index = json.loads(index_path.read_text(encoding="utf-8")); retention = index["artifact_retention"]
        with gzip.open(index_path.with_name(retention["full_lossless_artifact"]), "rt", encoding="utf-8") as stream:
            full = json.load(stream)
        canonical = json.dumps(full, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        assert hashlib.sha256(canonical).hexdigest() == retention["full_uncompressed_sha256"]
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        assert set(schema["required"]) <= set(full)
        assert full["schema_version"] == schema["properties"]["schema_version"]["const"]
