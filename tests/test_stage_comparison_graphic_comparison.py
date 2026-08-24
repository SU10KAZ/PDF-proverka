from __future__ import annotations

import json
from pathlib import Path

import cv2
import fitz
import numpy as np
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routers import stage_comparison as stage_comparison_router
from backend.app.services.stage_comparison import store
from backend.app.services.stage_comparison.graphic_comparison.contract import (
    LedgerValidationError,
    schema_path,
    validate_ledger,
)
from backend.app.services.stage_comparison.graphic_comparison.extraction import (
    PAGE_CACHE,
    _clip_segment_to_polygon_group,
    block_from_record,
    extract_ink,
    rasterize,
)
from backend.app.services.stage_comparison.graphic_comparison.policy import (
    EXPERIMENTALLY_CALIBRATED_V1,
)
from backend.app.services.stage_comparison.graphic_comparison import router as graphic_router
from backend.app.services.stage_comparison.graphic_comparison.router import (
    compare_prepared_blocks,
)


BASE_RECORD = {
    "block_id": "graphic-block",
    "page_index": 0,
    "page_label": 1,
    "block_type": "image",
    "coords_norm": [0.05, 0.05, 0.95, 0.95],
    "source": "upstream-test-fixture",
}


def _png_bytes(size: int = 160) -> bytes:
    image = np.full((size, size, 3), 255, np.uint8)
    cv2.rectangle(image, (5, 5), (size - 6, size - 6), (0, 0, 0), 4)
    cv2.line(image, (10, 20), (size - 10, size - 20), (0, 0, 0), 3)
    ok, encoded = cv2.imencode(".png", image)
    assert ok
    return encoded.tobytes()


def _draw_grid(page: fitz.Page, *, packaging: str = "separate") -> None:
    horizontal = [((30, y), (370, y)) for y in range(40, 341, 30)]
    vertical = [((x, 40), (x, 340)) for x in range(30, 371, 40)]
    if packaging == "single_path":
        shape = page.new_shape()
        for start, end in [*horizontal, *vertical]:
            shape.draw_line(start, end)
        shape.finish(color=(0, 0, 0), width=1)
        shape.commit()
    elif packaging == "segmented":
        for start, end in horizontal:
            middle = ((start[0] + end[0]) / 2, start[1])
            page.draw_line(start, middle, color=(0, 0, 0), width=1)
            page.draw_line(middle, end, color=(0, 0, 0), width=1)
        for start, end in vertical:
            middle = (start[0], (start[1] + end[1]) / 2)
            page.draw_line(start, middle, color=(0, 0, 0), width=1)
            page.draw_line(middle, end, color=(0, 0, 0), width=1)
    else:
        for start, end in [*horizontal, *vertical]:
            page.draw_line(start, end, color=(0, 0, 0), width=1)


def _make_pdf(
    path: Path,
    *,
    packaging: str = "separate",
    extra_lines: tuple[tuple[tuple[float, float], tuple[float, float]], ...] = (),
    text: str = "",
    rotation: int = 0,
    include_grid: bool = True,
    white_fill: bool = False,
    raster: bool = False,
    redesign: str = "",
) -> Path:
    document = fitz.open()
    page = document.new_page(width=400, height=400)
    if include_grid:
        _draw_grid(page, packaging=packaging)
    if redesign == "diagonal":
        for offset in range(-200, 401, 18):
            page.draw_line((max(20, offset), max(20, 20 - offset)),
                           (min(380, offset + 360), min(380, 380 - offset)),
                           color=(0, 0, 0), width=1)
        for center in range(50, 351, 50):
            page.draw_circle((center, 200), 12, color=(0, 0, 0), width=1)
    for start, end in extra_lines:
        page.draw_line(start, end, color=(0, 0, 0), width=1)
    if text:
        page.insert_text((75, 85), text, fontsize=18, color=(0, 0, 0))
    if white_fill:
        page.draw_rect((90, 90, 310, 310), color=None, fill=(1, 1, 1), width=0)
    if raster:
        page.insert_image((20, 20, 380, 380), stream=_png_bytes())
    if rotation:
        page.set_rotation(rotation)
    document.save(path)
    document.close()
    return path


def _compare(
    left: Path,
    right: Path,
    *,
    left_record: dict | None = None,
    right_record: dict | None = None,
) -> dict:
    return compare_prepared_blocks(
        left_pdf_path=left,
        right_pdf_path=right,
        left_records=[dict(BASE_RECORD if left_record is None else left_record)],
        right_records=[dict(BASE_RECORD if right_record is None else right_record)],
    )


def _make_boundary_pdf(path: Path, line) -> Path:
    document = fitz.open()
    page = document.new_page(width=400, height=400)
    for y in range(100, 301, 25):
        page.draw_line((90, y), (310, y), color=(0, 0, 0), width=1)
    for x in range(100, 301, 25):
        page.draw_line((x, 90), (x, 310), color=(0, 0, 0), width=1)
    page.draw_line(*line, color=(0, 0, 0), width=1)
    document.save(path)
    document.close()
    return path


def test_same_prepared_block_has_no_graphic_change(tmp_path: Path):
    pdf = _make_pdf(tmp_path / "same.pdf")
    ledger = _compare(pdf, pdf)
    assert ledger["route"] == "MODE_1_APPLICABLE"
    assert ledger["changes"] == []


def test_small_added_line_is_added_graphic(tmp_path: Path):
    left = _make_pdf(tmp_path / "left.pdf")
    right = _make_pdf(tmp_path / "right.pdf", extra_lines=(((105, 103), (105, 147)),))
    ledger = _compare(left, right)
    assert ledger["route"] == "MODE_1_APPLICABLE"
    assert "ADDED_GRAPHIC" in {change["type"] for change in ledger["changes"]}


def test_small_removed_line_is_removed_graphic(tmp_path: Path):
    left = _make_pdf(tmp_path / "left.pdf", extra_lines=(((105, 103), (105, 147)),))
    right = _make_pdf(tmp_path / "right.pdf")
    ledger = _compare(left, right)
    assert ledger["route"] == "MODE_1_APPLICABLE"
    assert "REMOVED_GRAPHIC" in {change["type"] for change in ledger["changes"]}
    removed = next(change for change in ledger["changes"] if change["type"] == "REMOVED_GRAPHIC")
    assert removed["right_region"] is None
    assert removed["evidence"][0]["right_comparison_bbox_visual_pt"]


def test_small_geometry_modification_is_conservative_geometry_change(tmp_path: Path):
    left = _make_pdf(tmp_path / "left.pdf", extra_lines=(((105, 103), (105, 150)),))
    right = _make_pdf(tmp_path / "right.pdf", extra_lines=(((105, 103), (119, 150)),))
    ledger = _compare(left, right)
    assert ledger["route"] == "MODE_1_APPLICABLE"
    assert "GEOMETRY_CHANGED" in {change["type"] for change in ledger["changes"]}


def test_small_change_inside_huge_block_is_not_diluted(tmp_path: Path):
    left = _make_pdf(tmp_path / "left.pdf", packaging="segmented")
    right = _make_pdf(
        tmp_path / "right.pdf", packaging="segmented",
        extra_lines=(((215, 203), (215, 244)),),
    )
    ledger = _compare(left, right)
    assert ledger["changes"]
    assert ledger["quality"]["diff"]["changed_ink_fraction"] < 0.02


def test_different_pdf_primitive_packaging_same_visible_graphics(tmp_path: Path):
    left = _make_pdf(tmp_path / "left.pdf", packaging="single_path")
    right = _make_pdf(tmp_path / "right.pdf", packaging="segmented")
    ledger = _compare(left, right)
    assert ledger["route"] == "MODE_1_APPLICABLE"
    assert ledger["changes"] == []


@pytest.mark.parametrize("drift", [0.005, 0.02, 0.10])
def test_block_boundary_drift_has_no_false_change(tmp_path: Path, drift: float):
    pdf = _make_pdf(tmp_path / f"drift-{drift}.pdf")
    left_record = {**BASE_RECORD, "coords_norm": [0.05, 0.05, 0.95, 0.95]}
    right_record = {
        **BASE_RECORD,
        "coords_norm": [0.05 + drift, 0.05 + drift, 0.95 - drift, 0.95 - drift],
    }
    ledger = _compare(pdf, pdf, left_record=left_record, right_record=right_record)
    assert ledger["changes"] == []


def test_region_outside_common_crop_is_filtered(tmp_path: Path):
    pdf = _make_boundary_pdf(tmp_path / "outside.pdf", ((30, 100), (30, 150)))
    left_record = {**BASE_RECORD, "coords_norm": [0.15, 0.15, 0.85, 0.85]}
    right_record = {**BASE_RECORD, "coords_norm": [0.05, 0.05, 0.95, 0.95]}
    ledger = _compare(pdf, pdf, left_record=left_record, right_record=right_record)
    assert ledger["changes"] == []
    assert ledger["diagnostics"]["filtered_region_counts"]["OUTSIDE_COMMON_AREA"] == 1


def test_border_region_found_on_source_page_is_crop_artifact(tmp_path: Path):
    pdf = _make_boundary_pdf(tmp_path / "border.pdf", ((50, 100), (70, 100)))
    left_record = {**BASE_RECORD, "coords_norm": [0.15, 0.15, 0.85, 0.85]}
    right_record = {**BASE_RECORD, "coords_norm": [0.05, 0.05, 0.95, 0.95]}
    ledger = _compare(pdf, pdf, left_record=left_record, right_record=right_record)
    assert ledger["changes"] == []
    assert ledger["diagnostics"]["filtered_region_counts"]["CROP_ARTIFACT"] == 1
    filtered = ledger["diagnostics"]["filtered_regions"][0]
    assert filtered["border_probe"]["lookup_only"] is True
    assert filtered["border_probe"]["upstream_bbox_changed"] is False


def test_text_only_edit_never_becomes_graphic_change(tmp_path: Path):
    left = _make_pdf(tmp_path / "left.pdf", text="LOAD 100")
    right = _make_pdf(tmp_path / "right.pdf", text="LOAD 200")
    ledger = _compare(left, right)
    assert ledger["route"] == "MODE_1_APPLICABLE"
    assert ledger["changes"] == []
    assert "before" not in json.dumps(ledger["changes"])
    assert "after" not in json.dumps(ledger["changes"])


@pytest.mark.parametrize("block_type", ["text", "table", "stamp"])
def test_non_graphic_prepared_types_do_not_run_mode1(tmp_path: Path, block_type: str):
    pdf = _make_pdf(tmp_path / f"{block_type}.pdf")
    record = {**BASE_RECORD, "block_type": block_type}
    ledger = _compare(pdf, pdf, left_record=record, right_record=record)
    assert ledger["route"] == "NO_GRAPHIC_COMPARISON"
    assert ledger["mode"] is None
    assert ledger["quality"] == {}


@pytest.mark.parametrize("rotation", [90, 180, 270])
def test_visual_coordinate_contract_handles_pdf_rotation(tmp_path: Path, rotation: int):
    pdf = _make_pdf(tmp_path / f"rotated-{rotation}.pdf", rotation=rotation)
    ledger = _compare(pdf, pdf)
    assert ledger["route"] == "MODE_1_APPLICABLE"
    assert ledger["changes"] == []
    assert ledger["quality"]["extraction"]["left"]["page_rotation"] == rotation


def test_degenerate_horizontal_and_vertical_paths_are_preserved(tmp_path: Path):
    pdf = _make_pdf(tmp_path / "axes.pdf", include_grid=False, extra_lines=(
        ((20, 100), (380, 100)),
        ((200, 20), (200, 380)),
        ((100, 100), (100, 100)),
    ))
    block = block_from_record(pdf, BASE_RECORD, EXPERIMENTALLY_CALIBRATED_V1)
    ink = extract_ink(block, EXPERIMENTALLY_CALIBRATED_V1)
    assert any(abs(segment[1] - segment[3]) < 1e-6 for segment in ink["segments"])
    assert any(abs(segment[0] - segment[2]) < 1e-6 for segment in ink["segments"])


def test_invisible_white_fill_only_paint_is_removed(tmp_path: Path):
    left = _make_pdf(tmp_path / "left.pdf", white_fill=True)
    right = _make_pdf(tmp_path / "right.pdf")
    ledger = _compare(left, right)
    assert ledger["changes"] == []
    assert ledger["quality"]["extraction"]["left"]["invisible_paths_removed"] >= 1


def test_clip_path_preserves_only_visible_segment_interval():
    polygon = np.asarray([(20, 20), (80, 20), (80, 80), (20, 80)], np.float32)
    parts = _clip_segment_to_polygon_group((0, 50, 100, 50), [polygon])
    assert parts == pytest.approx([(20, 50, 80, 50)])


def test_even_odd_filled_path_keeps_hole():
    outer = np.asarray([(10, 10), (90, 10), (90, 90), (10, 90)], np.float32)
    inner = np.asarray([(40, 40), (60, 40), (60, 60), (40, 60)], np.float32)
    mask = rasterize(
        np.zeros((0, 4), np.float32), None, (0, 0, 100, 100), 1.0,
        fills=[{"polys": [outer, inner], "even_odd": True, "clips": None}],
    )
    assert mask[20, 20] == 1
    assert mask[50, 50] == 0


def test_registration_failure_routes_to_mode2_without_change_explosion(tmp_path: Path):
    left = _make_pdf(tmp_path / "left.pdf", include_grid=True)
    right = _make_pdf(tmp_path / "right.pdf", include_grid=False, redesign="diagonal")
    ledger = _compare(left, right)
    assert ledger["route"] == "MODE_2_REQUIRED"
    assert ledger["changes"] == []


def test_strong_redesign_routes_to_mode2_without_publishing_regions(tmp_path: Path):
    left = _make_pdf(tmp_path / "left.pdf", include_grid=True)
    right = _make_pdf(
        tmp_path / "right.pdf", include_grid=True, redesign="diagonal",
        extra_lines=tuple((((20, y), (380, 380 - y))) for y in range(20, 381, 20)),
    )
    ledger = _compare(left, right)
    assert ledger["route"] == "MODE_2_REQUIRED"
    assert ledger["changes"] == []


def test_raster_backed_side_routes_to_targeted_vision_contract(tmp_path: Path):
    left = _make_pdf(tmp_path / "left.pdf", raster=True)
    right = _make_pdf(tmp_path / "right.pdf")
    ledger = _compare(left, right)
    assert ledger["route"] == "VISION_REQUIRED"
    assert ledger["diagnostics"]["routing"]["reason_code"] == "RASTER_BACKED_SOURCE"
    assert ledger["diagnostics"]["targeted_vision"]["whole_block_comparator_used"] is False
    assert ledger["diagnostics"]["targeted_vision"]["requests"] == []


def test_isolated_uncertain_region_creates_only_local_vision_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    pdf = _make_pdf(tmp_path / "uncertain.pdf")

    def uncertain_mode1(*_args, **_kwargs):
        return {
            "registration": {
                "success": True,
                "failure_reason": None,
                "coverage": {"sym_cov": 0.99},
                "confidence": 0.99,
            },
            "diff": {
                "changed_ink_fraction": 0.01,
                "n_regions_published": 1,
            },
            "regions": [{
                "classification": "UNCERTAIN_GRAPHIC_CHANGE",
                "left_bbox_visual_pt": [20.0, 30.0, 40.0, 50.0],
                "right_bbox_visual_pt": [21.0, 31.0, 41.0, 51.0],
                "left_only_cells": 12,
                "right_only_cells": 10,
                "left_only_ink_pt": 12.0,
                "right_only_ink_pt": 10.0,
                "matched_context_cells": 20,
                "text_overlap": 0.0,
                "border_status": "UNRESOLVED",
                "address_hints": [],
            }],
            "filtered_regions": [],
            "debug_view": {"mask_summary": {}},
        }

    monkeypatch.setattr(graphic_router, "run_mode1", uncertain_mode1)
    ledger = _compare(pdf, pdf)
    assert ledger["route"] == "VISION_REQUIRED"
    assert ledger["mode"] == "MODE_1"
    requests = ledger["diagnostics"]["targeted_vision"]["requests"]
    assert len(requests) == 1
    assert requests[0]["left_crop"]["bbox_visual_pt"] == [20.0, 30.0, 40.0, 50.0]
    assert requests[0]["right_crop"]["bbox_visual_pt"] == [21.0, 31.0, 41.0, 51.0]
    assert requests[0]["may_create_vector_coordinates"] is False
    assert ledger["diagnostics"]["targeted_vision"]["whole_block_forbidden"] is True


def test_graphic_change_ledger_schema_and_runtime_validator(tmp_path: Path):
    pdf = _make_pdf(tmp_path / "same.pdf")
    ledger = _compare(pdf, pdf)
    assert schema_path().is_file()
    assert json.loads(schema_path().read_text(encoding="utf-8"))["title"] == "GraphicChangeLedger"
    assert validate_ledger(ledger) is ledger
    damaged = dict(ledger)
    damaged["route"] = "RESEARCH_EOM_PARSER"
    with pytest.raises(LedgerValidationError):
        validate_ledger(damaged)


def test_repeated_execution_is_deterministic(tmp_path: Path):
    left = _make_pdf(tmp_path / "left.pdf")
    right = _make_pdf(tmp_path / "right.pdf", extra_lines=(((105, 103), (105, 147)),))
    first = _compare(left, right)
    second = _compare(left, right)
    assert first == second


def test_page_level_cache_parses_shared_page_once(tmp_path: Path):
    pdf = _make_pdf(tmp_path / "same.pdf")
    PAGE_CACHE.close()
    PAGE_CACHE.reset_stats()
    _compare(pdf, pdf)
    assert PAGE_CACHE.stats["page_parses"] == 1
    assert PAGE_CACHE.stats["page_hits"] >= 1


def test_array_contract_stops_at_mode2_for_many_to_many_scope(tmp_path: Path):
    pdf = _make_pdf(tmp_path / "same.pdf")
    records = [
        {**BASE_RECORD, "block_id": "one"},
        {**BASE_RECORD, "block_id": "two"},
    ]
    ledger = compare_prepared_blocks(
        left_pdf_path=pdf,
        right_pdf_path=pdf,
        left_records=records,
        right_records=records,
    )
    assert ledger["route"] == "MODE_2_REQUIRED"
    assert ledger["mode"] is None
    assert ledger["changes"] == []


def test_empty_prepared_scope_is_no_graphic_comparison(tmp_path: Path):
    pdf = _make_pdf(tmp_path / "same.pdf")
    ledger = compare_prepared_blocks(
        left_pdf_path=pdf,
        right_pdf_path=pdf,
        left_records=[],
        right_records=[],
    )
    assert ledger["route"] == "NO_GRAPHIC_COMPARISON"
    assert ledger["diagnostics"]["routing"]["reason_code"] == "EMPTY_PREPARED_SCOPE"


def test_session_api_resolves_blocks_json_and_persists_independent_ledger(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    stage_left = tmp_path / "stage_1"
    stage_right = tmp_path / "stage_2"
    stage_left.mkdir()
    stage_right.mkdir()
    left_pdf = _make_pdf(stage_left / "left.pdf")
    right_pdf = _make_pdf(stage_right / "right.pdf", extra_lines=(((105, 103), (105, 147)),))
    record = {
        **BASE_RECORD,
        "crop_url": "https://upstream.invalid/already-stored-reference",
    }
    for stage in (stage_left, stage_right):
        (stage / "blocks.json").write_text(
            json.dumps({"schema_version": 1, "pages": [], "blocks": [record]}),
            encoding="utf-8",
        )
    runtime = tmp_path / "runtime"
    monkeypatch.setenv("COMPARISON_ROOT", str(runtime))
    session, _warnings = store.create_session(str(stage_left), str(stage_right))
    pair_view = store.create_pair(session["id"], str(left_pdf), str(right_pdf))
    assert pair_view["graphic_change_ledger"] is None

    app = FastAPI()
    app.include_router(stage_comparison_router.router)
    client = TestClient(app)
    endpoint = (
        f"/api/stage-comparison/sessions/{session['id']}"
        f"/pairs/{pair_view['pair']['id']}/graphic-comparison"
    )
    response = client.post(endpoint, json={
        "left_block_ids": [BASE_RECORD["block_id"]],
        "right_block_ids": [BASE_RECORD["block_id"]],
    })
    assert response.status_code == 200
    ledger = response.json()
    assert ledger["route"] == "MODE_1_APPLICABLE"
    assert "ADDED_GRAPHIC" in {change["type"] for change in ledger["changes"]}
    artifact = runtime / "sessions" / session["id"] / "pairs" / pair_view["pair"]["id"] / "graphic_change_ledger.json"
    assert artifact.is_file()
    assert "upstream.invalid" not in artifact.read_text(encoding="utf-8")

    override = client.post(endpoint, json={
        "left_block_ids": [BASE_RECORD["block_id"]],
        "right_block_ids": [BASE_RECORD["block_id"]],
        "bbox": [0, 0, 1, 1],
    })
    assert override.status_code == 422

    restored = client.get(endpoint)
    assert restored.status_code == 200
    assert restored.json()["diagnostics"]["stale"] is False
