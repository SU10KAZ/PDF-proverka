"""Tests for the Function Lineage v3.0 region feasibility layer.

Every test runs on synthetic geometry.  Nothing here opens a PDF, calls a
model or touches production state.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.function_lineage_v3 import corpus
from experiments.function_lineage_v3 import page_geometry as geometry_module
from experiments.function_lineage_v3 import regions as region_module
from experiments.function_lineage_v3 import region_feasibility as feasibility


# ---------------------------------------------------------------------------
# CAD text repair
# ---------------------------------------------------------------------------


def test_cad_repair_recovers_the_drawing_title():
    raw = "ǙǯǸǹǶǳǸǰǴǸǫȊ\x01ǻǫǼȂǰǽǸǫȊ\x01ǼȀǰǷǫ"
    text, repaired = geometry_module.repair_cad_text(raw)
    assert text == "Однолинейная расчетная схема"
    assert repaired is True


def test_cad_repair_leaves_ordinary_text_alone():
    text, repaired = geometry_module.repair_cad_text("ВРУ-3 Корпус 1,2")
    assert text == "ВРУ-3 Корпус 1,2"
    assert repaired is False


def test_cad_repair_of_empty_text():
    assert geometry_module.repair_cad_text("") == ("", False)


# ---------------------------------------------------------------------------
# edges
# ---------------------------------------------------------------------------


def test_collinear_strokes_weld_into_one_edge():
    segments = np.array([
        [0.0, 10.0, 50.0, 10.0],
        [50.0, 10.0, 120.0, 10.0],
    ])
    horizontal, vertical = geometry_module.axis_edges(segments)
    assert len(vertical) == 0
    assert len(horizontal) == 1
    assert horizontal[0][0] == pytest.approx(0.0)
    assert horizontal[0][2] == pytest.approx(120.0)


def test_short_stroke_is_not_a_boundary():
    segments = np.array([[0.0, 10.0, 2.0, 10.0]])
    horizontal, vertical = geometry_module.axis_edges(segments)
    assert len(horizontal) == 0 and len(vertical) == 0


def test_slanted_stroke_is_never_welded():
    segments = np.array([[0.0, 0.0, 100.0, 100.0]])
    horizontal, vertical = geometry_module.axis_edges(segments)
    assert len(horizontal) == 0 and len(vertical) == 0
    assert geometry_module.unstructured_ink_share(segments) == pytest.approx(1.0)


def test_two_separate_boxes_are_two_structures():
    def box(x0, y0, x1, y1):
        return [
            [x0, y0, x1, y0], [x0, y1, x1, y1],
            [x0, y0, x0, y1], [x1, y0, x1, y1],
        ]

    segments = np.array(box(0, 0, 100, 100) + box(300, 0, 400, 100))
    horizontal, vertical = geometry_module.axis_edges(segments)
    labels, count = geometry_module.incidence_components(horizontal, vertical)
    assert count == 2
    assert len(set(labels.tolist())) == 2


# ---------------------------------------------------------------------------
# regions
# ---------------------------------------------------------------------------


def _page(segments, spans=(), width=1000.0, height=800.0, rotation=0):
    return geometry_module.PageGeometry(
        page=1, rotation=rotation, width=width, height=height,
        spans=list(spans), segments=np.asarray(segments, dtype=np.float64),
    )


def _lattice(x0, y0, x1, y1, rows, columns):
    segments = []
    for index in range(rows):
        y = y0 + (y1 - y0) * index / (rows - 1)
        segments.append([x0, y, x1, y])
    for index in range(columns):
        x = x0 + (x1 - x0) * index / (columns - 1)
        segments.append([x, y0, x, y1])
    return segments


def test_a_lattice_becomes_a_table_with_rows_and_columns():
    page = _page(_lattice(100, 100, 400, 300, rows=4, columns=3))
    found = region_module.build_regions(page)
    tables = [region for region in found if region.kind == "TABLE"]
    assert len(tables) == 1
    assert len(tables[0].rows) == 4
    assert len(tables[0].columns) == 3


def test_a_page_sized_frame_never_owns_anything():
    page = _page([
        [10, 10, 990, 10], [10, 790, 990, 790],
        [10, 10, 10, 790], [990, 10, 990, 790],
    ])
    found = region_module.build_regions(page)
    assert [region.kind for region in found] == ["SHEET_FRAME"]
    index = region_module.build_index(page, found)
    assert index.local == []
    span = {"bbox": [400.0, 400.0, 460.0, 412.0], "size": 10.0, "vertical": False}
    assert region_module.attribute(page, index, span)["relation"] == "UNKNOWN"


def test_text_inside_a_lattice_cell_is_owned_by_the_cell():
    page = _page(_lattice(100, 100, 400, 300, rows=4, columns=3))
    found = region_module.build_regions(page)
    index = region_module.build_index(page, found)
    span = {"bbox": [110.0, 110.0, 190.0, 150.0], "size": 10.0, "vertical": False}
    result = region_module.attribute(page, index, span)
    assert result["relation"] == "TABLE_CELL"
    assert result["applicability"] == "FRAGMENT_LOCAL"
    assert result["cell"] == {"row": 0, "column": 0}


def test_text_inside_a_drawn_box_is_owned_by_the_box():
    page = _page([
        [100, 100, 300, 100], [100, 200, 300, 200],
        [100, 100, 100, 200], [300, 100, 300, 200],
    ])
    found = region_module.build_regions(page)
    assert [region.kind for region in found] == ["BOX"]
    index = region_module.build_index(page, found)
    span = {"bbox": [120.0, 120.0, 200.0, 140.0], "size": 10.0, "vertical": False}
    assert region_module.attribute(page, index, span)["relation"] == "DIRECT_CONTAINMENT"


def test_a_leader_drawn_along_the_label_attaches_it():
    page = _page([[100.0, 100.0, 100.0, 200.0], [100.0, 200.0, 130.0, 200.0]])
    found = region_module.build_regions(page)
    index = region_module.build_index(page, found)
    span = {"bbox": [102.0, 100.0, 112.0, 200.0], "size": 10.0, "vertical": True}
    assert region_module.attribute(page, index, span)["relation"] == "CONNECTED_CALLOUT"


def test_a_stroke_that_merely_passes_nearby_does_not_attach():
    page = _page([[100.0, 100.0, 100.0, 120.0], [100.0, 120.0, 130.0, 120.0]])
    found = region_module.build_regions(page)
    index = region_module.build_index(page, found)
    span = {"bbox": [102.0, 100.0, 112.0, 200.0], "size": 10.0, "vertical": True}
    assert region_module.attribute(page, index, span)["relation"] == "UNKNOWN"


def test_two_candidate_leaders_leave_the_value_ambiguous():
    page = _page([[100.0, 100.0, 100.0, 200.0], [112.0, 100.0, 112.0, 200.0]])
    found = region_module.build_regions(page)
    assert len(found) == 2
    index = region_module.build_index(page, found)
    span = {"bbox": [102.0, 100.0, 110.0, 200.0], "size": 10.0, "vertical": True}
    result = region_module.attribute(page, index, span)
    assert result["relation"] == "AMBIGUOUS"
    assert result["applicability"] == "UNKNOWN"


def test_stamp_zone_text_is_sheet_shared():
    page = _page(_lattice(600, 700, 990, 790, rows=4, columns=3))
    found = region_module.build_regions(page)
    index = region_module.build_index(page, found)
    span = {"bbox": [700.0, 720.0, 800.0, 740.0], "size": 10.0, "vertical": False}
    result = region_module.attribute(page, index, span)
    assert result["relation"] == "SHEET_SHARED"
    assert result["applicability"] == "SHEET_SHARED"


def test_a_page_without_ink_yields_no_graphic_ownership():
    page = _page([])
    page.text_blocks = [{"text": "раздел", "bbox": [10.0, 10.0, 200.0, 40.0]}]
    found = region_module.build_regions(page)
    assert [region.kind for region in found] == ["TEXT_SECTION"]
    index = region_module.build_index(page, found)
    span = {"bbox": [12.0, 12.0, 100.0, 30.0], "size": 10.0, "vertical": False}
    assert region_module.attribute(page, index, span)["relation"] == "UNKNOWN"


# ---------------------------------------------------------------------------
# placement of documented values
# ---------------------------------------------------------------------------


def _record(spans, segments):
    page = _page(segments, spans)
    found = region_module.build_regions(page)
    index = region_module.build_index(page, found)
    return {
        "geometry": page,
        "regions": found,
        "index": index,
        "attributions": [region_module.attribute(page, index, span) for span in page.spans],
    }


def test_value_printed_in_one_region_is_region_local():
    record = _record(
        spans=[{"text": "Корпус 3", "bbox": [110.0, 110.0, 190.0, 150.0], "size": 10.0, "vertical": False}],
        segments=_lattice(100, 100, 400, 300, rows=4, columns=3),
    )
    placement = feasibility._value_placement(record, "Корпус 3")
    assert placement["status"] == "REGION_LOCAL"
    assert placement["applicability"] == "FRAGMENT_LOCAL"


def test_value_printed_in_two_regions_is_not_local():
    segments = _lattice(100, 100, 400, 300, rows=4, columns=3) + _lattice(
        500, 100, 800, 300, rows=4, columns=3
    )
    record = _record(
        spans=[
            {"text": "Корпус 3", "bbox": [110.0, 110.0, 190.0, 150.0], "size": 10.0, "vertical": False},
            {"text": "Корпус 3", "bbox": [510.0, 110.0, 590.0, 150.0], "size": 10.0, "vertical": False},
        ],
        segments=segments,
    )
    placement = feasibility._value_placement(record, "Корпус 3")
    assert placement["status"] == "MANY_REGIONS"
    assert placement["applicability"] == "UNKNOWN"


def test_value_only_in_the_stamp_stays_sheet_shared():
    record = _record(
        spans=[{"text": "Корпус 3", "bbox": [700.0, 720.0, 800.0, 740.0], "size": 10.0, "vertical": False}],
        segments=_lattice(600, 700, 990, 790, rows=4, columns=3),
    )
    placement = feasibility._value_placement(record, "Корпус 3")
    assert placement["status"] == "SHEET_SHARED"
    assert placement["applicability"] == "SHEET_SHARED"


def test_value_absent_from_the_text_layer_is_refused():
    record = _record(spans=[], segments=[])
    placement = feasibility._value_placement(record, "Корпус 3")
    assert placement["status"] == "NOT_IN_TEXT_LAYER"
    assert placement["applicability"] == "UNKNOWN"


def test_native_text_only_confirms_what_recognition_saw():
    assert feasibility._confirmed_by_markdown("Корпус 3", "… питание корпуса 3 …") is False
    assert feasibility._confirmed_by_markdown("Корпус 3", "… ВРУ3 — Корпус 3 …") is True


def test_normalization_folds_case_and_punctuation():
    assert corpus.normalize("Корпус №1,2") == "корпус 1 2"
    assert corpus.normalize("ЁЖ") == "еж"


# ---------------------------------------------------------------------------
# contract shape
# ---------------------------------------------------------------------------


def test_data_contract_names_every_channel_the_prototype_uses():
    contract = feasibility.data_contract()
    for section in ("text_spans", "boundaries", "structures", "table_cells", "attachments", "regions"):
        assert section in contract["sections"]
    assert "NATIVE_PDF_TEXT_CAD_REPAIRED" in contract["provenance_values"]
    assert set(contract["applicability_values"]) == set(feasibility.APPLICABILITY)


def test_block_drawing_rules_do_not_change_production_ui():
    rules = feasibility.block_drawing_rules()
    assert rules["production_ui_unchanged"] is True
    assert len(rules["rules"]) >= 5


def test_proving_relations_never_include_proximity_or_sheet_scope():
    assert "SHEET_SHARED" not in region_module.PROVING_RELATIONS
    assert "AMBIGUOUS" not in region_module.PROVING_RELATIONS
    assert "UNKNOWN" not in region_module.PROVING_RELATIONS


def test_cad_repair_leaves_a_stray_glyph_in_an_ordinary_font_alone():
    # ``Ʃ`` in ArialMT shifts into Coptic, not Cyrillic, so nothing is rewritten.
    text, repaired = geometry_module.repair_cad_text(" Ʃ")
    assert text == " Ʃ"
    assert repaired is False
