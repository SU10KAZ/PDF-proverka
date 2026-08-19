"""Тесты диагностической витрины новой цепочки сравнения.

Проверяют главные обещания режима: показываются ВСЕ группы, дубли не
склеиваются (только считаются), координаты пересчитываются на обе стороны,
и ни один артефакт не перезаписывается.
"""
from __future__ import annotations

import json

import pytest

from backend.app.services.stage_comparison import diagnostic_new_pipeline as dnp


# ─── фикстуры ────────────────────────────────────────────────────────────────


def _prepared(pages):
    return {
        "kind": "stage_comparison_prepared_document",
        "schema_version": 1,
        "pages": pages,
    }


def _page(pdf_page, sheet_number, sheet_name="Лист", width=1000.0, height=2000.0):
    return {
        "pdf_page": pdf_page, "page_index": pdf_page - 1,
        "sheet_number": sheet_number, "sheet_name": sheet_name,
        "page_size": {"width": width, "height": height},
        "source_type": "vector", "blocks": [],
    }


def _detection(matrix=None):
    return {
        "kind": dnp.CHANGE_DETECTION_KIND,
        "summary": {"aligned_pairs": 1, "change_groups": 3},
        "requires_alignment_fallback": [],
        "items": [{
            "left_page": 7, "right_page": 6, "status": "review_required",
            "review_reasons": ["many_change_groups"],
            "alignment": {
                "status": "aligned",
                "quality": {"confidence": 1.0},
                "transform": {"matrix": matrix or [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]]},
            },
            "metrics": {"atomic_regions": 3},
            "diagnostics": {},
            "atomic_regions": [],
            "change_groups": [],
        }],
    }


def _semantic(items):
    return {
        "kind": dnp.SEMANTIC_DIFF_KIND,
        "schema_version": 1,
        "summary": {"total_groups": len(items), "evidence_levels": {}},
        "settings": {"llm_used": False, "findings_created": False},
        "pilot_v6a1_parity": {"available": True, "unchanged": True},
        "items": items,
    }


def _item(group_id, before, after, *, level="strong", bbox=(100.0, 200.0, 300.0, 400.0), **extra):
    value = {
        "group_id": group_id, "left_page": 7, "right_page": 6,
        "bbox": list(bbox), "before": before, "after": after,
        "evidence_level": level, "confidence": 0.88, "source": "deterministic_v6a1",
        "semantic_type": "table", "region_role": "drawing",
        "change_kind": "changed", "change_summary": "Правка.",
        "change_types": ["text"], "resolution_status": "deterministically_resolved",
        "requires_human_review": False, "next_analysis": None, "unresolved_reason": None,
        "atomic_region_ids": [], "atomic_region_evidence": [],
        "table_changes": [], "numeric_context_changes": [], "stamp_field_changes": [],
        "inserted_table_rows": [], "removed_table_rows": [], "block_ids": [],
    }
    value.update(extra)
    return value


def _write(comparison, semantic, detection):
    dnp.semantic_diff_path(comparison).parent.mkdir(parents=True, exist_ok=True)
    dnp.change_detection_path(comparison).parent.mkdir(parents=True, exist_ok=True)
    dnp.semantic_diff_path(comparison).write_text(json.dumps(semantic, ensure_ascii=False), encoding="utf-8")
    dnp.change_detection_path(comparison).write_text(json.dumps(detection, ensure_ascii=False), encoding="utf-8")


@pytest.fixture()
def comparison(tmp_path):
    return tmp_path / "comparison"


# ─── недоступность ───────────────────────────────────────────────────────────


def test_missing_semantic_diff_reports_reason(comparison):
    payload = dnp.build_payload(comparison, _prepared([]), _prepared([]), [])
    assert payload["available"] is False
    assert payload["reason"] == "semantic_diff_v6a2_missing_run_stage_6a2_first"
    assert payload["items"] == []


def test_missing_change_detection_reports_reason(comparison):
    dnp.semantic_diff_path(comparison).parent.mkdir(parents=True, exist_ok=True)
    dnp.semantic_diff_path(comparison).write_text(json.dumps(_semantic([])), encoding="utf-8")
    payload = dnp.build_payload(comparison, _prepared([]), _prepared([]), [])
    assert payload["available"] is False
    assert payload["reason"] == "change_detection_v5b4_missing_run_stage_5b4_first"


def test_wrong_kind_is_not_accepted(comparison):
    semantic = _semantic([])
    semantic["kind"] = "stage_comparison_semantic_diff_v6a1_pilot"
    _write(comparison, semantic, _detection())
    assert dnp.build_payload(comparison, _prepared([]), _prepared([]), [])["available"] is False


# ─── полнота выдачи ──────────────────────────────────────────────────────────


def test_all_groups_are_returned_without_filtering(comparison):
    items = [_item(f"group_{i:03d}", "A", "B", level=lvl)
             for i, lvl in enumerate(["exact", "strong", "contextual", "insufficient"], start=1)]
    _write(comparison, _semantic(items), _detection())
    payload = dnp.build_payload(comparison, _prepared([_page(7, "2")]), _prepared([_page(6, "2")]), [])
    assert payload["available"] is True
    assert len(payload["items"]) == 4
    assert {row["evidence_level"] for row in payload["items"]} == {
        "exact", "strong", "contextual", "insufficient",
    }


def test_duplicates_are_counted_but_never_merged(comparison):
    """Одинаковые «Было → Стало» остаются отдельными строками — это и есть
    дефект локализации, который надо увидеть глазами."""
    items = [
        _item("group_001", "A", "B", bbox=(0, 0, 10, 10)),
        _item("group_002", "A", "B", bbox=(50, 50, 60, 60)),
        _item("group_003", "A", "B", bbox=(90, 90, 99, 99)),
        _item("group_004", "X", "Y", bbox=(10, 10, 20, 20)),
    ]
    _write(comparison, _semantic(items), _detection())
    payload = dnp.build_payload(comparison, _prepared([_page(7, "2")]), _prepared([_page(6, "2")]), [])

    assert len(payload["items"]) == 4, "строки не должны схлопываться"
    by_id = {row["group_id"]: row for row in payload["items"]}
    for group_id in ("group_001", "group_002", "group_003"):
        assert by_id[group_id]["same_semantic_result_as_other_groups"] == 2
        assert len(by_id[group_id]["same_semantic_result_group_ids"]) == 2
    assert by_id["group_004"]["same_semantic_result_as_other_groups"] == 0
    # bbox у каждой группы свой — геометрия не потеряна
    assert len({tuple(row["bbox"]) for row in payload["items"]}) == 4


# ─── геометрия ───────────────────────────────────────────────────────────────


def test_bbox_is_normalised_for_overlay(comparison):
    _write(comparison, _semantic([_item("group_001", "A", "B", bbox=(100, 200, 300, 400))]), _detection())
    payload = dnp.build_payload(
        comparison, _prepared([_page(7, "2", width=1000, height=2000)]),
        _prepared([_page(6, "2", width=1000, height=2000)]), [],
    )
    row = payload["items"][0]
    assert row["bbox_norm_left"] == [0.1, 0.1, 0.3, 0.2]


def test_right_bbox_uses_inverse_alignment_matrix(comparison):
    """bbox задан в координатах V2; правая сторона получается обратной матрицей."""
    matrix = [[1.0, 0.0, 25.0], [0.0, 1.0, -10.0], [0.0, 0.0, 1.0]]  # V3 → V2: сдвиг
    _write(comparison, _semantic([_item("group_001", "A", "B", bbox=(100, 200, 300, 400))]), _detection(matrix))
    payload = dnp.build_payload(
        comparison, _prepared([_page(7, "2", width=1000, height=2000)]),
        _prepared([_page(6, "2", width=1000, height=2000)]), [],
    )
    row = payload["items"][0]
    assert row["bbox_right"] == pytest.approx([75.0, 210.0, 275.0, 410.0])
    assert row["bbox_norm_right"] == pytest.approx([0.075, 0.105, 0.275, 0.205])


def test_degenerate_matrix_does_not_break_the_view(comparison):
    singular = [[0.0, 0.0, 0.0], [0.0, 0.0, 0.0], [0.0, 0.0, 1.0]]
    _write(comparison, _semantic([_item("group_001", "A", "B")]), _detection(singular))
    payload = dnp.build_payload(comparison, _prepared([_page(7, "2")]), _prepared([_page(6, "2")]), [])
    row = payload["items"][0]
    assert payload["available"] is True
    assert row["bbox_norm_left"] is not None
    assert row["bbox_right"] is None and row["bbox_norm_right"] is None


# ─── обогащение ──────────────────────────────────────────────────────────────


def test_sheet_numbers_and_slot_come_from_prepared_and_alignment(comparison):
    _write(comparison, _semantic([_item("group_001", "A", "B")]), _detection())
    payload = dnp.build_payload(
        comparison,
        _prepared([_page(7, "2", "Кладочный план")]),
        _prepared([_page(6, "2", "Кладочный план")]),
        [{"slot": 12, "left_page": 7, "right_page": 6}],
    )
    row = payload["items"][0]
    assert row["left_sheet"] == "2" and row["right_sheet"] == "2"
    assert row["left_sheet_name"] == "Кладочный план"
    assert row["alignment_slot"] == 12
    assert row["pair_status"] == "review_required"
    assert row["pair_review_reasons"] == ["many_change_groups"]


def test_cell_changes_cover_table_numbers_and_stamp(comparison):
    item = _item(
        "group_001", "A", "B",
        table_changes=[{"row_label": "СВ-1.1", "column_label": "Кол-во, м³",
                        "before": "331,47", "after": "180,48", "evidence_level": "exact"}],
        numeric_context_changes=[{"label": "В3", "unit": "мм", "before": "17,90",
                                  "after": "21,60", "evidence_level": "exact"}],
        stamp_field_changes=[{"field": "№док.", "change": "added", "before": "отсутствует",
                              "after": "1388/26", "evidence_level": "exact"}],
    )
    _write(comparison, _semantic([item]), _detection())
    payload = dnp.build_payload(comparison, _prepared([_page(7, "2")]), _prepared([_page(6, "2")]), [])
    kinds = [change["kind"] for change in payload["items"][0]["cell_changes"]]
    assert kinds == ["table_cell", "number", "stamp_field"]
    first = payload["items"][0]["cell_changes"][0]
    assert (first["position"], first["column"]) == ("СВ-1.1", "Кол-во, м³")
    assert (first["before"], first["after"]) == ("331,47", "180,48")


def test_atomic_regions_are_exposed_with_both_side_coordinates(comparison):
    item = _item("group_001", "A", "B", atomic_region_ids=["region_001"], atomic_region_evidence=[{
        "region_id": "region_001", "bbox": [100, 200, 300, 400], "change_types": ["text"],
        "region_role": "drawing", "confidence": 0.71, "strength": 0.25,
        "diff_counts": {"text_moved": 3}, "evidence_ids": ["raw_0015"],
        "left_block_ids": ["blk_a"], "right_block_ids": ["blk_b"],
    }])
    _write(comparison, _semantic([item]), _detection())
    payload = dnp.build_payload(
        comparison, _prepared([_page(7, "2", width=1000, height=2000)]),
        _prepared([_page(6, "2", width=1000, height=2000)]), [],
    )
    region = payload["items"][0]["atomic_regions"][0]
    assert region["region_id"] == "region_001"
    assert region["bbox_norm_left"] == [0.1, 0.1, 0.3, 0.2]
    assert region["bbox_norm_right"] == [0.1, 0.1, 0.3, 0.2]
    assert region["diff_counts"] == {"text_moved": 3}


def test_table_rows_are_flattened_for_display(comparison):
    item = _item("group_001", "A", "B", inserted_table_rows=[
        {"cells": [{"text": "П.УУ"}, {"text": "2500"}, {"text": ""}]},
    ], removed_table_rows=[{"cells": [{"text": "старая"}]}])
    _write(comparison, _semantic([item]), _detection())
    payload = dnp.build_payload(comparison, _prepared([_page(7, "2")]), _prepared([_page(6, "2")]), [])
    rows = payload["items"][0]["table_rows"]
    assert rows["inserted"] == ["П.УУ | 2500"]
    assert rows["removed"] == ["старая"]


# ─── кропы ───────────────────────────────────────────────────────────────────


def test_existing_pilot_crop_is_reused_and_overlay_is_not_invented(comparison):
    pilot = comparison.joinpath(*dnp.PILOT_CROPS_DIR)
    pilot.mkdir(parents=True, exist_ok=True)
    (pilot / "v2_007_v3_006_group_001_v2.png").write_bytes(b"png")
    (pilot / "v2_007_v3_006_group_001_overlay.png").write_bytes(b"png")
    _write(comparison, _semantic([_item("group_001", "A", "B"), _item("group_002", "C", "D")]), _detection())
    payload = dnp.build_payload(comparison, _prepared([_page(7, "2")]), _prepared([_page(6, "2")]), [])
    by_id = {row["group_id"]: row for row in payload["items"]}

    assert by_id["group_001"]["crops"]["v2"]["source"] == "pilot_file"
    assert by_id["group_001"]["crops"]["overlay"]["available"] is True
    # V3-кроп пилота нет — рендерим на лету, а overlay заново НЕ строим
    assert by_id["group_001"]["crops"]["v3"]["source"] == "on_demand_render"
    assert by_id["group_002"]["crops"]["overlay"]["available"] is False
    assert by_id["group_002"]["crops"]["v2"]["source"] == "on_demand_render"


def test_find_group_resolves_by_composite_id(comparison):
    _write(comparison, _semantic([_item("group_001", "A", "B"), _item("group_002", "C", "D")]), _detection())
    payload = dnp.build_payload(comparison, _prepared([_page(7, "2")]), _prepared([_page(6, "2")]), [])
    assert dnp.find_group(payload, 7, 6, "group_002")["after"] == "D"
    assert dnp.find_group(payload, 7, 6, "group_404") is None
    assert dnp.find_group(payload, 9, 8, "group_001") is None


# ─── неизменность артефактов ─────────────────────────────────────────────────


def test_building_the_view_writes_nothing(comparison):
    _write(comparison, _semantic([_item("group_001", "A", "B")]), _detection())
    before = {path: path.stat().st_mtime_ns for path in sorted(comparison.rglob("*")) if path.is_file()}
    listing_before = sorted(str(p.relative_to(comparison)) for p in comparison.rglob("*"))

    dnp.build_payload(comparison, _prepared([_page(7, "2")]), _prepared([_page(6, "2")]), [])

    after = {path: path.stat().st_mtime_ns for path in sorted(comparison.rglob("*")) if path.is_file()}
    listing_after = sorted(str(p.relative_to(comparison)) for p in comparison.rglob("*"))
    assert before == after, "витрина не должна перезаписывать артефакты"
    assert listing_before == listing_after, "витрина не должна создавать файлы"


def test_flag_disables_the_mode(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_NEW_PIPELINE_DIAGNOSTIC_ENABLED", "0")
    assert dnp.is_enabled() is False
    monkeypatch.setenv("STAGE_COMPARISON_NEW_PIPELINE_DIAGNOSTIC_ENABLED", "true")
    assert dnp.is_enabled() is True
    monkeypatch.delenv("STAGE_COMPARISON_NEW_PIPELINE_DIAGNOSTIC_ENABLED")
    assert dnp.is_enabled() is True, "по умолчанию режим доступен"
