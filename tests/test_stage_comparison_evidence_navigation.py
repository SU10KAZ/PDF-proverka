from __future__ import annotations

from backend.app.services.stage_comparison.evidence_navigation import (
    build_evidence_availability_index,
    build_evidence_navigation,
    build_inline_evidence_navigation,
)


def _target(source_mode="TEXT", evidence=None):
    return {
        "changes": [{
            "change_id": "change-1",
            "source_mode": source_mode,
            "evidence_refs": evidence or [{
                "evidence_ref": "text-evidence",
                "atom_id": "text-atom",
                "source": "TEXT",
                "source_artifact": {"artifact_ref": "text.json"},
            }],
        }],
        "review_items": [],
    }


def _text_atoms():
    return {"atoms": [{
        "atom_id": "text-atom",
        "provenance": {"locations": {
            "LEFT": [{"page": 10, "fragment_id": "lf", "bboxes": [[1, 2, 3, 4]]}],
            "RIGHT": [{"page": 24, "fragment_id": "rf", "bboxes": [[5, 6, 7, 8]]}],
        }},
    }]}


def _graphic_ledger():
    return {"changes": [{
        "change_id": "graphic-evidence",
        "left_region": {"page_index": 9, "block_id": "lb", "bbox_visual_pt": [10, 20, 30, 40]},
        "right_region": {"page_index": 23, "block_id": "rb", "bbox_visual_pt": [11, 21, 31, 41]},
        "structural": {"left_nodes": ["QS1"], "right_nodes": ["QF3"]},
    }]}


def test_text_click_contains_exact_pages_and_highlights():
    payload = build_evidence_navigation(
        "change-1",
        synthesis=_target(),
        text_atoms=_text_atoms(),
        documents={"LEFT": "left.pdf", "RIGHT": "right.pdf"},
    )

    assert payload["layout"] == "SIDE_BY_SIDE"
    assert payload["sides"]["LEFT"][0]["page"] == 10
    assert payload["sides"]["RIGHT"][0]["highlight"]["bboxes"] == [[5, 6, 7, 8]]
    assert payload["viewer_action"]["zoom_to_highlights"] is True


def test_graphic_click_contains_block_node_page_and_bbox():
    evidence = [{
        "evidence_ref": "graphic-evidence", "atom_id": "graphic-atom", "source": "GRAPHIC",
        "source_artifact": {"artifact_ref": "graphic.json"},
    }]
    payload = build_evidence_navigation(
        "change-1",
        synthesis=_target("GRAPHIC", evidence),
        graphic_ledger=_graphic_ledger(),
    )

    left = payload["sides"]["LEFT"][0]
    assert left["page"] == 10
    assert left["block_id"] == "lb"
    assert left["node_id"] == "QS1"
    assert left["highlight"] == {"kind": "BBOX", "bbox": [10, 20, 30, 40]}
    assert left["coordinate_space"] == "PDF_VISUAL_PT"


def test_qf_like_graphic_ref_uses_the_same_availability_as_navigation():
    evidence = [{
        "evidence_ref": "graphic-evidence",
        "atom_id": "graphic:qf-current",
        "source": "GRAPHIC",
        "source_artifact": {"kind": "graphic_change_ledger"},
    }]
    synthesis = _target("GRAPHIC", evidence)

    availability = build_evidence_availability_index(
        synthesis=synthesis,
        graphic_ledger=_graphic_ledger(),
    )
    navigation = build_evidence_navigation(
        "change-1",
        synthesis=synthesis,
        graphic_ledger=_graphic_ledger(),
    )

    assert availability == {"change-1": True}
    assert navigation["has_evidence"] is True
    assert navigation["sides"]["LEFT"] and navigation["sides"]["RIGHT"]


def test_unresolved_evidence_ref_is_not_reported_as_available():
    availability = build_evidence_availability_index(
        synthesis=_target("GRAPHIC", [{
            "evidence_ref": "missing",
            "atom_id": "graphic:missing",
            "source": "GRAPHIC",
            "source_artifact": {},
        }]),
        graphic_ledger={"changes": []},
    )

    assert availability == {"change-1": False}


def test_both_evidence_is_available_side_by_side():
    evidence = [
        {"evidence_ref": "text-evidence", "atom_id": "text-atom", "source": "TEXT", "source_artifact": {}},
        {"evidence_ref": "graphic-evidence", "atom_id": "graphic-atom", "source": "GRAPHIC", "source_artifact": {}},
    ]
    payload = build_evidence_navigation(
        "change-1",
        synthesis=_target("BOTH", evidence),
        text_atoms=_text_atoms(),
        graphic_ledger=_graphic_ledger(),
    )

    assert payload["layout"] == "SIDE_BY_SIDE"
    assert {item["source"] for item in payload["sides"]["LEFT"]} == {"TEXT", "GRAPHIC"}
    assert {item["source"] for item in payload["sides"]["RIGHT"]} == {"TEXT", "GRAPHIC"}


def test_missing_graphic_coordinates_are_reported_honestly():
    evidence = [{
        "evidence_ref": "graphic-evidence", "atom_id": "graphic-atom", "source": "GRAPHIC",
        "source_artifact": {},
    }]
    ledger = {"changes": [{"change_id": "graphic-evidence", "left_region": None, "right_region": None}]}

    payload = build_evidence_navigation(
        "change-1", synthesis=_target("GRAPHIC", evidence), graphic_ledger=ledger,
    )

    assert payload["sides"]["LEFT"][0]["highlight"] is None
    assert payload["sides"]["LEFT"][0]["coordinates_available"] is False


def test_graphic_bbox_is_normalized_when_page_size_is_available():
    evidence = [{
        "evidence_ref": "graphic-evidence", "atom_id": "graphic-atom", "source": "GRAPHIC",
        "source_artifact": {},
    }]
    payload = build_evidence_navigation(
        "change-1",
        synthesis=_target("GRAPHIC", evidence),
        graphic_ledger=_graphic_ledger(),
        page_sizes={
            "LEFT": {10: {"width": 100, "height": 200}},
            "RIGHT": {24: {"width": 100, "height": 200}},
        },
    )

    left = payload["sides"]["LEFT"][0]
    assert left["coordinate_space"] == "NORMALIZED_PAGE_TOP_LEFT"
    assert left["highlight"] == {"kind": "BBOX", "bbox": [.1, .1, .3, .2]}


def test_inline_human_review_evidence_uses_real_page_and_normalized_bbox():
    payload = build_inline_evidence_navigation(
        "hrg_mode_33434a66cf174adbf52396e7",
        evidence={
            "LEFT": [{"raw": "Рр=232,8/133,9 кВт", "bbox": [10, 20, 30, 40]}],
            "RIGHT": [{"raw": "Рр,кВт 131.2", "bbox": [50, 60, 70, 80]}],
        },
        default_pages={"LEFT": 1, "RIGHT": 2},
        page_sizes={
            "LEFT": {1: {"width": 100, "height": 200}},
            "RIGHT": {2: {"width": 100, "height": 200}},
        },
        source_mode="HUMAN_REVIEW_MODE_GROUP",
    )

    assert payload["layout"] == "SIDE_BY_SIDE"
    assert payload["source_mode"] == "HUMAN_REVIEW_MODE_GROUP"
    assert payload["sides"]["LEFT"][0]["page"] == 1
    assert payload["sides"]["RIGHT"][0]["page"] == 2
    assert payload["sides"]["LEFT"][0]["raw_text"] == "Рр=232,8/133,9 кВт"
    assert payload["sides"]["LEFT"][0]["highlight"] == {
        "kind": "BBOX", "bbox": [.1, .1, .3, .2],
    }


def test_inline_inconsistency_evidence_keeps_single_side_honest():
    payload = build_inline_evidence_navigation(
        "dinc_1",
        evidence={"RIGHT": {"bbox": [1, 2, 3, 4], "block_id": "right-block"}},
        default_pages={"RIGHT": 1},
        source_mode="DOCUMENT_INCONSISTENCY",
    )

    assert payload["layout"] == "SINGLE_SIDE"
    assert payload["sides"]["LEFT"] == []
    assert payload["sides"]["RIGHT"][0]["page"] == 1
    assert payload["sides"]["RIGHT"][0]["block_id"] == "right-block"


def test_inline_text_requirement_keeps_raw_text_and_normalized_source_span():
    payload = build_inline_evidence_navigation(
        "ureview_requirement",
        evidence={
            "RIGHT": [{
                "source": "TEXT",
                "page": 1,
                "fragment_id": "txt_requirement",
                "bbox": [.1, .2, .3, .25],
                "coordinate_space": "NORMALIZED_PAGE_TOP_LEFT",
                "raw_text": "Щиты выполнить не ниже IP31.",
                "bounded_absence": {
                    "opposite_side": "LEFT",
                    "recognition_coverage": "HIGH",
                    "exact_match": None,
                    "normalized_match": None,
                    "strong_semantic_candidate": None,
                },
            }],
        },
        source_mode="TEXT_REQUIREMENT_SOURCE",
    )

    right = payload["sides"]["RIGHT"][0]
    assert payload["layout"] == "SINGLE_SIDE"
    assert payload["source_mode"] == "TEXT_REQUIREMENT_SOURCE"
    assert right["page"] == 1
    assert right["raw_text"] == "Щиты выполнить не ниже IP31."
    assert right["highlight"] == {"kind": "BBOX", "bbox": [.1, .2, .3, .25]}
    assert payload["trace"][0]["record"]["bounded_absence"] == {
        "opposite_side": "LEFT",
        "recognition_coverage": "HIGH",
        "exact_match": None,
        "normalized_match": None,
        "strong_semantic_candidate": None,
    }


# --------------------------------------------------------------------------
# Изменения из таблиц нагрузок
# --------------------------------------------------------------------------
def _load_table_change(change_id: str) -> dict:
    return {
        "change_id": change_id,
        "subject": "ХМ1",
        "facet_ref": "demand_active_power_kw",
        "evidence": {
            "LEFT": {
                "row_id": "etrow_left",
                "page_index": 0,
                "bbox": [100.0, 200.0, 140.0, 260.0],
                "raw": "Рр=157,5 кВт",
            },
            "RIGHT": {
                "row_id": "etrow_right",
                "page_index": 0,
                "bbox": [300.0, 400.0, 340.0, 470.0],
                "raw": "335.0 кВт",
            },
        },
    }


def _table_synthesis(change_id: str) -> dict:
    return {
        "changes": [
            {
                "change_id": change_id,
                "source_mode": "GRAPHIC",
                "evidence_refs": [
                    {
                        "evidence_ref": change_id,
                        "atom_id": f"graphic:{change_id}",
                        "source": "GRAPHIC",
                        "source_artifact": {
                            "kind": "electrical_table_changes",
                            "schema_version": "electrical-table-diff.v1",
                            "artifact_ref": "sha256:0",
                        },
                    }
                ],
            }
        ],
        "review_items": [],
    }


def test_изменение_из_таблицы_ведёт_на_обе_строки():
    """У находки из таблицы нет узла графа — доказательство это сама строка.

    Без отдельной ветки кнопка «Открыть доказательство» вела бы в пустоту, и
    доказанная находка выглядела бы голословной.
    """
    payload = build_evidence_navigation(
        "etchg_1",
        synthesis=_table_synthesis("etchg_1"),
        electrical_table_changes={"changes": [_load_table_change("etchg_1")]},
        documents={"LEFT": {"document_ref": "L"}, "RIGHT": {"document_ref": "R"}},
        page_sizes={
            "LEFT": {1: {"width": 1000.0, "height": 500.0}},
            "RIGHT": {1: {"width": 2000.0, "height": 1000.0}},
        },
    )
    assert payload["layout"] == "SIDE_BY_SIDE"
    left = payload["sides"]["LEFT"][0]
    right = payload["sides"]["RIGHT"][0]
    assert left["page"] == 1 and right["page"] == 1
    assert left["coordinates_available"] is True
    assert left["coordinate_space"] == "NORMALIZED_PAGE_TOP_LEFT"
    assert left["highlight"]["bbox"] == [0.1, 0.4, 0.14, 0.52]
    assert right["highlight"]["bbox"] == [0.15, 0.4, 0.17, 0.47]
    assert left["fragment_id"] == "etrow_left"


def test_таблица_без_размеров_страницы_координаты_не_выдумывает():
    payload = build_evidence_navigation(
        "etchg_1",
        synthesis=_table_synthesis("etchg_1"),
        electrical_table_changes={"changes": [_load_table_change("etchg_1")]},
        documents={"LEFT": {"document_ref": "L"}, "RIGHT": {"document_ref": "R"}},
    )
    left = payload["sides"]["LEFT"][0]
    assert left["coordinate_space"] == "PDF_VISUAL_PT"
    assert left["page_size"] is None


def test_изменение_графа_читается_по_прежнему():
    """Ветка таблиц не должна перехватывать находки графа."""
    payload = build_evidence_navigation(
        "chg_1",
        synthesis=_table_synthesis("chg_1"),
        graphic_ledger={
            "changes": [
                {
                    "change_id": "chg_1",
                    "left_region": {
                        "block_id": "blk_l",
                        "page_index": 0,
                        "bbox_visual_pt": [10.0, 20.0, 30.0, 40.0],
                    },
                    "right_region": {
                        "block_id": "blk_r",
                        "page_index": 0,
                        "bbox_visual_pt": [50.0, 60.0, 70.0, 80.0],
                    },
                    "structural": {"left_nodes": ["N1"], "right_nodes": ["N1"]},
                }
            ]
        },
        electrical_table_changes={"changes": []},
        documents={"LEFT": {"document_ref": "L"}, "RIGHT": {"document_ref": "R"}},
    )
    assert payload["sides"]["LEFT"][0]["block_id"] == "blk_l"
    assert payload["sides"]["LEFT"][0]["node_id"] == "N1"
