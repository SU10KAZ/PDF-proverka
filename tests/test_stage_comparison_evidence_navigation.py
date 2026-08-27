from __future__ import annotations

from backend.app.services.stage_comparison.evidence_navigation import (
    build_evidence_navigation,
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
