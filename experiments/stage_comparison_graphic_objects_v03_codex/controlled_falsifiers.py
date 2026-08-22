"""Controlled mechanism checks, reported separately from the real corpus."""
from __future__ import annotations

from typing import Any

from .comparator import compare_graphic_scopes


def _object(index: int, *, x: float, y: float, signature: str | None = None, family: str | None = None, style: dict[str, Any] | None = None, labels: list[str] | None = None, segments: int = 4) -> dict[str, Any]:
    return {
        "object_id": f"obj_{index:05d}", "type": "SYMBOL_OBJECT",
        "bbox_norm": [x, y, x + .01, y + .01], "center_isotropic": [x, y], "size_isotropic": [.01, .01],
        "geometry_signature": signature or f"sig-{index}", "family_signature": family or f"family-{index}",
        "geometry": {"segment_count": segments, "length_isotropic": .04, "nodes": 4, "endpoints": 0, "branch_points": 0, "degree_histogram": {2: 4}, "angle_histogram_15deg": {0: 2, 6: 2}},
        "style": style or {"stroke": [0, 0, 0], "fill": None, "stroke_width": .5, "dashes": "[] 0", "stroke_opacity": 1, "fill_opacity": 1, "line_cap": [0], "line_join": 0},
        "label_anchor_ids": labels or [], "relation_ids": [], "formation": {"atom_count": 1, "rules": ["controlled"], "cap_sampled": False}, "provenance": [{"source": "controlled_falsifier"}],
    }


def _description(objects: list[dict[str, Any]], *, anchors: list[dict[str, Any]] | None = None, relations: list[dict[str, Any]] | None = None, status: str = "OBJECT_FORMATION_COMPLETE", block_type: str = "image") -> dict[str, Any]:
    return {
        "schema_version": "graphic-block-description-v0.3-codex", "research_only": True,
        "input": {"block_id": "controlled", "block_type": block_type, "prepared_text_metadata": anchors or []},
        "quality": {"status": status, "extraction_reliable": status != "VECTOR_DATA_INSUFFICIENT", "bbox_reliable": True, "object_formation_complete": status == "OBJECT_FORMATION_COMPLETE", "dangerous_cap": False},
        "objects": objects, "object_families": [], "relations": relations or [], "visible_geometry_summary": {}, "uncertainties": [],
    }


def _run(case_id: str, left: dict[str, Any], right: dict[str, Any], expected: list[str]) -> dict[str, Any]:
    ledger = compare_graphic_scopes(case_id, [left], [right])
    detected = sorted({row["status"] for row in ledger["object_statuses"] if row["status"] != "UNCHANGED"})
    return {"case_id": case_id, "expected_statuses": sorted(expected), "detected_statuses": detected, "passed": detected == sorted(expected), "route": ledger["decision"]["route"], "change_count": len(ledger["changes"])}


def run_controlled_falsifiers() -> dict[str, Any]:
    cases = []
    # 50,000 unrelated line primitives summarized into 1,000 addressable
    # objects; a single local object disappears.  No global similarity gate.
    left_objects = [_object(i, x=(i % 40) / 50, y=(i // 40) / 30, segments=50) for i in range(1000)]
    right_objects = [dict(row) for row in left_objects if row["object_id"] != "obj_00999"]
    cases.append(_run("local_removal_among_50000_lines", _description(left_objects), _description(right_objects), ["REMOVED"]))

    # Same rectangle emitted as two packaging signatures but identical generic
    # topology/geometry statistics.
    cases.append(_run("same_rectangle_repacked", _description([_object(1, x=.2, y=.2, signature="rect-path", family="rectangle")]), _description([_object(1, x=.2, y=.2, signature="four-lines", family="rectangle")]), []))

    # Two similar objects exchange locations; three static unique anchors keep
    # the block alignment fixed.
    static = [_object(i, x=.05 * i, y=.05, signature=f"static-{i}") for i in range(1, 4)]
    left_swap = static + [_object(4, x=.2, y=.4, signature="A", family="same"), _object(5, x=.7, y=.4, signature="B", family="same")]
    right_swap = static + [_object(4, x=.7, y=.4, signature="A", family="same"), _object(5, x=.2, y=.4, signature="B", family="same")]
    cases.append(_run("similar_objects_swapped", _description(left_swap), _description(right_swap), ["POSITION_CHANGED"]))

    cases.append(_run("unlabelled_object_removed", _description([_object(1, x=.1, y=.1), _object(2, x=.7, y=.7)]), _description([_object(1, x=.1, y=.1)]), ["REMOVED"]))

    anchors = [{"anchor_id": "qf1-left", "text": "QF1", "bbox_norm": [.1, .1, .2, .2]}]
    anchors_right = [{"anchor_id": "qf1-right", "text": "QF1", "bbox_norm": [.1, .1, .2, .2]}]
    cases.append(_run("good_label_anchor_repacked", _description([_object(1, x=.1, y=.1, signature="old", family="old", labels=["qf1-left"])], anchors=anchors), _description([_object(1, x=.1, y=.1, signature="new", family="new", labels=["qf1-right"])], anchors=anchors_right), []))

    changed_anchor = [{"anchor_id": "right-label", "text": "QF2", "bbox_norm": [.1, .1, .2, .2]}]
    cases.append(_run("text_changed_graphics_same", _description([_object(1, x=.1, y=.1, labels=["qf1-left"])], anchors=anchors), _description([_object(1, x=.1, y=.1, labels=["right-label"])], anchors=changed_anchor), []))

    table = _description([], status="GRAPHIC_NOT_APPLICABLE", block_type="table")
    cases.append(_run("table_content_changed", table, table, []))

    dashed = {**_object(1, x=.1, y=.1)["style"], "dashes": "[3 2] 0"}
    cases.append(_run("matched_object_style_changed", _description([_object(1, x=.1, y=.1)]), _description([_object(1, x=.1, y=.1, style=dashed)]), ["STYLE_CHANGED"]))

    objects = [_object(1, x=.1, y=.1), _object(2, x=.2, y=.2)]
    relation = [{"relation_id": "r1", "type": "CONNECTED_TO", "source_object": "obj_00001", "target_object": "obj_00002", "confidence": 1, "provenance": [{"source": "controlled"}]}]
    cases.append(_run("connection_removed", _description(objects, relations=relation), _description(objects), ["CONNECTION_CHANGED"]))
    return {"schema_version": "controlled-graphic-falsifiers-v0.3-codex", "not_real_benchmark": True, "cases": cases, "passed": sum(row["passed"] for row in cases), "total": len(cases)}


__all__ = ["run_controlled_falsifiers"]
