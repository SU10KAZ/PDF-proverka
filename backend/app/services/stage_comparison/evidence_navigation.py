"""Trace a unified atomic change to exact LEFT/RIGHT viewer highlights."""
from __future__ import annotations

from typing import Any, Mapping

from .production_artifacts import content_signature


KIND = "stage_comparison_evidence_navigation"
SCHEMA_VERSION = "evidence-navigation.v1"


def _target(synthesis: Mapping[str, Any], target_id: str) -> Mapping[str, Any]:
    for collection, key in (("changes", "change_id"), ("review_items", "review_evidence_id")):
        for value in synthesis.get(collection) or []:
            if isinstance(value, Mapping) and value.get(key) == target_id:
                return value
    raise KeyError("unified change/review evidence not found")


def _document_ref(documents: Mapping[str, Any] | None, side: str) -> Any:
    value = (documents or {}).get(side)
    if isinstance(value, Mapping):
        return value.get("document_ref", value.get("document_code", value.get("pdf_path")))
    return value


def _text_locations(
    atom: Mapping[str, Any],
    documents: Mapping[str, Any] | None,
) -> dict[str, list[dict[str, Any]]]:
    provenance = atom.get("provenance")
    locations = provenance.get("locations") if isinstance(provenance, Mapping) else None
    output = {"LEFT": [], "RIGHT": []}
    for side in output:
        values = locations.get(side) if isinstance(locations, Mapping) else []
        for location in values or []:
            if not isinstance(location, Mapping):
                continue
            bboxes = list(location.get("bboxes") or [])
            output[side].append({
                "source": "TEXT",
                "document_ref": _document_ref(documents, side),
                "page": location.get("page"),
                "fragment_id": location.get("fragment_id"),
                "block_id": None,
                "node_id": None,
                "highlight": {"kind": "BBOX_SET", "bboxes": bboxes} if bboxes else None,
                "coordinate_space": "NORMALIZED_PAGE_TOP_LEFT" if bboxes else None,
                "page_size": None,
                "coordinates_available": bool(bboxes),
            })
    return output


def _graphic_locations(
    change: Mapping[str, Any] | None,
    documents: Mapping[str, Any] | None,
    page_sizes: Mapping[str, Any] | None,
) -> dict[str, list[dict[str, Any]]]:
    output = {"LEFT": [], "RIGHT": []}
    if not change:
        return output
    structural = change.get("structural")
    nodes_by_side = {
        "LEFT": list(structural.get("left_nodes") or []) if isinstance(structural, Mapping) else [],
        "RIGHT": list(structural.get("right_nodes") or []) if isinstance(structural, Mapping) else [],
    }
    for side, region_key in (("LEFT", "left_region"), ("RIGHT", "right_region")):
        region = change.get(region_key)
        if not isinstance(region, Mapping):
            # Honest absence is still traceable to the change/evidence, but no
            # page coordinate is invented.
            output[side].append({
                "source": "GRAPHIC",
                "document_ref": _document_ref(documents, side),
                "page": None,
                "fragment_id": None,
                "block_id": None,
                "node_id": nodes_by_side[side][0] if len(nodes_by_side[side]) == 1 else None,
                "highlight": None,
                "coordinate_space": None,
                "page_size": None,
                "coordinates_available": False,
            })
            continue
        bbox = region.get("bbox_visual_pt")
        polygon = region.get("polygon")
        page_index = region.get("page_index")
        page = int(page_index) + 1 if isinstance(page_index, int) else None
        size = (
            (page_sizes or {}).get(side, {}).get(page)
            if page is not None and isinstance((page_sizes or {}).get(side), Mapping)
            else None
        )
        width = size.get("width") if isinstance(size, Mapping) else None
        height = size.get("height") if isinstance(size, Mapping) else None
        if isinstance(polygon, list) and polygon:
            highlight = {"kind": "POLYGON", "polygon": polygon}
        elif isinstance(bbox, list) and len(bbox) == 4:
            highlight = {"kind": "BBOX", "bbox": bbox}
        else:
            highlight = None
        normalized_highlight = None
        if (
            highlight is not None
            and isinstance(width, (int, float)) and float(width) > 0
            and isinstance(height, (int, float)) and float(height) > 0
        ):
            if highlight["kind"] == "BBOX":
                x0, y0, x1, y1 = (float(value) for value in highlight["bbox"])
                normalized_highlight = {
                    "kind": "BBOX",
                    "bbox": [x0 / float(width), y0 / float(height), x1 / float(width), y1 / float(height)],
                }
            else:
                normalized_highlight = {
                    "kind": "POLYGON",
                    "polygon": [
                        [float(point[0]) / float(width), float(point[1]) / float(height)]
                        for point in highlight["polygon"]
                    ],
                }
        output[side].append({
            "source": "GRAPHIC",
            "document_ref": _document_ref(documents, side),
            "page": page,
            "fragment_id": None,
            "block_id": region.get("block_id"),
            "node_id": nodes_by_side[side][0] if len(nodes_by_side[side]) == 1 else None,
            "highlight": normalized_highlight or highlight,
            "coordinate_space": (
                "NORMALIZED_PAGE_TOP_LEFT"
                if normalized_highlight is not None
                else "PDF_VISUAL_PT" if highlight is not None else None
            ),
            "page_size": (
                {"width": float(width), "height": float(height)}
                if isinstance(width, (int, float)) and isinstance(height, (int, float))
                else None
            ),
            "coordinates_available": highlight is not None,
        })
    return output


def _normalized_bbox(bbox: Any, width: Any, height: Any) -> dict[str, Any] | None:
    """Приводит рамку из визуальных пунктов к долям страницы."""
    if not (isinstance(bbox, (list, tuple)) and len(bbox) == 4):
        return None
    if not (isinstance(width, (int, float)) and float(width) > 0):
        return None
    if not (isinstance(height, (int, float)) and float(height) > 0):
        return None
    x0, y0, x1, y1 = (float(value) for value in bbox)
    return {
        "kind": "BBOX",
        "bbox": [
            x0 / float(width), y0 / float(height),
            x1 / float(width), y1 / float(height),
        ],
    }


def _load_table_locations(
    change: Mapping[str, Any] | None,
    documents: Mapping[str, Any] | None,
    page_sizes: Mapping[str, Any] | None,
) -> dict[str, list[dict[str, Any]]]:
    """Места строк таблицы нагрузок на обоих листах.

    У изменения из таблицы нет узла графа: доказательство — это сама строка
    подписи с её рамкой. Без этой ветки кнопка «Открыть доказательство» на
    таких находках вела бы в пустоту, а находка выглядела бы бездоказательной.
    """
    output: dict[str, list[dict[str, Any]]] = {"LEFT": [], "RIGHT": []}
    evidence = (change or {}).get("evidence")
    if not isinstance(evidence, Mapping):
        return output
    for side in output:
        record = evidence.get(side)
        if not isinstance(record, Mapping):
            continue
        page_index = record.get("page_index")
        page = int(page_index) + 1 if isinstance(page_index, int) else None
        size = (
            (page_sizes or {}).get(side, {}).get(page)
            if page is not None and isinstance((page_sizes or {}).get(side), Mapping)
            else None
        )
        bbox = record.get("bbox")
        normalized = _normalized_bbox(
            bbox,
            size.get("width") if isinstance(size, Mapping) else None,
            size.get("height") if isinstance(size, Mapping) else None,
        )
        highlight = normalized or (
            {"kind": "BBOX", "bbox": [float(value) for value in bbox]}
            if isinstance(bbox, (list, tuple)) and len(bbox) == 4
            else None
        )
        output[side].append({
            "source": "GRAPHIC",
            "document_ref": _document_ref(documents, side),
            "page": page,
            "fragment_id": record.get("row_id"),
            "block_id": None,
            "node_id": None,
            "highlight": highlight,
            "coordinate_space": (
                "NORMALIZED_PAGE_TOP_LEFT"
                if normalized is not None
                else "PDF_VISUAL_PT" if highlight is not None else None
            ),
            "page_size": (
                {"width": float(size["width"]), "height": float(size["height"])}
                if isinstance(size, Mapping)
                else None
            ),
            "coordinates_available": highlight is not None,
        })
    return output


def _load_table_change_index(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        return {}
    return {
        str(change.get("change_id")): change
        for change in payload.get("changes") or []
        if isinstance(change, Mapping) and change.get("change_id")
    }


def _graphic_change_index(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        return {}
    if payload.get("kind") not in {
        "stage_comparison_page_graphic_bundle",
        "stage_comparison_document_graphic_bundle",
    }:
        return {
            str(change.get("change_id")): change
            for change in payload.get("changes") or []
            if isinstance(change, Mapping)
        }
    output = {}
    for group in payload.get("groups") or []:
        if not isinstance(group, Mapping):
            continue
        ledger = group.get("ledger")
        if not isinstance(ledger, Mapping):
            continue
        changes = {
            str(change.get("change_id") or ""): change
            for change in ledger.get("changes") or []
            if isinstance(change, Mapping) and change.get("change_id")
        }
        for ref in group.get("change_refs") or []:
            if not isinstance(ref, Mapping):
                continue
            evidence_ref = str(ref.get("evidence_ref") or "")
            source_change_id = str(ref.get("source_change_id") or "")
            if evidence_ref and source_change_id in changes:
                output[evidence_ref] = changes[source_change_id]
    return output


def _resolve_target_evidence(
    target_id: str,
    target: Mapping[str, Any],
    *,
    text_by_atom: Mapping[str, Mapping[str, Any]],
    graphic_by_evidence: Mapping[str, Mapping[str, Any]],
    table_by_evidence: Mapping[str, Mapping[str, Any]],
    documents: Mapping[str, Any] | None,
    page_sizes: Mapping[str, Any] | None,
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    """Resolve one target exactly as the public evidence endpoint does."""
    sides: dict[str, list[dict[str, Any]]] = {"LEFT": [], "RIGHT": []}
    trace = []
    for evidence in target.get("evidence_refs") or []:
        if not isinstance(evidence, Mapping):
            continue
        source = evidence.get("source")
        atom_id = str(evidence.get("atom_id") or "")
        evidence_ref = str(evidence.get("evidence_ref") or "")
        if source == "TEXT":
            located = _text_locations(text_by_atom.get(atom_id, {}), documents)
        elif source == "GRAPHIC":
            if evidence_ref in table_by_evidence:
                located = _load_table_locations(
                    table_by_evidence[evidence_ref], documents, page_sizes,
                )
            else:
                located = _graphic_locations(
                    graphic_by_evidence.get(evidence_ref), documents, page_sizes,
                )
        else:
            continue
        for side in sides:
            sides[side].extend(located[side])
        trace.append({
            "target_id": target_id,
            "evidence_ref": evidence_ref,
            "atom_id": atom_id,
            "source": source,
            "source_artifact": evidence.get("source_artifact"),
            "locations": located,
        })
    for side in sides:
        sides[side].sort(key=lambda item: (
            item["page"] if isinstance(item.get("page"), int) else 10**9,
            item["source"],
            str(item.get("block_id") or item.get("fragment_id") or ""),
        ))
    return sides, trace


def evidence_is_available(sides: Mapping[str, Any]) -> bool:
    """Whether the canonical resolver found something the viewer can open."""
    return any(bool(sides.get(side)) for side in ("LEFT", "RIGHT"))


def build_evidence_availability_index(
    *,
    synthesis: Mapping[str, Any],
    text_atoms: Mapping[str, Any] | None = None,
    graphic_ledger: Mapping[str, Any] | None = None,
    electrical_table_changes: Mapping[str, Any] | None = None,
    documents: Mapping[str, Any] | None = None,
) -> dict[str, bool]:
    """Canonical per-target availability for read-only report projections.

    The report must not guess from an inline provenance shape: the evidence
    endpoint resolves text, graphic-ledger and load-table references through
    their respective indexes.  This projection deliberately uses that same
    resolver and carries only a boolean; page geometry remains owned by the
    evidence endpoint.
    """
    text_by_atom = {
        str(atom.get("atom_id")): atom
        for atom in (text_atoms or {}).get("atoms") or []
        if isinstance(atom, Mapping)
    }
    graphic_by_evidence = _graphic_change_index(graphic_ledger)
    table_by_evidence = _load_table_change_index(electrical_table_changes)
    availability: dict[str, bool] = {}
    for collection, key in (
        ("changes", "change_id"),
        ("review_items", "review_evidence_id"),
    ):
        for target in synthesis.get(collection) or []:
            if not isinstance(target, Mapping):
                continue
            target_id = str(target.get(key) or "")
            if not target_id:
                continue
            sides, _trace = _resolve_target_evidence(
                target_id,
                target,
                text_by_atom=text_by_atom,
                graphic_by_evidence=graphic_by_evidence,
                table_by_evidence=table_by_evidence,
                documents=documents,
                page_sizes=None,
            )
            availability[target_id] = evidence_is_available(sides)
    return availability


def build_evidence_navigation(
    target_id: str,
    *,
    synthesis: Mapping[str, Any],
    text_atoms: Mapping[str, Any] | None = None,
    graphic_ledger: Mapping[str, Any] | None = None,
    electrical_table_changes: Mapping[str, Any] | None = None,
    documents: Mapping[str, Any] | None = None,
    page_sizes: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    target = _target(synthesis, target_id)
    text_by_atom = {
        str(atom.get("atom_id")): atom
        for atom in (text_atoms or {}).get("atoms") or []
        if isinstance(atom, Mapping)
    }
    graphic_by_evidence = _graphic_change_index(graphic_ledger)
    table_by_evidence = _load_table_change_index(electrical_table_changes)
    sides, trace = _resolve_target_evidence(
        target_id,
        target,
        text_by_atom=text_by_atom,
        graphic_by_evidence=graphic_by_evidence,
        table_by_evidence=table_by_evidence,
        documents=documents,
        page_sizes=page_sizes,
    )
    has_both_sides = bool(sides["LEFT"] and sides["RIGHT"])
    return {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "version": 1,
        "target_id": target_id,
        "source_mode": target.get("source_mode", target.get("source")),
        "direction": "LEFT_TO_RIGHT",
        "layout": "SIDE_BY_SIDE" if has_both_sides else "SINGLE_SIDE",
        "has_evidence": evidence_is_available(sides),
        "sides": sides,
        "trace": trace,
        "input_signature": content_signature({
            "target": target,
            "trace": trace,
            "documents": documents,
            "page_sizes": page_sizes,
        }),
        "viewer_action": {
            "open_exact_pages": True,
            "zoom_to_highlights": any(
                location["coordinates_available"]
                for values in sides.values() for location in values
            ),
        },
    }


__all__ = [
    "KIND",
    "SCHEMA_VERSION",
    "build_evidence_availability_index",
    "build_evidence_navigation",
    "evidence_is_available",
]
