"""Production orchestration for direct LEFT-to-RIGHT page comparison.

The caller selects both pages.  This service does not infer design stages,
chronology, sheet pairing, or parent documents.  It only resolves one prepared
GRAPHIC block per selected page and orchestrates the existing deterministic
MODE 2 pipeline.
"""
from __future__ import annotations

import copy
import math
from pathlib import Path
from typing import Any

from backend.app.pipeline.stages.block_grounding.document_consistency import (
    detect_document_consistency,
)
from backend.app.pipeline.stages.block_grounding.electrical_load_table import (
    build_load_table,
)
from backend.app.pipeline.stages.block_grounding.electrical_table_diff import (
    compare_load_tables,
)
from backend.app.pipeline.stages.block_grounding.dense_sectioned_board import (
    build_dense_sectioned_board_graph,
    detect_dense_sectioned_board,
    evaluate_dense_sectioned_board_gate,
)
from backend.app.pipeline.stages.block_grounding.system_graph import (
    validate_system_graph,
)
from backend.app.pipeline.stages.block_grounding.system_graph_comparator import (
    compare_system_graphs,
    validate_comparison_result,
)
from backend.app.pipeline.stages.block_grounding.vector_evidence import (
    extract_vector_evidence,
)
from backend.app.services.common.blocks_json import load_blocks_json
from backend.app.services.stage_comparison.unified_entity_bridge.document_binding import (
    document_descriptor_for_block,
    document_identity_is_complete,
    normalize_document_descriptor,
)

from .contract import validate_ledger
from .graphic_change_ledger_adapter import (
    adapt_system_graph_comparison_to_ledger,
)


SCHEMA_VERSION = "direct-page-system-graph-comparison.v1"
SELECTION_KIND = "PAGE"
SIDES = ("LEFT", "RIGHT")
GRAPHIC_BLOCK_TYPES = frozenset({"image", "graphic"})


class DirectPageComparisonError(ValueError):
    """A selected source or a fail-closed MODE 2 gate is invalid."""


def _page_index(value: Any, where: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise DirectPageComparisonError(f"{where}: non-negative integer required")
    return value


def _locator(value: Any, where: str) -> Path:
    if not isinstance(value, (str, Path)) or not str(value).strip():
        raise DirectPageComparisonError(f"{where}: path required")
    path = Path(value)
    if not path.is_file():
        raise DirectPageComparisonError(f"{where}: file not found: {path}")
    return path


def _validate_bbox(value: Any, where: str) -> list[float]:
    if (
        not isinstance(value, list)
        or len(value) != 4
        or any(
            not isinstance(item, (int, float))
            or isinstance(item, bool)
            or not math.isfinite(float(item))
            or not 0.0 <= float(item) <= 1.0
            for item in value
        )
        or float(value[2]) <= float(value[0])
        or float(value[3]) <= float(value[1])
    ):
        raise DirectPageComparisonError(f"{where}: finite normalized bbox required")
    return [float(item) for item in value]


def _validate_polygon(value: Any, where: str) -> list[list[float]] | None:
    if value is None:
        return None
    if not isinstance(value, list) or len(value) < 3:
        raise DirectPageComparisonError(f"{where}: at least three normalized points required")
    output = []
    for index, point in enumerate(value):
        if (
            not isinstance(point, list)
            or len(point) < 2
            or any(
                not isinstance(item, (int, float))
                or isinstance(item, bool)
                or not math.isfinite(float(item))
                or not 0.0 <= float(item) <= 1.0
                for item in point[:2]
            )
        ):
            raise DirectPageComparisonError(f"{where}[{index}]: invalid point")
        output.append([float(point[0]), float(point[1])])
    return output


def resolve_selected_page_source(value: Any, side: str) -> dict[str, Any]:
    """Resolve one selected PAGE to exactly one prepared GRAPHIC block."""
    if side not in SIDES:
        raise DirectPageComparisonError(f"side: one of {SIDES} required")
    if not isinstance(value, dict):
        raise DirectPageComparisonError(f"{side}: source object required")
    allowed = {
        "document",
        "pdf_path",
        "blocks_path",
        "page_index_0based",
        "block_id",
    }
    required = {"document", "pdf_path", "blocks_path", "page_index_0based"}
    if set(value) - allowed or not required <= set(value):
        raise DirectPageComparisonError(f"{side}: invalid source fields")

    descriptor = normalize_document_descriptor(value["document"], f"{side}.document")
    if not document_identity_is_complete(descriptor):
        raise DirectPageComparisonError(
            f"{side}.document: document and version identity required"
        )
    pdf_path = _locator(value["pdf_path"], f"{side}.pdf_path")
    blocks_path = _locator(value["blocks_path"], f"{side}.blocks_path")
    if pdf_path.resolve().parent != blocks_path.resolve().parent:
        raise DirectPageComparisonError(
            f"{side}: PDF and blocks.json must belong to one prepared version directory"
        )
    page_index = _page_index(
        value["page_index_0based"], f"{side}.page_index_0based"
    )
    payload = load_blocks_json(blocks_path)
    if payload is None:
        raise DirectPageComparisonError(f"{side}.blocks_path: invalid blocks.json")
    if payload.get("schema_version") != 1:
        raise DirectPageComparisonError(f"{side}.blocks_path: schema_version 1 required")
    if payload.get("coordinate_space") != "normalized_page_top_left":
        raise DirectPageComparisonError(
            f"{side}.blocks_path: normalized_page_top_left required"
        )
    page_records = [
        item
        for item in payload["pages"]
        if isinstance(item, dict) and item.get("page_index") == page_index
    ]
    if len(page_records) != 1:
        raise DirectPageComparisonError(f"{side}: selected page metadata not unique")

    page_graphics = [
        item
        for item in payload["blocks"]
        if isinstance(item, dict)
        and item.get("page_index") == page_index
        and str(item.get("block_type") or "").casefold() in GRAPHIC_BLOCK_TYPES
    ]
    requested_block_id = value.get("block_id")
    if requested_block_id is None:
        if len(page_graphics) != 1:
            raise DirectPageComparisonError(
                f"{side}: selected page has {len(page_graphics)} GRAPHIC blocks; block_id required"
            )
        record = page_graphics[0]
    else:
        if not isinstance(requested_block_id, str) or not requested_block_id.strip():
            raise DirectPageComparisonError(f"{side}.block_id: non-empty string required")
        matches = [
            item
            for item in payload["blocks"]
            if isinstance(item, dict)
            and str(item.get("block_id") or item.get("id") or "") == requested_block_id
        ]
        if len(matches) != 1:
            raise DirectPageComparisonError(f"{side}: block_id must resolve exactly once")
        record = matches[0]
        if record.get("page_index") != page_index:
            raise DirectPageComparisonError(f"{side}: block is not on selected page")
        if str(record.get("block_type") or "").casefold() not in GRAPHIC_BLOCK_TYPES:
            raise DirectPageComparisonError(f"{side}: selected block is not GRAPHIC")

    block_id = str(record.get("block_id") or record.get("id") or "")
    if not block_id:
        raise DirectPageComparisonError(f"{side}: selected block has no block_id")
    document_descriptor_for_block(
        payload,
        block_id,
        document_code=descriptor["document_code"],
        version_id=descriptor["version_id"],
        storage_identity=descriptor["storage_identity"],
        source_path=descriptor["source_path"],
        provenance=descriptor["provenance"],
    )
    bbox = _validate_bbox(record.get("coords_norm"), f"{side}.block.coords_norm")
    polygon = _validate_polygon(
        record.get("polygon_points"), f"{side}.block.polygon_points"
    )
    reference = {
        "selection_kind": SELECTION_KIND,
        "document": descriptor,
        "page_index_0based": page_index,
        "block_id": block_id,
        "locators": {
            "pdf_path": str(pdf_path),
            "blocks_path": str(blocks_path),
        },
    }
    return {
        "reference": reference,
        "pdf_path": pdf_path,
        "blocks_path": blocks_path,
        "blocks_payload": payload,
        "page_metadata": page_records[0],
        "record": {**record, "coords_norm": bbox, "polygon_points": polygon},
    }


def _evidence_diagnostics(evidence: Any) -> dict[str, Any]:
    return {
        "extraction_ok": evidence.extraction_ok,
        "reasons": list(evidence.reasons),
        "page_index": evidence.page_index,
        "coordinate_system": evidence.coordinate_system,
        "page_size": list(evidence.page_size),
        "block_bbox_visual_pt": list(evidence.block_bbox),
        "counts": {
            "visual_words": len(evidence.visual_words),
            "paths": len(evidence.paths),
            "lines": len(evidence.lines),
            "curves": len(evidence.curves),
            "polygons": len(evidence.polygons),
        },
        "extraction_gate": copy.deepcopy(evidence.extraction_gate),
        "provenance": copy.deepcopy(evidence.provenance),
    }


def _build_side(side: str, source: dict[str, Any]) -> dict[str, Any]:
    record = source["record"]
    evidence = extract_vector_evidence(
        source["pdf_path"],
        page_index=source["reference"]["page_index_0based"],
        block_id=source["reference"]["block_id"],
        bbox_norm=record["coords_norm"],
        polygon_norm=record.get("polygon_points"),
    )
    evidence_diagnostics = _evidence_diagnostics(evidence)
    if not evidence.extraction_ok:
        raise DirectPageComparisonError(
            f"{side}: vector extraction failed: {evidence.reasons}"
        )
    detection = detect_dense_sectioned_board(evidence)
    if detection.get("detected") is not True:
        raise DirectPageComparisonError(
            f"{side}: dense detector failed: {detection.get('reasons') or detection}"
        )
    graph = build_dense_sectioned_board_graph(evidence, detection=detection)
    gate = evaluate_dense_sectioned_board_gate(graph)
    if gate.get("use") is not True:
        raise DirectPageComparisonError(
            f"{side}: dense graph gate failed: {gate.get('reasons') or gate}"
        )
    graph.setdefault("provenance", {})["selected_source"] = copy.deepcopy(
        source["reference"]
    )
    validation = validate_system_graph(graph)
    if validation.get("valid") is not True:
        raise DirectPageComparisonError(
            f"{side}: system graph invalid: {validation.get('errors')}"
        )
    # Таблица нагрузок читается из ТОЙ ЖЕ VectorEvidence, что и граф щита:
    # поворот страницы в ней применён ровно один раз, координаты колонок и
    # аппаратов лежат в одной системе, и расхождению взяться неоткуда.
    load_table = build_load_table(evidence, side=side)
    # Внутренняя согласованность листа считается по ТОЙ ЖЕ VectorEvidence и по
    # тому же графу: сравнения редакций здесь нет вовсе, и «было → стало» эти
    # находки породить не могут — у них одна сторона.
    consistency = detect_document_consistency(
        load_table=load_table, evidence=evidence, graph=graph, side=side
    )
    return {
        "graph": graph,
        "load_table": load_table,
        "consistency": consistency,
        "diagnostics": {
            "vector_evidence": evidence_diagnostics,
            "dense_detector": detection,
            "dense_gate": gate,
            "graph_validation": validation,
            "load_table": load_table["counts"] | {
                "contradictions": len(load_table["contradictions"]),
            },
            "document_consistency": consistency["counts"]
            | {"diagnostics": consistency["diagnostics"]},
        },
    }


def compare_selected_pages(left_source: Any, right_source: Any) -> dict[str, Any]:
    """Compare two user-selected pages in the literal LEFT-to-RIGHT direction."""
    sources = {
        "LEFT": resolve_selected_page_source(left_source, "LEFT"),
        "RIGHT": resolve_selected_page_source(right_source, "RIGHT"),
    }
    built = {side: _build_side(side, sources[side]) for side in SIDES}
    comparison = compare_system_graphs(
        built["LEFT"]["graph"], built["RIGHT"]["graph"]
    )
    for side, key in (("LEFT", "left_graph"), ("RIGHT", "right_graph")):
        comparison[key]["source_reference"] = copy.deepcopy(
            sources[side]["reference"]
        )
    comparison["provenance"]["stage_comparison_integration"] = True
    comparison["provenance"]["selected_sources"] = {
        side: copy.deepcopy(sources[side]["reference"]) for side in SIDES
    }
    comparison_validation = validate_comparison_result(comparison)
    if comparison_validation.get("valid") is not True:
        raise DirectPageComparisonError(
            f"comparison invalid: {comparison_validation.get('errors')}"
        )
    comparison["validation"] = comparison_validation

    # Таблицы нагрузок сравниваются отдельно от графа: значения мощностей и
    # токов подписаны у колонок листа, а не у аппаратов, и узел графа для них
    # существует не всегда — у АУКРМ левого листа его нет вовсе.
    load_table_diff = compare_load_tables(
        built["LEFT"]["load_table"], built["RIGHT"]["load_table"]
    )
    comparison["document_inconsistencies"] = (
        list(comparison.get("document_inconsistencies") or [])
        + [
            dict(item)
            for side in SIDES
            for item in built[side]["load_table"]["contradictions"]
        ]
        + [
            dict(item)
            for side in SIDES
            for item in built[side]["consistency"]["items"]
        ]
    )

    ledger = adapt_system_graph_comparison_to_ledger(
        comparison, built["LEFT"]["graph"], built["RIGHT"]["graph"]
    )
    for side, key in (("LEFT", "left_blocks"), ("RIGHT", "right_blocks")):
        ledger["comparison_scope"][key][0]["source"]["selected_source"] = copy.deepcopy(
            sources[side]["reference"]
        )
    ledger["diagnostics"]["direct_page_comparison"] = {
        "direction": "LEFT_TO_RIGHT",
        "parent_relation_required": False,
    }
    # Внутренние противоречия листа едут в диагностику, а не в changes:
    # у них нет второй стороны, и в перечне изменений они подделали бы
    # «было → стало».
    ledger["diagnostics"]["document_inconsistencies"] = copy.deepcopy(
        comparison.get("document_inconsistencies") or []
    )
    ledger["diagnostics"]["electrical_table_diff"] = copy.deepcopy(load_table_diff)
    ledger = validate_ledger(ledger)

    result = {
        "schema_version": SCHEMA_VERSION,
        "mode": "MODE_2",
        "direction": "LEFT_TO_RIGHT",
        "sources": {
            side: copy.deepcopy(sources[side]["reference"]) for side in SIDES
        },
        "left_graph": built["LEFT"]["graph"],
        "right_graph": built["RIGHT"]["graph"],
        "comparison_result": comparison,
        "graphic_change_ledger": ledger,
        "diagnostics": {
            side: copy.deepcopy(built[side]["diagnostics"]) for side in SIDES
        }
        | {
            "electrical_table_diff": copy.deepcopy(load_table_diff),
            "electrical_load_tables": {
                side: copy.deepcopy(built[side]["load_table"]) for side in SIDES
            },
            "document_consistency": {
                side: copy.deepcopy(built[side]["consistency"]) for side in SIDES
            },
        },
    }
    return validate_direct_page_comparison_result(result)


def _validate_source_reference(value: Any, where: str) -> dict[str, Any]:
    expected = {
        "selection_kind",
        "document",
        "page_index_0based",
        "block_id",
        "locators",
    }
    if not isinstance(value, dict) or set(value) != expected:
        raise DirectPageComparisonError(f"{where}: invalid source reference")
    if value["selection_kind"] != SELECTION_KIND:
        raise DirectPageComparisonError(f"{where}.selection_kind: PAGE required")
    descriptor = normalize_document_descriptor(value["document"], f"{where}.document")
    if not document_identity_is_complete(descriptor):
        raise DirectPageComparisonError(f"{where}.document: incomplete identity")
    _page_index(value["page_index_0based"], f"{where}.page_index_0based")
    if not isinstance(value["block_id"], str) or not value["block_id"].strip():
        raise DirectPageComparisonError(f"{where}.block_id: non-empty string required")
    locators = value["locators"]
    if not isinstance(locators, dict) or set(locators) != {"pdf_path", "blocks_path"}:
        raise DirectPageComparisonError(f"{where}.locators: invalid")
    if any(not isinstance(item, str) or not item for item in locators.values()):
        raise DirectPageComparisonError(f"{where}.locators: non-empty strings required")
    return value


def validate_direct_page_comparison_result(payload: Any) -> dict[str, Any]:
    """Validate the service envelope and all cross-artifact source links."""
    expected = {
        "schema_version",
        "mode",
        "direction",
        "sources",
        "left_graph",
        "right_graph",
        "comparison_result",
        "graphic_change_ledger",
        "diagnostics",
    }
    if not isinstance(payload, dict) or set(payload) != expected:
        raise DirectPageComparisonError("direct page comparison: invalid envelope")
    if payload["schema_version"] != SCHEMA_VERSION:
        raise DirectPageComparisonError("direct page comparison: unsupported version")
    if payload["mode"] != "MODE_2" or payload["direction"] != "LEFT_TO_RIGHT":
        raise DirectPageComparisonError("direct page comparison: invalid mode/direction")
    sources = payload["sources"]
    if not isinstance(sources, dict) or set(sources) != set(SIDES):
        raise DirectPageComparisonError("direct page comparison.sources: LEFT/RIGHT required")
    for side in SIDES:
        _validate_source_reference(sources[side], f"sources.{side}")
        graph = payload[f"{side.lower()}_graph"]
        validation = validate_system_graph(graph)
        if validation.get("valid") is not True:
            raise DirectPageComparisonError(f"{side}: invalid stored system graph")
        if (graph.get("provenance") or {}).get("selected_source") != sources[side]:
            raise DirectPageComparisonError(f"{side}: graph source reference mismatch")

    comparison = payload["comparison_result"]
    if validate_comparison_result(comparison).get("valid") is not True:
        raise DirectPageComparisonError("direct page comparison: invalid comparison result")
    ledger = validate_ledger(payload["graphic_change_ledger"])
    for side, graph_key, ledger_key in (
        ("LEFT", "left_graph", "left_blocks"),
        ("RIGHT", "right_graph", "right_blocks"),
    ):
        if (comparison.get(graph_key) or {}).get("source_reference") != sources[side]:
            raise DirectPageComparisonError(f"{side}: comparison source reference mismatch")
        blocks = (ledger.get("comparison_scope") or {}).get(ledger_key) or []
        if (
            len(blocks) != 1
            or (blocks[0].get("source") or {}).get("selected_source") != sources[side]
        ):
            raise DirectPageComparisonError(f"{side}: ledger source reference mismatch")
    if not isinstance(payload["diagnostics"], dict):
        raise DirectPageComparisonError("direct page comparison.diagnostics: object required")
    return payload


def schema_path() -> Path:
    """Return the JSON Schema for the direct PAGE comparison envelope."""
    return Path(__file__).with_name("direct_page_mode2.schema.json")


__all__ = [
    "DirectPageComparisonError",
    "GRAPHIC_BLOCK_TYPES",
    "SCHEMA_VERSION",
    "compare_selected_pages",
    "resolve_selected_page_source",
    "schema_path",
    "validate_direct_page_comparison_result",
]
