"""Two-level engineering context built from frozen deterministic artifacts."""
from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Iterable, Mapping

from ..ai import evidence as legacy_evidence
from ..ai import identity as table_identity
from ..preliminary_report import describe_change
from ..production_artifacts import content_signature

SHEET_CONTEXT_VERSION = "stage-comparison-ai-v2-sheet-context.v1"
FOCUSED_EVIDENCE_VERSION = "stage-comparison-ai-v2-focused-evidence.v1"
COMPACT_CONTEXT_VERSION = "stage-comparison-ai-v2-model-context.v2"


@dataclass
class ContextBundle:
    sheet_context: dict[str, Any]
    evidence_catalog: dict[str, dict[str, Any]]
    focused_by_task: dict[str, dict[str, Any]]

    @property
    def signature(self) -> str:
        return content_signature({
            "sheet_context": self.sheet_context,
            "focused": self.focused_by_task,
        })


def _small_attrs(value: Mapping[str, Any] | None) -> dict[str, Any]:
    allowed = (
        "rating_a", "cables", "status", "member_count", "device_count",
        "label_prefix", "identity_set", "nearby_text", "type_candidate",
    )
    return {
        key: value[key] for key in allowed
        if isinstance(value, Mapping) and key in value
    }


def _node(side: str, value: Mapping[str, Any]) -> dict[str, Any]:
    node_id = str(value.get("id") or "")
    ref = f"{side}:NODE:{node_id}"
    return {
        "ref": ref,
        "side": side,
        "entity_id": node_id,
        "entity_type": str(value.get("type") or ""),
        "label": str(value.get("display_label") or value.get("label") or ""),
        "canonical_identity": str(value.get("canonical_identity") or ""),
        "section": str(value.get("section") or ""),
        "confidence": value.get("confidence"),
        "source_tokens": [str(item) for item in value.get("source_tokens") or ()],
        "attrs": _small_attrs(value.get("attrs")),
    }


def _edge(side: str, value: Mapping[str, Any]) -> dict[str, Any]:
    edge_id = str(value.get("id") or "")
    return {
        "ref": f"{side}:EDGE:{edge_id}",
        "side": side,
        "relation": str(value.get("type") or ""),
        "from_entity": str(value.get("from") or ""),
        "to_entity": str(value.get("to") or ""),
        "confidence": value.get("confidence"),
        "source_tokens": [str(item) for item in value.get("source_tokens") or ()],
    }


def _row(side: str, value: Mapping[str, Any]) -> dict[str, Any]:
    row_id = str(value.get("row_id") or "")
    values = []
    for item in value.get("values") or ():
        if not isinstance(item, Mapping):
            continue
        values.append({
            "facet": str(item.get("facet_ref") or ""),
            "values": list(item.get("values") or ()),
            "unit": str(item.get("unit") or ""),
            "raw": str(item.get("raw") or ""),
            "mode": str(item.get("mode_label") or ""),
            "mode_status": str(item.get("mode_status") or ""),
        })
    return {
        "ref": f"{side}:ROW:{row_id}",
        "side": side,
        "row_id": row_id,
        "row_kind": str(value.get("row_kind") or ""),
        "label": str(value.get("consumer_label") or ""),
        "designations": table_identity.routing.row_designations(value),
        "section": str(value.get("section_ref") or ""),
        "panel": str(value.get("panel") or ""),
        "input_number": value.get("input_number"),
        "mode": str(value.get("mode_label") or ""),
        "cables": [str(item) for item in value.get("cables") or ()],
        "values": values,
        "text": table_identity.render_row_line(value),
    }


def _change(value: Mapping[str, Any]) -> dict[str, Any]:
    change_id = str(value.get("change_id") or "")
    structured = {}
    atoms = ((value.get("provenance") or {}).get("source_atoms") or [])
    if atoms and isinstance(atoms[0], Mapping):
        structured = ((atoms[0].get("provenance") or {}).get("structured") or {})
    relation = structured.get("relation") if isinstance(structured, Mapping) else {}
    return {
        "ref": f"FAST:CHANGE:{change_id}",
        "change_id": change_id,
        "summary": describe_change(value),
        "subject_ref": str(value.get("subject_ref") or ""),
        "facet": str(value.get("facet_ref") or ""),
        "dimension": str(value.get("dimension") or ""),
        "direction": str(value.get("direction") or ""),
        "outcome": str(value.get("outcome") or ""),
        "confidence": str((value.get("confidence") or {}).get("level") or ""),
        "before_value": value.get("before_value"),
        "after_value": value.get("after_value"),
        "left_nodes": list(structured.get("left_nodes") or ())
            if isinstance(structured, Mapping) else [],
        "right_nodes": list(structured.get("right_nodes") or ())
            if isinstance(structured, Mapping) else [],
        "relation": dict(relation) if isinstance(relation, Mapping) else {},
    }


def _inconsistency(value: Mapping[str, Any], index: int) -> dict[str, Any]:
    item_id = str(
        value.get("inconsistency_id") or value.get("row_id") or f"index-{index}"
    )
    return {
        "ref": f"FAST:INCONSISTENCY:{item_id}",
        "inconsistency_id": item_id,
        "kind": str(value.get("kind") or ""),
        "side": str(value.get("side") or ""),
        "subject": str(value.get("subject") or ""),
        "summary": str(value.get("summary") or ""),
        "verdict": str(value.get("verdict") or ""),
        "evidence": dict(value.get("evidence") or {}),
    }


def _recognition(direct: Mapping[str, Any]) -> dict[str, Any]:
    quality = (direct.get("comparison_result") or {}).get("comparison_quality") or {}
    return {
        "left_graph_valid": quality.get("left_graph_valid"),
        "right_graph_valid": quality.get("right_graph_valid"),
        "left_identity_coverage": quality.get("left_identity_coverage"),
        "right_identity_coverage": quality.get("right_identity_coverage"),
        "left_evidence_complete": quality.get("left_evidence_complete"),
        "right_evidence_complete": quality.get("right_evidence_complete"),
        "ambiguous_nodes": quality.get("ambiguous_nodes"),
        "ambiguous_right_nodes": quality.get("ambiguous_right_nodes"),
    }


def build_sheet_context(
    artifacts: Mapping[str, Mapping[str, Any]], *, pair_id: str = "",
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Build compact whole-sheet context and its addressable evidence catalog."""
    direct = artifacts.get("direct_page_mode2") or {}
    tables = ((direct.get("diagnostics") or {}).get("electrical_load_tables") or {})
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    side_context: dict[str, Any] = {}
    for side, graph_key in (("LEFT", "left_graph"), ("RIGHT", "right_graph")):
        graph = direct.get(graph_key) or {}
        side_nodes = [
            _node(side, value) for value in graph.get("nodes") or ()
            if isinstance(value, Mapping)
        ]
        side_edges = [
            _edge(side, value) for value in graph.get("edges") or ()
            if isinstance(value, Mapping)
        ]
        side_rows = [
            _row(side, value) for value in (tables.get(side) or {}).get("rows") or ()
            if isinstance(value, Mapping)
        ]
        nodes.extend(side_nodes)
        edges.extend(side_edges)
        rows.extend(side_rows)
        source = (direct.get("sources") or {}).get(side) or {}
        document = source.get("document") or {}
        sections = [
            value for value in side_nodes if value["entity_type"] == "BUS_SECTION"
        ]
        side_context[side] = {
            "document_code": str(document.get("document_code") or ""),
            "version_id": str(document.get("version_id") or ""),
            "page": int(source.get("page_index_0based") or 0) + 1,
            "block_id": str(source.get("block_id") or ""),
            "discipline": str(graph.get("discipline") or ""),
            "profile": str(graph.get("profile_id") or ""),
            "sections": sections,
            "entity_counts": dict((graph.get("quality") or {})),
            "table_counts": dict((tables.get(side) or {}).get("counts") or {}),
        }

    changes = [
        _change(value) for value in (artifacts.get("unified_synthesis") or {}).get(
            "changes"
        ) or () if isinstance(value, Mapping)
    ]
    inconsistencies = [
        _inconsistency(value, index)
        for index, value in enumerate(
            (artifacts.get("document_inconsistencies") or {}).get("items") or (), 1
        ) if isinstance(value, Mapping)
    ]
    result = direct.get("comparison_result") or {}
    matching = result.get("matching") or {}
    context = {
        "schema_version": SHEET_CONTEXT_VERSION,
        "pair_id": pair_id,
        "sides": side_context,
        "functional_areas": {
            "left": (result.get("functional_groups") or {}).get("left") or [],
            "right": (result.get("functional_groups") or {}).get("right") or [],
            "preserved": (result.get("functional_groups") or {}).get("preserved") or [],
            "changed": (result.get("functional_groups") or {}).get("changed") or [],
        },
        "entities": nodes,
        "graph_relations": edges,
        "table_rows": rows,
        "known_modes": sorted({row["mode"] for row in rows if row["mode"]}),
        "fast_findings": changes,
        "document_inconsistencies": inconsistencies,
        "recognition_quality": _recognition(direct),
        "matching_summary": {
            "metrics": dict(matching.get("metrics") or {}),
            "ambiguous": list(matching.get("ambiguous") or ()),
            "unmatched_left": list(matching.get("unmatched_left") or ()),
            "unmatched_right": list(matching.get("unmatched_right") or ()),
        },
    }
    catalog = {
        str(value["ref"]): dict(value)
        for group in (nodes, edges, rows, changes, inconsistencies)
        for value in group
    }
    context["input_signature"] = content_signature({
        "schema": SHEET_CONTEXT_VERSION,
        "context": context,
    })
    return context, catalog


def _node_neighbors(
    side: str, node_id: str, catalog: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    refs: list[str] = []
    for ref, item in catalog.items():
        if item.get("side") != side or "relation" not in item:
            continue
        if node_id in {item.get("from_entity"), item.get("to_entity")}:
            refs.append(ref)
            other = (
                item.get("to_entity") if item.get("from_entity") == node_id
                else item.get("from_entity")
            )
            node_ref = f"{side}:NODE:{other}"
            if node_ref in catalog:
                refs.append(node_ref)
    return refs[:12]


def _task_candidate_refs(
    task: Mapping[str, Any], catalog: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    payload = task.get("routing_payload") or {}
    refs: list[str] = []
    row_id = str(payload.get("row_id") or "")
    side = str(payload.get("side") or task.get("side") or "").upper()
    if row_id and side:
        refs.append(f"{side}:ROW:{row_id}")
        other = "RIGHT" if side == "LEFT" else "LEFT"
        refs.extend(f"{other}:ROW:{value}" for value in payload.get(
            "candidate_row_ids"
        ) or ())
    refs.extend(f"LEFT:ROW:{value}" for value in payload.get("left_row_ids") or ())
    refs.extend(f"RIGHT:ROW:{value}" for value in payload.get("right_row_ids") or ())
    left_node = str(payload.get("left_node_id") or "")
    if left_node:
        refs.append(f"LEFT:NODE:{left_node}")
        refs.extend(
            f"RIGHT:NODE:{value}" for value in payload.get("right_node_ids") or ()
        )
    change_id = str(payload.get("change_id") or "")
    if change_id:
        change_ref = f"FAST:CHANGE:{change_id}"
        refs.append(change_ref)
        change = catalog.get(change_ref) or {}
        refs.extend(f"LEFT:NODE:{value}" for value in change.get("left_nodes") or ())
        refs.extend(f"RIGHT:NODE:{value}" for value in change.get("right_nodes") or ())
    inconsistency = str(payload.get("inconsistency_id") or "")
    if inconsistency:
        refs.append(f"FAST:INCONSISTENCY:{inconsistency}")
    return list(dict.fromkeys(ref for ref in refs if ref in catalog))


def _related_row_refs(
    candidate_refs: Iterable[str],
    catalog: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    """Rows that can prove values for a focused graph candidate.

    They are context evidence, never identity candidates: otherwise a model
    could select both the graph node and its table witness as two entities on
    one side and correctly fail the verifier's one-left/one-right invariant.
    """
    refs: list[str] = []
    node_refs = [
        ref for ref in candidate_refs if ":NODE:" in ref and ref in catalog
    ]
    for node_ref in node_refs:
        node = catalog[node_ref]
        identity = str(node.get("canonical_identity") or "").strip().casefold()
        section = _section_key(node.get("section"))
        if not identity or not section:
            continue
        related = [
            ref for ref, value in catalog.items()
            if ":ROW:" in ref
            and value.get("side") == node.get("side")
            and _section_key(value.get("section")) == section
            and identity in {
                str(item).strip().casefold()
                for item in value.get("designations") or ()
            }
        ]
        refs.extend(sorted(related)[:6])
    return list(dict.fromkeys(refs))


def _section_key(value: Any) -> str:
    text = str(value or "").strip().upper().replace(" ", "")
    for prefix in ("BUS", "РП", "RP"):
        if text.startswith(prefix) and text[len(prefix):].isdigit():
            return text[len(prefix):]
    return text


def _legacy_text_views(
    artifacts: Mapping[str, Mapping[str, Any]],
    inventory: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    preparation = artifacts.get("text_preparation") or {}
    sheet_relations = artifacts.get("sheet_relations") or {}
    retrieved = {
        str(entry.get("task_id") or ""):
            (entry.get("routing_payload") or {}).get("retrieved") or {}
        for entry in inventory.get("items") or () if isinstance(entry, Mapping)
    }
    packages = legacy_evidence.build_packages(
        review_items=(artifacts.get("unified_synthesis") or {}).get(
            "review_items"
        ) or [],
        preparation=preparation,
        sheet_relations=sheet_relations,
        comparison_groups=preparation.get("comparison_groups") or [],
        batch_size=1000,
        retrieved=retrieved,
    )
    return {
        item.item_id: item.model_view()
        for package in packages for item in package.items
    }


def build_context_bundle(
    *,
    artifacts: Mapping[str, Mapping[str, Any]],
    inventory: Mapping[str, Any],
    pair_id: str = "",
) -> ContextBundle:
    sheet, catalog = build_sheet_context(artifacts, pair_id=pair_id)
    text_views = _legacy_text_views(artifacts, inventory)
    focused: dict[str, dict[str, Any]] = {}
    for task in inventory.get("items") or ():
        if not isinstance(task, Mapping) or not task.get("unresolved", True):
            continue
        task_id = str(task.get("task_id") or "")
        candidates = _task_candidate_refs(task, catalog)
        context_refs: list[str] = _related_row_refs(candidates, catalog)
        for ref in candidates:
            item = catalog.get(ref) or {}
            if ":NODE:" in ref:
                context_refs.extend(_node_neighbors(
                    str(item.get("side") or ""),
                    str(item.get("entity_id") or ""), catalog,
                ))
        text = text_views.get(task_id)
        if text is not None:
            text_ref = f"FOCUS:TEXT:{task_id}"
            catalog[text_ref] = {
                "ref": text_ref,
                "kind": "TEXT_EVIDENCE",
                "text": text,
            }
            candidates.append(text_ref)
        focused[task_id] = {
            "schema_version": FOCUSED_EVIDENCE_VERSION,
            "task_id": task_id,
            "task_type": task.get("task_type"),
            "problem": task.get("summary"),
            "subject": task.get("subject"),
            "candidate_refs": list(dict.fromkeys(candidates)),
            "context_refs": list(dict.fromkeys(
                ref for ref in context_refs if ref in catalog
            )),
            "available_evidence": list(task.get("available_evidence") or ()),
            "missing_evidence": list(task.get("missing_evidence") or ()),
        }
    return ContextBundle(sheet, catalog, focused)


def model_sheet_view(sheet_context: Mapping[str, Any]) -> dict[str, Any]:
    """Return a compact shared index; full records stay in focused evidence.

    Every catalog ref remains discoverable, but raw text, source tokens and
    repeated graph attributes are not copied into every session.  The backend
    still owns the lossless catalog and may dereference an allowlisted ref for
    a controlled expansion.
    """
    sides = {}
    for side, value in (sheet_context.get("sides") or {}).items():
        record = dict(value) if isinstance(value, Mapping) else {}
        sections = [
            {
                "ref": item.get("ref"),
                "label": item.get("label"),
                "canonical_identity": item.get("canonical_identity"),
            }
            for item in record.pop("sections", [])
            if isinstance(item, Mapping)
        ]
        sides[str(side)] = {**record, "sections": sections}

    evidence_index: list[dict[str, Any]] = []
    for entity in sheet_context.get("entities") or ():
        if not isinstance(entity, Mapping):
            continue
        evidence_index.append({
            "ref": entity.get("ref"),
            "kind": "ENTITY",
            "side": entity.get("side"),
            "type": entity.get("entity_type"),
            "label": entity.get("label"),
            "identity": entity.get("canonical_identity"),
            "section": entity.get("section"),
        })
    for edge in sheet_context.get("graph_relations") or ():
        if not isinstance(edge, Mapping):
            continue
        evidence_index.append({
            "ref": edge.get("ref"),
            "kind": "GRAPH_RELATION",
            "side": edge.get("side"),
            "relation": edge.get("relation"),
            "from": edge.get("from_entity"),
            "to": edge.get("to_entity"),
        })
    for row in sheet_context.get("table_rows") or ():
        if not isinstance(row, Mapping):
            continue
        evidence_index.append({
            "ref": row.get("ref"),
            "kind": "TABLE_ROW",
            "side": row.get("side"),
            "row_kind": row.get("row_kind"),
            "label": row.get("label"),
            "designations": list(row.get("designations") or ()),
            "section": row.get("section"),
            "mode": row.get("mode"),
        })
    for finding in sheet_context.get("fast_findings") or ():
        if not isinstance(finding, Mapping):
            continue
        evidence_index.append({
            "ref": finding.get("ref"),
            "kind": "FAST_FINDING",
            "subject_ref": finding.get("subject_ref"),
            "facet": finding.get("facet"),
            "direction": finding.get("direction"),
            "outcome": finding.get("outcome"),
            "before": finding.get("before_value"),
            "after": finding.get("after_value"),
        })
    for finding in sheet_context.get("document_inconsistencies") or ():
        if not isinstance(finding, Mapping):
            continue
        evidence_index.append({
            "ref": finding.get("ref"),
            "kind": "DOCUMENT_INCONSISTENCY",
            "side": finding.get("side"),
            "type": finding.get("kind"),
            "subject": finding.get("subject"),
            "summary": finding.get("summary"),
            "verdict": finding.get("verdict"),
        })
    evidence_index.sort(key=lambda item: str(item.get("ref") or ""))
    matching = sheet_context.get("matching_summary") or {}
    return {
        "schema_version": COMPACT_CONTEXT_VERSION,
        "pair_id": sheet_context.get("pair_id"),
        "sides": sides,
        "functional_areas": sheet_context.get("functional_areas") or {},
        "known_modes": list(sheet_context.get("known_modes") or ()),
        "recognition_quality": sheet_context.get("recognition_quality") or {},
        "matching_summary": {
            "metrics": matching.get("metrics") or {},
            "ambiguous_count": len(matching.get("ambiguous") or ()),
        },
        "evidence_index": evidence_index,
        "catalog_contract": {
            "full_records_are_in_focused_evidence": True,
            "need_more_evidence_is_backend_dereferenced": True,
            "indexed_refs": len(evidence_index),
        },
    }


def legacy_model_sheet_view(sheet_context: Mapping[str, Any]) -> dict[str, Any]:
    """The v1 payload, retained only for measured before/after accounting."""
    return {
        key: value for key, value in sheet_context.items()
        if key not in {"matching_summary"}
    } | {
        "matching_summary": {
            "metrics": (sheet_context.get("matching_summary") or {}).get("metrics"),
            "ambiguous": (sheet_context.get("matching_summary") or {}).get(
                "ambiguous"
            ),
        }
    }


def model_evidence_view(value: Mapping[str, Any]) -> dict[str, Any]:
    """Compact one full focused record without dropping engineering fields."""
    result = dict(value)
    result.pop("source_tokens", None)
    attrs = result.get("attrs")
    if isinstance(attrs, Mapping):
        result["attrs"] = {
            key: item for key, item in attrs.items() if key != "nearby_text"
        }
    if result.get("row_id"):
        result.pop("text", None)
        values = []
        for item in result.get("values") or ():
            if not isinstance(item, Mapping):
                continue
            compact = dict(item)
            # Numeric/string value and unit are the fact.  ``raw`` repeats the
            # same cell and remains losslessly available in the backend catalog.
            compact.pop("raw", None)
            values.append(compact)
        result["values"] = values
    return result


def serialized_bytes(value: Any) -> int:
    return len(json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8"))


def dereference(
    refs: Iterable[str], catalog: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [dict(catalog[ref]) for ref in refs if ref in catalog]


def dereference_for_model(
    refs: Iterable[str], catalog: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        model_evidence_view(catalog[ref]) for ref in refs if ref in catalog
    ]


__all__ = [
    "ContextBundle",
    "COMPACT_CONTEXT_VERSION",
    "FOCUSED_EVIDENCE_VERSION",
    "SHEET_CONTEXT_VERSION",
    "build_context_bundle",
    "build_sheet_context",
    "dereference",
    "dereference_for_model",
    "legacy_model_sheet_view",
    "model_evidence_view",
    "model_sheet_view",
    "serialized_bytes",
]
