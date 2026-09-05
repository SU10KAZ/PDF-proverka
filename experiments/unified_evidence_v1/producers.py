"""Adapters: from each frozen layer's own record shape to ``UnifiedFact``.

Every producer is read-only over the frozen bridge state and the frozen
lineage artifacts.  None of them re-derives a rule: a fact is emitted exactly
where the layer beneath already proved it, with the layer's own reference as
provenance.  What changes is only the shape.

Two volumes are deliberately not materialized as rows.  The low-level
topology holds 29 193 nodes and 33 456 edges, and V2's own storage decision was
that the raw graph lives per page in memory and the production form is compact;
here the graph contributes its *bindings* (a printed string bound to a node by
a drawn relation), its arrow-proven directions and per-page counts, and the
per-node rows stay in V2's artifact.  The same holds for the 48 578 printed
strings: each becomes one ``printed_string`` fact — that is the whole point of
the layer — but the designations, levels, quantities and cables read from them
are emitted as facts of their own only where the production parsers find
structure.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable, Mapping, Sequence

from backend.app.pipeline.stages.block_grounding import electrical_load_table as production_values
from backend.app.services.common import electrical_values as production_cables
from experiments.function_assembly_membership_v1 import evidence as membership_evidence
from experiments.function_assembly_membership_v1.contract import CERTIFIED
from experiments.function_lineage_v2 import instance_identity as production_marks
from experiments.function_lineage_v3 import corpus as frozen_corpus
from experiments.function_representation_bridge_v1.contract import (
    DRAWN_TABLE_LATTICE,
    PROVEN_CONNECTED_COMPONENT,
)
from experiments.pdf_evidence_v1.textnorm import MIN_COMPARABLE, normalize
from experiments.pdf_evidence_v2.contract import (
    LABEL_ANCHOR,
    LABEL_CONNECTION,
    PROVEN as DIRECTION_PROVEN,
    PROVEN_CONNECTION,
)

from .contract import (
    ASSEMBLY_LOCAL,
    DERIVED,
    DOCUMENT_SHARED,
    DRAWN_RELATION,
    EXACT_GEOMETRY,
    FUNCTION_LOCAL,
    FUNCTION_REGION,
    FUNCTION_TOPOLOGY_SUBGRAPH,
    FUNCTIONAL_ASSEMBLY,
    MARKDOWN_OCR,
    NATIVE_PDF_TEXT,
    PAGE_ONLY,
    POSITIVE_PRESENCE,
    SCHEMATIC_TOPOLOGY,
    SHEET_SHARED,
    SUPPORT_ONLY,
    TABLE,
    UNKNOWN,
    UnifiedFact,
    stable_id,
)

#: V1's attribution kinds that mean "a drawn thing owns this string".
DRAWN_OWNERSHIPS = ("TABLE_CELL", "DIRECT_CONTAINMENT", "CONNECTED_CALLOUT")
#: V1 attribution → applicability of a string on its own.
_V1_APPLICABILITY = {
    "FRAGMENT_LOCAL": None,      # decided by the container the assembly layer built
    "SHEET_SHARED": SHEET_SHARED,
    "DOCUMENT_SHARED": DOCUMENT_SHARED,
    "UNKNOWN": UNKNOWN,
}


class Certificates:
    """The certified (function, assembly) pairs, indexed both ways."""

    def __init__(self, rows: Sequence[Any]) -> None:
        self.by_assembly: dict[str, list[Any]] = defaultdict(list)
        self.by_function: dict[tuple[str, str, str], Any] = {}
        for row in rows:
            self.by_function[(row.pair_id, row.side, row.function_id)] = row
            if row.status == CERTIFIED:
                for assembly_id in row.certified_assembly_ids:
                    self.by_assembly[assembly_id].append(row)

    def certified_pairs(self) -> dict[tuple[str, str, str], tuple[str, ...]]:
        return {
            key: tuple(row.certified_assembly_ids)
            for key, row in self.by_function.items() if row.status == CERTIFIED
        }

    def attach(self, fact: UnifiedFact, assembly_id: str | None) -> UnifiedFact:
        if not assembly_id:
            return fact
        rows = self.by_assembly.get(assembly_id, [])
        if rows:
            fact.certified_assembly_id = assembly_id
            fact.certified_function_ids = tuple(sorted({row.function_id for row in rows}))
            scopes = sorted({row.scope_id for row in rows if row.scope_id})
            fact.certified_function_scope_id = scopes[0] if len(scopes) == 1 else None
            if len(scopes) > 1:
                fact.notes = fact.notes + (f"certified_scopes={len(scopes)}",)
        return fact


def _fact(**kwargs: Any) -> UnifiedFact:
    payload = {
        key: kwargs[key] for key in ("field", "normalized_value", "producer", "pair_id", "side",
                                     "physical_page", "provenance_refs")
    }
    return UnifiedFact(fact_id=stable_id("uef", payload), **kwargs)


def _container_of(page: Any, assembly: Any | None, row: Mapping[str, Any]) -> dict[str, Any] | None:
    if assembly is not None:
        kind = {PROVEN_CONNECTED_COMPONENT: "ISLAND", DRAWN_TABLE_LATTICE: "TABLE"}.get(
            assembly.assembly_channel, "ASSEMBLY")
        out = {"kind": kind, "id": assembly.assembly_id}
        if row.get("cell") is not None:
            out["cell"] = [int(value) for value in row["cell"]]
        return out
    if row.get("ownership") == "STAMP_ZONE":
        return {"kind": "STAMP", "id": f"stamp:{page.physical_page}"}
    if row.get("ownership") in DRAWN_OWNERSHIPS and row.get("region_id"):
        return {"kind": "REGION", "id": str(row["region_id"])}
    return None


# ---------------------------------------------------------------------------
# NATIVE_PDF_TEXT — every printed string, and what the production parsers read
# ---------------------------------------------------------------------------


def native_text_facts(state: Mapping[str, Any], certificates: Certificates) -> list[UnifiedFact]:
    out: list[UnifiedFact] = []
    for (pair_id, side), page_map in sorted(state["pages"].items()):
        assembly_map = state["assemblies_map"][(pair_id, side)]
        for page_number, page in sorted(page_map.items()):
            assemblies = assembly_map.get(page_number, [])
            owner_of_label = {
                label_id: assembly for assembly in assemblies for label_id in assembly.member_label_ids
            }
            for label_id, row in page.labels_by_id.items():
                assembly = owner_of_label.get(label_id)
                ownership = str(row.get("ownership") or "NO_OWNERSHIP")
                if assembly is not None:
                    applicability = ASSEMBLY_LOCAL
                elif ownership == "STAMP_ZONE":
                    applicability = SHEET_SHARED
                else:
                    applicability = UNKNOWN
                container = _container_of(page, assembly, row)
                refs = (f"label:{label_id}",)
                common = dict(
                    source_representation=NATIVE_PDF_TEXT, producer="pdf_evidence_v1",
                    pair_id=pair_id, document=page.document, side=side, physical_page=page_number,
                    applicability=applicability, claim_semantics=POSITIVE_PRESENCE,
                    provenance_grade=EXACT_GEOMETRY, provenance_refs=refs, container=container,
                )
                text = str(row["text"])
                folded = normalize(text)
                if len(folded) >= MIN_COMPARABLE:
                    fact = _fact(field="printed_string", normalized_value=folded, raw_value=text, **common)
                    out.append(certificates.attach(fact, assembly.assembly_id if assembly else None))
                for mark in sorted(membership_evidence.marks_of(text)):
                    out.append(certificates.attach(
                        _fact(field="designation", normalized_value=mark, raw_value=text, **common),
                        assembly.assembly_id if assembly else None))
                for level in production_marks.extract_levels(text):
                    out.append(certificates.attach(
                        _fact(field="level_mark", normalized_value=level, raw_value=text, **common),
                        assembly.assembly_id if assembly else None))
                for parsed in production_values.parse_values(text):
                    if parsed.get("reading") != "PREFIXED":
                        continue
                    for value in parsed["values"]:
                        out.append(certificates.attach(_fact(
                            field="quantity",
                            normalized_value={"facet": str(parsed["facet_ref"]), "value": float(value)},
                            raw_value=str(parsed.get("raw") or text), **common),
                            assembly.assembly_id if assembly else None))
                cable = production_cables.parse_cable(text)
                if cable and cable.get("cores") is not None and cable.get("section_mm2") is not None:
                    out.append(certificates.attach(_fact(
                        field="cable",
                        normalized_value={
                            "mark": production_cables.canonical_mark(cable.get("mark")),
                            "cores": cable.get("cores"), "section_mm2": cable.get("section_mm2"),
                        },
                        raw_value=text, **common), assembly.assembly_id if assembly else None))
    return out


# ---------------------------------------------------------------------------
# TABLE — ruled cells the bridge accepted as tables
# ---------------------------------------------------------------------------


def table_facts(state: Mapping[str, Any], certificates: Certificates) -> list[UnifiedFact]:
    out: list[UnifiedFact] = []
    for (pair_id, side), page_map in sorted(state["pages"].items()):
        assembly_map = state["assemblies_map"][(pair_id, side)]
        for page_number, page in sorted(page_map.items()):
            containers = {item.container_id: item for item in page.containers}
            for assembly in assembly_map.get(page_number, []):
                if assembly.assembly_channel != DRAWN_TABLE_LATTICE or not assembly.table_ids:
                    continue
                container = containers.get(assembly.table_ids[0])
                if container is None:
                    continue
                common = dict(
                    source_representation=TABLE, producer="function_representation_bridge_v1",
                    pair_id=pair_id, document=page.document, side=side, physical_page=page_number,
                    applicability=ASSEMBLY_LOCAL, claim_semantics=POSITIVE_PRESENCE,
                    provenance_grade=EXACT_GEOMETRY,
                    container={"kind": "TABLE", "id": assembly.assembly_id},
                )
                for caption in container.column_captions:
                    if normalize(caption):
                        out.append(certificates.attach(_fact(
                            field="table_caption", normalized_value=normalize(caption), raw_value=caption,
                            provenance_refs=(f"region:{container.region_id}", "row:0"), **common),
                            assembly.assembly_id))
                for (row_index, column), texts in sorted(container.cells.items()):
                    joined = " ".join(texts).strip()
                    if not normalize(joined):
                        continue
                    out.append(certificates.attach(_fact(
                        field="table_cell",
                        normalized_value={"row": int(row_index), "column": int(column), "text": normalize(joined)},
                        raw_value=joined,
                        provenance_refs=(f"region:{container.region_id}", f"cell:{row_index}:{column}"),
                        **common), assembly.assembly_id))
                    if column == 0 and row_index > 0:
                        out.append(certificates.attach(_fact(
                            field="table_row_leader", normalized_value=normalize(joined), raw_value=joined,
                            provenance_refs=(f"region:{container.region_id}", f"cell:{row_index}:0"),
                            **common), assembly.assembly_id))
    return out


# ---------------------------------------------------------------------------
# FUNCTION_REGION — V1's drawn regions as containers
# ---------------------------------------------------------------------------


def region_facts(state: Mapping[str, Any], certificates: Certificates) -> list[UnifiedFact]:
    out: list[UnifiedFact] = []
    for (pair_id, side), results in sorted(state["results"].items()):
        page_map = state["pages"][(pair_id, side)]
        assembly_map = state["assemblies_map"][(pair_id, side)]
        for result in results:
            page = page_map.get(result.page)
            if page is None:
                continue
            assembly_of_region = {
                assembly.source_region_ids[0]: assembly
                for assembly in assembly_map.get(result.page, []) if assembly.source_region_ids
            }
            owned: dict[str, list[str]] = defaultdict(list)
            for label_id, row in page.labels_by_id.items():
                if row.get("ownership") in DRAWN_OWNERSHIPS and row.get("region_id"):
                    owned[str(row["region_id"])].append(label_id)
            for region in result.data.regions:
                if region.kind == "SHEET_FRAME":
                    continue
                labels = owned.get(region.region_id, [])
                assembly = assembly_of_region.get(region.region_id)
                bbox = [round(float(v), 1) for v in region.bbox]
                container = (
                    {"kind": "TABLE" if assembly.assembly_channel == DRAWN_TABLE_LATTICE else "ASSEMBLY",
                     "id": assembly.assembly_id, "region_id": region.region_id, "bbox": bbox}
                    if assembly is not None else
                    {"kind": "REGION", "id": region.region_id, "bbox": bbox}
                )
                common = dict(
                    source_representation=FUNCTION_REGION, producer="pdf_evidence_v1",
                    pair_id=pair_id, document=page.document, side=side, physical_page=result.page,
                    applicability=ASSEMBLY_LOCAL if assembly is not None else UNKNOWN,
                    claim_semantics=POSITIVE_PRESENCE, provenance_grade=EXACT_GEOMETRY,
                    provenance_refs=(f"region:{region.region_id}",),
                    container=container,
                )
                out.append(certificates.attach(_fact(
                    field="region", normalized_value=region.kind, **common),
                    assembly.assembly_id if assembly else None))
                out.append(certificates.attach(_fact(
                    field="region_string_count", normalized_value=len(labels), **common),
                    assembly.assembly_id if assembly else None))
                marks = sorted({
                    mark for label_id in labels
                    for mark in membership_evidence.marks_of(page.labels_by_id[label_id]["text"])
                })
                if marks:
                    out.append(certificates.attach(_fact(
                        field="region_designations", normalized_value=marks, **common),
                        assembly.assembly_id if assembly else None))
    return out


# ---------------------------------------------------------------------------
# SCHEMATIC_TOPOLOGY — V2's bindings, directions and per-page counts
# ---------------------------------------------------------------------------


def topology_facts(state: Mapping[str, Any], certificates: Certificates) -> list[UnifiedFact]:
    out: list[UnifiedFact] = []
    for (pair_id, side), results in sorted(state["results"].items()):
        page_map = state["pages"][(pair_id, side)]
        assembly_map = state["assemblies_map"][(pair_id, side)]
        for result in results:
            page = page_map.get(result.page)
            if page is None:
                continue
            topology = result.topology
            if not topology.nodes:
                continue
            assembly_of_node = {
                node_id: assembly for assembly in assembly_map.get(result.page, [])
                for node_id in assembly.member_node_ids
            }
            nodes = {node.node_id: node for node in topology.nodes}
            common = dict(
                source_representation=SCHEMATIC_TOPOLOGY, producer="pdf_evidence_v2",
                pair_id=pair_id, document=page.document, side=side, physical_page=result.page,
            )
            electrical = [node for node in topology.nodes if node.node_kind != LABEL_ANCHOR]
            proven = [edge for edge in topology.edges if edge.connection_claim == PROVEN_CONNECTION
                      and edge.edge_kind != LABEL_CONNECTION]
            out.append(_fact(field="node_count", normalized_value=len(electrical),
                             applicability=SHEET_SHARED, claim_semantics=SUPPORT_ONLY,
                             provenance_grade=DERIVED, provenance_refs=(f"page:{result.page}",), **common))
            out.append(_fact(field="proven_connection_count", normalized_value=len(proven),
                             applicability=SHEET_SHARED, claim_semantics=SUPPORT_ONLY,
                             provenance_grade=DERIVED, provenance_refs=(f"page:{result.page}",), **common))
            for edge in topology.edges:
                if edge.edge_kind != LABEL_CONNECTION or edge.connection_claim != PROVEN_CONNECTION:
                    continue
                anchor = nodes.get(edge.from_node_id)
                target = nodes.get(edge.to_node_id)
                if anchor is None or target is None:
                    continue
                assembly = assembly_of_node.get(target.node_id)
                for text in anchor.labels:
                    out.append(certificates.attach(_fact(
                        field="label_binding",
                        normalized_value={"text": normalize(text), "node_kind": target.node_kind},
                        raw_value=text,
                        applicability=ASSEMBLY_LOCAL if assembly else UNKNOWN,
                        claim_semantics=POSITIVE_PRESENCE, provenance_grade=DRAWN_RELATION,
                        provenance_refs=(f"edge:{edge.edge_id}", f"node:{target.node_id}"),
                        container={"kind": "ISLAND", "id": assembly.assembly_id} if assembly else None,
                        **common), assembly.assembly_id if assembly else None))
            for edge in topology.edges:
                if edge.direction_status != DIRECTION_PROVEN:
                    continue
                assembly = assembly_of_node.get(edge.from_node_id) or assembly_of_node.get(edge.to_node_id)
                out.append(certificates.attach(_fact(
                    field="proven_direction",
                    normalized_value={"from": edge.from_node_id, "to": edge.to_node_id},
                    applicability=ASSEMBLY_LOCAL if assembly else UNKNOWN,
                    claim_semantics=POSITIVE_PRESENCE, provenance_grade=DRAWN_RELATION,
                    provenance_refs=(f"edge:{edge.edge_id}",),
                    container={"kind": "ISLAND", "id": assembly.assembly_id} if assembly else None,
                    **common), assembly.assembly_id if assembly else None))
            for node in electrical:
                if node.node_kind != "BUS":
                    continue
                assembly = assembly_of_node.get(node.node_id)
                out.append(certificates.attach(_fact(
                    field="bus_node", normalized_value=node.node_id,
                    applicability=ASSEMBLY_LOCAL if assembly else UNKNOWN,
                    claim_semantics=POSITIVE_PRESENCE, provenance_grade=EXACT_GEOMETRY,
                    provenance_refs=(f"node:{node.node_id}",),
                    container={"kind": "ISLAND", "id": assembly.assembly_id} if assembly else None,
                    **common), assembly.assembly_id if assembly else None))
            for index, arrow in enumerate(getattr(result, "arrowheads", []) or []):
                out.append(_fact(
                    field="arrowhead", normalized_value=index, applicability=UNKNOWN,
                    claim_semantics=POSITIVE_PRESENCE, provenance_grade=EXACT_GEOMETRY,
                    provenance_refs=(f"arrowhead:{result.page}:{index}",), **common))
    return out


# ---------------------------------------------------------------------------
# FUNCTION_TOPOLOGY_SUBGRAPH — Topology V1's aggregates
# ---------------------------------------------------------------------------


def subgraph_facts(state: Mapping[str, Any], certificates: Certificates) -> list[UnifiedFact]:
    out: list[UnifiedFact] = []
    for (pair_id, side), page_map in sorted(state["pages"].items()):
        assembly_map = state["assemblies_map"][(pair_id, side)]
        for page_number, page in sorted(page_map.items()):
            aggregation = getattr(page, "aggregation", None)
            if aggregation is None:
                continue
            assembly_of_subgraph = {
                assembly.topology_subgraph_ids[0]: assembly
                for assembly in assembly_map.get(page_number, []) if assembly.topology_subgraph_ids
            }
            for subgraph in aggregation.subgraphs:
                assembly = assembly_of_subgraph.get(subgraph.subgraph_id)
                common = dict(
                    source_representation=FUNCTION_TOPOLOGY_SUBGRAPH, producer="function_topology_v1",
                    pair_id=pair_id, document=page.document, side=side, physical_page=page_number,
                    applicability=ASSEMBLY_LOCAL, claim_semantics=POSITIVE_PRESENCE,
                    provenance_grade=DRAWN_RELATION,
                    provenance_refs=(f"subgraph:{subgraph.subgraph_id}",) + tuple(subgraph.evidence_refs[:4]),
                    container={"kind": "ISLAND", "id": assembly.assembly_id if assembly else subgraph.subgraph_id},
                )
                assembly_id = assembly.assembly_id if assembly else None
                for field_name, value in (
                    ("subgraph", subgraph.subgraph_id),
                    ("boundary_status", subgraph.boundary_status),
                    ("bus_count", len(subgraph.bus_node_ids)),
                    ("feeder_count", len(subgraph.feeder_node_ids)),
                    ("equipment_count", len(subgraph.equipment_node_ids)),
                    ("terminal_count", len(subgraph.terminal_node_ids)),
                ):
                    out.append(certificates.attach(_fact(field=field_name, normalized_value=value, **common), assembly_id))
                if subgraph.topology_signature:
                    out.append(certificates.attach(_fact(
                        field="topology_signature", normalized_value=subgraph.topology_signature, **common), assembly_id))
                for mark in subgraph.function_marks:
                    out.append(certificates.attach(_fact(field="owner_mark", normalized_value=mark, **common), assembly_id))
    return out


# ---------------------------------------------------------------------------
# FUNCTIONAL_ASSEMBLY — the bridge's closed fact vocabulary, one to one
# ---------------------------------------------------------------------------


def assembly_facts(state: Mapping[str, Any], certificates: Certificates) -> list[UnifiedFact]:
    out: list[UnifiedFact] = []
    assemblies = {item.assembly_id: item for item in state["assemblies"]}
    for fact in state["facts"]:
        assembly = assemblies.get(fact.assembly_id)
        if assembly is None:
            continue
        grade = DRAWN_RELATION if fact.source_representation == "SCHEMATIC" else EXACT_GEOMETRY
        out.append(certificates.attach(_fact(
            field=fact.key, normalized_value=fact.value,
            source_representation=FUNCTIONAL_ASSEMBLY, producer="function_representation_bridge_v1",
            pair_id=assembly.pair_id, document=assembly.document, side=assembly.side,
            physical_page=assembly.physical_page, applicability=ASSEMBLY_LOCAL,
            claim_semantics=POSITIVE_PRESENCE, provenance_grade=grade,
            provenance_refs=tuple(fact.evidence_refs[:8]) or (f"assembly:{fact.assembly_id}",),
            container={"kind": "ASSEMBLY", "id": fact.assembly_id},
        ), fact.assembly_id))
    for assembly in state["assemblies"]:
        out.append(certificates.attach(_fact(
            field="assembly",
            normalized_value={"channel": assembly.assembly_channel, "kind": assembly.assembly_kind,
                              "representation": assembly.representation_type, "extent": assembly.membership_status},
            source_representation=FUNCTIONAL_ASSEMBLY, producer="function_representation_bridge_v1",
            pair_id=assembly.pair_id, document=assembly.document, side=assembly.side,
            physical_page=assembly.physical_page, applicability=ASSEMBLY_LOCAL,
            claim_semantics=POSITIVE_PRESENCE,
            provenance_grade=DRAWN_RELATION if assembly.topology_subgraph_ids else EXACT_GEOMETRY,
            provenance_refs=tuple(assembly.evidence_refs[:4]) or (f"assembly:{assembly.assembly_id}",),
            container={"kind": "ASSEMBLY", "id": assembly.assembly_id},
        ), assembly.assembly_id))
    return out


# ---------------------------------------------------------------------------
# MARKDOWN_OCR — passports (declared) and fragment rows (page-only, promotable)
# ---------------------------------------------------------------------------


def _native_index(state: Mapping[str, Any]) -> dict[tuple[str, str, int], dict[str, list[str]]]:
    """Normalized printed strings of every page, for promoting OCR rows."""
    out: dict[tuple[str, str, int], dict[str, list[str]]] = {}
    for (pair_id, side), page_map in state["pages"].items():
        for page_number, page in page_map.items():
            table: dict[str, list[str]] = defaultdict(list)
            for label_id, row in page.labels_by_id.items():
                table[normalize(row["text"])].append(label_id)
            out[(pair_id, side, page_number)] = table
    return out


def markdown_facts(
    state: Mapping[str, Any],
    fragments: Mapping[tuple[str, str], Mapping[str, Mapping[str, Any]]],
    scope_of_function: Mapping[tuple[str, str], str],
) -> list[UnifiedFact]:
    out: list[UnifiedFact] = []
    native = _native_index(state)
    for pair_id in frozen_corpus.PROJECTS:
        project = frozen_corpus.PROJECTS[pair_id]
        passports = frozen_corpus.passports(pair_id)
        for side in frozen_corpus.SIDES:
            document = f"{project}/{side}"
            table = fragments.get((pair_id, side), {})
            for function_id, passport in sorted(passports[side].items()):
                page_number = int(passport["source_sheet"]["physical_page"])
                declared = (str(function_id),)
                common = dict(
                    source_representation=MARKDOWN_OCR, producer="function_lineage_passport",
                    pair_id=pair_id, document=document, side=side, physical_page=page_number,
                    claim_semantics=SUPPORT_ONLY, provenance_grade=PAGE_ONLY,
                    provenance_refs=(f"passport:{function_id}",), declared_function_ids=declared,
                )
                title = (passport.get("source_sheet") or {}).get("title")
                if title:
                    out.append(_fact(field="sheet_title", normalized_value=normalize(title), raw_value=title,
                                     applicability=SHEET_SHARED, **common))
                mark = membership_evidence.primary_mark_of(passport)
                if mark:
                    out.append(_fact(field="primary_mark", normalized_value=mark, applicability=SHEET_SHARED, **common))
                for field_name, key in (("function_class", "function_class"), ("component_role", "component_role")):
                    if passport.get(key):
                        out.append(_fact(field=field_name, normalized_value=str(passport[key]),
                                         applicability=FUNCTION_LOCAL, **common))
                for field_name, value in membership_evidence.documented_values(passport):
                    out.append(_fact(
                        field="passport_value", normalized_value={"field": field_name, "value": normalize(value)},
                        raw_value=value, applicability=FUNCTION_LOCAL, **common))
                for facet, values in membership_evidence.passport_quantities(passport).items():
                    for value in sorted(values):
                        out.append(_fact(field="passport_quantity",
                                         normalized_value={"facet": facet, "value": float(value)},
                                         applicability=FUNCTION_LOCAL, **common))
                # raw OCR rows of the fragment: page-only, promoted when printed natively
                page_native = native.get((pair_id, side, page_number), {})
                fragment_rows = [table[key] for key in passport.get("function_fragment_ids") or [] if key in table]
                for segment in membership_evidence.fragment_segments(fragment_rows):
                    label_ids = page_native.get(segment, [])
                    if label_ids:
                        out.append(_fact(
                            field="evidence_row", normalized_value=segment, applicability=UNKNOWN,
                            claim_semantics=POSITIVE_PRESENCE, provenance_grade=EXACT_GEOMETRY,
                            provenance_refs=(f"fragment:{passport.get('function_fragment_ids', [''])[0]}",)
                            + tuple(f"label:{item}" for item in label_ids[:4]),
                            source_representation=MARKDOWN_OCR, producer="function_lineage_fragment",
                            pair_id=pair_id, document=document, side=side, physical_page=page_number,
                            declared_function_ids=declared,
                            notes=("promoted: the native layer prints the same string on the same page",),
                        ))
                    else:
                        out.append(_fact(
                            field="evidence_row", normalized_value=segment, applicability=UNKNOWN,
                            claim_semantics=SUPPORT_ONLY, provenance_grade=PAGE_ONLY,
                            provenance_refs=(f"fragment:{passport.get('function_fragment_ids', [''])[0]}",),
                            source_representation=MARKDOWN_OCR, producer="function_lineage_fragment",
                            pair_id=pair_id, document=document, side=side, physical_page=page_number,
                            declared_function_ids=declared,
                        ))
    return out


# ---------------------------------------------------------------------------
# certificates and cross-sheet named references
# ---------------------------------------------------------------------------


def certificate_facts(state: Mapping[str, Any], certificates: Certificates) -> list[UnifiedFact]:
    out: list[UnifiedFact] = []
    for row in certificates.by_function.values():
        if row.status != CERTIFIED:
            continue
        for assembly_id in row.certified_assembly_ids:
            out.append(UnifiedFact(
                fact_id=stable_id("uef", {"certificate": row.certificate_id, "assembly": assembly_id}),
                field="membership_certificate",
                normalized_value={"channel": row.channel, "relation": row.relation_kind},
                source_representation=FUNCTIONAL_ASSEMBLY, producer="function_assembly_membership_v1",
                pair_id=row.pair_id, document=f"{row.project}/{row.side}", side=row.side,
                physical_page=int(row.physical_page or 0), applicability=ASSEMBLY_LOCAL,
                claim_semantics=POSITIVE_PRESENCE, provenance_grade=DRAWN_RELATION,
                provenance_refs=tuple(row.evidence_refs[:6]) or (f"certificate:{row.certificate_id}",),
                container={"kind": "ASSEMBLY", "id": assembly_id},
                declared_function_ids=(row.function_id,),
                certified_function_scope_id=row.scope_id, certified_function_ids=(row.function_id,),
                certified_assembly_id=assembly_id,
            ))
    return out


def cross_sheet_reference_facts(state: Mapping[str, Any]) -> list[UnifiedFact]:
    """A captioned container on one sheet naming the primary mark of a function on another.

    Positive and undirected: the sheet *names* the mark.  Which way the energy
    runs is not stated, because no arrow states it.
    """
    out: list[UnifiedFact] = []
    marks_of_function: dict[tuple[str, str], list[tuple[str, int]]] = defaultdict(list)
    for pair_id in frozen_corpus.PROJECTS:
        passports = frozen_corpus.passports(pair_id)
        for side in frozen_corpus.SIDES:
            for function_id, passport in passports[side].items():
                mark = membership_evidence.primary_mark_of(passport)
                if mark:
                    marks_of_function[(pair_id, side, mark)].append(
                        (str(function_id), int(passport["source_sheet"]["physical_page"])))
    for assembly in state["assemblies"]:
        if not assembly.owner_designation:
            continue
        for mark in sorted(membership_evidence.marks_of(assembly.owner_designation)):
            targets = [
                (function_id, page) for function_id, page
                in marks_of_function.get((assembly.pair_id, assembly.side, mark), [])
                if page != assembly.physical_page
            ]
            if not targets:
                continue
            out.append(UnifiedFact(
                fact_id=stable_id("uef", {"reference": assembly.assembly_id, "mark": mark}),
                field="cross_sheet_named_reference",
                normalized_value={"mark": mark, "named_on_page": assembly.physical_page,
                                  "function_pages": sorted({page for _f, page in targets})},
                source_representation=FUNCTIONAL_ASSEMBLY, producer="function_representation_bridge_v1",
                pair_id=assembly.pair_id, document=assembly.document, side=assembly.side,
                physical_page=assembly.physical_page, applicability=ASSEMBLY_LOCAL,
                claim_semantics=POSITIVE_PRESENCE, provenance_grade=EXACT_GEOMETRY,
                provenance_refs=tuple(assembly.evidence_refs[:2]) or (f"assembly:{assembly.assembly_id}",),
                container={"kind": "ASSEMBLY", "id": assembly.assembly_id},
                declared_function_ids=tuple(sorted({function_id for function_id, _p in targets})),
                notes=("undirected: the sheet names the mark; no arrow states the direction",),
            ))
    return out


def produce_all(
    state: Mapping[str, Any],
    certificate_rows: Sequence[Any],
    fragments: Mapping[tuple[str, str], Mapping[str, Mapping[str, Any]]],
) -> tuple[list[UnifiedFact], Certificates]:
    certificates = Certificates(certificate_rows)
    facts: list[UnifiedFact] = []
    facts.extend(native_text_facts(state, certificates))
    facts.extend(table_facts(state, certificates))
    facts.extend(region_facts(state, certificates))
    facts.extend(topology_facts(state, certificates))
    facts.extend(subgraph_facts(state, certificates))
    facts.extend(assembly_facts(state, certificates))
    facts.extend(markdown_facts(state, fragments, state["scope_model"]["scope_of_function"]))
    facts.extend(certificate_facts(state, certificates))
    facts.extend(cross_sheet_reference_facts(state))
    return facts, certificates


__all__ = [
    "Certificates",
    "assembly_facts",
    "certificate_facts",
    "cross_sheet_reference_facts",
    "markdown_facts",
    "native_text_facts",
    "produce_all",
    "region_facts",
    "subgraph_facts",
    "table_facts",
    "topology_facts",
]
