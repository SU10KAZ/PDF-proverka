"""The cross-representation fact vocabulary — what a drawing and a table can both say.

§6 of the track asks for one vocabulary that different representations can emit
into.  The vocabulary here is closed, and every key in it is something a sheet
*shows*.  There is no key whose value could say that a device is not there, a
consumer is not fed or a bus is not drawn: a count of zero is a count, and the
contract's vocabulary guard refuses the words that would turn one into a claim.

Three decisions shape what is emitted.

**The same normalizer on both sides, always.**  Designations come from the
production extractor, quantities from the production load-table parser, cables
from the production cable parser.  A layer that folded ``ГPЩ1`` its own way
would disagree with production about what a designation is, and the
disagreement would look like a finding.  The load-table parser also settles a
homoglyph trap this corpus lays: the ГРЩ sheet prints its current row as
``Ip,A`` in Latin letters while the parser's unit alternation is Cyrillic, so
without production's own ``to_cyrillic`` the current is simply not read.

**A quantity belongs to whatever the drawing attached it to, and to nothing
else.**  On the right document each of the nine consumer blocks is a ruled cell
holding ``Рр,кВт`` and its number, so the number is a fact about the named
block.  The left document prints the same kind of numbers loose on the sheet
next to the same names, and this layer attributes none of them, because the only
thing that would attach them there is proximity.  A drawn cell is what turns a
printed number into a fact about a named thing.

**Direction is not inferred from a count.**  §8 asks whether thirty drawn
feeders and thirty table rows can become one normalized fact.  They are emitted
as two facts — ``feeder_count`` and ``table_row_count`` — and the audit measures
how often the *designations* agree, because designations are printed and counts
merely coincide.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Iterable, Mapping, Sequence

from backend.app.pipeline.stages.block_grounding import electrical_load_table as production_values
from backend.app.services.common import electrical_values as production_cables
from experiments.function_lineage_v2 import instance_identity as production_marks
from experiments.function_topology_v1 import aggregation as topology_aggregation
from experiments.pdf_evidence_v1.textnorm import normalize
from experiments.pdf_evidence_v2.contract import FEEDER, PROVEN as EDGE_PROVEN

from .assembly import designations
from .contract import (
    AssemblyFact,
    DRAWN_TABLE_LATTICE,
    FunctionalAssembly,
    PROVEN_CONNECTED_COMPONENT,
    SCHEMATIC,
    TABLE,
    TEXT,
)
from .representation import PageRepresentation

#: How many folded strings an assembly carries into its artifact.  A cap on the
#: artifact, never on the measurement: every count above is computed over all of
#: them and only the listing is trimmed.
FOLDED_STRING_LIMIT = 64
#: How many evidence references one fact carries.  Same rule.
EVIDENCE_LIMIT = 24


def _provenance(rows: Sequence[Mapping[str, Any]]) -> str:
    kinds = sorted({str(row["provenance"]) for row in rows})
    if not kinds:
        return "NATIVE_PDF_VECTOR"
    return kinds[0] if len(kinds) == 1 else "+".join(kinds)


def quantity_facets(texts: Iterable[str]) -> dict[str, list[float]]:
    """Labelled electrical quantities, through the production parser only.

    Prefixed labels (``Ру=157,5кВт``) are self-sufficient and are read wherever
    they are printed.  The comma form the ruled blocks use (``Рр,кВт 449.3``) is
    read only when a cell hands over its own strings joined — that is the cell
    doing the attaching, not this layer.
    """
    out: dict[str, set[float]] = defaultdict(set)
    for text in texts:
        for row in production_values.parse_values(str(text)):
            for value in row["values"]:
                out[str(row["facet_ref"])].add(float(value))
    return {key: sorted(out[key]) for key in sorted(out)}


def cell_quantity_facets(cells: Mapping[tuple[int, int], Sequence[str]]) -> dict[str, list[float]]:
    """Quantities a ruled cell attaches by holding a caption and a number together."""
    out: dict[str, set[float]] = defaultdict(set)
    for texts in cells.values():
        joined = production_values.to_cyrillic(" ".join(str(text) for text in texts))
        match = production_values.RE_MODE_TABLE_VALUE.search(joined)
        if match is None or not match.group("val"):
            continue
        prefix = production_values._normalize_prefix(match.group("pfx"))
        facet = production_values._PREFIX_FACETS.get(prefix)
        if facet is None:
            continue
        out[facet[0]].add(production_values._decimal(match.group("val")))
    return {key: sorted(out[key]) for key in sorted(out)}


def cable_facets(texts: Iterable[str]) -> list[dict[str, Any]]:
    """Cable designations, through the production cable parser only.

    With one condition the production caller does not need and this one does.
    Production runs ``parse_cable`` on a table cell already known to hold a
    cable; run blind over every printed string of a sheet it is a *reader*
    asked to be a *detector*, and it obliges: ``САФИН`` and ``ДЖАМИЛОВ`` — the
    surnames in the title block — come back as cable marks with no cores and no
    section.  So a cable fact requires the parse to have found the structure
    that makes a designation a cable at all: how many conductors, of what
    section.  A mark on its own is a word.
    """
    seen: dict[str, dict[str, Any]] = {}
    for text in texts:
        parsed = production_cables.parse_cable(str(text))
        if not parsed:
            continue
        if parsed.get("cores") is None or parsed.get("section_mm2") is None:
            continue
        key = production_cables.canonical_mark(parsed.get("mark"))
        if not key:
            continue
        row = {facet: parsed.get(facet) for facet in production_cables.CABLE_FACETS}
        seen.setdefault(key, row)
    return [seen[key] for key in sorted(seen)]


def level_marks(texts: Iterable[str]) -> list[str]:
    found: set[str] = set()
    for text in texts:
        found.update(production_marks.extract_levels(str(text)))
    return sorted(found)


def _feeder_designations(
    assembly: FunctionalAssembly, result: Any
) -> tuple[list[str], int]:
    """Designations printed along the assembly's own feeder wires.

    "Outgoing" here is the drawn port and never a proven direction: V2 measured
    thirteen arrowheads on 278 pages, so which way the energy runs is almost
    never drawn.  What *is* drawn is a wire that ends free, and what is printed
    along it is its designation.
    """
    if not assembly.member_node_ids:
        return [], 0
    topology = result.topology
    kinds = {node.node_id: node.node_kind for node in topology.nodes}
    texts_by_node = topology_aggregation.bound_texts_by_node(topology)
    members = set(assembly.member_node_ids)
    feeders = [node_id for node_id in members if kinds.get(node_id) == FEEDER]
    texts: list[str] = []
    for node_id in feeders:
        texts.extend(texts_by_node.get(node_id, ()))
    return designations(texts), len(feeders)


def facts_of_assembly(
    assembly: FunctionalAssembly,
    page: PageRepresentation,
    result: Any,
    container_cells: Mapping[tuple[int, int], Sequence[str]] | None = None,
) -> list[AssemblyFact]:
    """Everything one assembly can positively state, in the closed vocabulary."""
    rows = [page.labels_by_id[label_id] for label_id in assembly.member_label_ids
            if label_id in page.labels_by_id]
    texts = [str(row["text"]) for row in rows]
    evidence = tuple(assembly.member_label_ids[:EVIDENCE_LIMIT]) or (
        f"assembly:{assembly.assembly_id}",
    )
    representation = assembly.representation_type
    provenance = _provenance(rows)
    out: list[AssemblyFact] = []

    def emit(key: str, value: Any, *, source: str = representation,
             refs: tuple[str, ...] = evidence, applicability: str = "FRAGMENT_LOCAL",
             where: str = provenance) -> None:
        out.append(AssemblyFact(
            assembly_id=assembly.assembly_id, key=key, value=value,
            source_representation=source, applicability=applicability,
            provenance=where, evidence_refs=refs,
        ))

    emit("named_designations", list(assembly.named_designations))
    if assembly.owner_designation:
        emit("owner_designation", assembly.owner_designation)
    emit("printed_string_count", len(texts))
    folded = sorted({normalize(text) for text in texts if normalize(text)})
    emit("folded_strings", folded[:FOLDED_STRING_LIMIT])
    levels = level_marks(texts)
    if levels:
        emit("level_marks", levels)
    quantities = quantity_facets(texts)
    if assembly.assembly_channel == DRAWN_TABLE_LATTICE and container_cells:
        for key, value in cell_quantity_facets(container_cells).items():
            quantities[key] = sorted(set(quantities.get(key, [])) | set(value))
    if quantities:
        emit("quantity_facets", {key: quantities[key] for key in sorted(quantities)})
    cables = cable_facets(texts)
    if cables:
        emit("cable_facets", cables)

    if assembly.assembly_channel == PROVEN_CONNECTED_COMPONENT:
        subgraph = next(
            (item for item in page.aggregation.subgraphs
             if item.subgraph_id in assembly.topology_subgraph_ids), None
        ) if page.aggregation else None
        if subgraph is not None:
            node_refs = tuple(f"node:{node_id}" for node_id in subgraph.member_node_ids[:EVIDENCE_LIMIT])
            shapes = Counter(
                node.symbol_signature or "-" for node in result.topology.nodes
                if node.node_id in set(subgraph.equipment_node_ids)
            )
            branch, feeder_count = _feeder_designations(assembly, result)
            for key, value in (
                ("bus_exists", bool(subgraph.bus_node_ids)),
                ("bus_count", len(subgraph.bus_node_ids)),
                ("feeder_count", feeder_count),
                ("equipment_count", len(subgraph.equipment_node_ids)),
                ("terminal_count", len(subgraph.terminal_node_ids)),
                ("device_shape_multiset", {key: shapes[key] for key in sorted(shapes)}),
                ("outgoing_branch_designations", branch),
                ("topology_signature", subgraph.topology_signature),
            ):
                emit(key, value, source=SCHEMATIC, refs=node_refs or evidence,
                     where="NATIVE_PDF_VECTOR")
            free = _free_ended_feeders(subgraph, result)
            emit("free_ended_feeder_count", free, source=SCHEMATIC,
                 refs=node_refs or evidence, where="NATIVE_PDF_VECTOR")
    elif assembly.assembly_channel == DRAWN_TABLE_LATTICE:
        container = next(
            (item for item in page.containers if item.container_id == assembly.table_ids[0]),
            None,
        ) if assembly.table_ids else None
        if container is not None:
            emit("table_row_count", max(container.rows - 1, 0), source=TABLE)
            emit("table_column_count", max(container.columns - 1, 0), source=TABLE)
            emit("table_column_captions", list(container.column_captions), source=TABLE)
            emit("table_filled_cell_count", len(container.cells), source=TABLE)
            leaders = sorted({
                " ".join(container.cells[key]).strip()
                for key in container.cells if key[1] == 0 and key[0] > 0
            })
            emit("table_row_leaders", [value for value in leaders if value][:FOLDED_STRING_LIMIT],
                 source=TABLE)
    return out


def _free_ended_feeders(subgraph: Any, result: Any) -> int:
    topology = result.topology
    kinds = {node.node_id: node.node_kind for node in topology.nodes}
    adjacency = topology_aggregation._adjacency(topology_aggregation._electrical_edges(topology))
    members = set(subgraph.member_node_ids)
    return sum(
        1 for node_id in members
        if kinds.get(node_id) == FEEDER
        and len(set(adjacency.get(node_id, ())) & members) <= 1
    )


def page_facts(
    page: PageRepresentation,
    assemblies: Sequence[FunctionalAssembly],
    result: Any,
) -> list[AssemblyFact]:
    cells_by_container = {
        container.container_id: container.cells for container in page.containers
    }
    out: list[AssemblyFact] = []
    for assembly in assemblies:
        cells = None
        if assembly.table_ids:
            cells = cells_by_container.get(assembly.table_ids[0])
        out.extend(facts_of_assembly(assembly, page, result, cells))
    return out


def fact_census(facts: Sequence[AssemblyFact]) -> dict[str, Any]:
    """What the corpus states, per key and per representation."""
    by_key = Counter(fact.key for fact in facts)
    by_representation = Counter(fact.source_representation for fact in facts)
    by_provenance = Counter(fact.provenance for fact in facts)
    assemblies = {fact.assembly_id for fact in facts}
    keys_of_assembly: dict[str, set[str]] = defaultdict(set)
    for fact in facts:
        keys_of_assembly[fact.assembly_id].add(fact.key)
    shared = Counter()
    for keys in keys_of_assembly.values():
        for key in keys:
            shared[key] += 1
    return {
        "facts": len(facts),
        "assemblies_with_facts": len(assemblies),
        "facts_by_key": {key: by_key[key] for key in sorted(by_key)},
        "facts_by_representation": {key: by_representation[key] for key in sorted(by_representation)},
        "facts_by_provenance": {key: by_provenance[key] for key in sorted(by_provenance)},
        "assemblies_stating_each_key": {key: shared[key] for key in sorted(shared)},
        "facts_asserting_a_gap": 0,
        "rule": "every value states what the sheet shows; none states what it does not",
    }


__all__ = [
    "EVIDENCE_LIMIT",
    "FOLDED_STRING_LIMIT",
    "cable_facets",
    "cell_quantity_facets",
    "fact_census",
    "facts_of_assembly",
    "level_marks",
    "page_facts",
    "quantity_facets",
]
