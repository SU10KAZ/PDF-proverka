"""The PDF Evidence V2 measurement: build the topology, then interrogate it.

Run:  ``python -m experiments.pdf_evidence_v2.audit``

Reads the six frozen documents of the v2.x / v3.0 / V1 corpus, builds the
schematic topology graph for every physical page, and writes the artifacts and
the report.  No model is called, no production module is touched, nothing is
written next to any PDF.

Every page of every document is processed.  The track asks for a representative
selection and this is the least biased way to give one: the pages are then
*profiled* from what they turned out to contain, so the metrics can be read per
kind of sheet without anyone having chosen which sheets to look at.
"""
from __future__ import annotations

import hashlib
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from experiments.function_lineage_v3 import corpus as frozen_corpus
from experiments.pdf_evidence_v1 import extraction as v1_extraction

from . import direction as direction_module
from . import identity as identity_module
from . import pipeline as pipeline_module
from . import reassessment as reassessment_module
from . import strokes as strokes_module
from . import symbols as symbols_module
from . import validation as validation_module
from .conductors import prove_conductors
from .contract import (
    BUS,
    EQUIPMENT,
    FEEDER,
    LABEL_ANCHOR,
    NO_CLAIM,
    PROVEN,
    PROVEN_CONNECTION,
    SCHEMA_VERSION,
    UNDIRECTED,
    assert_closed_vocabularies,
    assert_connection_evidence,
    assert_direction_evidence,
    assert_no_absence_vocabulary,
    assert_no_page_spanning_edges,
    assert_single_ownership,
    contract_document,
)
from .pipeline import PageResult

DEFAULT_OUTPUT = frozen_corpus.COMPARISON_ROOT / "20260904_pdf_evidence_v2"

#: The control sheet of the track: ``IOS1.1/RIGHT`` page 21, the ГРЩ single-line
#: diagram, 579 printed strings against 34 in the Markdown.
CONTROL = ("p19cd7f695a", "RIGHT", 21)

#: Page profiles, decided from what the page turned out to hold rather than
#: from a list of pages someone found convenient.
NO_VECTOR_GEOMETRY = "NO_VECTOR_GEOMETRY"
SPARSE_GEOMETRY = "SPARSE_GEOMETRY"
TABLE_SHEET = "TABLE_SHEET"
DRAWING_WITHOUT_A_BUS = "DRAWING_WITHOUT_A_BUS"
SINGLE_LINE_SCHEME = "SINGLE_LINE_SCHEME"
SCHEME_WITH_TABLES = "SCHEME_WITH_TABLES"
PROFILES = (
    NO_VECTOR_GEOMETRY, SPARSE_GEOMETRY, TABLE_SHEET,
    DRAWING_WITHOUT_A_BUS, SINGLE_LINE_SCHEME, SCHEME_WITH_TABLES,
)
#: Below this many welded edges a page is sparse rather than drawn.
SPARSE_EDGES = 40


def _document_code(project: str, side: str) -> str:
    return f"{project}/{side}"


def profile_of(result: PageResult) -> str:
    edges = len(result.data.strokes.edges)
    tables = sum(1 for region in result.data.regions if region.kind in {"TABLE", "STAMP"})
    buses = sum(1 for node in result.topology.nodes if node.node_kind == BUS)
    feeders = sum(1 for node in result.topology.nodes if node.node_kind == FEEDER)
    if edges == 0:
        return NO_VECTOR_GEOMETRY
    if edges < SPARSE_EDGES:
        return SPARSE_GEOMETRY
    if buses and feeders >= 5:
        return SCHEME_WITH_TABLES if tables else SINGLE_LINE_SCHEME
    if tables and not buses:
        return TABLE_SHEET
    return DRAWING_WITHOUT_A_BUS


def build_corpus(limit: int | None = None) -> dict[tuple[str, str], list[PageResult]]:
    """Every page of every frozen document, analysed once."""
    out: dict[tuple[str, str], list[PageResult]] = {}
    for pair_id, project, side, paths in frozen_corpus.documents():
        code = _document_code(project, side)
        body = frozen_corpus.markdown_pages(paths["markdown"])
        profile = v1_extraction.document_profile(str(paths["pdf"]), body)
        total = v1_extraction.page_count(str(paths["pdf"]))
        pages = range(total if limit is None else min(total, limit))
        out[(pair_id, side)] = [
            pipeline_module.analyse(code, str(paths["pdf"]), index, profile)
            for index in pages
        ]
    return out


# ---------------------------------------------------------------------------
# measurements
# ---------------------------------------------------------------------------


def graph_metrics(results: Mapping[tuple[str, str], list[PageResult]]) -> dict[str, Any]:
    """The per-corpus table the track asks for, plus what it needs to be read."""
    documents: list[dict[str, Any]] = []
    for pair_id, project, side, _ in frozen_corpus.documents():
        pages = results[(pair_id, side)]
        nodes = Counter()
        edges = Counter()
        claims = Counter()
        directions = Counter()
        junctions = Counter()
        counters: dict[str, int] = {}
        for result in pages:
            for node in result.topology.nodes:
                nodes[node.node_kind] += 1
            for edge in result.topology.edges:
                edges[edge.edge_kind] += 1
                claims[edge.connection_claim] += 1
                directions[edge.direction_status] += 1
                if edge.junction_evidence:
                    junctions[edge.junction_evidence] += 1
            validation_module._accumulate(counters, result.counters)
        documents.append({
            "document": _document_code(project, side),
            "pages_processed": len(pages),
            "topology_nodes": int(sum(nodes.values())),
            "topology_edges": int(sum(edges.values())),
            "proven_edges": int(claims.get(PROVEN_CONNECTION, 0)),
            "no_claim_edges": int(claims.get(NO_CLAIM, 0)),
            "edges_with_a_proven_direction": int(directions.get(PROVEN, 0)),
            "edges_undirected": int(directions.get(UNDIRECTED, 0)),
            "junctions": int(counters.get("dots_joining_conductors", 0)
                             + counters.get("endpoint_meetings", 0)
                             + counters.get("tee_meetings", 0)),
            "junction_dots": int(counters.get("dots_joining_conductors", 0)),
            "crossings_rejected": int(counters.get("crossings_rejected", 0)),
            "crossings_refused_by_a_hop": int(counters.get("hops_proving_non_connection", 0)),
            "table_grid_edges": int(counters.get("edges", 0)) and int(
                sum(1 for result in pages for value in result.facts.nature if value == "TABLE_GRID")),
            "frame_edges": int(
                sum(1 for result in pages for value in result.facts.nature if value == "FRAME")),
            "equipment_nodes": int(nodes.get(EQUIPMENT, 0)),
            "bus_nodes": int(nodes.get(BUS, 0)),
            "feeder_nodes": int(nodes.get(FEEDER, 0)),
            "labels_bound": int(counters.get("labels_bound", 0)),
            "labels_recorded_by_alignment": int(counters.get("column_edges", 0)),
            "islands": int(sum(result.topology.counters.get("islands", 0) for result in pages)),
            "nodes_by_kind": dict(sorted(nodes.items())),
            "edges_by_kind": dict(sorted(edges.items())),
            "junction_evidence": dict(sorted(junctions.items())),
        })
    totals: dict[str, int] = {}
    for row in documents:
        for key, value in row.items():
            if isinstance(value, int):
                totals[key] = totals.get(key, 0) + value
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "schematic_topology_graph",
        "model_calls": 0,
        "documents": documents,
        "totals": dict(sorted(totals.items())),
    }


def page_profiles(results: Mapping[tuple[str, str], list[PageResult]]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    by_profile: dict[str, Counter] = defaultdict(Counter)
    for pair_id, project, side, _ in frozen_corpus.documents():
        for result in results[(pair_id, side)]:
            profile = profile_of(result)
            nodes = Counter(node.node_kind for node in result.topology.nodes)
            row = {
                "document": _document_code(project, side),
                "physical_page": result.page,
                "profile": profile,
                "welded_edges": len(result.data.strokes.edges),
                "proven_conductors": int(result.facts.conductor.sum()),
                "nodes": len(result.topology.nodes),
                "proven_edges": sum(
                    1 for edge in result.topology.edges
                    if edge.connection_claim == PROVEN_CONNECTION),
                "bus_nodes": int(nodes.get(BUS, 0)),
                "labels": len(result.data.labels),
                "labels_bound": int(result.counters.get("labels_bound", 0)),
            }
            rows.append(row)
            bucket = by_profile[profile]
            for key in ("welded_edges", "proven_conductors", "nodes", "proven_edges",
                        "bus_nodes", "labels", "labels_bound"):
                bucket[key] += int(row[key])
            bucket["pages"] += 1
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "page_profiles",
        "model_calls": 0,
        "rule": "a page is profiled from what it turned out to hold, never chosen in advance",
        "profiles": list(PROFILES),
        "by_profile": {
            profile: dict(sorted(by_profile[profile].items()))
            for profile in PROFILES if profile in by_profile
        },
        "pages": rows,
    }


def representative_pages(results: Mapping[tuple[str, str], list[PageResult]]) -> list[tuple[str, str, int]]:
    """Pages written out in full: the control sheet plus the extremes of each profile.

    Chosen by measurement, deterministically: for every profile a document
    produced, the page with the most proven conductors and the page with the
    fewest.  Nothing here can prefer a page because it reads well.
    """
    chosen: set[tuple[str, str, int]] = set()
    control_pages = results.get((CONTROL[0], CONTROL[1])) or []
    if any(result.page == CONTROL[2] for result in control_pages):
        chosen.add(CONTROL)
    for pair_id, project, side, _ in frozen_corpus.documents():
        grouped: dict[str, list[PageResult]] = defaultdict(list)
        for result in results[(pair_id, side)]:
            grouped[profile_of(result)].append(result)
        for profile in PROFILES:
            rows = grouped.get(profile)
            if not rows:
                continue
            ordered = sorted(rows, key=lambda row: (int(row.facts.conductor.sum()), row.page))
            chosen.add((pair_id, side, ordered[-1].page))
            chosen.add((pair_id, side, ordered[0].page))
    return sorted(chosen)


#: Rows written per node or edge table.  The shape of the graph is visible on
#: one sheet; a row per node for the corpus is sixteen megabytes of JSON that
#: nobody reads and that the track explicitly asks not to be committed.
DETAIL_ROW_CAP = 6000


def detail_pages(results: Mapping[tuple[str, str], list[PageResult]]) -> list[tuple[str, str, int]]:
    """The sheet whose every node and edge is written out: the control sheet."""
    control_pages = results.get((CONTROL[0], CONTROL[1])) or []
    if any(result.page == CONTROL[2] for result in control_pages):
        return [CONTROL]
    return []


def node_edge_tables(
    results: Mapping[tuple[str, str], list[PageResult]],
    wanted: Sequence[tuple[str, str, int]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Full node and edge rows for the representative pages, totals for the rest."""
    keep = set(wanted)
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    node_totals: Counter = Counter()
    edge_totals: Counter = Counter()
    for pair_id, project, side, _ in frozen_corpus.documents():
        for result in results[(pair_id, side)]:
            for node in result.topology.nodes:
                node_totals[node.node_kind] += 1
            for edge in result.topology.edges:
                edge_totals[edge.edge_kind] += 1
            if (pair_id, side, result.page) not in keep:
                continue
            nodes.extend(node.to_dict() for node in result.topology.nodes)
            edges.extend(edge.to_dict() for edge in result.topology.edges)
    return (
        {
            "schema_version": SCHEMA_VERSION,
            "kind": "topology_nodes",
            "model_calls": 0,
            "detail_pages": [[pair_id, side, page] for pair_id, side, page in sorted(keep)],
            "corpus_totals_by_kind": dict(sorted(node_totals.items())),
            "corpus_total": int(sum(node_totals.values())),
            "rows_written": min(len(nodes), DETAIL_ROW_CAP),
            "rows_on_the_detail_pages": len(nodes),
            "rows": nodes[:DETAIL_ROW_CAP],
        },
        {
            "schema_version": SCHEMA_VERSION,
            "kind": "topology_edges",
            "model_calls": 0,
            "detail_pages": [[pair_id, side, page] for pair_id, side, page in sorted(keep)],
            "corpus_totals_by_kind": dict(sorted(edge_totals.items())),
            "corpus_total": int(sum(edge_totals.values())),
            "rows_written": min(len(edges), DETAIL_ROW_CAP),
            "rows_on_the_detail_pages": len(edges),
            "rows": edges[:DETAIL_ROW_CAP],
        },
    )


def binding_audit(
    results: Mapping[tuple[str, str], list[PageResult]],
    wanted: Sequence[tuple[str, str, int]],
) -> dict[str, Any]:
    keep = set(wanted)
    status: Counter = Counter()
    channels: Counter = Counter()
    targets: Counter = Counter()
    offsets: Counter = Counter()
    v1_versus_v2 = Counter()
    rows: list[dict[str, Any]] = []
    for pair_id, project, side, _ in frozen_corpus.documents():
        for result in results[(pair_id, side)]:
            bound_ids = set()
            for record in result.bindings:
                status[record.status] += 1
                if record.channel:
                    channels[record.channel] += 1
                if record.target_kind:
                    targets[record.target_kind] += 1
                if record.offset_em is not None:
                    offsets[f"{round(float(record.offset_em), 1):.1f}"] += 1
                if record.status == "BOUND":
                    bound_ids.add(record.label_id)
            v1_local = {
                label_id for label_id, value in result.v1_ownership.items()
                if value.get("applicability") == "FRAGMENT_LOCAL"
            }
            v1_versus_v2["v1_fragment_local"] += len(v1_local)
            v1_versus_v2["v2_bound"] += len(bound_ids)
            v1_versus_v2["both"] += len(v1_local & bound_ids)
            v1_versus_v2["only_v1"] += len(v1_local - bound_ids)
            v1_versus_v2["only_v2"] += len(bound_ids - v1_local)
            if (pair_id, side, result.page) in keep:
                rows.extend({
                    "document": _document_code(project, side),
                    "physical_page": result.page,
                    **record.to_dict(),
                } for record in result.bindings)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "topology_binding_audit",
        "model_calls": 0,
        "status": dict(sorted(status.items())),
        "channels": dict(sorted(channels.items())),
        "bound_target_kind": dict(sorted(targets.items())),
        "offset_in_ems": dict(sorted(offsets.items())),
        "against_v1_ownership": dict(sorted(v1_versus_v2.items())),
        "note": (
            "offset_em is the perpendicular distance from the string to the "
            "conductor that took it, in ems of the string's own size; 0.0 means "
            "the conductor passes inside the string's own box"
        ),
        "detail_pages": [[pair_id, side, page] for pair_id, side, page in sorted(keep)],
        "rows_written": min(len(rows), DETAIL_ROW_CAP),
        "rows_on_the_detail_pages": len(rows),
        "rows": rows[:DETAIL_ROW_CAP],
    }


def negative_controls(results: Mapping[tuple[str, str], list[PageResult]]) -> dict[str, Any]:
    per_document: list[dict[str, Any]] = []
    for pair_id, project, side, _ in frozen_corpus.documents():
        rows = [result.controls for result in results[(pair_id, side)]]
        per_document.append({
            "document": _document_code(project, side),
            **validation_module.summarize(rows),
        })
    total = validation_module.summarize(
        result.controls for pages in results.values() for result in pages)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "topology_negative_controls",
        "model_calls": 0,
        "controls": {
            "A": "two lines cross and no junction is drawn — no proven connection",
            "B": "a table lattice is not schematic topology",
            "C": "a drawing frame is not a bus",
            "D": "a rule drawn under a word is not a conductor",
            "E": "the nearest conductor to a label binds nothing",
            "F": "independent drawings on one sheet are not joined",
            "G": "one block drawn twice is two nodes",
        },
        "per_document": per_document,
        "totals": total,
    }


def graph_validation(
    results: Mapping[tuple[str, str], list[PageResult]],
    wanted: Sequence[tuple[str, str, int]],
) -> dict[str, Any]:
    """Structural checks, the contract guards, and an independent replay."""
    consistency = validation_module.summarize(
        result.consistency for pages in results.values() for result in pages)
    guards: dict[str, Any] = {}
    for name, guard in (
        ("CLOSED_VOCABULARIES", assert_closed_vocabularies),
        ("CONNECTION_REQUIRES_DRAWN_EVIDENCE", assert_connection_evidence),
        ("DIRECTION_REQUIRES_AN_ARROWHEAD", assert_direction_evidence),
        ("NO_EDGE_SPANS_TWO_PAGES", assert_no_page_spanning_edges),
        ("A_LABEL_BINDS_TO_ONE_NODE", assert_single_ownership),
    ):
        violations = 0
        for pages in results.values():
            for result in pages:
                try:
                    if name in {"CLOSED_VOCABULARIES", "NO_EDGE_SPANS_TWO_PAGES"}:
                        guard(result.topology.nodes, result.topology.edges)
                    else:
                        guard(result.topology.edges)
                except AssertionError:
                    violations += 1
        guards[name] = violations

    replay: list[dict[str, Any]] = []
    for pair_id, side, page in wanted:
        paths = frozen_corpus.document_paths(pair_id, side)
        code = _document_code(frozen_corpus.PROJECTS[pair_id], side)
        body = frozen_corpus.markdown_pages(paths["markdown"])
        profile = v1_extraction.document_profile(str(paths["pdf"]), body)
        original = next(
            (result for result in results[(pair_id, side)] if result.page == page), None)
        if original is None:
            continue
        again = pipeline_module.analyse(code, str(paths["pdf"]), page - 1, profile)
        replay.append({
            "document": code,
            "physical_page": page,
            "first": _graph_digest(original),
            "second": _graph_digest(again),
            "identical": _graph_digest(original) == _graph_digest(again),
        })
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "topology_graph_validation",
        "model_calls": 0,
        "consistency": consistency,
        "producer_guards": guards,
        "replay": {
            "pages": len(replay),
            "identical": sum(1 for row in replay if row["identical"]),
            "rows": replay,
        },
    }


def _graph_digest(result: PageResult) -> str:
    payload = {
        "nodes": [node.to_dict() for node in result.topology.nodes],
        "edges": [edge.to_dict() for edge in result.topology.edges],
    }
    return hashlib.sha256(_canonical(payload)).hexdigest()


def symbol_inventory(results: Mapping[tuple[str, str], list[PageResult]]) -> dict[str, Any]:
    """Recurring blocks, discovered rather than listed."""
    occurrences: Counter = Counter()
    nodes_by_signature: dict[str, set[str]] = defaultdict(set)
    pages_by_signature: dict[str, set[str]] = defaultdict(set)
    named: dict[str, Counter] = defaultdict(Counter)
    for pair_id, project, side, _ in frozen_corpus.documents():
        for result in results[(pair_id, side)]:
            marks = identity_module.bound_marks(result.topology)
            for node in result.topology.nodes:
                if not node.symbol_signature:
                    continue
                occurrences[node.symbol_signature] += 1
                nodes_by_signature[node.symbol_signature].add(node.node_id)
                pages_by_signature[node.symbol_signature].add(
                    f"{_document_code(project, side)}#{result.page}")
                for text in marks.get(node.node_id, ()):  # a name, if the sheet gave one
                    named[node.symbol_signature][str(text)] += 1
    top = occurrences.most_common(40)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "topology_symbol_inventory",
        "model_calls": 0,
        "rule": (
            "a signature says two clusters were drawn from one block and says "
            "nothing about what the block means; a name arrives only from a "
            "label bound by a drawn relation"
        ),
        "signatures": len(occurrences),
        "signatures_used_more_than_once": sum(1 for value in occurrences.values() if value > 1),
        "occurrences": int(sum(occurrences.values())),
        "signatures_seen_on_more_than_one_document": sum(
            1 for signature, pages in pages_by_signature.items()
            if len({page.split("#")[0] for page in pages}) > 1),
        "most_frequent": [
            {
                "signature": signature,
                "occurrences": count,
                "distinct_nodes": len(nodes_by_signature[signature]),
                "pages": len(pages_by_signature[signature]),
                "names_the_sheet_gave": [
                    text for text, _ in named[signature].most_common(3)
                ],
            }
            for signature, count in top
        ],
    }


def direction_audit(results: Mapping[tuple[str, str], list[PageResult]]) -> dict[str, Any]:
    arrowheads = 0
    directed = 0
    trap_totals: Counter = Counter()
    examples: list[dict[str, Any]] = []
    for pair_id, project, side, _ in frozen_corpus.documents():
        for result in results[(pair_id, side)]:
            arrowheads += len(result.arrowheads)
            directed += int(result.counters.get("edges_directed", 0))
            trap = direction_module.keyword_trap(result.topology)
            trap_totals["nodes_a_keyword_rule_would_direct"] += int(
                trap["nodes_a_keyword_rule_would_direct"])
            trap_totals["of_those_carrying_a_counter_name_on_the_same_conductor"] += int(
                trap["of_those_carrying_a_counter_name_on_the_same_conductor"])
            for row in trap["examples"]:
                if len(examples) < 12:
                    examples.append({
                        "document": _document_code(project, side),
                        "physical_page": result.page, **row,
                    })
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "topology_direction_audit",
        "model_calls": 0,
        "arrowheads_found": arrowheads,
        "edges_given_a_proven_direction": directed,
        "keyword_trap": dict(sorted(trap_totals.items())),
        "edges_directed_from_a_keyword": 0,
        "examples": examples,
        "rule": (
            "only an arrowhead proves a direction; a direction word names the "
            "far end of the wire and would invert every outgoing feeder"
        ),
    }


def control_sheet(results: Mapping[tuple[str, str], list[PageResult]]) -> dict[str, Any]:
    """The sheet the track named, walked end to end.

    ``IOS1.1/RIGHT`` page 21 is the ГРЩ single-line diagram: 579 printed strings
    against 34 in the Markdown.  The question the track asks of it is whether a
    feed label, a bus, an outgoing feeder, a device and a consumer label can be
    put on one object.  It is answered by walking, for every feeder whose cable
    mark binds to a conductor, the shortest proven path to a bus — and by
    reporting what the walk does *not* reach.

    Nothing here is a rule.  The page is measured by the same code as every
    other page; it is only printed at greater length.
    """
    from collections import deque

    pair_id, side, page_number = CONTROL
    pages = results.get((pair_id, side)) or []
    result = next((row for row in pages if row.page == page_number), None)
    if result is None:
        return {
            "schema_version": SCHEMA_VERSION,
            "kind": "control_sheet_walk",
            "model_calls": 0,
            "available": False,
        }
    graph = result.topology
    nodes = graph.node_by_id()
    adjacency = identity_module.electrical_adjacency(graph)
    buses = {node.node_id for node in graph.nodes if node.node_kind == BUS}
    bound = identity_module.bound_marks(graph)
    aligned = identity_module.aligned_marks(graph)

    def walk(start: str) -> list[str] | None:
        queue: deque[tuple[str, list[str]]] = deque([(start, [start])])
        seen = {start}
        while queue:
            current, path = queue.popleft()
            if current in buses and current != start:
                return path
            for neighbour in sorted(adjacency.get(current, ())):
                if neighbour in seen:
                    continue
                seen.add(neighbour)
                queue.append((neighbour, path + [neighbour]))
        return None

    walks: list[dict[str, Any]] = []
    for node_id, texts in sorted(bound.items()):
        marks = [text for text in texts if _DESIGNATION_LIKE(text)]
        if not marks:
            continue
        path = walk(node_id)
        walks.append({
            "node_id": node_id,
            "cable_mark": sorted(marks)[0],
            "other_bound_strings": [text for text in texts if text not in marks],
            "strings_recorded_by_alignment_only": aligned.get(node_id, []),
            "reaches_a_bus": path is not None,
            "path": [
                {"node_id": step, "node_kind": nodes[step].node_kind}
                for step in (path or [])
            ],
        })
    kinds = Counter(node.node_kind for node in graph.nodes)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "control_sheet_walk",
        "model_calls": 0,
        "available": True,
        "document": result.data.document,
        "physical_page": page_number,
        "printed_strings": len(result.data.labels),
        "welded_edges": len(result.data.strokes.edges),
        "proven_conductors": int(result.facts.conductor.sum()),
        "nodes_by_kind": dict(sorted(kinds.items())),
        "junction_dots": int(result.counters.get("dots_joining_conductors", 0)),
        "crossings_refused": int(result.counters.get("crossings_rejected", 0)),
        "crossings_refused_by_a_hop": int(result.counters.get("hops_proving_non_connection", 0)),
        "series_gaps": int(result.counters.get("series_gaps", 0)),
        "labels_bound": int(result.counters.get("labels_bound", 0)),
        "labels_recorded_by_alignment": int(result.counters.get("column_edges", 0)),
        "feeders_named_by_a_cable_mark": len(walks),
        "of_those_reaching_a_bus": sum(1 for row in walks if row["reaches_a_bus"]),
        "path_length_histogram": {
            str(key): value for key, value in sorted(
                Counter(len(row["path"]) for row in walks if row["reaches_a_bus"]).items())
        },
        "walks": walks,
    }


def _DESIGNATION_LIKE(text: str) -> bool:
    from .direction import _is_designation

    return _is_designation(text)


def storage_design(results: Mapping[tuple[str, str], list[PageResult]]) -> dict[str, Any]:
    raw_floats = 0
    fill_items = 0
    normalized_floats = 0
    node_rows = 0
    edge_rows = 0
    compact_bytes = 0
    for pages in results.values():
        for result in pages:
            counters = result.data.strokes.counters
            raw_floats += int(counters.get("raw_segments", 0)) * 4
            raw_floats += int(counters.get("fill_ink_items", 0)) * 4
            fill_items += int(counters.get("fill_ink_items", 0))
            normalized_floats += len(result.data.strokes.edges) * 4
            normalized_floats += len(result.data.strokes.slanted) * 4
            normalized_floats += len(result.data.strokes.blobs) * 4
            node_rows += len(result.topology.nodes)
            edge_rows += len(result.topology.edges)
            compact_bytes += sum(
                len(node.node_id) + 24 for node in result.topology.nodes)
            compact_bytes += sum(
                len(edge.from_node_id) + len(edge.to_node_id) + 24
                for edge in result.topology.edges)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "topology_storage_design",
        "model_calls": 0,
        "research_raw": {
            "coordinate_floats": raw_floats,
            "bytes_at_eight_per_float": raw_floats * 8,
            "of_which_fill_ink_items": fill_items,
            "verdict": "never persisted; read, measured, released",
        },
        "normalized": {
            "coordinate_floats": normalized_floats,
            "bytes_at_eight_per_float": normalized_floats * 8,
            "compression_against_raw": (
                round(raw_floats / normalized_floats, 1) if normalized_floats else None),
            "verdict": "research artifact only",
        },
        "topology_graph": {
            "nodes": node_rows,
            "edges": edge_rows,
            "compact_bytes_estimate": compact_bytes,
            "verdict": "the production shape: identifiers, kinds, claims, one evidence reference",
        },
        "recommendation": {
            "production_compact": (
                "nodes and edges without geometry, one evidence reference each, "
                "per physical page"
            ),
            "research_raw": "kept in memory for one page at a time and never written",
        },
    }


# ---------------------------------------------------------------------------
# artifact
# ---------------------------------------------------------------------------


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def measure(results: Mapping[tuple[str, str], list[PageResult]]) -> dict[str, Any]:
    wanted = representative_pages(results)
    detail = detail_pages(results)
    nodes, edges = node_edge_tables(results, detail)
    artifact: dict[str, Any] = {
        "contract": contract_document(),
        "schematic_topology_graph": graph_metrics(results),
        "topology_nodes": nodes,
        "topology_edges": edges,
        "topology_binding_audit": binding_audit(results, detail),
        "page_profiles": page_profiles(results),
        "topology_symbol_inventory": symbol_inventory(results),
        "topology_direction_audit": direction_audit(results),
        "topology_negative_controls": negative_controls(results),
        "topology_graph_validation": graph_validation(results, wanted),
        "topology_passport_reassessment": reassessment_module.field_placement(results),
        "topology_relational_facts": reassessment_module.relational_facts(results),
        "function_lineage_reassessment": reassessment_module.tier_reassessment(results),
        "topology_merge_and_split": reassessment_module.merge_and_split(results),
        "control_sheet_walk": control_sheet(results),
        "topology_storage_design": storage_design(results),
    }
    artifact["identity_signatures"] = identity_signatures(results)
    artifact["verdict"] = verdict(artifact)
    for key, payload in artifact.items():
        if key == "contract":
            continue
        assert_no_absence_vocabulary(payload)
    return artifact


def identity_signatures(results: Mapping[tuple[str, str], list[PageResult]]) -> dict[str, Any]:
    power: dict[str, Counter] = defaultdict(Counter)
    convergence: Counter = Counter()
    for pages in results.values():
        for result in pages:
            fingerprints = identity_module.signatures(result.topology)
            for kind, row in identity_module.distinguishing_power(
                    result.topology, fingerprints).items():
                for key, value in row.items():
                    power[kind][key] += int(value)
            structures = identity_module.convergences(result.topology)
            convergence["convergence_candidates"] += int(structures["convergence_candidates"])
            convergence["series_pairs"] += int(structures["series_pairs"])
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "topology_identity_signatures",
        "model_calls": 0,
        "rule": (
            "an engineering identity is the class, the device in series, the "
            "distance to a bus and the branch shape — never the page"
        ),
        "by_node_kind": {kind: dict(sorted(row.items())) for kind, row in sorted(power.items())},
        "structures": dict(sorted(convergence.items())),
    }


def verdict(artifact: Mapping[str, Any]) -> dict[str, Any]:
    graph = artifact["schematic_topology_graph"]["totals"]
    controls = artifact["topology_negative_controls"]["totals"]
    validation = artifact["topology_graph_validation"]
    profiles = artifact["page_profiles"]["by_profile"]
    reassessed = artifact["topology_passport_reassessment"]
    relational = artifact["topology_relational_facts"]
    tiers = artifact["function_lineage_reassessment"]["tiers"]
    scheme_pages = sum(
        int(profiles.get(profile, {}).get("pages", 0))
        for profile in (SINGLE_LINE_SCHEME, SCHEME_WITH_TABLES)
    )
    all_pages = int(graph.get("pages_processed", 0))
    leaks = (
        int(controls.get("B_table_grid_edges_that_conduct", 0))
        + int(controls.get("C_frame_edges_that_conduct", 0))
        + int(controls.get("D_underline_edges_that_conduct", 0))
        + int(controls.get("F_proven_edges_between_two_islands", 0))
        + int(controls.get("E_labels_attributed_by_proximity", 0))
    )
    guards_clean = all(value == 0 for value in validation["producer_guards"].values())
    replay_clean = validation["replay"]["pages"] == validation["replay"]["identical"]
    by_regime = {row["regime"]: row for row in reassessed["regimes"]}
    letter = "C"
    if leaks == 0 and guards_clean and replay_clean and int(graph.get("proven_edges", 0)) > 0:
        letter = "A" if scheme_pages >= 0.5 * max(all_pages, 1) else "B"
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "verdict",
        "model_calls": 0,
        "verdict": letter,
        "pages_processed": all_pages,
        "pages_that_are_schematics": scheme_pages,
        "proven_edges": int(graph.get("proven_edges", 0)),
        "no_claim_edges": int(graph.get("no_claim_edges", 0)),
        "edges_with_a_proven_direction": int(graph.get("edges_with_a_proven_direction", 0)),
        "leaks": leaks,
        "producer_guards_clean": guards_clean,
        "replay_byte_identical": replay_clean,
        "fragment_local_total": {
            regime: int(row["fragment_local_total"]) for regime, row in by_regime.items()
        },
        "functions_joined_to_a_node_set": int(
            relational["functions_joined_to_a_node_set_by_their_own_printed_mark"]),
        "functions_with_a_proven_neighbour": int(
            relational["functions_with_at_least_one_proven_neighbour"]),
        "tiers": {name: {"before": row["before"], "after": row["after"]} for name, row in tiers.items()},
        "deploy": "none",
        "shadow": "none",
        "materialization": "none",
    }


ARTIFACT_FILES = (
    ("topology_contract.json", "contract"),
    ("schematic_topology_graph.json", "schematic_topology_graph"),
    ("topology_nodes.json", "topology_nodes"),
    ("topology_edges.json", "topology_edges"),
    ("topology_binding_audit.json", "topology_binding_audit"),
    ("page_profiles.json", "page_profiles"),
    ("topology_symbol_inventory.json", "topology_symbol_inventory"),
    ("topology_direction_audit.json", "topology_direction_audit"),
    ("topology_negative_controls.json", "topology_negative_controls"),
    ("topology_graph_validation.json", "topology_graph_validation"),
    ("topology_passport_reassessment.json", "topology_passport_reassessment"),
    ("topology_relational_facts.json", "topology_relational_facts"),
    ("function_lineage_reassessment.json", "function_lineage_reassessment"),
    ("topology_merge_and_split.json", "topology_merge_and_split"),
    ("control_sheet_walk.json", "control_sheet_walk"),
    ("topology_identity_signatures.json", "identity_signatures"),
    ("topology_storage_design.json", "topology_storage_design"),
    ("verdict.json", "verdict"),
)


def write(output: Path | None = None, *, limit: int | None = None) -> Path:
    target = Path(output or DEFAULT_OUTPUT)
    target.mkdir(parents=True, exist_ok=True)
    results = build_corpus(limit=limit)
    artifact = measure(results)
    first = hashlib.sha256(_canonical(artifact)).hexdigest()
    again = measure(results)
    second = hashlib.sha256(_canonical(again)).hexdigest()
    artifact["determinism"] = {"runs": 2, "identical": first == second, "sha256": first}
    for name, key in ARTIFACT_FILES:
        _write_json(target / name, artifact[key])
    _write_json(target / "determinism.json", artifact["determinism"])
    from .report import render_report

    (target / "report.md").write_text(render_report(artifact), encoding="utf-8")
    return target


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    output = Path(args[0]) if args else None
    print(write(output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
