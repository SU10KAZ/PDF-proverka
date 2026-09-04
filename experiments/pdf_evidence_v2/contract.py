"""The topology contract: closed vocabularies and the guards that keep them closed.

The whole package rests on one asymmetry inherited from V1 and one added here.

*Inherited*: a native-PDF producer may assert presence and may never assert
absence.  The words themselves (``REMOVED``, ``ABSENT``, …) are refused
anywhere in a produced value, because the danger was never a wrong enum member —
it was a well-meaning string reaching a consumer that then published 212 false
``REMOVED`` on a single sheet.

*Added*: **an intersection is not a connection**.  A schematic is dense with
strokes that cross and do not touch electrically; a producer that treats
overlap as connectivity manufactures a plausible, wrong, unfalsifiable graph.
So an edge of this graph carries a ``connection_claim`` and the guard
``assert_connection_evidence`` refuses ``PROVEN_CONNECTION`` on any edge that
does not name the drawn fact that proves it.

What the contract deliberately does not contain: confidence numbers, scores,
"likely" connections, and any rule whose input is distance alone.  ``UNKNOWN``
is a real answer here, used often and on purpose.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator, Mapping, Sequence

from experiments.pdf_evidence_v1.contract import (
    APPLICABILITY,
    FORBIDDEN_CLAIM_TERMS,
    ContractViolation,
)

SCHEMA_VERSION = "pdf-topology.v2"

# ---------------------------------------------------------------------------
# what a stroke is
# ---------------------------------------------------------------------------

#: The nature of one drawn edge.  Every value except ``SCHEMATIC_CONDUCTOR`` is
#: a reason to keep the edge out of the electrical graph; ``UNKNOWN`` is the
#: default and stays the default unless a positive rule fires.
SCHEMATIC_CONDUCTOR = "SCHEMATIC_CONDUCTOR"
TABLE_GRID = "TABLE_GRID"
FRAME = "FRAME"
DIMENSION_LINE = "DIMENSION_LINE"
TEXT_UNDERLINE = "TEXT_UNDERLINE"
LEADER_CALLOUT = "LEADER_CALLOUT"
DECORATIVE_LINE = "DECORATIVE_LINE"
UNKNOWN = "UNKNOWN"
LINE_NATURE = (
    SCHEMATIC_CONDUCTOR,
    TABLE_GRID,
    FRAME,
    DIMENSION_LINE,
    TEXT_UNDERLINE,
    LEADER_CALLOUT,
    DECORATIVE_LINE,
    UNKNOWN,
)

#: How a stroke chain was welded.  A dashed conductor is a conductor, but the
#: chaining that recovers it can also fuse two unrelated collinear strokes, so
#: the two cases never share a value.
SOLID = "SOLID"
PROVEN_DASHED = "PROVEN_DASHED"
STROKE_PATTERN = (SOLID, PROVEN_DASHED)

# ---------------------------------------------------------------------------
# what a crossing is
# ---------------------------------------------------------------------------

#: The verdict on one place where two edges meet or cross.  Only the first
#: value is a connection.  ``HOP_PROVEN_NON_CONNECTION`` is the drawing's own
#: statement: the semicircular jump a draughtsman puts where a wire passes over
#: another without touching it.
CONNECTED_JUNCTION = "CONNECTED_JUNCTION"
CROSSING_WITHOUT_JUNCTION = "CROSSING_WITHOUT_JUNCTION"
HOP_PROVEN_NON_CONNECTION = "HOP_PROVEN_NON_CONNECTION"
CROSSING_VERDICT = (
    CONNECTED_JUNCTION,
    CROSSING_WITHOUT_JUNCTION,
    HOP_PROVEN_NON_CONNECTION,
    UNKNOWN,
)

#: The drawn fact that proves a junction.  Each of these is something a
#: draughtsman put on the sheet on purpose; none of them is proximity.
JUNCTION_DOT = "JUNCTION_DOT"
COINCIDENT_ENDPOINTS = "COINCIDENT_ENDPOINTS"
TEE_TERMINATION = "TEE_TERMINATION"
CONTINUOUS_POLYLINE = "CONTINUOUS_POLYLINE"
EQUIPMENT_PORT = "EQUIPMENT_PORT"
JUNCTION_EVIDENCE = (
    JUNCTION_DOT,
    COINCIDENT_ENDPOINTS,
    TEE_TERMINATION,
    CONTINUOUS_POLYLINE,
    EQUIPMENT_PORT,
)

# ---------------------------------------------------------------------------
# what a node is
# ---------------------------------------------------------------------------

BUS = "BUS"
FEEDER = "FEEDER"
EQUIPMENT = "EQUIPMENT"
TERMINAL = "TERMINAL"
CONNECTOR = "CONNECTOR"
JUNCTION = "JUNCTION"
LABEL_ANCHOR = "LABEL_ANCHOR"
TABLE_PORT = "TABLE_PORT"
NODE_KIND = (
    BUS,
    FEEDER,
    EQUIPMENT,
    TERMINAL,
    CONNECTOR,
    JUNCTION,
    LABEL_ANCHOR,
    TABLE_PORT,
    UNKNOWN,
)

ELECTRICAL_CONNECTION = "ELECTRICAL_CONNECTION"
FEED = "FEED"
BRANCH = "BRANCH"
LABEL_CONNECTION = "LABEL_CONNECTION"
TABLE_REFERENCE = "TABLE_REFERENCE"
EDGE_KIND = (
    ELECTRICAL_CONNECTION,
    FEED,
    BRANCH,
    JUNCTION,
    LABEL_CONNECTION,
    TABLE_REFERENCE,
    UNKNOWN,
)

#: Whether the edge asserts an electrical connection at all.  An edge with
#: ``NO_CLAIM`` is recorded geometry, not a statement about the installation;
#: the metrics count the two separately and the guard keeps them apart.
PROVEN_CONNECTION = "PROVEN_CONNECTION"
NO_CLAIM = "NO_CLAIM"
CONNECTION_CLAIM = (PROVEN_CONNECTION, NO_CLAIM)

#: Direction.  ``UNDIRECTED`` means the connection is proven and its direction
#: is not; it is not a weaker form of ``PROVEN``, it is a different statement.
PROVEN = "PROVEN"
UNDIRECTED = "UNDIRECTED"
DIRECTION_STATUS = (PROVEN, UNDIRECTED, UNKNOWN)

#: The only facts that may prove a direction.  A textual keyword is absent from
#: this tuple on purpose and the reason is measured in ``direction.py``: on this
#: corpus the most tempting keyword (``Ввод``) names the direction relative to
#: the *far* switchboard and would invert every outgoing feeder of the sheet.
ARROWHEAD = "ARROWHEAD"
DIRECTION_EVIDENCE = (ARROWHEAD,)

#: How a label was bound to a node.  Three drawn relations, inherited from V1's
#: ownership channels, re-aimed from a region at a single conductor.  Proximity
#: is not among them.
RUNS_ALONG_SINGLE_CONDUCTOR = "RUNS_ALONG_SINGLE_CONDUCTOR"
INSIDE_SINGLE_SYMBOL_BOX = "INSIDE_SINGLE_SYMBOL_BOX"
INSIDE_SINGLE_TABLE_CELL = "INSIDE_SINGLE_TABLE_CELL"
BINDING_CHANNEL = (
    RUNS_ALONG_SINGLE_CONDUCTOR,
    INSIDE_SINGLE_SYMBOL_BOX,
    INSIDE_SINGLE_TABLE_CELL,
)
#: Outcomes of an attempted binding, including the two ways it may fail.
BOUND = "BOUND"
AMBIGUOUS = "AMBIGUOUS"
UNBOUND = "UNBOUND"
BINDING_STATUS = (BOUND, AMBIGUOUS, UNBOUND)

#: Ownership status of a node — whether anything on the sheet names it.
NAMED = "NAMED"
UNNAMED = "UNNAMED"
OWNERSHIP_STATUS = (NAMED, UNNAMED, AMBIGUOUS)


# ---------------------------------------------------------------------------
# the records
# ---------------------------------------------------------------------------


@dataclass
class TopologyNode:
    """One thing the drawing shows, with the geometry that shows it."""

    node_id: str
    document: str
    physical_page: int
    node_kind: str
    bbox: tuple[float, float, float, float]
    anchor: tuple[float, float]
    region_id: str | None = None
    island_id: str | None = None
    symbol_signature: str | None = None
    ownership_status: str = UNNAMED
    labels: tuple[str, ...] = ()
    function_id: str | None = None
    fragment_id: str | None = None
    evidence_refs: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "document": self.document,
            "physical_page": self.physical_page,
            "node_kind": self.node_kind,
            "bbox": [round(float(value), 2) for value in self.bbox],
            "anchor": [round(float(value), 2) for value in self.anchor],
            "region_id": self.region_id,
            "island_id": self.island_id,
            "symbol_signature": self.symbol_signature,
            "ownership_status": self.ownership_status,
            "labels": list(self.labels),
            "function_id": self.function_id,
            "fragment_id": self.fragment_id,
            "evidence_refs": list(self.evidence_refs),
            "notes": list(self.notes),
        }


@dataclass
class TopologyEdge:
    """One relation between two nodes, and the drawn fact that proves it."""

    edge_id: str
    document: str
    physical_page: int
    from_node_id: str
    to_node_id: str
    edge_kind: str
    connection_claim: str
    direction_status: str
    junction_evidence: str | None = None
    direction_evidence: str | None = None
    binding_channel: str | None = None
    geometry_refs: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "edge_id": self.edge_id,
            "document": self.document,
            "physical_page": self.physical_page,
            "from_node_id": self.from_node_id,
            "to_node_id": self.to_node_id,
            "edge_kind": self.edge_kind,
            "connection_claim": self.connection_claim,
            "direction_status": self.direction_status,
            "junction_evidence": self.junction_evidence,
            "direction_evidence": self.direction_evidence,
            "binding_channel": self.binding_channel,
            "geometry_refs": list(self.geometry_refs),
            "evidence_refs": list(self.evidence_refs),
            "notes": list(self.notes),
        }


# ---------------------------------------------------------------------------
# guards
# ---------------------------------------------------------------------------


def _strings(value: Any) -> Iterator[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for key, item in value.items():
            yield str(key)
            yield from _strings(item)
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            yield from _strings(item)


#: The one path allowed to name a forbidden term: this module's own listing of
#: what is forbidden.  A guard that reads its own schema as data denounces
#: itself, and the exception is a single literal rather than a pattern.
_VOCABULARY_EXEMPTION = "$.contract.vocabularies.forbidden_claim_terms"


def assert_no_absence_vocabulary(payload: Any, *, exempt: Sequence[str] = ()) -> None:
    """No produced value may carry a word that states absence."""
    allowed = set(exempt) | {_VOCABULARY_EXEMPTION}
    if isinstance(payload, Mapping) and payload.get("__exempt__") in allowed:
        return
    for text in _strings(payload):
        upper = text.upper()
        for term in FORBIDDEN_CLAIM_TERMS:
            if term in upper and text not in allowed:
                raise ContractViolation(f"absence vocabulary in a produced value: {text!r}")


def assert_closed_vocabularies(
    nodes: Sequence[TopologyNode], edges: Sequence[TopologyEdge]
) -> None:
    for node in nodes:
        if node.node_kind not in NODE_KIND:
            raise ContractViolation(f"node kind outside the vocabulary: {node.node_kind}")
        if node.ownership_status not in OWNERSHIP_STATUS:
            raise ContractViolation(f"ownership outside the vocabulary: {node.ownership_status}")
    for edge in edges:
        if edge.edge_kind not in EDGE_KIND:
            raise ContractViolation(f"edge kind outside the vocabulary: {edge.edge_kind}")
        if edge.connection_claim not in CONNECTION_CLAIM:
            raise ContractViolation(f"claim outside the vocabulary: {edge.connection_claim}")
        if edge.direction_status not in DIRECTION_STATUS:
            raise ContractViolation(f"direction outside the vocabulary: {edge.direction_status}")


def assert_connection_evidence(edges: Sequence[TopologyEdge]) -> None:
    """An asserted connection must name the drawn fact that proves it."""
    for edge in edges:
        if edge.connection_claim != PROVEN_CONNECTION:
            continue
        if edge.edge_kind in {ELECTRICAL_CONNECTION, FEED, BRANCH, JUNCTION}:
            if edge.junction_evidence not in JUNCTION_EVIDENCE:
                raise ContractViolation(
                    f"{edge.edge_id} asserts a connection without junction evidence"
                )
        elif edge.edge_kind == LABEL_CONNECTION:
            if edge.binding_channel not in BINDING_CHANNEL:
                raise ContractViolation(
                    f"{edge.edge_id} asserts a binding outside the channel vocabulary"
                )
        elif edge.edge_kind == TABLE_REFERENCE:
            if not edge.geometry_refs:
                raise ContractViolation(f"{edge.edge_id} asserts a table reference without geometry")
        else:
            raise ContractViolation(f"{edge.edge_id} asserts a connection it may not assert")
        if not edge.geometry_refs:
            raise ContractViolation(f"{edge.edge_id} asserts a connection without geometry")


def assert_direction_evidence(edges: Sequence[TopologyEdge]) -> None:
    """A proven direction must name an arrowhead; nothing else may prove one."""
    for edge in edges:
        if edge.direction_status == PROVEN:
            if edge.direction_evidence not in DIRECTION_EVIDENCE:
                raise ContractViolation(f"{edge.edge_id} claims a direction without an arrowhead")
            if edge.connection_claim != PROVEN_CONNECTION:
                raise ContractViolation(f"{edge.edge_id} directs a connection it has not proven")
        elif edge.direction_evidence is not None:
            raise ContractViolation(f"{edge.edge_id} carries direction evidence it does not use")


def assert_no_page_spanning_edges(
    nodes: Sequence[TopologyNode], edges: Sequence[TopologyEdge]
) -> None:
    """A drawn connection cannot leave the sheet it is drawn on."""
    page_of = {node.node_id: node.physical_page for node in nodes}
    document_of = {node.node_id: node.document for node in nodes}
    for edge in edges:
        for endpoint in (edge.from_node_id, edge.to_node_id):
            if endpoint not in page_of:
                raise ContractViolation(f"{edge.edge_id} names a node that does not exist")
        if page_of[edge.from_node_id] != page_of[edge.to_node_id]:
            raise ContractViolation(f"{edge.edge_id} spans two physical pages")
        if document_of[edge.from_node_id] != document_of[edge.to_node_id]:
            raise ContractViolation(f"{edge.edge_id} spans two documents")


def assert_single_ownership(edges: Sequence[TopologyEdge]) -> None:
    """A label binds to at most one node."""
    seen: dict[str, str] = {}
    for edge in edges:
        if edge.edge_kind != LABEL_CONNECTION or edge.connection_claim != PROVEN_CONNECTION:
            continue
        previous = seen.get(edge.from_node_id)
        if previous is not None and previous != edge.to_node_id:
            raise ContractViolation(f"label {edge.from_node_id} bound to two nodes")
        seen[edge.from_node_id] = edge.to_node_id


def contract_document() -> dict[str, Any]:
    """The contract as an artifact, exempted from its own vocabulary guard."""
    return {
        "__exempt__": _VOCABULARY_EXEMPTION,
        "schema_version": SCHEMA_VERSION,
        "kind": "pdf_topology_contract",
        "model_calls": 0,
        "inherits": "pdf-evidence.v1",
        "rules": [
            "a producer may assert what the sheet shows and never what it does not",
            "an intersection is not a connection",
            "a connection must name the drawn fact that proves it",
            "a direction must name an arrowhead",
            "proximity proves nothing, at any stage",
            "an unclassified stroke stays UNKNOWN",
        ],
        "vocabularies": {
            "line_nature": list(LINE_NATURE),
            "stroke_pattern": list(STROKE_PATTERN),
            "crossing_verdict": list(CROSSING_VERDICT),
            "junction_evidence": list(JUNCTION_EVIDENCE),
            "node_kind": list(NODE_KIND),
            "edge_kind": list(EDGE_KIND),
            "connection_claim": list(CONNECTION_CLAIM),
            "direction_status": list(DIRECTION_STATUS),
            "direction_evidence": list(DIRECTION_EVIDENCE),
            "binding_channel": list(BINDING_CHANNEL),
            "binding_status": list(BINDING_STATUS),
            "ownership_status": list(OWNERSHIP_STATUS),
            "applicability": list(APPLICABILITY),
            "forbidden_claim_terms": list(FORBIDDEN_CLAIM_TERMS),
        },
    }


__all__ = [name for name in dir() if not name.startswith("_")]
