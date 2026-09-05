"""The function-topology contract: what may be aggregated, and on what proof.

Three rules, inherited or added, and each one enforced by a guard rather than
by a convention.

*Inherited from V1* — a native-PDF producer may assert what the sheet shows and
may never assert what it does not.  The words themselves are refused anywhere in
a produced value.

*Inherited from V2* — an intersection is not a connection.  This package never
creates connectivity; it only reads the connectivity V2 proved.

*Added here* — **aggregation needs a drawn reason.**  Two branches belong to one
function because the sheet joined them, not because they sit on the same page,
not because a similar string is printed near both.  So a subgraph carries an
``aggregation_channel`` and ``assert_aggregation_evidence`` refuses any subgraph
whose members are not connected to each other by the very edges it lists.

And one prohibition that has its own guard because it is the failure this whole
research line was built to avoid: **a label may name a group, never create
one.**  ``assert_label_never_aggregates`` refuses a subgraph whose owner marks
reach a node outside it.

What the contract does not contain, on purpose: similarity, confidence,
distance, rank and score.  ``assert_no_similarity_evidence`` walks the produced
payload and refuses a key that smells of any of them, because the danger is not
a wrong enum member — it is a plausible number that a later consumer reads as
proof.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator, Mapping, Sequence

from experiments.pdf_evidence_v1.contract import (
    APPLICABILITY,
    FORBIDDEN_CLAIM_TERMS,
    ContractViolation,
)
from experiments.pdf_evidence_v2.contract import (
    BUS,
    CONNECTOR,
    EQUIPMENT,
    FEEDER,
    JUNCTION,
    LABEL_ANCHOR,
    TABLE_PORT,
    TERMINAL,
)

SCHEMA_VERSION = "function-topology.v1"

# ---------------------------------------------------------------------------
# how a group came to be a group
# ---------------------------------------------------------------------------

#: The maximal set of nodes joined to each other by proven connections — V2's
#: island.  Its boundary is closed by the graph itself: control F of V2 measured
#: zero proven edges between two islands, so nothing drawn crosses it.
CONNECTED_COMPONENT = "CONNECTED_COMPONENT"
#: Reported for measurement, never used as the unit.  The feeders that reach one
#: proven bus without passing through another.  §5 of the track asks whether
#: this can be the base unit of one board; the answer is measured, not assumed.
BUS_ANCHORED_GROUP = "BUS_ANCHORED_GROUP"
AGGREGATION_CHANNEL = (CONNECTED_COMPONENT, BUS_ANCHORED_GROUP)

#: How well the extent of a subgraph is proven.
#:
#: ``PROVEN``    — a maximal connected component that contains a proven bus: a
#:                 closed drawn thing with a distribution point in it.
#: ``PARTIAL``   — a maximal connected component with no bus: closed, drawn, but
#:                 no proven point of distribution, so it is a piece of an
#:                 installation rather than a board.
#: ``AMBIGUOUS`` — the extent is closed, but two independent naming families are
#:                 each exclusive to it and never co-printed: the sheet may have
#:                 welded two entities, and this layer will not choose.
#: ``UNKNOWN``   — a component too small to be anything and named by nothing.
PROVEN = "PROVEN"
PARTIAL = "PARTIAL"
AMBIGUOUS = "AMBIGUOUS"
UNKNOWN = "UNKNOWN"
BOUNDARY_STATUS = (PROVEN, PARTIAL, AMBIGUOUS, UNKNOWN)

# ---------------------------------------------------------------------------
# what a printed mark is allowed to be
# ---------------------------------------------------------------------------

#: Every node the mark is bound to lies inside one subgraph.  The mark names
#: that subgraph.  It did not build it.
COMMON_OWNER_LABEL = "COMMON_OWNER_LABEL"
#: The mark's bound nodes lie in two or more subgraphs.  It may be a sheet
#: convention, a cross-reference or a series name; it cannot own anything, and
#: above all it may not join the subgraphs it appears in.
REPEATED_LABEL_ACROSS_SUBGRAPHS = "REPEATED_LABEL_ACROSS_SUBGRAPHS"
#: Printed on the sheet, bound to no conductor at all.  A sheet-scoped presence:
#: true about the sheet, and silent about which wire it belongs to.
SHEET_SCOPED_LABEL = "SHEET_SCOPED_LABEL"
LABEL_OWNERSHIP = (
    COMMON_OWNER_LABEL,
    REPEATED_LABEL_ACROSS_SUBGRAPHS,
    SHEET_SCOPED_LABEL,
)

# ---------------------------------------------------------------------------
# how a FunctionScope is bound to a subgraph
# ---------------------------------------------------------------------------

#: The scope's own printed mark runs along a member conductor, or sits inside a
#: member symbol box.  V2's drawn-relation channels, unchanged.
MARK_BOUND_TO_MEMBER_NODE = "MARK_BOUND_TO_MEMBER_NODE"
#: The mark is printed on the sheet and the sheet carries exactly one subgraph
#: whose extent is PROVEN.  This is uniqueness, not proximity: a sheet-scoped
#: assertion with exactly one candidate owner on the sheet.  It is never enough
#: for PROVEN_BINDING, and the guard enforces that.
SHEET_MARK_WITH_ONE_PROVEN_SUBGRAPH = "SHEET_MARK_WITH_ONE_PROVEN_SUBGRAPH"
BINDING_CHANNEL = (MARK_BOUND_TO_MEMBER_NODE, SHEET_MARK_WITH_ONE_PROVEN_SUBGRAPH)
#: Only this channel may carry a proven binding.
PROVING_BINDING_CHANNELS = (MARK_BOUND_TO_MEMBER_NODE,)

PROVEN_BINDING = "PROVEN_BINDING"
PARTIAL_BINDING = "PARTIAL_BINDING"
AMBIGUOUS_BINDING = "AMBIGUOUS_BINDING"
#: This layer asserts no pairing here.  It is a statement about the producer and
#: never about the installation: a scope with ``NO_BINDING`` is not a scope whose
#: function is missing from the drawing, and §12 of the track forbids reading it
#: that way.
NO_BINDING = "NO_BINDING"
BINDING_STATUS = (
    PROVEN_BINDING,
    PARTIAL_BINDING,
    AMBIGUOUS_BINDING,
    NO_BINDING,
    UNKNOWN,
)

#: Why a scope reached no proven binding.  Every value names a mechanism that
#: can be checked in the artifact; none of them is "probably".
NO_VECTOR_LAYER = "NO_VECTOR_LAYER"
NO_SCHEMA_PAGE = "NO_SCHEMA_PAGE"
NO_PROVEN_AGGREGATE_ON_THE_SHEET = "NO_PROVEN_AGGREGATE_ON_THE_SHEET"
SCOPE_HAS_NO_PRINTED_MARK = "SCOPE_HAS_NO_PRINTED_MARK"
MARK_NOT_ON_A_CONDUCTOR = "MARK_NOT_ON_A_CONDUCTOR"
FUNCTION_GRANULARITY_MISMATCH = "FUNCTION_GRANULARITY_MISMATCH"
TEXT_REPRESENTATION_ONLY = "TEXT_REPRESENTATION_ONLY"
SEVERAL_SUBGRAPHS_CARRY_THE_MARK = "SEVERAL_SUBGRAPHS_CARRY_THE_MARK"
BINDING_CAUSE = (
    NO_VECTOR_LAYER,
    NO_SCHEMA_PAGE,
    NO_PROVEN_AGGREGATE_ON_THE_SHEET,
    SCOPE_HAS_NO_PRINTED_MARK,
    MARK_NOT_ON_A_CONDUCTOR,
    FUNCTION_GRANULARITY_MISMATCH,
    TEXT_REPRESENTATION_ONLY,
    SEVERAL_SUBGRAPHS_CARRY_THE_MARK,
    UNKNOWN,
)

#: Signature tiers.  Four, because the track asks two different questions — does
#: identity survive a re-layout (§10) and does it separate two functions of the
#: same class (§16) — and one number cannot answer both.  The tiers are nested:
#: each adds one kind of ingredient to the one before it.
SHAPE_ONLY = "SHAPE_ONLY"
SHAPE_AND_DEVICES = "SHAPE_AND_DEVICES"
SHAPE_AND_NAMES = "SHAPE_AND_NAMES"
SHAPE_AND_CONSUMERS = "SHAPE_AND_CONSUMERS"
SIGNATURE_TIERS = (SHAPE_ONLY, SHAPE_AND_DEVICES, SHAPE_AND_NAMES, SHAPE_AND_CONSUMERS)

#: Which side of a lineage task carries a topology graph at all.  §19.
BOTH_SIDES_ON_TOPOLOGY = "BOTH_SIDES_ON_TOPOLOGY"
LEFT_ONLY_ON_TOPOLOGY = "LEFT_ONLY_ON_TOPOLOGY"
RIGHT_ONLY_ON_TOPOLOGY = "RIGHT_ONLY_ON_TOPOLOGY"
NEITHER_SIDE_ON_TOPOLOGY = "NEITHER_SIDE_ON_TOPOLOGY"
REPRESENTATION_CLASS = (
    BOTH_SIDES_ON_TOPOLOGY,
    LEFT_ONLY_ON_TOPOLOGY,
    RIGHT_ONLY_ON_TOPOLOGY,
    NEITHER_SIDE_ON_TOPOLOGY,
)

#: Node kinds that count as a drawn port of a subgraph.
PORT_KINDS = (TERMINAL, TABLE_PORT)


# ---------------------------------------------------------------------------
# the records
# ---------------------------------------------------------------------------


def stable_id(prefix: str, payload: Any) -> str:
    """A deterministic address.  An address, not an identity — identity is the
    signature, and the guard for that lives in ``signature.py``."""
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return f"{prefix}_{hashlib.sha256(blob.encode('utf-8')).hexdigest()[:20]}"


@dataclass
class FunctionTopologySubgraph:
    """One aggregate of the low-level graph, with the proof of its extent."""

    subgraph_id: str
    document: str
    physical_page: int
    aggregation_channel: str
    boundary_status: str
    member_node_ids: tuple[str, ...] = ()
    member_edge_ids: tuple[str, ...] = ()
    bus_node_ids: tuple[str, ...] = ()
    feeder_node_ids: tuple[str, ...] = ()
    equipment_node_ids: tuple[str, ...] = ()
    terminal_node_ids: tuple[str, ...] = ()
    source_region_ids: tuple[str, ...] = ()
    label_evidence_ids: tuple[str, ...] = ()
    function_marks: tuple[str, ...] = ()
    consumer_labels: tuple[str, ...] = ()
    source_labels: tuple[str, ...] = ()
    topology_signature: str | None = None
    function_scope_id: str | None = None
    function_fragment_id: str | None = None
    evidence_refs: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "subgraph_id": self.subgraph_id,
            "document": self.document,
            "physical_page": self.physical_page,
            "aggregation_channel": self.aggregation_channel,
            "boundary_status": self.boundary_status,
            "member_node_ids": list(self.member_node_ids),
            "member_node_count": len(self.member_node_ids),
            "member_edge_ids": list(self.member_edge_ids),
            "member_edge_count": len(self.member_edge_ids),
            "bus_node_ids": list(self.bus_node_ids),
            "feeder_node_ids": list(self.feeder_node_ids),
            "equipment_node_ids": list(self.equipment_node_ids),
            "terminal_node_ids": list(self.terminal_node_ids),
            "source_region_ids": list(self.source_region_ids),
            "label_evidence_ids": list(self.label_evidence_ids),
            "function_marks": list(self.function_marks),
            "consumer_labels": list(self.consumer_labels),
            "source_labels": list(self.source_labels),
            "topology_signature": self.topology_signature,
            "function_scope_id": self.function_scope_id,
            "function_fragment_id": self.function_fragment_id,
            "evidence_refs": list(self.evidence_refs),
            "notes": list(self.notes),
        }


@dataclass
class ScopeBinding:
    """One (FunctionScope, subgraph) pairing, or the reasoned absence of one."""

    binding_id: str
    pair_id: str
    project: str
    side: str
    scope_id: str
    function_id: str | None
    fragment_id: str | None
    physical_page: int | None
    primary_mark: str | None
    binding_status: str
    binding_channel: str | None = None
    cause: str = UNKNOWN
    subgraph_id: str | None = None
    candidate_subgraph_ids: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "binding_id": self.binding_id,
            "pair_id": self.pair_id,
            "project": self.project,
            "side": self.side,
            "scope_id": self.scope_id,
            "function_id": self.function_id,
            "fragment_id": self.fragment_id,
            "physical_page": self.physical_page,
            "primary_mark": self.primary_mark,
            "binding_status": self.binding_status,
            "binding_channel": self.binding_channel,
            "cause": self.cause,
            "subgraph_id": self.subgraph_id,
            "candidate_subgraph_ids": list(self.candidate_subgraph_ids),
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


_VOCABULARY_EXEMPTION = "$.function_topology.vocabularies.forbidden_claim_terms"


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


#: Keys whose very presence would smuggle resemblance back in.  ``*_count`` and
#: ``*_id`` are safe; anything that ranks or scores is not.
_SIMILARITY_KEY = re.compile(
    r"(?:^|_)(score|scores|similarity|confidence|probability|likelihood|rank|"
    r"ranking|distance|proximity|closeness|weight|threshold)(?:$|_)",
    re.IGNORECASE,
)
#: The one place a forbidden key name may appear: this contract's own listing of
#: what it forbids, and the audit's statement that it found none.  The exemption
#: is a literal path or the ``__exempt__`` marker, never a pattern — a guard that
#: can be talked out of firing by a clever key name is not a guard.
_SIMILARITY_EXEMPT_PATHS = (
    "$.prohibitions",
    "$.vocabularies",
    "$.safety",
    "$.controls",
)


def assert_no_similarity_evidence(payload: Any, *, path: str = "$") -> None:
    """Refuse a produced key that ranks, scores or measures resemblance."""
    if any(path.startswith(prefix) for prefix in _SIMILARITY_EXEMPT_PATHS):
        return
    if path == "$" and isinstance(payload, Mapping) and payload.get("__exempt__"):
        # the contract describing itself; its own vocabulary listing is data
        payload = {
            key: value for key, value in payload.items()
            if key not in {"prohibitions", "vocabularies"}
        }
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            name = str(key)
            if _SIMILARITY_KEY.search(name):
                raise ContractViolation(f"resemblance key in a produced value: {path}.{name}")
            assert_no_similarity_evidence(value, path=f"{path}.{name}")
    elif isinstance(payload, (list, tuple)):
        for index, value in enumerate(payload):
            assert_no_similarity_evidence(value, path=f"{path}[{index}]")


def assert_closed_vocabularies(
    subgraphs: Sequence[FunctionTopologySubgraph],
    bindings: Sequence[ScopeBinding],
) -> None:
    for subgraph in subgraphs:
        if subgraph.aggregation_channel not in AGGREGATION_CHANNEL:
            raise ContractViolation(
                f"aggregation channel outside the vocabulary: {subgraph.aggregation_channel}"
            )
        if subgraph.boundary_status not in BOUNDARY_STATUS:
            raise ContractViolation(
                f"boundary status outside the vocabulary: {subgraph.boundary_status}"
            )
    for binding in bindings:
        if binding.binding_status not in BINDING_STATUS:
            raise ContractViolation(
                f"binding status outside the vocabulary: {binding.binding_status}"
            )
        if binding.binding_channel is not None and binding.binding_channel not in BINDING_CHANNEL:
            raise ContractViolation(
                f"binding channel outside the vocabulary: {binding.binding_channel}"
            )
        if binding.cause not in BINDING_CAUSE:
            raise ContractViolation(f"cause outside the vocabulary: {binding.cause}")


def assert_aggregation_evidence(
    subgraphs: Sequence[FunctionTopologySubgraph],
    edges_by_id: Mapping[str, tuple[str, str]],
) -> None:
    """A group must be held together by the very edges it lists.

    This is the guard that makes "same page" unusable as a reason: a subgraph
    whose members are not connected to each other through its own listed edges
    is refused, whatever else is true about them.
    """
    for subgraph in subgraphs:
        members = set(subgraph.member_node_ids)
        if not members:
            raise ContractViolation(f"{subgraph.subgraph_id} aggregates nothing")
        parent = {node_id: node_id for node_id in members}

        def find(item: str) -> str:
            while parent[item] != item:
                parent[item] = parent[parent[item]]
                item = parent[item]
            return item

        for edge_id in subgraph.member_edge_ids:
            endpoints = edges_by_id.get(edge_id)
            if endpoints is None:
                raise ContractViolation(f"{subgraph.subgraph_id} lists an edge that does not exist")
            left, right = endpoints
            if left not in members or right not in members:
                raise ContractViolation(
                    f"{subgraph.subgraph_id} lists an edge leaving its own members"
                )
            a, b = find(left), find(right)
            if a != b:
                parent[a] = b
        roots = {find(node_id) for node_id in members}
        if len(roots) != 1:
            raise ContractViolation(
                f"{subgraph.subgraph_id} is not held together by its own edges "
                f"({len(roots)} pieces)"
            )


def assert_label_never_aggregates(
    subgraphs: Sequence[FunctionTopologySubgraph],
    subgraph_of_node: Mapping[str, str],
    nodes_of_mark: Mapping[str, Sequence[str]],
) -> None:
    """An owner mark may not reach a node outside the subgraph it names."""
    for subgraph in subgraphs:
        for mark in subgraph.function_marks:
            outside = [
                node_id for node_id in nodes_of_mark.get(mark, ())
                if subgraph_of_node.get(node_id) not in {None, subgraph.subgraph_id}
            ]
            if outside:
                raise ContractViolation(
                    f"{subgraph.subgraph_id} claims owner mark {mark!r} that also names "
                    f"{len(outside)} node(s) elsewhere"
                )


def assert_single_page_membership(
    subgraphs: Sequence[FunctionTopologySubgraph],
    page_of_node: Mapping[str, int],
    document_of_node: Mapping[str, str],
) -> None:
    """A drawn aggregate cannot leave the sheet it is drawn on."""
    for subgraph in subgraphs:
        for node_id in subgraph.member_node_ids:
            if node_id not in page_of_node:
                raise ContractViolation(f"{subgraph.subgraph_id} names a node that does not exist")
            if page_of_node[node_id] != subgraph.physical_page:
                raise ContractViolation(f"{subgraph.subgraph_id} spans two physical pages")
            if document_of_node[node_id] != subgraph.document:
                raise ContractViolation(f"{subgraph.subgraph_id} spans two documents")


def assert_binding_evidence(bindings: Sequence[ScopeBinding]) -> None:
    """A proven binding names a drawn relation; a sheet-scoped one may not."""
    for binding in bindings:
        if binding.binding_status == PROVEN_BINDING:
            if binding.binding_channel not in PROVING_BINDING_CHANNELS:
                raise ContractViolation(
                    f"{binding.binding_id} proves a binding on a channel that may not prove one"
                )
            if not binding.subgraph_id or not binding.evidence_refs:
                raise ContractViolation(f"{binding.binding_id} proves a binding without evidence")
        elif binding.binding_status == PARTIAL_BINDING:
            if binding.binding_channel not in BINDING_CHANNEL:
                raise ContractViolation(f"{binding.binding_id} binds outside the channels")
            if not binding.subgraph_id:
                raise ContractViolation(f"{binding.binding_id} binds to nothing")
        elif binding.binding_status == AMBIGUOUS_BINDING:
            if len(binding.candidate_subgraph_ids) < 2:
                raise ContractViolation(
                    f"{binding.binding_id} is ambiguous between fewer than two subgraphs"
                )
            if binding.subgraph_id is not None:
                raise ContractViolation(f"{binding.binding_id} is ambiguous yet chose one")
        else:
            if binding.subgraph_id is not None:
                raise ContractViolation(
                    f"{binding.binding_id} asserts no binding yet names a subgraph"
                )


def contract_document() -> dict[str, Any]:
    """The contract as an artifact, exempted from its own vocabulary guard."""
    return {
        "__exempt__": _VOCABULARY_EXEMPTION,
        "schema_version": SCHEMA_VERSION,
        "kind": "function_topology_contract",
        "model_calls": 0,
        "inherits": ["pdf-evidence.v1", "pdf-topology.v2"],
        "rules": [
            "the low-level topology of V2 is a frozen reference and is never widened",
            "aggregation needs a drawn reason: members must be held together by "
            "the edges the subgraph lists",
            "a printed mark may name a group and may never create one",
            "one physical page is never a reason to group anything",
            "a sheet-scoped mark may support a binding and may never prove one",
            "an unbound scope asserts nothing about the installation",
        ],
        "prohibitions": {
            "resemblance_keys_refused": _SIMILARITY_KEY.pattern,
            "page_proximity_as_evidence": False,
            "nearest_label_ownership": False,
            "a_topology_gap_read_as_a_contradiction": False,
        },
        "vocabularies": {
            "aggregation_channel": list(AGGREGATION_CHANNEL),
            "boundary_status": list(BOUNDARY_STATUS),
            "label_ownership": list(LABEL_OWNERSHIP),
            "binding_channel": list(BINDING_CHANNEL),
            "proving_binding_channels": list(PROVING_BINDING_CHANNELS),
            "binding_status": list(BINDING_STATUS),
            "binding_cause": list(BINDING_CAUSE),
            "signature_tiers": list(SIGNATURE_TIERS),
            "representation_class": list(REPRESENTATION_CLASS),
            "applicability": list(APPLICABILITY),
            "forbidden_claim_terms": list(FORBIDDEN_CLAIM_TERMS),
        },
    }


__all__ = [name for name in dir() if not name.startswith("_")]
