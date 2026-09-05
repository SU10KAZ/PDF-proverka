"""The representation-bridge contract: what an assembly is, and on what proof.

Three tracks stand behind this one and none of their rules is relaxed here.

*From PDF EVIDENCE V1* — a native producer may assert what the sheet shows and
may never assert what it does not.  A representation that is missing on one side
is a gap in the producer, never a disagreement with the other side.

*From PDF EVIDENCE V2* — an intersection is not a connection.  This layer never
creates connectivity; it reads the connectivity V2 proved.

*From FUNCTION TOPOLOGY V1* — a printed label may **name** a drawn group and may
never **create** one, and aggregation needs a drawn reason.

What this track adds is one idea and one prohibition.

**An assembly is a drawn container, whichever representation drew it.**  Three
containers qualify, and each carries its own strength because the drawings prove
different things:

* ``PROVEN_CONNECTED_COMPONENT`` — V2's island: the boundary is proven by
  connectivity, so this container may support a *proven* membership;
* ``DRAWN_TABLE_LATTICE`` — a ruled grid that declares its own columns in its
  first row.  The rulings are drawn and the header is printed, so the grid is a
  table rather than a drawing that happens to have long lines;
* ``DRAWN_STROKE_GROUP`` — a connected component of strokes carrying printed
  text through V1's drawn relations (a leader along the label, or a closed box
  around it).  Drawn, bounded, and silent about whether its strokes conduct.

**A page is never a container.**  ``assert_assembly_is_a_drawn_container``
refuses an assembly that does not name the drawn thing it is, and
``assert_no_sheet_wide_assembly`` refuses one that swallows a page carrying more
than one of them.  "Same physical page", "nearest label", "most similar" and any
rank or score are refused structurally, by
``assert_no_similarity_evidence`` walking the produced payload — the danger is
never a wrong enum member, it is a plausible number a later consumer reads as
proof.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any, Iterator, Mapping, Sequence

from experiments.pdf_evidence_v1.contract import (
    APPLICABILITY,
    FORBIDDEN_CLAIM_TERMS,
    ContractViolation,
)

SCHEMA_VERSION = "function-representation-bridge.v1"

# ---------------------------------------------------------------------------
# how a document says a thing
# ---------------------------------------------------------------------------

SCHEMATIC = "SCHEMATIC"
TABLE = "TABLE"
TEXT = "TEXT"
MIXED = "MIXED"
REPRESENTATION_TYPE = (SCHEMATIC, TABLE, TEXT, MIXED)

# ---------------------------------------------------------------------------
# what drew the boundary of an assembly
# ---------------------------------------------------------------------------

#: V2's island, taken exactly as V2 left it.  Control F of that track measured
#: zero proven edges between two islands, so nothing drawn crosses the boundary.
PROVEN_CONNECTED_COMPONENT = "PROVEN_CONNECTED_COMPONENT"
#: A ruled grid whose first row prints a caption in a contiguous run of columns
#: starting at the first one.  A table declares its own columns; a riser diagram
#: drawn on a grid does not, and that is what separates them here.
DRAWN_TABLE_LATTICE = "DRAWN_TABLE_LATTICE"
#: A connected component of strokes that carries printed text through a drawn
#: relation of V1 — a leader running along the label, or a box containing it.
DRAWN_STROKE_GROUP = "DRAWN_STROKE_GROUP"
ASSEMBLY_CHANNEL = (
    PROVEN_CONNECTED_COMPONENT,
    DRAWN_TABLE_LATTICE,
    DRAWN_STROKE_GROUP,
)
#: Only this channel may carry a proven scope membership.
PROVING_ASSEMBLY_CHANNELS = (PROVEN_CONNECTED_COMPONENT,)

# ---------------------------------------------------------------------------
# what kind of engineering thing the drawing shows
# ---------------------------------------------------------------------------

#: A drawn distribution point: a schematic assembly holding a proven bus.
BOARD = "BOARD"
#: A schematic assembly with branches and no proven bus.
DISTRIBUTION_GROUP = "DISTRIBUTION_GROUP"
#: A table headed by a single caption cell: a titled parameter block about one
#: named thing.
PANEL = "PANEL"
#: A table declaring several columns: a schedule of many things.
SYSTEM_GROUP = "SYSTEM_GROUP"
#: Declared because the track names them, and never emitted: this corpus offers
#: no *structural* signal that separates a riser group or a pump group from any
#: other drawn group, and a keyword list over printed text would be a guess
#: wearing a vocabulary's clothes.  The audit reports both as measured zeros.
RISER_GROUP = "RISER_GROUP"
PUMP_GROUP = "PUMP_GROUP"
UNKNOWN = "UNKNOWN"
ASSEMBLY_KIND = (
    BOARD,
    PANEL,
    SYSTEM_GROUP,
    RISER_GROUP,
    PUMP_GROUP,
    DISTRIBUTION_GROUP,
    UNKNOWN,
)
#: Kinds this layer refuses to decide, with the reason attached to each.
UNDECIDABLE_KINDS = {
    RISER_GROUP: "no structural signal separates a riser group from any other drawn group",
    PUMP_GROUP: "no structural signal separates a pump group from any other drawn group",
}

# ---------------------------------------------------------------------------
# how well an assembly's own extent is drawn
# ---------------------------------------------------------------------------

PROVEN = "PROVEN"
PARTIAL = "PARTIAL"
AMBIGUOUS = "AMBIGUOUS"
MEMBERSHIP_STATUS = (PROVEN, PARTIAL, AMBIGUOUS, UNKNOWN)

# ---------------------------------------------------------------------------
# how a FunctionScope joins an assembly
# ---------------------------------------------------------------------------

#: The frozen topology binding: the scope's own printed mark runs along a member
#: conductor or sits inside a member symbol box.  Unchanged from V1 of the
#: topology track, and the only channel allowed to prove.
PROVEN_TOPOLOGY_OWNERSHIP = "PROVEN_TOPOLOGY_OWNERSHIP"
#: A documented value of the passport is printed, literally, inside exactly one
#: drawn container of the function's own page.  Containment is drawn; what the
#: containment means for the whole function is not, so this channel is capped.
DOCUMENTED_VALUE_IN_ONE_ASSEMBLY = "DOCUMENTED_VALUE_IN_ONE_ASSEMBLY"
#: The scope's mark is printed on the sheet and the sheet draws exactly one
#: assembly with a proven extent.  Uniqueness, not proximity — and capped.
SHEET_MARK_WITH_ONE_ASSEMBLY = "SHEET_MARK_WITH_ONE_ASSEMBLY"
MEMBERSHIP_CHANNEL = (
    PROVEN_TOPOLOGY_OWNERSHIP,
    DOCUMENTED_VALUE_IN_ONE_ASSEMBLY,
    SHEET_MARK_WITH_ONE_ASSEMBLY,
)
PROVING_MEMBERSHIP_CHANNELS = (PROVEN_TOPOLOGY_OWNERSHIP,)

#: Channels named so that the artifact can say what it refuses, and so that a
#: later reader cannot mistake their absence for an oversight.
REFUSED_MEMBERSHIP_CHANNELS = (
    "SAME_PHYSICAL_PAGE",
    "NEAREST_PRINTED_LABEL",
    "MOST_SIMILAR_STRING",
    "HIGHEST_RANKED_CANDIDATE",
)

#: Why a scope reached no assembly.  Every value names a mechanism visible in
#: the artifact; none of them is "probably".
NO_VECTOR_LAYER = "NO_VECTOR_LAYER"
NO_ASSEMBLY_ON_THE_SHEET = "NO_ASSEMBLY_ON_THE_SHEET"
SCOPE_HAS_NO_PRINTED_MARK = "SCOPE_HAS_NO_PRINTED_MARK"
MARK_NOT_ON_A_CONDUCTOR = "MARK_NOT_ON_A_CONDUCTOR"
NO_DOCUMENTED_VALUE_IS_PRINTED = "NO_DOCUMENTED_VALUE_IS_PRINTED"
PRINTED_VALUES_LIE_OUTSIDE_EVERY_CONTAINER = "PRINTED_VALUES_LIE_OUTSIDE_EVERY_CONTAINER"
SEVERAL_ASSEMBLIES_CARRY_THE_EVIDENCE = "SEVERAL_ASSEMBLIES_CARRY_THE_EVIDENCE"
MEMBERSHIP_CAUSE = (
    NO_VECTOR_LAYER,
    NO_ASSEMBLY_ON_THE_SHEET,
    SCOPE_HAS_NO_PRINTED_MARK,
    MARK_NOT_ON_A_CONDUCTOR,
    NO_DOCUMENTED_VALUE_IS_PRINTED,
    PRINTED_VALUES_LIE_OUTSIDE_EVERY_CONTAINER,
    SEVERAL_ASSEMBLIES_CARRY_THE_EVIDENCE,
    UNKNOWN,
)

# ---------------------------------------------------------------------------
# how many scopes one assembly carries — §15
# ---------------------------------------------------------------------------

ONE_SCOPE = "ONE_SCOPE"
MULTI_SCOPE_EXACT = "MULTI_SCOPE_EXACT"
MULTI_SCOPE_PARTIAL = "MULTI_SCOPE_PARTIAL"
AMBIGUOUS_SCOPE_MEMBERSHIP = "AMBIGUOUS_SCOPE_MEMBERSHIP"
SCOPE_COMPOSITION = (
    ONE_SCOPE,
    MULTI_SCOPE_EXACT,
    MULTI_SCOPE_PARTIAL,
    AMBIGUOUS_SCOPE_MEMBERSHIP,
    UNKNOWN,
)

# ---------------------------------------------------------------------------
# the cross-representation fact vocabulary — §6, closed
# ---------------------------------------------------------------------------

#: Facts any representation may state.  Every one of them is something the sheet
#: *shows*; there is no key here whose value could say that something is not
#: drawn, and the vocabulary guard refuses the words that would turn a count of
#: zero into such a statement.
COMMON_FACT_KEYS = (
    "named_designations",
    "owner_designation",
    "printed_string_count",
    "folded_strings",
    "level_marks",
    "quantity_facets",
    "cable_facets",
)
#: Facts only a drawn schematic can state.
SCHEMATIC_FACT_KEYS = (
    "bus_exists",
    "bus_count",
    "feeder_count",
    "equipment_count",
    "terminal_count",
    "free_ended_feeder_count",
    "device_shape_multiset",
    "outgoing_branch_designations",
    "topology_signature",
)
#: Facts only a ruled table can state.
TABLE_FACT_KEYS = (
    "table_row_count",
    "table_column_count",
    "table_column_captions",
    "table_row_leaders",
    "table_filled_cell_count",
)
FACT_KEYS = COMMON_FACT_KEYS + SCHEMATIC_FACT_KEYS + TABLE_FACT_KEYS

#: Signature tiers.  Nested, and measured separately because §12 asks whether an
#: identity survives a change of representation while §16 asks whether it tells
#: two instances of one class apart, and one number answers neither well.
NAMES_ONLY = "NAMES_ONLY"
NAMES_AND_ROLES = "NAMES_AND_ROLES"
NAMES_AND_COUNTS = "NAMES_AND_COUNTS"
SIGNATURE_TIERS = (NAMES_ONLY, NAMES_AND_ROLES, NAMES_AND_COUNTS)

#: Ingredients a representation-neutral signature may never contain.  §12 names
#: them; ``assert_signature_representation_neutral`` enforces them.
FORBIDDEN_SIGNATURE_INGREDIENTS = (
    "physical_page",
    "bbox",
    "node_id",
    "table_row_number",
    "markdown_paragraph_position",
    "region_id",
    "assembly_id",
)

# ---------------------------------------------------------------------------
# how the two sides of a lineage task speak — §13, §14
# ---------------------------------------------------------------------------

ASSEMBLY_FACTS_BOTH_SIDES = "ASSEMBLY_FACTS_BOTH_SIDES"
ASSEMBLY_FACTS_LEFT_ONLY = "ASSEMBLY_FACTS_LEFT_ONLY"
ASSEMBLY_FACTS_RIGHT_ONLY = "ASSEMBLY_FACTS_RIGHT_ONLY"
NO_ASSEMBLY_FACTS = "NO_ASSEMBLY_FACTS"
BRIDGE_COVERAGE_CLASS = (
    ASSEMBLY_FACTS_BOTH_SIDES,
    ASSEMBLY_FACTS_LEFT_ONLY,
    ASSEMBLY_FACTS_RIGHT_ONLY,
    NO_ASSEMBLY_FACTS,
)

#: The nine ordered pairs of §13 plus the mixed case.  A side's representation is
#: the representation of the assembly it reached, never of the page it sits on.
REPRESENTATION_PAIRS = tuple(
    f"{left}_TO_{right}"
    for left in (SCHEMATIC, TABLE, TEXT)
    for right in (SCHEMATIC, TABLE, TEXT)
) + ("MIXED",)


# ---------------------------------------------------------------------------
# the records
# ---------------------------------------------------------------------------


def stable_id(prefix: str, payload: Any) -> str:
    """A deterministic address.  An address, not an identity."""
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return f"{prefix}_{hashlib.sha256(blob.encode('utf-8')).hexdigest()[:20]}"


@dataclass
class FunctionalAssembly:
    """One drawn container, read as an engineering assembly."""

    assembly_id: str
    document: str
    pair_id: str
    side: str
    physical_page: int
    assembly_channel: str
    representation_type: str
    assembly_kind: str
    membership_status: str
    source_region_ids: tuple[str, ...] = ()
    topology_subgraph_ids: tuple[str, ...] = ()
    table_ids: tuple[str, ...] = ()
    text_section_ids: tuple[str, ...] = ()
    member_label_ids: tuple[str, ...] = ()
    member_node_ids: tuple[str, ...] = ()
    member_function_ids: tuple[str, ...] = ()
    member_function_scope_ids: tuple[str, ...] = ()
    member_fragment_ids: tuple[str, ...] = ()
    owner_designation: str | None = None
    named_designations: tuple[str, ...] = ()
    scope_composition: str = UNKNOWN
    assembly_signature: str | None = None
    evidence_refs: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "assembly_id": self.assembly_id,
            "document": self.document,
            "pair_id": self.pair_id,
            "side": self.side,
            "physical_pages": [self.physical_page],
            "assembly_channel": self.assembly_channel,
            "representation_type": self.representation_type,
            "assembly_kind": self.assembly_kind,
            "membership_status": self.membership_status,
            "source_region_ids": list(self.source_region_ids),
            "topology_subgraph_ids": list(self.topology_subgraph_ids),
            "table_ids": list(self.table_ids),
            "text_section_ids": list(self.text_section_ids),
            "member_label_ids": list(self.member_label_ids),
            "member_label_count": len(self.member_label_ids),
            "member_node_ids": list(self.member_node_ids),
            "member_node_count": len(self.member_node_ids),
            "member_function_ids": list(self.member_function_ids),
            "member_function_scope_ids": list(self.member_function_scope_ids),
            "member_fragment_ids": list(self.member_fragment_ids),
            "owner_designation": self.owner_designation,
            "named_designations": list(self.named_designations),
            "scope_composition": self.scope_composition,
            "assembly_signature": self.assembly_signature,
            "evidence_refs": list(self.evidence_refs),
            "notes": list(self.notes),
        }


@dataclass
class AssemblyFact:
    """One positive statement an assembly makes about itself."""

    assembly_id: str
    key: str
    value: Any
    source_representation: str
    applicability: str
    provenance: str
    evidence_refs: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "assembly_id": self.assembly_id,
            "key": self.key,
            "value": self.value,
            "source_representation": self.source_representation,
            "applicability": self.applicability,
            "provenance": self.provenance,
            "evidence_refs": list(self.evidence_refs),
        }


@dataclass
class AssemblyMembership:
    """One (function, assembly) pairing, or the reasoned absence of one."""

    membership_id: str
    pair_id: str
    project: str
    side: str
    function_id: str
    scope_id: str | None
    fragment_id: str | None
    physical_page: int | None
    primary_mark: str | None
    membership_status: str
    membership_channel: str | None = None
    cause: str = UNKNOWN
    assembly_id: str | None = None
    candidate_assembly_ids: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "membership_id": self.membership_id,
            "pair_id": self.pair_id,
            "project": self.project,
            "side": self.side,
            "function_id": self.function_id,
            "scope_id": self.scope_id,
            "fragment_id": self.fragment_id,
            "physical_page": self.physical_page,
            "primary_mark": self.primary_mark,
            "membership_status": self.membership_status,
            "membership_channel": self.membership_channel,
            "cause": self.cause,
            "assembly_id": self.assembly_id,
            "candidate_assembly_ids": list(self.candidate_assembly_ids),
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


_VOCABULARY_EXEMPTION = "$.function_representation_bridge.vocabularies.forbidden_claim_terms"


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


_SIMILARITY_KEY = re.compile(
    r"(?:^|_)(score|scores|similarity|confidence|probability|likelihood|rank|"
    r"ranking|distance|proximity|closeness|weight|threshold)(?:$|_)",
    re.IGNORECASE,
)
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
    assemblies: Sequence[FunctionalAssembly],
    memberships: Sequence[AssemblyMembership],
    facts: Sequence[AssemblyFact],
) -> None:
    for assembly in assemblies:
        if assembly.assembly_channel not in ASSEMBLY_CHANNEL:
            raise ContractViolation(
                f"assembly channel outside the vocabulary: {assembly.assembly_channel}")
        if assembly.representation_type not in REPRESENTATION_TYPE:
            raise ContractViolation(
                f"representation outside the vocabulary: {assembly.representation_type}")
        if assembly.assembly_kind not in ASSEMBLY_KIND:
            raise ContractViolation(f"kind outside the vocabulary: {assembly.assembly_kind}")
        if assembly.assembly_kind in UNDECIDABLE_KINDS:
            raise ContractViolation(
                f"{assembly.assembly_id} claims {assembly.assembly_kind}, which this layer "
                f"refuses to decide: {UNDECIDABLE_KINDS[assembly.assembly_kind]}")
        if assembly.membership_status not in MEMBERSHIP_STATUS:
            raise ContractViolation(
                f"membership status outside the vocabulary: {assembly.membership_status}")
        if assembly.scope_composition not in SCOPE_COMPOSITION:
            raise ContractViolation(
                f"scope composition outside the vocabulary: {assembly.scope_composition}")
    for row in memberships:
        if row.membership_status not in MEMBERSHIP_STATUS:
            raise ContractViolation(f"membership outside the vocabulary: {row.membership_status}")
        if row.membership_channel is not None and row.membership_channel not in MEMBERSHIP_CHANNEL:
            raise ContractViolation(f"channel outside the vocabulary: {row.membership_channel}")
        if row.cause not in MEMBERSHIP_CAUSE:
            raise ContractViolation(f"cause outside the vocabulary: {row.cause}")
    for fact in facts:
        if fact.key not in FACT_KEYS:
            raise ContractViolation(f"fact key outside the vocabulary: {fact.key}")
        if fact.source_representation not in REPRESENTATION_TYPE:
            raise ContractViolation(
                f"fact representation outside the vocabulary: {fact.source_representation}")
        if fact.applicability not in APPLICABILITY:
            raise ContractViolation(f"applicability outside the vocabulary: {fact.applicability}")
        if not fact.evidence_refs:
            raise ContractViolation(f"{fact.assembly_id}:{fact.key} states a fact without evidence")


def assert_assembly_is_a_drawn_container(assemblies: Sequence[FunctionalAssembly]) -> None:
    """An assembly must name the drawn thing it is, and hold something.

    This is the guard that makes "same page" unusable: a page is not a region, a
    subgraph or a lattice, so an assembly built on one cannot satisfy this.
    """
    for assembly in assemblies:
        drawn = (
            assembly.source_region_ids
            or assembly.topology_subgraph_ids
            or assembly.table_ids
        )
        if not drawn:
            raise ContractViolation(f"{assembly.assembly_id} names no drawn container")
        if not assembly.member_label_ids and not assembly.member_node_ids:
            raise ContractViolation(f"{assembly.assembly_id} contains nothing")
        if assembly.assembly_channel in PROVING_ASSEMBLY_CHANNELS:
            if not assembly.topology_subgraph_ids:
                raise ContractViolation(
                    f"{assembly.assembly_id} claims the proven channel without a subgraph")
        elif assembly.membership_status == PROVEN:
            raise ContractViolation(
                f"{assembly.assembly_id} is PROVEN on a channel that may not prove")


def assert_no_sheet_wide_assembly(
    assemblies: Sequence[FunctionalAssembly],
    printed_strings_by_page: Mapping[tuple[str, int], int],
) -> None:
    """One assembly may not swallow a page that carries more than one of them."""
    by_page: dict[tuple[str, int], list[FunctionalAssembly]] = {}
    for assembly in assemblies:
        by_page.setdefault((assembly.document, assembly.physical_page), []).append(assembly)
    for key, members in by_page.items():
        if len(members) < 2:
            continue
        printed = printed_strings_by_page.get(key, 0)
        if not printed:
            continue
        for assembly in members:
            if len(assembly.member_label_ids) >= printed:
                raise ContractViolation(
                    f"{assembly.assembly_id} claims every printed string of a page that "
                    f"carries {len(members)} assemblies")


def assert_one_owner_per_label(assemblies: Sequence[FunctionalAssembly]) -> None:
    """No printed string belongs to two assemblies of one page."""
    seen: dict[tuple[str, int, str], str] = {}
    for assembly in assemblies:
        for label_id in assembly.member_label_ids:
            key = (assembly.document, assembly.physical_page, label_id)
            if key in seen and seen[key] != assembly.assembly_id:
                raise ContractViolation(
                    f"printed string {label_id} is claimed by two assemblies")
            seen[key] = assembly.assembly_id


def assert_membership_evidence(rows: Sequence[AssemblyMembership]) -> None:
    """A proven membership names a drawn relation; the capped channels may not."""
    for row in rows:
        if row.membership_status == PROVEN:
            if row.membership_channel not in PROVING_MEMBERSHIP_CHANNELS:
                raise ContractViolation(
                    f"{row.membership_id} proves on a channel that may not prove")
            if not row.assembly_id or not row.evidence_refs:
                raise ContractViolation(f"{row.membership_id} proves without evidence")
        elif row.membership_status == PARTIAL:
            if row.membership_channel not in MEMBERSHIP_CHANNEL:
                raise ContractViolation(f"{row.membership_id} joins outside the channels")
            if not row.assembly_id:
                raise ContractViolation(f"{row.membership_id} joins nothing")
        elif row.membership_status == AMBIGUOUS:
            if len(row.candidate_assembly_ids) < 2:
                raise ContractViolation(
                    f"{row.membership_id} is ambiguous between fewer than two assemblies")
            if row.assembly_id is not None:
                raise ContractViolation(f"{row.membership_id} is ambiguous yet chose one")
        else:
            if row.assembly_id is not None:
                raise ContractViolation(
                    f"{row.membership_id} asserts no membership yet names an assembly")


def assert_signature_representation_neutral(rows: Sequence[Mapping[str, Any]]) -> None:
    """A signature carrying where it was printed is a layout identity in disguise."""
    for row in rows:
        payload = {
            key: row[key] for key in ("signatures", "ingredients") if key in row
        }
        blob = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        for forbidden in FORBIDDEN_SIGNATURE_INGREDIENTS:
            if f'"{forbidden}"' in blob:
                raise ContractViolation(f"signature payload carries {forbidden!r}")
        for forbidden in ("n:", "reg_", "l:p", "fts_", "fasm_"):
            if forbidden in blob:
                raise ContractViolation(f"signature payload carries an address ({forbidden!r})")


def contract_document() -> dict[str, Any]:
    """The contract as an artifact, exempted from its own vocabulary guard."""
    return {
        "__exempt__": _VOCABULARY_EXEMPTION,
        "schema_version": SCHEMA_VERSION,
        "kind": "function_representation_bridge_contract",
        "model_calls": 0,
        "inherits": ["pdf-evidence.v1", "pdf-topology.v2", "function-topology.v1"],
        "rules": [
            "an assembly is a drawn container, and it must name the drawn thing it is",
            "a physical page is never a container and never joins two things",
            "a printed caption may name a drawn container and may never create one",
            "only a proven connected component may carry a proven membership",
            "a representation missing on one side is a gap in the producer, never a "
            "disagreement with the other side",
            "two quantities are equated only when the same printed designation carries "
            "both, never because two counts happen to agree",
        ],
        "prohibitions": {
            "resemblance_keys_refused": _SIMILARITY_KEY.pattern,
            "refused_membership_channels": list(REFUSED_MEMBERSHIP_CHANNELS),
            "page_as_a_container": False,
            "nearest_label_ownership": False,
            "a_representation_gap_read_as_a_contradiction": False,
        },
        "kinds_this_layer_refuses_to_decide": dict(UNDECIDABLE_KINDS),
        "vocabularies": {
            "representation_type": list(REPRESENTATION_TYPE),
            "assembly_channel": list(ASSEMBLY_CHANNEL),
            "proving_assembly_channels": list(PROVING_ASSEMBLY_CHANNELS),
            "assembly_kind": list(ASSEMBLY_KIND),
            "membership_status": list(MEMBERSHIP_STATUS),
            "membership_channel": list(MEMBERSHIP_CHANNEL),
            "proving_membership_channels": list(PROVING_MEMBERSHIP_CHANNELS),
            "membership_cause": list(MEMBERSHIP_CAUSE),
            "scope_composition": list(SCOPE_COMPOSITION),
            "fact_keys": list(FACT_KEYS),
            "signature_tiers": list(SIGNATURE_TIERS),
            "bridge_coverage_class": list(BRIDGE_COVERAGE_CLASS),
            "representation_pairs": list(REPRESENTATION_PAIRS),
            "applicability": list(APPLICABILITY),
            "forbidden_signature_ingredients": list(FORBIDDEN_SIGNATURE_INGREDIENTS),
            "forbidden_claim_terms": list(FORBIDDEN_CLAIM_TERMS),
        },
    }


__all__ = [name for name in dir() if not name.startswith("_")]
