"""The membership-certificate contract: five statuses, and what each may rest on.

Four tracks stand behind this one and none of their rules is relaxed here.

*From PDF EVIDENCE V1* — a producer asserts what the sheet shows and never what
it does not.  An uncertified function is a function this layer says nothing
about; no status below reads as "the function is not there".

*From PDF EVIDENCE V2 and FUNCTION TOPOLOGY V1* — an intersection is not a
connection, a label names a drawn group and never creates one, and a mark that
claims a whole board is bound to more than one of its members.

*From FUNCTION REPRESENTATION BRIDGE V1* — an assembly is a drawn container; a
page is never one; a value must distinguish before it may vote; a
representation missing on one side is a gap in the producer and never a
disagreement.

What this track adds is a **certificate**: a statement about one function and
one drawn container that names the structural relation it rests on.  Only four
channels may certify, and each composes two frozen structural relations rather
than inventing a third:

``TOPOLOGY_OWNER_MARK_ON_MEMBERS``
    the scope's own printed mark is bound, by a drawn label relation V2 proved,
    to two or more member nodes of one proven island, and to no node outside it.
    The function *is drawn as* this assembly.

``CAPTION_NAMES_SCOPE_WITH_DOCUMENTED_VALUE``
    a ruled lattice declares a single caption cell, the caption prints the
    scope's mark (through the production designation extractor on both sides),
    and a documented value of the scope is printed inside the same lattice.
    The function *is documented by* this assembly.

``MARK_INSIDE_ONE_CONTAINER_WITH_DOCUMENTED_VALUE``
    the scope's mark is printed inside exactly one non-schematic drawn container
    of its own page — inside, by V1's cell, box or leader relation — together
    with a documented value of the scope.

``FRAGMENT_EVIDENCE_IN_ONE_CONTAINER``
    the raw Markdown evidence rows the frozen deterministic extractor attributed
    to the scope's fragment are printed inside one drawn container and inside no
    other; the number of located rows and their minimum length are published as
    curves, and the operating point is chosen on a plateau.

**What is refused, by name.**  Same physical page, nearest container, most
similar string, highest-ranked candidate, single remaining candidate and any
distance to a container are listed as refused channels so that a later reader
cannot mistake their absence for an oversight.  ``assert_no_similarity_evidence``
of the bridge walks every produced payload and refuses a key that ranks, scores
or measures resemblance.

**CONTRADICTORY is narrow on purpose.**  It is emitted only when two *positive*
structural proofs about the same function name two containers that the drawing
itself names with two different designations, or when a container the proof
names with the scope's own mark prints a single value of a quantity the
passport also states as a single, different value.  A gap is never a
contradiction; a container that carries several scopes is never a contradiction.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from experiments.function_representation_bridge_v1.contract import (
    DOCUMENTED_VALUE_IN_ONE_ASSEMBLY,
    SHEET_MARK_WITH_ONE_ASSEMBLY,
    assert_no_absence_vocabulary as _bridge_no_absence_vocabulary,
    assert_no_similarity_evidence,
)
from experiments.pdf_evidence_v1.contract import FORBIDDEN_CLAIM_TERMS, ContractViolation

SCHEMA_VERSION = "function-assembly-membership.v1"

# ---------------------------------------------------------------------------
# statuses
# ---------------------------------------------------------------------------

CERTIFIED = "CERTIFIED"
PARTIAL = "PARTIAL"
AMBIGUOUS = "AMBIGUOUS"
CONTRADICTORY = "CONTRADICTORY"
UNKNOWN = "UNKNOWN"
CERTIFICATE_STATUS = (CERTIFIED, PARTIAL, AMBIGUOUS, CONTRADICTORY, UNKNOWN)

# ---------------------------------------------------------------------------
# channels
# ---------------------------------------------------------------------------

TOPOLOGY_OWNER_MARK_ON_MEMBERS = "TOPOLOGY_OWNER_MARK_ON_MEMBERS"
CAPTION_NAMES_SCOPE_WITH_DOCUMENTED_VALUE = "CAPTION_NAMES_SCOPE_WITH_DOCUMENTED_VALUE"
MARK_INSIDE_ONE_CONTAINER_WITH_DOCUMENTED_VALUE = "MARK_INSIDE_ONE_CONTAINER_WITH_DOCUMENTED_VALUE"
FRAGMENT_EVIDENCE_IN_ONE_CONTAINER = "FRAGMENT_EVIDENCE_IN_ONE_CONTAINER"
#: The only channels allowed to certify, in the order a certificate prefers them
#: when several prove at once.
CERTIFYING_CHANNELS = (
    TOPOLOGY_OWNER_MARK_ON_MEMBERS,
    CAPTION_NAMES_SCOPE_WITH_DOCUMENTED_VALUE,
    MARK_INSIDE_ONE_CONTAINER_WITH_DOCUMENTED_VALUE,
    FRAGMENT_EVIDENCE_IN_ONE_CONTAINER,
)

#: The scope's mark is bound to exactly one member of an island.  Topology V1
#: measured that a mark bound along a single feeder names the consumer that
#: feeder feeds (``ХМ1``, ``ЩНО``) and not the board, so one bound member may
#: support and may never certify.
MARK_BOUND_TO_ONE_MEMBER = "MARK_BOUND_TO_ONE_MEMBER"
#: The bridge's two capped channels, inherited as support and never promoted.
SUPPORT_CHANNELS = (
    MARK_BOUND_TO_ONE_MEMBER,
    DOCUMENTED_VALUE_IN_ONE_ASSEMBLY,
    SHEET_MARK_WITH_ONE_ASSEMBLY,
)
CERTIFICATE_CHANNEL = CERTIFYING_CHANNELS + SUPPORT_CHANNELS

REFUSED_CHANNELS = (
    "SAME_PHYSICAL_PAGE",
    "NEAREST_CONTAINER",
    "MOST_SIMILAR_STRING",
    "HIGHEST_RANKED_CANDIDATE",
    "SINGLE_REMAINING_CANDIDATE",
    "DISTANCE_TO_A_CONTAINER",
    "SHEET_TITLE_WITH_ONE_PROVEN_BOARD_AS_PROOF",
)

# ---------------------------------------------------------------------------
# what a certified relation means
# ---------------------------------------------------------------------------

#: The drawn container is the function: its board, its island.
IS_DRAWN_AS = "IS_DRAWN_AS"
#: The drawn container is a named block *about* the function: a captioned
#: parameter table, a nameplate box.  Positive, structural, and not the board.
IS_DOCUMENTED_BY = "IS_DOCUMENTED_BY"
#: The drawn container holds the rows the function was extracted from.  Which
#: of the two above it is, the drawing does not say.
HOLDS_SCOPE_EVIDENCE = "HOLDS_SCOPE_EVIDENCE"
RELATION_KIND = (IS_DRAWN_AS, IS_DOCUMENTED_BY, HOLDS_SCOPE_EVIDENCE)
RELATION_OF_CHANNEL = {
    TOPOLOGY_OWNER_MARK_ON_MEMBERS: IS_DRAWN_AS,
    CAPTION_NAMES_SCOPE_WITH_DOCUMENTED_VALUE: IS_DOCUMENTED_BY,
    MARK_INSIDE_ONE_CONTAINER_WITH_DOCUMENTED_VALUE: IS_DOCUMENTED_BY,
    FRAGMENT_EVIDENCE_IN_ONE_CONTAINER: HOLDS_SCOPE_EVIDENCE,
}

# ---------------------------------------------------------------------------
# why a certificate stopped where it stopped
# ---------------------------------------------------------------------------

NO_VECTOR_LAYER = "NO_VECTOR_LAYER"
NO_CONTAINER_ON_THE_SHEET = "NO_CONTAINER_ON_THE_SHEET"
SCOPE_HAS_NO_PRINTED_MARK = "SCOPE_HAS_NO_PRINTED_MARK"
MARK_LIES_OUTSIDE_EVERY_CONTAINER = "MARK_LIES_OUTSIDE_EVERY_CONTAINER"
MARK_BOUND_AS_A_CONSUMER_OF_ANOTHER_OWNER = "MARK_BOUND_AS_A_CONSUMER_OF_ANOTHER_OWNER"
SEVERAL_CONTAINERS_CARRY_THE_MARK = "SEVERAL_CONTAINERS_CARRY_THE_MARK"
SEVERAL_ISLANDS_CARRY_THE_MARK = "SEVERAL_ISLANDS_CARRY_THE_MARK"
NO_DOCUMENTED_VALUE_INSIDE_THE_NAMED_CONTAINER = "NO_DOCUMENTED_VALUE_INSIDE_THE_NAMED_CONTAINER"
NO_FRAGMENT_EVIDENCE_IS_LOCATED = "NO_FRAGMENT_EVIDENCE_IS_LOCATED"
LOCATED_EVIDENCE_LIES_OUTSIDE_EVERY_CONTAINER = "LOCATED_EVIDENCE_LIES_OUTSIDE_EVERY_CONTAINER"
EVIDENCE_SPANS_SEVERAL_CONTAINERS = "EVIDENCE_SPANS_SEVERAL_CONTAINERS"
TOO_FEW_LOCATED_SEGMENTS = "TOO_FEW_LOCATED_SEGMENTS"
NAMED_CONTAINERS_DISAGREE = "NAMED_CONTAINERS_DISAGREE"
QUANTITY_VALUES_DISAGREE = "QUANTITY_VALUES_DISAGREE"
CERTIFICATE_CAUSE = (
    NO_VECTOR_LAYER,
    NO_CONTAINER_ON_THE_SHEET,
    SCOPE_HAS_NO_PRINTED_MARK,
    MARK_LIES_OUTSIDE_EVERY_CONTAINER,
    MARK_BOUND_AS_A_CONSUMER_OF_ANOTHER_OWNER,
    SEVERAL_CONTAINERS_CARRY_THE_MARK,
    SEVERAL_ISLANDS_CARRY_THE_MARK,
    NO_DOCUMENTED_VALUE_INSIDE_THE_NAMED_CONTAINER,
    NO_FRAGMENT_EVIDENCE_IS_LOCATED,
    LOCATED_EVIDENCE_LIES_OUTSIDE_EVERY_CONTAINER,
    EVIDENCE_SPANS_SEVERAL_CONTAINERS,
    TOO_FEW_LOCATED_SEGMENTS,
    NAMED_CONTAINERS_DISAGREE,
    QUANTITY_VALUES_DISAGREE,
    UNKNOWN,
)

# ---------------------------------------------------------------------------
# scopes and assemblies — §1B
# ---------------------------------------------------------------------------

#: How the components of one FunctionScope came out.
COMPONENTS_CERTIFIED_TO_ONE_CONTAINER = "COMPONENTS_CERTIFIED_TO_ONE_CONTAINER"
COMPONENTS_CERTIFIED_TO_SEVERAL_CONTAINERS = "COMPONENTS_CERTIFIED_TO_SEVERAL_CONTAINERS"
SOME_COMPONENTS_UNCERTIFIED = "SOME_COMPONENTS_UNCERTIFIED"
A_COMPONENT_IS_AMBIGUOUS = "A_COMPONENT_IS_AMBIGUOUS"
A_COMPONENT_IS_CONTRADICTORY = "A_COMPONENT_IS_CONTRADICTORY"
NO_COMPONENT_JOINED = "NO_COMPONENT_JOINED"
SCOPE_CAUSE = (
    COMPONENTS_CERTIFIED_TO_ONE_CONTAINER,
    COMPONENTS_CERTIFIED_TO_SEVERAL_CONTAINERS,
    SOME_COMPONENTS_UNCERTIFIED,
    A_COMPONENT_IS_AMBIGUOUS,
    A_COMPONENT_IS_CONTRADICTORY,
    NO_COMPONENT_JOINED,
    UNKNOWN,
)

#: How many certified scopes one assembly carries.  ``MULTI_SCOPE`` is a
#: finding about the passport's granularity and never a defect to split away.
ONE_SCOPE = "ONE_SCOPE"
MULTI_SCOPE = "MULTI_SCOPE"
NO_CERTIFIED_SCOPE = "NO_CERTIFIED_SCOPE"
ASSEMBLY_COMPOSITION = (ONE_SCOPE, MULTI_SCOPE, NO_CERTIFIED_SCOPE)

# ---------------------------------------------------------------------------
# passport fields
# ---------------------------------------------------------------------------

#: ``systems`` is a bag of tokenised words from the sheet description
#: ("щита", "кабелей", "электроснабжение"); the merge certificate of v2.8
#: excluded it for carrying no engineering meaning, and the value channel of
#: the bridge measured it voting with title words printed in a box.  It may
#: not carry identity here.
EXCLUDED_VALUE_FIELDS = ("systems",)


def stable_id(prefix: str, payload: Any) -> str:
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return f"{prefix}_{hashlib.sha256(blob.encode('utf-8')).hexdigest()[:20]}"


# ---------------------------------------------------------------------------
# the records
# ---------------------------------------------------------------------------


@dataclass
class MembershipCertificate:
    """One function against the drawn containers of its own page."""

    certificate_id: str
    pair_id: str
    project: str
    side: str
    function_id: str
    scope_id: str | None
    fragment_ids: tuple[str, ...]
    physical_page: int | None
    primary_mark: str | None
    status: str
    channel: str | None = None
    relation_kind: str | None = None
    assembly_id: str | None = None
    certified_assembly_ids: tuple[str, ...] = ()
    candidate_assembly_ids: tuple[str, ...] = ()
    cause: str = UNKNOWN
    structural_basis: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()
    located_segments: int = 0
    conflict: dict[str, Any] | None = None
    support_channels: tuple[str, ...] = ()
    channel_outcomes: dict[str, str] | None = None
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "certificate_id": self.certificate_id,
            "pair_id": self.pair_id,
            "project": self.project,
            "side": self.side,
            "function_id": self.function_id,
            "scope_id": self.scope_id,
            "fragment_ids": list(self.fragment_ids),
            "physical_page": self.physical_page,
            "primary_mark": self.primary_mark,
            "status": self.status,
            "channel": self.channel,
            "relation_kind": self.relation_kind,
            "assembly_id": self.assembly_id,
            "certified_assembly_ids": list(self.certified_assembly_ids),
            "candidate_assembly_ids": list(self.candidate_assembly_ids),
            "cause": self.cause,
            "structural_basis": list(self.structural_basis),
            "evidence_refs": list(self.evidence_refs),
            "located_segments": self.located_segments,
            "conflict": self.conflict,
            "support_channels": list(self.support_channels),
            "channel_outcomes": dict(self.channel_outcomes or {}),
            "notes": list(self.notes),
        }


# ---------------------------------------------------------------------------
# guards
# ---------------------------------------------------------------------------

_VOCABULARY_EXEMPTION = "$.function_assembly_membership.vocabularies.forbidden_claim_terms"


def assert_no_absence_vocabulary(payload: Any) -> None:
    _bridge_no_absence_vocabulary(payload, exempt=(_VOCABULARY_EXEMPTION,))


def assert_certificate_evidence(rows: Sequence[MembershipCertificate]) -> None:
    """A certified row names a certifying channel, a container and a basis."""
    for row in rows:
        if row.status not in CERTIFICATE_STATUS:
            raise ContractViolation(f"{row.certificate_id}: status outside the vocabulary")
        if row.channel is not None and row.channel not in CERTIFICATE_CHANNEL:
            raise ContractViolation(f"{row.certificate_id}: channel outside the vocabulary")
        if row.cause not in CERTIFICATE_CAUSE:
            raise ContractViolation(f"{row.certificate_id}: cause outside the vocabulary")
        if row.relation_kind is not None and row.relation_kind not in RELATION_KIND:
            raise ContractViolation(f"{row.certificate_id}: relation outside the vocabulary")
        if row.status == CERTIFIED:
            if row.channel not in CERTIFYING_CHANNELS:
                raise ContractViolation(
                    f"{row.certificate_id} certifies on a channel that may not certify")
            if not row.assembly_id or not row.evidence_refs or not row.structural_basis:
                raise ContractViolation(f"{row.certificate_id} certifies without a basis")
            if row.assembly_id not in row.certified_assembly_ids:
                raise ContractViolation(f"{row.certificate_id} certifies a container it does not list")
            if row.relation_kind != RELATION_OF_CHANNEL[row.channel]:
                raise ContractViolation(f"{row.certificate_id} misstates its relation")
        elif row.status == PARTIAL:
            if row.channel not in CERTIFICATE_CHANNEL:
                raise ContractViolation(f"{row.certificate_id} joins outside the channels")
            if not row.assembly_id and len(row.candidate_assembly_ids) < 1:
                raise ContractViolation(f"{row.certificate_id} joins nothing")
            if row.certified_assembly_ids:
                raise ContractViolation(f"{row.certificate_id} is partial yet lists a certified container")
        elif row.status == AMBIGUOUS:
            if len(row.candidate_assembly_ids) < 2:
                raise ContractViolation(
                    f"{row.certificate_id} is ambiguous between fewer than two containers")
            if row.assembly_id is not None or row.certified_assembly_ids:
                raise ContractViolation(f"{row.certificate_id} is ambiguous yet chose one")
        elif row.status == CONTRADICTORY:
            if not row.conflict or len(row.conflict.get("evidence_refs", ())) < 2:
                raise ContractViolation(f"{row.certificate_id} contradicts without two proofs")
            if row.assembly_id is not None:
                raise ContractViolation(f"{row.certificate_id} is contradictory yet chose one")
        else:
            if row.assembly_id is not None or row.certified_assembly_ids:
                raise ContractViolation(
                    f"{row.certificate_id} asserts no membership yet names a container")


def assert_certified_container_lies_on_the_function_page(
    rows: Sequence[MembershipCertificate],
    page_of_assembly: Mapping[str, tuple[str, int]],
) -> None:
    """The domain of the question is the function's own page, and only it."""
    for row in rows:
        for assembly_id in row.certified_assembly_ids + row.candidate_assembly_ids:
            located = page_of_assembly.get(assembly_id)
            if located is None:
                raise ContractViolation(f"{row.certificate_id} names an unknown container")
            if located != (f"{row.project}/{row.side}", row.physical_page):
                raise ContractViolation(
                    f"{row.certificate_id} names a container from another page")


def contract_document() -> dict[str, Any]:
    return {
        "__exempt__": _VOCABULARY_EXEMPTION,
        "schema_version": SCHEMA_VERSION,
        "kind": "function_assembly_membership_contract",
        "model_calls": 0,
        "inherits": [
            "pdf-evidence.v1", "pdf-topology.v2", "function-topology.v1",
            "function-representation-bridge.v1",
        ],
        "rules": [
            "a certificate names the structural relation it rests on, or it is not a certificate",
            "only the four certifying channels may certify; every other channel supports",
            "a mark that claims a whole island is bound to two or more of its members; "
            "one bound member names a consumer and may only support",
            "the domain of the question is the function's own physical page; the page is "
            "never the evidence",
            "a printed string that lies in two containers votes for neither",
            "an uncertified function is a function this layer says nothing about",
            "several certified scopes on one container are the passport's granularity, "
            "never a defect to split away",
            "CONTRADICTORY needs two positive structural proofs that name incompatible "
            "things; a gap is never a contradiction",
        ],
        "prohibitions": {
            "refused_channels": list(REFUSED_CHANNELS),
            "page_as_evidence": False,
            "single_remaining_candidate_as_proof": False,
            "absence_inference": False,
            "excluded_value_fields": list(EXCLUDED_VALUE_FIELDS),
        },
        "vocabularies": {
            "certificate_status": list(CERTIFICATE_STATUS),
            "certifying_channels": list(CERTIFYING_CHANNELS),
            "support_channels": list(SUPPORT_CHANNELS),
            "relation_kind": list(RELATION_KIND),
            "relation_of_channel": dict(RELATION_OF_CHANNEL),
            "certificate_cause": list(CERTIFICATE_CAUSE),
            "scope_cause": list(SCOPE_CAUSE),
            "assembly_composition": list(ASSEMBLY_COMPOSITION),
            "forbidden_claim_terms": list(FORBIDDEN_CLAIM_TERMS),
        },
    }


__all__ = [name for name in dir() if not name.startswith("_")]
