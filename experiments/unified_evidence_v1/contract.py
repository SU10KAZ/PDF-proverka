"""The unified evidence contract: one fact shape, seven producers, no absence.

Every layer this line of research built states things in its own vocabulary,
and the lineage layer then has to ask seven different questions to learn one
thing.  This contract gives them one shape.  It inherits every prohibition of
the layers beneath it and adds none of its own beyond the shape:

* a fact states what a document *shows*; there is no key whose value could say
  that something is not shown, and the vocabulary guard refuses the words;
* ``POSITIVE_PRESENCE`` is reserved for a fact with exact provenance — a page
  and a rectangle, or a drawn relation V2 proved.  An OCR row has a page and no
  rectangle, so on its own it may only ``SUPPORT``; it is promoted when the
  native layer prints the same string on the same page, and the promotion
  carries both references;
* a fact names a FunctionScope or an assembly only through a CERTIFIED
  membership certificate; a passport's own claim about its function is kept
  separately as *declared*, never as certified;
* applicability travels with the fact and is never widened: a stamp value is
  ``SHEET_SHARED`` and would be a lie as ``FUNCTION_LOCAL``.

Resemblance keys are refused structurally by the bridge's guard, which this
contract re-exports.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Iterator, Mapping, Sequence

from experiments.function_representation_bridge_v1.contract import (
    assert_no_absence_vocabulary as _bridge_no_absence_vocabulary,
    assert_no_similarity_evidence,
)
from experiments.pdf_evidence_v1.contract import FORBIDDEN_CLAIM_TERMS, ContractViolation

SCHEMA_VERSION = "unified-engineering-evidence.v1"

# ---------------------------------------------------------------------------
# producers — §2 of the track, closed
# ---------------------------------------------------------------------------

NATIVE_PDF_TEXT = "NATIVE_PDF_TEXT"
MARKDOWN_OCR = "MARKDOWN_OCR"
TABLE = "TABLE"
FUNCTION_REGION = "FUNCTION_REGION"
SCHEMATIC_TOPOLOGY = "SCHEMATIC_TOPOLOGY"
FUNCTION_TOPOLOGY_SUBGRAPH = "FUNCTION_TOPOLOGY_SUBGRAPH"
FUNCTIONAL_ASSEMBLY = "FUNCTIONAL_ASSEMBLY"
SOURCE_REPRESENTATION = (
    NATIVE_PDF_TEXT,
    MARKDOWN_OCR,
    TABLE,
    FUNCTION_REGION,
    SCHEMATIC_TOPOLOGY,
    FUNCTION_TOPOLOGY_SUBGRAPH,
    FUNCTIONAL_ASSEMBLY,
)
#: Which frozen layer produced the fact.  A producer is a module, a
#: representation is what the sheet used; they are different axes.
PRODUCERS = (
    "pdf_evidence_v1",
    "pdf_evidence_v2",
    "function_topology_v1",
    "function_representation_bridge_v1",
    "function_assembly_membership_v1",
    "function_lineage_passport",
    "function_lineage_fragment",
)

# ---------------------------------------------------------------------------
# applicability and claims
# ---------------------------------------------------------------------------

FUNCTION_LOCAL = "FUNCTION_LOCAL"
ASSEMBLY_LOCAL = "ASSEMBLY_LOCAL"
SHEET_SHARED = "SHEET_SHARED"
DOCUMENT_SHARED = "DOCUMENT_SHARED"
UNKNOWN = "UNKNOWN"
APPLICABILITY = (FUNCTION_LOCAL, ASSEMBLY_LOCAL, SHEET_SHARED, DOCUMENT_SHARED, UNKNOWN)

POSITIVE_PRESENCE = "POSITIVE_PRESENCE"
SUPPORT_ONLY = "SUPPORT_ONLY"
CLAIM_SEMANTICS = (POSITIVE_PRESENCE, SUPPORT_ONLY)

#: How exactly the fact can be pointed at.  An enum, never a number.
EXACT_GEOMETRY = "EXACT_GEOMETRY"
DRAWN_RELATION = "DRAWN_RELATION"
PAGE_ONLY = "PAGE_ONLY"
DERIVED = "DERIVED"
PROVENANCE_GRADE = (EXACT_GEOMETRY, DRAWN_RELATION, PAGE_ONLY, DERIVED)
#: Grades on which a fact may assert presence by itself.
PRESENCE_GRADES = (EXACT_GEOMETRY, DRAWN_RELATION)

# ---------------------------------------------------------------------------
# fields — closed
# ---------------------------------------------------------------------------

TEXT_FIELDS = ("printed_string", "designation", "level_mark", "quantity", "cable")
TABLE_FIELDS = ("table_cell", "table_caption", "table_row_leader")
REGION_FIELDS = ("region", "region_string_count", "region_designations")
TOPOLOGY_FIELDS = (
    "node_count", "proven_connection_count", "label_binding", "proven_direction",
    "bus_node", "arrowhead",
)
SUBGRAPH_FIELDS = (
    "subgraph", "bus_count", "feeder_count", "equipment_count", "terminal_count",
    "topology_signature", "owner_mark", "boundary_status",
)
ASSEMBLY_FIELDS = (
    "assembly", "named_designations", "owner_designation", "printed_string_count",
    "folded_strings", "level_marks", "quantity_facets", "cable_facets", "bus_exists",
    "free_ended_feeder_count", "device_shape_multiset", "outgoing_branch_designations",
    "table_row_count", "table_column_count", "table_column_captions", "table_row_leaders",
    "table_filled_cell_count",
)
MARKDOWN_FIELDS = (
    "sheet_title", "primary_mark", "function_class", "component_role", "passport_value",
    "evidence_row", "passport_quantity",
)
LINEAGE_FIELDS = ("membership_certificate", "cross_sheet_named_reference")
FIELDS = (
    TEXT_FIELDS + TABLE_FIELDS + REGION_FIELDS + TOPOLOGY_FIELDS + SUBGRAPH_FIELDS
    + ASSEMBLY_FIELDS + MARKDOWN_FIELDS + LINEAGE_FIELDS
)

#: Container kinds a fact may sit in.
CONTAINER_KINDS = ("TABLE_CELL", "TABLE", "REGION", "ISLAND", "ASSEMBLY", "STAMP", "PAGE")


def stable_id(prefix: str, payload: Any) -> str:
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return f"{prefix}_{hashlib.sha256(blob.encode('utf-8')).hexdigest()[:20]}"


@dataclass
class UnifiedFact:
    """One positive statement about one document, in one shape."""

    fact_id: str
    field: str
    normalized_value: Any
    source_representation: str
    producer: str
    pair_id: str
    document: str
    side: str
    physical_page: int
    applicability: str
    claim_semantics: str
    provenance_grade: str
    provenance_refs: tuple[str, ...] = ()
    container: dict[str, Any] | None = None
    declared_function_ids: tuple[str, ...] = ()
    certified_function_scope_id: str | None = None
    certified_function_ids: tuple[str, ...] = ()
    certified_assembly_id: str | None = None
    raw_value: Any = None
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        out = {
            "fact_id": self.fact_id,
            "field": self.field,
            "normalized_value": self.normalized_value,
            "source_representation": self.source_representation,
            "producer": self.producer,
            "pair_id": self.pair_id,
            "document": self.document,
            "side": self.side,
            "physical_page": self.physical_page,
            "applicability": self.applicability,
            "claim_semantics": self.claim_semantics,
            "provenance_grade": self.provenance_grade,
            "provenance_refs": list(self.provenance_refs),
            "container": self.container,
            "declared_function_ids": list(self.declared_function_ids),
            "certified_function_scope_id": self.certified_function_scope_id,
            "certified_function_ids": list(self.certified_function_ids),
            "certified_assembly_id": self.certified_assembly_id,
        }
        if self.raw_value is not None:
            out["raw_value"] = self.raw_value
        if self.notes:
            out["notes"] = list(self.notes)
        return out


# ---------------------------------------------------------------------------
# guards
# ---------------------------------------------------------------------------

_VOCABULARY_EXEMPTION = "$.unified_engineering_evidence.vocabularies.forbidden_claim_terms"


def assert_no_absence_vocabulary(payload: Any) -> None:
    _bridge_no_absence_vocabulary(payload, exempt=(_VOCABULARY_EXEMPTION,))


def assert_fact_contract(
    facts: Sequence[UnifiedFact],
    *,
    certified_pairs: Mapping[tuple[str, str, str], tuple[str, ...]] | None = None,
) -> None:
    """Closed vocabularies, presence needs exact provenance, certification needs a certificate."""
    for fact in facts:
        if fact.field not in FIELDS:
            raise ContractViolation(f"{fact.fact_id}: field outside the vocabulary: {fact.field}")
        if fact.source_representation not in SOURCE_REPRESENTATION:
            raise ContractViolation(f"{fact.fact_id}: representation outside the vocabulary")
        if fact.producer not in PRODUCERS:
            raise ContractViolation(f"{fact.fact_id}: producer outside the vocabulary")
        if fact.applicability not in APPLICABILITY:
            raise ContractViolation(f"{fact.fact_id}: applicability outside the vocabulary")
        if fact.claim_semantics not in CLAIM_SEMANTICS:
            raise ContractViolation(f"{fact.fact_id}: claim outside the vocabulary")
        if fact.provenance_grade not in PROVENANCE_GRADE:
            raise ContractViolation(f"{fact.fact_id}: provenance grade outside the vocabulary")
        if fact.container is not None and fact.container.get("kind") not in CONTAINER_KINDS:
            raise ContractViolation(f"{fact.fact_id}: container kind outside the vocabulary")
        if not fact.provenance_refs:
            raise ContractViolation(f"{fact.fact_id} states a fact without provenance")
        if fact.claim_semantics == POSITIVE_PRESENCE and fact.provenance_grade not in PRESENCE_GRADES:
            raise ContractViolation(
                f"{fact.fact_id} asserts presence on grade {fact.provenance_grade}")
        if fact.applicability == FUNCTION_LOCAL and not (
            fact.declared_function_ids or fact.certified_function_ids
        ):
            raise ContractViolation(f"{fact.fact_id} is FUNCTION_LOCAL and names no function")
        if fact.applicability == ASSEMBLY_LOCAL and not fact.certified_assembly_id and not (
            fact.container and fact.container.get("kind") in {"ASSEMBLY", "ISLAND", "TABLE"}
        ):
            raise ContractViolation(f"{fact.fact_id} is ASSEMBLY_LOCAL and names no container")
        if certified_pairs is not None and fact.certified_function_ids:
            for function_id in fact.certified_function_ids:
                allowed = certified_pairs.get((fact.pair_id, fact.side, function_id), ())
                if fact.certified_assembly_id not in allowed:
                    raise ContractViolation(
                        f"{fact.fact_id} names function {function_id} without a certificate "
                        f"for {fact.certified_assembly_id}")
        if fact.certified_function_scope_id and not fact.certified_function_ids:
            raise ContractViolation(f"{fact.fact_id} names a scope without a certified function")


def contract_document() -> dict[str, Any]:
    return {
        "__exempt__": _VOCABULARY_EXEMPTION,
        "schema_version": SCHEMA_VERSION,
        "kind": "unified_engineering_evidence_contract",
        "model_calls": 0,
        "inherits": [
            "pdf-evidence.v1", "pdf-topology.v2", "function-topology.v1",
            "function-representation-bridge.v1", "function-assembly-membership.v1",
        ],
        "rules": [
            "a fact states what a document shows and never what it does not",
            "POSITIVE_PRESENCE requires exact geometry or a drawn relation; a page number alone supports",
            "an OCR row is promoted to presence only when the native layer prints the same "
            "string on the same page, and the promotion carries both references",
            "a fact names a FunctionScope or an assembly only through a CERTIFIED membership "
            "certificate; a passport's own claim is kept as declared",
            "applicability travels with the fact and is never widened",
            "a fact missing on one side of a pair is a gap in the producer, never a disagreement",
        ],
        "prohibitions": {
            "absence_facts": False,
            "resemblance_keys": False,
            "applicability_widening": False,
            "certification_without_a_certificate": False,
        },
        "vocabularies": {
            "source_representation": list(SOURCE_REPRESENTATION),
            "producers": list(PRODUCERS),
            "applicability": list(APPLICABILITY),
            "claim_semantics": list(CLAIM_SEMANTICS),
            "provenance_grade": list(PROVENANCE_GRADE),
            "presence_grades": list(PRESENCE_GRADES),
            "fields": list(FIELDS),
            "container_kinds": list(CONTAINER_KINDS),
            "forbidden_claim_terms": list(FORBIDDEN_CLAIM_TERMS),
        },
    }


__all__ = [name for name in dir() if not name.startswith("_")]
