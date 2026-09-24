"""Versioned contracts of the Consolidator V1 (projectchange_consolidator_runtime/1).

Two schemas:

* ``INPUT_SCHEMA`` — the payload of ONE candidate-cluster call.  Validated
  locally before anything is sent; it may use the full JSON Schema vocabulary.
* ``DECISION_SCHEMA`` — what the model must return.  It is handed to the
  provider transport, so it uses ONLY the keywords the V3 Miner schemas already
  use on both subscription transports (type, properties, required,
  additionalProperties, enum, items, minItems/maxItems): no pattern, no const,
  no anyOf.  Formats of references are checked by the deterministic validator.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any

ENGINE_NAME = "projectchange_consolidator"
ENGINE_VERSION = "1.0.0"
RUNTIME_CONTRACT = "projectchange_consolidator_runtime/1"
INPUT_CONTRACT = "projectchange_consolidator_runtime_input/1"
DECISION_CONTRACT = "projectchange_consolidator_decision/1"
CONSOLIDATED_SCHEMA_VERSION = "projectchange_consolidated/2"
SHADOW_RESULT_SCHEMA = "projectchange_consolidator_shadow_result/1"
SHADOW_RUN_SCHEMA = "projectchange-consolidator-shadow-run/1"
HINT_IDENTITY_SCHEMA = "projectchange_hint_identity/1"
PREFILTER_VERSION = "prefilter/1"
STAGE = "CONSOLIDATE"

MODE_CLUSTER = "CLUSTER"
MODE_SINGLETON_REVIEW = "SINGLETON_REVIEW"
MERGE, KEEP_SEPARATE, UNCERTAIN = "MERGE", "KEEP_SEPARATE", "UNCERTAIN"
ENGINEERING, DOCUMENTARY, REVIEW = "ENGINEERING_CHANGE", "DOCUMENTARY_CHANGE", "REVIEW"
COMPOSITE_CARD = "COMPOSITE_CARD"
SCOPE_TYPES = ("SYSTEM_WIDE", "MULTI_BUILDING", "BUILDING_SPECIFIC", "LOCAL_ELEMENT", "DESIGN_BASIS")
SCOPE_BASES = ("EXPLICIT_GENERAL_STATEMENT", "ALL_LOCATION_UNITS_COVERED", "UNION_OF_MANIFESTATIONS",
               "SINGLE_MANIFESTATION", "CALCULATION_BASIS")

MAX_CLUSTER_CARDS = 8
FRAGMENT_CHARS = 360
ROLE_CHARS = 120
HINT_TEXT_CHARS = 420
HINTS_PER_CARD = 6
PAYLOAD_BUDGET_CHARS = 200_000

ID_RE = re.compile(r"^[A-Za-z0-9_.:-]+$")
LOCATION_ID_RE = re.compile(r"^L[0-9]{2,3}$")
PARAM_REF_RE = re.compile(r"^([A-Za-z0-9_.:-]+)#p([0-9]+)$")
EVIDENCE_REF_RE = re.compile(r"^([A-Za-z0-9_.:-]+)#e([0-9]+)$")
PLACEHOLDER_RE = re.compile(r"\{\{(P[0-9]+)\.(old|new|unit)\}\}")
# Evaluation vocabulary that must never reach a model payload.
LABEL_LEAK_RE = re.compile(
    r"\bSF-\d{3}\b|\bDC-\d{2,3}\b|\bTOO_ATOMIC\b|\bOVER_MERGED\b|\bGOOD_HUMAN_LEVEL\b|"
    r"\bDUPLICATE_CLUSTER\b|\bP_STRICT\b|\bPARTIALLY_SUPPORTED\b|\bNOT_SUPPORTED\b|source[_-]first",
    re.IGNORECASE)


def sha256_json(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _obj(required: list[str], **props: Any) -> dict[str, Any]:
    return {"type": "object", "additionalProperties": False, "required": required, "properties": props}


S = {"type": "string"}
STRS = {"type": "array", "items": S}
_IDS = {"type": "string", "pattern": ID_RE.pattern}
_LOC = {"type": "string", "pattern": LOCATION_ID_RE.pattern}
_HINT_KEY_IN = _obj(["region_id", "hint_id"], region_id=_IDS, hint_id=_IDS)

INPUT_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": INPUT_CONTRACT,
    **_obj(
        ["contract", "mode", "pair_id", "source_run_id", "cluster_id", "location_catalog", "location_units",
         "regions", "cards", "hints", "pair_features"],
        contract={"enum": [INPUT_CONTRACT]},
        mode={"enum": [MODE_CLUSTER, MODE_SINGLETON_REVIEW]},
        pair_id=_IDS, source_run_id=_IDS, cluster_id=_IDS,
        location_catalog={"type": "array", "items": _obj(
            ["location_id", "text", "origins"], location_id=_LOC, text={"type": "string", "minLength": 1},
            origins={"type": "array", "minItems": 1, "items": _obj(
                ["kind", "ref"], kind={"enum": ["CARD_LOCATION", "PARAMETER_LOCATION", "MAPPER_REGION"]},
                ref={"type": "string", "minLength": 1})})},
        location_units=_obj(["available", "units", "unavailable_reason"], available={"type": "boolean"},
                            unavailable_reason=S,
                            units={"type": "array", "items": _obj(
                                ["unit_id", "label", "location_ids"],
                                unit_id={"type": "string", "pattern": r"^U[0-9]{2}$"},
                                label={"type": "string", "minLength": 1},
                                location_ids={"type": "array", "items": _LOC})}),
        regions={"type": "array", "items": _obj(["region_id", "engineering_domain", "locations"],
                                                region_id=_IDS, engineering_domain=S, locations=STRS)},
        cards={"type": "array", "minItems": 1, "maxItems": MAX_CLUSTER_CARDS, "items": _obj(
            ["card_ref", "region_id", "dedupe_lineage", "subject", "region_scope_claim", "locations",
             "location_ids", "summary", "old_state", "new_state", "why_one_event", "parameters", "evidence",
             "attached_hint_keys"],
            card_ref=_IDS, region_id=_IDS, dedupe_lineage={"type": "array", "minItems": 1, "items": _IDS},
            subject=S, region_scope_claim=S, locations=STRS, location_ids={"type": "array", "items": _LOC},
            summary=S, old_state=S, new_state=S, why_one_event=S,
            parameters={"type": "array", "items": _obj(
                ["p", "name", "old_value", "new_value", "unit", "location", "location_ids"],
                p={"type": "string", "pattern": r"^p[0-9]+$"}, name=S, old_value=S, new_value=S, unit=S,
                location=S, location_ids={"type": "array", "items": _LOC})},
            evidence={"type": "array", "items": _obj(
                ["e", "side", "page", "block", "type", "fragment", "role"],
                e={"type": "string", "pattern": r"^e[0-9]+$"}, side={"enum": ["OLD", "NEW"]},
                page={"type": "integer", "minimum": 1}, block={"type": "string", "minLength": 1},
                type={"enum": ["TEXT", "TABLE", "GRAPHIC"]},
                fragment={"type": "string", "maxLength": FRAGMENT_CHARS},
                role={"type": "string", "maxLength": ROLE_CHARS})},
            attached_hint_keys={"type": "array", "items": _HINT_KEY_IN})},
        hints={"type": "array", "items": _obj(
            ["hint_key", "hint_ref", "kind", "subject", "suspected", "conflict_or_missing", "evidence",
             "attached_to_cards"],
            hint_key=_HINT_KEY_IN, hint_ref={"type": "string", "pattern": r"^[A-Za-z0-9_.:-]+/[A-Za-z0-9_.:-]+$"},
            kind={"enum": ["UNRESOLVED_HINT", "SOURCE_CONFLICT"]}, subject=S,
            suspected={"type": "string", "maxLength": HINT_TEXT_CHARS},
            conflict_or_missing={"type": "string", "maxLength": HINT_TEXT_CHARS},
            evidence={"type": "array", "items": _obj(
                ["h", "side", "page", "block", "fragment"], h={"type": "string", "pattern": r"^h[0-9]+$"},
                side={"enum": ["OLD", "NEW"]}, page={"type": "integer", "minimum": 1},
                block={"type": "string", "minLength": 1}, fragment={"type": "string", "maxLength": FRAGMENT_CHARS})},
            attached_to_cards={"type": "array", "minItems": 1, "items": _IDS})},
        pair_features={"type": "array", "items": _obj(
            ["a", "b", "same_region", "shared_blocks", "frag_sim", "shared_transitions", "shared_designations",
             "lex_cos"],
            a=_IDS, b=_IDS, same_region={"type": "boolean"}, shared_blocks={"type": "integer"},
            frag_sim={"type": "number"}, shared_transitions={"type": "integer"}, shared_designations=STRS,
            lex_cos={"type": "number"})},
    ),
}

# ---------------------------------------------------------------------------
# Decision schema (model-facing).  Transport-safe vocabulary only.
# ---------------------------------------------------------------------------
_HINT_KEY = _obj(["region_id", "hint_id"], region_id=S, hint_id=S)
_REL = ["ANCHOR", "MANIFESTATION_OF", "FACET_OF", "OTHER_REPRESENTATION_OF", "IDENTICAL_REMINE"]
_BASIS = ["SAME_SOURCE_BLOCK", "SAME_GENERAL_REQUIREMENT_TEXT", "SAME_OLD_NEW_TRANSITION",
          "SAME_DECISION_DIFFERENT_LOCATION", "FACET_OF_ONE_DECISION", "SAME_EVENT_OTHER_REPRESENTATION"]
FLAGS = ["COMPOSITE_CARD", "DOCUMENTARY_SUSPECTED", "CONFLICT_MATERIAL", "NEEDS_SOURCE"]
GROUP = _obj(
    ["group_id", "decision", "member_refs", "relations", "merge_basis", "distinguishing_check",
     "uncertainty_reason", "flags", "channel", "related_engineering_card_ref"],
    group_id=S, decision={"enum": [MERGE, KEEP_SEPARATE, UNCERTAIN]},
    member_refs={"type": "array", "minItems": 1, "maxItems": MAX_CLUSTER_CARDS, "items": S},
    relations={"type": "array", "items": _obj(["card_ref", "relation"], card_ref=S, relation={"enum": _REL})},
    merge_basis={"type": "array", "items": {"enum": _BASIS}},
    distinguishing_check=S, uncertainty_reason=S,
    flags={"type": "array", "items": {"enum": FLAGS}},
    channel={"enum": [ENGINEERING, DOCUMENTARY, REVIEW]},
    related_engineering_card_ref=S,
)
RECOMPOSITION = _obj(
    ["group_id", "engineering_subject", "system_designations", "scope", "change_summary", "old_state", "new_state",
     "why_one_event", "parameters", "duplicate_parameter_refs", "manifestations", "open_conflicts", "claim_status"],
    group_id=S, engineering_subject=S, system_designations=STRS,
    scope=_obj(["scope_type", "scope_basis", "affected_location_ids", "basis_evidence_refs"],
               scope_type={"enum": list(SCOPE_TYPES)}, scope_basis={"enum": list(SCOPE_BASES)},
               affected_location_ids=STRS, basis_evidence_refs=STRS),
    change_summary=S, old_state=S, new_state=S, why_one_event=S,
    parameters={"type": "array", "items": _obj(
        ["param_key", "name", "sources", "status", "applies_to_location_ids"],
        param_key=S, name=S, sources={"type": "array", "minItems": 1, "items": S},
        status={"enum": ["CONSISTENT", "LOCATION_VARIANT", "CONFLICT"]}, applies_to_location_ids=STRS)},
    duplicate_parameter_refs={"type": "array", "items": _obj(["ref", "duplicate_of"], ref=S, duplicate_of=S)},
    manifestations={"type": "array", "minItems": 1, "items": _obj(
        ["location_ids", "member_refs", "local_note", "local_param_keys"],
        location_ids={"type": "array", "minItems": 1, "items": S}, member_refs={"type": "array", "minItems": 1, "items": S},
        local_note=S, local_param_keys=STRS)},
    open_conflicts={"type": "array", "items": _obj(
        ["source", "hint_key", "member_param_refs", "statement", "affected_param_keys"],
        source={"enum": ["HINT", "MEMBER_DISAGREEMENT", "MEMBER_TEXT"]}, hint_key=_HINT_KEY,
        member_param_refs=STRS, statement=S, affected_param_keys=STRS)},
    claim_status={"enum": ["SUPPORTED_BY_MEMBERS", "CONTAINS_UNRESOLVED_CONFLICT", "PARTIALLY_SUPPORTED_BY_MEMBERS"]},
)
DECISION_SCHEMA: dict[str, Any] = _obj(
    ["cluster_id", "groups", "recompositions", "conflict_dispositions"],
    cluster_id=S,
    groups={"type": "array", "minItems": 1, "items": GROUP},
    recompositions={"type": "array", "items": RECOMPOSITION},
    conflict_dispositions={"type": "array", "items": _obj(
        ["hint_key", "group_id", "disposition", "reason"], hint_key=_HINT_KEY, group_id=S,
        disposition={"enum": ["MATERIAL_REPRESENTED", "NOT_MATERIAL"]}, reason=S)},
)

INPUT_SCHEMA_SHA256 = sha256_json(INPUT_SCHEMA)
DECISION_SCHEMA_SHA256 = sha256_json(DECISION_SCHEMA)
