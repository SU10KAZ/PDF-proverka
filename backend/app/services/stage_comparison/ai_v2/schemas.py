"""Strict typed contracts for AI Analyst v2.

The transport schema is intentionally boring: every field is required and
free-form nested objects are forbidden.  Type-specific verdict constraints
are checked a second time by :mod:`verifier`; this protects cached and test
responses in addition to live structured output.
"""
from __future__ import annotations

SCHEMA_VERSION = "stage-comparison-ai-analyst-v2.response.v1"
PROMPT_VERSION = "stage-comparison-ai-analyst-v2.compact-context.v3"

ENTITY_IDENTITY = "ENTITY_IDENTITY"
TABLE_ROW_IDENTITY = "TABLE_ROW_IDENTITY"
FUNCTIONAL_IDENTITY = "FUNCTIONAL_IDENTITY"
LABEL_CONFLICT = "LABEL_CONFLICT"
DOCUMENT_INCONSISTENCY_REVIEW = "DOCUMENT_INCONSISTENCY_REVIEW"
MODE_RELATION = "MODE_RELATION"
CHANGE_INTERPRETATION = "CHANGE_INTERPRETATION"
NEED_MORE_EVIDENCE = "NEED_MORE_EVIDENCE"
UNRESOLVABLE = "UNRESOLVABLE"

TASK_TYPES = (
    ENTITY_IDENTITY,
    TABLE_ROW_IDENTITY,
    FUNCTIONAL_IDENTITY,
    LABEL_CONFLICT,
    DOCUMENT_INCONSISTENCY_REVIEW,
    MODE_RELATION,
    CHANGE_INTERPRETATION,
    NEED_MORE_EVIDENCE,
    UNRESOLVABLE,
)

VERDICTS_BY_TYPE = {
    ENTITY_IDENTITY: ("SAME_ENTITY", "DIFFERENT_ENTITY", "INSUFFICIENT_EVIDENCE"),
    TABLE_ROW_IDENTITY: (
        "SAME_ENTITY", "DIFFERENT_ENTITY", "INSUFFICIENT_EVIDENCE"
    ),
    FUNCTIONAL_IDENTITY: (
        "SAME_ENTITY", "DIFFERENT_ENTITY", "INSUFFICIENT_EVIDENCE"
    ),
    LABEL_CONFLICT: (
        "DOCUMENT_ERROR", "INTENTIONAL_DIFFERENCE", "INSUFFICIENT_EVIDENCE"
    ),
    DOCUMENT_INCONSISTENCY_REVIEW: (
        "CONFIRMED_CONTRADICTION", "NOT_A_CONTRADICTION",
        "INSUFFICIENT_EVIDENCE",
    ),
    MODE_RELATION: ("EQUIVALENT", "DIFFERENT", "INSUFFICIENT_EVIDENCE"),
    CHANGE_INTERPRETATION: (
        "SUPPORTED_CHANGE", "FORMATTING_ONLY", "INSUFFICIENT_EVIDENCE"
    ),
    NEED_MORE_EVIDENCE: ("NEED_MORE_EVIDENCE",),
    UNRESOLVABLE: ("UNRESOLVABLE",),
}

ALL_VERDICTS = tuple(sorted({
    value for values in VERDICTS_BY_TYPE.values() for value in values
}))

RESOLUTION_STATUSES = ("RESOLVED", "NEED_MORE_EVIDENCE", "UNRESOLVABLE")
CONFIDENCE_LEVELS = ("HIGH", "MEDIUM", "LOW", "UNKNOWN")

EXPANSION_ALLOWLIST = (
    "neighboring_rows",
    "neighboring_entities",
    "summary_row",
    "opposite_section_peer",
    "graph_neighbors",
    "larger_text_window",
    "bounded_image_crop",
)

CLAIM_KINDS = (
    "IDENTITY_FEATURE",
    "VALUE",
    "GRAPH_RELATION",
    "ARITHMETIC",
)
CLAIM_ATTRIBUTES = (
    "canonical_identity",
    "designation",
    "label",
    "node_type",
    "section",
    "mode",
    "facet",
    "value",
    "unit",
    "relation",
    "count",
)
ARITHMETIC_OPERATIONS = ("NONE", "SUM", "DIFFERENCE", "PRODUCT", "RATIO")

_OPERAND = {
    "type": "object",
    "additionalProperties": False,
    "required": ["evidence_ref", "value"],
    "properties": {
        "evidence_ref": {"type": "string"},
        "value": {"type": "number"},
    },
}

_CLAIM = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "kind", "subject_ref", "object_ref", "attribute", "value", "unit",
        "operation", "operands", "expected", "evidence_refs",
    ],
    "properties": {
        "kind": {"type": "string", "enum": list(CLAIM_KINDS)},
        "subject_ref": {"type": ["string", "null"]},
        "object_ref": {"type": ["string", "null"]},
        "attribute": {"type": ["string", "null"], "enum": [
            *CLAIM_ATTRIBUTES, None,
        ]},
        "value": {"type": ["string", "number", "null"]},
        "unit": {"type": ["string", "null"]},
        "operation": {"type": "string", "enum": list(ARITHMETIC_OPERATIONS)},
        "operands": {"type": "array", "items": _OPERAND},
        "expected": {"type": ["number", "null"]},
        "evidence_refs": {"type": "array", "items": {"type": "string"}},
    },
}

_RESOLUTION = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "task_id", "task_type", "status", "verdict",
        "selected_candidate_refs", "evidence_refs", "claims", "confidence",
        "requested_evidence", "engineering_summary", "human_question",
    ],
    "properties": {
        "task_id": {"type": "string"},
        "task_type": {"type": "string", "enum": list(TASK_TYPES)},
        "status": {"type": "string", "enum": list(RESOLUTION_STATUSES)},
        "verdict": {"type": "string", "enum": list(ALL_VERDICTS)},
        "selected_candidate_refs": {
            "type": "array", "items": {"type": "string"},
        },
        "evidence_refs": {"type": "array", "items": {"type": "string"}},
        "claims": {"type": "array", "items": _CLAIM},
        "confidence": {"type": "string", "enum": list(CONFIDENCE_LEVELS)},
        "requested_evidence": {
            "type": "array",
            "items": {"type": "string", "enum": list(EXPANSION_ALLOWLIST)},
        },
        "engineering_summary": {"type": "string"},
        "human_question": {"type": ["string", "null"]},
    },
}

ANALYST_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["resolutions"],
    "properties": {
        "resolutions": {"type": "array", "items": _RESOLUTION},
    },
}


def task_schema(task_type: str) -> dict:
    """Return a standalone strict schema for one typed task.

    It is useful for contract tests and for future per-type transports.  The
    live whole-document batch still uses ``ANALYST_SCHEMA`` and the verifier
    enforces these narrower enums per item.
    """
    if task_type not in VERDICTS_BY_TYPE:
        raise ValueError(f"unknown task type: {task_type}")
    resolution = {
        **_RESOLUTION,
        "properties": {
            **_RESOLUTION["properties"],
            "task_type": {"type": "string", "const": task_type},
            "verdict": {
                "type": "string", "enum": list(VERDICTS_BY_TYPE[task_type]),
            },
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["resolution"],
        "properties": {"resolution": resolution},
    }


TASK_SCHEMAS = {task_type: task_schema(task_type) for task_type in TASK_TYPES}

__all__ = [
    "ANALYST_SCHEMA",
    "ARITHMETIC_OPERATIONS",
    "CHANGE_INTERPRETATION",
    "CLAIM_ATTRIBUTES",
    "CLAIM_KINDS",
    "DOCUMENT_INCONSISTENCY_REVIEW",
    "ENTITY_IDENTITY",
    "EXPANSION_ALLOWLIST",
    "FUNCTIONAL_IDENTITY",
    "LABEL_CONFLICT",
    "MODE_RELATION",
    "NEED_MORE_EVIDENCE",
    "PROMPT_VERSION",
    "SCHEMA_VERSION",
    "TABLE_ROW_IDENTITY",
    "TASK_SCHEMAS",
    "TASK_TYPES",
    "UNRESOLVABLE",
    "VERDICTS_BY_TYPE",
    "task_schema",
]
