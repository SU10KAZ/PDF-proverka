"""Versioned semantic-observability policy for graphic comparison routes."""
from __future__ import annotations


POLICY_VERSION = "graphic-coverage-policy-v2"

DIMENSIONS = (
    "STRUCTURE",
    "CONNECTION",
    "TYPE",
    "QUANTITY",
    "PARAMETER",
    "METHOD",
    "PRINCIPLE",
    "SPACE",
)

MODE2_OBSERVABLE_DIMENSIONS = frozenset(
    {"STRUCTURE", "CONNECTION", "TYPE", "QUANTITY"}
)
ENTITY_OBSERVABLE_DIMENSIONS = frozenset({"STRUCTURE", "CONNECTION", "TYPE"})
UNSUPPORTED_SEMANTIC_DIMENSIONS = frozenset(
    {"PARAMETER", "METHOD", "PRINCIPLE", "SPACE"}
)


def public_policy() -> dict:
    return {
        "version": POLICY_VERSION,
        "dimensions": list(DIMENSIONS),
        "routes": {
            "MODE_2": {
                "observable": sorted(MODE2_OBSERVABLE_DIMENSIONS),
                "entity_observable": sorted(ENTITY_OBSERVABLE_DIMENSIONS),
                "quantity_subject": "repeated_group_only; individual entities are not applicable",
                "not_applicable": sorted(UNSUPPORTED_SEMANTIC_DIMENSIONS),
                "basis": "SYSTEM_GRAPH comparison and its existing quality gate",
            },
            "MODE_1": {
                "observable": [],
                "not_applicable": list(DIMENSIONS),
                "basis": "local graphic delta is not semantic SYSTEM_GRAPH coverage",
            },
        },
    }


__all__ = [
    "DIMENSIONS",
    "ENTITY_OBSERVABLE_DIMENSIONS",
    "MODE2_OBSERVABLE_DIMENSIONS",
    "POLICY_VERSION",
    "UNSUPPORTED_SEMANTIC_DIMENSIONS",
    "public_policy",
]
