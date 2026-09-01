"""Closed contracts for backend candidates and the tiny selector response."""
from __future__ import annotations

FACTORY_VERSION = "stage-comparison-ai-v3-candidate-factory.v1"
CANDIDATE_SCHEMA_VERSION = "stage-comparison-ai-v3-candidates.v1"
BUNDLE_SCHEMA_VERSION = "stage-comparison-ai-v3-candidate-bundles.v1"
SELECTOR_SCHEMA_VERSION = "stage-comparison-ai-v3-selector.response.v1"
PROMPT_VERSION = "stage-comparison-ai-v3-bounded-selector.v1"
VERIFIER_VERSION = "stage-comparison-ai-v3-selection-verifier.v1"
RUN_SCHEMA_VERSION = "stage-comparison-ai-v3-run.v1"

ENTITY_IDENTITY = "ENTITY_IDENTITY"
TABLE_ROW_IDENTITY = "TABLE_ROW_IDENTITY"
CHANGE_INTERPRETATION = "CHANGE_INTERPRETATION"
TEXT_EQUIVALENCE = "TEXT_EQUIVALENCE"
LABEL_CONFLICT = "LABEL_CONFLICT"
MODE_MAPPING = "MODE_MAPPING"

TASK_TYPES = (
    ENTITY_IDENTITY,
    TABLE_ROW_IDENTITY,
    CHANGE_INTERPRETATION,
    TEXT_EQUIVALENCE,
    LABEL_CONFLICT,
    MODE_MAPPING,
)

AUTO = "ELIGIBLE_FOR_AUTO_RESOLUTION"
ADVISORY = "ADVISORY_ONLY"
INVALID = "INVALID"
ELIGIBILITY = (AUTO, ADVISORY, INVALID)

HIGH = "HIGH"
MEDIUM = "MEDIUM"
LOW = "LOW"
CONFIDENCE_BUCKETS = (HIGH, MEDIUM, LOW)

VERIFIED_SELECTION = "VERIFIED_SELECTION"
REJECTED_SELECTION = "REJECTED_SELECTION"
HUMAN_REQUIRED = "HUMAN_REQUIRED"
INVALID_RESPONSE = "INVALID_RESPONSE"


def selector_schema(candidate_ids: list[str]) -> dict:
    """Return a strict schema; arbitrary candidate IDs are impossible."""
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["selections"],
        "properties": {
            "selections": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "task_id",
                        "selected_candidate_id",
                        "confidence_bucket",
                        "optional_short_reason",
                    ],
                    "properties": {
                        "task_id": {"type": "string"},
                        "selected_candidate_id": {
                            "type": "string",
                            "enum": sorted(set(candidate_ids)),
                        },
                        "confidence_bucket": {
                            "type": "string",
                            "enum": list(CONFIDENCE_BUCKETS),
                        },
                        "optional_short_reason": {
                            "type": "string",
                            "maxLength": 240,
                        },
                    },
                },
            }
        },
    }


__all__ = [name for name in globals() if name.isupper()] + ["selector_schema"]
