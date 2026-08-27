"""Deterministic G2.4.6 unified change synthesis."""

from .contract import (
    DIRECTION,
    IDENTITY_VERSION,
    INPUT_VERSION,
    KIND,
    SYNTHESIS_VERSION,
    SynthesisValidationError,
    normalize_candidate,
    normalize_source_artifact,
    normalize_synthesis_atom,
)
from .identity import (
    canonical_atomic_identity,
    content_signature,
    stable_atomic_change_id,
    stable_group_id,
    stable_review_item_id,
)
from .normalization import (
    ledger_to_graphic_atoms,
    normalize_atoms,
    stage53_to_text_atoms,
)
from .presentation import PRESENTATION_VERSION, build_presentation_groups
from .synthesizer import (
    OBSERVATION_ONLY_GATES,
    STRICT_MERGE_GATES,
    SYNTHESIZER_VERSION,
    synthesize_unified_changes,
)
from .validation import schema_path, validate_synthesis

__all__ = [
    "DIRECTION",
    "IDENTITY_VERSION",
    "INPUT_VERSION",
    "KIND",
    "OBSERVATION_ONLY_GATES",
    "PRESENTATION_VERSION",
    "STRICT_MERGE_GATES",
    "SYNTHESIS_VERSION",
    "SYNTHESIZER_VERSION",
    "SynthesisValidationError",
    "canonical_atomic_identity",
    "content_signature",
    "build_presentation_groups",
    "ledger_to_graphic_atoms",
    "normalize_atoms",
    "normalize_candidate",
    "normalize_source_artifact",
    "normalize_synthesis_atom",
    "schema_path",
    "stable_atomic_change_id",
    "stable_group_id",
    "stable_review_item_id",
    "stage53_to_text_atoms",
    "synthesize_unified_changes",
    "validate_synthesis",
]
