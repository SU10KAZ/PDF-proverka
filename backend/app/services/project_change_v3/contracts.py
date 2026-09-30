"""Versioned schemas and prompts for Project Comparison V3.

Self-contained production contracts. No Pair A/B schema limits.
No runtime imports from research trees.
"""
from __future__ import annotations

import hashlib
import os
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ENGINE_NAME = "projectchange_v3"
# 3.2.0: frozen-V3 packaging parity, semantic HM membership, fail-closed states,
# presentation adapter.  Prompts/model/reasoning unchanged.
# 3.3.0: lossless versioned provider transport for payloads above one Codex
#        turn (transport.py); per-call transport receipts in provenance.
# 3.4.0: one bounded Miner retry after an evidence-traceability rejection
#        (identical model-visible input, receipts of both attempts); per-call
#        token usage in the transport receipts (exec path: `codex exec --json`).
# 3.4.1: job state only — RUNNING names the working stage (mapping / region i
#        of n / dedupe) and a user cancel stops the run (FAILED, v3_cancelled).
#        What the model sees, and every result field, is unchanged from 3.4.0.
# 3.5.0: provider switch — EVERY model stage (Mapper, Miner, Dedupe) runs on
#        Claude Opus through the Claude Code CLI subscription transport.  The
#        prompts, schemas, source packaging, evidence selection, dedupe and
#        confidence rules are byte-for-byte those of 3.4.x; only the model and
#        the transport that carries the same payload changed.  A 3.5.x result
#        is never comparable call-for-call with a gpt-6-astra result.
# 3.5.1: technical isolation and persistence only — a call in which any model
#        besides claude-opus-5 took part is rejected (provider_auxiliary_model_used);
#        every accepted Miner region is checkpointed atomically at once, so a
#        later failure does not lose paid answers (the checkpoint is an internal
#        run artifact, never a published result; nothing resumes from it).
#        Prompts, schemas, packaging and every result field of 3.5.0 unchanged.
# 3.5.2: every completed Miner answer, accepted or rejected, is written to an
#        append-only run-scoped attempt store BEFORE validation decides anything;
#        the structural rejection is reported under separate codes
#        (MINER_PAGE_OUTSIDE_REGION / MINER_ONE_SIDED_PROJECTCHANGE) and gets the
#        same single identical retry as a provenance rejection.  The acceptance
#        rule, the prompts, the schemas and the packaging are unchanged.
# 3.6.0: explicit startup provider selection; algorithms and prompts unchanged.
# 3.7.0: provider-independent page coverage and claim-quality ledgers; bounded
#        unmatched-content review and independent source verification.  The
#        frozen Mapper, ordinary Miner and Dedupe contracts remain unchanged.
# 3.8.0: deterministic layers behind flags that are OFF by default (0 model calls):
#        lossless Dedupe merge (PROJECT_COMPARISON_V3_DEDUPE_LOSSLESS) and source checks
#        (source_checks.py: ABSENCE / NUMERIC / OPTION / DOCUMENTARY / TABLE_ROWS).
#        With every flag off the result is byte-identical to 3.7.0; prompts, schemas
#        and model calls are unchanged.
# 3.9.0: the model is chosen per run from an allowlist of profiles (``astra`` =
#        gpt-6-astra via Codex, ``opus55`` = claude-opus-5-5 via Claude Code CLI),
#        still ONE model for every stage of the run.  Without a choice the
#        startup default applies, so a run without it is identical to 3.8.0.
#        Prompts, schemas, packaging and validation are unchanged.
# 3.10.0: portioned Mapper (mapper_portions.py) — when Claude CLI cannot carry
#        the Mapper call whole (>100 images or >24 MiB of images), NEW is split
#        into portions (assembly projects, else page runs), OLD goes whole into
#        each; each answer is validated, then merged into one map of the same
#        schema.  A pair that fits one call is unchanged byte for byte.
ENGINE_VERSION = "3.11.0"
SCHEMA_VERSION = "projectchange_v3_schema/1"
# /2: frozen-V3 parity — content-SHA image identity, frozen block-type table,
# fail-closed on missing Markdown/bbox/page_index/unknown type.
SOURCE_PACKAGING_VERSION = "projectchange_v3_source_pack/2"
# One model configuration for the whole run: no stage may use another model.
# Read once at process startup: a running analysis cannot change model halfway.
PROVIDER_SELECTION = os.environ.get("PROJECT_COMPARISON_V3_PROVIDER", "claude").strip().lower()
if PROVIDER_SELECTION not in {"claude", "codex"}:
    raise ValueError("PROJECT_COMPARISON_V3_PROVIDER must be claude or codex")
PROVIDER = "codex_cli_subscription" if PROVIDER_SELECTION == "codex" else "claude_code_cli_subscription"
MODEL = "gpt-6-astra" if PROVIDER_SELECTION == "codex" else "claude-opus-5"
ENGINE_VARIANT = "ProjectChange V3 / Astra" if PROVIDER_SELECTION == "codex" else "ProjectChange V3 / Opus"
REASONING = "xhigh"
THINKING = {"type": "reasoning" if PROVIDER_SELECTION == "codex" else "adaptive", "effort": REASONING}


# ---- Per-run model profile (3.9.0) --------------------------------------------
# The constants above stay the STARTUP DEFAULT.  A run may pick another profile
# from the allowlist; the choice holds for the whole run (every stage, the
# readiness check and the provenance), never for part of it.
@dataclass(frozen=True)
class ModelProfile:
    key: str
    label: str
    selection: str  # "codex" | "claude"
    provider: str
    model: str
    engine_variant: str

    @property
    def thinking(self) -> dict[str, str]:
        return {"type": "reasoning" if self.selection == "codex" else "adaptive", "effort": REASONING}


MODEL_PROFILES: dict[str, ModelProfile] = {
    "astra": ModelProfile("astra", "Astra 6 (gpt-6-astra)", "codex", "codex_cli_subscription",
                          "gpt-6-astra", "ProjectChange V3 / Astra"),
    "opus55": ModelProfile("opus55", "Opus 5.5 (claude-opus-5-5)", "claude", "claude_code_cli_subscription",
                           "claude-opus-5-5", "ProjectChange V3 / Opus 5.5"),
    # The pre-3.9 Claude default; kept so PROVIDER=claude behaves exactly as before.
    "opus5": ModelProfile("opus5", "Opus 5 (claude-opus-5)", "claude", "claude_code_cli_subscription",
                          "claude-opus-5", "ProjectChange V3 / Opus"),
}
DEFAULT_PROFILE_KEY = "astra" if PROVIDER_SELECTION == "codex" else "opus5"


def profile_choices() -> list[ModelProfile]:
    """Profiles the UI may offer: ``PROJECT_COMPARISON_V3_MODEL_CHOICES`` (comma list).

    The startup default is always offered and listed first; unknown keys fail
    loudly instead of silently vanishing from the menu.
    """
    raw = os.environ.get("PROJECT_COMPARISON_V3_MODEL_CHOICES", "")
    keys = [key.strip().lower() for key in raw.split(",") if key.strip()]
    unknown = [key for key in keys if key not in MODEL_PROFILES]
    if unknown:
        raise ValueError(f"PROJECT_COMPARISON_V3_MODEL_CHOICES: unknown profiles {unknown}")
    ordered = [DEFAULT_PROFILE_KEY, *[key for key in keys if key != DEFAULT_PROFILE_KEY]]
    return [MODEL_PROFILES[key] for key in ordered]


def resolve_profile(key: str | None) -> ModelProfile:
    """The profile a run asked for; ``None`` is the startup default."""
    if not key:
        return MODEL_PROFILES[DEFAULT_PROFILE_KEY]
    allowed = {profile.key: profile for profile in profile_choices()}
    if key not in allowed:
        raise ValueError(f"model profile {key!r} is not allowed on this installation")
    return allowed[key]


_ACTIVE_PROFILE: ContextVar[ModelProfile | None] = ContextVar("projectchange_v3_model_profile", default=None)


def active_profile() -> ModelProfile:
    return _ACTIVE_PROFILE.get() or MODEL_PROFILES[DEFAULT_PROFILE_KEY]


@contextmanager
def use_profile(profile: ModelProfile):
    """Bind one profile to the current run (the engine runs in one thread)."""
    token = _ACTIVE_PROFILE.set(profile)
    try:
        yield profile
    finally:
        _ACTIVE_PROFILE.reset(token)

_PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _load_prompt(name: str) -> str:
    return (_PROMPTS_DIR / name).read_text(encoding="utf-8")


MAPPER_PROMPT = _load_prompt("MAPPER_PROMPT.txt")
MINER_PROMPT = _load_prompt("MINER_PROMPT.txt")
DEDUPE_PROMPT = _load_prompt("DEDUPE_PROMPT.txt")
UNMATCHED_PROMPT = _load_prompt("UNMATCHED_PROMPT.txt")
VERIFY_PROMPT = _load_prompt("VERIFY_PROMPT.txt")

MAPPER_PROMPT_SHA256 = _sha256_text(MAPPER_PROMPT)
MINER_PROMPT_SHA256 = _sha256_text(MINER_PROMPT)
DEDUPE_PROMPT_SHA256 = _sha256_text(DEDUPE_PROMPT)
UNMATCHED_PROMPT_SHA256 = _sha256_text(UNMATCHED_PROMPT)
VERIFY_PROMPT_SHA256 = _sha256_text(VERIFY_PROMPT)

MAPPER_PROMPT_VERSION = MAPPER_PROMPT_SHA256
MINER_PROMPT_VERSION = MINER_PROMPT_SHA256
DEDUPE_PROMPT_VERSION = DEDUPE_PROMPT_SHA256
DEDUPE_VERSION = DEDUPE_PROMPT_SHA256


def obj(**properties: Any) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": properties,
        "required": list(properties),
        "additionalProperties": False,
    }


S = {"type": "string"}
N = {"type": "number", "minimum": 0, "maximum": 1}
STRINGS = {"type": "array", "items": S}
PAGES = {"type": "array", "items": {"type": "integer", "minimum": 1}}
PAIR_ID = {"type": "string", "minLength": 1}

BLOCK_REF = obj(
    side={"type": "string", "enum": ["OLD", "NEW"]},
    physical_page={"type": "integer", "minimum": 1},
    block_id=S,
    block_type={"type": "string", "enum": ["TEXT", "TABLE", "GRAPHIC"]},
    relevance=S,
)
REGION = obj(
    region_id=S,
    old_pages=PAGES,
    new_pages=PAGES,
    engineering_domain=S,
    scope=S,
    locations=STRINGS,
    reason_for_correspondence=S,
    important_text_blocks={"type": "array", "items": BLOCK_REF},
    important_table_blocks={"type": "array", "items": BLOCK_REF},
    important_graphic_blocks={"type": "array", "items": BLOCK_REF},
    confidence=N,
)
MAP_SCHEMA = obj(
    pair=PAIR_ID,
    regions={"type": "array", "items": REGION},
    unmatched_old=PAGES,
    unmatched_new=PAGES,
    coverage_notes=STRINGS,
)
DETAIL = obj(name=S, old_value=S, new_value=S, unit=S, location=S)
EVIDENCE = obj(
    side={"type": "string", "enum": ["OLD", "NEW"]},
    source_pdf=S,
    physical_page={"type": "integer", "minimum": 1},
    block_id=S,
    block_type={"type": "string", "enum": ["TEXT", "TABLE", "GRAPHIC"]},
    bbox={"type": "array", "items": {"type": "number"}, "minItems": 4, "maxItems": 4},
    crop_ref=S,
    relevant_fragment=S,
    evidence_role=S,
)
PROJECTCHANGE = obj(
    projectchange_id=S,
    engineering_subject=S,
    scope=S,
    locations=STRINGS,
    change_summary=S,
    old_state=S,
    new_state=S,
    changed_parameters={"type": "array", "items": DETAIL},
    old_pages=PAGES,
    new_pages=PAGES,
    evidence_items={"type": "array", "items": EVIDENCE},
    modalities={
        "type": "array",
        "items": {"type": "string", "enum": ["TEXT", "TABLE", "GRAPHIC"]},
    },
    confidence=N,
    why_one_event=S,
)
HINT = obj(
    hint_id=S,
    kind={"type": "string", "enum": ["UNRESOLVED_HINT", "SOURCE_CONFLICT"]},
    engineering_subject=S,
    suspected_change=S,
    old_pages=PAGES,
    new_pages=PAGES,
    evidence_items={"type": "array", "items": EVIDENCE},
    missing_proof_or_conflict=S,
)
MINER_SCHEMA = obj(
    pair=PAIR_ID,
    region_id=S,
    projectchanges={"type": "array", "items": PROJECTCHANGE},
    unresolved_hints={"type": "array", "items": HINT},
    coverage_notes=STRINGS,
)
DEDUPE_ITEM = obj(
    decision={"type": "string", "enum": ["KEEP_SEPARATE", "MERGE_DUPLICATES"]},
    projectchange_ids=STRINGS,
    reason=S,
)
DEDUPE_SCHEMA = obj(
    pair=PAIR_ID,
    decisions={"type": "array", "items": DEDUPE_ITEM},
    notes=STRINGS,
)

PROMPT_HASHES = {
    "MAPPER_PROMPT.txt": MAPPER_PROMPT_SHA256,
    "MINER_PROMPT.txt": MINER_PROMPT_SHA256,
    "DEDUPE_PROMPT.txt": DEDUPE_PROMPT_SHA256,
    "UNMATCHED_PROMPT.txt": UNMATCHED_PROMPT_SHA256,
    "VERIFY_PROMPT.txt": VERIFY_PROMPT_SHA256,
}

VERIFICATION_RESULT = obj(
    work_item_id=S,
    verdict={"type": "string", "enum": ["CONFIRMED", "CORRECTED", "CONFLICT", "UNREADABLE"]},
    old_value=S,
    new_value=S,
    unit=S,
    explanation=S,
    evidence_refs={
        "type": "array",
        "items": obj(
            side={"type": "string", "enum": ["OLD", "NEW"]},
            physical_page={"type": "integer", "minimum": 1},
            block_id=S,
        ),
    },
)
VERIFICATION_SCHEMA = obj(
    pair=PAIR_ID,
    results={"type": "array", "items": VERIFICATION_RESULT},
)

# ---------------------------------------------------------------------------
# V3.1-B COMPACT MINER OUTPUT — research only, OFF by default
# (``PROJECT_COMPARISON_V31_COMPACT_MINER=1``; see source_ref.py).
#
# The model sees the SAME region input as V3.  It no longer echoes the source
# metadata of each evidence item (source_pdf / block_type / bbox / crop_ref)
# nor the card index (modalities / old_pages / new_pages): it cites a block by
# side + physical_page + block_id, and the expander restores the rest from the
# run's source package, fail-closed.  After expansion the answer has the V3
# MINER_SCHEMA shape and goes through the unchanged V3 validator/dedupe/result.
# Nothing above this block is changed by it.
# ---------------------------------------------------------------------------
MINER_FORMAT_V3 = "V3"
MINER_FORMAT_V31_COMPACT = "V3.1_COMPACT"
MINER_V31_SCHEMA_VERSION = "projectchange_v31_compact_miner_schema/option2"
MINER_V31_EXPANSION_VERSION = "projectchange_v31_source_ref_expansion/1"
MINER_PROMPT_V31 = _load_prompt("MINER_PROMPT_V31.txt")
MINER_PROMPT_V31_SHA256 = _sha256_text(MINER_PROMPT_V31)

EVIDENCE_REF_V31 = obj(
    side={"type": "string", "enum": ["OLD", "NEW"]},
    physical_page={"type": "integer", "minimum": 1},
    block_id=S,
    relevant_fragment=S,
    evidence_role=S,
)
PROJECTCHANGE_V31 = obj(
    projectchange_id=S,
    engineering_subject=S,
    scope=S,
    locations=STRINGS,
    change_summary=S,
    old_state=S,
    new_state=S,
    changed_parameters={"type": "array", "items": DETAIL},
    evidence_items={"type": "array", "items": EVIDENCE_REF_V31},
    confidence=N,
    why_one_event=S,
)
# Hints keep their model-declared pages: a hint may point at a page exactly
# because the proof is missing there.
HINT_V31 = obj(
    hint_id=S,
    kind={"type": "string", "enum": ["UNRESOLVED_HINT", "SOURCE_CONFLICT"]},
    engineering_subject=S,
    suspected_change=S,
    old_pages=PAGES,
    new_pages=PAGES,
    evidence_items={"type": "array", "items": EVIDENCE_REF_V31},
    missing_proof_or_conflict=S,
)
MINER_SCHEMA_V31 = obj(
    pair=PAIR_ID,
    region_id=S,
    projectchanges={"type": "array", "items": PROJECTCHANGE_V31},
    unresolved_hints={"type": "array", "items": HINT_V31},
    coverage_notes=STRINGS,
)
