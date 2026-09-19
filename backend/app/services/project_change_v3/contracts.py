"""Versioned schemas and prompts for Project Comparison V3.

Self-contained production contracts. No Pair A/B schema limits.
No runtime imports from research trees.
"""
from __future__ import annotations

import hashlib
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
ENGINE_VERSION = "3.4.0"
SCHEMA_VERSION = "projectchange_v3_schema/1"
# /2: frozen-V3 parity — content-SHA image identity, frozen block-type table,
# fail-closed on missing Markdown/bbox/page_index/unknown type.
SOURCE_PACKAGING_VERSION = "projectchange_v3_source_pack/2"
MODEL = "gpt-6-astra"
REASONING = "xhigh"

_PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _load_prompt(name: str) -> str:
    return (_PROMPTS_DIR / name).read_text(encoding="utf-8")


MAPPER_PROMPT = _load_prompt("MAPPER_PROMPT.txt")
MINER_PROMPT = _load_prompt("MINER_PROMPT.txt")
DEDUPE_PROMPT = _load_prompt("DEDUPE_PROMPT.txt")

MAPPER_PROMPT_SHA256 = _sha256_text(MAPPER_PROMPT)
MINER_PROMPT_SHA256 = _sha256_text(MINER_PROMPT)
DEDUPE_PROMPT_SHA256 = _sha256_text(DEDUPE_PROMPT)

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
}
