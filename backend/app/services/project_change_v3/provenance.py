"""Technical provenance for V3 production artifacts."""
from __future__ import annotations

from typing import Any

from .contracts import (
    DEDUPE_PROMPT_SHA256,
    DEDUPE_VERSION,
    ENGINE_NAME,
    ENGINE_VERSION,
    MAPPER_PROMPT_SHA256,
    MAPPER_PROMPT_VERSION,
    MINER_PROMPT_SHA256,
    MINER_PROMPT_VERSION,
    MODEL,
    REASONING,
    SCHEMA_VERSION,
    SOURCE_PACKAGING_VERSION,
)


def build_provenance(*, source_prep_version: str | None = None, **extra: Any) -> dict[str, Any]:
    out: dict[str, Any] = {
        "engine": ENGINE_NAME,
        "engine_version": ENGINE_VERSION,
        "schema_version": SCHEMA_VERSION,
        "mapper_prompt_version": MAPPER_PROMPT_VERSION,
        "miner_prompt_version": MINER_PROMPT_VERSION,
        "dedupe_version": DEDUPE_VERSION,
        "mapper_prompt_sha256": MAPPER_PROMPT_SHA256,
        "miner_prompt_sha256": MINER_PROMPT_SHA256,
        "dedupe_prompt_sha256": DEDUPE_PROMPT_SHA256,
        "source_packaging_version": source_prep_version or SOURCE_PACKAGING_VERSION,
        "source_prep_version": source_prep_version or SOURCE_PACKAGING_VERSION,
        "model": MODEL,
        "reasoning": REASONING,
    }
    out.update(extra)
    return out


def provenance_lines(prov: dict[str, Any]) -> list[str]:
    return [
        f"engine: {prov.get('engine')}",
        f"engine_version: {prov.get('engine_version')}",
        f"schema_version: {prov.get('schema_version')}",
        f"mapper_prompt_version: {prov.get('mapper_prompt_version')}",
        f"miner_prompt_version: {prov.get('miner_prompt_version')}",
        f"dedupe_version: {prov.get('dedupe_version')}",
        f"source_packaging_version: {prov.get('source_packaging_version')}",
        f"model: {prov.get('model')}",
        f"reasoning: {prov.get('reasoning')}",
    ]
