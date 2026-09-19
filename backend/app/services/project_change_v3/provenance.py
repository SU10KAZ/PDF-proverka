"""Technical provenance for V3 production artifacts (engine versions only)."""
from __future__ import annotations

from typing import Any

from .contracts import (
    DEDUPE_PROMPT_VERSION,
    ENGINE_NAME,
    ENGINE_VERSION,
    MAPPER_PROMPT_VERSION,
    MINER_PROMPT_VERSION,
    MODEL,
    REASONING,
)


def build_provenance(*, source_prep_version: str, **extra: Any) -> dict[str, Any]:
    """Return provenance dict for attachment on result artifacts.

    Values are engine/prompt versions ? never research evaluation truth.
    """
    out: dict[str, Any] = {
        "engine": ENGINE_NAME,
        "engine_version": ENGINE_VERSION,
        "mapper_prompt_version": MAPPER_PROMPT_VERSION,
        "miner_prompt_version": MINER_PROMPT_VERSION,
        "dedupe_version": DEDUPE_PROMPT_VERSION,
        "model": MODEL,
        "reasoning": REASONING,
        "source_prep_version": source_prep_version,
    }
    out.update(extra)
    return out


def provenance_lines(prov: dict[str, Any]) -> list[str]:
    """Presentation-friendly technical_provenance strings."""
    return [
        f"engine: {prov.get('engine')}",
        f"engine_version: {prov.get('engine_version')}",
        f"mapper_prompt_version: {prov.get('mapper_prompt_version')}",
        f"miner_prompt_version: {prov.get('miner_prompt_version')}",
        f"dedupe_version: {prov.get('dedupe_version')}",
        f"model: {prov.get('model')}",
        f"reasoning: {prov.get('reasoning')}",
        f"source_prep_version: {prov.get('source_prep_version')}",
    ]
