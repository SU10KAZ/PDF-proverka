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
    REASONING,
    SCHEMA_VERSION,
    SOURCE_PACKAGING_VERSION,
    UNMATCHED_PROMPT_SHA256,
    VERIFY_PROMPT_SHA256,
)
from .contracts import active_profile
from .transport import provider_transport_version


def build_provenance(*, source_prep_version: str | None = None, **extra: Any) -> dict[str, Any]:
    profile = active_profile()
    transport_version = provider_transport_version()
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
        "unmatched_prompt_sha256": UNMATCHED_PROMPT_SHA256,
        "verification_prompt_sha256": VERIFY_PROMPT_SHA256,
        "source_packaging_version": source_prep_version or SOURCE_PACKAGING_VERSION,
        "source_prep_version": source_prep_version or SOURCE_PACKAGING_VERSION,
        # The profile of THIS run (3.9.0): the startup default unless the run chose one.
        "engine_variant": profile.engine_variant,
        "provider": profile.provider,
        "model": profile.model,
        "model_profile": profile.key,
        "reasoning": REASONING,
        "thinking": profile.thinking,
        "provider_transport_version": transport_version,
        "transport_version": transport_version,
    }
    out.update(extra)
    return out


def provenance_lines(prov: dict[str, Any]) -> list[str]:
    handoff = prov.get("model_handoff") or {}
    lines = []
    if handoff:
        lines.append(
            f"Досборка: Mapper/Miner/unmatched — {handoff['source_model']['model']}; "
            f"Dedupe/проверка параметров — {handoff['remaining_model']['model']}"
        )
    return lines + [
        f"engine: {prov.get('engine')}",
        f"engine_version: {prov.get('engine_version')}",
        f"engine_variant: {prov.get('engine_variant')}",
        f"provider: {prov.get('provider')}",
        f"schema_version: {prov.get('schema_version')}",
        f"mapper_prompt_version: {prov.get('mapper_prompt_version')}",
        f"miner_prompt_version: {prov.get('miner_prompt_version')}",
        f"dedupe_version: {prov.get('dedupe_version')}",
        f"unmatched_prompt_sha256: {prov.get('unmatched_prompt_sha256')}",
        f"verification_prompt_sha256: {prov.get('verification_prompt_sha256')}",
        f"source_packaging_version: {prov.get('source_packaging_version')}",
        f"model: {prov.get('model')}",
        f"reasoning: {prov.get('reasoning')}",
        f"provider_transport_version: {prov.get('provider_transport_version')}",
        f"oversize_transport_calls: {sum(1 for c in prov.get('transport_calls') or [] if c.get('oversize_transport_used'))}",
    ]
