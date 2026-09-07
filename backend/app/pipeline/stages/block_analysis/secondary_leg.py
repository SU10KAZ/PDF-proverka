"""Production Stage 01 secondary-leg routing.

The selector is deliberately small: explicit values choose a two-leg control
or Astra route, while an unset selector leaves the exact legacy topology to the
caller.  No prompt text, retrieval implementation, schema, judge or
evidence-gate policy lives here.
"""
from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from typing import Mapping


SECOND_LEG_ENV = "AUDIT_SECOND_LEG"
# Internal resolver fallback after the caller's presence gate.  This is the
# two-leg control profile, not the rollback mode; unset is handled by callers.
DEFAULT_SECOND_LEG = "gpt54"
PRODUCTION_RETRIEVAL_PROFILE = "production"
DETERMINISTIC_EVIDENCE_GATE = "finding_evidence_gate"


@dataclass(frozen=True)
class SecondaryLegConfig:
    alias: str
    leg_name: str
    provider: str
    model: str
    prompt_profile: str
    reasoning_effort: str
    retrieval_profile: str
    gate: str

    def telemetry(self) -> dict[str, str]:
        return asdict(self)


_PROFILES = {
    "gpt54": SecondaryLegConfig(
        alias="gpt54",
        leg_name="secondary",
        provider="openrouter",
        model="openai/gpt-5.4",
        prompt_profile="production",
        reasoning_effort="low",
        retrieval_profile=PRODUCTION_RETRIEVAL_PROFILE,
        gate=DETERMINISTIC_EVIDENCE_GATE,
    ),
    "astra": SecondaryLegConfig(
        alias="astra",
        leg_name="secondary",
        provider="codex",
        model="codex/gpt-6-astra",
        prompt_profile="astra_shadow_v2",
        reasoning_effort="low",
        retrieval_profile=PRODUCTION_RETRIEVAL_PROFILE,
        gate=DETERMINISTIC_EVIDENCE_GATE,
    ),
}


def resolve_secondary_leg(
    value: str | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> SecondaryLegConfig:
    """Resolve the production secondary leg, rejecting unknown values early."""
    source = os.environ if env is None else env
    raw = source.get(SECOND_LEG_ENV, DEFAULT_SECOND_LEG) if value is None else value
    alias = str(raw).strip().lower()
    try:
        return _PROFILES[alias]
    except KeyError as exc:
        allowed = ", ".join(sorted(_PROFILES))
        raise ValueError(
            f"{SECOND_LEG_ENV}={raw!r}: expected one of {allowed}"
        ) from exc


__all__ = [
    "DEFAULT_SECOND_LEG",
    "DETERMINISTIC_EVIDENCE_GATE",
    "PRODUCTION_RETRIEVAL_PROFILE",
    "SECOND_LEG_ENV",
    "SecondaryLegConfig",
    "resolve_secondary_leg",
]
