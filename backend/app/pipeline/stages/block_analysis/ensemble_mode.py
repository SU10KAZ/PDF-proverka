"""Explicit Stage 01 composition; no replacement provider and no error fallback."""
from backend.app.services.llm.openrouter_gate import openrouter_is_enabled

TWO_MODEL_MODE = "TWO_MODEL_NO_OPENROUTER"
SKIPPED_OPENROUTER = "SKIPPED_OPENROUTER_DISABLED"
ASTRA_MODEL = "codex/gpt-6-astra"
SOL_MODEL = "codex/gpt-5.6-sol"


def two_model_no_openrouter(
    model, *, third_leg_enabled, third_leg_model=SOL_MODEL,
    codex_model=ASTRA_MODEL, explicit_secondary=False, env=None,
):
    """Only the existing GPT + Astra + Sol ensemble may omit GPT.

    Explicit secondary-leg profiles and single-model stages keep their contracts.
    Invalid flags raise a configuration error, never authorize the reduced mode.
    """
    eligible = (
        model == "ensemble/gpt-codex"
        and third_leg_enabled
        and third_leg_model == SOL_MODEL
        and codex_model == ASTRA_MODEL
        and not explicit_secondary
    )
    return bool(eligible and not openrouter_is_enabled(env))
