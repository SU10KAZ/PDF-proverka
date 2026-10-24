"""Таблицы удалённых пресетов «Claude+GPT +Codex» и «Full Codex» (до 24.10.2026).

Пресеты убраны из портала и сервера, но их таблицы остаются допустимой «своей
раскладкой» (боевая таблица прода до перехода на «Только Codex» — именно такая),
а тесты компилятора, плана и воркеров проверяют на них топологию ансамблей.
"""
from __future__ import annotations

from backend.app.services.audit_routing import presets

CLAUDE_GPT_CODEX = "claude_gpt_codex"
FULL_CODEX = "codex_exec"


def reference_config(preset_id: str, *, codex_model_id: str = "codex/gpt-6-astra") -> dict[str, str]:
    base = {
        "text_analysis": "claude-opus-5-5",
        "block_batch": presets.ENSEMBLE_BLOCK,
        "findings_merge": "claude-opus-5-5",
        "findings_critic": "claude-sonnet-5-5",
        "findings_corrector": "claude-sonnet-5-5",
        "norm_verify": "claude-opus-5-5",
        "norm_fix": "claude-opus-5-5",
        "norm_requote": "claude-opus-5-5",
        "optimization": presets.ENSEMBLE_OPTIMIZATION,
        "optimization_critic": "claude-sonnet-5-5",
        "optimization_corrector": "claude-sonnet-5-5",
    }
    if preset_id == CLAUDE_GPT_CODEX:
        return base
    if preset_id == FULL_CODEX:
        codex = str(codex_model_id)
        return {
            **{k: codex for k in presets.STAGE_MODEL_KEYS},
            "block_batch": presets.ENSEMBLE_BLOCK,
            "optimization": presets.ENSEMBLE_OPTIMIZATION,
        }
    return presets.reference_config(preset_id, codex_model_id=codex_model_id)
