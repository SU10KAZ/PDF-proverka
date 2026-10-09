"""Переход Claude 5 → 5.5 (09.10.2026): сохранённая таблица и эталоны центра."""

from backend.app.core import config
from backend.app.services.audit_routing import center_models, presets


def test_persisted_claude5_selectors_migrate_to_55():
    stages = {
        "text_analysis": "claude-opus-5",
        "optimization_critic": "claude-sonnet-5",
        "block_batch": "ensemble/gpt-codex",
        "norm_fix": "codex/gpt-6-astra",
    }

    assert config._migrate_legacy_claude_stage_models(stages) is True
    assert stages == {
        "text_analysis": "claude-opus-5-5",
        "optimization_critic": "claude-sonnet-5-5",
        "block_batch": "ensemble/gpt-codex",
        "norm_fix": "codex/gpt-6-astra",
    }


def test_claude55_table_needs_no_migration():
    stages = {"text_analysis": "claude-opus-5-5", "findings_critic": "claude-sonnet-5-5"}

    assert config._migrate_legacy_claude_stage_models(stages) is False
    assert stages == {"text_analysis": "claude-opus-5-5", "findings_critic": "claude-sonnet-5-5"}


def test_claude_preset_and_center_policy_use_55():
    ref = presets.reference_config(
        presets.PRESET_CLAUDE_GPT_CODEX, codex_model_id=config.CODEX_STAGE_MODEL_ID
    )
    claude_ids = {v for v in ref.values() if v.startswith("claude-")}

    assert claude_ids == {"claude-opus-5-5", "claude-sonnet-5-5"}
    assert center_models.CLAUDE_STRONG_MODEL == "claude-opus-5-5"
    assert center_models.CLAUDE_CHEAP_MODEL == "claude-sonnet-5-5"
    assert config.OPTIMIZATION_ENSEMBLE_CLAUDE_MODEL == "claude-opus-5-5"
    # Codex-модели переходом не затронуты.
    assert config.CODEX_STAGE_MODEL_ID == "codex/gpt-6-astra"
    assert config.OPTIMIZATION_ENSEMBLE_CODEX_MODEL == "codex/gpt-5.6-sol"
