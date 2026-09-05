"""Regression coverage for the persisted Codex 5.4 -> Astra transition."""

from backend.app.core import config
from backend.app.services.audit_routing.compiler import CompilerInputs


def test_default_codex_selector_is_astra():
    assert config.CODEX_MODEL_DEFAULT == "gpt-6-astra"
    assert config.CODEX_STAGE_MODEL_ID == "codex/gpt-6-astra"
    assert config.CODEX_STAGE_MODEL_LABEL == "Astra"
    assert CompilerInputs.__dataclass_fields__["codex_model_id"].default == (
        "codex/gpt-6-astra"
    )


def test_legacy_persisted_codex_selectors_migrate_without_touching_custom_models():
    stages = {
        "text_analysis": "codex/gpt-5.4",
        "block_batch": "ensemble/gpt-codex",
        "findings_merge": "codex/custom-model",
        "optimization": "claude-opus-5",
    }

    changed = config._migrate_legacy_codex_stage_models(stages)

    assert changed is True
    assert stages == {
        "text_analysis": config.CODEX_STAGE_MODEL_ID,
        "block_batch": "ensemble/gpt-codex",
        "findings_merge": "codex/custom-model",
        "optimization": "claude-opus-5",
    }


def test_astra_config_needs_no_migration():
    stages = {"text_analysis": config.CODEX_STAGE_MODEL_ID}

    assert config._migrate_legacy_codex_stage_models(stages) is False
    assert stages == {"text_analysis": config.CODEX_STAGE_MODEL_ID}
