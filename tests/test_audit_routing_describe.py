"""Описание фактического маршрута для окна «Конфигурация моделей»."""

import pytest

from backend.app.core import config
from backend.app.services.audit_routing import describe, presets

PROD_ENV = {
    "AUDIT_OPENROUTER_ENABLED": "false",
    "STAGE01_THIRD_LEG_ENABLED": "true",
    "STAGE01_DUAL_GAP_SEARCH_ENABLED": "true",
    "OPTIMIZATION_CRITIC_DETERMINISTIC": "true",
}


def _routing(preset_id, env=PROD_ENV):
    stages = presets.reference_config(preset_id, codex_model_id=config.CODEX_STAGE_MODEL_ID)
    result = describe.describe_routing(stages, env=env)
    assert result["error"] == ""
    return result["stages"]


def _actions(stage):
    return {a["action_id"]: a for a in stage["actions"]}


@pytest.fixture(autouse=True)
def _codex_effort(monkeypatch):
    monkeypatch.setattr(describe, "codex_cli_default_effort", lambda: "high")
    monkeypatch.setattr(config, "CLAUDE_STAGE_EFFORT", "high")


def test_full_codex_shows_claude_where_it_really_runs():
    st = _routing(presets.PRESET_FULL_CODEX)

    guard = _actions(st["findings_corrector"])["absence_guard"]
    assert guard["model"].startswith("claude-sonnet")
    assert (guard["effort"], guard["effort_source"]) == ("high", describe.EFFORT_SOURCE_PIPELINE)

    opt = _actions(st["optimization"])
    assert opt["optimization_primary"]["model"] == config.OPTIMIZATION_ENSEMBLE_CLAUDE_MODEL
    assert opt["optimization_visual"]["effort"] == config.OPTIMIZATION_ENSEMBLE_CODEX_REASONING_EFFORT
    assert st["optimization"]["parallel_legs"] == 2


def test_deterministic_stages_are_marked_without_model():
    st = _routing(presets.PRESET_FULL_CODEX)
    assert st["findings_critic"]["deterministic_only"] is True
    assert st["optimization_corrector"]["deterministic_only"] is True


def test_block_legs_efforts_and_skipped_gpt():
    st = _routing(presets.PRESET_FULL_CODEX)["block_batch"]
    acts = _actions(st)

    assert acts["detector_codex_standard"]["effort"] == "low"
    assert acts["detector_codex_strong"]["active"] is True
    judge = acts["judge_gap_search"]
    # Конвейер зовёт Codex с --ignore-user-config: без флага действует умолчание
    # модели, а не config.toml (фикстура ставит там high).
    expected = describe.codex_model_default_effort(judge["model"], "high")
    assert (judge["effort"], judge["effort_source"]) == (expected, describe.EFFORT_SOURCE_CLI)
    assert st["parallel_legs"] == 2
    assert [s["model"] for s in st["skipped"]] == ["openai/gpt-5.4"]


def test_disabled_flag_marks_leg_inactive():
    st = _routing(presets.PRESET_FULL_CODEX, env={**PROD_ENV, "STAGE01_THIRD_LEG_ENABLED": "false"})
    strong = _actions(st["block_batch"]).get("detector_codex_strong")
    if strong is not None:
        assert strong["active"] is False
        assert "STAGE01_THIRD_LEG_ENABLED" in strong["condition"]


def test_claude_preset_text_stage_runs_opus():
    text = _actions(_routing(presets.PRESET_CLAUDE_GPT_CODEX)["text_analysis"])["text_audit"]
    assert text["model_label"] == "Claude Opus 5.5"
    assert text["effort"] == "high"


def test_invalid_table_returns_error_not_exception():
    result = describe.describe_routing({"text_analysis": "nonsense-model"}, env=PROD_ENV)
    assert result["error"]
    assert result["stages"] == {}


def test_optional_claude_leg_is_listed_with_its_disciplines(monkeypatch):
    from backend.app.pipeline.stages.block_analysis import gemma_findings_only as gfo

    monkeypatch.setattr(gfo, "STAGE01_CLAUDE_LEG_ENABLED", True)
    st = _routing(presets.PRESET_FULL_CODEX)["block_batch"]
    ids = [a["action_id"] for a in st["actions"]]
    assert ids.index("detector_claude") == ids.index("detector_codex_strong") + 1
    leg = _actions(st)["detector_claude"]
    assert (leg["model_label"], leg["effort"], leg["active"]) == ("Claude Opus 5.5", "medium", True)
    assert leg["condition"].startswith("только АР/АИ/ЭОМ/СС/ИТП")
    assert st["parallel_legs"] == 3

    monkeypatch.setattr(gfo, "STAGE01_CLAUDE_LEG_ENABLED", False)
    assert "detector_claude" not in _actions(_routing(presets.PRESET_FULL_CODEX)["block_batch"])
