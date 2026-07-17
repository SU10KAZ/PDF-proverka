from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_codex_optimization_template_is_specification_first():
    template = (
        REPO_ROOT / "prompts" / "pipeline" / "en" / "optimization_task.md"
    ).read_text(encoding="utf-8")

    assert "Keep findings separate from optimization" in template
    assert "must not exceed 25%" in template
    assert "Only now read audit findings" in template
    assert "Do not present a register correction" in template
    assert "Norms-main MCP only" in template
    assert "WebSearch/WebFetch are forbidden" in template


def test_ru_optimization_template_requires_norms_mcp_without_web_fallback():
    template = (
        REPO_ROOT / "prompts" / "pipeline" / "ru" / "optimization_task.md"
    ).read_text(encoding="utf-8")

    assert "Источник норм — только Norms-main MCP" in template
    assert "WebSearch/WebFetch" in template
    assert "MCP не подтвердил — не угадывай" in template


def test_codex_critic_rejects_finding_restatements():
    template = (
        REPO_ROOT / "prompts" / "pipeline" / "en" / "optimization_critic_task.md"
    ).read_text(encoding="utf-8")

    assert "restatement of an existing F/T finding" in template
    assert "contains no independent optimization" in template
