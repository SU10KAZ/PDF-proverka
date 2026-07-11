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


def test_codex_critic_rejects_finding_restatements():
    template = (
        REPO_ROOT / "prompts" / "pipeline" / "en" / "optimization_critic_task.md"
    ).read_text(encoding="utf-8")

    assert "restatement of an existing F/T finding" in template
    assert "contains no independent optimization" in template
