"""Compatibility tests for task_builder legacy wrappers."""

from webapp.services import task_builder


def test_build_text_analysis_prompt_uses_explicit_paths():
    prompt = task_builder.build_text_analysis_prompt(
        {
            "project_id": "compat-project",
            "name": "Compatibility Project",
            "section": "EOM",
        },
        output_path="/tmp/out",
        md_file_path="/tmp/project.md",
    )

    assert "compat-project" in prompt
    assert "/tmp/out" in prompt
    assert "/tmp/project.md" in prompt


def test_prepare_main_audit_task_uses_legacy_argument_order(monkeypatch):
    captured = {}

    def fake_prepare(project_info, project_id):
        captured["project_info"] = project_info
        captured["project_id"] = project_id
        return "ok"

    monkeypatch.setattr(task_builder, "prepare_text_analysis_task", fake_prepare)

    result = task_builder.prepare_main_audit_task("P-001", {"section": "EOM"})
    assert result == "ok"
    assert captured == {"project_info": {"section": "EOM"}, "project_id": "P-001"}


def test_prepare_triage_task_uses_legacy_argument_order(monkeypatch):
    captured = {}

    def fake_prepare(project_info, project_id):
        captured["project_info"] = project_info
        captured["project_id"] = project_id
        return "ok"

    monkeypatch.setattr(task_builder, "prepare_text_analysis_task", fake_prepare)

    result = task_builder.prepare_triage_task("P-002", {"section": "OV"})
    assert result == "ok"
    assert captured == {"project_info": {"section": "OV"}, "project_id": "P-002"}


def test_prepare_smart_merge_task_uses_legacy_argument_order(monkeypatch):
    captured = {}

    def fake_prepare(project_info, project_id):
        captured["project_info"] = project_info
        captured["project_id"] = project_id
        return "ok"

    monkeypatch.setattr(task_builder, "prepare_findings_merge_task", fake_prepare)

    result = task_builder.prepare_smart_merge_task("P-003", {"section": "AR"})
    assert result == "ok"
    assert captured == {"project_info": {"section": "AR"}, "project_id": "P-003"}
