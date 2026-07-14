import json
from pathlib import Path

import pytest

from backend.app.models.usage import CLIResult


def test_codex_stage_model_is_available_and_resolves(monkeypatch):
    from backend.app.core import config

    model_id = config.CODEX_STAGE_MODEL_ID
    assert config.is_codex_model(model_id)
    assert config.validate_stage_model_choice("findings_merge", model_id) is None
    assert config.resolve_codex_model(model_id) == config.CODEX_MODEL_DEFAULT

    monkeypatch.setitem(config.STAGE_MODEL_CONFIG, "findings_merge", model_id)
    assert config.is_codex_stage("findings_merge") is True
    assert config.is_claude_stage("findings_merge") is False


@pytest.mark.asyncio
async def test_claude_runner_dispatches_codex_model_to_codex_transport(monkeypatch):
    import backend.app.services.llm.claude_runner as claude_runner
    import backend.app.services.llm.codex_runner as codex_runner

    captured = {}

    async def fake_run_codex_exec(task_text, **kwargs):
        captured["task_text"] = task_text
        captured.update(kwargs)
        return 0, "codex ok", CLIResult(result_text="done", duration_ms=7, num_turns=1)

    monkeypatch.setattr(codex_runner, "run_codex_exec", fake_run_codex_exec)

    exit_code, output, result = await claude_runner._run_cli(
        "TASK BODY",
        "Read,Write",
        123,
        stage="findings_merge",
        project_id="DOC-1",
        model="codex/gpt-5.4",
    )

    assert exit_code == 0
    assert output == "codex ok"
    assert result.result_text == "done"
    assert captured["task_text"] == "TASK BODY"
    assert captured["timeout"] == 123
    assert captured["stage"] == "findings_merge"
    assert captured["project_id"] == "DOC-1"
    assert captured["model"] == "codex/gpt-5.4"


@pytest.mark.asyncio
async def test_run_norm_fix_uses_agentic_codex_exec(monkeypatch, tmp_path):
    import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder
    import backend.app.services.llm.claude_runner as claude_runner

    captured = {}

    def fail_json_builder(*args, **kwargs):
        raise AssertionError("norm_fix must not use JSON-only Codex builder")

    def fake_prepare(findings_to_fix_text, project_id, project_info=None):
        captured["prepare"] = (findings_to_fix_text, project_id, project_info)
        return "AGENTIC NORM FIX TASK"

    async def fake_run_cli(task_text, tools, timeout, on_output=None, stage="", project_id="", model=None):
        captured["run_cli"] = {
            "task_text": task_text,
            "tools": tools,
            "timeout": timeout,
            "stage": stage,
            "project_id": project_id,
            "model": model,
        }
        return 0, "codex norm ok", CLIResult(result_text="done", duration_ms=11, num_turns=1)

    def fake_save_audit_trail(*args, **kwargs):
        captured["audit"] = {"args": args, "kwargs": kwargs}

    monkeypatch.setattr(claude_runner, "get_stage_model", lambda stage: "codex/gpt-5.4")
    monkeypatch.setattr(prompt_builder, "build_norm_fix_messages", fail_json_builder)
    monkeypatch.setattr(claude_runner, "prepare_norm_fix_task", fake_prepare)
    monkeypatch.setattr(claude_runner, "_run_cli", fake_run_cli)
    monkeypatch.setattr(claude_runner, "_save_audit_trail", fake_save_audit_trail)

    exit_code, output, result = await claude_runner.run_norm_fix(
        "### F-001",
        "DOC-5",
        project_info={"discipline": "AR"},
        output_dir=tmp_path,
        version_dir=tmp_path.parent,
        version_id="v002",
    )

    assert exit_code == 0
    assert output == "codex norm ok"
    assert result.result_text == "done"
    assert captured["prepare"] == ("### F-001", "DOC-5", {"discipline": "AR"})
    assert "Codex exec mode override" in captured["run_cli"]["task_text"]
    assert "Do not refuse only because MCP tools are unavailable" in captured["run_cli"]["task_text"]
    assert "AGENTIC NORM FIX TASK" in captured["run_cli"]["task_text"]
    assert captured["run_cli"]["stage"] == "norm_fix"
    assert captured["run_cli"]["project_id"] == "DOC-5"
    assert captured["run_cli"]["model"] == "codex/gpt-5.4"
    assert captured["audit"]["args"][1] == "04b_norm_fix"


@pytest.mark.asyncio
async def test_codex_runner_builds_exec_command_and_reads_output_file(monkeypatch):
    import backend.app.services.llm.codex_runner as codex_runner

    captured = {}

    monkeypatch.setattr(codex_runner, "find_codex_cli", lambda: "/usr/bin/codex")
    monkeypatch.setenv("AUDIT_CODEX_SANDBOX", "danger-full-access")

    async def fake_run_command(cmd, **kwargs):
        captured["cmd"] = cmd
        captured.update(kwargs)
        out_file = Path(cmd[cmd.index("-o") + 1])
        captured["out_file"] = out_file
        out_file.write_text("FINAL STATUS", encoding="utf-8")
        return 0, "progress noise", ""

    monkeypatch.setattr(codex_runner, "run_command", fake_run_command)

    exit_code, output, result = await codex_runner.run_codex_exec(
        "WRITE THE REQUESTED JSON FILE",
        timeout=42,
        stage="optimization",
        project_id="DOC-2",
        model="codex/gpt-5.4",
        reasoning_effort="xhigh",
    )

    cmd = captured["cmd"]
    assert exit_code == 0
    assert "FINAL STATUS" in output
    assert result.result_text == "FINAL STATUS"
    assert result.cost_usd == 0.0
    assert cmd[:2] == ["/usr/bin/codex", "exec"]
    assert cmd[cmd.index("--sandbox") + 1] == "danger-full-access"
    assert cmd[cmd.index("--model") + 1] == "gpt-5.4"
    assert cmd[cmd.index("-c") + 1] == 'model_reasoning_effort="xhigh"'
    assert cmd[-1] == "-"
    assert captured["timeout"] == 42
    assert "filesystem access" in captured["input_text"]
    assert "WRITE THE REQUESTED JSON FILE" in captured["input_text"]
    assert not captured["out_file"].exists()


@pytest.mark.asyncio
async def test_codex_runner_attaches_images_to_exec_command(monkeypatch, tmp_path):
    import backend.app.services.llm.codex_runner as codex_runner

    captured = {}
    png_path = tmp_path / "block_A.png"
    jpg_path = tmp_path / "block_B.jpg"
    ignored_txt = tmp_path / "notes.txt"
    png_path.write_bytes(b"png")
    jpg_path.write_bytes(b"jpg")
    ignored_txt.write_text("not an image", encoding="utf-8")

    monkeypatch.setattr(codex_runner, "find_codex_cli", lambda: "/usr/bin/codex")

    async def fake_run_command(cmd, **kwargs):
        captured["cmd"] = cmd
        captured.update(kwargs)
        out_file = Path(cmd[cmd.index("-o") + 1])
        captured["out_file"] = out_file
        out_file.write_text("FINAL STATUS", encoding="utf-8")
        return 0, "", ""

    monkeypatch.setattr(codex_runner, "run_command", fake_run_command)

    exit_code, _, _ = await codex_runner.run_codex_exec(
        "USE DRAWINGS",
        timeout=42,
        stage="optimization",
        project_id="DOC-IMG",
        model="codex/gpt-5.4",
        image_paths=[png_path, jpg_path, ignored_txt, tmp_path / "missing.png"],
    )

    cmd = captured["cmd"]
    assert exit_code == 0
    image_args = [cmd[index + 1] for index, item in enumerate(cmd) if item == "--image"]
    assert image_args == [str(png_path.resolve()), str(jpg_path.resolve())]
    assert str(ignored_txt.resolve()) not in cmd
    assert cmd[-1] == "-"
    assert "<ATTACHED_IMAGES>" in captured["input_text"]
    assert str(png_path.resolve()) in captured["input_text"]
    assert "USE DRAWINGS" in captured["input_text"]


@pytest.mark.asyncio
async def test_codex_json_runner_uses_inline_context_and_parses_final_json(monkeypatch):
    import backend.app.services.llm.codex_runner as codex_runner

    captured = {}
    monkeypatch.setattr(codex_runner, "find_codex_cli", lambda: "/usr/bin/codex")

    async def fake_run_command(cmd, **kwargs):
        captured["cmd"] = cmd
        captured.update(kwargs)
        out_file = Path(cmd[cmd.index("-o") + 1])
        captured["out_file"] = out_file
        out_file.write_text('{"items": []}', encoding="utf-8")
        return 0, "tokens used\n1 234", ""

    monkeypatch.setattr(codex_runner, "run_command", fake_run_command)

    result = await codex_runner.run_codex_json_messages(
        [
            {"role": "system", "content": "Return JSON."},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "INLINE PROJECT CONTEXT"},
                    {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
                ],
            },
        ],
        timeout=77,
        stage="optimization",
        project_id="DOC-3",
        model="codex/gpt-5.4",
    )

    cmd = captured["cmd"]
    assert result.is_error is False
    assert result.json_data == {"items": []}
    assert result.output_tokens == 1234
    assert result.cost_usd == 0.0
    assert cmd[cmd.index("--sandbox") + 1] == "read-only"
    assert cmd[cmd.index("--model") + 1] == "gpt-5.4"
    assert "Do not read files" in captured["input_text"]
    assert "INLINE PROJECT CONTEXT" in captured["input_text"]
    assert "image attachment(s) omitted" in captured["input_text"]
    assert not captured["out_file"].exists()


@pytest.mark.asyncio
async def test_codex_json_runner_attaches_local_images(monkeypatch, tmp_path):
    import backend.app.services.llm.codex_runner as codex_runner

    captured = {}
    image = tmp_path / "block.png"
    image.write_bytes(b"png")
    monkeypatch.setattr(codex_runner, "find_codex_cli", lambda: "/usr/bin/codex")

    async def fake_run_command(cmd, **kwargs):
        captured["cmd"] = cmd
        captured.update(kwargs)
        Path(cmd[cmd.index("-o") + 1]).write_text('{"findings": []}', encoding="utf-8")
        return 0, "", ""

    monkeypatch.setattr(codex_runner, "run_command", fake_run_command)

    result = await codex_runner.run_codex_json_messages(
        [{"role": "user", "content": "Inspect the attached block."}],
        timeout=77,
        stage="block_analysis",
        project_id="DOC-IMG-JSON",
        model="codex/gpt-5.4",
        image_paths=[image],
    )

    assert result.is_error is False
    assert captured["cmd"][captured["cmd"].index("--image") + 1] == str(image.resolve())
    assert str(image.resolve()) in captured["input_text"]


@pytest.mark.asyncio
async def test_codex_json_runner_accepts_valid_json_despite_nonzero_cli_exit(monkeypatch):
    import backend.app.services.llm.codex_runner as codex_runner

    monkeypatch.setattr(codex_runner, "find_codex_cli", lambda: "/usr/bin/codex")

    async def fake_run_command(cmd, **kwargs):
        out_file = Path(cmd[cmd.index("-o") + 1])
        out_file.write_text('{"ok": true}', encoding="utf-8")
        return 9, "warning noise", "tool warning"

    monkeypatch.setattr(codex_runner, "run_command", fake_run_command)

    result = await codex_runner.run_codex_json_messages(
        [{"role": "user", "content": "Return JSON."}],
        timeout=77,
        stage="text_analysis",
        project_id="DOC-4",
        model="codex/gpt-5.4",
    )

    assert result.is_error is False
    assert result.json_data == {"ok": True}
    assert result.error_message == "codex_exec_exit_9_ignored_after_valid_json"


@pytest.mark.asyncio
async def test_run_findings_merge_codex_applies_targeted_passes(monkeypatch, tmp_path):
    import backend.app.pipeline.stages.prepare.codex_targeted_findings as targeted
    import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder
    import backend.app.services.llm.claude_runner as claude_runner
    from backend.app.models.usage import LLMResult

    calls = []

    def fake_build_findings_merge_messages(project_info, project_id):
        assert project_info == {"section": "SS"}
        assert project_id == "DOC-1"
        return [{"role": "user", "content": "base merge"}]

    def fake_build_targeted_findings_passes(project_info, project_id):
        assert project_info == {"section": "SS"}
        assert project_id == "DOC-1"
        return [
            targeted.CodexTargetedPass(
                stage="alia_ss_lowcurrent_audit",
                output_filename="03_findings_targeted_alia_ss_lowcurrent_audit.json",
                messages=[{"role": "user", "content": "targeted"}],
            )
        ]

    async def fake_run_codex_json_stage(
        *,
        stage,
        messages,
        model,
        timeout,
        project_id,
        on_output,
        output_filename,
        audit_stage,
        output_dir=None,
    ):
        calls.append(
            {
                "stage": stage,
                "messages": messages,
                "model": model,
                "output_filename": output_filename,
                "audit_stage": audit_stage,
            }
        )
        if stage == "findings_merge":
            payload = {
                "meta": {"total_findings": 1},
                "findings": [{"id": "F-001", "problem": "base"}],
            }
            duration = 10
        else:
            payload = {"findings": [{"id": "SS-001", "problem": "targeted"}]}
            duration = 20

        out_path = Path(output_dir) / output_filename
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return (
            0,
            json.dumps(payload, ensure_ascii=False),
            LLMResult(
                text=json.dumps(payload, ensure_ascii=False),
                json_data=payload,
                model=model,
                duration_ms=duration,
                input_tokens=1,
                output_tokens=2,
            ),
        )

    monkeypatch.setattr(claude_runner, "get_stage_model", lambda stage: "codex/gpt-5.4")
    monkeypatch.setattr(prompt_builder, "build_findings_merge_messages", fake_build_findings_merge_messages)
    monkeypatch.setattr(targeted, "build_targeted_findings_passes", fake_build_targeted_findings_passes)
    monkeypatch.setattr(claude_runner, "_run_codex_json_stage", fake_run_codex_json_stage)
    monkeypatch.setattr(claude_runner, "_save_audit_trail", lambda *args, **kwargs: None)

    exit_code, output, result = await claude_runner.run_findings_merge(
        {"section": "SS"},
        "DOC-1",
        output_dir=tmp_path,
        version_dir=tmp_path.parent,
        version_id="v001",
    )

    final_data = json.loads((tmp_path / "03_findings.json").read_text(encoding="utf-8"))
    base_data = json.loads((tmp_path / "03_findings_codex_base.json").read_text(encoding="utf-8"))

    assert exit_code == 0
    assert "base" in output
    assert "targeted" in output
    assert result.json_data == final_data
    assert base_data["findings"] == [{"id": "F-001", "problem": "base"}]
    assert (tmp_path / "03_findings_targeted_alia_ss_lowcurrent_audit.json").is_file()
    assert [call["stage"] for call in calls] == ["findings_merge", "alia_ss_lowcurrent_audit"]
    assert final_data["findings"] == [
        {"id": "F-001", "problem": "base"},
        {"id": "F-002", "problem": "targeted", "source_stage": "alia_ss_lowcurrent_audit"},
    ]
    assert final_data["meta"]["total_findings"] == 2
    assert final_data["meta"]["codex_targeted_added"] == 1
    assert final_data["meta"]["codex_targeted_stages"] == ["alia_ss_lowcurrent_audit"]


@pytest.mark.asyncio
async def test_run_optimization_codex_uses_agentic_exec_with_visual_context(monkeypatch, tmp_path):
    import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder
    import backend.app.services.llm.claude_runner as claude_runner

    captured = {}
    image_dir = tmp_path / "blocks_gemma_100"
    image_dir.mkdir()
    image_path = image_dir / "block_B1.png"
    image_path.write_bytes(b"png")
    (image_dir / "index.json").write_text(
        json.dumps(
            {
                "blocks": [
                    {
                        "block_id": "B1",
                        "file": "block_B1.png",
                        "page": 7,
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (tmp_path / "01_blocks_analysis.json").write_text(
        json.dumps(
            {
                "block_analyses": [
                    {
                        "block_id": "B1",
                        "page": 7,
                        "sheet": "План системы отопления. Этаж 02",
                        "label": "Схема разводки трубопроводов с коллекторными узлами",
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def fail_json_builder(*args, **kwargs):
        raise AssertionError("optimization must not use JSON-only Codex builder")

    async def fake_run_cli(
        task_text,
        tools,
        timeout,
        on_output=None,
        stage="",
        project_id="",
        model=None,
        clean_cwd=False,
        image_paths=None,
    ):
        captured["run_cli"] = {
            "task_text": task_text,
            "stage": stage,
            "project_id": project_id,
            "model": model,
            "image_paths": image_paths,
        }
        return 0, "codex optimization ok", CLIResult(result_text="done", duration_ms=15, num_turns=1)

    def fake_save_audit_trail(*args, **kwargs):
        captured["audit"] = {"args": args, "kwargs": kwargs}

    monkeypatch.setenv("AUDIT_CODEX_OPTIMIZATION_IMAGES", "1")
    monkeypatch.setattr(claude_runner, "get_stage_model", lambda stage: "codex/gpt-5.4")
    monkeypatch.setattr(prompt_builder, "build_optimization_messages", fail_json_builder)
    monkeypatch.setattr(claude_runner, "prepare_optimization_task", lambda project_info, project_id: "OPT TASK")
    monkeypatch.setattr(claude_runner, "_run_cli", fake_run_cli)
    monkeypatch.setattr(claude_runner, "_save_audit_trail", fake_save_audit_trail)

    exit_code, output, result = await claude_runner.run_optimization(
        {"section": "OV"},
        "DOC-OV",
        output_dir=tmp_path,
        version_dir=tmp_path.parent,
        version_id="v001",
    )

    assert exit_code == 0
    assert output == "codex optimization ok"
    assert result.result_text == "done"
    assert captured["run_cli"]["stage"] == "optimization"
    assert captured["run_cli"]["project_id"] == "DOC-OV"
    assert captured["run_cli"]["model"] == "codex/gpt-5.4"
    assert captured["run_cli"]["image_paths"] == [image_path]
    assert "OPT TASK" in captured["run_cli"]["task_text"]
    assert "Графический контекст" in captured["run_cli"]["task_text"]
    assert "block_id=B1" in captured["run_cli"]["task_text"]
    assert captured["audit"]["args"][1] == "05_optimization"
    assert captured["audit"]["args"][6]["codex_exec_agentic"] is True
