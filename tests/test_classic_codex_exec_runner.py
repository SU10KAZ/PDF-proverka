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
    assert captured["allowed_tools"] == "Read,Write"


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
async def test_codex_runner_configures_required_norms_mcp_and_disables_web(monkeypatch):
    import backend.app.services.llm.codex_runner as codex_runner

    captured = {}
    monkeypatch.setattr(codex_runner, "find_codex_cli", lambda: "/usr/bin/codex")

    async def fake_run_command(cmd, **kwargs):
        captured["cmd"] = cmd
        captured.update(kwargs)
        out_file = Path(cmd[cmd.index("-o") + 1])
        out_file.write_text("DONE", encoding="utf-8")
        return 0, "", ""

    monkeypatch.setattr(codex_runner, "run_command", fake_run_command)

    tools = (
        "Read,Write,"
        "mcp__norms__get_norm_status,"
        "mcp__norms__get_paragraph_json,"
        "mcp__norms__semantic_search_json"
    )
    exit_code, _, _ = await codex_runner.run_codex_exec(
        "VERIFY NORMS",
        timeout=42,
        stage="optimization",
        project_id="DOC-NORMS",
        model="codex/gpt-5.4",
        allowed_tools=tools,
    )

    cmd = captured["cmd"]
    config_values = [cmd[index + 1] for index, value in enumerate(cmd) if value == "-c"]
    assert exit_code == 0
    assert 'web_search="disabled"' in config_values
    assert "mcp_servers.norms.required=true" in config_values
    assert 'mcp_servers.norms.default_tools_approval_mode="approve"' in config_values
    assert any(value.startswith("mcp_servers.norms.command=") for value in config_values)
    assert any(value.startswith("mcp_servers.norms.args=") for value in config_values)
    enabled = next(
        value for value in config_values
        if value.startswith("mcp_servers.norms.enabled_tools=")
    )
    assert "get_norm_status" in enabled
    assert "get_paragraph_json" in enabled
    assert "semantic_search_json" in enabled
    assert "Normative status, clauses, and quotations" in captured["input_text"]
    assert "Web search is disabled" in captured["input_text"]


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
        schema_file = Path(cmd[cmd.index("--output-schema") + 1])
        captured["schema_file"] = schema_file
        captured["schema"] = json.loads(schema_file.read_text(encoding="utf-8"))
        out_file.write_text('{"items": []}', encoding="utf-8")
        stdout = "\n".join([
            json.dumps({"type": "thread.started", "thread_id": "thread-123"}),
            json.dumps({
                "type": "item.completed",
                "item": {"type": "agent_message", "text": '{"items": []}'},
            }),
            json.dumps({
                "type": "turn.completed",
                "usage": {
                    "input_tokens": 2345,
                    "cached_input_tokens": 2000,
                    "output_tokens": 345,
                    "reasoning_output_tokens": 123,
                },
            }),
        ])
        return 0, stdout, ""

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
        reasoning_effort="low",
        output_schema={
            "type": "object",
            "properties": {"items": {"type": "array"}},
            "required": ["items"],
            "additionalProperties": False,
        },
    )

    cmd = captured["cmd"]
    assert result.is_error is False
    assert result.json_data == {"items": []}
    assert result.input_tokens == 2345
    assert result.cached_tokens == 2000
    assert result.output_tokens == 345
    assert result.reasoning_tokens == 123
    assert result.response_id == "thread-123"
    assert result.cost_usd == 0.0
    assert cmd[cmd.index("--sandbox") + 1] == "read-only"
    assert cmd[cmd.index("--model") + 1] == "gpt-5.4"
    assert cmd[cmd.index("-c") + 1] == 'model_reasoning_effort="low"'
    assert "--json" in cmd
    assert captured["schema"] == {
        "type": "object",
        "properties": {"items": {"type": "array"}},
        "required": ["items"],
        "additionalProperties": False,
    }
    assert "Do not read files" in captured["input_text"]
    assert "INLINE PROJECT CONTEXT" in captured["input_text"]
    assert "image attachment(s) omitted" in captured["input_text"]
    assert not captured["out_file"].exists()
    assert not captured["schema_file"].exists()


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
async def test_stage01_codex_block_keeps_full_context_image_schema_and_effort(monkeypatch, tmp_path):
    from backend.app.models.usage import LLMResult
    from backend.app.pipeline.stages.block_analysis import gemma_findings_only as findings_only
    import backend.app.services.llm.codex_runner as codex_runner

    image = tmp_path / "block_FULL.png"
    image.write_bytes(b"png")
    captured = {}

    async def fake_run_codex_json_messages(messages, **kwargs):
        captured["messages"] = messages
        captured.update(kwargs)
        return LLMResult(
            text='{"findings": []}',
            json_data={"findings": []},
            input_tokens=4321,
            cached_tokens=3210,
            output_tokens=123,
            reasoning_tokens=45,
            model="codex/gpt-5.4",
            cost_source="subscription",
        )

    monkeypatch.setattr(codex_runner, "run_codex_json_messages", fake_run_codex_json_messages)
    full_context = "CONTEXT_START\n" + ("точный контекст блока\n" * 500) + "CONTEXT_END"
    monkeypatch.setattr(
        findings_only,
        "build_effective_block_context",
        lambda *_args, **_kwargs: (full_context, "structured_water"),
    )

    result = await findings_only.call_codex_for_block(
        {"block_id": "FULL", "page": 1, "file": image.name},
        {"block_type": "scheme"},
        "FULL PAGE TEXT",
        tmp_path,
        model="codex/gpt-5.4",
        system_prompt="FULL SYSTEM PROMPT",
        timeout=60,
        reasoning_effort="low",
        project_id="DOC-FULL",
        output_dir=tmp_path,
    )

    assert captured["messages"] == [
        {"role": "system", "content": "FULL SYSTEM PROMPT"},
        {"role": "user", "content": full_context},
    ]
    assert captured["image_paths"] == [image]
    assert captured["reasoning_effort"] == "low"
    assert captured["output_schema"] == findings_only.RESPONSE_SCHEMA["schema"]
    assert result["ok"] is True
    assert result["input_tokens"] == 4321
    assert result["cached_input_tokens"] == 3210
    assert result["output_tokens"] == 123
    assert result["reasoning_tokens"] == 45


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
async def test_codex_json_stage_retries_only_broken_successful_response(monkeypatch, tmp_path):
    import backend.app.services.llm.claude_runner as claude_runner
    import backend.app.services.llm.codex_runner as codex_runner
    from backend.app.models.usage import LLMResult

    calls = []
    output = []
    responses = [
        LLMResult(
            text='{"items": [',
            json_data=None,
            model="codex/gpt-5.4",
            is_error=True,
            error_message="codex_json_not_found",
        ),
        LLMResult(
            text='{"items": []}',
            json_data={"items": []},
            model="codex/gpt-5.4",
            is_error=False,
        ),
    ]

    async def fake_run_codex_json_messages(messages, **kwargs):
        calls.append((messages, kwargs))
        return responses.pop(0)

    async def capture_output(line):
        output.append(line)

    monkeypatch.setattr(codex_runner, "run_codex_json_messages", fake_run_codex_json_messages)
    monkeypatch.setattr(claude_runner, "_CODEX_JSON_ATTEMPTS", 3)
    monkeypatch.setattr(claude_runner, "_save_audit_trail", lambda *args, **kwargs: None)

    exit_code, _text, result = await claude_runner._run_codex_json_stage(
        stage="findings_merge",
        messages=[{"role": "user", "content": "Return JSON"}],
        model="codex/gpt-5.4",
        timeout=60,
        project_id="DOC-RETRY",
        on_output=capture_output,
        output_filename="03_findings.json",
        audit_stage="03_findings",
        output_dir=tmp_path,
    )

    assert exit_code == 0
    assert result.json_data == {"items": []}
    assert len(calls) == 2
    assert any("[RETRY] findings_merge" in line and "повтор 2/3" in line for line in output)
    assert json.loads((tmp_path / "03_findings.json").read_text(encoding="utf-8")) == {"items": []}


@pytest.mark.parametrize(
    ("text", "error_message"),
    [
        ("connection refused", "codex_exec_exit_1; codex_json_not_found"),
        ('{"error":{"message":"usage limit reached"}}', "codex_json_not_found"),
        ("rate limit exceeded", "codex_json_not_found"),
    ],
)
def test_codex_json_retry_rejects_transport_and_limit_errors(text, error_message):
    import backend.app.services.llm.claude_runner as claude_runner
    from backend.app.models.usage import LLMResult

    result = LLMResult(
        text=text,
        json_data=None,
        model="codex/gpt-5.4",
        is_error=True,
        error_message=error_message,
    )

    assert claude_runner._codex_json_broken(result) is False


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
            "tools": tools,
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
    assert "mcp__norms__get_norm_status" in captured["run_cli"]["tools"]
    assert "WebSearch" not in captured["run_cli"]["tools"]
    assert captured["run_cli"]["image_paths"] == [image_path]
    assert "OPT TASK" in captured["run_cli"]["task_text"]
    assert "Графический контекст" in captured["run_cli"]["task_text"]
    # block_id остаётся в листинге для структурного поля source_block_ids,
    # но промпт явно запрещает копировать его в видимые current/proposed/risks
    assert "block_id=B1" in captured["run_cli"]["task_text"]
    assert "source_block_ids" in captured["run_cli"]["task_text"]
    assert captured["audit"]["args"][1] == "05_optimization"
    assert captured["audit"]["args"][6]["codex_exec_agentic"] is True


# ─── Повтор при неразобранном JSON ───────────────────────────────────────
# Живой случай 16.07.2026: 13АВ-РД-ВК2.2-ПА V1 и 13АВ-РД-ДК-К1 V1 упали на своде
# замечаний. Модель печатает JSON без output-schema, и на больших ответах
# (~18K токенов выхода) изредка рвёт структуру — не хватало закрывающей скобки.
# Готовый аудит с 34 находками выбрасывался. Поломка не детерминирована: повтор
# того же запроса дал валидный JSON (проверено на живом проекте, 1 из 2).


def _llm_broken():
    from backend.app.models.usage import LLMResult
    return LLMResult(text='{"meta":', is_error=True, error_message="codex_json_not_found")


def _llm_good():
    from backend.app.models.usage import LLMResult
    return LLMResult(text='{"ok":1}', json_data={"ok": 1})


def _patch_codex_json(monkeypatch, responses: list):
    """Подменить codex-вызов очередью ответов. Возвращает счётчик вызовов."""
    import backend.app.services.llm.codex_runner as codex_mod

    calls = {"n": 0}

    async def fake(messages, **kwargs):
        index = min(calls["n"], len(responses) - 1)
        calls["n"] += 1
        return responses[index]

    monkeypatch.setattr(codex_mod, "run_codex_json_messages", fake)
    return calls


@pytest.mark.asyncio
async def test_codex_json_stage_gives_up_after_attempts(tmp_path, monkeypatch):
    """Всегда битый → попытки исчерпаны, артефакт не пишется, стадия падает."""
    from backend.app.services.llm import claude_runner

    calls = _patch_codex_json(monkeypatch, [_llm_broken()])
    monkeypatch.setattr(claude_runner, "_CODEX_JSON_ATTEMPTS", 3)

    exit_code, _text, _result = await claude_runner._run_codex_json_stage(
        stage="findings_merge",
        messages=[{"role": "user", "content": "x"}],
        model="codex/gpt-5.4",
        timeout=10,
        project_id="DOC-VK",
        on_output=None,
        output_filename="03_findings.json",
        audit_stage="03_findings_merge",
        output_dir=tmp_path,
    )

    assert calls["n"] == 3
    assert exit_code == 1
    assert not (tmp_path / "03_findings.json").exists()


@pytest.mark.asyncio
async def test_codex_json_stage_valid_json_runs_once(tmp_path, monkeypatch):
    """Здоровый ответ — ровно один вызов, повторов нет."""
    from backend.app.services.llm import claude_runner

    calls = _patch_codex_json(monkeypatch, [_llm_good()])
    monkeypatch.setattr(claude_runner, "_CODEX_JSON_ATTEMPTS", 3)

    exit_code, _text, _result = await claude_runner._run_codex_json_stage(
        stage="findings_merge",
        messages=[{"role": "user", "content": "x"}],
        model="codex/gpt-5.4",
        timeout=10,
        project_id="DOC-VK",
        on_output=None,
        output_filename="03_findings.json",
        audit_stage="03_findings_merge",
        output_dir=tmp_path,
    )

    assert calls["n"] == 1
    assert exit_code == 0


def test_codex_json_mode_wires_norms_mcp_when_stage_declares_tools():
    """JSON-вход codex обязан подключать сервер норм так же, как exec-вход.

    Регресс: `_tool_config_args` вклеивался только в run_codex_exec, а
    norm_verify на codex уходит в JSON-режим — и оставался вообще без MCP,
    сверяя статус норм по памяти модели. Молча, без единой ошибки.
    """
    from backend.app.core.config import NORM_VERIFY_TOOLS
    from backend.app.services.llm.codex_runner import _json_tool_args

    args = _json_tool_args(NORM_VERIFY_TOOLS)
    assert any("mcp_servers.norms.command=" in a for a in args)
    assert "mcp_servers.norms.required=true" in args


def test_codex_json_mode_keeps_web_disabled_without_tools():
    """Стадия без инструментов сохраняет исторический дефолт: веб выключен."""
    from backend.app.services.llm.codex_runner import _json_tool_args

    assert _json_tool_args(None) == ["-c", 'web_search="disabled"']


def test_normative_stage_refuses_to_run_without_norms_tools():
    """Нормативная стадия падает закрыто, если сервер норм не пробросили.

    Предохранитель против класса ошибки, а не против модели: цитата нормы по
    памяти неотличима от настоящей, поэтому невыполненный этап безопаснее.
    """
    import pytest

    from backend.app.core.config import NORM_VERIFY_TOOLS
    from backend.app.services.llm.codex_runner import (
        NormsMcpUnavailableError,
        assert_norms_stage_wired,
    )

    with pytest.raises(NormsMcpUnavailableError):
        assert_norms_stage_wired("norm_verify", None)
    with pytest.raises(NormsMcpUnavailableError):
        assert_norms_stage_wired("norm_verify", "Read,Write,Grep")

    assert_norms_stage_wired("norm_verify", NORM_VERIFY_TOOLS)  # не бросает
    assert_norms_stage_wired("optimization", None)  # ненормативная — свободна


def test_missing_norms_venv_raises_actionable_setup_error(monkeypatch):
    """Отсутствие venv сервера норм даёт внятную ошибку, а не `os error 2`.

    Интерпретатор в gitignore, поэтому в свежем клоне/worktree/CI его нет:
    codex обрывал сессию невнятно, Claude молча терял mcp__norms__*.
    """
    import pathlib

    import pytest

    from backend.app.services.llm import codex_runner
    from backend.app.services.llm.codex_runner import NormsMcpUnavailableError

    monkeypatch.setattr(codex_runner, "_NORMS_MCP_PYTHON", pathlib.Path("/nonexistent/python"))
    with pytest.raises(NormsMcpUnavailableError) as exc:
        codex_runner.assert_norms_mcp_available()
    assert "norms/tools/README.md" in str(exc.value)
