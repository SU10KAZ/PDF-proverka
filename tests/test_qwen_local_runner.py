from __future__ import annotations

import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def test_available_models_includes_local_qwen():
    from webapp.config import AVAILABLE_MODELS, CHANDRA_QWEN_MODEL, STAGE_MODEL_RESTRICTIONS

    ids = [m["id"] for m in AVAILABLE_MODELS]
    assert CHANDRA_QWEN_MODEL in ids
    entry = next(m for m in AVAILABLE_MODELS if m["id"] == CHANDRA_QWEN_MODEL)
    assert entry["provider"] == "chandra_local"
    assert CHANDRA_QWEN_MODEL in STAGE_MODEL_RESTRICTIONS["block_batch"]


def test_pipeline_expands_qwen_block_batches_to_single_block(monkeypatch):
    from webapp.services import pipeline_service

    monkeypatch.setattr(pipeline_service, "get_stage_model", lambda stage: "qwen/qwen3.6-35b-a3b")

    batches = [
        {
            "batch_id": 1,
            "blocks": [
                {"block_id": "IMG-001", "page": 1, "file": "block_IMG-001.png", "size_kb": 111.0},
                {"block_id": "IMG-002", "page": 1, "file": "block_IMG-002.png", "size_kb": 222.0},
            ],
            "pages_included": [1],
            "block_count": 2,
            "total_size_kb": 333.0,
        },
        {
            "batch_id": 2,
            "blocks": [
                {"block_id": "IMG-003", "page": 2, "file": "block_IMG-003.png", "size_kb": 123.0},
            ],
            "pages_included": [2],
            "block_count": 1,
            "total_size_kb": 123.0,
        },
    ]

    expanded, single_block_mode = pipeline_service._expand_block_batches_for_local_model(batches)

    assert single_block_mode is True
    assert [batch["batch_id"] for batch in expanded] == [1, 2, 3]
    assert [batch["block_count"] for batch in expanded] == [1, 1, 1]
    assert [batch["blocks"][0]["block_id"] for batch in expanded] == ["IMG-001", "IMG-002", "IMG-003"]
    assert expanded[0]["source_batch_id"] == 1
    assert expanded[2]["source_batch_id"] == 2
    assert all(batch["single_block_mode"] is True for batch in expanded)


def test_build_text_analysis_messages_uses_document_graph_fallback_for_local_qwen(monkeypatch, tmp_path):
    from webapp.services import prompt_builder

    monkeypatch.setattr(prompt_builder, "get_stage_model", lambda stage: "qwen/qwen3.6-35b-a3b")
    monkeypatch.setattr(prompt_builder, "_load_and_clean_template", lambda *args, **kwargs: "SYSTEM")
    monkeypatch.setattr(prompt_builder, "_read_norms_reference", lambda project_info: "VERY LARGE NORMS")
    monkeypatch.setattr(prompt_builder, "resolve_project_dir", lambda project_id: tmp_path)
    monkeypatch.setattr(
        prompt_builder,
        "_load_document_graph",
        lambda project_id: {
            "pages": [
                {
                    "page": 1,
                    "sheet_no_raw": "1",
                    "sheet_name": "Общие данные",
                    "text_blocks": [{"text": "Извлечённый текст"}],
                }
            ]
        },
    )

    messages = prompt_builder.build_text_analysis_messages({"md_file": "missing.md"}, "demo-project")

    assert messages[0]["content"] == (
        "SYSTEM\n\n## Normative Reference\n\n"
        "Stage 04 will verify normative references separately. "
        "At this stage, extract only norms explicitly present in the provided source text."
    )
    assert '"text_source": "extracted_text"' in messages[1]["content"]
    assert "Извлечённый текст" in messages[1]["content"]
    assert "VERY LARGE NORMS" not in messages[0]["content"]


@pytest.mark.asyncio
async def test_run_llm_routes_structured_qwen_stage_to_chat_completions(monkeypatch):
    from webapp.services import llm_runner

    captured: dict = {}

    class FakeResponse:
        status_code = 200
        text = '{"id":"chatcmpl-local"}'

        def json(self):
            return {
                "id": "chatcmpl-local",
                "choices": [
                    {
                        "message": {
                            "content": "",
                            "reasoning_content": '{"ok": true}',
                        },
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 123,
                    "completion_tokens": 45,
                    "completion_tokens_details": {
                        "reasoning_tokens": 44,
                    },
                },
            }

    class FakeAsyncClient:
        def __init__(self, timeout):
            captured["timeout"] = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, *, headers=None, json=None):
            captured["url"] = url
            captured["headers"] = headers
            captured["json"] = json
            return FakeResponse()

    monkeypatch.setattr(llm_runner, "CHANDRA_BASE_URL", "https://chandra.local")
    monkeypatch.setattr(llm_runner, "CHANDRA_BASIC_USER", "user")
    monkeypatch.setattr(llm_runner, "CHANDRA_BASIC_PASS", "pass")
    monkeypatch.setattr(llm_runner.httpx, "AsyncClient", FakeAsyncClient)

    result = await llm_runner.run_llm(
        stage="text_analysis",
        messages=[{"role": "user", "content": "return json"}],
        model_override="qwen/qwen3.6-35b-a3b",
        timeout=77,
    )

    assert captured["timeout"] == 77
    assert captured["url"] == "https://chandra.local/v1/chat/completions"
    assert captured["json"]["model"] == "qwen/qwen3.6-35b-a3b"
    assert captured["json"]["messages"] == [{"role": "user", "content": "return json"}]
    assert captured["json"]["response_format"]["type"] == "json_schema"
    assert result.is_error is False
    assert result.json_data == {"ok": True}
    assert result.input_tokens == 123
    assert result.output_tokens == 45
    assert result.reasoning_tokens == 44
    assert result.cost_usd == 0.0
    assert result.cost_source == "local"


@pytest.mark.asyncio
async def test_run_llm_qwen_unknown_stage_falls_back_to_text(monkeypatch):
    from webapp.services import llm_runner

    captured: dict = {}

    class FakeResponse:
        status_code = 200
        text = '{"output":[{"type":"message","content":"plain text"}]}'

        def json(self):
            return {
                "output": [{"type": "message", "content": "plain text"}],
                "stats": {"input_tokens": 1, "total_output_tokens": 2},
            }

    class FakeAsyncClient:
        def __init__(self, timeout):
            self.timeout = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, *, headers=None, json=None):
            captured["url"] = url
            captured["json"] = json
            return FakeResponse()

    monkeypatch.setattr(llm_runner, "CHANDRA_BASE_URL", "https://chandra.local")
    monkeypatch.setattr(llm_runner, "CHANDRA_BASIC_USER", "user")
    monkeypatch.setattr(llm_runner, "CHANDRA_BASIC_PASS", "pass")
    monkeypatch.setattr(llm_runner.httpx, "AsyncClient", FakeAsyncClient)

    result = await llm_runner.run_llm(
        stage="some_unknown_stage",
        messages=[{"role": "user", "content": "hello"}],
        model_override="qwen/qwen3.6-35b-a3b",
    )

    assert "response_format" not in captured["json"]
    assert captured["url"] == "https://chandra.local/api/v1/chat"
    assert captured["json"]["input"] == "hello"
    assert result.text == "plain text"


@pytest.mark.asyncio
async def test_run_llm_qwen_uses_reasoning_content_when_content_empty(monkeypatch):
    from webapp.services import llm_runner

    class FakeResponse:
        status_code = 200
        text = '{"id":"chatcmpl-local"}'

        def json(self):
            return {
                "id": "chatcmpl-local",
                "choices": [
                    {
                        "message": {
                            "content": "",
                            "reasoning_content": '{"ok": true}',
                        },
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 5,
                    "completion_tokens": 7,
                    "completion_tokens_details": {"reasoning_tokens": 7},
                },
            }

    class FakeAsyncClient:
        def __init__(self, timeout):
            self.timeout = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, *, headers=None, json=None):
            return FakeResponse()

    monkeypatch.setattr(llm_runner, "CHANDRA_BASE_URL", "https://chandra.local")
    monkeypatch.setattr(llm_runner, "CHANDRA_BASIC_USER", "user")
    monkeypatch.setattr(llm_runner, "CHANDRA_BASIC_PASS", "pass")
    monkeypatch.setattr(llm_runner.httpx, "AsyncClient", FakeAsyncClient)

    result = await llm_runner.run_llm(
        stage="text_analysis",
        messages=[{"role": "user", "content": "return json"}],
        model_override="qwen/qwen3.6-35b-a3b",
    )

    assert result.is_error is False
    assert result.text == '{"ok": true}'
    assert result.json_data == {"ok": True}


@pytest.mark.asyncio
async def test_run_llm_qwen_reloads_context_and_retries(monkeypatch):
    from webapp.services import llm_runner

    calls = {"post": 0}
    unloads: list[str] = []
    loads: list[int] = []

    class OverflowResponse:
        status_code = 400
        text = "The number of tokens to keep from the initial prompt is greater than the context length (n_keep: 30984>= n_ctx: 4096)."

        def json(self):
            return {"error": {"message": self.text}}

    class SuccessResponse:
        status_code = 200
        text = '{"ok": true}'

        def json(self):
            return {
                "id": "chatcmpl-local",
                "choices": [
                    {
                        "message": {
                            "content": "",
                            "reasoning_content": '{"ok": true}',
                        },
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 20,
                    "completion_tokens_details": {"reasoning_tokens": 19},
                },
            }

    class FakeAsyncClient:
        def __init__(self, timeout):
            self.timeout = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, *, headers=None, json=None):
            calls["post"] += 1
            if calls["post"] == 1:
                return OverflowResponse()
            return SuccessResponse()

    def fake_get_status():
        return {
            "loaded_instances": [
                {
                    "model_key": "qwen/qwen3.6-35b-a3b",
                    "instance_id": "qwen/qwen3.6-35b-a3b",
                    "config": {
                        "context_length": 4096,
                        "eval_batch_size": 512,
                        "flash_attention": True,
                        "num_experts": 8,
                        "offload_kv_cache_to_gpu": True,
                    },
                }
            ]
        }

    def fake_unload_instance(*, instance_id):
        unloads.append(instance_id)
        return {"ok": True}

    def fake_load_model(
        *,
        model,
        context_length,
        flash_attention=True,
        offload_kv_cache_to_gpu=True,
        eval_batch_size=None,
        num_experts=None,
    ):
        loads.append(context_length)
        return {
            "ok": True,
            "response": {
                "status": "loaded",
                "load_config": {"context_length": context_length},
            },
        }

    monkeypatch.setattr(llm_runner, "CHANDRA_BASE_URL", "https://chandra.local")
    monkeypatch.setattr(llm_runner, "CHANDRA_BASIC_USER", "user")
    monkeypatch.setattr(llm_runner, "CHANDRA_BASIC_PASS", "pass")
    monkeypatch.setattr(llm_runner.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setattr(llm_runner.model_control_service, "get_status", fake_get_status)
    monkeypatch.setattr(llm_runner.model_control_service, "unload_instance", fake_unload_instance)
    monkeypatch.setattr(llm_runner.model_control_service, "load_model", fake_load_model)

    result = await llm_runner.run_llm(
        stage="text_analysis",
        messages=[{"role": "user", "content": "return json"}],
        model_override="qwen/qwen3.6-35b-a3b",
    )

    assert unloads == ["qwen/qwen3.6-35b-a3b"]
    assert loads == [98304]
    assert calls["post"] == 2
    assert result.is_error is False
    assert result.json_data == {"ok": True}


@pytest.mark.asyncio
async def test_run_llm_qwen_fails_cleanly_without_chandra_config(monkeypatch):
    from webapp.services import llm_runner

    monkeypatch.setattr(llm_runner, "CHANDRA_BASE_URL", "")
    monkeypatch.setattr(llm_runner, "CHANDRA_BASIC_USER", "")
    monkeypatch.setattr(llm_runner, "CHANDRA_BASIC_PASS", "")

    result = await llm_runner.run_llm(
        stage="text_analysis",
        messages=[{"role": "user", "content": "hello"}],
        model_override="qwen/qwen3.6-35b-a3b",
    )

    assert result.is_error is True
    assert "CHANDRA_BASE_URL" in result.error_message


def test_build_findings_merge_messages_compacts_blocks_for_local_qwen(monkeypatch, tmp_path):
    from webapp.services import prompt_builder

    monkeypatch.setattr(prompt_builder, "get_stage_model", lambda stage: "qwen/qwen3.6-35b-a3b")
    monkeypatch.setattr(prompt_builder, "_load_and_clean_template", lambda *args, **kwargs: "SYSTEM")
    monkeypatch.setattr(prompt_builder, "resolve_project_dir", lambda project_id: tmp_path)

    output_dir = tmp_path / "_output"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "01_text_analysis.json").write_text('{"text_findings":[{"id":"T-001"}]}', encoding="utf-8")
    (output_dir / "02_blocks_analysis.json").write_text(
        """
        {
          "stage": "02_blocks_analysis",
          "meta": {"blocks_analyzed": 2},
          "block_analyses": [
            {
              "block_id": "IMG-001",
              "page": 1,
              "sheet": "Лист 1",
              "label": "План",
              "sheet_type": "plan",
              "summary": "Очень длинный summary, который не нужен в compact mode",
              "key_values_read": [{"key": "A", "value": "B"}],
              "findings": [
                {
                  "id": "G-001",
                  "severity": "КРИТИЧЕСКОЕ",
                  "category": "coordination",
                  "finding": "Проблема",
                  "norm": "СП 1.1",
                  "value_found": "42",
                  "highlight_regions": [{"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4, "label": "узел"}]
                }
              ]
            },
            {
              "block_id": "IMG-002",
              "page": 2,
              "sheet": "Лист 2",
              "label": "Разрез",
              "sheet_type": "section",
              "summary": "Пустой блок",
              "findings": []
            }
          ]
        }
        """,
        encoding="utf-8",
    )

    messages = prompt_builder.build_findings_merge_messages({}, "demo-project")
    content = messages[1]["content"]

    assert "Local merge note" in content
    assert '"block_id": "IMG-001"' in content
    assert '"key_values_read"' not in content
    assert "Очень длинный summary" not in content
    assert '"block_id": "IMG-002"' not in content


def test_is_context_exceeded_error_matches_real_qwen_message():
    import qwen_enrich

    real_msg = (
        "request (4570 tokens) exceeds the available context size "
        "(4096 tokens), try increasing it"
    )
    assert qwen_enrich._is_context_exceeded_error(
        {"error": {"message": real_msg, "type": "internal_error"}}, ""
    )
    assert qwen_enrich._is_context_exceeded_error(None, real_msg)
    assert not qwen_enrich._is_context_exceeded_error(
        {"error": {"message": "Invalid image"}}, ""
    )
    assert not qwen_enrich._is_context_exceeded_error(None, "")


@pytest.mark.asyncio
async def test_single_pass_downscales_on_context_exceeded(monkeypatch, tmp_path):
    """На context-exceeded scale_idx инкрементируется и следующий вызов идёт
    с меньшим scale, как уже работает для invalid image."""
    import qwen_enrich

    png = tmp_path / "blk.png"
    png.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 16)
    blocks_dir = tmp_path

    calls: list[float] = []

    async def fake_attempt(client, base_url, user_text, png_path, scale, model, timeout, max_output_tokens):
        calls.append(scale)
        if scale >= 0.99:
            return (
                400,
                {"error": {"message": "request (4570 tokens) exceeds the available context size (4096 tokens)"}},
                "",
                10,
            )
        json_text = (
            '{"block_type":"схема","subject":"x","marks":[],"rebar_specs":[],'
            '"dimensions":[],"references_on_block":[],"axes":[],"level_marks":[],'
            '"concrete_class":null,"notes":"ok"}'
        )
        ok_payload = {
            "output": [{"type": "message", "content": json_text}],
            "stats": {"input_tokens": 100, "total_output_tokens": 50},
        }
        return (200, ok_payload, "", 10)

    monkeypatch.setattr(qwen_enrich, "_qwen_call_attempt", fake_attempt)
    monkeypatch.setattr(qwen_enrich, "_load_page_text", lambda graph, page: "")
    monkeypatch.setattr(qwen_enrich, "_load_sheet_no", lambda graph, page: "")

    block = {"block_id": "BLK-1", "page": 1, "file": "blk.png", "ocr_label": ""}
    res = await qwen_enrich._enrich_block_single_pass(
        client=None, base_url="http://x", block=block, graph={},
        blocks_dir=blocks_dir, model="m", timeout=10, max_output_tokens=2048,
    )

    assert calls[0] == 1.0
    assert calls[1] == 0.6
    assert res.ok, f"expected ok after downscale, got error={res.error}"
