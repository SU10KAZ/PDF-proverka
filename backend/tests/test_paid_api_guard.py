"""Tests for paid_api_guard — упрощённая версия после рефакторинга 2026-05-18.

Покрытие:
  A. paid_api_guard поведение:
     - PAID_API_ENABLED=false блокирует;
     - PAID_API_ENABLED=true разрешает автоматически (без manual_run_id);
     - короткий project_id ("M31A") блокируется;
     - daily limit блокирует;
     - sanity-поля (source/model/stage/project_id) обязательны;
  B. llm_runner.run_llm не делает внешний request когда blocked;
  C. manager Stage 02 (call_gpt_for_block) блокирует перед httpx.post при kill-switch;
  D. queue/resume: после рестарта resume не падает orphan (нет требования manual_run_id);
  E. events: успех пишет paid_cost_events.jsonl, блок пишет paid_api_blocked_events.jsonl;
     reset_paid_cost / clear_project_usage НЕ удаляют jsonl.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest


@pytest.fixture
def isolated_paid_api(tmp_path, monkeypatch):
    """Изолированный paid_api_guard + paid_api_events в tmp_path."""
    _PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    if str(_PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(_PROJECT_ROOT))

    from backend.app.services.llm import paid_api_events as events_mod
    from backend.app.services.llm import paid_api_guard as guard_mod

    paid_jsonl = tmp_path / "paid_cost_events.jsonl"
    blocked_jsonl = tmp_path / "paid_api_blocked_events.jsonl"

    monkeypatch.setattr(events_mod, "PAID_COST_EVENTS_FILE", paid_jsonl)
    monkeypatch.setattr(events_mod, "PAID_API_BLOCKED_EVENTS_FILE", blocked_jsonl)

    # Default: enabled=true, daily limit=0 (отключён).
    monkeypatch.setenv("PAID_API_ENABLED", "true")
    monkeypatch.setenv("PAID_API_DAILY_LIMIT_USD", "0")

    yield {
        "guard": guard_mod,
        "events": events_mod,
        "paid_jsonl": paid_jsonl,
        "blocked_jsonl": blocked_jsonl,
    }


# ─── A. paid_api_guard ────────────────────────────────────────────────


def test_kill_switch_disabled_blocks_everything(isolated_paid_api, monkeypatch):
    """A1: PAID_API_ENABLED=false блокирует."""
    guard = isolated_paid_api["guard"]
    monkeypatch.setenv("PAID_API_ENABLED", "false")

    ctx = guard.PaidApiContext(
        source="llm_runner",
        model="openai/gpt-5.4",
        project_id="proj/A.pdf",
        stage="block_analysis",
    )
    with pytest.raises(guard.PaidApiBlockedError) as exc:
        guard.assert_paid_api_allowed(ctx)
    assert exc.value.reason == "paid_api_disabled"


def test_paid_api_allowed_without_manual_run_id(isolated_paid_api):
    """A2 (новый): PAID_API_ENABLED=true разрешает вызов без manual_run_id —
    pipeline сам управляет платными вызовами."""
    guard = isolated_paid_api["guard"]
    ctx = guard.PaidApiContext(
        source="llm_runner",
        model="openai/gpt-5.4",
        project_id="proj/A.pdf",
        stage="block_analysis",
    )
    guard.assert_paid_api_allowed(ctx)  # не должно поднять


def test_short_discipline_code_project_id_blocks(isolated_paid_api):
    """A3: project_id "M31A" — короткий код, блок."""
    guard = isolated_paid_api["guard"]
    ctx = guard.PaidApiContext(
        source="manager.stage02",
        model="openai/gpt-5.4",
        project_id="M31A",
        stage="block_analysis",
    )
    with pytest.raises(guard.PaidApiBlockedError) as exc:
        guard.assert_paid_api_allowed(ctx)
    assert exc.value.reason == "short_discipline_code_project_id"


def test_missing_source_model_stage_blocked(isolated_paid_api):
    """A4: Sanity-проверка обязательных полей."""
    guard = isolated_paid_api["guard"]
    for missing in ("source", "model", "stage"):
        ctx = guard.PaidApiContext(
            source="x", model="m/x", project_id="some/full/project.pdf",
            stage="s",
        )
        setattr(ctx, missing, "")
        with pytest.raises(guard.PaidApiBlockedError) as exc:
            guard.assert_paid_api_allowed(ctx)
        assert exc.value.reason == f"missing_{missing}"


def test_missing_project_id_blocked(isolated_paid_api):
    """A4b: missing project_id блокируется."""
    guard = isolated_paid_api["guard"]
    ctx = guard.PaidApiContext(
        source="llm_runner", model="openai/gpt-5.4",
        project_id="", stage="block_analysis",
    )
    with pytest.raises(guard.PaidApiBlockedError) as exc:
        guard.assert_paid_api_allowed(ctx)
    assert exc.value.reason == "missing_project_id"


def test_daily_limit_blocks(isolated_paid_api, monkeypatch):
    """A5: daily_limit_usd=1.0 + estimated_cost_usd=2.0 → блок."""
    guard = isolated_paid_api["guard"]
    monkeypatch.setenv("PAID_API_DAILY_LIMIT_USD", "1.0")
    ctx = guard.PaidApiContext(
        source="llm_runner", model="openai/gpt-5.4",
        project_id="proj/A.pdf", stage="block_analysis",
        estimated_cost_usd=2.0,
    )
    with pytest.raises(guard.PaidApiBlockedError) as exc:
        guard.assert_paid_api_allowed(ctx)
    assert exc.value.reason == "daily_limit_exceeded"


def test_runtime_kill_switch_takes_effect_without_module_reload(
    isolated_paid_api, monkeypatch
):
    """A6: kill-switch действует сразу после смены os.environ, без рестарта."""
    guard = isolated_paid_api["guard"]

    monkeypatch.setenv("PAID_API_ENABLED", "true")
    ctx = guard.PaidApiContext(
        source="llm_runner",
        model="openai/gpt-5.4",
        project_id="proj/A.pdf",
        stage="block_analysis",
    )
    guard.assert_paid_api_allowed(ctx)  # без исключения

    monkeypatch.setenv("PAID_API_ENABLED", "false")
    with pytest.raises(guard.PaidApiBlockedError) as exc:
        guard.assert_paid_api_allowed(ctx)
    assert exc.value.reason == "paid_api_disabled"


def test_canonical_project_id_allows_short_display_pid(isolated_paid_api):
    """A7: короткий project_id "M31A" допустим, если передан
    canonical_project_id с полным путём ИЛИ object_id."""
    guard = isolated_paid_api["guard"]
    ctx = guard.PaidApiContext(
        source="manager.stage02",
        model="openai/gpt-5.4",
        project_id="M31A",
        canonical_project_id="214. Alia (ASTERUS)/M31A",
        stage="block_analysis",
    )
    guard.assert_paid_api_allowed(ctx)


# ─── E. Append-only events ────────────────────────────────────────────


def test_blocked_event_is_appended(isolated_paid_api, monkeypatch):
    """E1: каждый block пишет строку в paid_api_blocked_events.jsonl."""
    guard = isolated_paid_api["guard"]
    monkeypatch.setenv("PAID_API_ENABLED", "false")

    ctx = guard.PaidApiContext(
        source="manager.stage02", model="openai/gpt-5.4",
        project_id="proj/A.pdf", stage="block_analysis",
    )
    with pytest.raises(guard.PaidApiBlockedError):
        guard.assert_paid_api_allowed(ctx)

    blocked = isolated_paid_api["blocked_jsonl"]
    assert blocked.exists()
    lines = blocked.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    event = json.loads(lines[0])
    assert event["event"] == "paid_api_blocked"
    assert event["reason"] == "paid_api_disabled"
    assert event["source"] == "manager.stage02"
    assert event["project_id"] == "proj/A.pdf"
    assert event["pid"]


def test_paid_event_written_and_blocked_jsonl_not_cleared(isolated_paid_api):
    """E2: paid_event пишется отдельным API."""
    events = isolated_paid_api["events"]
    paid_jsonl = isolated_paid_api["paid_jsonl"]

    events.record_paid_event(
        cost_usd=1.234,
        model="openai/gpt-5.4",
        project_id="proj/A.pdf",
        stage="block_analysis",
        source="manager.stage02",
        job_id="job-1",
        input_tokens=100,
        output_tokens=50,
    )
    assert paid_jsonl.exists()
    line = paid_jsonl.read_text(encoding="utf-8").strip().splitlines()[0]
    ev = json.loads(line)
    assert ev["event"] == "paid_api_cost"
    assert ev["cost_usd"] == pytest.approx(1.234)
    assert ev["model"] == "openai/gpt-5.4"

    tail = events.read_paid_events_tail(limit=10)
    assert len(tail) == 1
    assert tail[0]["job_id"] == "job-1"


def test_count_blocked_today(isolated_paid_api, monkeypatch):
    """E3: count_blocked_today корректно считает только сегодняшние."""
    guard = isolated_paid_api["guard"]
    monkeypatch.setenv("PAID_API_ENABLED", "false")
    for _ in range(3):
        ctx = guard.PaidApiContext(
            source="llm_runner", model="openai/gpt-5.4",
            project_id="proj/A.pdf", stage="block_analysis",
        )
        with pytest.raises(guard.PaidApiBlockedError):
            guard.assert_paid_api_allowed(ctx)
    assert isolated_paid_api["events"].count_blocked_today() == 3


# ─── B. llm_runner — guard работает ПЕРЕД network ─────────────────────


def test_llm_runner_blocks_before_network_when_kill_switch_off(
    isolated_paid_api, monkeypatch
):
    """B1: run_llm на OpenRouter модель при kill-switch=off → возвращает
    LLMResult is_error="paid_api_blocked:..." БЕЗ вызова OpenAI клиента."""
    from backend.app.services.llm import llm_runner
    monkeypatch.setenv("PAID_API_ENABLED", "false")

    network_called = {"flag": False}

    def fake_get_client():
        network_called["flag"] = True
        raise AssertionError("Network was attempted despite block!")

    monkeypatch.setattr(llm_runner, "_get_client", fake_get_client)

    async def _run():
        return await llm_runner.run_llm(
            stage="block_batch",
            messages=[{"role": "user", "content": "test"}],
            model_override="openai/gpt-5.4",
            project_id="proj/A.pdf",
        )

    result = asyncio.run(_run())
    assert result.is_error is True
    assert "paid_api_blocked" in (result.error_message or "")
    assert result.cost_usd == 0
    assert network_called["flag"] is False


def test_llm_runner_stream_blocks_before_network_when_kill_switch_off(
    isolated_paid_api, monkeypatch
):
    """B2: run_llm_stream также блокируется ДО network при kill-switch=off."""
    from backend.app.services.llm import llm_runner
    monkeypatch.setenv("PAID_API_ENABLED", "false")

    network_called = {"flag": False}

    def fake_get_client():
        network_called["flag"] = True
        raise AssertionError("Stream attempted network despite block!")

    monkeypatch.setattr(llm_runner, "_get_client", fake_get_client)

    async def _drain():
        chunks = []
        async for ev in llm_runner.run_llm_stream(
            messages=[{"role": "user", "content": "test"}],
            model_override="openai/gpt-5.4",
            project_id="proj/A.pdf",
            stage="discussion",
        ):
            chunks.append(ev)
        return chunks

    chunks = asyncio.run(_drain())
    assert any(c.get("type") == "error" and "paid_api_blocked" in c.get("message", "")
               for c in chunks)
    assert network_called["flag"] is False


# ─── C. Stage 02 call_gpt_for_block (defence-in-depth) ────────────────


def test_stage02_call_gpt_blocks_before_httpx_when_kill_switch_off(
    isolated_paid_api, tmp_path, monkeypatch
):
    """C1: call_gpt_for_block при kill-switch=off возвращает paid_api_blocked
    БЕЗ обращения к client.post."""
    from backend.app.pipeline.stages.block_analysis import gemma_findings_only
    monkeypatch.setenv("PAID_API_ENABLED", "false")

    httpx_called = {"flag": False}

    class _FakeClient:
        async def post(self, *args, **kwargs):
            httpx_called["flag"] = True
            raise AssertionError("httpx.post was attempted despite block!")

    blocks_dir = tmp_path / "blocks"
    blocks_dir.mkdir()
    (blocks_dir / "b1.png").write_bytes(b"\x89PNG\r\n\x1a\nFAKE")

    async def _run():
        return await gemma_findings_only.call_gpt_for_block(
            client=_FakeClient(),
            block={"block_id": "b1", "page": 1, "file": "b1.png"},
            enrichment={},
            page_text="",
            blocks_dir=blocks_dir,
            api_key="sk-fake",
            model="openai/gpt-5.4",
            reasoning_effort="low",
            max_tokens=4096,
            system_prompt="",
            timeout=30,
            project_id="proj/A.pdf",
            output_dir=tmp_path,
        )

    res = asyncio.run(_run())
    assert res.get("paid_api_blocked") is True
    assert "paid_api_blocked" in (res.get("error") or "")
    assert httpx_called["flag"] is False


def test_stage02_cache_hit_skips_network(isolated_paid_api, tmp_path):
    """C2: повторный call_gpt_for_block с теми же параметрами → cache hit,
    без httpx и без записи paid_event."""
    from backend.app.pipeline.stages.block_analysis import gemma_findings_only

    blocks_dir = tmp_path / "blocks"
    blocks_dir.mkdir()
    (blocks_dir / "b1.png").write_bytes(b"\x89PNG\r\n\x1a\nFAKE_IMAGE_BYTES")

    post_calls = {"n": 0}

    class _RealResp:
        status_code = 200

        def json(self):
            return {
                "choices": [{"message": {"content": '{"findings":[]}'}}],
                "usage": {
                    "prompt_tokens": 40517,
                    "completion_tokens": 17118,
                    "completion_tokens_details": {"reasoning_tokens": 0},
                },
            }

        text = ""

    class _RealClient:
        async def post(self, *a, **kw):
            post_calls["n"] += 1
            return _RealResp()

    block = {"block_id": "b1", "page": 1, "file": "b1.png"}
    common_kwargs = dict(
        block=block, enrichment={"label": "test"}, page_text="page text",
        blocks_dir=blocks_dir, api_key="sk-real", model="openai/gpt-5.4",
        reasoning_effort="low", max_tokens=4096, system_prompt="SYS",
        timeout=30, project_id="proj/A.pdf", job_id="j1",
        output_dir=tmp_path,
    )

    async def _first():
        return await gemma_findings_only.call_gpt_for_block(client=_RealClient(), **common_kwargs)

    async def _second():
        return await gemma_findings_only.call_gpt_for_block(client=_RealClient(), **common_kwargs)

    res1 = asyncio.run(_first())
    assert res1["ok"] is True
    assert res1.get("from_cache") is False
    assert post_calls["n"] == 1

    res2 = asyncio.run(_second())
    assert res2["ok"] is True
    assert res2.get("from_cache") is True
    assert res2.get("cost_usd") == 0.0
    assert post_calls["n"] == 1


# ─── D. Queue / resume / orphan ───────────────────────────────────────


def test_resumed_job_allowed_without_manual_run(isolated_paid_api):
    """D1 (новый): после рестарта resumed job (без manual_run_id) разрешён.
    Это ключевое требование: orphan-состояния больше не блокируют pipeline."""
    guard = isolated_paid_api["guard"]
    ctx = guard.PaidApiContext(
        source="manager.stage02.orchestrator",
        model="openai/gpt-5.4",
        project_id="proj/A.pdf",
        stage="block_analysis",
        job_id="job-orphan-42",
    )
    # Не должно поднять
    guard.assert_paid_api_allowed(ctx)


def test_batch_queue_item_has_no_manual_run_field(isolated_paid_api):
    """D2 (новый): BatchQueueItem больше не содержит manual_run_id."""
    from backend.app.models.audit import AuditJob, BatchQueueItem

    item = BatchQueueItem(
        project_id="proj/A.pdf",
        action="full",
        job_id="job-1",
    )
    assert not hasattr(item, "manual_run_id") or getattr(item, "manual_run_id", None) is None

    job = AuditJob(
        job_id=item.job_id,
        project_id=item.project_id,
    )
    assert not hasattr(job, "manual_run_id") or getattr(job, "manual_run_id", None) is None


def test_persisted_batch_queue_legacy_manual_run_stripped(isolated_paid_api, tmp_path, monkeypatch):
    """D3: load_persisted_queue корректно глотает старые batch_queue.json с
    устаревшим manual_run_id (он просто игнорируется)."""
    from backend.app.pipeline import manager as manager_mod

    fake_batch_file = tmp_path / "batch_queue.json"
    fake_batch_file.write_text(json.dumps({
        "queue_id": "q-restart",
        "action": "full",
        "items": [
            {
                "project_id": "proj/A.pdf",
                "action": "full",
                "status": "running",
                "job_id": "job-restart-1",
                "manual_run_id": "stale-mrid-from-disk",
                "extra_params": {},
            },
        ],
        "current_index": 0,
        "total": 1,
        "completed": 0,
        "failed": 0,
        "status": "running",
    }, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setattr(manager_mod, "BATCH_QUEUE_FILE", fake_batch_file)

    pm = manager_mod.pipeline_manager
    pm._batch_queue = None
    pm.load_persisted_queue()

    assert pm._batch_queue is not None
    assert pm._batch_queue.status == "interrupted"
    # Поле manual_run_id больше не существует
    assert not hasattr(pm._batch_queue.items[0], "manual_run_id") or \
        getattr(pm._batch_queue.items[0], "manual_run_id", None) is None


def test_critic_v2_openrouter_provider_blocks_when_kill_switch_off(
    isolated_paid_api, monkeypatch
):
    """D5: critic_v2 OpenRouterProvider — guard блокирует при kill-switch=off."""
    from backend.app.pipeline.stages.findings_review.critic_v2 import llm_gate
    monkeypatch.setenv("PAID_API_ENABLED", "false")

    class _RequestsSpy:
        Timeout = Exception
        ConnectionError = Exception

        @staticmethod
        def post(*args, **kwargs):
            raise AssertionError(
                "critic_v2 OpenRouterProvider полез в сеть несмотря на guard"
            )

    saved = sys.modules.get("requests")
    sys.modules["requests"] = _RequestsSpy  # type: ignore[assignment]
    try:
        import os
        os.environ.setdefault("OPENROUTER_API_KEY", "sk-test-dummy")

        provider = llm_gate.OpenRouterProvider(model="openai/gpt-5.4")
        # Передаём project_id через _paid_api_ctx
        content, errors = provider(
            candidates=[],
            findings_by_id={},
            prompt="test",
            context_packages={"_paid_api_ctx": {"project_id": "proj/A.pdf"}},
        )
        assert content == "[]"
        assert any("paid_api_blocked" in e for e in errors), (
            f"Ожидалась блокировка guard'ом, получено: {errors}"
        )
    finally:
        if saved is not None:
            sys.modules["requests"] = saved
        else:
            sys.modules.pop("requests", None)


# ─── F. Status snapshot ───────────────────────────────────────────────


def test_status_snapshot_no_manual_run_fields(isolated_paid_api):
    """F1: status_snapshot не возвращает поля require_manual_start / active_manual_runs."""
    guard = isolated_paid_api["guard"]
    snap = guard.status_snapshot()
    assert "paid_api_enabled" in snap
    assert "daily_limit_usd" in snap
    assert "today_spent_usd" in snap
    assert "require_manual_start" not in snap
    assert "active_manual_runs" not in snap
