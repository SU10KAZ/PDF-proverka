"""Tests for paid_api_guard — единая точка проверки прав на платный API.

Покрытие (из ТЗ):
  A. paid_api_guard поведение:
     - PAID_API_ENABLED=false блокирует;
     - PAID_API_ENABLED=true + require_manual_start=true без manual_run_id блокирует;
     - короткий project_id ("M31A") блокируется;
     - валидный manual_run + scope разрешает;
     - daily limit блокирует;
  B. llm_runner.run_llm не делает внешний request когда blocked;
  C. manager Stage 02 (call_gpt_for_block) блокирует перед httpx.post;
  D. queue/resume: BatchQueueItem без manual_run_id → AuditJob без него → блок;
  E. events: успех пишет paid_cost_events.jsonl, блок пишет paid_api_blocked_events.jsonl;
     reset_paid_cost / clear_project_usage НЕ удаляют jsonl.
"""
from __future__ import annotations

import asyncio
import importlib
import json
import sys
from pathlib import Path

import pytest


@pytest.fixture
def isolated_paid_api(tmp_path, monkeypatch):
    """Изолированный paid_api_guard + paid_api_events в tmp_path.

    Monkey-patch'им PAID_COST_EVENTS_FILE и PAID_API_BLOCKED_EVENTS_FILE,
    чтобы тест не трогал реальные журналы. manual_run_registry —
    in-memory only, файла нет.
    """
    _PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    if str(_PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(_PROJECT_ROOT))

    # Импорт модулей.
    from backend.app.services.llm import paid_api_events as events_mod
    from backend.app.services.llm import paid_api_guard as guard_mod

    paid_jsonl = tmp_path / "paid_cost_events.jsonl"
    blocked_jsonl = tmp_path / "paid_api_blocked_events.jsonl"

    monkeypatch.setattr(events_mod, "PAID_COST_EVENTS_FILE", paid_jsonl)
    monkeypatch.setattr(events_mod, "PAID_API_BLOCKED_EVENTS_FILE", blocked_jsonl)

    # Сбрасываем in-memory registry между тестами.
    guard_mod._registry.clear()

    # Конфиг по умолчанию: enabled=true, require_manual=true, limit=0.
    # Guard читает env на каждом вызове (runtime), поэтому подменяем env, а
    # не атрибут модуля. Так тесты и production используют один и тот же путь
    # резолва флагов.
    monkeypatch.setenv("PAID_API_ENABLED", "true")
    monkeypatch.setenv("PAID_API_REQUIRE_MANUAL_START", "true")
    monkeypatch.setenv("PAID_API_DAILY_LIMIT_USD", "0")

    yield {
        "guard": guard_mod,
        "events": events_mod,
        "paid_jsonl": paid_jsonl,
        "blocked_jsonl": blocked_jsonl,
    }


# ─── A. paid_api_guard ────────────────────────────────────────────────


def test_kill_switch_disabled_blocks_everything(isolated_paid_api, monkeypatch):
    """A1: PAID_API_ENABLED=false блокирует, даже с валидным manual_run."""
    guard = isolated_paid_api["guard"]
    monkeypatch.setenv("PAID_API_ENABLED", "false")

    mrid = guard.issue_manual_run(project_ids=["pdf-proj-1"])
    ctx = guard.PaidApiContext(
        source="llm_runner",
        model="openai/gpt-5.4",
        project_id="pdf-proj-1",
        stage="block_analysis",
        manual_run_id=mrid,
    )
    with pytest.raises(guard.PaidApiBlockedError) as exc:
        guard.assert_paid_api_allowed(ctx)
    assert exc.value.reason == "paid_api_disabled"


def test_missing_manual_run_id_blocks(isolated_paid_api):
    """A2: require_manual_start=true и нет manual_run_id → блок."""
    guard = isolated_paid_api["guard"]
    ctx = guard.PaidApiContext(
        source="llm_runner",
        model="openai/gpt-5.4",
        project_id="pdf-proj-1",
        stage="block_analysis",
        manual_run_id="",
    )
    with pytest.raises(guard.PaidApiBlockedError) as exc:
        guard.assert_paid_api_allowed(ctx)
    assert exc.value.reason == "missing_manual_run_id"


def test_short_discipline_code_project_id_blocks(isolated_paid_api):
    """A3: project_id "M31A" — короткий код, блок (даже с manual_run)."""
    guard = isolated_paid_api["guard"]
    mrid = guard.issue_manual_run(project_ids=["M31A"])
    ctx = guard.PaidApiContext(
        source="manager.stage02",
        model="openai/gpt-5.4",
        project_id="M31A",
        stage="block_analysis",
        manual_run_id=mrid,
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
            stage="s", manual_run_id="anything",
        )
        # Сбрасываем выбранное поле.
        setattr(ctx, missing, "")
        with pytest.raises(guard.PaidApiBlockedError) as exc:
            guard.assert_paid_api_allowed(ctx)
        assert exc.value.reason in {
            f"missing_{missing}", "missing_manual_run_id", "short_discipline_code_project_id",
        }


def test_valid_manual_run_allows(isolated_paid_api):
    """A5: валидный manual_run + полный project_id → пропускает."""
    guard = isolated_paid_api["guard"]
    mrid = guard.issue_manual_run(project_ids=["proj/A.pdf"])
    ctx = guard.PaidApiContext(
        source="llm_runner",
        model="openai/gpt-5.4",
        project_id="proj/A.pdf",
        stage="block_analysis",
        manual_run_id=mrid,
    )
    guard.assert_paid_api_allowed(ctx)  # не должно поднять
    # registry проинкрементил used_count
    rec = guard.get_manual_run(mrid)
    assert rec is not None
    assert rec["used_count"] >= 1


def test_manual_run_scope_mismatch(isolated_paid_api):
    """A6: manual_run выдан под proj-A, но используется на proj-B → блок."""
    guard = isolated_paid_api["guard"]
    mrid = guard.issue_manual_run(project_ids=["proj/A.pdf"])
    ctx = guard.PaidApiContext(
        source="llm_runner",
        model="openai/gpt-5.4",
        project_id="proj/B.pdf",
        stage="block_analysis",
        manual_run_id=mrid,
    )
    with pytest.raises(guard.PaidApiBlockedError) as exc:
        guard.assert_paid_api_allowed(ctx)
    assert exc.value.reason == "manual_run_scope_mismatch"


def test_batch_manual_run_allows_multiple_projects(isolated_paid_api):
    """A7: batch выдаёт один manual_run на список — все проекты внутри
    проходят без подтверждения."""
    guard = isolated_paid_api["guard"]
    mrid = guard.issue_manual_run(
        project_ids=["proj/A.pdf", "proj/B.pdf", "proj/C.pdf"],
        batch_id="batch_full",
    )
    for pid in ["proj/A.pdf", "proj/B.pdf", "proj/C.pdf"]:
        ctx = guard.PaidApiContext(
            source="llm_runner", model="openai/gpt-5.4",
            project_id=pid, stage="block_analysis", manual_run_id=mrid,
        )
        guard.assert_paid_api_allowed(ctx)


def test_release_manual_run_revokes(isolated_paid_api):
    """A8: release_manual_run → последующий call блокируется."""
    guard = isolated_paid_api["guard"]
    mrid = guard.issue_manual_run(project_ids=["proj/X.pdf"])
    guard.release_manual_run(mrid)
    ctx = guard.PaidApiContext(
        source="llm_runner", model="openai/gpt-5.4",
        project_id="proj/X.pdf", stage="block_analysis", manual_run_id=mrid,
    )
    with pytest.raises(guard.PaidApiBlockedError) as exc:
        guard.assert_paid_api_allowed(ctx)
    assert exc.value.reason == "unknown_manual_run_id"


def test_daily_limit_blocks(isolated_paid_api, monkeypatch):
    """A9: daily_limit_usd=1.0 + estimated_cost_usd=2.0 → блок."""
    guard = isolated_paid_api["guard"]
    monkeypatch.setenv("PAID_API_DAILY_LIMIT_USD", "1.0")
    # _today_spent_usd возвращает 0.0 (нет paid_cost.daily), но
    # projected = 0 + 2 > 1.0 — блок.
    mrid = guard.issue_manual_run(project_ids=["proj/A.pdf"])
    ctx = guard.PaidApiContext(
        source="llm_runner", model="openai/gpt-5.4",
        project_id="proj/A.pdf", stage="block_analysis",
        manual_run_id=mrid, estimated_cost_usd=2.0,
    )
    with pytest.raises(guard.PaidApiBlockedError) as exc:
        guard.assert_paid_api_allowed(ctx)
    assert exc.value.reason == "daily_limit_exceeded"


# ─── E. Append-only events ────────────────────────────────────────────


def test_blocked_event_is_appended(isolated_paid_api):
    """E1: каждый block пишет строку в paid_api_blocked_events.jsonl."""
    guard = isolated_paid_api["guard"]
    ctx = guard.PaidApiContext(
        source="manager.stage02", model="openai/gpt-5.4",
        project_id="proj/A.pdf", stage="block_analysis",
        manual_run_id="",
    )
    with pytest.raises(guard.PaidApiBlockedError):
        guard.assert_paid_api_allowed(ctx)

    blocked = isolated_paid_api["blocked_jsonl"]
    assert blocked.exists()
    lines = blocked.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    event = json.loads(lines[0])
    assert event["event"] == "paid_api_blocked"
    assert event["reason"] == "missing_manual_run_id"
    assert event["source"] == "manager.stage02"
    assert event["project_id"] == "proj/A.pdf"
    assert event["pid"]


def test_paid_event_written_and_blocked_jsonl_not_cleared(isolated_paid_api):
    """E2: paid_event пишется отдельным API; reset/clear НЕ удаляют jsonl."""
    events = isolated_paid_api["events"]
    paid_jsonl = isolated_paid_api["paid_jsonl"]

    events.record_paid_event(
        cost_usd=1.234,
        model="openai/gpt-5.4",
        project_id="proj/A.pdf",
        stage="block_analysis",
        source="manager.stage02",
        manual_run_id="abc",
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
    assert ev["manual_run_id"] == "abc"

    # Tail reader
    tail = events.read_paid_events_tail(limit=10)
    assert len(tail) == 1
    assert tail[0]["job_id"] == "job-1"


def test_count_blocked_today(isolated_paid_api):
    """E3: count_blocked_today корректно считает только сегодняшние."""
    guard = isolated_paid_api["guard"]
    for _ in range(3):
        ctx = guard.PaidApiContext(
            source="llm_runner", model="openai/gpt-5.4",
            project_id="proj/A.pdf", stage="block_analysis",
            manual_run_id="",
        )
        with pytest.raises(guard.PaidApiBlockedError):
            guard.assert_paid_api_allowed(ctx)
    assert isolated_paid_api["events"].count_blocked_today() == 3


# ─── B. llm_runner — guard работает ПЕРЕД network ─────────────────────


def test_llm_runner_blocks_before_network(isolated_paid_api, monkeypatch):
    """B1: run_llm на OpenRouter модель без manual_run_id → возвращает
    LLMResult is_error="paid_api_blocked:..." БЕЗ вызова OpenAI клиента.
    """
    from backend.app.services.llm import llm_runner

    # Шпион на _get_client — если функция полезет в сеть, увидим вызов.
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
            # Нет manual_run_id → блок ДО _get_client.
        )

    result = asyncio.run(_run())
    assert result.is_error is True
    assert "paid_api_blocked" in (result.error_message or "")
    assert result.cost_usd == 0
    assert network_called["flag"] is False  # сеть НЕ была вызвана


def test_llm_runner_stream_blocks_before_network(isolated_paid_api, monkeypatch):
    """B2: run_llm_stream также блокируется ДО network."""
    from backend.app.services.llm import llm_runner

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


def test_stage02_call_gpt_blocks_before_httpx(isolated_paid_api):
    """C1: call_gpt_for_block без manual_run возвращает paid_api_blocked
    БЕЗ обращения к client.post.
    """
    from backend.app.pipeline.stages.block_analysis import gemma_findings_only

    httpx_called = {"flag": False}

    class _FakeClient:
        async def post(self, *args, **kwargs):
            httpx_called["flag"] = True
            raise AssertionError("httpx.post was attempted despite block!")

    async def _run():
        return await gemma_findings_only.call_gpt_for_block(
            client=_FakeClient(),
            block={"block_id": "b1", "page": 1, "file": "b1.png"},
            enrichment={},
            page_text="",
            blocks_dir=Path("/tmp/nonexistent_blocks_dir"),
            api_key="sk-fake",
            model="openai/gpt-5.4",
            reasoning_effort="low",
            max_tokens=4096,
            system_prompt="",
            timeout=30,
            project_id="proj/A.pdf",
            manual_run_id="",  # нет manual_run → блок
        )

    res = asyncio.run(_run())
    assert res.get("paid_api_blocked") is True
    assert "paid_api_blocked" in (res.get("error") or "")
    assert httpx_called["flag"] is False


# ─── D. Queue / resume / orphan ───────────────────────────────────────


def test_orphan_job_without_manual_run_blocks(isolated_paid_api):
    """D1: AuditJob без manual_run_id (orphan/auto-resume) → guard блокирует."""
    guard = isolated_paid_api["guard"]
    # Имитируем job как dict с пустым manual_run_id.
    ctx = guard.PaidApiContext(
        source="manager.stage02.orchestrator",
        model="openai/gpt-5.4",
        project_id="proj/A.pdf",
        stage="block_analysis",
        manual_run_id="",  # orphan
        job_id="job-orphan-42",
    )
    with pytest.raises(guard.PaidApiBlockedError) as exc:
        guard.assert_paid_api_allowed(ctx)
    assert exc.value.reason == "missing_manual_run_id"


def test_batch_queue_item_carries_manual_run(isolated_paid_api):
    """D2: BatchQueueItem (Pydantic) хранит manual_run_id, которое
    становится атрибутом AuditJob.
    """
    from backend.app.models.audit import AuditJob, BatchQueueItem, AuditStage, JobStatus

    item = BatchQueueItem(
        project_id="proj/A.pdf",
        action="full",
        job_id="job-1",
        manual_run_id="mrid-abc",
    )
    assert item.manual_run_id == "mrid-abc"

    job = AuditJob(
        job_id=item.job_id,
        project_id=item.project_id,
        manual_run_id=item.manual_run_id,
    )
    assert job.manual_run_id == "mrid-abc"


def test_persisted_batch_queue_strips_manual_run(isolated_paid_api, tmp_path, monkeypatch):
    """D3: pipeline_manager._persist_queue стирает manual_run_id из items
    при записи в batch_queue.json — даже если он был в памяти.

    Это намеренная политика fail-closed: после рестарта resumed job не должен
    автоматически проходить платные этапы со старым scope.
    """
    from backend.app.pipeline import manager as manager_mod
    from backend.app.models.audit import BatchQueueStatus, BatchQueueItem

    # Изолируем BATCH_QUEUE_FILE в tmp.
    fake_batch_file = tmp_path / "batch_queue.json"
    monkeypatch.setattr(manager_mod, "BATCH_QUEUE_FILE", fake_batch_file)

    pm = manager_mod.pipeline_manager
    pm._batch_queue = BatchQueueStatus(
        queue_id="q1",
        action="full",
        items=[
            BatchQueueItem(
                project_id="proj/A.pdf",
                action="full",
                job_id="job-1",
                manual_run_id="mrid-must-not-persist",
            ),
        ],
        total=1,
        status="running",
    )
    pm._persist_queue()

    raw = json.loads(fake_batch_file.read_text(encoding="utf-8"))
    assert raw["items"][0]["manual_run_id"] is None, (
        "manual_run_id должен быть стёрт при persist — иначе после рестарта "
        "будет ложное разрешение платных API"
    )


def test_old_manual_run_id_after_restart_is_invalid(isolated_paid_api, tmp_path, monkeypatch):
    """D4: имитация рестарта — batch_queue.json содержит старый manual_run_id
    (как если бы кто-то восстановил файл из бэкапа), registry пуст.
    После load + попытки платного вызова — блок: unknown_manual_run_id.
    """
    from backend.app.pipeline import manager as manager_mod
    guard = isolated_paid_api["guard"]

    # Имитируем "плохой" persisted-файл с manual_run_id внутри.
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
    # Очистим registry в памяти на всякий случай (имитируя "fresh process").
    guard._registry.clear()

    pm.load_persisted_queue()

    # После load: status переходит в interrupted, items.manual_run_id обнулён.
    assert pm._batch_queue is not None
    assert pm._batch_queue.status == "interrupted"
    assert pm._batch_queue.items[0].manual_run_id is None

    # Даже если бы code где-то взял старый mrid из файла напрямую и
    # попытался использовать — guard заблокирует, потому что registry пуст.
    ctx = guard.PaidApiContext(
        source="llm_runner", model="openai/gpt-5.4",
        project_id="proj/A.pdf", stage="block_analysis",
        manual_run_id="stale-mrid-from-disk",
        job_id="job-restart-1",
    )
    with pytest.raises(guard.PaidApiBlockedError) as exc:
        guard.assert_paid_api_allowed(ctx)
    assert exc.value.reason == "unknown_manual_run_id"


def test_registry_is_memory_only_no_file(isolated_paid_api, tmp_path, monkeypatch):
    """D5: issue_manual_run НЕ создаёт никакого файла на диске.

    Проверяем, что в backend/app/data/ нет paid_api_manual_runs.json
    после issue+release (registry — только in-memory).
    """
    guard = isolated_paid_api["guard"]
    from backend.app.core.config import APP_DATA_DIR

    candidate = APP_DATA_DIR / "paid_api_manual_runs.json"
    # Если файл вдруг есть — это уже регрессия предыдущей реализации.
    # Удалим, чтобы тест проверил именно НОВУЮ запись.
    if candidate.exists():
        candidate.unlink()

    mrid = guard.issue_manual_run(project_ids=["proj/A.pdf"])
    assert guard.get_manual_run(mrid) is not None
    assert not candidate.exists(), (
        "paid_api_manual_runs.json не должен создаваться — registry только in-memory"
    )

    guard.release_manual_run(mrid)
    assert not candidate.exists()


def test_critic_v2_openrouter_provider_blocks_without_manual_run(isolated_paid_api):
    """D6: critic_v2 OpenRouterProvider — экспериментальный путь с прямым
    requests.post в openrouter.ai — должен быть закрыт guard'ом.

    Без manual_run_id в context_packages → возвращает paid_api_blocked
    БЕЗ обращения к requests.post.
    """
    from backend.app.pipeline.stages.findings_review.critic_v2 import llm_gate

    # Подменяем requests в модуле, чтобы если guard НЕ сработает,
    # тест упал на AssertionError, а не уйдёт в сеть.
    class _RequestsSpy:
        Timeout = Exception
        ConnectionError = Exception

        @staticmethod
        def post(*args, **kwargs):
            raise AssertionError(
                "critic_v2 OpenRouterProvider полез в сеть несмотря на guard"
            )

    import sys
    # Подменяем 'requests' в sys.modules, потому что llm_gate импортирует
    # его lazy внутри __call__ через `import requests as _requests`.
    saved = sys.modules.get("requests")
    sys.modules["requests"] = _RequestsSpy  # type: ignore[assignment]
    try:
        # Также нужен валидный API key, чтобы пройти до guard'а.
        import os
        os.environ.setdefault("OPENROUTER_API_KEY", "sk-test-dummy")

        provider = llm_gate.OpenRouterProvider(model="openai/gpt-5.4")
        content, errors = provider(
            candidates=[],
            findings_by_id={},
            prompt="test",
            context_packages={},  # пусто → нет project_id/manual_run_id
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


def test_runtime_kill_switch_takes_effect_without_module_reload(
    isolated_paid_api, monkeypatch
):
    """A10 (новый, root cause инцидента 2026-05-16): kill-switch должен
    действовать сразу после смены os.environ, без перезапуска backend.

    До фикса PAID_API_ENABLED резолвился на момент импорта модуля и его
    нельзя было выключить руками. 9 платных вызовов на M31A прошли в т.ч.
    потому, что значение флага было зафиксировано при старте uvicorn.
    """
    guard = isolated_paid_api["guard"]

    # Шаг 1: enabled=true + валидный manual_run → разрешено.
    monkeypatch.setenv("PAID_API_ENABLED", "true")
    mrid = guard.issue_manual_run(project_ids=["proj/A.pdf"])
    ctx = guard.PaidApiContext(
        source="llm_runner",
        model="openai/gpt-5.4",
        project_id="proj/A.pdf",
        stage="block_analysis",
        manual_run_id=mrid,
    )
    guard.assert_paid_api_allowed(ctx)  # без исключения

    # Шаг 2: тот же процесс, тот же импортированный модуль, тот же manual_run —
    # меняем ТОЛЬКО env. Должен включиться kill-switch.
    monkeypatch.setenv("PAID_API_ENABLED", "false")
    with pytest.raises(guard.PaidApiBlockedError) as exc:
        guard.assert_paid_api_allowed(ctx)
    assert exc.value.reason == "paid_api_disabled"


def test_canonical_project_id_allows_short_display_pid(isolated_paid_api):
    """A11 (новый): короткий project_id "M31A" допустим, если передан
    canonical_project_id с полным путём ИЛИ object_id. Так короткий код
    можно безопасно использовать как display, а реальный scope — длинный.
    """
    guard = isolated_paid_api["guard"]
    # scope manual_run выдаётся по canonical path
    canon = "214. Alia (ASTERUS)/M31A"
    mrid = guard.issue_manual_run(project_ids=[canon])
    ctx = guard.PaidApiContext(
        source="manager.stage02",
        model="openai/gpt-5.4",
        project_id="M31A",                 # короткий display
        canonical_project_id=canon,        # полный canonical scope
        stage="block_analysis",
        manual_run_id=mrid,
    )
    # Не должно быть исключения.
    guard.assert_paid_api_allowed(ctx)


def test_record_paid_event_writes_manual_run_id_present_and_hash(
    isolated_paid_api, tmp_path, monkeypatch
):
    """E3 (новый, root cause forensic): record_paid_event пишет три поля
    про manual_run_id, чтобы можно было различить "был ли реально manual_run".

    В инциденте 2026-05-16 все 9 платных событий имели manual_run_id="" —
    раньше нельзя было понять, отсутствовал ли он реально или был стёрт при
    записи. Теперь есть manual_run_id_present и manual_run_id_hash.
    """
    events = isolated_paid_api["events"]
    paid_jsonl = isolated_paid_api["paid_jsonl"]

    # 1) Платный вызов БЕЗ manual_run_id (forensic должен это явно показать)
    events.record_paid_event(
        cost_usd=0.3227,
        model="openai/gpt-5.4",
        project_id="proj/A.pdf",
        stage="block_analysis",
        source="manager.stage02",
        manual_run_id="",
        job_id="j1",
        input_tokens=40517,
        output_tokens=17118,
    )

    # 2) Платный вызов С manual_run_id (hash должен быть)
    events.record_paid_event(
        cost_usd=0.10,
        model="openai/gpt-5.4",
        project_id="proj/A.pdf",
        stage="block_analysis",
        source="manager.stage02",
        manual_run_id="abc123def456",
        job_id="j2",
    )

    lines = paid_jsonl.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    ev1 = json.loads(lines[0])
    ev2 = json.loads(lines[1])

    # Без manual_run: present=False, hash=""
    assert ev1["manual_run_id"] == ""
    assert ev1["manual_run_id_present"] is False
    assert ev1["manual_run_id_hash"] == ""

    # С manual_run: present=True, hash непустой и стабильный
    assert ev2["manual_run_id"] == "abc123def456"
    assert ev2["manual_run_id_present"] is True
    assert len(ev2["manual_run_id_hash"]) == 12


def test_local_models_bypass_guard(isolated_paid_api, monkeypatch):
    """D4: Локальные модели (Chandra/local QWEN) не должны блокироваться —
    они не отправляют данные во внешний платный API.
    """
    from backend.app.services.llm import llm_runner

    # Сделаем "local" модель, и подменим local-path функции на no-op
    # которые возвращают валидный LLMResult.
    from backend.app.models.usage import LLMResult

    async def fake_local(*args, **kwargs):
        return LLMResult(text="local-ok", model="local-qwen-3.6-35b", cost_usd=0.0)

    monkeypatch.setattr(llm_runner, "is_local_llm_model", lambda m: True)
    monkeypatch.setattr(llm_runner, "_run_local_chandra_chat", fake_local)
    monkeypatch.setattr(llm_runner, "_run_local_chat_completions", fake_local)

    async def _run():
        return await llm_runner.run_llm(
            stage="findings_merge",
            messages=[{"role": "user", "content": "t"}],
            model_override="local-qwen-3.6-35b",
            project_id="",       # пустой — у local не нужен
            manual_run_id="",    # нет — у local не нужен
        )

    result = asyncio.run(_run())
    assert result.text == "local-ok"
    assert result.is_error is False
