"""
test_audit_prefetch_version_aware.py
------------------------------------
Регрессия Task 2/6: audit queue pre-crop и Gemma prefetch стали version-aware.

До фикса prefetch для V2+ проектов использовал V1/root-dir:
- `_precrop_project` имел explicit guard `latest_vid != "v1"` → V2 вообще не
  кропился (а если бы кропился — писал в V1 root);
- `_run_precrop_loop` искал `*_result.json` в V1 root и терял version_id;
- `_run_gemma_prefetch_loop` резолвил `proj_dir` через resolve_project_dir
  (PRIMARY=V1) и создавал phantom_job БЕЗ version_id → ctx уходил в V1;
- `_run_gemma_enrichment_stage` double-check читал V1 root → валидная Gemma V1
  заставляла main worker пропустить Gemma для V2.

Все тесты дискриминирующие: на старом коде падают (inspected dir == V1).
LLM/crop-скрипты/реальный Gemma instance не вызываются — мокаются.

Run:
    python -m pytest tests/test_audit_prefetch_version_aware.py -v
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from fastapi.testclient import TestClient  # noqa: E402


# ─── Fixtures (изолированный projects/ с M31A + созданной V2) ─────────────────


@pytest.fixture
def projects_dir(tmp_path, monkeypatch):
    """Изолированный projects/ с одним M31A, у которого V1 наполнена данными."""
    p = tmp_path / "projects"
    p.mkdir()
    pdir = p / "M31A"
    out = pdir / "_output"
    out.mkdir(parents=True)
    (pdir / "project_info.json").write_text(
        json.dumps({
            "project_id": "M31A",
            "name": "M31A",
            "section": "EOM",
            "pdf_file": "document.pdf",
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    (pdir / "document.pdf").write_bytes(b"%PDF-1.4 fake")
    (out / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": "F-001", "severity": "КРИТИЧЕСКОЕ"}]},
                   ensure_ascii=False),
        encoding="utf-8",
    )
    (out / "pipeline_log.json").write_text(
        json.dumps({"version": 1, "stages": {"findings_merge": {"status": "done"}}},
                   ensure_ascii=False),
        encoding="utf-8",
    )

    import backend.app.services.common.project_service as ps
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: p)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)
    monkeypatch.setattr(ps, "_document_cache", {})
    return p


@pytest.fixture
def v2_created(projects_dir):
    """Создать V2 для M31A. Возвращает (projects_dir, v1_dir, v2_dir, v2_output)."""
    from backend.app.main import app
    c = TestClient(app)
    r = c.post("/api/projects/M31A/versions", json={"comment": "V2"})
    assert r.status_code == 200, r.text
    v1_dir = projects_dir / "M31A(main)" / "M31A"
    v2_dir = projects_dir / "M31A(main)" / "M31A V2"
    v2_output = v2_dir / "_output"
    assert v2_output.exists()
    return projects_dir, v1_dir, v2_dir, v2_output


def _async_noop():
    async def _f(*a, **k):
        return None
    return _f


# ─── Fix 1: _precrop_project version-aware ────────────────────────────────────


def test_precrop_v2_crops_into_version_dir_not_v1_root(v2_created, monkeypatch):
    """V2 pre-crop передаёт в blocks.py путь ПАПКИ ВЕРСИИ, не V1 root.

    Закрывает Risk 1: V2 pre-crop пишет в V1 root. На старом коде функция
    возвращала False (guard `latest_vid != 'v1'`) ещё ДО вызова run_script —
    тест бы упал на `captured`.
    """
    import backend.app.pipeline.manager as mgr
    projects_dir, v1_dir, v2_dir, _ = v2_created

    # result.json должен лежать в папке ВЕРСИИ, иначе crop пропускается.
    (v2_dir / "M31A_result.json").write_text("{}", encoding="utf-8")

    captured: list = []

    async def fake_run_script(script, arglist, *, project_id=None, **kw):
        captured.append((script, list(arglist), project_id))
        return (0, "", "")

    monkeypatch.setattr(mgr, "run_script", fake_run_script)
    monkeypatch.setattr(mgr.ws_manager, "broadcast_global", _async_noop())

    pm = mgr.PipelineManager()
    ok = asyncio.run(pm._precrop_project("M31A", "v2"))

    assert ok is True, "V2 pre-crop должен выполниться (guard убран)"
    assert len(captured) == 1, "run_script обязан быть вызван (на старом коде — нет)"
    _script, arglist, proj_id = captured[0]
    # build_crop_args → ["crop", <project_path>, "--output-dir", ...]
    crop_path = arglist[1]
    assert "M31A V2" in crop_path, f"crop path должен быть в V2 dir, получено: {crop_path}"
    assert proj_id == "__PRECROP_M31A__"


def test_precrop_v1_still_uses_root(v2_created, monkeypatch):
    """Регрессия: V1 pre-crop по-прежнему кропит V1 root (не уходит в V2)."""
    import backend.app.pipeline.manager as mgr
    projects_dir, v1_dir, v2_dir, _ = v2_created

    (v1_dir / "M31A_result.json").write_text("{}", encoding="utf-8")

    captured: list = []

    async def fake_run_script(script, arglist, *, project_id=None, **kw):
        captured.append(list(arglist))
        return (0, "", "")

    monkeypatch.setattr(mgr, "run_script", fake_run_script)
    monkeypatch.setattr(mgr.ws_manager, "broadcast_global", _async_noop())

    pm = mgr.PipelineManager()
    ok = asyncio.run(pm._precrop_project("M31A", "v1"))

    assert ok is True
    crop_path = captured[0][1]
    assert "M31A V2" not in crop_path, f"V1 crop не должен уходить в V2: {crop_path}"
    # V1 primary живёт в контейнере M31A(main)/M31A
    assert crop_path.endswith(str(Path("M31A(main)") / "M31A"))


# ─── Fix 2: _run_precrop_loop version-aware selection + version_id passthrough ──


def test_precrop_loop_passes_version_id_and_checks_version_dir(v2_created, monkeypatch):
    """Loop находит result.json в папке ВЕРСИИ и передаёт version_id в _precrop_project.

    Дискриминирующе: result.json кладём ТОЛЬКО в V2 dir. На старом коде loop
    глобил V1 root → target не найден → _precrop_project не вызван.
    """
    import backend.app.pipeline.manager as mgr
    from backend.app.models.audit import BatchQueueStatus, BatchQueueItem
    projects_dir, v1_dir, v2_dir, _ = v2_created

    (v2_dir / "M31A_result.json").write_text("{}", encoding="utf-8")
    # V1 root специально оставляем без result.json (фикстура его не создаёт).
    assert not list(v1_dir.glob("*_result.json"))

    queue = BatchQueueStatus(
        queue_id="q", action="full", status="running", total=1,
        items=[BatchQueueItem(project_id="M31A", version_id="v2",
                              action="full", status="pending", job_id="j1")],
    )

    captured: list = []

    async def fake_precrop(self, pid, version_id=None):
        captured.append((pid, version_id))
        queue.status = "done"  # остановить loop после первой итерации
        return True

    monkeypatch.setattr(type(mgr.PipelineManager()), "_precrop_project", fake_precrop)

    pm = mgr.PipelineManager()

    async def _run():
        await asyncio.wait_for(pm._run_precrop_loop(queue), timeout=10)

    asyncio.run(_run())

    assert captured == [("M31A", "v2")], (
        f"loop должен был выбрать V2 по result.json в version dir и передать "
        f"version_id; получено: {captured}"
    )


def test_precrop_loop_skips_when_result_only_in_v1_for_v2_item(v2_created, monkeypatch):
    """Если result.json есть только в V1, а item — V2, loop НЕ кропит (version-isolation).

    На старом коде (глоб V1 root) — ошибочно бы закропил.
    """
    import backend.app.pipeline.manager as mgr
    from backend.app.models.audit import BatchQueueStatus, BatchQueueItem
    projects_dir, v1_dir, v2_dir, _ = v2_created

    (v1_dir / "M31A_result.json").write_text("{}", encoding="utf-8")  # только V1

    queue = BatchQueueStatus(
        queue_id="q", action="full", status="running", total=1,
        items=[BatchQueueItem(project_id="M31A", version_id="v2",
                              action="full", status="pending", job_id="j1")],
    )

    captured: list = []

    async def fake_precrop(self, pid, version_id=None):
        captured.append((pid, version_id))
        queue.status = "done"
        return True

    monkeypatch.setattr(type(mgr.PipelineManager()), "_precrop_project", fake_precrop)

    pm = mgr.PipelineManager()

    async def _run():
        # loop будет sleep(5) т.к. кандидата нет → останавливаем через таймаут
        task = asyncio.create_task(pm._run_precrop_loop(queue))
        await asyncio.sleep(0.5)
        queue.status = "done"
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(_run())
    assert captured == [], "V2-item не должен кропиться по V1 result.json"


# ─── Fix 3: _run_gemma_prefetch_loop version-aware proj_dir + phantom version_id ─


def test_gemma_prefetch_loop_phantom_job_carries_version_id(v2_created, monkeypatch):
    """phantom_job уносит version_id=v2 → ctx идёт в V2 (_output версии).

    Закрывает Risk: prefetch enrichment пишет в V1 root.
    Дискриминирующе: на старом коде phantom_job без version_id → ctx.version_id
    is None, ctx.output_dir = V1 root _output.
    """
    import backend.app.pipeline.manager as mgr
    from backend.app.models.audit import BatchQueueStatus, BatchQueueItem
    from backend.app.pipeline.stages.gemma_enrichment import (
        gemma_enrichment_contract as gec,
    )
    from backend.app.pipeline.stages.prepare.prepare_service import prepare_state

    projects_dir, v1_dir, v2_dir, v2_output = v2_created

    # Crops V2: index.json существует → кандидат не отбрасывается по "no crops".
    (v2_output / "blocks_gemma_100").mkdir(parents=True, exist_ok=True)
    (v2_output / "blocks_gemma_100" / "index.json").write_text("{}", encoding="utf-8")

    queue = BatchQueueStatus(
        queue_id="q", action="full", status="running", total=2, current_index=0,
        items=[
            BatchQueueItem(project_id="__RUN__", version_id="v1",
                          action="full", status="running", job_id="r0"),
            BatchQueueItem(project_id="M31A", version_id="v2",
                          action="full", status="pending", job_id="p1"),
        ],
    )

    state = {"ran": False}
    post_run_dirs: list = []

    def fake_valid(pd):
        p = Path(pd)
        if state["ran"]:
            post_run_dirs.append(p)
            return (True, "ok")
        return (False, "pending")

    monkeypatch.setattr(gec, "gemma_outputs_are_valid", fake_valid)

    captured: dict = {}

    async def fake_fn(ctx, force=False):
        captured["version_id"] = ctx.version_id
        captured["output_dir"] = Path(ctx.output_dir)
        state["ran"] = True
        queue.status = "done"  # остановить loop
        return SimpleNamespace(success=True, cancelled=False, error=None)

    monkeypatch.setattr(mgr, "_run_gemma_enrichment_stage_fn", fake_fn)
    monkeypatch.setattr(mgr.ws_manager, "broadcast_global", _async_noop())

    pm = mgr.PipelineManager()
    monkeypatch.setattr(pm, "_persist_queue", lambda *a, **k: None)
    monkeypatch.setattr(pm, "_broadcast_batch_progress", _async_noop())
    monkeypatch.setattr(pm, "_log", _async_noop())
    pm._batch_queue = queue
    pm._mark_gemma_stage_done("__RUN__")  # gate открыт

    prepare_state._global_lock = None  # свежий общий Gemma-lock
    pause = asyncio.Event(); pause.set()

    async def _run():
        await asyncio.wait_for(
            pm._run_gemma_prefetch_loop(queue, pause), timeout=15
        )

    asyncio.run(_run())

    assert captured.get("version_id") == "v2", (
        "phantom_job должен нести version_id=v2 (на старом коде — None)"
    )
    assert captured.get("output_dir") == v2_output, (
        f"ctx.output_dir должен быть V2 _output, получено: {captured.get('output_dir')}"
    )
    # Post-run валидация loop'а смотрит V2 dir, не V1 primary.
    assert v2_dir in post_run_dirs
    assert v1_dir not in post_run_dirs


# ─── Fix 4: _run_gemma_enrichment_stage double-check уходит в version dir ──────


def _make_stage_setup(v2_created, monkeypatch, *, v2_valid: bool):
    """Общая обвязка для тестов Fix 4. v2_valid управляет gemma_outputs_are_valid."""
    import backend.app.pipeline.manager as mgr
    from backend.app.models.audit import (
        AuditJob, AuditStage, JobStatus, BatchQueueStatus, BatchQueueItem,
    )
    from backend.app.pipeline.stages.gemma_enrichment import (
        gemma_enrichment_contract as gec,
    )
    from backend.app.pipeline.stages.prepare.prepare_service import prepare_state

    def fake_valid(pd):
        is_v2 = "M31A V2" in str(pd)
        if is_v2:
            return (v2_valid, "v2")
        return (True, "v1-valid")  # V1 всегда «валиден» — ловушка для старого кода

    monkeypatch.setattr(gec, "gemma_outputs_are_valid", fake_valid)

    inner_called = {"v": False}

    async def fake_fn(ctx, force=False):
        inner_called["v"] = True
        return SimpleNamespace(success=True, cancelled=False, error=None)

    monkeypatch.setattr(mgr, "_run_gemma_enrichment_stage_fn", fake_fn)
    monkeypatch.setattr(mgr.ws_manager, "broadcast_global", _async_noop())

    pm = mgr.PipelineManager()
    monkeypatch.setattr(pm, "_log", _async_noop())
    monkeypatch.setattr(pm, "_persist_queue", lambda *a, **k: None)

    queue = BatchQueueStatus(
        queue_id="q", action="full", status="running", total=1,
        items=[BatchQueueItem(project_id="M31A", version_id="v2",
                              action="full", status="running", job_id="j1",
                              gemma_prefetched=True)],
    )
    pm._batch_queue = queue
    prepare_state._global_lock = None

    job = AuditJob(
        job_id="j1", project_id="M31A", version_id="v2",
        stage=AuditStage.PREPARE, status=JobStatus.RUNNING,
    )
    return pm, job, inner_called


def test_gemma_stage_doublecheck_runs_when_v2_invalid_despite_valid_v1(
    v2_created, monkeypatch
):
    """V2 outputs невалидны, V1 валидны → main всё равно запускает Gemma.

    Дискриминирующе: на старом коде double-check читал V1 (валиден) → ошибочно
    пропускал Gemma для V2 (inner_called оставался False).
    """
    pm, job, inner_called = _make_stage_setup(v2_created, monkeypatch, v2_valid=False)
    asyncio.run(pm._run_gemma_enrichment_stage(job, force=False))
    assert inner_called["v"] is True, (
        "Gemma inner runner должен был вызваться: V2 outputs невалидны "
        "(на старом коде V1-валидность ошибочно пропускала этап)"
    )
    assert pm._current_gemma_stage_done.get("M31A") is True


def test_gemma_stage_doublecheck_skips_when_v2_valid(v2_created, monkeypatch):
    """V2 outputs валидны → Gemma пропускается (skip-путь работает на version dir)."""
    pm, job, inner_called = _make_stage_setup(v2_created, monkeypatch, v2_valid=True)
    asyncio.run(pm._run_gemma_enrichment_stage(job, force=False))
    assert inner_called["v"] is False, "при валидных V2 outputs inner runner не нужен"
    assert pm._current_gemma_stage_done.get("M31A") is True


# ─── Task 4/6: Gemma-lock priority — prefetch opportunistic, не блокирует main ─


def test_gemma_lock_intent_helpers():
    """begin/end/query счётчика намерения: монотонно, не уходит в минус."""
    import backend.app.pipeline.manager as mgr
    pm = mgr.PipelineManager()
    assert pm._main_wants_gemma_lock() is False
    pm._begin_main_gemma_lock_intent()
    assert pm._main_wants_gemma_lock() is True
    pm._begin_main_gemma_lock_intent()  # реентрантность
    assert pm._main_gemma_lock_intent == 2
    pm._end_main_gemma_lock_intent()
    assert pm._main_wants_gemma_lock() is True
    pm._end_main_gemma_lock_intent()
    assert pm._main_wants_gemma_lock() is False
    pm._end_main_gemma_lock_intent()  # не уходит в минус
    assert pm._main_gemma_lock_intent == 0


def test_main_audit_holds_intent_during_gemma_run_and_clears_after(
    v2_created, monkeypatch
):
    """main `_run_gemma_enrichment_stage` держит intent > 0 ВО ВРЕМЯ Gemma-run
    и очищает его после (в т.ч. при early-return / raise)."""
    import backend.app.pipeline.manager as mgr
    from backend.app.models.audit import (
        AuditJob, AuditStage, JobStatus, BatchQueueStatus, BatchQueueItem,
    )
    from backend.app.pipeline.stages.gemma_enrichment import (
        gemma_enrichment_contract as gec,
    )
    from backend.app.pipeline.stages.prepare.prepare_service import prepare_state

    monkeypatch.setattr(gec, "gemma_outputs_are_valid", lambda pd: (False, "pending"))

    intent_during_run = {"v": None}

    async def fake_fn(ctx, force=False):
        intent_during_run["v"] = ctx is not None and \
            pm._main_wants_gemma_lock()  # noqa: F821 (pm определён ниже до вызова)
        return SimpleNamespace(success=True, cancelled=False, error=None)

    monkeypatch.setattr(mgr, "_run_gemma_enrichment_stage_fn", fake_fn)
    monkeypatch.setattr(mgr.ws_manager, "broadcast_global", _async_noop())

    pm = mgr.PipelineManager()
    monkeypatch.setattr(pm, "_log", _async_noop())
    monkeypatch.setattr(pm, "_persist_queue", lambda *a, **k: None)
    pm._batch_queue = BatchQueueStatus(
        queue_id="q", action="full", status="running", total=1,
        items=[BatchQueueItem(project_id="M31A", version_id="v2",
                              action="full", status="running", job_id="j1")],
    )
    prepare_state._global_lock = None

    job = AuditJob(job_id="j1", project_id="M31A", version_id="v2",
                   stage=AuditStage.PREPARE, status=JobStatus.RUNNING)

    assert pm._main_wants_gemma_lock() is False  # до запуска
    asyncio.run(pm._run_gemma_enrichment_stage(job, force=False))

    assert intent_during_run["v"] is True, "во время Gemma-run intent должен быть поднят"
    assert pm._main_wants_gemma_lock() is False, "после завершения intent должен быть снят"
    assert pm._main_gemma_lock_intent == 0


def _build_prefetch_scenario(v2_created, monkeypatch):
    """Готовит prefetch-loop сценарий с готовым V2-кандидатом. Возвращает
    (mgr, pm, queue, pause, fn_called)."""
    import backend.app.pipeline.manager as mgr
    from backend.app.models.audit import BatchQueueStatus, BatchQueueItem
    from backend.app.pipeline.stages.gemma_enrichment import (
        gemma_enrichment_contract as gec,
    )
    from backend.app.pipeline.stages.prepare.prepare_service import prepare_state

    _projects_dir, _v1_dir, _v2_dir, v2_output = v2_created
    (v2_output / "blocks_gemma_100").mkdir(parents=True, exist_ok=True)
    (v2_output / "blocks_gemma_100" / "index.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(gec, "gemma_outputs_are_valid", lambda pd: (False, "pending"))

    fn_called = {"v": False}

    async def fake_fn(ctx, force=False):
        fn_called["v"] = True
        return SimpleNamespace(success=True, cancelled=False, error=None)

    monkeypatch.setattr(mgr, "_run_gemma_enrichment_stage_fn", fake_fn)
    monkeypatch.setattr(mgr.ws_manager, "broadcast_global", _async_noop())

    pm = mgr.PipelineManager()
    monkeypatch.setattr(pm, "_persist_queue", lambda *a, **k: None)
    monkeypatch.setattr(pm, "_broadcast_batch_progress", _async_noop())
    monkeypatch.setattr(pm, "_log", _async_noop())

    queue = BatchQueueStatus(
        queue_id="q", action="full", status="running", total=2, current_index=0,
        items=[
            BatchQueueItem(project_id="__RUN__", version_id="v1",
                          action="full", status="running", job_id="r0"),
            BatchQueueItem(project_id="M31A", version_id="v2",
                          action="full", status="pending", job_id="p1"),
        ],
    )
    pm._batch_queue = queue
    pm._mark_gemma_stage_done("__RUN__")  # gate открыт
    prepare_state._global_lock = None
    pause = asyncio.Event(); pause.set()
    return mgr, pm, queue, pause, fn_called


def test_prefetch_yields_when_main_wants_lock_pre_acquire(v2_created, monkeypatch):
    """Когда main держит intent — prefetch НЕ захватывает Gemma-лок и НЕ запускает
    enrichment (opportunistic). Дискриминирующе: на старом коде fn был бы вызван."""
    from backend.app.pipeline.stages.prepare.prepare_service import prepare_state
    mgr, pm, queue, pause, fn_called = _build_prefetch_scenario(v2_created, monkeypatch)

    pm._begin_main_gemma_lock_intent()  # main «хочет» лок

    async def _run():
        task = asyncio.create_task(pm._run_gemma_prefetch_loop(queue, pause))
        await asyncio.sleep(0.4)  # несколько итераций цикла
        queue.status = "done"
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(_run())

    assert fn_called["v"] is False, (
        "prefetch не должен запускать Gemma пока main держит intent"
    )
    assert prepare_state.get_lock().locked() is False, (
        "prefetch не должен держать общий Gemma-лок при активном main intent"
    )


def test_prefetch_post_acquire_releases_when_main_intent_appears(
    v2_created, monkeypatch
):
    """intent появляется в окне между pre-acquire проверкой и захватом: prefetch
    освобождает лок ПОСТ-acquire, не начав дорогой run.

    Эмулируем гонку: _main_wants_gemma_lock → False на 1-м вызове (pre-acquire),
    True на последующих (post-acquire и далее)."""
    from backend.app.pipeline.stages.prepare.prepare_service import prepare_state
    mgr, pm, queue, pause, fn_called = _build_prefetch_scenario(v2_created, monkeypatch)

    calls = {"n": 0}

    def fake_wants():
        calls["n"] += 1
        return calls["n"] >= 2  # 1-й (pre-acquire) False, со 2-го (post-acquire) True

    monkeypatch.setattr(pm, "_main_wants_gemma_lock", fake_wants)

    async def _run():
        task = asyncio.create_task(pm._run_gemma_prefetch_loop(queue, pause))
        await asyncio.sleep(0.4)
        queue.status = "done"
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(_run())

    assert calls["n"] >= 2, "post-acquire проверка intent должна была сработать"
    assert fn_called["v"] is False, "dорогой Gemma-run не должен стартовать после yield"
    assert prepare_state.get_lock().locked() is False, "лок должен быть освобождён"


def test_prefetch_runs_when_main_not_wanting_lock(v2_created, monkeypatch):
    """Sanity / регрессия happy-path: при intent==0 prefetch штатно запускает run."""
    mgr, pm, queue, pause, fn_called = _build_prefetch_scenario(v2_created, monkeypatch)
    assert pm._main_wants_gemma_lock() is False

    async def _run():
        await asyncio.wait_for(
            pm._run_gemma_prefetch_loop(queue, pause), timeout=15
        )

    # fn_called выставит queue.status? Нет — fake_fn здесь не трогает status.
    # Остановим loop из fake_fn:
    orig_fn = mgr._run_gemma_enrichment_stage_fn

    async def stopping_fn(ctx, force=False):
        fn_called["v"] = True
        queue.status = "done"
        return SimpleNamespace(success=True, cancelled=False, error=None)

    monkeypatch.setattr(mgr, "_run_gemma_enrichment_stage_fn", stopping_fn)
    asyncio.run(_run())
    assert fn_called["v"] is True, "при отсутствии main intent prefetch обязан выполнить run"


# ─── Task 5/6: lookahead — предсказуемое опережение B → C → D ──────────────────


@pytest.fixture
def proj_fs(tmp_path, monkeypatch):
    """Лёгкий мок ФС проектов: pid(+vid) → tmp-папка. Патчит resolve_project_dir
    и version_service.get_version_dir, чтобы тестировать чистые селекторы
    lookahead без полной контейнерно-версионной машинерии."""
    import backend.app.pipeline.manager as mgr
    from backend.app.services.common import version_service
    base = tmp_path / "projects"
    base.mkdir()

    def _dir_for(pid, vid=None):
        d = base / pid / (vid or "v1")
        d.mkdir(parents=True, exist_ok=True)
        return d

    monkeypatch.setattr(mgr, "resolve_project_dir", lambda pid: _dir_for(pid))
    monkeypatch.setattr(
        version_service, "get_version_dir",
        lambda root, pid, vid=None: _dir_for(pid, vid),
    )
    return SimpleNamespace(base=base, dir_for=_dir_for)


def _item(pid, vid="v1", status="pending"):
    from backend.app.models.audit import BatchQueueItem
    return BatchQueueItem(project_id=pid, version_id=vid, action="full",
                          status=status, job_id=f"j_{pid}")


def _queue(items, current_index=0):
    from backend.app.models.audit import BatchQueueStatus
    return BatchQueueStatus(queue_id="q", action="full", status="running",
                            total=len(items), current_index=current_index,
                            items=items)


def _mark_ocr_ready(proj_fs, pid, vid="v1"):
    (proj_fs.dir_for(pid, vid) / f"{pid}_result.json").write_text("{}", encoding="utf-8")


def _mark_crops_ready(proj_fs, pid, vid="v1"):
    (proj_fs.dir_for(pid, vid) / "index.json").write_text("{}", encoding="utf-8")


# ── pre-crop lookahead ──

def test_precrop_candidate_order_b_c_d(proj_fs):
    """pre-crop выбирает pending В ПОРЯДКЕ очереди: B → C → D."""
    import backend.app.pipeline.manager as mgr
    for pid in ("B", "C", "D"):
        _mark_ocr_ready(proj_fs, pid)
    q = _queue([_item("A", status="running"), _item("B"), _item("C"), _item("D")])
    pm = mgr.PipelineManager()

    precropped: set = set()
    order = []
    for _ in range(5):
        cand = pm._select_precrop_candidate(q, precropped)
        if cand is None:
            break
        order.append(cand.project_id)
        precropped.add((cand.project_id, cand.version_id))
    assert order == ["B", "C", "D"], f"ожидался порядок очереди, получено {order}"
    assert pm._select_precrop_candidate(q, precropped) is None


def test_precrop_candidate_bounded_by_window_and_slides(proj_fs, monkeypatch):
    """pre-crop не уезжает дальше окна; окно скользит с current_index."""
    import backend.app.pipeline.manager as mgr
    monkeypatch.setattr(mgr, "BATCH_PRECROP_WINDOW", 2)
    for pid in ("B", "C", "D"):
        _mark_ocr_ready(proj_fs, pid)
    q = _queue([_item("A", status="running"), _item("B"), _item("C"), _item("D")],
               current_index=0)
    pm = mgr.PipelineManager()

    precropped: set = set()
    got = []
    for _ in range(6):
        cand = pm._select_precrop_candidate(q, precropped)
        if cand is None:
            break
        got.append(cand.project_id)
        precropped.add((cand.project_id, cand.version_id))
    # window=2, current=0 → B(dist1), C(dist2) готовятся; D(dist3) ВНЕ окна.
    assert got == ["B", "C"], f"D не должен готовиться при окне 2 / current 0; got {got}"

    # main advance → окно сдвинулось, D вошёл.
    q.current_index = 1
    cand = pm._select_precrop_candidate(q, precropped)
    assert cand is not None and cand.project_id == "D", "после сдвига окна D должен войти"


def test_precrop_candidate_skip_ahead_when_no_ocr(proj_fs):
    """Если у B ещё нет result.json (OCR не готов) — pre-crop временно берёт C,
    а когда B готов — возвращается к B (порядок восстанавливается)."""
    import backend.app.pipeline.manager as mgr
    _mark_ocr_ready(proj_fs, "C")  # только C OCR-готов
    q = _queue([_item("A", status="running"), _item("B"), _item("C")])
    pm = mgr.PipelineManager()

    precropped: set = set()
    cand = pm._select_precrop_candidate(q, precropped)
    assert cand.project_id == "C", "skip-ahead: B без OCR пропущен, взят C"
    precropped.add((cand.project_id, cand.version_id))

    # B всё ещё без OCR → кандидата нет.
    assert pm._select_precrop_candidate(q, precropped) is None

    # B получил result.json → возвращаемся к нему.
    _mark_ocr_ready(proj_fs, "B")
    cand2 = pm._select_precrop_candidate(q, precropped)
    assert cand2.project_id == "B", "после готовности B порядок восстановлен"


def test_precrop_loop_crops_b_c_d_in_order(proj_fs, monkeypatch):
    """End-to-end через сам _run_precrop_loop: проекты кропятся B → C → D."""
    import backend.app.pipeline.manager as mgr
    for pid in ("B", "C", "D"):
        _mark_ocr_ready(proj_fs, pid)
    q = _queue([_item("A", status="running"), _item("B"), _item("C"), _item("D")])
    pm = mgr.PipelineManager()

    captured: list = []

    async def fake_precrop(self, pid, version_id=None):
        captured.append(pid)
        if len(captured) >= 3:
            q.status = "done"  # остановить loop после B,C,D
        return True

    monkeypatch.setattr(type(pm), "_precrop_project", fake_precrop)

    async def _run():
        await asyncio.wait_for(pm._run_precrop_loop(q), timeout=20)

    asyncio.run(_run())
    assert captured == ["B", "C", "D"], f"loop должен кропить в порядке очереди, got {captured}"


# ── pre-Gemma lookahead ──

@pytest.fixture
def pregemma_fs(proj_fs, monkeypatch):
    """proj_fs + патч gemma-contract: outputs всегда невалидны (кандидат «готов,
    но не сделан»), индекс crops = <dir>/index.json."""
    from backend.app.pipeline.stages.gemma_enrichment import (
        gemma_enrichment_contract as gec,
    )
    monkeypatch.setattr(gec, "gemma_outputs_are_valid", lambda pd: (False, "pending"))
    monkeypatch.setattr(gec, "gemma_blocks_index_path", lambda pd: pd / "index.json")
    return proj_fs


def test_pregemma_candidate_prefers_nearest_b(pregemma_fs):
    """pre-Gemma предпочитает БЛИЖАЙШИЙ готовый pending (B перед C)."""
    import backend.app.pipeline.manager as mgr
    for pid in ("B", "C"):
        _mark_crops_ready(pregemma_fs, pid)
    q = _queue([_item("A", status="running"), _item("B"), _item("C")])
    pm = mgr.PipelineManager()

    target, _mut = pm._select_pregemma_candidate(q)
    assert target is not None and target.project_id == "B"


def test_pregemma_candidate_skip_ahead_and_restore(pregemma_fs):
    """B без crops → берём C (skip-ahead); B получил crops → снова B."""
    import backend.app.pipeline.manager as mgr
    _mark_crops_ready(pregemma_fs, "C")  # только у C готовы crops
    q = _queue([_item("A", status="running"), _item("B"), _item("C")])
    pm = mgr.PipelineManager()

    target, _ = pm._select_pregemma_candidate(q)
    assert target.project_id == "C", "skip-ahead на C, пока у B нет crops"

    _mark_crops_ready(pregemma_fs, "B")
    target2, _ = pm._select_pregemma_candidate(q)
    assert target2.project_id == "B", "после готовности crops у B он снова ближайший"


def test_pregemma_candidate_bounded_by_window(pregemma_fs, monkeypatch):
    """pre-Gemma не выходит за BATCH_PREGEMMA_WINDOW."""
    import backend.app.pipeline.manager as mgr
    monkeypatch.setattr(mgr, "BATCH_PREGEMMA_WINDOW", 1)
    for pid in ("B", "C"):
        _mark_crops_ready(pregemma_fs, pid)
    q = _queue([_item("A", status="running"), _item("B"), _item("C")], current_index=0)
    pm = mgr.PipelineManager()

    # window=1 → виден только B (idx1); C (idx2) вне окна.
    target, _ = pm._select_pregemma_candidate(q)
    assert target is not None and target.project_id == "B"

    # B отмечен done → в окне=1 больше никого, C не виден.
    q.items[1].gemma_prefetch_status = "done"
    target2, _ = pm._select_pregemma_candidate(q)
    assert target2 is None, "C вне окна=1, не должен выбираться"

    # current advance на 1 → C (dist1) входит в окно.
    q.current_index = 1
    target3, _ = pm._select_pregemma_candidate(q)
    assert target3 is not None and target3.project_id == "C"
