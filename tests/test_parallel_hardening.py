"""Три вещи, которые ломались бы именно при параллельных проектах.

Каждая безобидна, пока очередь ведёт один проект, и каждая кусается на пяти.

  1. Выгрузка норм-моделей. Кэш e5-large + bge-reranker (~4,3 ГБ) глобальный
     на процесс. Проект, закончивший норм-этап, обнулял его — и остальные
     получали повторную загрузку и десятки секунд простоя на КАЖДОМ чужом
     завершении. Выгружать имеет право только последний уходящий.
  2. Ожидание rate limit. Метод вызывался на каждый job: пять проектов уходили
     в пять независимых ожиданий и после сброса синхронно били в API, снова
     упираясь в лимит. Нужен общий дедлайн и разбежка пробуждений.
  3. Пул потоков под asyncio.to_thread. По умолчанию ~20 на 16 ядрах, и он
     общий с длинными норм-задачами. Выедается — event loop залипает,
     health-проверка молчит, вотчдог убивает живой аудит.

Run: python -m pytest tests/test_parallel_hardening.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.models.audit import AuditJob, AuditStage, JobStatus  # noqa: E402


# ─── 1. Выгрузка норм-моделей ────────────────────────────────────────────────


def _job(pid: str, stage: AuditStage, status=JobStatus.RUNNING) -> AuditJob:
    return AuditJob(
        job_id=f"j-{pid}", project_id=pid, stage=stage,
        status=status, started_at="2026-08-06T00:00:00",
    )


def test_norm_models_released_only_by_last_project(monkeypatch):
    from backend.app.pipeline.stages.norms import runner as norms_runner
    from backend.app.pipeline import manager as mgr_mod

    active = {}
    monkeypatch.setattr(mgr_mod.pipeline_manager, "active_jobs", active)

    # Один проект на норм-этапе — он же и уходит: выгружать можно.
    active.clear()
    active["A"] = _job("A", AuditStage.NORM_VERIFY)
    assert norms_runner.other_projects_on_norm_stage("A") == 0

    # Рядом ещё два проекта на норм-этапе — выгружать НЕЛЬЗЯ.
    active["B"] = _job("B", AuditStage.NORM_VERIFY)
    active["C"] = _job("C", AuditStage.NORM_FIX)
    assert norms_runner.other_projects_on_norm_stage("A") == 2

    # Проекты на других этапах моделями не пользуются — не считаем.
    active.clear()
    active["A"] = _job("A", AuditStage.NORM_VERIFY)
    active["D"] = _job("D", AuditStage.BLOCK_ANALYSIS)
    active["__BATCH__"] = _job("__BATCH__", AuditStage.PREPARE)
    assert norms_runner.other_projects_on_norm_stage("A") == 0

    # Завершённый сосед тоже не в счёт.
    active["E"] = _job("E", AuditStage.NORM_VERIFY, status=JobStatus.COMPLETED)
    assert norms_runner.other_projects_on_norm_stage("A") == 0


def test_norm_stage_count_is_derived_not_counted():
    """Счётчик «на входе +1» протёк бы на первом же early-return.

    У run_norm_verification много ранних return'ов; ручной счётчик после
    любого из них навсегда остался бы > 0, и модели перестали бы выгружаться
    вообще — это профиль OOM-инцидента 01.07. Поэтому считаем по живому
    состоянию очереди, а модуль не должен держать своей изменяемой переменной.
    """
    from backend.app.pipeline.stages.norms import runner as norms_runner

    assert not hasattr(norms_runner, "_ACTIVE_NORM_STAGES"), (
        "вернулся ручной счётчик — он протекает на early-return"
    )


# ─── 2. Согласованное ожидание rate limit ────────────────────────────────────


def test_rate_limit_state_starts_clean():
    from backend.app.pipeline.manager import PipelineManager

    mgr = PipelineManager()
    assert mgr._rate_limit_deadline == 0.0
    assert mgr._rate_limit_waiters == 0


@pytest.mark.asyncio
async def test_rate_limit_waiters_share_deadline_and_stagger(monkeypatch):
    """Второй ждущий наследует дедлайн первого и просыпается позже него."""
    import backend.app.pipeline.manager as mgr_mod
    from backend.app.pipeline.manager import PipelineManager

    mgr = PipelineManager()

    async def _anoop(*a, **k):
        return None

    mgr._log = _anoop
    monkeypatch.setattr(mgr_mod.ws_manager, "broadcast_to_project", _anoop)
    monkeypatch.setattr(mgr_mod.claude_runner, "parse_rate_limit_reset", lambda _o: 600)
    monkeypatch.setattr(
        mgr_mod.global_scanner, "check_rate_limit",
        lambda _pct: {"can_proceed": False, "wait_seconds": 600,
                      "resets_in_text": "10 мин", "usage_pct": 95, "reason": "лимит"},
    )
    monkeypatch.setattr(mgr_mod, "RATE_LIMIT_STAGGER_SEC", 30)

    class _Done(Exception):
        pass

    async def _fake_sleep(_sec):
        # Обрываем ожидание сразу — реально спать в тесте незачем.
        raise _Done()

    monkeypatch.setattr(mgr_mod.asyncio, "sleep", _fake_sleep)

    logged: list[str] = []

    async def _capture_log(_job, msg, level="info"):
        logged.append(str(msg))

    mgr._log = _capture_log

    # Сосед уже ждёт и знает сброс через час; наш CLI сообщает всего 10 минут.
    import time as _time

    far_deadline = _time.monotonic() + 3600
    mgr._rate_limit_deadline = far_deadline
    mgr._rate_limit_waiters = 1

    with pytest.raises(_Done):
        await mgr._wait_for_rate_limit(
            _job("B", AuditStage.NORM_VERIFY), reason="лимит", cli_output="reset"
        )

    # Дедлайн НЕ опущен до своих 10 минут: просыпаться раньше соседа
    # бессмысленно — сброса ещё не будет.
    assert mgr._rate_limit_deadline >= far_deadline, "общий дедлайн опущен под свой"
    # Второй ждущий получает разбежку, чтобы не стартовать одновременно с первым.
    assert any("разбежка" in m for m in logged), f"нет разбежки в логах: {logged}"
    # Сосед всё ещё ждёт → дедлайн не снимается.
    assert mgr._rate_limit_waiters == 1
    assert mgr._rate_limit_deadline != 0.0


@pytest.mark.asyncio
async def test_rate_limit_deadline_cleared_by_last_waiter(monkeypatch):
    """Протухший общий дедлайн не должен наследоваться следующим лимитом."""
    import backend.app.pipeline.manager as mgr_mod
    from backend.app.pipeline.manager import PipelineManager

    mgr = PipelineManager()

    async def _anoop(*a, **k):
        return None

    mgr._log = _anoop
    monkeypatch.setattr(mgr_mod.ws_manager, "broadcast_to_project", _anoop)
    monkeypatch.setattr(mgr_mod.claude_runner, "parse_rate_limit_reset", lambda _o: 1)
    monkeypatch.setattr(
        mgr_mod.global_scanner, "check_rate_limit",
        lambda _pct: {"can_proceed": True, "wait_seconds": 1,
                      "resets_in_text": "1 мин", "usage_pct": 10, "reason": ""},
    )

    async def _instant_sleep(_sec):
        return None

    monkeypatch.setattr(mgr_mod.asyncio, "sleep", _instant_sleep)

    ok = await mgr._wait_for_rate_limit(_job("A", AuditStage.NORM_VERIFY),
                                        reason="лимит", cli_output="reset")
    assert ok is True
    assert mgr._rate_limit_waiters == 0
    assert mgr._rate_limit_deadline == 0.0, "последний ждавший не снял дедлайн"


# ─── 3. Пул потоков ──────────────────────────────────────────────────────────


def test_thread_pool_is_larger_than_python_default(monkeypatch):
    """Дефолт Python (~min(32, ядра+4)) выедается пятью проектами."""
    import os as _os

    from backend.app.main import default_thread_pool_size

    monkeypatch.delenv("THREAD_POOL_WORKERS", raising=False)
    size = default_thread_pool_size()
    python_default = min(32, (_os.cpu_count() or 4) + 4)
    assert size >= 32
    assert size >= python_default, "пул не больше дефолтного — смысла в правке нет"

    monkeypatch.setenv("THREAD_POOL_WORKERS", "48")
    assert default_thread_pool_size() == 48

    # Мусор не должен ронять старт бэкенда.
    monkeypatch.setenv("THREAD_POOL_WORKERS", "не-число")
    assert default_thread_pool_size() >= 32
