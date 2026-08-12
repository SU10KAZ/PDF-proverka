"""Локальное исполнение — тонкая обёртка над существующим поведением.

Единственное, что делает `run()`, — вызывает тот же `_dispatch_action` с теми
же аргументами, что и раньше. Ни последовательность этапов, ни статусы, ни
отмена, ни resume, ни batch queue, ни WebSocket-события, ни состав выходных
файлов здесь не переизобретаются: их переизобретение и было бы регрессией.

Остальные семь операций контракта существуют не ради симметрии, а ради того,
чтобы менеджер задавал ОДИН и тот же вопрос обоим backend'ам. Каждая из них
отвечает по уже существующим сигналам платформы: живые дочерние процессы,
живой asyncio-таск, in-memory job. Новых источников истины локальный backend не
заводит.
"""
from __future__ import annotations

import time
from typing import Any, Optional

from backend.app.pipeline.execution.contracts import (
    ExecutionBackend,
    ExecutionContext,
    ExecutionHandle,
    ExecutionMode,
    ExecutionRequest,
    ExecutionResult,
    ExecutionSnapshot,
    ExecutionState,
    Liveness,
    LivenessVerdict,
)


class LocalExecutionBackend(ExecutionBackend):
    """Исполнение в процессе центра. Поведение — прежнее, байт в байт."""

    backend_type = ExecutionMode.LOCAL

    def __init__(self, manager: Any):
        self._manager = manager

    # ─── Контракт ────────────────────────────────────────────────────────────
    async def prepare(
        self, request: ExecutionRequest, ctx: ExecutionContext
    ) -> ExecutionHandle:
        """Побочных эффектов нет: локальному исполнению готовить нечего."""
        return ExecutionHandle(
            backend_type=ExecutionMode.LOCAL,
            handle_id=request.job_id,
            project_id=request.project_id,
            version_id=request.version_id,
            created_at=time.time(),
        )

    async def start(self, handle: ExecutionHandle, ctx: ExecutionContext) -> None:
        """Локально старт и есть исполнение — оно происходит в `run()`."""
        return None

    async def run(
        self, request: ExecutionRequest, ctx: ExecutionContext
    ) -> ExecutionResult:
        """ЕДИНСТВЕННАЯ содержательная строка backend'а.

        Аргументы и их порядок совпадают с прежним вызовом из
        `_batch_slot_worker` дословно. Всё, что было до и после вызова
        (регистрация job, ContextVar, cleanup, статусы item'а), осталось у
        менеджера и сюда не переехало: перенос кода ради красоты интерфейса и
        есть тот способ, которым ломают работающие конвейеры.
        """
        await self._manager._dispatch_action(     # noqa: SLF001 — тот же вызов
            ctx.item,
            ctx.job,
            default_action=ctx.default_action,
            action_override=ctx.action_override,
        )
        job = ctx.job
        status = getattr(job.status, "value", str(job.status))
        return ExecutionResult(
            success=status == "completed",
            cancelled=status == "cancelled",
            error=job.error_message,
        )

    async def wait(
        self, handle: ExecutionHandle, ctx: ExecutionContext
    ) -> ExecutionResult:
        return await self.run(
            ExecutionRequest(
                project_id=handle.project_id,
                version_id=handle.version_id,
                job_id=handle.handle_id,
            ),
            ctx,
        )

    async def status(self, handle: ExecutionHandle) -> ExecutionSnapshot:
        job = self._manager.active_jobs.get(handle.project_id)
        if job is None:
            return ExecutionSnapshot(
                execution_state=ExecutionState.PENDING,
                liveness=Liveness.UNKNOWN,
                liveness_reason="job не зарегистрирован в active_jobs",
            )
        verdict = await self.liveness(handle)
        status = getattr(job.status, "value", str(job.status))
        mapping = {
            "queued": ExecutionState.PENDING,
            "running": ExecutionState.RUNNING,
            "completed": ExecutionState.COMPLETED,
            "failed": ExecutionState.FAILED,
            "cancelled": ExecutionState.CANCELLED,
        }
        return ExecutionSnapshot(
            execution_state=mapping.get(status, ExecutionState.RUNNING),
            stage=getattr(job.stage, "value", None),
            progress_current=int(job.progress_current or 0),
            progress_total=int(job.progress_total or 0),
            liveness=verdict.state,
            liveness_reason=verdict.reason,
            error=job.error_message,
        )

    async def cancel(self, handle: ExecutionHandle, *, reason: str = "") -> bool:
        """Прежняя отмена: убить дочерние процессы и снять задачу."""
        return await self._manager.cancel(handle.project_id)

    async def liveness(self, handle: ExecutionHandle) -> LivenessVerdict:
        """Живость по СУЩЕСТВУЮЩИМ локальным сигналам.

        Порядок тот же, что у `cleanup_zombies`: живые дочерние процессы →
        живой asyncio-таск → heartbeat. Новых правил здесь нет намеренно.
        """
        from backend.app.services.common.process_runner import has_live_processes

        pid = handle.project_id
        if has_live_processes(pid):
            return LivenessVerdict(Liveness.ALIVE, "есть живые дочерние процессы")
        task = self._manager._tasks.get(pid)          # noqa: SLF001
        if task is not None and not task.done():
            return LivenessVerdict(Liveness.ALIVE, "asyncio-таск жив")
        job = self._manager.active_jobs.get(pid)
        if job is None:
            return LivenessVerdict(Liveness.DEAD, "job снят с учёта")
        return LivenessVerdict(
            Liveness.UNKNOWN,
            "нет живых процессов и таска — решает heartbeat-таймаут менеджера",
        )

    async def reattach(self, handle: ExecutionHandle) -> Optional[ExecutionSnapshot]:
        """Подхватывать нечего: локальное исполнение не переживает рестарт.

        Прежнее поведение: `load_persisted_queue` переводит `running` в
        `interrupted`, а авто-resume продолжает с места обрыва. Backend в это
        не вмешивается.
        """
        return None

    async def collect_result(self, handle: ExecutionHandle) -> ExecutionResult:
        job = self._manager.active_jobs.get(handle.project_id)
        if job is None:
            return ExecutionResult(success=False, error="job не найден")
        status = getattr(job.status, "value", str(job.status))
        return ExecutionResult(
            success=status == "completed",
            cancelled=status == "cancelled",
            error=job.error_message,
        )
