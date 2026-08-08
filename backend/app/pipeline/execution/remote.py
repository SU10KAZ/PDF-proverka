"""Удалённое исполнение аудита на audit-worker.

Чего этот backend НЕ делает — важнее того, что делает:

  * **не зовёт `_dispatch_action`** ни при каких обстоятельствах (E-02): один
    проект не может выполняться локально и удалённо одновременно;
  * **не читает worker-token**, не ходит по SSH, не запускает подпроцессов на
    центре и не монтирует чужую файловую систему;
  * **не считает отсутствие heartbeat доказательством остановки** (E-08);
  * **не создаёт новую попытку сам** и не переносит задание другому воркеру
    (E-05, I-03): и то и другое — решение оператора;
  * **не удерживает HTTP-запрос** до конца аудита: работа идёт в корутине
    слота очереди, а состояние живёт в workers.db и переживает рестарт.

Ожидание построено на опросе центрального хранилища, а не на удержании
соединения с воркером: соединение рвут прокси и таймауты, а строка в БД —
нет. Частота опроса низкая (секунды), потому что прогресс приходит событиями
и ретранслируется в WebSocket по факту, а не по расписанию.
"""
from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any, Optional

from backend.app.models.websocket import WSMessage
from backend.app.pipeline.execution.contracts import (
    ExecutionBackend,
    ExecutionContext,
    ExecutionError,
    ExecutionHandle,
    ExecutionMode,
    ExecutionRequest,
    ExecutionResult,
    ExecutionSnapshot,
    ExecutionState,
    Liveness,
    LivenessVerdict,
)
from backend.app.ws.manager import ws_manager

#: Пауза между опросами центрального хранилища.
POLL_INTERVAL_SEC = 2.0

#: Отображение состояний подсистемы в состояния платформы.
_STATE_MAP = {
    "created": ExecutionState.PENDING,
    "assigned": ExecutionState.PENDING,
    "source_uploading": ExecutionState.PREPARING,
    "source_ready": ExecutionState.PREPARING,
    "accepted_by_worker": ExecutionState.PREPARING,
    "running": ExecutionState.RUNNING,
    "cancel_requested": ExecutionState.RUNNING,
    "completed_locally": ExecutionState.RETURNING,
    "result_uploading": ExecutionState.RETURNING,
    "result_received": ExecutionState.IMPORTING,
    "validating": ExecutionState.IMPORTING,
    "completed": ExecutionState.COMPLETED,
    "failed": ExecutionState.FAILED,
    "cancelled": ExecutionState.CANCELLED,
    "superseded_result_received": ExecutionState.FAILED,
}

_TERMINAL = {"completed", "failed", "cancelled", "superseded_result_received"}


class RemoteWorkerExecutionBackend(ExecutionBackend):
    backend_type = ExecutionMode.REMOTE_WORKER

    def __init__(self, manager: Any):
        self._manager = manager

    # ─── Служебное ───────────────────────────────────────────────────────────
    @staticmethod
    def _settings():
        from backend.app.services.distributed_workers.settings import get_settings

        settings = get_settings()
        if not settings.enabled:
            raise ExecutionError("Подсистема распределённых воркеров выключена")
        return settings

    @staticmethod
    def _attempt(handle: ExecutionHandle) -> Optional[dict[str, Any]]:
        from backend.app.services.distributed_workers import repositories
        from backend.app.services.distributed_workers.settings import get_settings

        if not handle.attempt_id:
            return None
        return repositories.get_attempt(handle.attempt_id, settings=get_settings())

    # ─── Контракт ────────────────────────────────────────────────────────────
    async def prepare(
        self, request: ExecutionRequest, ctx: ExecutionContext
    ) -> ExecutionHandle:
        """Создать удалённое задание либо ВЕРНУТЬ уже созданное.

        Идемпотентность здесь — не удобство, а инвариант: повторный HTTP-запуск,
        рестарт центра и авто-resume прерванного элемента не должны порождать
        второе исполнение того же проекта (E-04, E-05).
        """
        from backend.app.pipeline.execution import registry as execution_registry
        from backend.app.services.distributed_workers import (
            audit_job_service,
            database,
            repositories,
        )

        settings = self._settings()
        existing = execution_registry.handle_from_item(ctx.item)
        if existing is not None and existing.attempt_id:
            attempt = await database.run_db(
                repositories.get_attempt, existing.attempt_id, settings=settings
            )
            if attempt is not None:
                # Попытка жива — переиспользуем её. Второго задания нет.
                return existing

        if not request.assigned_worker_id:
            raise ExecutionError(
                "Удалённый запуск требует явно выбранного воркера: "
                "автоматического выбора на этом этапе нет"
            )

        _root, version_dir, _output = self._manager._resolve_job_paths(  # noqa: SLF001
            ctx.job
        )
        created = await database.run_db(
            audit_job_service.create_audit_job,
            worker_id=request.assigned_worker_id,
            project_id=request.project_id,
            version_id=request.version_id,
            version_dir=Path(version_dir),
            action=request.options.action,
            include_optimization=request.options.include_optimization,
            retry_stage=request.options.retry_stage,
            actor=f"operator:{ctx.extra.get('actor', 'unknown')}",
            display_name=request.project_id,
            settings=settings,
        )
        handle = ExecutionHandle(
            backend_type=ExecutionMode.REMOTE_WORKER,
            handle_id=created["attempt_id"],
            project_id=request.project_id,
            version_id=request.version_id,
            attempt_id=created["attempt_id"],
            remote_job_id=created["job_id"],
            worker_id=request.assigned_worker_id,
            execution_profile=created.get("execution_profile"),
            created_at=time.time(),
        )
        self._persist(ctx.item, handle)
        return handle

    def _persist(self, item: Any, handle: ExecutionHandle) -> None:
        """Сохранить ссылку в элемент очереди и на диск.

        Без этого рестарт центра не смог бы отличить «этот проект идёт на
        VPS» от «этот проект прерван и его надо запустить заново локально».
        """
        item.execution_handle = handle.model_dump()
        item.execution_mode = ExecutionMode.REMOTE_WORKER.value
        item.worker_id = handle.worker_id
        item.execution_profile = handle.execution_profile
        try:
            self._manager._persist_queue()          # noqa: SLF001
        except Exception:                           # noqa: BLE001 — fail-soft
            pass

    async def start(self, handle: ExecutionHandle, ctx: ExecutionContext) -> None:
        """Стартовать нечего: воркер забирает задание сам, опросом.

        Центр не «ходит» на VPS — порт там наружу не открыт, и это свойство
        архитектуры, а не недоделка.
        """
        await ws_manager.broadcast_to_project(
            handle.project_id,
            WSMessage.log(
                handle.project_id,
                f"Задание передано на audit-worker {handle.worker_id}. "
                "Нормативный этап и финальная сборка останутся на центре.",
                "info",
            ),
        )

    async def status(self, handle: ExecutionHandle) -> ExecutionSnapshot:
        from backend.app.services.distributed_workers import database, job_service

        attempt = await database.run_db(self._attempt, handle)
        if attempt is None:
            return ExecutionSnapshot(
                execution_state=ExecutionState.PENDING,
                liveness=Liveness.UNKNOWN,
                liveness_reason="попытка не найдена в workers.db",
            )
        verdict = await self.liveness(handle)
        progress = job_service._loads(attempt.get("progress_snapshot"), {}) or {}
        return ExecutionSnapshot(
            execution_state=_STATE_MAP.get(
                str(attempt.get("state")), ExecutionState.RUNNING
            ),
            connectivity_state=attempt.get("connectivity_state"),
            stage=progress.get("stage"),
            progress_current=int(progress.get("processed") or 0),
            progress_total=int(progress.get("total") or 0),
            last_event_at=attempt.get("last_event_at"),
            liveness=verdict.state,
            liveness_reason=verdict.reason,
            error=str(attempt.get("error") or "") or None,
        )

    async def liveness(self, handle: ExecutionHandle) -> LivenessVerdict:
        """Живость удалённого исполнения.

        Отсутствие локального процесса на центре не значит НИЧЕГО: его там нет
        по определению. Молчание канала тоже не доказательство — оно меняет
        только ось связи. `DEAD` возвращается ровно в двух случаях: попытка
        достигла терминального состояния своим ходом, либо оператор явно
        распорядился ей вручную.
        """
        from backend.app.services.distributed_workers import database

        attempt = await database.run_db(self._attempt, handle)
        if attempt is None:
            return LivenessVerdict(
                Liveness.UNKNOWN, "попытка не найдена — судьбу решает оператор"
            )
        state = str(attempt.get("state") or "")
        disposition = str(attempt.get("attempt_disposition") or "active")
        if state in _TERMINAL:
            return LivenessVerdict(Liveness.DEAD, f"попытка завершена: {state}")
        if disposition == "operator_declared_lost":
            return LivenessVerdict(
                Liveness.UNKNOWN,
                "оператор признал попытку потерянной: процесс на VPS мог остаться жив",
            )
        connectivity = str(attempt.get("connectivity_state") or "")
        if connectivity == "offline":
            return LivenessVerdict(
                Liveness.UNKNOWN,
                "связь с воркером потеряна — это НЕ основание считать работу "
                "остановленной",
                connectivity=connectivity,
            )
        return LivenessVerdict(
            Liveness.ALIVE, f"удалённое исполнение в состоянии {state}",
            connectivity=connectivity,
        )

    async def cancel(self, handle: ExecutionHandle, *, reason: str = "") -> bool:
        from backend.app.services.distributed_workers import attempt_service, database

        settings = self._settings()
        if not (handle.remote_job_id and handle.attempt_id):
            return False
        try:
            await database.run_db(
                attempt_service.request_cancel,
                job_id=handle.remote_job_id,
                attempt_id=handle.attempt_id,
                reason=reason or "отмена оператором",
                confirmation=attempt_service.CONFIRM_CANCEL,
                actor="operator:pipeline",
                idempotency_key=f"cancel:{handle.attempt_id}",
                settings=settings,
            )
        except attempt_service.OperatorError:
            return False
        return True

    async def reattach(self, handle: ExecutionHandle) -> Optional[ExecutionSnapshot]:
        """Подхватить исполнение после рестарта центра.

        Новое задание НЕ создаётся: если попытка на месте, работа продолжается
        там, где шла. Если попытки нет — возвращаем None, и вызывающий решает
        сам (обычно это означает, что задание было отменено или удалено).
        """
        from backend.app.services.distributed_workers import database

        attempt = await database.run_db(self._attempt, handle)
        if attempt is None:
            return None
        return await self.status(handle)

    async def wait(
        self, handle: ExecutionHandle, ctx: ExecutionContext
    ) -> ExecutionResult:
        """Дождаться терминального состояния, транслируя прогресс в UI."""
        from backend.app.services.distributed_workers import database, event_service

        settings = self._settings()
        last_seq = 0
        last_state = ""
        while True:
            attempt = await database.run_db(self._attempt, handle)
            if attempt is None:
                return ExecutionResult(
                    success=False,
                    error="Удалённая попытка исчезла из workers.db",
                )
            state = str(attempt.get("state") or "")
            if state != last_state:
                last_state = state
                await self._relay_state(handle, attempt)
            last_seq = await self._relay_events(handle, last_seq, settings)
            if state in _TERMINAL:
                break
            if ctx.job is not None and getattr(ctx.job, "status", None) is not None:
                # Оператор нажал «Остановить» — job уже помечен CANCELLED
                # менеджером. Работу это не обрывает: воркеру уходит команда,
                # а мы продолжаем ждать её подтверждения.
                pass
            await asyncio.sleep(POLL_INTERVAL_SEC)

        return await self.collect_result(handle)

    async def _relay_state(self, handle: ExecutionHandle, attempt: dict[str, Any]) -> None:
        from backend.app.services.distributed_workers import job_service

        await ws_manager.broadcast_to_project(
            handle.project_id,
            WSMessage.log(
                handle.project_id,
                f"Удалённый аудит: {job_service.display_status(attempt)}",
                "info",
            ),
        )

    async def _relay_events(
        self, handle: ExecutionHandle, last_seq: int, settings: Any
    ) -> int:
        """Переложить новые события воркера в существующие WS-сообщения.

        Свой формат не заводится: фронтенд не должен отличать локальный аудит
        от удалённого. Выдуманного процента здесь нет — показывается ровно то,
        что прислал воркер.
        """
        from backend.app.services.distributed_workers import database, repositories

        if not (handle.remote_job_id and handle.attempt_id):
            return last_seq
        events = await database.run_db(
            repositories.list_events,
            handle.remote_job_id,
            attempt_id=handle.attempt_id,
            after_seq=last_seq,
            limit=200,
            settings=settings,
        )
        for event in events:
            last_seq = max(last_seq, int(event.get("sequence") or 0))
            await self._relay_one(handle, event)
        return last_seq

    async def _relay_one(self, handle: ExecutionHandle, event: dict[str, Any]) -> None:
        import json as _json

        etype = str(event.get("event_type") or "")
        try:
            payload = _json.loads(event.get("payload") or "{}")
        except (TypeError, ValueError):
            payload = {}
        pid = handle.project_id
        if etype == "stage_progress":
            processed = int(payload.get("processed") or 0)
            total = int(payload.get("total") or 0)
            if total:
                await ws_manager.broadcast_to_project(
                    pid, WSMessage.progress(pid, processed, total)
                )
            return
        if etype in ("stage_started", "stage_completed", "job_started"):
            stage = payload.get("stage") or ""
            status = payload.get("status") or ("начат" if "started" in etype else "")
            await ws_manager.broadcast_to_project(
                pid, WSMessage.log(pid, f"[воркер] {stage} {status}".strip(), "info")
            )
            return
        if etype == "log_line":
            await ws_manager.broadcast_to_project(
                pid,
                WSMessage.log(
                    pid, f"[воркер] {payload.get('message', '')}",
                    str(payload.get("level") or "info"),
                ),
            )
            return
        if etype in ("job_failed", "quota_warning", "resource_warning"):
            await ws_manager.broadcast_to_project(
                pid,
                WSMessage.log(
                    pid, f"[воркер] {etype}: {payload.get('message', '')}", "error"
                ),
            )

    async def collect_result(self, handle: ExecutionHandle) -> ExecutionResult:
        """Забрать итог. Применение артефактов делает импортёр, и один раз."""
        from backend.app.services.distributed_workers import database, result_import

        settings = self._settings()
        attempt = await database.run_db(self._attempt, handle)
        if attempt is None:
            return ExecutionResult(success=False, error="попытка исчезла")
        state = str(attempt.get("state") or "")
        if state == "cancelled":
            return ExecutionResult(cancelled=True, success=False)
        if state != "completed":
            return ExecutionResult(
                success=False,
                error=f"удалённое исполнение завершилось состоянием {state}",
            )
        report = await database.run_db(
            result_import.import_result_for_attempt,
            attempt=attempt,
            settings=settings,
        )
        return ExecutionResult(
            success=bool(report.get("applied")),
            package_id=attempt.get("package_id"),
            package_hash=attempt.get("result_package_hash"),
            returned_artifacts=list(report.get("applied_paths") or []),
            resume_stage=report.get("resume_stage"),
            usage_report=report.get("usage_report"),
            error=report.get("error"),
        )
