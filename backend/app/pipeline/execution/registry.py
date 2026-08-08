"""Выбор backend'а исполнения и флаги, которые его включают.

Правило по умолчанию одно и оно fail-closed: **если удалённое исполнение не
включено явно и элемент очереди не помечен как удалённый — работает локальный
backend, ровно как раньше**. Включение подсистемы воркеров
(`DISTRIBUTED_WORKERS_ENABLED`) удалённый аудит НЕ включает: для него есть
отдельный флаг, и это намеренно (§29 задания).
"""
from __future__ import annotations

import os
from typing import Any, Optional

from backend.app.pipeline.execution.contracts import (
    AuditExecutionOptions,
    ExecutionBackend,
    ExecutionHandle,
    ExecutionMode,
    ExecutionRequest,
)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def distributed_audit_execution_enabled() -> bool:
    """Разрешено ли вообще удалённое исполнение аудита.

    Читается на каждый вызов, как и остальные флаги подсистемы: правка `.env` +
    рестарт backend меняет поведение, кэш инвалидировать не нужно.
    """
    from backend.app.core import config

    return _env_bool(
        "DISTRIBUTED_AUDIT_EXECUTION_ENABLED",
        getattr(config, "DISTRIBUTED_AUDIT_EXECUTION_ENABLED", False),
    )


def remote_execution_available() -> tuple[bool, str]:
    """Можно ли предлагать оператору удалённый запуск. (можно, причина отказа)."""
    from backend.app.services.distributed_workers.settings import get_settings

    if not distributed_audit_execution_enabled():
        return False, "Удалённое исполнение аудита выключено (DISTRIBUTED_AUDIT_EXECUTION_ENABLED)"
    if not get_settings().enabled:
        return False, "Подсистема распределённых воркеров выключена (DISTRIBUTED_WORKERS_ENABLED)"
    return True, ""


def item_execution_mode(item: Any) -> ExecutionMode:
    """Режим исполнения элемента очереди. Источник истины — сам элемент.

    Старая очередь (`batch_queue.json` без новых полей) читается как локальная:
    поля добавлены с дефолтами, и элемент, записанный прошлой версией, не
    может внезапно оказаться удалённым.
    """
    raw = getattr(item, "execution_mode", None) or ExecutionMode.LOCAL.value
    try:
        mode = ExecutionMode(str(raw))
    except ValueError:
        return ExecutionMode.LOCAL
    if mode is ExecutionMode.REMOTE_WORKER and not getattr(item, "worker_id", None):
        # Удалённый режим без воркера бессмыслен. Молча исполнить локально
        # безопаснее, чем упасть: элемент попал сюда из персистентной очереди.
        return ExecutionMode.LOCAL
    return mode


def select_backend(manager: Any, item: Any) -> ExecutionBackend:
    """Backend для конкретного элемента очереди.

    Выбор происходит ДО фактического вызова `_dispatch_action` — это и есть
    точка врезки. Локальный путь остаётся делегатом в прежнюю реализацию.
    """
    from backend.app.pipeline.execution.local import LocalExecutionBackend

    if item_execution_mode(item) is ExecutionMode.REMOTE_WORKER:
        allowed, reason = remote_execution_available()
        if not allowed:
            from backend.app.pipeline.execution.contracts import ExecutionError

            raise ExecutionError(
                f"Элемент очереди помечен удалённым, но {reason}. "
                "Локально он НЕ запускается: это создало бы второе исполнение "
                "того же проекта."
            )
        from backend.app.pipeline.execution.remote import RemoteWorkerExecutionBackend

        return RemoteWorkerExecutionBackend(manager)
    return LocalExecutionBackend(manager)


def build_request(item: Any, job: Any, *, default_action: str,
                  action_override: Optional[str]) -> ExecutionRequest:
    """Собрать типизированный запрос из элемента очереди и job.

    В запрос попадают ТОЛЬКО описательные поля. `extra_params` переносится
    выборочно: неизвестное поле сюда не проходит (`extra="forbid"`), и это
    защита от «оператор дописал в JSON произвольный ключ, а он доехал до
    воркера».
    """
    extra = getattr(item, "extra_params", None) or {}
    options = AuditExecutionOptions(
        action=action_override or getattr(item, "action", None) or default_action or "full",
        retry_stage=getattr(item, "retry_stage", None),
        start_from=extra.get("start_from") if isinstance(extra, dict) else None,
    )
    return ExecutionRequest(
        project_id=job.project_id,
        version_id=job.version_id,
        object_id=job.object_id,
        job_id=job.job_id,
        options=options,
        execution_mode=item_execution_mode(item),
        assigned_worker_id=getattr(item, "worker_id", None),
        execution_profile=getattr(item, "execution_profile", None),
        correlation_id=job.job_id,
    )


def handle_from_item(item: Any) -> Optional[ExecutionHandle]:
    """Восстановить ссылку на исполнение из персистентного элемента очереди."""
    raw = getattr(item, "execution_handle", None)
    if not isinstance(raw, dict) or not raw:
        return None
    try:
        return ExecutionHandle(**raw)
    except Exception:                     # noqa: BLE001 — битое поле не должно ронять очередь
        return None
