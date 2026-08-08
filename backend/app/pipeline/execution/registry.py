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


CENTRAL_STAGES_DISABLED_ENV = "AUDIT_PIPELINE_CENTRAL_STAGES_DISABLED"


def central_stages_disabled() -> bool:
    """Запрещены ли в ЭТОМ процессе этапы, которые выполняются только на центре.

    Единица запрета — процесс, а не элемент очереди, и это точно соответствует
    реальности: процесс `remote_audit_runner` целиком является удалённой ногой
    одного аудита. На центре переменная не выставлена никогда, поэтому обычный
    локальный конвейер этой проверки не замечает.

    Без такого гейта `_dispatch_action(action="full")` на воркере выполнял бы
    `norm_verify`, `debt_control`, `decision_carryover` и Excel — то есть ровно
    те четыре этапа, которые профиль `remote_audit_pilot_v1` объявляет
    центральными. Проверка `FORBIDDEN_STAGES` в `remote_audit_runner` их не
    ловит: она сверяет только явный `retry_stage`.
    """
    return _env_bool(CENTRAL_STAGES_DISABLED_ENV, False)


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
        mode = ExecutionMode.LOCAL
    if mode is ExecutionMode.REMOTE_WORKER and not getattr(item, "worker_id", None):
        # Удалённый режим без воркера бессмыслен. Молча исполнить локально
        # безопаснее, чем упасть: элемент попал сюда из персистентной очереди.
        return ExecutionMode.LOCAL
    if mode is ExecutionMode.LOCAL and handle_from_item(item) is not None:
        # У элемента есть ссылка на УЖЕ созданное удалённое исполнение, но
        # режим/worker_id потерялись (ручная правка JSON, частичная запись,
        # будущая правка кода). Локальный прогон здесь означал бы второе
        # исполнение того же проекта поверх живой удалённой попытки — с
        # `_clean_stage_files` в начале. Возвращаем удалённый режим: пусть
        # `select_backend` честно откажет, если флаг выключен.
        return ExecutionMode.REMOTE_WORKER
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


def note_central_handoff(
    handle: Any,
    state: str,
    *,
    detail: Optional[dict] = None,
    resume_stage: Optional[str] = None,
) -> None:
    """Отметить этап ЦЕНТРАЛЬНОГО хвоста удалённой попытки.

    Живёт здесь, а не в `PipelineManager`, ровно по той же причине, по которой
    здесь живёт выбор backend'а: менеджер не знает о подсистеме воркеров, и это
    машинно проверяемая граница. Локальному исполнению отмечать нечего —
    вызов для него просто ничего не делает.

    Fail-soft: ось хвоста — диагностика и точка восстановления после рестарта,
    а не условие завершения аудита. Её отказ не должен превращать выполненный
    центральный хвост в проваленный.
    """
    attempt_id = getattr(handle, "attempt_id", None)
    if not attempt_id:
        return
    try:
        from backend.app.services.distributed_workers import central_handoff
        from backend.app.services.distributed_workers.settings import get_settings

        central_handoff.advance(
            str(attempt_id),
            central_handoff.HandoffState(state),
            settings=get_settings(),
            detail=detail,
            resume_stage=resume_stage,
            allow_regress=(state == central_handoff.HandoffState.FAILED.value),
        )
    except Exception:                      # noqa: BLE001 — ось не блокер
        pass


def central_handoff_state(handle: Any) -> Optional[str]:
    """Где сейчас центральный хвост этой попытки (или None).

    Нужно ровно для одного решения: рестарт центра НЕ должен прогонять
    нормативный этап и Excel второй раз по уже завершённому аудиту. Импорт
    идемпотентен сам по себе, а центральные этапы — нет: они стоят денег и
    перезаписывают финальные артефакты.
    """
    attempt_id = getattr(handle, "attempt_id", None)
    if not attempt_id:
        return None
    try:
        from backend.app.services.distributed_workers import central_handoff, repositories
        from backend.app.services.distributed_workers.settings import get_settings

        settings = get_settings()
        row = repositories.get_attempt(str(attempt_id), settings=settings)
        if row is None:
            return None
        return central_handoff.current(row).value
    except Exception:                      # noqa: BLE001 — диагностика не блокер
        return None


def handle_from_item(item: Any) -> Optional[ExecutionHandle]:
    """Восстановить ссылку на исполнение из персистентного элемента очереди."""
    raw = getattr(item, "execution_handle", None)
    if not isinstance(raw, dict) or not raw:
        return None
    try:
        return ExecutionHandle(**raw)
    except Exception:                     # noqa: BLE001 — битое поле не должно ронять очередь
        return None
