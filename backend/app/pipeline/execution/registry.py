"""Выбор backend'а исполнения и флаги, которые его включают.

Правило по умолчанию одно и оно fail-closed: **если удалённое исполнение не
включено явно и элемент очереди не помечен как удалённый — работает локальный
backend, ровно как раньше**. Включение подсистемы воркеров
(`DISTRIBUTED_WORKERS_ENABLED`) удалённый аудит НЕ включает: для него есть
отдельный флаг, и это намеренно (§29 задания).
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)

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


class FrozenRoutingPlanError(RuntimeError):
    """Новый distributed job потерял или повредил обязательный frozen plan."""


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


#: Точка детерминированной остановки центрального хвоста. ТОЛЬКО для стенда:
#: по умолчанию переменная не задана и функция ничего не делает.
#:
#: Доказать «рестарт центра между приёмом результата и импортом не создаёт
#: второе задание» иначе нельзя: окно между этими шагами — доли секунды, и
#: попадание в него по таймеру не воспроизводится. Единственная альтернатива —
#: ждать «повезёт», а это не доказательство.
HANDOFF_TEST_PAUSE_ENV = "AUDIT_HANDOFF_TEST_PAUSE_AT"
HANDOFF_TEST_PAUSE_DIR_ENV = "AUDIT_HANDOFF_TEST_PAUSE_DIR"


def handoff_test_pause(stage: str, *, detail: Optional[dict] = None) -> None:
    """Остановиться в названной точке хвоста и ждать снаружи.

    Пишет маркер (по нему стенд узнаёт, что точка достигнута) и блокируется.
    Процесс снимается извне — ровно так, как его снял бы вотчдог или оператор.
    Исключение здесь не годится: оно пометило бы элемент очереди провалившимся,
    то есть проверялся бы не рестарт, а обработка ошибки.
    """
    import time as _time

    wanted = (os.environ.get(HANDOFF_TEST_PAUSE_ENV) or "").strip()
    if not wanted or wanted != stage:
        return
    marker_dir = (os.environ.get(HANDOFF_TEST_PAUSE_DIR_ENV) or "").strip()
    if marker_dir:
        import json as _json
        from pathlib import Path as _Path

        target = _Path(marker_dir)
        try:
            target.mkdir(parents=True, exist_ok=True)
            (target / f"paused_{stage}.json").write_text(
                _json.dumps(
                    {"stage": stage, "pid": os.getpid(), "detail": detail or {}},
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
        except OSError:
            pass
    while True:                                    # снимается извне
        _time.sleep(0.2)


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
    except Exception as exc:               # noqa: BLE001 — ось не блокер
        # Fail-soft, но не молча. Непрошедшая запись `completed` означает, что
        # после рестарта гейт не увидит завершённого хвоста и прогонит
        # нормативный этап и Excel второй раз — а это деньги и перезапись
        # финальных артефактов. Единственный след такого исхода — эта строка.
        logger.warning(
            "Ось центрального хвоста не записана (попытка %s, состояние %s): %s",
            attempt_id, state, exc,
        )


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


def frozen_routing_plan(handle: Any) -> Any:
    """ЗАМОРОЖЕННЫЙ план этого задания — тот же, что уехал на воркер.

    Источник — нагрузка логического задания в `workers.db`, а не повторная
    компиляция и не текущая глобальная конфигурация центра. Это принципиально:
    смысл 11J в том, что маршрут задания зафиксирован в момент его создания, и
    центральный хвост обязан доигрывать ТОТ ЖЕ маршрут, даже если оператор
    успел переключить пресет (KI-11I-3, §18 задания).

    Повторная компиляция дала бы «план, который получился бы, если бы задание
    создавали сейчас» — то есть ровно ту подмену, от которой уходим. Разбор
    сверяет `routing_plan_hash` с содержимым (`RoutingPlan.from_dict`), так что
    подменённая в БД запись до исполнения не доедет.

    `None` — законный ответ ТОЛЬКО для явно распознанного legacy contract v0.
    Новый job несёт ``routing_plan_contract_version=1``; отсутствие либо
    повреждение его плана является INVALID и останавливает хвост. Иначе потеря
    одного JSON-поля незаметно переключала уже идущее задание на live config.
    """
    job_id = getattr(handle, "remote_job_id", None)
    if not job_id:
        # ExecutionHandle до контракта маршрутизации не нёс remote_job_id.
        # Это наблюдаемый legacy marker, а не общий fail-open: новые handles
        # создаются только с id и дальше проверяются по contract marker job.
        logger.info(
            "FROZEN_ROUTING_PLAN NOT_FOUND: legacy_handle_v0 без remote_job_id; "
            "разрешён fallback к live config"
        )
        return None
    try:
        from backend.app.services.audit_routing.plan import RoutingPlan
        from backend.app.services.distributed_workers import repositories
        from backend.app.services.distributed_workers.settings import get_settings

        row = repositories.get_logical_job(str(job_id), settings=get_settings())
        if row is None:
            logger.error(
                "FROZEN_ROUTING_PLAN INVALID: задания %s нет в workers.db", job_id,
            )
            raise FrozenRoutingPlanError(
                f"FROZEN_ROUTING_PLAN INVALID: job {job_id} отсутствует"
            )
        payload = row.get("payload")
        if isinstance(payload, str):
            payload = json.loads(payload or "{}")
        if not isinstance(payload, dict):
            raise FrozenRoutingPlanError("payload задания не является объектом")
        params = payload.get("params") or {}
        if not isinstance(params, dict):
            raise FrozenRoutingPlanError("payload.params не является объектом")
        contract = params.get("routing_plan_contract_version")
        raw = params.get("routing_plan")
        if not isinstance(raw, dict) or not raw:
            if contract == 1:
                logger.error(
                    "FROZEN_ROUTING_PLAN NOT_FOUND: job=%s contract=v1; "
                    "fail closed", job_id,
                )
                raise FrozenRoutingPlanError(
                    "FROZEN_ROUTING_PLAN NOT_FOUND: обязательный plan отсутствует"
                )
            # Единственный разрешённый fallback: в нагрузке нет ни плана, ни
            # маркера обязательности, то есть это contract v0 до 11J. Маркер
            # пишется дословно, чтобы live-config fallback был трассируемым.
            logger.info(
                "FROZEN_ROUTING_PLAN NOT_FOUND: job=%s legacy_contract_v0; "
                "разрешён fallback к live config", job_id,
            )
            return None
        if contract not in (None, 1):
            raise FrozenRoutingPlanError(
                f"неподдерживаемый routing contract {contract!r}"
            )
        plan = RoutingPlan.from_dict(raw)
        logger.info(
            "FROZEN_ROUTING_PLAN FOUND: job=%s plan=%s hash=%s",
            job_id, plan.routing_plan_id, plan.plan_hash(),
        )
        return plan
    except FrozenRoutingPlanError as exc:
        if "FROZEN_ROUTING_PLAN" not in str(exc):
            logger.error(
                "FROZEN_ROUTING_PLAN INVALID: job=%s: %s", job_id, exc,
            )
            raise FrozenRoutingPlanError(
                f"FROZEN_ROUTING_PLAN INVALID: {exc}"
            ) from exc
        raise
    except Exception as exc:                # noqa: BLE001 — fail closed ниже
        logger.error(
            "FROZEN_ROUTING_PLAN INVALID: job=%s: %s", job_id, exc,
        )
        raise FrozenRoutingPlanError(
            f"FROZEN_ROUTING_PLAN INVALID: {type(exc).__name__}: {exc}"
        ) from exc


def handle_from_item(item: Any) -> Optional[ExecutionHandle]:
    """Восстановить ссылку на исполнение из персистентного элемента очереди."""
    raw = getattr(item, "execution_handle", None)
    if not isinstance(raw, dict) or not raw:
        return None
    try:
        return ExecutionHandle(**raw)
    except Exception:                     # noqa: BLE001 — битое поле не должно ронять очередь
        return None
