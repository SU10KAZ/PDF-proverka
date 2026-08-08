"""Операторское управление попытками: отмена, признание потерянной, новая попытка.

Три действия, которых на этапе 0 не было вовсе, и из-за которых застрявшее
задание чинилось только правкой БД. Все три опасны по-разному, поэтому
устроены по-разному:

  * ОТМЕНА — просьба, а не факт. Центр ставит persistent-команду и переводит
    попытку в `cancel_requested`. `cancelled` появляется ТОЛЬКО после
    подтверждения воркера (§5.1, критерий готовности 6). Офлайн-VPS не
    превращает отмену в фиктивную: команда ждёт восстановления связи.

  * ПРИЗНАНИЕ ПОТЕРЯННОЙ — заявление оператора о том, что он больше не считает
    попытку текущей. Оно НЕ утверждает, что удалённый процесс остановлен
    (I-06), поэтому `execution_state` не подменяется выдуманным `failed`:
    меняется только ось disposition. Токен старой попытки НЕ отзывается — по
    нему вернувшийся воркер попадёт в контур своей попытки и сдаст результат
    в отдельное хранилище, а не в актуальную попытку (I-07, I-08).

  * НОВАЯ ПОПЫТКА — единственный способ «повторить». Поверх работающей попытки
    создать её нельзя: физически мешает частичный уникальный индекс
    ux_attempts_one_active (I-04, I-05).

Идемпотентность у всех трёх — через журнал операторских действий: повтор с тем
же Idempotency-Key возвращает записанный результат и не выполняет действие
второй раз.
"""
from __future__ import annotations

import json
import time
from typing import Any, Optional

from backend.app.models.distributed_workers import (
    TERMINAL_JOB_STATES,
    JobState,
    WorkerCommandType,
)
from backend.app.services.distributed_workers import (
    auth,
    job_service,
    repositories,
)
from backend.app.services.distributed_workers.settings import DistributedWorkersSettings

# Подтверждающие фразы. Их вводит оператор руками — это не галочка, которую
# можно проскочить мышкой, а осознанное действие с последствиями.
CONFIRM_CANCEL = "ОТМЕНИТЬ"
CONFIRM_MARK_LOST = "ПОПЫТКА ПОТЕРЯНА"
CONFIRM_NEW_ATTEMPT = "НОВАЯ ПОПЫТКА"
CONFIRM_DELETE_DATA = "УДАЛИТЬ ДАННЫЕ"

DEFAULT_CANCEL_GRACE_SEC = 30

# Расположения, поверх которых разрешено заводить новую попытку (§5.3).
RESUMABLE_DISPOSITIONS = frozenset(
    {"completed", "cancelled", "operator_declared_lost", "superseded"}
)


# Сколько последних записей журнала просматривается при поиске повтора.
_IDEMPOTENCY_WINDOW = 500


class OperatorError(RuntimeError):
    """Нарушение правила операторского действия (422/409, не 500)."""


class ConfirmationRequired(OperatorError):
    """Подтверждающая фраза не совпала."""


def _require(condition: bool, message: str, cls=OperatorError) -> None:
    if not condition:
        raise cls(message)


def load_attempt(
    *, job_id: str, attempt_id: str, settings: DistributedWorkersSettings
) -> dict[str, Any]:
    attempt = repositories.get_attempt(attempt_id, settings=settings)
    if attempt is None or attempt["job_id"] != job_id:
        raise OperatorError("Попытка не найдена у этого задания")
    return attempt


def _prior_action(
    *, action_type: str, idempotency_key: Optional[str],
    settings: DistributedWorkersSettings,
    job_id: Optional[str] = None,
    attempt_id: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    """Ранее выполненное действие с тем же ключом (защита от двойного клика).

    Ключ сверяется ВМЕСТЕ с адресом действия. Раньше искали только по паре
    (тип, ключ): повтор того же ключа на ДРУГОЙ попытке возвращал результат
    первой, ничего не выполняя, — и ответ выглядел успешным.

    Окно поиска ограничено: за его пределами повтор перестаёт распознаваться и
    действие выполнится заново. Это осознанно — все три действия защищены ещё
    и проверками состояния, а бесконечный скан журнала на каждый клик дороже.
    """
    if not idempotency_key:
        return None
    for item in repositories.list_admin_actions(
        limit=_IDEMPOTENCY_WINDOW, settings=settings
    ):
        if item.get("action_type") != action_type:
            continue
        if item.get("idempotency_key") != idempotency_key:
            continue
        if attempt_id is not None and item.get("attempt_id") != attempt_id:
            continue
        if job_id is not None and item.get("job_id") != job_id:
            continue
        return item
    return None


# ─── Отмена ──────────────────────────────────────────────────────────────────
def request_cancel(
    *,
    job_id: str,
    attempt_id: str,
    reason: str,
    confirmation: str,
    grace_period_sec: int = DEFAULT_CANCEL_GRACE_SEC,
    actor: str,
    idempotency_key: Optional[str] = None,
    audit: Optional[dict[str, Any]] = None,
    settings: DistributedWorkersSettings,
) -> dict[str, Any]:
    """Запросить отмену попытки. Возвращает описание запроса и команды."""
    _require(bool(reason.strip()), "Причина отмены обязательна")
    _require(
        confirmation.strip() == CONFIRM_CANCEL,
        f"Подтверждение не совпало: ожидается «{CONFIRM_CANCEL}»",
        ConfirmationRequired,
    )
    prior = _prior_action(
        action_type="cancel_attempt", idempotency_key=idempotency_key, settings=settings,
        job_id=job_id, attempt_id=attempt_id,
    )
    if prior:
        return {**(prior.get("result") or {}), "replayed": True}

    attempt = load_attempt(job_id=job_id, attempt_id=attempt_id, settings=settings)
    state = JobState(attempt["state"])

    if state in TERMINAL_JOB_STATES:
        # Задача успела закончиться. Результат не уничтожается и попытка не
        # становится cancelled задним числом (§15.1).
        result = {
            "outcome": "already_final",
            "state": state.value,
            "message": "Попытка уже завершена — отмена не применяется, результат сохранён",
        }
        _log_action(
            action_type="cancel_attempt", actor=actor, attempt=attempt, reason=reason,
            idempotency_key=idempotency_key, audit=audit, result=result,
            requested={"grace_period_sec": grace_period_sec}, settings=settings,
        )
        return result

    # Ключ команды: пока прежняя не подтверждена — переиспользуем её, чтобы
    # не плодить дубли. Если воркер уже ответил чем-то неразрешающим
    # (ownership_mismatch, ambiguous_not_running), оператору нужна ВОЗМОЖНОСТЬ
    # попросить ещё раз — иначе попытка навсегда застревает в cancel_requested.
    prior_cancels = [
        c for c in repositories.commands_for_job(job_id, settings=settings)
        if c.get("attempt_id") == attempt_id
        and c.get("command_type") == WorkerCommandType.CANCEL_ATTEMPT.value
    ]
    unfinished = [c for c in prior_cancels if c.get("acknowledged_at") is None]
    command_key = (
        unfinished[0]["idempotency_key"]
        if unfinished
        else f"cancel:{attempt_id}:{len(prior_cancels) + 1}"
    )
    if state is not JobState.CANCEL_REQUESTED:
        job_service.transition(
            attempt_id=attempt_id,
            to_state=JobState.CANCEL_REQUESTED,
            actor=actor,
            reason=f"отмена оператором: {reason}",
            fields={"cancel_requested_at": time.time(), "cancel_reason": reason},
            settings=settings,
        )
    worker_id = attempt.get("assigned_worker_id")
    command: dict[str, Any] = {}
    if worker_id:
        command = repositories.enqueue_command(
            worker_id=worker_id,
            command_type=WorkerCommandType.CANCEL_ATTEMPT.value,
            payload={
                "job_id": job_id,
                "attempt_id": attempt_id,
                "grace_period_sec": int(grace_period_sec),
                "reason": reason[:500],
            },
            idempotency_key=command_key,
            job_id=job_id,
            attempt_id=attempt_id,
            settings=settings,
        )
    result = {
        "outcome": "cancel_requested",
        "state": JobState.CANCEL_REQUESTED.value,
        "command_id": command.get("command_id"),
        "command_status": command.get("status", "pending"),
        "message": (
            "Отмена запрошена. Если VPS сейчас офлайн, команда будет доставлена "
            "после восстановления связи — мгновенная остановка не гарантируется."
        ),
    }
    _log_action(
        action_type="cancel_attempt", actor=actor, attempt=attempt, reason=reason,
        idempotency_key=idempotency_key, audit=audit, result=result,
        requested={"grace_period_sec": grace_period_sec}, settings=settings,
    )
    return result


def apply_cancel_ack(
    *,
    command: dict[str, Any],
    result: dict[str, Any],
    settings: DistributedWorkersSettings,
) -> Optional[str]:
    """Применить подтверждение отмены от воркера. Возвращает новое состояние.

    Правило простое и намеренно узкое: `cancelled` ставится ТОЛЬКО когда воркер
    доказал, что исполнять больше нечего. «Не смог опознать процесс» и «не
    понял, работает ли» такими доказательствами не являются — попытка остаётся
    в cancel_requested и видна оператору (I-06, I-17).
    """
    attempt_id = command.get("attempt_id")
    if not attempt_id:
        return None
    detail = result.get("detail") if isinstance(result.get("detail"), dict) else {}
    outcome = str(detail.get("outcome") or result.get("outcome") or "")
    attempt = repositories.get_attempt(attempt_id, settings=settings)
    if attempt is None:
        return None
    if attempt["state"] != JobState.CANCEL_REQUESTED.value:
        return attempt["state"]
    if outcome in ("cancelled", "not_running_locally", "already_cancelled"):
        updated = job_service.transition(
            attempt_id=attempt_id,
            to_state=JobState.CANCELLED,
            actor="worker",
            reason=f"подтверждение воркера: {outcome}",
            fields={"cancelled_at": time.time()},
            settings=settings,
        )
        return updated["state"]
    # already_completed / ownership_mismatch / ambiguous_not_running: состояние
    # не меняем. Оператор увидит фактический ответ команды на экране.
    return attempt["state"]


# ─── Признание попытки потерянной ────────────────────────────────────────────
def mark_lost(
    *,
    job_id: str,
    attempt_id: str,
    reason: str,
    typed_confirmation: str,
    observed_worker_state: str = "",
    operator_note: str = "",
    actor: str,
    idempotency_key: Optional[str] = None,
    audit: Optional[dict[str, Any]] = None,
    settings: DistributedWorkersSettings,
) -> dict[str, Any]:
    """Признать попытку потерянной. НЕ утверждает, что процесс остановлен."""
    _require(bool(reason.strip()), "Причина обязательна")
    _require(
        typed_confirmation.strip() == CONFIRM_MARK_LOST,
        f"Подтверждение не совпало: введите «{CONFIRM_MARK_LOST}»",
        ConfirmationRequired,
    )
    prior = _prior_action(
        action_type="mark_attempt_lost", idempotency_key=idempotency_key, settings=settings,
        job_id=job_id, attempt_id=attempt_id,
    )
    if prior:
        return {**(prior.get("result") or {}), "replayed": True}

    attempt = load_attempt(job_id=job_id, attempt_id=attempt_id, settings=settings)
    if attempt.get("attempt_disposition") == "operator_declared_lost":
        return {
            "outcome": "already_declared_lost",
            "attempt_disposition": "operator_declared_lost",
            "execution_state": attempt["state"],
            "replayed": True,
        }
    _require(
        attempt.get("attempt_disposition") == "active",
        "Попытка уже не активна — признавать её потерянной нечего",
    )

    now = time.time()
    # execution_state НЕ трогаем: центр не знает, остановился ли процесс, и
    # выдумывать `failed` было бы ложью (I-06). Меняется только ось disposition.
    repositories.update_attempt_fields(
        attempt_id,
        {
            "attempt_disposition": "operator_declared_lost",
            "declared_lost_at": now,
            "lost_reason": reason[:1000],
            "operator_note": operator_note[:1000] or None,
            "observed_worker_state": observed_worker_state[:200] or None,
        },
        settings=settings,
    )
    repositories.update_logical_job(
        job_id, {"overall_state": "needs_operator"}, settings=settings
    )
    result = {
        "outcome": "operator_declared_lost",
        "attempt_disposition": "operator_declared_lost",
        # Показываем ФАКТИЧЕСКОЕ состояние исполнения: оно осталось прежним.
        "execution_state": attempt["state"],
        "message": (
            "Удалённый процесс может продолжать работу. После создания новой "
            "попытки результаты старой будут считаться устаревшими."
        ),
    }
    _log_action(
        action_type="mark_attempt_lost", actor=actor, attempt=attempt, reason=reason,
        idempotency_key=idempotency_key, audit=audit, result=result,
        requested={
            "observed_worker_state": observed_worker_state,
            "operator_note": operator_note,
        },
        settings=settings,
    )
    return result


# ─── Новая попытка ───────────────────────────────────────────────────────────
def create_attempt(
    *,
    job_id: str,
    worker_id: str,
    reason: str,
    source_attempt_id: Optional[str],
    confirmation: str,
    actor: str,
    idempotency_key: Optional[str] = None,
    audit: Optional[dict[str, Any]] = None,
    settings: DistributedWorkersSettings,
) -> dict[str, Any]:
    """Создать НОВУЮ попытку задания. Старая сохраняется целиком."""
    _require(bool(reason.strip()), "Причина обязательна")
    _require(
        confirmation.strip() == CONFIRM_NEW_ATTEMPT,
        f"Подтверждение не совпало: ожидается «{CONFIRM_NEW_ATTEMPT}»",
        ConfirmationRequired,
    )
    prior = _prior_action(
        action_type="create_attempt", idempotency_key=idempotency_key, settings=settings,
        job_id=job_id, attempt_id=source_attempt_id,
    )
    if prior:
        return {**(prior.get("result") or {}), "replayed": True}

    logical = repositories.get_logical_job(job_id, settings=settings)
    _require(logical is not None, "Задание не найдено")
    assert logical is not None

    previous = repositories.get_attempt(
        source_attempt_id or logical.get("current_attempt_id") or "", settings=settings
    )
    _require(previous is not None, "Исходная попытка не найдена")
    assert previous is not None
    _require(
        previous.get("attempt_disposition") in RESUMABLE_DISPOSITIONS,
        "Новую попытку нельзя создать поверх работающей: сначала отмените её "
        "либо признайте потерянной",
    )

    worker = repositories.get_worker(worker_id, settings=settings)
    _require(worker is not None, "Воркер не найден")

    attempt = repositories.create_next_attempt(
        job_id=job_id, worker_id=worker_id, settings=settings
    )
    params = job_service.job_params(logical)
    manifest = job_service.build_source_package(
        job=attempt,
        params=params,
        compression=job_service.package_service.pick_compression(
            job_service.worker_capabilities(worker or {}).get("compressions")
        ),
        settings=settings,
    )
    token = auth.generate_execution_token()
    job_service.transition(
        attempt_id=attempt["attempt_id"],
        to_state=JobState.ASSIGNED,
        actor=actor,
        reason=f"новая попытка: {reason}",
        fields={
            "assigned_worker_id": worker_id,
            "assigned_at": time.time(),
            "execution_token_sha256": auth.hash_token(token),
            "package_id": manifest["package_id"],
            "source_package_hash": manifest["archive"]["sha256"],
        },
        settings=settings,
    )
    # Прежняя попытка помечается вытесненной. Её результат, события и журнал
    # остаются на месте: «вытеснена» — это не «удалена».
    repositories.update_attempt_fields(
        previous["attempt_id"],
        {
            "superseded_by_attempt": attempt["attempt_id"],
            "superseded_at": time.time(),
        },
        settings=settings,
    )
    result = {
        "outcome": "attempt_created",
        "attempt_id": attempt["attempt_id"],
        "attempt_number": attempt["attempt_no"],
        "assignment_generation": attempt["assignment_generation"],
        "worker_id": worker_id,
        "superseded_attempt_id": previous["attempt_id"],
    }
    _log_action(
        action_type="create_attempt", actor=actor, attempt=previous, reason=reason,
        idempotency_key=idempotency_key, audit=audit, result=result,
        requested={"worker_id": worker_id, "source_attempt_id": previous["attempt_id"]},
        settings=settings,
    )
    return result


# ─── Запрос удаления данных попытки с воркера ────────────────────────────────
def request_data_deletion(
    *,
    job_id: str,
    attempt_id: str,
    reason: str,
    confirmation: str,
    actor: str,
    idempotency_key: Optional[str] = None,
    audit: Optional[dict[str, Any]] = None,
    settings: DistributedWorkersSettings,
) -> dict[str, Any]:
    """Попросить воркер удалить локальные данные попытки.

    Неподтверждённый результат этой командой удалить нельзя (I-12): проверка
    стоит и здесь, и на самом воркере — каждый рубеж держит оборону сам.
    """
    _require(bool(reason.strip()), "Причина обязательна")
    _require(
        confirmation.strip() == CONFIRM_DELETE_DATA,
        f"Подтверждение не совпало: ожидается «{CONFIRM_DELETE_DATA}»",
        ConfirmationRequired,
    )
    prior = _prior_action(
        action_type="request_worker_data_deletion",
        idempotency_key=idempotency_key, settings=settings,
        job_id=job_id, attempt_id=attempt_id,
    )
    if prior:
        return {**(prior.get("result") or {}), "replayed": True}

    attempt = load_attempt(job_id=job_id, attempt_id=attempt_id, settings=settings)
    _require(
        attempt.get("attempt_disposition") != "active",
        "Активную попытку удалять нельзя",
    )
    _require(
        not job_service.retention_unconfirmed(attempt),
        "Результат не подтверждён центром — удалять его нельзя даже вручную",
    )
    worker_id = attempt.get("assigned_worker_id")
    _require(bool(worker_id), "Попытка не закреплена за воркером")

    # Ключ со счётчиком — как у отмены. С фиксированным ключом повторный заказ
    # удаления возвращал СТАРУЮ строку (в том числе протухшую или уже
    # отвеченную), но рапортовал «команда поставлена в очередь»: воркер при
    # этом не получал ничего.
    prior_deletes = [
        c for c in repositories.commands_for_job(job_id, settings=settings)
        if c.get("attempt_id") == attempt_id
        and c.get("command_type") == WorkerCommandType.DELETE_ATTEMPT_DATA.value
    ]
    unfinished = [c for c in prior_deletes if c.get("acknowledged_at") is None]
    command_key = (
        unfinished[0]["idempotency_key"]
        if unfinished
        else f"delete:{attempt_id}:{len(prior_deletes) + 1}"
    )
    command = repositories.enqueue_command(
        worker_id=str(worker_id),
        command_type=WorkerCommandType.DELETE_ATTEMPT_DATA.value,
        payload={"job_id": job_id, "attempt_id": attempt_id, "reason": reason[:500]},
        idempotency_key=command_key,
        job_id=job_id,
        attempt_id=attempt_id,
        settings=settings,
    )
    result = {
        "outcome": "deletion_requested",
        "command_id": command.get("command_id"),
        "command_status": command.get("status", "pending"),
        "message": "Если VPS офлайн, команда останется в очереди до восстановления связи.",
    }
    _log_action(
        action_type="request_worker_data_deletion", actor=actor, attempt=attempt,
        reason=reason, idempotency_key=idempotency_key, audit=audit, result=result,
        requested={}, settings=settings,
    )
    return result


def apply_deletion_ack(
    *,
    command: dict[str, Any],
    result: dict[str, Any],
    settings: DistributedWorkersSettings,
) -> None:
    """Отразить факт удаления локальной копии. Центральная копия НЕ трогается (I-14)."""
    attempt_id = command.get("attempt_id")
    if not attempt_id:
        return
    detail = result.get("detail") if isinstance(result.get("detail"), dict) else {}
    outcome = str(detail.get("outcome") or "")
    if outcome not in ("deleted", "already_deleted"):
        return
    current = repositories.get_attempt(attempt_id, settings=settings)
    if current and current.get("retention_state") == "deleted_from_worker":
        return                      # повтор ACK не должен двигать отметку времени
    repositories.update_attempt_fields(
        attempt_id,
        {
            "retention_state": "deleted_from_worker",
            "deleted_from_worker_at": time.time(),
        },
        settings=settings,
    )


# ─── Журнал ──────────────────────────────────────────────────────────────────
def _log_action(
    *,
    action_type: str,
    actor: str,
    attempt: dict[str, Any],
    reason: str,
    idempotency_key: Optional[str],
    audit: Optional[dict[str, Any]],
    result: dict[str, Any],
    requested: dict[str, Any],
    settings: DistributedWorkersSettings,
) -> None:
    meta = audit or {}
    repositories.record_admin_action(
        actor_id=actor,
        actor_display_name=str(meta.get("actor_display_name") or actor.split(":", 1)[-1]),
        action_type=action_type,
        worker_id=attempt.get("assigned_worker_id"),
        job_id=attempt.get("job_id"),
        attempt_id=attempt.get("attempt_id"),
        previous_state={
            "execution_state": attempt.get("state"),
            "attempt_disposition": attempt.get("attempt_disposition"),
        },
        requested_state=requested,
        reason=reason[:1000],
        idempotency_key=idempotency_key,
        request_id=meta.get("request_id"),
        source_ip=meta.get("source_ip"),
        user_agent=meta.get("user_agent"),
        result_status=str(result.get("outcome") or "ok"),
        result=result,
        settings=settings,
    )


# ─── История попыток ─────────────────────────────────────────────────────────
def attempts_view(
    *, job_id: str, settings: DistributedWorkersSettings
) -> list[dict[str, Any]]:
    """История попыток задания для операторского экрана."""
    logical = repositories.get_logical_job(job_id, settings=settings)
    current = (logical or {}).get("current_attempt_id")
    actions = repositories.list_admin_actions(job_id=job_id, limit=200, settings=settings)
    commands = repositories.commands_for_job(job_id, settings=settings)
    out: list[dict[str, Any]] = []
    for attempt in repositories.list_attempts(job_id, settings=settings):
        view = job_service.to_view(attempt, settings=settings)
        view["is_current"] = attempt["attempt_id"] == current
        view["attempt_number"] = attempt["attempt_no"]
        view["attempt_disposition"] = attempt.get("attempt_disposition")
        view["disposition_label"] = DISPOSITION_LABELS.get(
            attempt.get("attempt_disposition") or "", attempt.get("attempt_disposition")
        )
        view["operator_actions"] = [
            {
                "action_type": a["action_type"],
                "actor": a["actor_id"],
                "reason": a["reason"],
                "at": a["created_at"],
                "result_status": a["result_status"],
            }
            for a in actions
            if a.get("attempt_id") == attempt["attempt_id"]
        ]
        view["commands"] = [
            {
                "command_id": c["command_id"],
                "command_type": c["command_type"],
                "status": c.get("status"),
                "created_at": c.get("created_at"),
                "delivered_at": c.get("delivered_at"),
                "acknowledged_at": c.get("acknowledged_at"),
                "result": json.loads(c["result"]) if c.get("result") else None,
            }
            for c in commands
            if c.get("attempt_id") == attempt["attempt_id"]
        ]
        view["can_cancel"] = (
            attempt.get("attempt_disposition") == "active"
            and JobState(attempt["state"]) not in TERMINAL_JOB_STATES
            and attempt["state"] != JobState.CANCEL_REQUESTED.value
        )
        view["can_mark_lost"] = attempt.get("attempt_disposition") == "active"
        out.append(view)
    return out


DISPOSITION_LABELS = {
    "active": "текущая",
    "completed": "завершена",
    "cancelled": "отменена",
    "operator_declared_lost": "признана потерянной",
    "superseded": "вытеснена новой попыткой",
}
