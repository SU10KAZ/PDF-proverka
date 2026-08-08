"""Состояние ЦЕНТРАЛЬНОГО хвоста удалённой попытки.

**Почему второй оси не избежать.** `JobState` описывает исполнение НА ВОРКЕРЕ:
он честно доходит до `completed` в момент, когда центр принял архив, и дальше
не двигается. Всё, что происходит потом — проверка пакета, нормализация путей,
staging-импорт, `detect_resume_stage`, нормативный этап, Excel — на этой оси
неразличимо: попытка «completed» и в середине импорта, и после падения между
импортом и резюмом, и после успешного завершения аудита.

Практическое следствие было ровно одно и оно нехорошее: рестарт центра между
приёмом архива и центральным хвостом не оставлял НИ ОДНОГО признака, по
которому можно было бы понять, что делать дальше — повторить импорт (и
получить второе применение) или продолжить с резюма (и потерять артефакты,
если импорт не дошёл).

Ось намеренно узкая: только центральный хвост, только вперёд, каждый переход
идемпотентен. Существующие поля не дублируются — `result_import_state` остаётся
источником истины о ФАКТЕ применения, а эта ось отвечает на вопрос «где мы
сейчас».
"""
from __future__ import annotations

import json
import time
from enum import Enum
from typing import Any, Optional

from backend.app.services.distributed_workers import repositories
from backend.app.services.distributed_workers.settings import DistributedWorkersSettings


class HandoffState(str, Enum):
    """Этапы центрального хвоста. Порядок значений = порядок движения."""

    #: Работа идёт на воркере; центральный хвост ещё не начинался.
    WORKER_RUNNING = "worker_running"
    #: Воркер объявил работу законченной локально, архив ещё не у нас.
    WORKER_COMPLETED_LOCALLY = "worker_completed_locally"
    #: Идёт загрузка чанков.
    RESULT_UPLOADING = "result_uploading"
    #: Архив собран центром целиком.
    RESULT_RECEIVED = "result_received"
    #: Идёт проверка манифеста, хэшей, дисциплины и границы этапов.
    RESULT_VALIDATING = "result_validating"
    #: Проверки пройдены. Импорт ещё НЕ начинался.
    RESULT_VALIDATED = "result_validated"
    #: Идёт staging-импорт: план, нормализация путей, атомарные замены.
    RESULT_IMPORTING = "result_importing"
    #: Импорт применён и зафиксирован.
    RESULT_IMPORTED = "result_imported"
    #: Импорт применён, центральный этап определён, но ещё не запущен.
    CENTRAL_RESUME_PENDING = "central_resume_pending"
    #: Центральные этапы идут.
    CENTRAL_RESUME_RUNNING = "central_resume_running"
    #: Хвост пройден, финальные артефакты созданы.
    COMPLETED = "completed"
    #: Хвост оборван. Значение хранит причину в `central_handoff_detail`.
    FAILED = "failed"


#: Порядок для сравнения «дальше/раньше». `FAILED` вне порядка намеренно: это
#: не «дальше», это в сторону.
_ORDER: tuple[HandoffState, ...] = (
    HandoffState.WORKER_RUNNING,
    HandoffState.WORKER_COMPLETED_LOCALLY,
    HandoffState.RESULT_UPLOADING,
    HandoffState.RESULT_RECEIVED,
    HandoffState.RESULT_VALIDATING,
    HandoffState.RESULT_VALIDATED,
    HandoffState.RESULT_IMPORTING,
    HandoffState.RESULT_IMPORTED,
    HandoffState.CENTRAL_RESUME_PENDING,
    HandoffState.CENTRAL_RESUME_RUNNING,
    HandoffState.COMPLETED,
)

#: Состояния, после которых повторять шаг НЕЛЬЗЯ.
TERMINAL: frozenset[HandoffState] = frozenset({HandoffState.COMPLETED, HandoffState.FAILED})

#: Отображение состояния воркера (`JobState`) в стартовое состояние хвоста.
#: Нужно для попыток, созданных до появления оси: у них колонка пуста, и
#: «неизвестно» лучше выводить из того, что уже записано, чем считать нулём.
_FROM_JOB_STATE: dict[str, HandoffState] = {
    "created": HandoffState.WORKER_RUNNING,
    "assigned": HandoffState.WORKER_RUNNING,
    "source_uploading": HandoffState.WORKER_RUNNING,
    "source_ready": HandoffState.WORKER_RUNNING,
    "accepted_by_worker": HandoffState.WORKER_RUNNING,
    "running": HandoffState.WORKER_RUNNING,
    "cancel_requested": HandoffState.WORKER_RUNNING,
    "completed_locally": HandoffState.WORKER_COMPLETED_LOCALLY,
    "result_uploading": HandoffState.RESULT_UPLOADING,
    "result_received": HandoffState.RESULT_RECEIVED,
    "validating": HandoffState.RESULT_VALIDATING,
    "completed": HandoffState.RESULT_VALIDATED,
}


class HandoffError(RuntimeError):
    """Недопустимый переход центрального хвоста."""


def index_of(state: HandoffState) -> int:
    try:
        return _ORDER.index(state)
    except ValueError:
        return -1


def current(attempt: dict[str, Any]) -> HandoffState:
    """Текущее состояние хвоста. Пустая колонка выводится из состояния попытки."""
    raw = str(attempt.get("central_handoff_state") or "").strip()
    if raw:
        try:
            return HandoffState(raw)
        except ValueError:
            # Неизвестное значение в колонке — не повод падать при чтении:
            # оператор должен увидеть попытку, а не 500.
            return HandoffState.WORKER_RUNNING
    return _FROM_JOB_STATE.get(
        str(attempt.get("state") or ""), HandoffState.WORKER_RUNNING
    )


def is_at_least(attempt: dict[str, Any], state: HandoffState) -> bool:
    """Прошли ли мы это состояние. `FAILED` не «больше» ничего."""
    now = current(attempt)
    if now is HandoffState.FAILED:
        return False
    return index_of(now) >= index_of(state)


def advance(
    attempt_id: str,
    state: HandoffState,
    *,
    settings: DistributedWorkersSettings,
    detail: Optional[dict[str, Any]] = None,
    resume_stage: Optional[str] = None,
    allow_regress: bool = False,
) -> dict[str, Any]:
    """Продвинуть хвост. Назад — только явным `allow_regress`.

    Идемпотентность здесь не удобство: повторный приём того же результата и
    авто-resume после рестарта проходят по тем же ветвям, и «записал второй
    раз» на них означало бы второй импорт либо второй центральный хвост.
    """
    row = repositories.get_attempt(attempt_id, settings=settings)
    if row is None:
        raise HandoffError(f"Попытка {attempt_id} не найдена")
    now = current(row)
    if not allow_regress and state is not HandoffState.FAILED:
        if index_of(state) < index_of(now):
            # Движение назад молча — это способ повторить уже сделанный шаг.
            return {"state": now.value, "changed": False, "reason": "already_ahead"}
        if state is now:
            return {"state": now.value, "changed": False, "reason": "already_there"}
    fields: dict[str, Any] = {
        "central_handoff_state": state.value,
        "central_handoff_at": time.time(),
    }
    if detail is not None:
        fields["central_handoff_detail"] = json.dumps(detail, ensure_ascii=False)
    if resume_stage is not None:
        fields["central_resume_stage"] = str(resume_stage)
    if state is HandoffState.COMPLETED:
        fields["central_completed_at"] = time.time()
    repositories.update_attempt_fields(attempt_id, fields, settings=settings)
    return {"state": state.value, "changed": True, "previous": now.value}


def detail_of(attempt: dict[str, Any]) -> dict[str, Any]:
    raw = attempt.get("central_handoff_detail")
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def describe(attempt: dict[str, Any]) -> dict[str, Any]:
    """Человекочитаемая сводка для оператора и API.

    Оператор обязан видеть ФАКТИЧЕСКИЙ этап, а не общий `running`: между
    «архив принят» и «центральные этапы идут» проходят десятки минут, и разница
    между ними — разница между «ждать» и «разбираться».
    """
    state = current(attempt)
    return {
        "central_handoff_state": state.value,
        "central_handoff_label": HANDOFF_LABELS.get(state, state.value),
        "central_handoff_at": attempt.get("central_handoff_at"),
        "central_resume_stage": attempt.get("central_resume_stage"),
        "central_completed_at": attempt.get("central_completed_at"),
        "result_import_state": attempt.get("result_import_state"),
        "detail": detail_of(attempt),
    }


HANDOFF_LABELS: dict[HandoffState, str] = {
    HandoffState.WORKER_RUNNING: "Идёт на воркере",
    HandoffState.WORKER_COMPLETED_LOCALLY: "Воркер закончил, архив ещё не передан",
    HandoffState.RESULT_UPLOADING: "Загрузка результата",
    HandoffState.RESULT_RECEIVED: "Результат принят центром",
    HandoffState.RESULT_VALIDATING: "Проверка результата",
    HandoffState.RESULT_VALIDATED: "Результат проверен",
    HandoffState.RESULT_IMPORTING: "Применение результата",
    HandoffState.RESULT_IMPORTED: "Результат применён",
    HandoffState.CENTRAL_RESUME_PENDING: "Ожидание центральных этапов",
    HandoffState.CENTRAL_RESUME_RUNNING: "Идут центральные этапы",
    HandoffState.COMPLETED: "Аудит завершён на центре",
    HandoffState.FAILED: "Центральный хвост оборван",
}
