"""Типизированный контракт исполнения аудита.

Что через этот интерфейс НЕ передаётся и передано быть не может (§6 задания,
инварианты E-11 и E-12):

  * shell-команда, argv, имя исполняемого файла, путь к нему;
  * переменные окружения;
  * произвольный callable из запроса;
  * абсолютный путь, контролируемый пользователем.

Запрос описывает РАБОТУ («какой проект, какая версия, какое действие»), а не
СПОСОБ её выполнения. Способ выбирает исполнитель: локальный backend зовёт
существующий `_dispatch_action`, удалённый — фиксированный тип задания
`audit_pipeline_v1`, реализацию которого воркер выбирает сам по имени.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class ExecutionError(RuntimeError):
    """Отказ бизнес-правила исполнения (не 500)."""


class ExecutionMode(str, Enum):
    """Где выполняется работа. Значение хранится в состоянии элемента очереди."""

    LOCAL = "local"
    REMOTE_WORKER = "remote_worker"


class ExecutionState(str, Enum):
    """Высокоуровневое состояние исполнения — общее для обоих backend'ов.

    Это НЕ копия `JobState` подсистемы воркеров: там 15 значений про протокол
    передачи пакетов, здесь — то, что видит платформа и показывает оператору.
    """

    PENDING = "pending"
    PREPARING = "preparing"
    RUNNING = "running"
    RETURNING = "returning"       # работа закончена, результат едет к центру
    IMPORTING = "importing"       # центр применяет возвращённые артефакты
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Liveness(str, Enum):
    """Три ответа, и «не знаю» — полноценный из них.

    Ключевое отличие от локальной модели зомби: отсутствие СИГНАЛА никогда не
    означает `DEAD`. Для удалённой работы это прямо инвариант E-08.
    """

    ALIVE = "alive"
    UNKNOWN = "unknown"
    DEAD = "dead"


@dataclass(frozen=True)
class LivenessVerdict:
    """Вердикт о живости + причина. Причина обязательна: «мертво» без
    объяснения — это то, из-за чего удалялись артефакты живого аудита."""

    state: Liveness
    reason: str
    last_signal_at: Optional[float] = None
    connectivity: Optional[str] = None

    @property
    def may_be_reclaimed(self) -> bool:
        """Разрешено ли снимать регистрацию и возвращать элемент в очередь."""
        return self.state is Liveness.DEAD


class AuditExecutionOptions(BaseModel):
    """Закрытый набор параметров аудита. `extra="forbid"`.

    Здесь нет и не может быть путей, команд и имён моделей: набор моделей
    фиксируется снимком конфигурации, а не полем запроса.
    """

    model_config = ConfigDict(extra="forbid")

    action: str = Field(default="full", max_length=64)
    retry_stage: Optional[str] = Field(default=None, max_length=64)
    include_optimization: bool = True
    # Нормативный этап на первом пилоте всегда остаётся на центре (E-19).
    include_norms: bool = False
    start_from: Optional[int] = Field(default=None, ge=1, le=10_000)


class ExecutionRequest(BaseModel):
    """Что нужно сделать. Одинаково для обоих backend'ов."""

    model_config = ConfigDict(extra="forbid")

    project_id: str = Field(min_length=1, max_length=300)
    version_id: Optional[str] = Field(default=None, max_length=64)
    object_id: Optional[str] = Field(default=None, max_length=128)
    job_id: str = Field(min_length=1, max_length=64)
    options: AuditExecutionOptions = Field(default_factory=AuditExecutionOptions)
    execution_mode: ExecutionMode = ExecutionMode.LOCAL
    assigned_worker_id: Optional[str] = Field(default=None, max_length=64)
    execution_profile: Optional[str] = Field(default=None, max_length=64)
    pipeline_revision: Optional[str] = Field(default=None, max_length=200)
    correlation_id: str = Field(default="", max_length=64)


class ExecutionHandle(BaseModel):
    """Ссылка на конкретное исполнение. Переживает рестарт центра.

    Сериализуется в элемент очереди — именно по ней после рестарта менеджер
    находит удалённую попытку вместо того, чтобы запустить работу локально
    поверх живой (E-06).
    """

    model_config = ConfigDict(extra="forbid")

    backend_type: ExecutionMode
    handle_id: str = Field(min_length=1, max_length=128)
    project_id: str
    version_id: Optional[str] = None
    attempt_id: Optional[str] = None
    remote_job_id: Optional[str] = None
    worker_id: Optional[str] = None
    execution_profile: Optional[str] = None
    created_at: float = 0.0


class ExecutionSnapshot(BaseModel):
    """Наблюдаемое состояние исполнения."""

    model_config = ConfigDict(extra="allow")

    execution_state: ExecutionState
    connectivity_state: Optional[str] = None
    stage: Optional[str] = None
    progress_current: int = 0
    progress_total: int = 0
    last_event_at: Optional[float] = None
    liveness: Liveness = Liveness.UNKNOWN
    liveness_reason: str = ""
    error: Optional[str] = None


class ExecutionResult(BaseModel):
    """Итог исполнения. Применяет его центр, и ровно один раз."""

    model_config = ConfigDict(extra="allow")

    success: bool = False
    cancelled: bool = False
    package_id: Optional[str] = None
    package_hash: Optional[str] = None
    returned_artifacts: list[str] = Field(default_factory=list)
    resume_stage: Optional[str] = None
    usage_report: Optional[dict[str, Any]] = None
    error: Optional[str] = None


@dataclass
class ExecutionContext:
    """Живые объекты платформы, которые backend'у нужны для работы.

    Вынесены из `ExecutionRequest` намеренно: запрос обязан быть
    сериализуемым и не содержать ссылок на объекты процесса. Контекст,
    наоборот, никогда не сериализуется и не уходит на воркер.
    """

    item: Any                     # BatchQueueItem
    job: Any                      # AuditJob
    default_action: str = "full"
    action_override: Optional[str] = None
    extra: dict[str, Any] = field(default_factory=dict)


class ExecutionBackend:
    """Контракт исполнения. Реализаций две: локальная и удалённая.

    Восемь операций §6 задания. `run()` — не девятая операция, а композиция
    остальных: локальному backend'у важно, чтобы она осталась ОДНИМ вызовом
    прежнего `_dispatch_action`, поэтому он её переопределяет.
    """

    backend_type: ExecutionMode = ExecutionMode.LOCAL

    async def prepare(
        self, request: ExecutionRequest, ctx: ExecutionContext
    ) -> ExecutionHandle:
        raise NotImplementedError

    async def start(self, handle: ExecutionHandle, ctx: ExecutionContext) -> None:
        raise NotImplementedError

    async def status(self, handle: ExecutionHandle) -> ExecutionSnapshot:
        raise NotImplementedError

    async def wait(
        self, handle: ExecutionHandle, ctx: ExecutionContext
    ) -> ExecutionResult:
        raise NotImplementedError

    async def cancel(self, handle: ExecutionHandle, *, reason: str = "") -> bool:
        raise NotImplementedError

    async def liveness(self, handle: ExecutionHandle) -> LivenessVerdict:
        raise NotImplementedError

    async def reattach(self, handle: ExecutionHandle) -> Optional[ExecutionSnapshot]:
        """Подхватить исполнение после рестарта центра. None = такого нет."""
        raise NotImplementedError

    async def collect_result(self, handle: ExecutionHandle) -> ExecutionResult:
        raise NotImplementedError

    async def run(
        self, request: ExecutionRequest, ctx: ExecutionContext
    ) -> ExecutionResult:
        handle = await self.prepare(request, ctx)
        await self.start(handle, ctx)
        return await self.wait(handle, ctx)
