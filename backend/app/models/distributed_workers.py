"""Pydantic-модели и закрытые enum распределённых audit-worker.

Ключевое свойство этого модуля — **закрытость перечислений**. Центр не может
попросить воркера сделать что-то, чего нет в JobType / WorkerCommandType, а
воркер обязан отвергать неизвестные значения. Это машинная реализация
инвариантов I-10 и I-11 техпроекта: канала «выполни произвольную команду»
не существует и не должно появиться незаметно.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator


# ─── Закрытые перечисления ───────────────────────────────────────────────────
class JobType(str, Enum):
    """Типы заданий, которые центр вправе выдать.

    Их РОВНО ДВА, и оба — фиксированные имена реализаций, установленных на
    воркере. Ни одно из них не описывает, ЧТО выполнять: ни команды, ни argv,
    ни пути, ни модуля. Воркер сопоставляет имя со своей встроенной
    реализацией и отказывается от любого другого значения (I-10, E-10).
    """

    TEST_PIPELINE_V1 = "test_pipeline_v1"
    #: Реальный аудит проекта. Появился на этапе ExecutionBackend.
    AUDIT_PIPELINE_V1 = "audit_pipeline_v1"


#: Профиль пилотного удалённого аудита. Один и фиксированный: несколько почти
#: одинаковых профилей гарантированно разъедутся в поведении.
REMOTE_AUDIT_PILOT_V1 = "remote_audit_pilot_v1"

#: Этапы, которые профилю РАЗРЕШЕНО выполнять на воркере. Список живёт в коде,
#: а не приходит произвольным JSON от центра (§5 задания). Нормативный этап и
#: перенос вердиктов сюда не входят: оба пишут в общие центральные файлы.
REMOTE_AUDIT_PILOT_STAGES: tuple[str, ...] = (
    "crop_blocks",
    "document_graph",
    "block_context",
    "block_analysis",
    "text_analysis",
    "findings_merge",
    "findings_review",
    "optimization",
    "optimization_review",
)

#: Этапы, которые остаются на центре ВСЕГДА.
CENTRAL_ONLY_STAGES: tuple[str, ...] = (
    "norm_verify",
    "debt_control",
    "decision_carryover",
    "excel",
)


class JobState(str, Enum):
    """Ось ИСПОЛНЕНИЯ (15 значений, см. §10.1 техпроекта).

    Отсутствие heartbeat НИКОГДА не переводит задание в failed — молчание
    меняет только ось связи (ConnectivityState).
    """

    CREATED = "created"
    ASSIGNED = "assigned"
    SOURCE_UPLOADING = "source_uploading"
    SOURCE_READY = "source_ready"
    ACCEPTED_BY_WORKER = "accepted_by_worker"
    RUNNING = "running"
    COMPLETED_LOCALLY = "completed_locally"
    RESULT_UPLOADING = "result_uploading"
    RESULT_RECEIVED = "result_received"
    VALIDATING = "validating"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCEL_REQUESTED = "cancel_requested"
    CANCELLED = "cancelled"
    # Терминальное состояние ОТОЗВАННОЙ попытки: результат принят на хранение,
    # но не публикуется никогда. У актуальной попытки его быть не может.
    SUPERSEDED_RESULT_RECEIVED = "superseded_result_received"


TERMINAL_JOB_STATES: frozenset[JobState] = frozenset(
    {
        JobState.COMPLETED,
        JobState.FAILED,
        JobState.CANCELLED,
        JobState.SUPERSEDED_RESULT_RECEIVED,
    }
)


class ConnectivityState(str, Enum):
    """Ось СВЯЗИ. Вычисляется исключительно из свежести heartbeat."""

    ONLINE = "online"
    STALE = "stale"
    OFFLINE = "offline"
    RECONNECTING = "reconnecting"


class RetentionState(str, Enum):
    """Ось ХРАНЕНИЯ копии пакета на воркере."""

    RETAINED = "retained"
    DELETION_PENDING = "deletion_pending"
    DELETED_FROM_WORKER = "deleted_from_worker"
    EXPIRED_AUTO_DELETED = "expired_auto_deleted"


class RegistrationStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    REVOKED = "revoked"


class WorkerState(str, Enum):
    """Готовность воркера принимать работу (ортогональна связи)."""

    UNREGISTERED = "unregistered"
    PENDING_APPROVAL = "pending_approval"
    IDLE = "idle"
    BUSY = "busy"
    DEGRADED = "degraded"
    DRAINING = "draining"
    DRAINED = "drained"
    REVOKED = "revoked"


class WorkerEventType(str, Enum):
    """Единый формат событий (§14.2 техпроекта).

    Часть типов на этапе 0 не порождается (нет реального аудита), но входит в
    enum, потому что контракт протокола фиксируется сейчас.
    """

    JOB_ACCEPTED = "job_accepted"
    JOB_STARTED = "job_started"
    STAGE_STARTED = "stage_started"
    STAGE_PROGRESS = "stage_progress"
    LOG_LINE = "log_line"
    STAGE_COMPLETED = "stage_completed"
    ARTIFACT_CREATED = "artifact_created"
    QUOTA_WARNING = "quota_warning"
    RESOURCE_WARNING = "resource_warning"
    JOB_COMPLETED_LOCALLY = "job_completed_locally"
    RESULT_UPLOAD_STARTED = "result_upload_started"
    RESULT_UPLOAD_PROGRESS = "result_upload_progress"
    JOB_COMPLETED = "job_completed"
    JOB_FAILED = "job_failed"
    CANCELLATION_RECEIVED = "cancellation_received"
    WORKER_RECONNECTED = "worker_reconnected"
    WORKER_RESTARTED = "worker_restarted"
    SOURCE_VERIFIED = "source_verified"
    SOURCE_INVALID = "source_invalid"
    EVENTS_TRUNCATED = "events_truncated"


# События-строки лога уходят в файл, а не в таблицу (§9.5).
FILE_ONLY_EVENT_TYPES: frozenset[str] = frozenset({WorkerEventType.LOG_LINE.value})


class WorkerCommandType(str, Enum):
    """Закрытый набор команд центра.

    Значений `run_shell` / `exec` / `eval` / `script` / `argv` здесь нет и быть
    не может (I-10). Этап 3.5 добавляет ровно два адресных типа — отмену
    попытки и удаление её локальных данных.
    """

    CANCEL_ATTEMPT = "cancel_attempt"
    DELETE_ATTEMPT_DATA = "delete_attempt_data"
    # Историческое имя отмены из этапа 0. Читается ради совместимости со
    # старыми строками worker_commands; новые команды им не создаются.
    CANCEL_JOB = "cancel_job"
    DRAIN = "drain"
    UNDRAIN = "undrain"


ACTIVE_COMMAND_TYPES: frozenset[str] = frozenset(
    {WorkerCommandType.CANCEL_ATTEMPT.value, WorkerCommandType.DELETE_ATTEMPT_DATA.value}
)


class CancelAttemptPayload(BaseModel):
    """Полезная нагрузка отмены. `extra="forbid"`: лишнее поле — отказ."""

    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(min_length=1, max_length=64)
    attempt_id: str = Field(min_length=1, max_length=64)
    grace_period_sec: int = Field(default=30, ge=0, le=600)
    reason: str = Field(default="", max_length=500)


class DeleteAttemptDataPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(min_length=1, max_length=64)
    attempt_id: str = Field(min_length=1, max_length=64)
    reason: str = Field(default="", max_length=500)


COMMAND_PAYLOAD_MODELS: dict[str, type[BaseModel]] = {
    WorkerCommandType.CANCEL_ATTEMPT.value: CancelAttemptPayload,
    WorkerCommandType.DELETE_ATTEMPT_DATA.value: DeleteAttemptDataPayload,
}


# ─── Регистрация ─────────────────────────────────────────────────────────────
class WorkerCapabilities(BaseModel):
    """Возможности воркера. Расширяемо: неизвестные поля не ломают разбор."""

    model_config = ConfigDict(extra="allow")

    providers: list[str] = Field(default_factory=list)
    compressions: list[str] = Field(default_factory=lambda: ["gzip"])
    job_types: list[str] = Field(default_factory=lambda: [JobType.TEST_PIPELINE_V1.value])
    python: Optional[str] = None
    os: Optional[str] = None
    cores: Optional[int] = None
    ram_total_gb: Optional[float] = None
    disk_total_gb: Optional[float] = None
    max_package_bytes: Optional[int] = None


class RegisterRequest(BaseModel):
    instance_id: str = Field(min_length=4, max_length=128)
    display_name_hint: str = Field(default="", max_length=200)
    worker_version: str = Field(default="0.0.0", max_length=64)
    protocol_version: int = 1
    pipeline_revision: Optional[str] = Field(default=None, max_length=200)
    capabilities: WorkerCapabilities = Field(default_factory=WorkerCapabilities)
    configured_max_slots_hint: int = Field(default=1, ge=1, le=64)


class RegisterResponse(BaseModel):
    worker_id: str
    registration_status: RegistrationStatus
    # ОДНОРАЗОВЫЙ claim-secret. Токена доступа здесь НЕТ намеренно: он
    # выдаётся только после одобрения оператором, в обмен на этот секрет
    # (POST /claim). Центр хранит только его sha256.
    claim_secret: Optional[str] = None
    heartbeat_interval_sec: int
    poll_timeout_sec: int
    chunk_size_bytes: int
    protocol_version: int
    message: str


class ClaimRequest(BaseModel):
    """Обмен одноразового claim-secret на постоянный токен."""

    worker_id: str = Field(min_length=4, max_length=64)
    instance_id: str = Field(min_length=4, max_length=128)
    claim_secret: str = Field(min_length=16, max_length=256)


class ClaimResponse(BaseModel):
    worker_id: str
    registration_status: RegistrationStatus
    # Единственный раз в жизни. Повторный claim → 409.
    worker_token: str
    heartbeat_interval_sec: int
    poll_timeout_sec: int
    chunk_size_bytes: int
    protocol_version: int


class RegistrationUpdateRequest(BaseModel):
    instance_id: str = Field(min_length=4, max_length=128)
    worker_version: str = Field(default="0.0.0", max_length=64)
    protocol_version: int = 1
    pipeline_revision: Optional[str] = Field(default=None, max_length=200)
    capabilities: WorkerCapabilities = Field(default_factory=WorkerCapabilities)


# ─── Ресурсы и heartbeat ─────────────────────────────────────────────────────
class ResourceSnapshot(BaseModel):
    model_config = ConfigDict(extra="allow")

    at: float
    ram: dict[str, Any] = Field(default_factory=dict)
    cpu: dict[str, Any] = Field(default_factory=dict)
    disk: dict[str, Any] = Field(default_factory=dict)
    processes: dict[str, Any] = Field(default_factory=dict)
    slots: dict[str, Any] = Field(default_factory=dict)


class ActiveJobRef(BaseModel):
    """Ссылка на активное задание в heartbeat.

    Длины ограничены: строка целиком уходит в колонку `workers.active_jobs`,
    и воркер (пусть и одобренный) не должен иметь возможности раздувать БД
    центра мегабайтными «идентификаторами».
    """

    job_id: str = Field(max_length=64)
    attempt_id: str = Field(max_length=64)
    project_id: str = Field(default="", max_length=200)
    stage: str = Field(default="", max_length=64)
    last_event_seq: int = 0
    started_at: Optional[float] = None


class ExecutorSnapshot(BaseModel):
    """Состояние ЛОКАЛЬНОГО исполнителя, отдельного от сетевого агента.

    Агент онлайн ≠ VPS работает: процессы держит executor, и его молчание —
    самостоятельная новость, которую экран обязан показать отдельно (§16.6).
    """

    model_config = ConfigDict(extra="allow")

    # Типы намеренно свободные: блок приходит от полу-доверенного воркера, и
    # мусор в одном поле не должен ронять ВЕСЬ heartbeat 422-й ошибкой — иначе
    # воркер выглядит офлайн из-за кривой строки. Приведение и отбраковка —
    # в worker_registry.sanitize_executor.
    executor_instance_id: Optional[Any] = None
    status: Any = "unknown"          # online | stale | offline | unknown
    last_heartbeat_at: Optional[Any] = None
    version: Optional[Any] = None
    running_processes: Any = 0
    ambiguous_processes: Any = 0


class DiskSnapshot(BaseModel):
    """Диск воркера в разрезе, который нужен для решения об удалении (§12.5)."""

    model_config = ConfigDict(extra="allow")

    # Как и у ExecutorSnapshot: типы свободные, приведение — в sanitize_disk.
    # Кривое число от воркера не должно превращать heartbeat в 422.
    total_bytes: Optional[Any] = None
    used_bytes: Optional[Any] = None
    free_bytes: Optional[Any] = None
    jobs_bytes: Optional[Any] = None
    confirmed_results_bytes: Optional[Any] = None
    unconfirmed_results_bytes: Optional[Any] = None
    cleanup_candidates_bytes: Optional[Any] = None
    cleanup_candidates: Any = 0
    level: Any = "ok"                 # ok | warning | critical


class HeartbeatRequest(BaseModel):
    instance_id: str = Field(min_length=4, max_length=128)
    sent_at: float
    worker_state: WorkerState = WorkerState.IDLE
    configured_max_slots: int = Field(default=1, ge=0, le=64)
    calculated_free_slots: int = Field(default=0, ge=0, le=64)
    # Верхние границы здесь — санитарный предел против абсурдного ввода, а НЕ
    # заявление о поддерживаемом числе слотов. Единственный потолок системы —
    # slots.MAX_VERIFIED_SLOTS (=2), и он применяется зажимом с предупреждением
    # в normalize_max_slots. Прежнее `le=5` делало из pydantic второго судью:
    # 3-5 зажималось с объяснением, а 6 отбивалось голым 422, и OpenAPI-схема
    # публично обещала пять слотов, которых нет.
    # Список активных заданий — своя граница, не связанная со слотами.
    active_jobs: list[ActiveJobRef] = Field(default_factory=list, max_length=16)
    resource_snapshot: Optional[ResourceSnapshot] = None
    warnings: list[dict[str, Any]] = Field(default_factory=list, max_length=64)
    # Агент отчитывается и за СЕБЯ, и за наблюдаемый им executor. Если
    # executor молчит, а агент онлайн — это отдельная новость, а не «всё ок».
    executor: Optional[ExecutorSnapshot] = None
    disk: Optional[DiskSnapshot] = None
    # ─── Слоты (пред-пайплайновый этап) ─────────────────────────────────────
    # Что сборка воркера ПРОВЕРИЛА, а не что оператор пожелал. Старый агент
    # поля не пришлёт — для него это 1, и это честный ответ: доказательств
    # двух слотов у его сборки нет.
    max_verified_slots: int = Field(default=1, ge=0, le=64)
    # Диагностика локального учёта. Центр решение принимает по СВОЕЙ базе, а
    # эти числа сравнивает со своими и при расхождении показывает
    # slot_count_mismatch (S-15) — доверять им как источнику истины нельзя.
    active_local_jobs: int = Field(default=0, ge=0, le=64)
    running_processes: int = Field(default=0, ge=0, le=64)
    locally_reserved_slots: int = Field(default=0, ge=0, le=64)
    # ─── Состояние провайдеров (этап 11) ────────────────────────────────────
    # Тип намеренно максимально широкий (`Any`, не `dict`) — и это поведенческое
    # решение, а не лень.
    #
    # Heartbeat — сигнал ЖИВОСТИ. Отбить его с 422 из-за одного неразобранного
    # элемента в снимке провайдера значит превратить исправный воркер в «пропал
    # со связи»: центр перестанет считать его онлайн, перестанет выдавать
    # задания и покажет оператору аварию там, где сломалось наблюдение за
    # подпиской. Цена несоразмерна поводу (§27 задания).
    #
    # `list[dict[str, Any]]` этой гарантии НЕ даёт: pydantic отвергает весь
    # запрос, если хоть один элемент не объект. Проверено тестом
    # `test_heartbeat_carries_providers_and_survives_bad_snapshot`.
    #
    # Строгую форму задаёт `provider_accounts.sanitize_provider_snapshot`: он
    # ПЕРЕСОБИРАЕТ объект из разрешённых значений и молча отбрасывает мусор.
    # Старый воркер поля не пришлёт вовсе — для него это пустой список.
    providers: list[Any] = Field(default_factory=list, max_length=8)


class CursorAck(BaseModel):
    job_id: str
    attempt_id: str
    last_seen_seq: int


class RetentionUpdate(BaseModel):
    job_id: str
    attempt_id: str
    retention_until: Optional[float] = None


class HeartbeatResponse(BaseModel):
    server_time: float
    connection_status: ConnectivityState
    has_pending_commands: bool
    has_available_work: bool
    next_heartbeat_in_sec: int
    acked_cursors: list[CursorAck] = Field(default_factory=list)
    retention_updates: list[RetentionUpdate] = Field(default_factory=list)


# ─── Выдача задания ──────────────────────────────────────────────────────────
class TestJobParams(BaseModel):
    """Полезная нагрузка `test_pipeline_v1`.

    Здесь НЕТ и не может быть: shell-команды, имени исполняемого файла,
    аргументов, Python-кода, пути к файлу, переменных окружения. Только
    безопасные скалярные параметры, которые воркер валидирует и зажимает
    повторно на своей стороне (§4 задания, §20 техпроекта).
    """

    model_config = ConfigDict(extra="forbid")

    label: str = Field(default="smoke", min_length=1, max_length=64,
                       pattern=r"^[A-Za-z0-9._-]+$")
    steps: int = Field(default=5, ge=1, le=100)
    step_seconds: float = Field(default=0.5, ge=0.0, le=10.0)
    result_bytes: int = Field(default=4096, ge=0, le=8 * 1024 * 1024)
    fail_at_step: Optional[int] = Field(default=None, ge=1, le=100)


class AuditPipelineParams(BaseModel):
    """Полезная нагрузка `audit_pipeline_v1`. `extra="forbid"` — обязательно.

    Что здесь ЗАПРЕЩЕНО по построению, а не по договорённости: command, argv,
    executable, script, module, cwd, env, hook, tool list, любой путь. Поля
    просто нет — значит его нельзя ни прислать, ни «случайно поддержать».
    Реализацию выбирает воркер по имени типа задания.
    """

    model_config = ConfigDict(extra="forbid")

    execution_profile: Literal["remote_audit_pilot_v1"] = REMOTE_AUDIT_PILOT_V1
    #: Действие конвейера. Закрытый набор: произвольная строка сюда не пройдёт.
    action: Literal["full", "audit", "resume"] = "full"
    retry_stage: Optional[str] = Field(default=None, max_length=64)
    include_optimization: bool = True
    #: Нормативный этап на воркере не выполняется НИКОГДА (E-19). Тип Literal,
    #: а не bool: «случайно передать true» невозможно.
    include_norms: Literal[False] = False
    project_layout_version: int = Field(default=2, ge=1, le=100)
    pipeline_revision: str = Field(min_length=1, max_length=200)
    #: Ожидаемые хэши. Воркер сверяет их с тем, что реально распаковал.
    expected_source_tree_hash: str = Field(min_length=8, max_length=128)
    prompt_bundle_hash: str = Field(min_length=8, max_length=128)
    model_config_hash: str = Field(min_length=8, max_length=128)
    feature_flags_hash: str = Field(min_length=8, max_length=128)
    #: Хэш снимка runtime-конфигурации. Обязателен: без него режим записи
    #: хранилища взялся бы с ХОСТА воркера, и результат зависел бы от машины.
    runtime_snapshot_hash: str = Field(min_length=8, max_length=128)
    #: Дисциплина попытки в КАНОНИЧЕСКОЙ форме и хэш снимка её профиля.
    #: Оба обязательны: задание без них означало бы «выбери профиль сам», а
    #: выбирал воркер из дерева установленного кода — и при кириллическом
    #: `section` молча брал EOM.
    discipline_id: str = Field(min_length=1, max_length=32, pattern=r"^[^/\\\s]+$")
    discipline_profile_hash: str = Field(min_length=8, max_length=128)
    #: Обязательные артефакты результата. Список фиксирован центром, но воркер
    #: сверяет его со своим встроенным — расширить его заданием нельзя.
    required_result_artifacts: list[str] = Field(default_factory=list, max_length=64)

    @field_validator("retry_stage")
    @classmethod
    def _check_stage(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        if value not in REMOTE_AUDIT_PILOT_STAGES:
            raise ValueError(
                f"Этап {value!r} не входит в профиль {REMOTE_AUDIT_PILOT_V1}. "
                f"Разрешены: {', '.join(REMOTE_AUDIT_PILOT_STAGES)}"
            )
        return value


class PackageRef(BaseModel):
    package_id: str
    package_type: Literal["source", "result", "superseded_result"]
    url: str
    size_bytes: int
    sha256: str
    compression: str
    manifest_version: int


class JobAssignment(BaseModel):
    job_id: str
    attempt_id: str
    attempt_no: int
    execution_token: str
    assigned_at: float
    assign_ttl_sec: int
    job_type: JobType
    project_id: str
    version_id: Optional[str] = None
    # Порядок в объединении значим: обе модели с `extra="forbid"`, поэтому
    # нагрузка реального аудита не может «сойти» за тестовую и наоборот.
    params: Union[AuditPipelineParams, TestJobParams]
    package: PackageRef
    fingerprints: dict[str, Any] = Field(default_factory=dict)
    event_start_seq: int = 1
    heartbeat_interval_sec: int = 30


class JobsNextRequest(BaseModel):
    free_slots: int = Field(default=1, ge=0, le=64)
    # Занятость НА СТОРОНЕ ВОРКЕРА в том же снимке, что и free_slots.
    # Вместе они дают его собственную ёмкость, не зависящую от того,
    # что центр успел выдать в этом же окне. Старый агент поля не шлёт —
    # для него центр считает по-прежнему (см. claim_next_job_for_worker).
    busy_slots: Optional[int] = Field(default=None, ge=0, le=64)
    accepts: dict[str, Any] = Field(default_factory=dict)
    wait_sec: int = Field(default=25, ge=0, le=60)
    # Состояние локального исполнителя НА МОМЕНТ ЗАПРОСА. Свежее, чем снимок из
    # heartbeat: между ударами проходит до 30 секунд, и «исполнитель был офлайн
    # полминуты назад» — плохое основание отказать в работе, которую уже есть
    # кому делать. Если поле не пришло (старый агент), центр берёт heartbeat.
    executor_status: Optional[str] = Field(default=None, max_length=32)


class SourceVerification(BaseModel):
    model_config = ConfigDict(extra="allow")

    sha256_ok: bool
    manifest_version: int
    files_checked: int = 0
    files_total: int = 0
    unpacked_bytes: int = 0


class AcceptRequest(BaseModel):
    attempt_id: str
    accepted_at: float
    source_verified: SourceVerification
    planned_stages: list[str] = Field(default_factory=list)


class RejectRequest(BaseModel):
    attempt_id: str
    reason: str = Field(max_length=500)


# ─── События ─────────────────────────────────────────────────────────────────
class WorkerEventIn(BaseModel):
    seq: int = Field(ge=1)
    event_id: str = Field(min_length=1, max_length=128)
    # job_id/attempt_id несёт конверт батча (EventBatchRequest) — внутри
    # события они избыточны. Оставлены необязательными для диагностики и
    # совместимости, но источником истины является конверт.
    job_id: Optional[str] = None
    attempt_id: Optional[str] = None
    event_type: WorkerEventType
    occurred_at: float
    schema_version: int = 1
    payload: dict[str, Any] = Field(default_factory=dict)


class EventBatchRequest(BaseModel):
    job_id: str
    attempt_id: str
    first_seq: int = Field(ge=1)
    count: int = Field(ge=0)
    events: list[WorkerEventIn] = Field(default_factory=list)


class EventBatchResponse(BaseModel):
    last_seen_seq: int
    accepted: int
    skipped_duplicates: int
    replayed: bool = False


# ─── Загрузка результата ─────────────────────────────────────────────────────
class UploadCreateRequest(BaseModel):
    job_id: str
    attempt_id: str
    package_type: Literal["result", "superseded_result"] = "result"
    expected_size: int = Field(ge=0)
    expected_hash: str = Field(min_length=64, max_length=71)
    compression: str = "gzip"
    manifest_version: int = 1


class UploadSessionInfo(BaseModel):
    upload_id: str
    chunk_size: int
    chunks_total: int
    received_chunks: list[int]
    expires_at: float
    status: str = "open"
    replayed: bool = False


class UploadCompleteRequest(BaseModel):
    job_id: str
    attempt_id: str
    sha256: str = Field(min_length=64, max_length=71)
    total_size: int = Field(ge=0)
    chunks_sent: int = Field(ge=0)


class UploadCompleteResponse(BaseModel):
    state: JobState
    validation: dict[str, Any] = Field(default_factory=dict)
    server_time: float
    retention_until: Optional[float] = None


# ─── Команды ─────────────────────────────────────────────────────────────────
class WorkerCommandOut(BaseModel):
    command_id: str
    command_type: WorkerCommandType
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: float
    idempotency_key: str
    job_id: Optional[str] = None
    attempt_id: Optional[str] = None
    expires_at: Optional[float] = None


class CommandsNextRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    wait_sec: int = Field(default=0, ge=0, le=60)


class CommandAckRequest(BaseModel):
    result: dict[str, Any] = Field(default_factory=dict)
    acknowledged_at: float


# ─── Reconciliation ──────────────────────────────────────────────────────────
class ReconcileKnownJob(BaseModel):
    job_id: str
    attempt_id: str
    local_state: str
    last_written_seq: int = 0
    last_acked_seq: int = 0
    result_ready: bool = False
    result_hash: Optional[str] = None
    # Этап 3.5: воркер отчитывается о своём взгляде на попытку и процессы.
    # Поля объявлены явно — pydantic молча выбрасывает необъявленные.
    local_disposition: Optional[str] = None
    process_status: Optional[str] = None
    completed_marker: Optional[dict[str, Any]] = None
    pending_local_commands: int = 0
    result_acknowledged: bool = False
    deleted_from_worker: bool = False
    executor_instance_id: Optional[str] = None
    # Поля ниже объявлены явно: pydantic молча выбрасывает необъявленные, и
    # присланное воркером «процесс мёртв» иначе не доезжает до решения центра.
    pipeline_stage: Optional[str] = None
    processes_alive: bool = False
    source_present: bool = False
    result_present: bool = False
    upload_id: Optional[str] = None
    # Что воркер помнит о ретеншне. Если здесь null, а центр результат принял,
    # ответ вернёт retention_until — иначе пакет будет храниться вечно.
    retention_until: Optional[float] = None


class ReconcileRequest(BaseModel):
    instance_id: str
    previous_instance_id: Optional[str] = None
    restarted_at: float
    known_jobs: list[ReconcileKnownJob] = Field(default_factory=list)
    agent_instance_id: Optional[str] = None
    executor: Optional[ExecutorSnapshot] = None
    pending_central_commands: int = 0
    disk: Optional[dict[str, Any]] = None


class ReconcileJobVerdict(BaseModel):
    job_id: str
    attempt_id: str
    center_state: Optional[JobState] = None
    attempt_valid: bool
    expected_next_seq: int
    # Закрытый enum действий: воркер не принимает решений о судьбе задания сам.
    action: Literal[
        "continue", "upload_result", "stop_superseded", "discard_unknown", "await_operator"
    ]
    upload_hint: Optional[dict[str, Any]] = None
    # Действителен ли execution_token попытки. Отличается от attempt_valid
    # тем, что попытка может совпадать, а задание — уже быть терминальным.
    execution_token_valid: bool = False
    # Приём результата подтверждён центром (I-08): только после этого воркеру
    # разрешено заводить таймер удаления.
    result_accepted: bool = False
    retention_until: Optional[float] = None
    # Этап 3.5: попытка может быть НЕ текущей, но всё ещё иметь право сдать
    # свои события и результат в собственный контур (I-07).
    attempt_disposition: Optional[str] = None
    current_attempt_id: Optional[str] = None
    assignment_generation: int = 1
    # Разрешён ли обычный приём событий этой попытки. Для отозванной — да,
    # но они уходят в историю СТАРОЙ попытки и прогресс новой не трогают.
    event_ingestion_allowed: bool = True
    # Запрет повторного запуска: центр прямо говорит, что стартовать процесс
    # заново нельзя. Решение о повторе принимает только оператор.
    restart_forbidden: bool = True
    deletion_status: Optional[str] = None


class ReconcileResponse(BaseModel):
    server_time: float
    jobs: list[ReconcileJobVerdict] = Field(default_factory=list)
    unknown_jobs: list[str] = Field(default_factory=list)
    # Попытки, отозванные в пользу новых: воркер обязан остановить процессы.
    superseded_jobs: list[str] = Field(default_factory=list)
    # Задания, возвращённые в очередь: воркер получит их обычным опросом.
    reoffered_jobs: list[str] = Field(default_factory=list)
    pending_commands: int = 0


# ─── Административные (операторские) модели ──────────────────────────────────
class WorkerView(BaseModel):
    model_config = ConfigDict(extra="allow")

    worker_id: str
    display_name: str
    instance_id: Optional[str] = None
    registration_status: RegistrationStatus
    connection_status: ConnectivityState
    worker_state: WorkerState
    last_seen_at: Optional[float] = None
    seconds_since_seen: Optional[float] = None
    worker_version: Optional[str] = None
    protocol_version: int = 1
    pipeline_revision: Optional[str] = None
    capabilities: dict[str, Any] = Field(default_factory=dict)
    configured_max_slots: int = 1
    calculated_free_slots: int = 0
    active_jobs: list[dict[str, Any]] = Field(default_factory=list)
    resource_snapshot: Optional[dict[str, Any]] = None
    warnings: list[dict[str, Any]] = Field(default_factory=list)
    created_at: float
    updated_at: float


class ApproveRequest(BaseModel):
    display_name: Optional[str] = Field(default=None, max_length=200)
    configured_max_slots: int = Field(default=1, ge=1, le=64)


class CreateTestJobRequest(BaseModel):
    worker_id: str
    # Внешний код проекта: кириллица, пробелы, кавычки и «/» допустимы —
    # реальные коды выглядят как «13АВ/РД-АР3-К7». Компонентом пути он не
    # становится никогда (I-11), поэтому запрещать «/» здесь незачем; запрещены
    # только NUL и управляющие символы (см. identifiers.normalize_external_id).
    project_id: str = Field(default="test-project", min_length=1, max_length=200)
    project_display_name: str = Field(default="", max_length=300)
    version_id: Optional[str] = Field(default=None, max_length=200)
    params: TestJobParams = Field(default_factory=TestJobParams)

    @field_validator("project_id")
    @classmethod
    def _check_project_id(cls, value: str) -> str:
        from backend.app.services.distributed_workers import identifiers

        try:
            return identifiers.normalize_external_id(value, field="project_id")
        except identifiers.UnsafeIdentifier as exc:
            raise ValueError(str(exc)) from exc

    @field_validator("version_id")
    @classmethod
    def _check_version_id(cls, value: Optional[str]) -> Optional[str]:
        from backend.app.services.distributed_workers import identifiers

        if value is None or not value.strip():
            return None
        try:
            return identifiers.normalize_external_id(value, field="version_id")
        except identifiers.UnsafeIdentifier as exc:
            raise ValueError(str(exc)) from exc


# ─── Операторское управление попытками (этап 3.5) ────────────────────────────
class CancelAttemptRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str = Field(min_length=1, max_length=1000)
    # Совпадает с CONFIRM_CANCEL в attempt_service. Не галочка: фразу вводят.
    confirmation: str = Field(min_length=1, max_length=64)
    grace_period_sec: int = Field(default=30, ge=0, le=600)


class MarkAttemptLostRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mandatory_reason: str = Field(min_length=1, max_length=1000)
    typed_confirmation: str = Field(min_length=1, max_length=64)
    observed_worker_state: str = Field(default="", max_length=200)
    optional_operator_note: str = Field(default="", max_length=1000)


class CreateAttemptRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    worker_id: str = Field(min_length=1, max_length=64)
    reason: str = Field(min_length=1, max_length=1000)
    source_attempt_id: Optional[str] = Field(default=None, max_length=64)
    confirmation: str = Field(min_length=1, max_length=64)
    # Явное признание риска превышения ёмкости. Требуется в одном случае:
    # предыдущая попытка признана потерянной, её процесс мог остаться жив, а
    # связи с VPS нет — то есть центр не может ни увидеть чужой процесс, ни
    # попросить его остановиться (§34 задания). Во всех остальных случаях поле
    # не нужно и ни на что не влияет.
    accept_capacity_risk: bool = False


class RemoteAuditLaunchRequest(BaseModel):
    """Ручной запуск реального аудита на выбранном воркере.

    Полей ровно четыре, и ни одно не описывает СПОСОБ исполнения. Воркер
    выбирает оператор: автовыбора на этом этапе нет намеренно (§3.2).
    """

    model_config = ConfigDict(extra="forbid")

    worker_id: str = Field(min_length=1, max_length=64)
    project_id: str = Field(min_length=1, max_length=300)
    version_id: Optional[str] = Field(default=None, max_length=64)
    action: Literal["full", "audit", "resume"] = "full"


class SubscriptionAccountUpdate(BaseModel):
    """Ручные поля учётной записи подписки (§13, §14, §22 задания).

    Чего в модели НЕТ и не появится: токена, пароля, refresh-token, cookie и
    API-ключа. Это не «мы решили не принимать» — поля отсутствуют, значит
    прислать их некуда, и путь «секрет провайдера доехал до центра» не
    существует структурно.

    `exclude_unset` на стороне обработчика важен: незаполненное поле формы
    означает «не трогать», а не «стереть». Для явного стирания даты сброса
    есть отдельный флаг `clear_manual_reset` — иначе любая частичная правка
    молча удаляла бы дату, которую оператор ставил руками.
    """

    model_config = ConfigDict(extra="forbid")

    display_name: Optional[str] = Field(default=None, min_length=1, max_length=120)
    notes: Optional[str] = Field(default=None, max_length=4000)
    manual_reset_label: Optional[str] = Field(default=None, max_length=120)
    #: Unix-время в секундах. Диапазон проверяется в сервисе (2020..2100):
    #: «сброс 12.03.1970» на экране хуже отсутствия даты.
    manual_next_reset_at: Optional[float] = None
    manual_reset_recurrence: Optional[str] = Field(default=None, max_length=32)
    reset_timezone: Optional[str] = Field(default=None, max_length=64)
    warning_days: Optional[list[int]] = Field(default=None, max_length=10)
    #: «Лимит почти не использован» — утверждение ОПЕРАТОРА. Единственный
    #: способ зажечь предупреждение о сгорающем лимите там, где остаток
    #: объективно неизвестен (§23). Автоматика такого вывода не делает.
    operator_marked_unused: Optional[bool] = None
    policy_state: Optional[str] = Field(default=None, max_length=32)
    account_kind: Optional[str] = Field(default=None, max_length=32)
    clear_manual_reset: bool = False


class WorkerProviderGroupUpdate(BaseModel):
    """Привязка провайдера воркера к общей учётной записи (§15).

    `None` и пустая строка означают «отвязать»: воркер продолжит показывать
    состояние провайдера на своей карточке, но ни к какому аккаунту отнесён
    не будет.
    """

    model_config = ConfigDict(extra="forbid")

    account_group_id: Optional[str] = Field(default=None, max_length=64)


class RequestDeletionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str = Field(min_length=1, max_length=1000)
    confirmation: str = Field(min_length=1, max_length=64)


class JobView(BaseModel):
    model_config = ConfigDict(extra="allow")

    job_id: str
    job_type: JobType
    project_id: str
    version_id: Optional[str] = None
    attempt_id: str
    attempt_no: int
    assigned_worker_id: Optional[str] = None
    state: JobState
    connectivity_state: ConnectivityState
    retention_state: RetentionState
    # Вычисляемый признак, НЕ состояние: результат лежит на воркере, а центр
    # приём не подтвердил → автоудаление запрещено (§10.6 техпроекта).
    retention_unconfirmed: bool = False
    display_status: str = ""
    package_id: Optional[str] = None
    source_package_hash: Optional[str] = None
    result_package_hash: Optional[str] = None
    created_at: float
    assigned_at: Optional[float] = None
    started_at: Optional[float] = None
    completed_locally_at: Optional[float] = None
    returned_at: Optional[float] = None
    validated_at: Optional[float] = None
    retention_until: Optional[float] = None
    last_event_seq: int = 0
    error: Optional[dict[str, Any]] = None
    progress_snapshot: Optional[dict[str, Any]] = None
