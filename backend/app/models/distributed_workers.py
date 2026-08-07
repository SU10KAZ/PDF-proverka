"""Pydantic-модели и закрытые enum распределённых audit-worker.

Ключевое свойство этого модуля — **закрытость перечислений**. Центр не может
попросить воркера сделать что-то, чего нет в JobType / WorkerCommandType, а
воркер обязан отвергать неизвестные значения. Это машинная реализация
инвариантов I-10 и I-11 техпроекта: канала «выполни произвольную команду»
не существует и не должно появиться незаметно.
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


# ─── Закрытые перечисления ───────────────────────────────────────────────────
class JobType(str, Enum):
    """Типы заданий, которые центр вправе выдать.

    На этапе 0 существует РОВНО ОДИН тип — безопасный тестовый конвейер.
    Реальный аудит появится отдельным типом только после интеграции с
    PipelineManager, которая в этот этап не входит.
    """

    TEST_PIPELINE_V1 = "test_pipeline_v1"


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

    Значения `run_shell` / `exec` / `eval` здесь нет и быть не может (I-10).
    На этапе 0 реализованы только те, что нужны вертикальному срезу.
    """

    CANCEL_JOB = "cancel_job"
    DRAIN = "drain"
    UNDRAIN = "undrain"


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
    configured_max_slots_hint: int = Field(default=1, ge=1, le=5)


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
    job_id: str
    attempt_id: str
    project_id: str = ""
    stage: str = ""
    last_event_seq: int = 0
    started_at: Optional[float] = None


class HeartbeatRequest(BaseModel):
    instance_id: str = Field(min_length=4, max_length=128)
    sent_at: float
    worker_state: WorkerState = WorkerState.IDLE
    configured_max_slots: int = Field(default=1, ge=0, le=5)
    calculated_free_slots: int = Field(default=0, ge=0, le=5)
    active_jobs: list[ActiveJobRef] = Field(default_factory=list)
    resource_snapshot: Optional[ResourceSnapshot] = None
    warnings: list[dict[str, Any]] = Field(default_factory=list)


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
    params: TestJobParams
    package: PackageRef
    fingerprints: dict[str, Any] = Field(default_factory=dict)
    event_start_seq: int = 1
    heartbeat_interval_sec: int = 30


class JobsNextRequest(BaseModel):
    free_slots: int = Field(default=1, ge=0, le=5)
    accepts: dict[str, Any] = Field(default_factory=dict)
    wait_sec: int = Field(default=25, ge=0, le=60)


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
    configured_max_slots: int = Field(default=1, ge=1, le=5)


class CreateTestJobRequest(BaseModel):
    worker_id: str
    project_id: str = Field(default="test-project", min_length=1, max_length=120,
                            pattern=r"^[A-Za-z0-9._\- ]+$")
    version_id: Optional[str] = Field(default=None, max_length=40,
                                      pattern=r"^[A-Za-z0-9._-]*$")
    params: TestJobParams = Field(default_factory=TestJobParams)


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
