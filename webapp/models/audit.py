"""Pydantic-модели для аудита."""
from pydantic import BaseModel
from typing import Optional
from enum import Enum
from datetime import datetime


class AuditStage(str, Enum):
    PREPARE = "prepare"
    TILE_BATCHES = "tile_batches"
    TILE_AUDIT = "tile_audit"
    MAIN_AUDIT = "main_audit"
    MERGE = "merge"
    NORM_VERIFY = "norm_verify"
    NORM_FIX = "norm_fix"
    EXCEL = "excel"
    OPTIMIZATION = "optimization"
    # OCR-пайплайн
    CROP_BLOCKS = "crop_blocks"
    QWEN_ENRICHMENT = "qwen_enrichment"  # Stage 00: Qwen-обогащение MD (после crop, до text_analysis)
    TEXT_ANALYSIS = "text_analysis"
    BLOCK_ANALYSIS = "block_analysis"
    FLASH_PRO_TRIAGE = "flash_pro_triage"
    FINDINGS_MERGE = "findings_merge"
    FINDINGS_REVIEW = "findings_review"


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class AuditJob(BaseModel):
    """Текущая задача аудита."""
    job_id: str
    project_id: str
    # object_id фиксируется на старте job. Pipeline обязан использовать именно
    # его для резолва путей проекта, иначе переключение current_id в UI
    # перекинет write-пути в чужой объект (см. resolve_project_dir binding).
    object_id: Optional[str] = None
    stage: AuditStage = AuditStage.PREPARE
    status: JobStatus = JobStatus.QUEUED
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    progress_current: int = 0
    progress_total: int = 0
    error_message: Optional[str] = None
    # Heartbeat & ETA
    last_heartbeat: Optional[str] = None       # ISO timestamp последнего heartbeat
    batch_started_at: Optional[str] = None      # когда начался текущий пакет
    batch_durations: list[float] = []            # длительности завершённых пакетов (сек)
    # Rate limit паузы — чистое время = wall-clock минус pause_total_sec
    pause_total_sec: float = 0.0                 # суммарное время пауз (сек)
    # Потребление токенов
    tokens_input: int = 0
    tokens_output: int = 0
    cost_usd: float = 0.0
    cli_calls: int = 0


class BatchStatus(BaseModel):
    """Статус одного пакета тайлов."""
    batch_id: int
    tile_count: int = 0
    pages_included: list[int] = []
    status: str = "pending"  # pending / running / done / error
    result_size_kb: float = 0.0
    duration_minutes: float = 0.0


class AuditStatusResponse(BaseModel):
    """Ответ на запрос статуса аудита."""
    project_id: str
    is_running: bool = False
    current_job: Optional[AuditJob] = None
    batches: list[BatchStatus] = []


# ─── Batch (групповые действия) ───

class BatchAction(str, Enum):
    """Тип группового действия."""
    FULL = "full"
    RESUME = "resume"
    AUDIT = "audit"
    OPTIMIZATION = "optimization"
    AUDIT_OPTIMIZATION = "audit+optimization"
    # Legacy aliases
    STANDARD = "standard"
    PRO = "pro"
    STANDARD_OPTIMIZATION = "standard+optimization"
    PRO_OPTIMIZATION = "pro+optimization"


class BatchRequest(BaseModel):
    """Запрос на групповое действие."""
    project_ids: list[str]
    action: BatchAction


class BatchQueueItem(BaseModel):
    """Элемент очереди группового действия."""
    project_id: str
    action: str = "full"
    retry_stage: Optional[str] = None  # конкретный этап для retry (например "block_analysis")
    status: str = "pending"  # pending / running / completed / failed / skipped / cancelled
    error: Optional[str] = None
    # Доп. параметры для actions с аргументами (start_from, max_pro_cost_usd, ...)
    extra_params: dict = {}
    # job_id, который вернётся клиенту при enqueue — используется для трассировки
    job_id: Optional[str] = None


class BatchQueueStatus(BaseModel):
    """Состояние очереди группового действия."""
    queue_id: str
    action: str = "full"
    items: list[BatchQueueItem] = []
    current_index: int = 0
    total: int = 0
    completed: int = 0
    failed: int = 0
    status: str = "running"  # running / completed / cancelled


class PrepareQueueItem(BaseModel):
    """Элемент очереди подготовки данных (crop + Qwen enrichment)."""
    project_id: str
    status: str = "pending"  # pending / running / completed / failed / skipped
    blocks_total: Optional[int] = None
    blocks_done: int = 0
    blocks_failed: int = 0
    blocks_truncated: int = 0  # обрыв из-за max_output_tokens (нужно увеличить лимит)
    max_output_tokens_seen: int = 0  # макс. реальный output_tokens у успешных блоков
    started_at: Optional[float] = None  # epoch seconds
    elapsed_sec: float = 0.0
    eta_sec: Optional[float] = None
    error: Optional[str] = None
    force: bool = False
    # Pre-crop: блоки скачиваются параллельно с Qwen enrichment предыдущих проектов.
    # crop_status: pending → running → done / failed
    crop_status: str = "pending"
    crop_blocks_total: int = 0   # сколько блоков обработал crop (новые + пропущенные)


class PrepareQueueStatus(BaseModel):
    """Состояние очереди подготовки данных (Qwen enrichment)."""
    items: list[PrepareQueueItem] = []
    current_index: int = 0
    total: int = 0
    completed: int = 0
    failed: int = 0
    status: str = "idle"  # idle / running / paused
    paused: bool = False
    # Суммарные счётчики по всей очереди (для индикации в шапке)
    blocks_total_all: int = 0   # сумма items.blocks_total
    blocks_done_all: int = 0    # сумма items.blocks_done
    blocks_failed_all: int = 0
    blocks_truncated_all: int = 0
    current_project: Optional[str] = None  # identifier текущего running проекта
    # Сумма wall-clock времени по проектам (running + completed). На завершении
    # очереди = общее время от старта первого до конца последнего, минус параллельные
    # пересечения с pre-crop (т.к. crop складывается с enrich). Считаем как сумму
    # item.elapsed_sec, чтоб дать честную оценку time-on-project.
    total_elapsed_sec: float = 0.0
