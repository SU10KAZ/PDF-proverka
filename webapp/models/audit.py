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
    TEXT_ANALYSIS = "text_analysis"
    BLOCK_ANALYSIS = "block_analysis"
    FINDINGS_MERGE = "findings_merge"
    FINDINGS_REVIEW = "findings_review"
    # v4 pipeline (fact-first) — заменяет block_analysis + findings_merge
    V4_EXTRACTION = "v4_extraction"
    V4_MEMORY = "v4_memory"
    V4_CANDIDATES = "v4_candidates"
    V4_FORMATTER = "v4_formatter"


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
