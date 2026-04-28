"""Pydantic-модели для проектов."""
from pydantic import BaseModel
from typing import Optional


class TextExtractionQuality(BaseModel):
    overall_quality: str = "UNKNOWN"  # OK / PARTIAL_OCR / CRITICAL
    direct_ok: int = 0
    ocr_fallback: int = 0
    corrupted_kept: int = 0
    corrupted_fonts: list[str] = []
    ocr_engine: Optional[str] = None


class PipelineStatus(BaseModel):
    crop_blocks: str = "pending"            # pending / running / done / error / skipped
    text_analysis: str = "pending"          # pending / running / done / error / skipped
    blocks_analysis: str = "pending"        # pending / running / done / error / partial / skipped
    block_retry: str = "pending"               # pending / running / done / error / skipped
    findings: str = "pending"               # pending / running / done / error / skipped
    findings_critic: str = "pending"        # pending / running / done / error / skipped
    findings_corrector: str = "pending"     # pending / running / done / error / skipped
    norms_verified: str = "pending"         # pending / running / done / error / partial / skipped
    optimization: str = "pending"           # pending / running / done / error / skipped
    optimization_critic: str = "pending"    # pending / running / done / error / skipped
    optimization_corrector: str = "pending" # pending / running / done / error / skipped
    excel: str = "pending"                  # pending / running / done / error / skipped


class ProjectInfo(BaseModel):
    project_id: str
    name: str
    object: Optional[str] = None
    section: str = "EOM"
    description: str = ""
    pdf_file: str = "document.pdf"
    pdf_files: list[str] = []          # несколько PDF в одном проекте
    tile_config: dict = {}
    tile_quality: str = "standard"
    text_extraction_quality: Optional[TextExtractionQuality] = None
    pipeline_version: str = "legacy"


class ProjectStatus(BaseModel):
    """Полный статус проекта для Dashboard."""
    project_id: str
    name: str
    description: str = ""
    section: str = "EOM"
    object: Optional[str] = None
    has_pdf: bool = False
    pdf_size_mb: float = 0.0
    pdf_files: list[str] = []              # все PDF файлы проекта
    has_extracted_text: bool = False
    text_size_kb: float = 0.0
    # MD-файл (структурированный текст из внешнего OCR)
    has_md_file: bool = False
    md_file_name: Optional[str] = None
    md_file_size_kb: float = 0.0
    text_source: str = "extracted_text"  # "md" | "extracted_text" | "none"
    pipeline: PipelineStatus = PipelineStatus()
    findings_count: int = 0
    findings_by_severity: dict[str, int] = {}
    optimization_count: int = 0
    optimization_by_type: dict[str, int] = {}
    optimization_savings_pct: float = 0
    last_audit_date: Optional[str] = None
    # Пакетный анализ тайлов
    total_batches: int = 0
    completed_batches: int = 0
    # OCR-данные (result.json от OCR-сервера)
    has_ocr: bool = False
    block_count: int = 0
    block_errors: int = 0
    block_expected: int = 0
    # Детальное саммари конвейера из pipeline_log.json
    pipeline_summary: list[dict] = []
    # Проблемы конвейера (для индикатора на дашборде)
    pipeline_issues: list[str] = []
    pipeline_version: str = "legacy"
    # Экспертная оценка: "complete" (все решения), "partial" (частично), "" (нет)
    expert_review_status: str = ""


class ProjectCreate(BaseModel):
    """Запрос на создание проекта."""
    project_id: str
    name: str
    section: str = "EOM"
    description: str = ""
    object: Optional[str] = None
