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
    init: str = "pending"            # pending / running / done / error / skipped
    text_analysis: str = "pending"   # pending / running / done / error / skipped
    tiles_analysis: str = "pending"  # pending / running / done / error / partial / skipped
    findings: str = "pending"        # pending / running / done / error / skipped
    norms_verified: str = "pending"  # pending / running / done / error / partial / skipped
    optimization: str = "pending"    # pending / running / done / error / skipped


class ProjectInfo(BaseModel):
    project_id: str
    name: str
    object: Optional[str] = None
    section: str = "EM"
    description: str = ""
    pdf_file: str = "document.pdf"
    tile_config: dict = {}
    tile_quality: str = "standard"
    text_extraction_quality: Optional[TextExtractionQuality] = None


class ProjectStatus(BaseModel):
    """Полный статус проекта для Dashboard."""
    project_id: str
    name: str
    description: str = ""
    section: str = "EM"
    object: Optional[str] = None
    has_pdf: bool = False
    pdf_size_mb: float = 0.0
    has_extracted_text: bool = False
    text_size_kb: float = 0.0
    # MD-файл (структурированный текст из внешнего OCR)
    has_md_file: bool = False
    md_file_name: Optional[str] = None
    md_file_size_kb: float = 0.0
    text_source: str = "extracted_text"  # "md" | "extracted_text" | "none"
    has_tiles: bool = False
    tile_count: int = 0
    tile_pages: int = 0
    pipeline: PipelineStatus = PipelineStatus()
    findings_count: int = 0
    findings_by_severity: dict[str, int] = {}
    last_audit_date: Optional[str] = None
    # Пакетный анализ тайлов
    total_batches: int = 0
    completed_batches: int = 0


class ProjectCreate(BaseModel):
    """Запрос на создание проекта."""
    project_id: str
    name: str
    section: str = "EM"
    description: str = ""
    object: Optional[str] = None
