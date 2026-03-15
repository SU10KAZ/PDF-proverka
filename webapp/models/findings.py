"""Pydantic-модели для замечаний аудита."""
from pydantic import BaseModel
from typing import Optional, Any


class Finding(BaseModel):
    """Одно замечание аудита."""
    id: str = ""                         # F-001, T-001, G-001
    severity: str = ""                    # КРИТИЧЕСКОЕ / ЭКОНОМИЧЕСКОЕ / ...
    category: str = ""
    sheet: str = ""
    problem: Optional[str] = None
    description: Optional[str] = None
    finding: Optional[str] = None         # обратная совместимость
    norm: str = ""
    solution: Optional[str] = None
    recommendation: Optional[str] = None
    risk: Optional[str] = None
    source: Optional[dict] = None
    md_pdf_discrepancy: Optional[dict] = None

    @property
    def display_text(self) -> str:
        """Текст замечания для отображения."""
        return self.problem or self.finding or self.description or ""


class FindingsSummary(BaseModel):
    """Сводка замечаний по проекту."""
    project_id: str
    total: int = 0
    by_severity: dict[str, int] = {}
    audit_date: Optional[str] = None


class FindingsResponse(BaseModel):
    """Ответ API со списком замечаний."""
    project_id: str
    total: int = 0
    filtered_total: Optional[int] = None  # кол-во после фильтрации (до пагинации)
    by_severity: dict[str, int] = {}
    findings: list[dict] = []            # raw dict для гибкости
    audit_date: Optional[str] = None
