"""Pydantic-модели реестра внешних замечаний."""
from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class CustomerResponse(str, Enum):
    """Решение заказчика по нашему отправленному finding'у."""
    UCHTENO = "Учтено"               # передадим Генпроектировщику
    OTKLONENO = "Отклонено"          # не принимается
    VNESENO = "Внесено"              # уже внесено в РД
    PO_SOGLASOVANIYU = "По согласованию Заказчика"
    UNKNOWN = "Не определено"

    @classmethod
    def from_raw(cls, raw: str) -> "CustomerResponse":
        """Нормализовать OCR-варианты («Учитено», «Учено», «Учено» → «Учтено» etc.)."""
        if not raw:
            return cls.UNKNOWN
        t = (raw or "").strip().lower()
        if not t:
            return cls.UNKNOWN
        if t.startswith("отклон"):
            return cls.OTKLONENO
        if t.startswith("внес") or t.startswith("уже внес"):
            return cls.VNESENO
        if t.startswith("по соглас"):
            return cls.PO_SOGLASOVANIYU
        if t.startswith("учт") or t.startswith("учит") or t.startswith("учен"):
            return cls.UCHTENO
        return cls.UNKNOWN


class MatchStatus(str, Enum):
    """Текущий статус сопоставления записи реестра с finding'ами."""
    UNMATCHED = "unmatched"            # default
    NEEDS_REVIEW = "needs_review"      # LLM нашёл, но confidence в серой зоне
    AUTO_MATCHED = "auto_matched"      # LLM нашёл, confidence ≥ THRESHOLD, ждёт подтверждения
    CONFIRMED = "confirmed"            # пользователь подтвердил
    REJECTED = "rejected"              # пользователь отверг авто-match


class FindingMatch(BaseModel):
    """Ссылка на finding в нашей платформе."""
    project_id: str = Field(..., description="Путь подпроекта относительно projects_dir объекта")
    finding_id: str = Field(..., description="id внутри 03_findings.json")
    confidence: float = Field(0.0, ge=0.0, le=1.0)
    rationale: Optional[str] = Field(None, description="Краткое обоснование от LLM")


class RegisterEntry(BaseModel):
    """Одно внешнее замечание из реестра."""
    key: str = Field(..., description="Composite key, e.g. АР1#1 или СОТ#3")
    section_code: str = Field(..., description="Нормализованный код раздела (133/23-ГК-АР1, 1141-КИС-РД-М-АИ-П)")
    local_no: Optional[int] = Field(None, description="Номер строки в подсекции, если есть")
    sheet_ref: str = Field("", description="Лист/Раздел из реестра, как написано")
    problem: str = ""
    description: str = ""
    proposed_solution: str = ""
    cat_su10: str = Field("", description="Категория от СУ-10: Критическая/Экономическая/...")
    risk: str = ""
    customer_response_raw: str = Field("", description="Ответ заказчика «как написано»")
    customer_response: CustomerResponse = CustomerResponse.UNKNOWN
    customer_comment: str = ""

    match_status: MatchStatus = MatchStatus.UNMATCHED
    match: Optional[FindingMatch] = None
    user_confirmed_by: Optional[str] = None
    user_confirmed_at: Optional[str] = None

    # Источник: page-anchor из исходного MD, чтобы можно было прыгнуть к PDF
    source_page: Optional[int] = None


class RegisterFile(BaseModel):
    """Корневая структура файла реестра."""
    register_id: str
    object_id: str
    source_md: str = Field("", description="Путь к исходному markdown")
    imported_at: str = ""
    matched_at: Optional[str] = None
    entries: list[RegisterEntry] = Field(default_factory=list)

    # Денормализованные поля warning'ов
    unmapped_sections: list[str] = Field(default_factory=list)
