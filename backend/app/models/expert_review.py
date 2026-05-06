"""Pydantic-модели для экспертной оценки и базы знаний."""

from __future__ import annotations

from pydantic import BaseModel
from typing import Optional


class ExpertDecision(BaseModel):
    """Решение эксперта по одному замечанию/оптимизации."""
    item_id: str                          # F-001, OPT-003
    item_type: str                        # "finding" | "optimization"
    decision: str                         # "accepted" | "rejected"
    rejection_reason: Optional[str] = None
    reviewer: str = ""
    timestamp: str = ""


class ExpertReviewSubmission(BaseModel):
    """Пакет решений эксперта по проекту."""
    decisions: list[ExpertDecision]
    reviewer: str = ""


class KnowledgeBaseEntry(BaseModel):
    """Запись в глобальной базе знаний."""
    id: str                               # DEC-0001
    source_project: str                   # EOM/133_23-ГК-ГРЩ
    section: str                          # EOM
    item_id: str                          # F-003
    item_type: str                        # "finding" | "optimization"

    # Контекст замечания/оптимизации
    severity: str = ""
    category: str = ""
    summary: str = ""                     # краткое описание проблемы
    norm_refs: list[str] = []
    sheet: str = ""
    page: Optional[object] = None

    # Решение эксперта
    expert_decision: str = ""             # "accepted" | "rejected"
    expert_reason: str = ""
    expert_reviewer: str = ""
    expert_date: str = ""

    # Согласование заказчиком (только для accepted)
    customer_confirmed: bool = False
    customer_date: Optional[str] = None
    customer_note: Optional[str] = None

    @property
    def status(self) -> str:
        if self.customer_confirmed:
            return "customer_confirmed"
        return self.expert_decision  # "accepted" | "rejected"


class CustomerConfirmRequest(BaseModel):
    """Запрос на подтверждение заказчиком."""
    entry_ids: list[str]
    note: str = ""


class PatternSuggestion(BaseModel):
    """Обнаруженный паттерн из отклонённых решений."""
    pattern_id: str                       # PAT-001
    section: str                          # EOM
    description: str
    frequency: int                        # сколько раз встречался
    projects_affected: list[str]
    example_ids: list[str]                # DEC-id примеров
    suggested_fix: str                    # предложение по корректировке промпта
    target_file: str = ""                 # куда применить (checklist.md и т.п.)

    status: str = "pending"               # pending | applied | dismissed | edited
    proposed_at: str = ""
    decided_by: Optional[str] = None
    decided_at: Optional[str] = None


class PatternActionRequest(BaseModel):
    """Запрос на действие с паттерном."""
    edited_fix: Optional[str] = None      # для status=edited
