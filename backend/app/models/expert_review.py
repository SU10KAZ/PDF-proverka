"""Pydantic-модели для экспертной оценки и базы знаний."""

from __future__ import annotations

from pydantic import BaseModel
from typing import Optional


class ExpertDecision(BaseModel):
    """Решение эксперта по одному замечанию/оптимизации."""
    item_id: str                          # F-001, OPT-003
    item_type: str                        # "finding" | "optimization"
    decision: str                         # "accepted" | "rejected"
    # `rejection_reason` используется как ОБЩИЙ комментарий эксперта (не только для
    # отклонений): для авто-переноса вердикта из прошлой версии сюда кладётся
    # пояснение и для accepted, и для rejected (Excel-отчёт уже читает это поле).
    rejection_reason: Optional[str] = None
    reviewer: str = ""
    timestamp: str = ""

    # Авто-перенос вердикта из предыдущей проверенной версии (decision carryover).
    # carried_over=True → решение проставлено автоматически, эксперт может
    # переопределить. Ручные решения (carried_over=False) авто-этап не трогает.
    carried_over: bool = False
    carried_from_version: Optional[str] = None
    carried_from_item_id: Optional[str] = None


class ExpertReviewSubmission(BaseModel):
    """Пакет решений эксперта по проекту."""
    decisions: list[ExpertDecision]
    removed_ids: list[str] = []
    reviewer: str = ""


class KnowledgeBaseEntry(BaseModel):
    """Запись в глобальной базе знаний."""
    id: str                               # DEC-0001
    object_id: str = ""                   # 0b540226 (объект — здание/комплекс)
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

    # Evidence / grounding (from source finding)
    grounding_level: str = ""
    primary_block_ids: list[str] = []
    evidence_types: list[str] = []

    # Решение эксперта
    expert_decision: str = ""             # "accepted" | "rejected"
    expert_reason: str = ""
    expert_reviewer: str = ""
    expert_date: str = ""

    # Дословный ответ заказчика из реестра (Внесено / Требует внесения /
    # По согласованию / Отклонено), если замечание пришло из external_register.
    customer_response: str = ""

    # Согласование заказчиком (только для accepted)
    customer_confirmed: bool = False
    customer_date: Optional[str] = None
    customer_note: Optional[str] = None

    # Авто-перенос вердикта из предыдущей версии (decision carryover).
    # current_version_id нужен, чтобы отличать запись V2 от записи V1 в
    # decisions_log.json (ключ дедупа (source_project, item_id) не версионный).
    carried_over: bool = False
    carried_from_version: str = ""
    current_version_id: str = ""

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
