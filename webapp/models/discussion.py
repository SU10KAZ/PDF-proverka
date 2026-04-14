"""Pydantic-модели для обсуждений замечаний/оптимизаций."""

from __future__ import annotations

from pydantic import BaseModel
from typing import Optional


class DiscussionMessage(BaseModel):
    """Одно сообщение в обсуждении."""
    role: str  # "user" | "assistant"
    content: str
    timestamp: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0


class Discussion(BaseModel):
    """Полная история обсуждения одного замечания/оптимизации."""
    item_id: str
    item_type: str  # "finding" | "optimization"
    model: str = ""
    status: Optional[str] = None  # None | "confirmed" | "rejected" | "revised"
    resolution_summary: Optional[str] = None
    messages: list[DiscussionMessage] = []
    summary_of_old: Optional[str] = None  # сжатие старых сообщений
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_cost_usd: float = 0.0


class ChatRequest(BaseModel):
    """Запрос на отправку сообщения в чат."""
    message: str
    model: str  # ID модели OpenRouter
    image: Optional[str] = None  # base64 data URL изображения


class ResolutionRequest(BaseModel):
    """Запрос на установку резолюции."""
    status: str  # "confirmed" | "rejected" | "revised"
    summary: str = ""


class ReviseRequest(BaseModel):
    """Запрос на генерацию изменённой версии замечания."""
    model: str


class DiscussionListItem(BaseModel):
    """Элемент списка обсуждений (замечание/оптимизация + статус дискуссии)."""
    item_id: str
    item_type: str
    severity: str = ""
    problem: str = ""
    discussion_status: Optional[str] = None  # None | "confirmed" | "rejected" | "revised"
    has_discussion: bool = False
    message_count: int = 0
    # Extended fields for table view
    sheet: str = ""
    norm: str = ""
    recommendation: str = ""
    resolution_summary: str = ""
    page: Optional[object] = None
    sub_findings: Optional[list[dict]] = None
    # Optimization-specific
    opt_type: str = ""
    current: str = ""
    proposed: str = ""
    savings_pct: Optional[float] = None
    savings_basis: str = ""
    risks: str = ""
    section: str = ""
    spec_items: list[str] = []
