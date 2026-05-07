"""
stage_result.py
---------------
Общий базовый результат для stage runner-ов.

Для простых этапов (prepare, report, crop_blocks) используйте StageResult.
Для этапов с расширенным состоянием (optimization, findings_review) можно
использовать собственные dataclass-ы с теми же полями success/cancelled/error.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class StageResult:
    """Минимальный результат выполнения одного stage runner-а.

    Поля:
        success   — True если этап завершился без ошибки.
        cancelled — True если job был отменён (CancelledError или пользователь).
        error     — строка с описанием ошибки (только если success=False).
        data      — опциональный dict с дополнительными данными этапа.
    """
    success: bool
    cancelled: bool = False
    error: Optional[str] = None
    data: Optional[dict[str, Any]] = field(default=None)

    @classmethod
    def ok(cls, **data) -> "StageResult":
        return cls(success=True, data=data if data else None)

    @classmethod
    def fail(cls, error: str, **data) -> "StageResult":
        return cls(success=False, error=error, data=data if data else None)

    @classmethod
    def cancel(cls) -> "StageResult":
        return cls(success=False, cancelled=True)
