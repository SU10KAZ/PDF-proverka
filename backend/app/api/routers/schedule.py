"""
REST API графика производства работ.

`GET /api/schedule?from=YYYY-MM-DD&to=YYYY-MM-DD&object_id=...`

Отдаёт реальные события (инженер × день × проект) на основе
knowledge_base/decisions_log.json. Read-only, ничего не пишет.
"""
from datetime import date, timedelta
from typing import Optional
import asyncio

from fastapi import APIRouter, HTTPException, Query

import backend.app.services.common.schedule_service as schedule_service

router = APIRouter(prefix="/api/schedule", tags=["schedule"])


def _default_period() -> tuple[str, str]:
    """Текущая неделя (понедельник–воскресенье) как дефолт периода."""
    today = date.today()
    monday = today - timedelta(days=today.weekday())
    sunday = monday + timedelta(days=6)
    return monday.isoformat(), sunday.isoformat()


def _is_iso_day(s: str) -> bool:
    try:
        date.fromisoformat(s)
        return True
    except ValueError:
        return False


@router.get("")
async def get_schedule(
    from_: Optional[str] = Query(None, alias="from", description="Начало периода YYYY-MM-DD"),
    to: Optional[str] = Query(None, alias="to", description="Конец периода YYYY-MM-DD"),
    object_id: Optional[str] = Query(None, description="Фильтр по объекту (опционально)"),
):
    """События графика за период. from/to — по умолчанию текущая неделя."""
    d_from, d_to = _default_period()
    from_day = (from_ or "").strip() or d_from
    to_day = (to or "").strip() or d_to

    if not _is_iso_day(from_day) or not _is_iso_day(to_day):
        raise HTTPException(400, "Параметры from/to должны быть в формате YYYY-MM-DD")
    if from_day > to_day:
        from_day, to_day = to_day, from_day

    obj = (object_id or "").strip() or None
    # Разбор большого decisions_log не должен блокировать event loop.
    return await asyncio.to_thread(schedule_service.get_schedule, from_day, to_day, obj)
