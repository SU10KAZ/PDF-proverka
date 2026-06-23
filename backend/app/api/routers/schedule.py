"""
REST API графика производства работ.

`GET /api/schedule?from=YYYY-MM-DD&to=YYYY-MM-DD&object_id=...`

Отдаёт реальные события (инженер × день × проект) на основе
knowledge_base/decisions_log.json. Read-only, ничего не пишет.
"""
from datetime import date, timedelta
from typing import List, Literal, Optional
import asyncio

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, conint, field_validator

import backend.app.services.common.schedule_service as schedule_service
import backend.app.services.common.user_service as user_service
from backend.app.core import portal_auth

router = APIRouter(prefix="/api/schedule", tags=["schedule"])


def _default_period(period_type: str = "week") -> tuple[str, str]:
    """Дефолт периода: текущая неделя (Пн–Вс) или текущий месяц (1-е…конец)."""
    today = date.today()
    if period_type == "month":
        first = today.replace(day=1)
        nxt = date(today.year + 1, 1, 1) if today.month == 12 else date(today.year, today.month + 1, 1)
        return first.isoformat(), (nxt - timedelta(days=1)).isoformat()
    monday = today - timedelta(days=today.weekday())
    return monday.isoformat(), (monday + timedelta(days=6)).isoformat()


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


# ─── План работ (work_plans.json) ────────────────────────────────────────────

class WorkPlanItem(BaseModel):
    engineer_id: str
    engineer_name: str = ""
    plan: conint(ge=schedule_service.PLAN_MIN, le=schedule_service.PLAN_MAX)

    @field_validator("engineer_id")
    @classmethod
    def _eng_id_not_empty(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("engineer_id обязателен")
        return v


class WorkPlanUpdate(BaseModel):
    period_type: Literal["week", "month"]
    period_start: str
    period_end: str
    object_id: Optional[str] = None
    plans: List[WorkPlanItem]

    @field_validator("period_start", "period_end")
    @classmethod
    def _iso_day(cls, v: str) -> str:
        try:
            date.fromisoformat((v or "").strip())
        except ValueError:
            raise ValueError("period_start/period_end должны быть в формате YYYY-MM-DD")
        return v.strip()


def _require_admin(request: Request) -> str:
    """Admin-гейт для редактирования плана.

    Использует существующую portal-auth + user_service. Если auth выключена
    (dev/локальная разработка) — PUT разрешён, updated_by = текущий пользователь
    (если задан). Если auth включена — нужен сотрудник с role=admin, иначе 403.
    """
    settings = portal_auth.get_settings()
    if not settings.enabled:
        cur = user_service.get_current_user()
        return (cur or {}).get("name", "") if cur else ""
    username = portal_auth.request_username(request, settings)
    user = user_service.get_user_by_login(username) if username else None
    if not user or (user.get("role") or "") != "admin":
        raise HTTPException(403, "Редактировать план может только администратор")
    return user.get("name") or username or ""


@router.get("/plan")
async def get_plan(
    from_: Optional[str] = Query(None, alias="from", description="Начало периода YYYY-MM-DD"),
    to: Optional[str] = Query(None, alias="to", description="Конец периода YYYY-MM-DD"),
    period_type: str = Query("week", description="week | month"),
    object_id: Optional[str] = Query(None, description="Фильтр по объекту (опционально)"),
):
    """План работ по инженерам для периода. Нет файла → пустой список."""
    if period_type not in ("week", "month"):
        raise HTTPException(400, "period_type должен быть week или month")
    d_from, d_to = _default_period(period_type)   # дефолт зависит от week/month
    from_day = (from_ or "").strip() or d_from
    to_day = (to or "").strip() or d_to
    if not _is_iso_day(from_day) or not _is_iso_day(to_day):
        raise HTTPException(400, "Параметры from/to должны быть в формате YYYY-MM-DD")
    if from_day > to_day:
        from_day, to_day = to_day, from_day
    obj = (object_id or "").strip() or None
    return await asyncio.to_thread(
        schedule_service.get_plans,
        period_type=period_type, period_start=from_day, period_end=to_day, object_id=obj,
    )


@router.put("/plan")
async def put_plan(payload: WorkPlanUpdate, request: Request):
    """Обновить план для одного периода (только администратор)."""
    updated_by = _require_admin(request)
    if payload.period_start > payload.period_end:
        raise HTTPException(400, "period_start не может быть позже period_end")
    obj = (payload.object_id or "").strip() or None
    plans = [
        {"engineer_id": p.engineer_id, "engineer_name": p.engineer_name, "plan": int(p.plan)}
        for p in payload.plans
    ]
    return await asyncio.to_thread(
        schedule_service.save_plans,
        period_type=payload.period_type,
        period_start=payload.period_start,
        period_end=payload.period_end,
        object_id=obj,
        plans=plans,
        updated_by=updated_by,
    )
