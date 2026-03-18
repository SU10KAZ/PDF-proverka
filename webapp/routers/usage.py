"""REST API для трекинга потребления токенов."""

from fastapi import APIRouter
from webapp.services.usage_service import (
    usage_tracker, global_scanner,
    WINDOW_5H_TOKEN_LIMIT, WEEKLY_TOKEN_LIMIT,
)

router = APIRouter(prefix="/api/usage", tags=["usage"])


@router.get("/counters")
async def get_counters():
    """Получить текущие значения трёх счётчиков (сессия, 5ч окно, неделя).
    Только вызовы через webapp."""
    return usage_tracker.get_counters().model_dump()


@router.get("/global")
async def get_global_counters():
    """Глобальная статистика из ВСЕХ сессий Claude Code (парсинг JSONL).
    Формат как на дашборде Anthropic."""
    return global_scanner.get_counters().model_dump()


@router.post("/global/refresh")
async def refresh_global():
    """Принудительно пересканировать JSONL."""
    global_scanner.invalidate_cache()
    return global_scanner.get_counters().model_dump()


@router.post("/global/limits")
async def update_limits(session_5h: int = 0, weekly_all: int = 0):
    """Обновить лимиты (для калибровки под реальные данные дашборда)."""
    global_scanner.set_limits(session_5h=session_5h, weekly_all=weekly_all)
    return {"status": "ok", "session_5h_limit": global_scanner.session_5h_limit,
            "weekly_all_limit": global_scanner.weekly_all_limit}


@router.post("/global/weekly-reset")
async def update_weekly_reset(weekday: int = 4, hour_utc: int = 6):
    """Изменить день/время еженедельного сброса.
    weekday: 0=пн..6=вс, hour_utc: час UTC."""
    global_scanner.set_weekly_reset(weekday=weekday, hour_utc=hour_utc)
    return {"status": "ok", "weekday": weekday, "hour_utc": hour_utc}


@router.post("/reset-session")
async def reset_session():
    """Сброс сессионного счётчика (только webapp)."""
    usage_tracker.reset_session()
    return {"status": "ok", "message": "Сессионный счётчик сброшен"}


@router.get("/project/{project_id}")
async def get_project_usage(project_id: str):
    """Агрегация токенов по проекту: total + по этапам."""
    return usage_tracker.get_project_usage(project_id)


@router.get("/projects-summary")
async def get_all_projects_usage():
    """Краткая сводка токенов по всем проектам (для дашборда)."""
    return usage_tracker.get_all_projects_usage()


@router.get("/history")
async def get_history(limit: int = 50):
    """Последние N записей потребления."""
    records = usage_tracker.get_recent(limit)
    return {"records": records}


@router.get("/config")
async def get_limits():
    """Текущие лимиты и настройки."""
    return {
        "window_5h_limit": global_scanner.session_5h_limit,
        "weekly_limit": global_scanner.weekly_all_limit,
        "weekly_reset_weekday": global_scanner.weekly_reset_weekday,
        "weekly_reset_hour_utc": global_scanner.weekly_reset_hour_utc,
        "plan": "Max 20x ($200/month)",
    }
