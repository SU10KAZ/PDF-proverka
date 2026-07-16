"""
API журнала действий: чтение и сводная статистика.

Источник данных — суточные JSONL-файлы ACTION_LOG_DIR (см. core/action_log.py).
Файловое чтение обёрнуто в asyncio.to_thread, чтобы не блокировать event loop
(watchdog убивает сервер при блокировке /api/info > 5с).
"""
import asyncio
from datetime import date

from fastapi import APIRouter, HTTPException, Query

from backend.app.core import action_log

router = APIRouter(prefix="/api/action-log", tags=["action-log"])


def _norm_date(value: str | None, name: str) -> str | None:
    """Канонизировать дату или дать явный 422 (кривая дата без валидации
    молча ломала лексикографический фильтр по имени файла)."""
    if value is None or not value.strip():
        return None
    try:
        return date.fromisoformat(value.strip()).isoformat()
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail=f"{name}: ожидается дата YYYY-MM-DD, получено {value!r}",
        )


def _persons_for(items: list[dict]) -> dict[str, str]:
    """Маппинг логин → ФИО инженера (users.json). Fail-soft."""
    persons: dict[str, str] = {}
    try:
        from backend.app.services.common import user_service

        for actor in {it.get("actor") for it in items if it.get("actor")}:
            user = user_service.get_user_by_login(actor)
            if isinstance(user, dict) and user.get("name"):
                persons[actor] = user["name"]
    except Exception:
        pass
    return persons


@router.get("")
async def list_events(
    date_from: str | None = Query(None, description="YYYY-MM-DD включительно"),
    date_to: str | None = Query(None, description="YYYY-MM-DD включительно"),
    kind: str | None = Query(None, description="api | pipeline | app_log | system"),
    actor: str | None = Query(None, description="логин портала"),
    q: str | None = Query(None, description="подстрока по событию"),
    errors_only: bool = Query(False),
    limit: int = Query(200, ge=1, le=2000),
    offset: int = Query(0, ge=0),
):
    """События журнала, новые → старые."""
    date_from = _norm_date(date_from, "date_from")
    date_to = _norm_date(date_to, "date_to")
    try:
        result = await asyncio.to_thread(
            action_log.read_events,
            date_from=date_from,
            date_to=date_to,
            kind=kind,
            actor=actor,
            q=q,
            errors_only=errors_only,
            limit=limit,
            offset=offset,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка чтения журнала: {e}")
    # users.json читается с диска — тоже уводим с event loop.
    result["persons"] = await asyncio.to_thread(_persons_for, result.get("items", []))
    return result


@router.get("/stats")
async def get_stats(days: int = Query(7, ge=1, le=366)):
    """Сводка по последним N дням: объёмы, ошибки, активность по инженерам."""
    try:
        return await asyncio.to_thread(action_log.stats, days)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка чтения журнала: {e}")
