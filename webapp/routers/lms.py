"""REST API для управления удалённой LM Studio."""
import asyncio

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from webapp.services import lms_service

router = APIRouter(prefix="/api/lms", tags=["lms"])


# Все вызовы lms_service синхронные (httpx + lmstudio SDK).
# Чтобы они не блокировали event loop FastAPI — обёртываем в thread pool.
async def _to_thread(fn, *args, **kwargs):
    return await asyncio.to_thread(fn, *args, **kwargs)


class LoadModelRequest(BaseModel):
    model_key: str
    context_length: int = 16384
    identifier: Optional[str] = None


class UnloadModelRequest(BaseModel):
    identifier: str


@router.get("/models/loaded")
async def models_loaded():
    """Список текущих загруженных моделей с identifier и context_length."""
    try:
        loaded = await _to_thread(lms_service.list_loaded)
        return {"loaded": loaded}
    except Exception as e:
        raise HTTPException(500, f"LM Studio недоступна: {e}")


@router.get("/health")
async def health(model_id: str | None = None):
    """Ping модели + счётчик активных prepare-задач.

    Использует кешированный health_check (не чаще раза в 20 сек реальный пинг).
    Все блокирующие вызовы — в thread pool, чтобы не вешать event loop.
    """
    try:
        # Кешированный list (15с TTL) — health-poll не бьёт ngrok каждые 30с впустую
        loaded = await _to_thread(lms_service.list_loaded_cached)
    except Exception:
        loaded = []
    if loaded:
        h = await _to_thread(lms_service.health_check_cached, model_id)
    else:
        h = {"alive": False, "model": None, "latency_ms": None, "error": "Нет загруженных моделей"}
    inflight = lms_service.get_inflight_count()
    return {
        "health": h,
        "inflight": inflight,
        "loaded_count": len(loaded),
    }


@router.get("/models/all")
async def models_all():
    """Все скачанные модели LM Studio (loaded + not-loaded)."""
    try:
        models = await _to_thread(lms_service.list_downloaded)
        return {"models": models}
    except Exception as e:
        raise HTTPException(500, f"LM Studio недоступна: {e}")


@router.post("/models/load")
async def load_model(req: LoadModelRequest):
    if req.context_length < 256 or req.context_length > 1_000_000:
        raise HTTPException(400, "context_length должен быть в [256, 1_000_000]")
    try:
        return await _to_thread(
            lms_service.load_model,
            req.model_key,
            context_length=req.context_length,
            identifier=req.identifier,
        )
    except Exception as e:
        raise HTTPException(500, f"Ошибка load: {e}")


@router.post("/models/unload")
async def unload_model(req: UnloadModelRequest):
    try:
        return await _to_thread(lms_service.unload_model, req.identifier)
    except Exception as e:
        raise HTTPException(500, f"Ошибка unload: {e}")


@router.post("/models/{model_key:path}/unload-all")
async def unload_all(model_key: str):
    try:
        count = await _to_thread(lms_service.unload_all_for, model_key)
        return {"unloaded": count}
    except Exception as e:
        raise HTTPException(500, f"Ошибка unload-all: {e}")


@router.post("/models/{model_key:path}/reload")
async def reload_model(model_key: str, context_length: int = 16384):
    if context_length < 256 or context_length > 1_000_000:
        raise HTTPException(400, "context_length должен быть в [256, 1_000_000]")
    try:
        unloaded = await _to_thread(lms_service.unload_all_for, model_key)
        result = await _to_thread(lms_service.load_model, model_key, context_length=context_length)
        return {"unloaded": unloaded, **result}
    except Exception as e:
        raise HTTPException(500, f"Ошибка reload: {e}")
