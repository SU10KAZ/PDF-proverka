"""REST API для отдельного окна управления моделями."""

from __future__ import annotations

import subprocess
from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

import backend.app.services.llm.model_control_service as model_control_service
import backend.app.services.llm.model_capabilities as model_capabilities
import backend.app.services.llm.server_profiles as server_profiles
from backend.app.core.config import ROOT_DIR


router = APIRouter(prefix="/api/model-control", tags=["model-control"])


class LoadModelRequest(BaseModel):
    model: str
    context_length: int = Field(..., ge=512, le=262144)
    flash_attention: bool = True
    offload_kv_cache_to_gpu: bool = True
    eval_batch_size: int | None = Field(default=None, ge=1, le=65536)
    num_experts: int | None = Field(default=None, ge=1, le=256)


class EstimateLoadRequest(BaseModel):
    model: str
    context_length: int = Field(..., ge=512, le=262144)
    gpu: str | None = None


class UnloadInstanceRequest(BaseModel):
    instance_id: str


class ActivateProfileRequest(BaseModel):
    profile_id: str


def _schedule_backend_restart() -> None:
    """Отложенный detached-рестарт backend (той же связкой, что и cron-watchdog).

    Запускается в отдельной сессии (`start_new_session`), чтобы пережить убийство
    текущего процесса stop_server.sh. Пауза даёт FastAPI отдать HTTP-ответ до
    рестарта. Watchdog (раз в минуту) — страховка, если detached-старт не поднимет.
    """
    webapp_dir = Path(ROOT_DIR) / "webapp"
    log = webapp_dir / "profile_switch_restart.log"
    cmd = (
        f"sleep 2; cd {webapp_dir!s} && "
        f"./stop_server.sh && ./start_server_deploy.sh"
    )
    with open(log, "ab") as fh:
        subprocess.Popen(
            ["setsid", "bash", "-lc", cmd],
            stdin=subprocess.DEVNULL,
            stdout=fh,
            stderr=fh,
            start_new_session=True,
            cwd=str(webapp_dir),
        )


@router.get("/status")
async def get_status():
    """Получить текущий статус подключения, моделей и памяти."""
    return model_control_service.get_status()


@router.get("/remote-status")
async def get_remote_status():
    """Честный статус для удалённого bearer-сервера (01.vibe).

    Ходит только в OpenAI /v1/models (нативное управление недоступно). Плюс
    ресурсы AUDIT-хоста (RAM/CPU) — это webapp-сервер, а не LLM-хост; GPU LLM
    находится на удалённом хосте и локально не виден.
    """
    status = model_capabilities.get_remote_models_status()
    try:
        status["audit_host"] = model_control_service._system_memory()
    except Exception as exc:  # noqa: BLE001
        status["audit_host"] = {"error": f"{type(exc).__name__}: {exc}"}
    status["gpu_note"] = "GPU/VRAM находятся на удалённом LLM-хосте (01.vibe) — локально недоступны."
    return status


@router.post("/estimate")
async def estimate_load(req: EstimateLoadRequest):
    """Локально оценить требования к памяти при выбранном контексте."""
    return model_control_service.estimate_load(
        model=req.model,
        context_length=req.context_length,
        gpu=req.gpu,
    )


@router.post("/load")
async def load_model(req: LoadModelRequest):
    """Загрузить модель в LM Studio с выбранными параметрами."""
    return model_control_service.load_model(
        model=req.model,
        context_length=req.context_length,
        flash_attention=req.flash_attention,
        offload_kv_cache_to_gpu=req.offload_kv_cache_to_gpu,
        eval_batch_size=req.eval_batch_size,
        num_experts=req.num_experts,
    )


@router.post("/unload")
async def unload_instance(req: UnloadInstanceRequest):
    """Выгрузить конкретный instance модели."""
    return model_control_service.unload_instance(instance_id=req.instance_id)


@router.post("/unload-all")
async def unload_all():
    """Выгрузить все загруженные instance моделей."""
    return model_control_service.unload_all()


@router.get("/server-profiles")
async def get_server_profiles():
    """Список профилей LLM-серверов + какой активен сейчас (по живому config)."""
    return server_profiles.list_profiles()


@router.get("/server-profiles/probe")
async def probe_server_profiles():
    """Health обоих серверов — чтобы видеть, какой жив, ДО переключения."""
    return server_profiles.probe_all()


@router.post("/server-profiles/activate")
async def activate_server_profile(req: ActivateProfileRequest):
    """Переключить весь пайплайн на выбранный сервер: правка .env + рестарт backend.

    ВНИМАНИЕ: рестарт прервёт идущие джобы. Фронтенд предупреждает пользователя.
    """
    try:
        result = server_profiles.apply_profile(req.profile_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    _schedule_backend_restart()
    result["restarting"] = True
    result["message"] = (
        "Профиль применён. Backend перезапускается (~5–10 с) — страница "
        "переподключится автоматически."
    )
    return result
