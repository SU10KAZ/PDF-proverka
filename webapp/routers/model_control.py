"""REST API для отдельного окна управления моделями."""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel, Field

from webapp.services import model_control_service


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


@router.get("/status")
async def get_status():
    """Получить текущий статус подключения, моделей и памяти."""
    return model_control_service.get_status()


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
