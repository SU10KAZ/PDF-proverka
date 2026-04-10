"""
REST API для управления объектами (строительные объекты).
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from webapp.services import object_service

router = APIRouter(prefix="/api/objects", tags=["objects"])


class AddObjectRequest(BaseModel):
    name: str
    projects_dir: Optional[str] = None


class UpdateObjectRequest(BaseModel):
    name: Optional[str] = None


class SwitchObjectRequest(BaseModel):
    id: str


@router.get("")
async def list_objects():
    """Список всех объектов."""
    objects = object_service.list_objects()
    current = object_service.get_current_object()
    return {
        "objects": objects,
        "current_id": current["id"] if current else None,
    }


@router.post("")
async def add_object(req: AddObjectRequest):
    """Добавить новый объект."""
    try:
        obj = object_service.add_object(req.name, req.projects_dir)
        return {"status": "ok", "object": obj}
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/switch")
async def switch_object(req: SwitchObjectRequest):
    """Переключиться на другой объект."""
    try:
        obj = object_service.switch_object(req.id)
        return {"status": "ok", "object": obj}
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.put("/{object_id}")
async def update_object(object_id: str, req: UpdateObjectRequest):
    """Обновить название объекта."""
    try:
        obj = object_service.update_object(object_id, req.name)
        return {"status": "ok", "object": obj}
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.delete("/{object_id}")
async def delete_object(object_id: str):
    """Удалить объект (файлы проектов не удаляются)."""
    try:
        object_service.delete_object(object_id)
        return {"status": "ok"}
    except ValueError as e:
        raise HTTPException(400, str(e))
