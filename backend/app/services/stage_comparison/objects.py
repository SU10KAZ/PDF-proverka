"""Объекты и пути источников раздела «Сравнение стадий».

Выбор берётся из общего реестра объектов. Источники сравнения хранятся рядом
с обычными данными объекта::

    projects_v2/objects/<object>/comparison/stage_1
    projects_v2/objects/<object>/comparison/stage_2

GET списка ничего не создаёт: физический каркас появляется при первой
загрузке стадии.
"""
from __future__ import annotations

import logging
from pathlib import Path

from backend.app.services.common import object_service

from . import stage_storage
from . import stage_upload

logger = logging.getLogger(__name__)


def _stage_pdf_count(path: Path) -> int:
    try:
        if stage_storage.is_versioned_stage(path):
            return stage_storage.current_document_count(path)
        if path.is_dir():
            return sum(1 for item in path.rglob("*.pdf") if item.is_file())
    except OSError:
        pass
    return 0


def list_objects() -> dict:
    """Все platform-объекты с вычисленными путями comparison/stage_1|2."""
    items: list[dict] = []
    for obj in object_service.list_objects():
        oid = str(obj.get("id") or "")
        if not oid:
            continue
        try:
            _, comparison_dir = stage_upload.resolve_object_dir(oid, create=False)
        except stage_upload.StageUploadError as exc:
            logger.warning("Не удалось определить comparison-путь объекта %s: %s", oid, exc)
            continue
        stage_a = comparison_dir / "stage_1"
        stage_b = comparison_dir / "stage_2"
        stages = [
            {"name": "stage_1", "path": str(stage_a), "pdf_count": _stage_pdf_count(stage_a)},
            {"name": "stage_2", "path": str(stage_b), "pdf_count": _stage_pdf_count(stage_b)},
        ]
        items.append({
            "id": oid,
            "name": str(obj.get("name") or oid),
            "root_path": str(comparison_dir),
            "parent_root": str(comparison_dir.parent),
            "stages": stages,
            "default_stage_a": {"name": "stage_1", "path": str(stage_a)},
            "default_stage_b": {"name": "stage_2", "path": str(stage_b)},
        })
    items.sort(key=lambda item: item["name"].casefold())
    roots = [str((stage_upload._projects_v2_root() / "objects").resolve())]
    return {
        "roots": roots,
        "items": items,
        "count": len(items),
    }


__all__ = ["list_objects"]
