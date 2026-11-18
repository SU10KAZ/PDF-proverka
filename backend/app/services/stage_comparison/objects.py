"""Объекты и пути источников раздела «Сравнение стадий».

Выбор берётся из общего реестра объектов. Источники сравнения хранятся рядом
с обычными данными объекта::

    projects_v2/objects/<object>/comparison/stage_1
    projects_v2/objects/<object>/comparison/stage_2

У других версий сравнения — ``comparison/versions/cvNNN/stage_N``
(``comparison_versions``); ``list_objects`` отдаёт стадии выбранной версии.

GET списка ничего не создаёт: физический каркас появляется при первой
загрузке стадии.
"""
from __future__ import annotations

import logging
from pathlib import Path

from backend.app.services.common import object_service
from backend.app.services.project_change_preview.service import OBJECT as SNAPSHOT_VIEWER_OBJECT

from . import comparison_versions
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


def list_objects(object_id: str | None = None, comparison_version: str | None = None) -> dict:
    """Все platform-объекты с вычисленными путями comparison/stage_1|2.

    ``comparison_version`` — стадии этой версии сравнения (без него — Версии 1);
    неизвестная версия — ``ComparisonVersionError`` (404).
    """
    # Версия выбирается только у одного объекта; общий список — всегда Версия 1.
    version = comparison_versions.normalize(comparison_version if object_id is not None else None)
    items: list[dict] = []
    for obj in object_service.list_objects():
        oid = str(obj.get("id") or "")
        if not oid or (object_id is not None and oid != object_id):
            continue
        try:
            _, legacy_dir = stage_upload.resolve_object_dir(oid, create=False)
        except stage_upload.StageUploadError as exc:
            logger.warning("Не удалось определить comparison-путь объекта %s: %s", oid, exc)
            continue
        comparison_dir = comparison_versions.workspace_from_legacy(legacy_dir, version)
        scope = comparison_versions.scope_id(oid, version)
        stage_a = comparison_dir / "stage_1"
        stage_b = comparison_dir / "stage_2"
        stages = [
            {"name": "stage_1", "path": str(stage_a), "pdf_count": _stage_pdf_count(stage_a)},
            {"name": "stage_2", "path": str(stage_b), "pdf_count": _stage_pdf_count(stage_b)},
        ]
        from backend.app.services.project_assemblies import service as assemblies
        for stage in stages:
            stage["assembly_count"] = assemblies.attached_count(scope, stage["name"])
            stage["pdf_count"] += stage["assembly_count"]
        items.append({
            "id": oid,
            "name": str(obj.get("name") or oid),
            # Only the sealed preview needs an object-wide presentation to bind
            # its viewer. Ordinary pairs load result cards when opened.
            "requires_snapshot_viewer": oid == SNAPSHOT_VIEWER_OBJECT,
            "comparison_version": version,
            "root_path": str(comparison_dir),
            "parent_root": str(legacy_dir.parent),
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
