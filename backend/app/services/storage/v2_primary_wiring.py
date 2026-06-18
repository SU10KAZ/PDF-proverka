"""Шаг 6A/10 — подключение non-destructive write-чокпоинтов к V2_PRIMARY.

Активно ТОЛЬКО при `AUDIT_PROJECTS_V2_WRITE_MODE=projects_v2_primary`. В проде
этот флаг НЕ выставлен (`legacy`), поэтому весь код модуля — мёртвая ветка в
production: чокпоинты вызывают его лишь после явной проверки `v2_is_primary()`.

Семантика v2-primary (наследуется от `StorageWriteFacade._execute`):
  * v2-запись ПЕРВИЧНА — исключение пробрасывается (primary failure НЕ
    маскируется legacy-плечом);
  * legacy — fail-soft архив (выполняется ТОЛЬКО после успешной v2-записи).

Резолв `V2Target` из legacy `project_id` переиспользует проверенные helpers из
`scripts/projects_v2/v2lib.py` (object_id_for / resolve_object_folder), как и
production-миграция, поэтому адрес документа в v2 совпадает с миграционным.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable, Optional

from backend.app.services.storage import v2_primary_prototype as _proto
from backend.app.services.storage.storage_write_facade import (
    StorageWriteFacade,
    V2Target,
)


_V2LIB_CACHE = None


def _load_v2lib():
    """Лениво загрузить scripts/projects_v2/v2lib.py (переиспользует loader фасада).

    Кэшируется на модуль, чтобы не re-exec'ить v2lib на каждый вызов (loader
    фасада кэширует на инстанс, а инстанс здесь одноразовый)."""
    global _V2LIB_CACHE
    if _V2LIB_CACHE is None:
        _V2LIB_CACHE = StorageWriteFacade()._load_v2lib()
    return _V2LIB_CACHE


def _root_entry(legacy_project_dir: Path) -> Path:
    """Верхнеуровневая запись проекта: контейнер `(main)` или сам проект —
    ровно то, что `migrate_project` ожидает как `project_path`."""
    p = Path(legacy_project_dir).resolve()
    try:
        from backend.app.services.common.version_service import container_dir_for
        c = container_dir_for(p)
        if c is not None:
            return Path(c).resolve()
    except Exception:
        pass
    parent = p.parent
    if parent.name.endswith("(main)") and (parent / "version_group.json").exists():
        return parent
    return p


def resolve_v2_target(
    legacy_project_dir: Path,
    version_id: str,
    *,
    v2_root: Path,
    objects_map: Optional[dict] = None,
) -> Optional[V2Target]:
    """Построить `V2Target` ТЕМ ЖЕ способом, что и `v2lib.migrate_project`,
    чтобы адрес документа в v2 совпадал с миграционным (никакого
    path-divergence): discipline = имя папки-раздела, document_code =
    `document_code_for(root_entry)` (снимает `.pdf`, читает контейнерный
    `logical_project_id`), object_folder = `allocate_object_folder(...)`.

    Возвращает None, если раздел/код документа не извлекаются — в v2-primary
    это surfaced failure (а НЕ молчаливый откат в legacy).
    """
    v2lib = _load_v2lib()
    root_entry = _root_entry(legacy_project_dir)

    parent = root_entry.parent
    object_dir = parent.parent
    # слишком мелкий путь (нет уровня объект/раздел) → не резолвится
    if object_dir == object_dir.parent:
        return None

    discipline = parent.name
    if not discipline:
        return None

    if objects_map is None:
        # objects.json лежит в <DATA>/backend/app/data; <DATA> = parent от projects_v2
        objects_map = v2lib.load_objects_map(root=Path(v2_root).parent)
    object_id = v2lib.object_id_for(object_dir, objects_map)
    object_folder = v2lib.allocate_object_folder(
        Path(v2_root), object_id, object_dir.name,
    ).name
    document_code = (v2lib.document_code_for(root_entry) or "").strip()
    if not document_code:
        return None

    return V2Target(
        object_folder=object_folder,
        discipline=v2lib.safe_component(discipline),
        document_code=v2lib.safe_component(document_code),
        version_id=version_id or "v001",
    )


def save_project_info_v2_primary(
    project_id: str,
    data: dict,
    *,
    version_id: str,
    legacy_root: Path,
    legacy_path: Path,
    v2_root: Optional[Path] = None,
) -> bool:
    """v2-primary запись project_info (вызывается ТОЛЬКО при v2_is_primary()).

    Пишет project_info как version.json-метаданные в projects_v2 (первично);
    legacy project_info.json — fail-soft архив. Возвращает True только если
    v2-запись удалась. Любой сбой резолва/записи v2 → False (не маскируется).
    """
    facade = StorageWriteFacade(v2_root=v2_root) if v2_root is not None else StorageWriteFacade()
    resolved_root = facade.v2_root()
    if resolved_root is None:
        return False

    target = resolve_v2_target(
        Path(legacy_root), version_id, v2_root=resolved_root,
    )
    if target is None:
        # surfaced failure: в v2-primary мы НЕ откатываемся молча в legacy
        return False

    def _legacy_archive() -> Any:
        # fail-soft архив: выполняется только после успешной v2-записи (_execute)
        with open(legacy_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True

    try:
        res = _proto.write_project_metadata_v2(
            facade, target, data, legacy_write=_legacy_archive,
        )
    except Exception:
        # v2 primary write упал — НЕ маскируем legacy-плечом
        return False
    return bool(res.v2_ok)
