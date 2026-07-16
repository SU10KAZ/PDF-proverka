"""
Сервис управления объектами (строительные объекты).
Каждый объект — это здание/комплекс с набором проектов по дисциплинам.
"""
import json
import uuid
from pathlib import Path
from datetime import datetime
from typing import Optional

from backend.app.core.config import OBJECTS_FILE_PATH

OBJECTS_FILE = OBJECTS_FILE_PATH


def _load_objects() -> dict:
    """Загрузить список объектов из JSON."""
    if not OBJECTS_FILE.exists():
        return {"objects": [], "current_id": None}
    try:
        data = json.loads(OBJECTS_FILE.read_text(encoding="utf-8"))
        return data
    except (json.JSONDecodeError, KeyError):
        return {"objects": [], "current_id": None}


def _save_objects(data: dict):
    """Сохранить список объектов."""
    OBJECTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    OBJECTS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _ensure_default_object(data: dict) -> dict:
    """Если объектов нет — создать дефолтный из config.OBJECT_NAME."""
    if data["objects"]:
        return data
    from backend.app.core.config import OBJECT_NAME, PROJECTS_DIR
    default_id = str(uuid.uuid4())[:8]
    data["objects"].append({
        "id": default_id,
        "name": OBJECT_NAME,
        "projects_dir": str(PROJECTS_DIR),
        "created_at": datetime.now().isoformat(),
    })
    data["current_id"] = default_id
    _save_objects(data)
    return data


def list_objects() -> list[dict]:
    """Список всех объектов."""
    data = _ensure_default_object(_load_objects())
    return data["objects"]


def get_current_object() -> Optional[dict]:
    """Текущий активный объект."""
    data = _ensure_default_object(_load_objects())
    if not data["current_id"]:
        return data["objects"][0] if data["objects"] else None
    for obj in data["objects"]:
        if obj["id"] == data["current_id"]:
            return obj
    return data["objects"][0] if data["objects"] else None


def get_current_id() -> Optional[str]:
    """ID текущего объекта."""
    obj = get_current_object()
    return obj["id"] if obj else None


def get_current_projects_dir() -> Path:
    """Папка проектов текущего объекта."""
    obj = get_current_object()
    if obj:
        return Path(obj["projects_dir"])
    from backend.app.core.config import PROJECTS_DIR
    return PROJECTS_DIR


def get_object_by_id(object_id: Optional[str]) -> Optional[dict]:
    """Вернуть объект по id (не читает current_id).

    Нужен для pipeline, который должен резолвить пути независимо от
    переключения текущего объекта.
    """
    if not object_id:
        return None
    data = _ensure_default_object(_load_objects())
    for obj in data["objects"]:
        if obj["id"] == object_id:
            return obj
    return None


def get_projects_dir_for(object_id: Optional[str]) -> Optional[Path]:
    """projects_dir конкретного объекта; None если объект не найден."""
    obj = get_object_by_id(object_id)
    return Path(obj["projects_dir"]) if obj else None


def list_projects_dirs() -> list[Path]:
    """projects_dir всех объектов (для ambiguity-детектора)."""
    data = _ensure_default_object(_load_objects())
    return [Path(o["projects_dir"]) for o in data["objects"]]


def switch_object(object_id: str) -> dict:
    """Переключиться на другой объект."""
    data = _ensure_default_object(_load_objects())
    found = None
    for obj in data["objects"]:
        if obj["id"] == object_id:
            found = obj
            break
    if not found:
        raise ValueError(f"Объект с ID '{object_id}' не найден")
    data["current_id"] = object_id
    _save_objects(data)
    # Сбросить кеш project_service
    _invalidate_project_cache()
    return found


def _create_v2_object_scaffold(obj: dict) -> Optional[Path]:
    """Create the on-disk object skeleton when projects_v2 is primary.

    ``projects_dir`` remains the object's logical legacy path.  The upload
    service uses its basename while staging a new bundle, but in v2-primary
    mode the path itself must not be created under ``projects/``.
    """
    from backend.app.services.storage.storage_write_facade import (
        get_write_facade,
        v2_is_primary,
    )

    if not v2_is_primary():
        return None

    facade = get_write_facade()
    v2_root = facade.v2_root()
    if v2_root is None:
        raise RuntimeError("Не удалось определить папку projects_v2")

    # Use the same allocator as migration/upload so object folder names and
    # collision handling stay identical across all v2 write paths.
    v2lib = facade._load_v2lib()
    object_dir = v2lib.allocate_object_folder(
        Path(v2_root), obj["id"], obj["name"],
    )
    object_dir.mkdir(parents=True, exist_ok=True)

    object_json = object_dir / "object.json"
    if object_json.exists():
        try:
            existing_id = str(
                json.loads(object_json.read_text(encoding="utf-8")).get("object_id") or ""
            )
        except (OSError, json.JSONDecodeError):
            existing_id = ""
        if existing_id != obj["id"]:
            raise RuntimeError(f"Папка объекта уже занята: {object_dir}")
    else:
        metadata = {
            "schema_version": 1,
            "object_id": obj["id"],
            "legacy_name": obj["name"],
            "legacy_path": obj["projects_dir"],
            "created_at": obj["created_at"],
            "display_name": obj["name"],
            "folder_name": object_dir.name,
        }
        object_json.write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    # DOC is retained for object-level source documents.  Project bundles are
    # uploaded below their discipline's documents directory.
    (object_dir / "DOC").mkdir(exist_ok=True)
    from backend.app.services.common.discipline_service import get_supported_codes
    for code in get_supported_codes():
        (object_dir / "disciplines" / code / "documents").mkdir(
            parents=True, exist_ok=True,
        )
    return object_dir


def add_object(name: str, projects_dir: Optional[str] = None) -> dict:
    """Добавить новый объект."""
    data = _ensure_default_object(_load_objects())
    if not name.strip():
        raise ValueError("Название объекта не может быть пустым")
    from backend.app.core.config import PROJECTS_DIR
    explicit_projects_dir = bool(projects_dir)
    if projects_dir:
        proj_dir = Path(projects_dir)
    else:
        proj_dir = PROJECTS_DIR / name.strip()
    created_at = datetime.now().isoformat()
    new_obj = {
        "id": str(uuid.uuid4())[:8],
        "name": name.strip(),
        "projects_dir": str(proj_dir),
        "created_at": created_at,
    }

    v2_object_dir = None
    if not explicit_projects_dir:
        v2_object_dir = _create_v2_object_scaffold(new_obj)
    if v2_object_dir is None:
        # Legacy mode and explicitly requested custom paths keep the existing
        # behaviour.
        proj_dir.mkdir(parents=True, exist_ok=True)

    data["objects"].append(new_obj)
    _save_objects(data)
    return new_obj


def update_object(object_id: str, name: Optional[str] = None) -> dict:
    """Обновить название объекта."""
    data = _load_objects()
    for obj in data["objects"]:
        if obj["id"] == object_id:
            if name is not None:
                obj["name"] = name.strip()
            _save_objects(data)
            return obj
    raise ValueError(f"Объект с ID '{object_id}' не найден")


def delete_object(object_id: str):
    """Удалить объект (не удаляет файлы проектов)."""
    data = _load_objects()
    data["objects"] = [o for o in data["objects"] if o["id"] != object_id]
    if data["current_id"] == object_id:
        data["current_id"] = data["objects"][0]["id"] if data["objects"] else None
    _save_objects(data)


def _invalidate_project_cache():
    """Сбросить кеш проектов при смене объекта."""
    import backend.app.services.common.project_service as project_service
    project_service._PROJECT_DIRS_CACHE.clear()
    project_service._PROJECT_DIRS_CACHE_TIME = 0.0
