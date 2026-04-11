"""
Сервис управления объектами (строительные объекты).
Каждый объект — это здание/комплекс с набором проектов по дисциплинам.
"""
import json
import uuid
from pathlib import Path
from datetime import datetime
from typing import Optional

from webapp.config import BASE_DIR

OBJECTS_FILE = Path(__file__).resolve().parent.parent / "data" / "objects.json"


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
    from webapp.config import OBJECT_NAME, PROJECTS_DIR
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
    from webapp.config import PROJECTS_DIR
    return PROJECTS_DIR


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


def add_object(name: str, projects_dir: Optional[str] = None) -> dict:
    """Добавить новый объект."""
    data = _ensure_default_object(_load_objects())
    if not name.strip():
        raise ValueError("Название объекта не может быть пустым")
    # Создать папку для проектов
    if projects_dir:
        proj_dir = Path(projects_dir)
    else:
        # Создаём подпапку в projects_root/
        safe_name = name.strip().replace(" ", "_").replace('"', '').replace("'", "")
        proj_dir = BASE_DIR / "projects_objects" / safe_name
    proj_dir.mkdir(parents=True, exist_ok=True)
    new_obj = {
        "id": str(uuid.uuid4())[:8],
        "name": name.strip(),
        "projects_dir": str(proj_dir),
        "created_at": datetime.now().isoformat(),
    }
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
    from webapp.services import project_service
    project_service._PROJECT_DIRS_CACHE.clear()
    project_service._PROJECT_DIRS_CACHE_TIME = 0.0
