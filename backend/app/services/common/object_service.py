"""
Сервис управления объектами (строительные объекты).
Каждый объект — это здание/комплекс с набором проектов по дисциплинам.
"""
import json
import os
import stat
import tempfile
import threading
import uuid
from pathlib import Path
from datetime import datetime
from typing import Optional

from backend.app.core.config import OBJECTS_FILE_PATH

OBJECTS_FILE = OBJECTS_FILE_PATH
_REGISTRY_LOCK = threading.RLock()


class ObjectRegistryError(RuntimeError):
    """Реестр объектов существует, но не может быть безопасно прочитан."""


def _validate_objects(data: object) -> dict:
    """Проверить минимальный контракт реестра до его использования/записи."""
    if not isinstance(data, dict):
        raise ObjectRegistryError("objects.json должен содержать JSON-объект")
    objects = data.get("objects")
    if not isinstance(objects, list):
        raise ObjectRegistryError("objects.json: поле 'objects' должно быть списком")

    ids: set[str] = set()
    for index, obj in enumerate(objects):
        if not isinstance(obj, dict):
            raise ObjectRegistryError(
                f"objects.json: objects[{index}] должен быть JSON-объектом"
            )
        for key in ("id", "name", "projects_dir"):
            if not isinstance(obj.get(key), str) or not obj[key].strip():
                raise ObjectRegistryError(
                    f"objects.json: objects[{index}].{key} отсутствует или пуст"
                )
        object_id = obj["id"]
        if object_id in ids:
            raise ObjectRegistryError(
                f"objects.json: повторяющийся object_id '{object_id}'"
            )
        ids.add(object_id)

    current_id = data.get("current_id")
    if current_id is not None and current_id not in ids:
        raise ObjectRegistryError(
            f"objects.json: current_id '{current_id}' отсутствует в objects"
        )
    return data


def _load_objects() -> dict:
    """Загрузить реестр; повреждённый существующий JSON не считать пустым.

    Раньше transient JSONDecodeError во время неатомарной записи превращался в
    пустой список, после чего ``_ensure_default_object`` затирал пять объектов
    одним дефолтным. Отсутствующий файл остаётся допустимым первым запуском, но
    существующий повреждённый файл теперь fail-closed.
    """
    with _REGISTRY_LOCK:
        if not OBJECTS_FILE.exists():
            return {"objects": [], "current_id": None}
        try:
            raw = OBJECTS_FILE.read_text(encoding="utf-8")
        except OSError as exc:
            raise ObjectRegistryError(
                f"Не удалось прочитать реестр объектов {OBJECTS_FILE}: {exc}"
            ) from exc
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ObjectRegistryError(
                f"Повреждён реестр объектов {OBJECTS_FILE}: {exc}"
            ) from exc
        return _validate_objects(data)


def _save_objects(data: dict):
    """Атомарно сохранить реестр в том же каталоге и синхронизировать на диск."""
    with _REGISTRY_LOCK:
        validated = _validate_objects(data)
        target = OBJECTS_FILE
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            mode = stat.S_IMODE(target.stat().st_mode)
        except FileNotFoundError:
            mode = 0o660

        payload = json.dumps(validated, ensure_ascii=False, indent=2) + "\n"
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{target.name}.", suffix=".tmp", dir=str(target.parent)
        )
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(tmp_path, mode)
            os.replace(tmp_path, target)
            try:
                dir_fd = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError:
                # Файл уже заменён атомарно; не все файловые системы разрешают
                # fsync каталога, поэтому это durability best-effort.
                pass
        finally:
            tmp_path.unlink(missing_ok=True)


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


def _load_or_create_default() -> dict:
    """Прочитать/создать реестр внутри одной внутрипроцессной транзакции."""
    with _REGISTRY_LOCK:
        return _ensure_default_object(_load_objects())


def list_objects() -> list[dict]:
    """Список всех объектов."""
    data = _load_or_create_default()
    return data["objects"]


def _bound_object_id_safe() -> Optional[str]:
    """object_id, привязанный к ContextVar (per-request заголовок X-Object-Id
    через CurrentObjectMiddleware либо per-job binding конвейера). None, если
    привязки нет. Fail-soft — при любой ошибке импорта возвращает None."""
    try:
        from backend.app.services.common.project_service import _get_bound_object_id
        return _get_bound_object_id()
    except Exception:
        return None


def get_current_object() -> Optional[dict]:
    """Текущий активный объект.

    Приоритет: объект, привязанный через ContextVar (per-request `X-Object-Id`
    или per-job binding), → глобальный `current_id` из objects.json. Привязка
    учитывается только если резолвится в известный объект — иначе падаем на
    глобальный дефолт (кривой override не должен прятать все проекты)."""
    data = _load_or_create_default()
    objects = data["objects"]
    bound = _bound_object_id_safe()
    target_id = None
    if bound and any(o["id"] == bound for o in objects):
        target_id = bound
    elif data["current_id"]:
        target_id = data["current_id"]
    if target_id:
        for obj in objects:
            if obj["id"] == target_id:
                return obj
    return objects[0] if objects else None


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
    data = _load_or_create_default()
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
    data = _load_or_create_default()
    return [Path(o["projects_dir"]) for o in data["objects"]]


def switch_object(object_id: str) -> dict:
    """Переключиться на другой объект."""
    with _REGISTRY_LOCK:
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
    if not name.strip():
        raise ValueError("Название объекта не может быть пустым")
    with _REGISTRY_LOCK:
        data = _ensure_default_object(_load_objects())
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
    with _REGISTRY_LOCK:
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
    with _REGISTRY_LOCK:
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
