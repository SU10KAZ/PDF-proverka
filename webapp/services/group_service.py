"""
Сервис управления пользовательскими группами проектов.

Группы хранятся per-object per-section в webapp/data/project_groups.json.
Структура: { "object_id": { "section": [group, ...] } }
"""
import json
from pathlib import Path

DATA_FILE = Path(__file__).resolve().parent.parent / "data" / "project_groups.json"

_SENTINEL = "__global__"  # legacy-ключ до введения объектов


def _load_raw() -> dict:
    if not DATA_FILE.exists():
        return {}
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_all(data: dict):
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _object_key(object_id: str | None) -> str:
    return object_id if object_id else _SENTINEL


def load_groups(object_id: str | None = None) -> dict:
    """Загрузить группы объекта. Возвращает {section: [group, ...]}."""
    raw = _load_raw()
    key = _object_key(object_id)
    # Поддержка старого формата (плоский {section: [...]}):
    # если в файле нет ни одного вложенного dict-of-dicts — это старый формат
    if raw and not any(isinstance(v, dict) and any(isinstance(vv, list) for vv in v.values())
                       for v in raw.values() if isinstance(v, dict)):
        # старый плоский формат — мигрируем на лету в __global__
        migrated = {_SENTINEL: raw}
        _save_all(migrated)
        raw = migrated
    return raw.get(key, {})


def save_section_groups(section: str, groups: list, object_id: str | None = None):
    """Перезаписать группы одной секции целиком."""
    raw = _load_raw()
    # миграция старого формата
    load_groups(object_id)  # side-effect: мигрирует файл если нужно
    raw = _load_raw()
    key = _object_key(object_id)
    if key not in raw:
        raw[key] = {}
    raw[key][section] = groups
    _save_all(raw)


def delete_group(section: str, group_id: str, object_id: str | None = None) -> bool:
    """Удалить одну группу. Возвращает True если найдена и удалена."""
    raw = _load_raw()
    key = _object_key(object_id)
    section_groups = raw.get(key, {}).get(section, [])
    before = len(section_groups)
    section_groups = [g for g in section_groups if g.get("id") != group_id]
    if len(section_groups) == before:
        return False
    if key not in raw:
        raw[key] = {}
    raw[key][section] = section_groups
    _save_all(raw)
    return True
