"""
Сервис управления пользовательскими группами проектов.

Группы хранятся per-section в webapp/data/project_groups.json.
"""
import json
from pathlib import Path

DATA_FILE = Path(__file__).resolve().parent.parent / "data" / "project_groups.json"


def load_groups() -> dict:
    """Загрузить все группы. Возвращает {section: [group, ...]}."""
    if not DATA_FILE.exists():
        return {}
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_all(data: dict):
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def save_section_groups(section: str, groups: list):
    """Перезаписать группы одной секции целиком."""
    data = load_groups()
    data[section] = groups
    _save_all(data)


def delete_group(section: str, group_id: str) -> bool:
    """Удалить одну группу. Возвращает True если найдена и удалена."""
    data = load_groups()
    section_groups = data.get(section, [])
    before = len(section_groups)
    section_groups = [g for g in section_groups if g.get("id") != group_id]
    if len(section_groups) == before:
        return False
    data[section] = section_groups
    _save_all(data)
    return True
