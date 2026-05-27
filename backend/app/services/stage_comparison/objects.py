"""Автоопределение «объектов» для раздела «Сравнение стадий».

Объект — это папка верхнего уровня внутри одного из allowlist-root'ов
(`AUDIT_STAGE_COMPARISON_ROOTS`), в которой есть как минимум две
поддиректории, начинающиеся с `stage_` (например `stage_1` и `stage_2`).

UI вместо ручного ввода двух полных путей выбирает «объект» из списка —
backend сразу подставляет `stage_a_path = <root>/<object>/stage_1` и
`stage_b_path = <root>/<object>/stage_2`.

Если объект содержит больше двух стадий (`stage_1`, `stage_2`, `stage_3`…),
все они возвращаются в `stages[]`, а в `default_stage_a`/`default_stage_b`
кладутся первая и последняя (обычно это «старая» и «новая» редакции).
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Iterable

from . import store as store_mod

logger = logging.getLogger(__name__)


# Паттерн «stage_NN» — буква, цифры, латиница, кириллица для гибкости.
_STAGE_DIR_RE = re.compile(r"^stage[_\-\.]?(\w+)$", re.IGNORECASE | re.UNICODE)


def _comparison_roots() -> list[Path]:
    """Список корневых каталогов, в которых ищем объекты.

    Берём `AUDIT_STAGE_COMPARISON_ROOTS` (`;` или `:`-separated). Если
    переменная не задана — fallback на `<project_root>/comparison_sources/`,
    если такая папка существует.
    """
    roots = store_mod._parse_allowlist()
    if roots:
        return [r for r in roots if r.exists() and r.is_dir()]
    # Fallback: дефолтная папка comparison_sources/
    try:
        from backend.app.core.config import ROOT_DIR
        default = (Path(ROOT_DIR) / "comparison_sources").resolve()
        if default.exists() and default.is_dir():
            return [default]
    except Exception:
        pass
    return []


def _list_stage_dirs(object_dir: Path) -> list[tuple[str, Path]]:
    """Все подпапки `stage_*` внутри объекта. Отсортированы по натуральному
    порядку (`stage_1`, `stage_2`, ..., `stage_10`)."""
    out: list[tuple[str, Path]] = []
    try:
        for child in object_dir.iterdir():
            if not child.is_dir():
                continue
            m = _STAGE_DIR_RE.match(child.name)
            if not m:
                continue
            out.append((child.name, child))
    except (PermissionError, OSError):
        return []

    def _key(item):
        name, _ = item
        m = _STAGE_DIR_RE.match(name)
        suffix = m.group(1) if m else name
        # Если числовой суффикс — сортируем по числу
        try:
            return (0, int(suffix))
        except ValueError:
            return (1, suffix.lower())

    out.sort(key=_key)
    return out


def _scan_root(root: Path) -> list[dict]:
    """Найти все «объекты» внутри одного root."""
    objs: list[dict] = []
    try:
        candidates = sorted(root.iterdir(), key=lambda p: p.name.lower())
    except (PermissionError, OSError):
        return []
    for obj_dir in candidates:
        if not obj_dir.is_dir():
            continue
        # Скрытые папки игнорируем
        if obj_dir.name.startswith("."):
            continue
        stages = _list_stage_dirs(obj_dir)
        if len(stages) < 2:
            continue
        stages_payload = [
            {"name": name, "path": str(path)}
            for name, path in stages
        ]
        default_a = stages[0]
        default_b = stages[-1]
        objs.append({
            "id": obj_dir.name,
            "name": obj_dir.name,
            "root_path": str(obj_dir),
            "parent_root": str(root),
            "stages": stages_payload,
            "default_stage_a": {"name": default_a[0], "path": str(default_a[1])},
            "default_stage_b": {"name": default_b[0], "path": str(default_b[1])},
        })
    return objs


def list_objects() -> dict:
    """Все объекты по всем allowlist-root'ам.

    Возвращает:
      {
        "roots": ["/.../comparison_sources", ...],
        "items": [
          {id, name, root_path, parent_root, stages[], default_stage_a, default_stage_b}
        ]
      }
    """
    roots = _comparison_roots()
    items: list[dict] = []
    seen_paths: set[str] = set()
    for root in roots:
        for obj in _scan_root(root):
            if obj["root_path"] in seen_paths:
                continue
            seen_paths.add(obj["root_path"])
            items.append(obj)
    return {
        "roots": [str(r) for r in roots],
        "items": items,
        "count": len(items),
    }


__all__ = ["list_objects"]
