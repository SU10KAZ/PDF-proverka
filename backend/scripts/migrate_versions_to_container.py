#!/usr/bin/env python3
"""Миграция legacy `_versions/v{N}/` → контейнерная раскладка `<база>(main)/`.

Старая модель:
    <discipline>/<база>/                      ← V1 (корень)
        project_versions.json
        _versions/v2/                          ← V2
        _versions/v3/                          ← V3 ...

Новая модель:
    <discipline>/<база>(main)/                 ← контейнер
        version_group.json
        <база>/                                ← V1 (basename сохранён)
        <база> V2/                             ← V2 (братская папка)
        <база> V3/ ...

Гарантии:
- `basename` папки V1 НЕ меняется → `project_id` стабилен (замечания/обсуждения/
  реестры/стоимость не переписываются);
- содержимое каждой версии (включая `_output/`) переносится целиком;
- идемпотентно: проект, уже лежащий в контейнере, пропускается;
- `--dry-run` печатает план перемещений без изменений на диске;
- лог фактических перемещений пишется в `--log` (по умолчанию рядом со скриптом).

Использование:
    python backend/scripts/migrate_versions_to_container.py --dry-run
    python backend/scripts/migrate_versions_to_container.py            # реальный прогон
    python backend/scripts/migrate_versions_to_container.py --projects-dir /path
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

# Запуск как скрипт: добавить корень репозитория в sys.path.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.app.core.config import PROJECTS_DIR  # noqa: E402
from backend.app.services.common import version_service as vs  # noqa: E402

LEGACY_MANIFEST = "project_versions.json"
LEGACY_VERSIONS_DIR = "_versions"


def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def find_legacy_projects(projects_dir: Path) -> list[Path]:
    """Все папки проектов, содержащие legacy `_versions/`."""
    found: list[Path] = []
    for versions_dir in projects_dir.rglob(LEGACY_VERSIONS_DIR):
        if not versions_dir.is_dir():
            continue
        project_root = versions_dir.parent
        # Пропускаем уже мигрированные (в контейнере).
        if vs.container_dir_for(project_root) is not None:
            continue
        found.append(project_root)
    return sorted(set(found))


def _read_legacy_manifest(project_root: Path) -> Optional[dict[str, Any]]:
    path = project_root / LEGACY_MANIFEST
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def plan_project(project_root: Path) -> Optional[dict[str, Any]]:
    """Построить план миграции одного проекта (без изменений на диске)."""
    base_name = project_root.name
    manifest = _read_legacy_manifest(project_root)
    if manifest is None:
        return None

    container = project_root.parent / vs.container_name_for(base_name)

    versions_out: list[dict[str, Any]] = []
    moves: list[tuple[Path, Path]] = []

    for v in manifest.get("versions", []):
        vid = str(v.get("version_id") or "")
        folder = str(v.get("folder") or ".")
        no = int(v.get("version_no") or (1 if vid == "v1" else 0))
        label = v.get("label") or (f"V{no}" if no else vid.upper())

        if folder in (".", ""):
            # V1 — сам project_root → container/<base_name>
            new_folder = base_name
            src = project_root
        else:
            # V2+ — _versions/v{N} → container/<base> V{N}
            new_folder = f"{base_name} V{no}" if no else f"{base_name} {vid}"
            src = project_root / folder
            moves.append((src, container / new_folder))

        entry = {k: v[k] for k in v if k not in {"folder"}}
        entry["folder"] = new_folder
        versions_out.append(entry)

    group_manifest = {
        "schema_version": vs.SCHEMA_VERSION,
        "logical_project_id": manifest.get("logical_project_id", base_name),
        "container": container.name,
        "primary_version_id": "v1",
        "latest_version_id": manifest.get("latest_version_id", "v1"),
        "versions": versions_out,
    }

    return {
        "project_root": project_root,
        "container": container,
        "base_name": base_name,
        "version_moves": moves,             # _versions/v{N} → container/<name>
        "group_manifest": group_manifest,
    }


def apply_plan(plan: dict[str, Any], log: list[dict[str, Any]]) -> None:
    """Выполнить план: переместить версии, V1, записать version_group.json."""
    project_root: Path = plan["project_root"]
    container: Path = plan["container"]
    base_name: str = plan["base_name"]

    if container.exists():
        raise FileExistsError(f"Контейнер уже существует: {container}")

    container.mkdir(parents=True, exist_ok=False)

    # 1. Перенести старшие версии (_versions/v{N}) в братские папки.
    for src, dest in plan["version_moves"]:
        if not src.exists():
            log.append({"warn": f"источник версии не найден: {src}"})
            continue
        shutil.move(str(src), str(dest))
        log.append({"move": [str(src), str(dest)]})

    # 2. Подчистить остатки в project_root: пустой _versions/ и legacy-манифест.
    legacy_versions = project_root / LEGACY_VERSIONS_DIR
    if legacy_versions.exists():
        leftovers = [p for p in legacy_versions.iterdir()]
        if leftovers:
            # Непредвиденные папки внутри _versions → перенесём как есть.
            for p in leftovers:
                dest = container / f"{base_name} {p.name}"
                shutil.move(str(p), str(dest))
                log.append({"move_leftover": [str(p), str(dest)]})
        legacy_versions.rmdir()
        log.append({"rmdir": str(legacy_versions)})
    legacy_manifest = project_root / LEGACY_MANIFEST
    if legacy_manifest.exists():
        legacy_manifest.unlink()
        log.append({"unlink": str(legacy_manifest)})

    # 3. Переместить V1 (project_root) внутрь контейнера под родным именем.
    primary_dest = container / base_name
    shutil.move(str(project_root), str(primary_dest))
    log.append({"move_v1": [str(project_root), str(primary_dest)]})

    # 4. Записать version_group.json.
    with open(container / vs.GROUP_MANIFEST_FILENAME, "w", encoding="utf-8") as f:
        json.dump(plan["group_manifest"], f, ensure_ascii=False, indent=2)
    log.append({"write_manifest": str(container / vs.GROUP_MANIFEST_FILENAME)})


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--projects-dir", default=str(PROJECTS_DIR),
                    help="Корень projects/ (по умолчанию из config)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Только показать план, ничего не менять")
    ap.add_argument("--log", default=str(Path(__file__).with_name(
        "migrate_versions_to_container.last_run.json")),
        help="Куда писать лог фактических перемещений")
    args = ap.parse_args()

    projects_dir = Path(args.projects_dir).expanduser().resolve()
    if not projects_dir.exists():
        print(f"projects-dir не найден: {projects_dir}", file=sys.stderr)
        return 2

    legacy = find_legacy_projects(projects_dir)
    print(f"Найдено legacy-проектов с _versions/: {len(legacy)}")
    if not legacy:
        return 0

    run_log: list[dict[str, Any]] = [{"started_at": _now_iso(),
                                      "dry_run": args.dry_run}]
    migrated = 0
    skipped = 0
    for project_root in legacy:
        plan = plan_project(project_root)
        rel = project_root.relative_to(projects_dir)
        if plan is None:
            print(f"  ПРОПУСК (нет {LEGACY_MANIFEST}): {rel}")
            skipped += 1
            continue
        if plan["container"].exists():
            print(f"  ПРОПУСК (контейнер уже есть): {rel}")
            skipped += 1
            continue

        print(f"\n● {rel}")
        print(f"    → контейнер: {plan['container'].name}")
        print(f"    → V1: {project_root.name}/ → {plan['container'].name}/{plan['base_name']}/")
        for src, dest in plan["version_moves"]:
            print(f"    → версия: {src.relative_to(project_root)} → "
                  f"{plan['container'].name}/{dest.name}")

        if args.dry_run:
            continue
        try:
            apply_plan(plan, run_log)
            migrated += 1
        except Exception as e:  # noqa: BLE001
            print(f"    ОШИБКА: {e}", file=sys.stderr)
            run_log.append({"error": f"{rel}: {e}"})

    run_log.append({"finished_at": _now_iso(),
                    "migrated": migrated, "skipped": skipped})

    if not args.dry_run:
        try:
            from backend.app.services.common.project_service import invalidate_project_cache
            invalidate_project_cache()
        except Exception:
            pass
        with open(args.log, "w", encoding="utf-8") as f:
            json.dump(run_log, f, ensure_ascii=False, indent=2)
        print(f"\nМигрировано: {migrated}, пропущено: {skipped}. Лог: {args.log}")
    else:
        print(f"\n[dry-run] к миграции: {len(legacy) - skipped}, пропущено: {skipped}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
