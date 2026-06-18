#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
repair_v2_shadow_rename.py — одноразовая синхронизация projects_v2-shadow для
проектов, уже переименованных в legacy ДО фикса `sync_v2_shadow_rename`.

Контекст: до фикса `rename_project` обновлял legacy + JSON-сторы, но не трогал
`projects_v2/.../documents/<doc>/document.json`. Production читает list/resolve
из v2, поэтому UI продолжал показывать старое имя. Этот скрипт приводит v2-shadow
в соответствие текущему legacy source of truth.

Что делает (через `project_rename_service.sync_v2_shadow_rename`):
  * находит v2-документ по СТАРОЙ идентичности в рамках object_id;
  * обновляет `document_code` / `legacy_project_name` / `legacy_project_path` /
    `versions[].legacy_folder_name`;
  * при необходимости переименовывает папку документа в новый код;
  * делает backup `document.json` (`*.json.rename_bak`) перед записью;
  * НЕ трогает artifacts/01_input/анализ и соседние документы.

Безопасность: по умолчанию `--dry-run` (ничего не пишет). Реальный прогон —
только с `--apply`. Имена можно задать явно или взять из reverse-лога
переименования (`project_rename.reverse.json`).

Usage:
  # план для двух уже затронутых проектов (ничего не меняет):
  python backend/scripts/repair_v2_shadow_rename.py \
      --object-id 73a0e59a \
      --map "13АВ-РД-АР1.1-К7 V1=13АВ-РД-АР1.1-К7" \
      --map "13АВ-РД-ЭМ-К1 V1=13АВ-РД-ЭМ-К1" --dry-run

  # реальный прогон:
  python backend/scripts/repair_v2_shadow_rename.py --object-id 73a0e59a \
      --map "13АВ-РД-АР1.1-К7 V1=13АВ-РД-АР1.1-К7" \
      --map "13АВ-РД-ЭМ-К1 V1=13АВ-РД-ЭМ-К1" --apply
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.services.common import project_rename_service as prs  # noqa: E402


def _parse_map(items: list[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for it in items or []:
        if "=" not in it:
            raise SystemExit(f"--map ожидает формат 'old=new', получено: {it!r}")
        old, new = it.split("=", 1)
        old, new = old.strip(), new.strip()
        if not old or not new:
            raise SystemExit(f"--map: пустое old/new в {it!r}")
        out.append((old, new))
    return out


def _from_reverse_log(path: Path) -> list[tuple[str, str]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    old = data.get("old_base")
    new = data.get("new_base")
    if not old or not new:
        raise SystemExit(f"reverse-log {path}: нет old_base/new_base")
    return [(old, new)]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--object-id", default=None,
                    help="ограничить поиск v2-документов этим object_id")
    ap.add_argument("--map", action="append", default=[],
                    help="пара 'old_name=new_name' (можно несколько)")
    ap.add_argument("--from-reverse-log", default=None,
                    help="взять old/new из project_rename.reverse.json")
    ap.add_argument("--v2-root", default=None, help="override projects_v2 root")
    ap.add_argument("--apply", action="store_true", help="применить (иначе dry-run)")
    args = ap.parse_args()

    pairs = _parse_map(args.map)
    if args.from_reverse_log:
        pairs += _from_reverse_log(Path(args.from_reverse_log))
    if not pairs:
        raise SystemExit("нужно указать хотя бы один --map или --from-reverse-log")

    v2_root = Path(args.v2_root) if args.v2_root else None
    dry_run = not args.apply
    print(f"== repair v2-shadow == object_id={args.object_id} "
          f"mode={'DRY-RUN' if dry_run else 'APPLY'}")
    any_change = False
    for old, new in pairs:
        res = prs.sync_v2_shadow_rename(
            old, new, object_id=args.object_id, v2_root=v2_root,
            backup=True, dry_run=dry_run,
        )
        print(f"\n[{old!r} -> {new!r}]")
        print(f"  updated:      {res['updated']}")
        print(f"  renamed_dirs: {res['renamed_dirs']}")
        print(f"  fields:       {res['fields']}")
        if res["warnings"]:
            print(f"  warnings:     {res['warnings']}")
        if res["updated"] or res["renamed_dirs"]:
            any_change = True

    if dry_run:
        print("\n(dry-run — ничего не изменено; добавьте --apply для записи)")
    elif not any_change:
        print("\n(нечего менять — v2-shadow уже консистентен)")


if __name__ == "__main__":
    main()
