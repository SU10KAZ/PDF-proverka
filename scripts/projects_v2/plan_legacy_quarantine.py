#!/usr/bin/env python3
"""
plan_legacy_quarantine.py — DRY-RUN планировщик карантина legacy `projects/`.

Шаг 9/10 миграции projects_v2. Скрипт НИЧЕГО не удаляет, не переименовывает и
не перемещает. Он только:

  * проверяет, что `projects/` существует;
  * считает размер и число файлов;
  * строит manifest (список файлов) — пишет ТОЛЬКО в `--output` (default /tmp);
  * печатает предлагаемые будущие команды (backup + `mv` в archive);
  * НЕ имеет `--execute`: реальный карантин выполняется человеком вручную после
    deletion-checklist и периода наблюдения (см.
    docs/projects_v2_legacy_quarantine_plan и _deletion_checklist).

КАРАНТИН != УДАЛЕНИЕ. Сначала `mv projects projects_legacy_archive_<date>`,
наблюдение, и только потом (отдельно, после checklist) — удаление архива.

Использование:
  python scripts/projects_v2/plan_legacy_quarantine.py
  python scripts/projects_v2/plan_legacy_quarantine.py --projects-root <path> --output /tmp/plan.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def scan_projects(projects_root: Path) -> dict:
    """READ-ONLY обход: число файлов, суммарный размер, top-level entries."""
    total_files = 0
    total_bytes = 0
    manifest: list[str] = []
    for dp, _dns, fns in os.walk(projects_root):
        for fn in fns:
            p = Path(dp) / fn
            try:
                total_bytes += p.stat().st_size
            except OSError:
                pass
            total_files += 1
            manifest.append(os.path.relpath(str(p), str(projects_root)))
    top = sorted(e.name for e in projects_root.iterdir()) if projects_root.is_dir() else []
    return {
        "projects_root": str(projects_root),
        "exists": projects_root.is_dir(),
        "top_level_entries": top,
        "total_files": total_files,
        "total_bytes": total_bytes,
        "total_mb": round(total_bytes / (1024 * 1024), 1),
        "manifest_count": len(manifest),
        "_manifest": manifest,
    }


def build_plan(projects_root: Path, *, date_token: str) -> dict:
    info = scan_projects(projects_root)
    archive_name = f"projects_legacy_archive_{date_token}"
    backup_tar = f"/backup/projects_legacy_{date_token}.tar.gz"
    info["proposed"] = {
        "backup_command": (
            f"tar -czf {backup_tar} -C {projects_root.parent} {projects_root.name}"
        ),
        "manifest_command": (
            f"find {projects_root.name} -type f | sort > "
            f"projects_legacy_manifest_{date_token}.txt"
        ),
        "future_quarantine_command": (
            f"mv {projects_root.name} {archive_name}   # ВРУЧНУЮ, после checklist"
        ),
        "archive_name": archive_name,
        "note": "DRY-RUN: скрипт ничего не выполняет. --execute отсутствует намеренно.",
    }
    return info


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="DRY-RUN планировщик карантина legacy projects/")
    ap.add_argument("--projects-root", default=None,
                    help="legacy projects/ (default: <repo>/projects)")
    ap.add_argument("--output", default=None,
                    help="куда писать manifest+plan JSON (default: только stdout-summary)")
    ap.add_argument("--date-token", default="YYYYMMDD",
                    help="токен даты для имён (Date.now недоступен в скрипте; передать снаружи)")
    # --execute намеренно НЕ объявлен. Любая попытка передать его → argparse error.
    args = ap.parse_args(argv)

    projects_root = (Path(args.projects_root).resolve() if args.projects_root
                     else _repo_root() / "projects")
    if not projects_root.is_dir():
        print(f"[ERROR] projects root not found: {projects_root}", file=sys.stderr)
        return 2

    plan = build_plan(projects_root, date_token=args.date_token)

    if args.output:
        out = Path(args.output).resolve()
        # guard: НИКОГДА не писать внутрь самого projects_root
        if out == projects_root or projects_root.resolve() in out.parents:
            print(f"[ERROR] отказ писать внутрь projects/: {out}", file=sys.stderr)
            return 2
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w", encoding="utf-8") as f:
            json.dump(plan, f, ensure_ascii=False, indent=2)
        print(f"[OK] plan+manifest -> {out}")

    p = plan["proposed"]
    print("=== legacy quarantine plan (DRY-RUN, ничего не выполнено) ===")
    print(f"projects_root : {plan['projects_root']}")
    print(f"files         : {plan['total_files']}")
    print(f"size          : {plan['total_mb']} MB")
    print(f"top entries   : {len(plan['top_level_entries'])}")
    print("\nПредлагаемые ВРУЧНУЮ-команды (НЕ выполнены):")
    print(f"  1) backup:    {p['backup_command']}")
    print(f"  2) manifest:  {p['manifest_command']}")
    print(f"  3) quarantine:{p['future_quarantine_command']}")
    print("\nКарантин != удаление. Удалять архив — только после deletion-checklist.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
