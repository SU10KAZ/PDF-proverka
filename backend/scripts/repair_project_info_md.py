#!/usr/bin/env python3
"""
repair_project_info_md.py — dry-run диагностика и БЕЗОПАСНЫЙ ремонт project_info.json
(только поле md_file) для проектов с проблемами разрешения MD.

ПО УМОЛЧАНИЮ — DRY-RUN: ничего не меняет, только печатает план.
`--apply` применяет ТОЛЬКО проставление md_file (с backup project_info.json).
pdf_file (указывающий на другой проект) — только репорт, авто-фикс НЕ выполняется.
НИКОГДА не трогает артефакты 01/02/03/04 и сами MD/PDF файлы.

Примеры:
    python backend/scripts/repair_project_info_md.py "13АВ-РД-АР1.2-К6"
    python backend/scripts/repair_project_info_md.py PID1 PID2 --json
    python backend/scripts/repair_project_info_md.py "13АВ-РД-АР1.2-К6" --apply
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.services.common.project_service import resolve_project_dir  # noqa: E402
from backend.app.services.common import version_service as vs  # noqa: E402
from backend.app.services.common.md_resolver import plan_project_info_repair  # noqa: E402


def _resolve_dirs(pid: str):
    root_dir = resolve_project_dir(pid)
    latest_vid = None
    version_dir = root_dir
    try:
        latest_vid = vs.get_latest_version_id(root_dir, pid)
    except Exception:
        latest_vid = None
    try:
        version_dir = vs.get_version_dir(root_dir, pid, None)  # None → latest
    except Exception:
        version_dir = root_dir
    return root_dir, version_dir, latest_vid


def _load_info(version_dir: Path) -> dict:
    p = version_dir / "project_info.json"
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def plan_for(pid: str):
    root_dir, version_dir, latest_vid = _resolve_dirs(pid)
    info = _load_info(version_dir)
    plan = plan_project_info_repair(
        version_dir, pid, info, root_dir=root_dir, latest_version_id=latest_vid,
    )
    return root_dir, version_dir, latest_vid, info, plan


def apply_md_repair(version_dir: Path, plan) -> str | None:
    """Применить ТОЛЬКО set_md_file с backup. Возвращает путь backup или None."""
    if not plan.set_md_file:
        return None
    info_path = version_dir / "project_info.json"
    info = {}
    if info_path.exists():
        info = json.loads(info_path.read_text(encoding="utf-8"))
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = info_path.with_suffix(f".json.bak_{ts}")
    if info_path.exists():
        backup.write_text(info_path.read_text(encoding="utf-8"), encoding="utf-8")
    info["md_file"] = plan.set_md_file
    info_path.write_text(json.dumps(info, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(backup) if info_path.exists() else None


def main():
    ap = argparse.ArgumentParser(description="Dry-run/repair project_info.json md_file")
    ap.add_argument("project_ids", nargs="+", help="project_id (basename)")
    ap.add_argument("--apply", action="store_true", help="применить set md_file (с backup)")
    ap.add_argument("--json", action="store_true", help="вывести план в JSON")
    args = ap.parse_args()

    results = []
    for pid in args.project_ids:
        try:
            root_dir, version_dir, latest_vid, info, plan = plan_for(pid)
        except Exception as e:
            print(f"\n=== {pid} ===\n  [ERROR] resolve: {e}")
            results.append({"project_id": pid, "error": str(e)})
            continue

        entry = {
            "project_id": pid,
            "root_dir": str(root_dir),
            "version_dir": str(version_dir),
            "latest_version_id": latest_vid,
            "md_status": plan.md_status,
            "needs_repair": plan.needs_repair,
            "current_md_file": plan.current_md_file,
            "set_md_file": plan.set_md_file,
            "current_pdf_file": plan.current_pdf_file,
            "pdf_file_mismatch": plan.pdf_file_mismatch,
            "candidates": plan.candidates,
            "root_candidates": plan.root_candidates,
            "local_pdfs": plan.local_pdfs,
            "actions": plan.actions,
        }

        if not args.json:
            print(f"\n=== {pid} ===")
            print(f"  root_dir:          {root_dir}")
            print(f"  version_dir:       {version_dir}")
            print(f"  latest_version_id: {latest_vid}")
            print(f"  md_status:         {plan.md_status}")
            print(f"  current md_file:   {plan.current_md_file!r}")
            print(f"  current pdf_file:  {plan.current_pdf_file!r}")
            print(f"  candidates(ver):   {plan.candidates}")
            print(f"  candidates(root):  {plan.root_candidates}")
            print(f"  local pdfs:        {plan.local_pdfs}")
            if plan.actions:
                print("  PLAN:")
                for a in plan.actions:
                    print(f"    - {a}")
            else:
                print("  PLAN: (нет изменений)")

        if args.apply and plan.set_md_file:
            backup = apply_md_repair(version_dir, plan)
            entry["applied"] = True
            entry["backup"] = backup
            if not args.json:
                print(f"  APPLIED: md_file := {plan.set_md_file!r} (backup: {backup})")
        elif args.apply:
            entry["applied"] = False
            if not args.json and plan.needs_repair:
                print("  APPLY: нечего применять автоматически (только репорт)")

        results.append(entry)

    if args.json:
        print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
