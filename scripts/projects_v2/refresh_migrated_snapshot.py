#!/usr/bin/env python3
"""
refresh_migrated_snapshot.py — безопасное обновление snapshot ОДНОГО уже
мигрированного документа/версии в projects_v2, если legacy дрейфнул.

Зачем: при живом backend-аудите legacy `_output/*` (pipeline_log.json,
audit_log.jsonl, optimization_pre_review.json и т.п.) может измениться ПОСЛЕ
миграции. Тогда `validate_migration.py` помечает файлы `LEGACY CHANGED`. Это не
ошибка миграции — это дрейф источника. Скрипт пере-снимает только эти
изменившиеся файлы, но лишь если legacy СТАБИЛЕН (не пишется прямо сейчас).

БЕЗОПАСНОСТЬ:
  * без `--execute` — DRY-RUN (ничего не копирует);
  * работает ТОЛЬКО с документом/версией, уже записанными в old_to_new_map.json;
  * не создаёт новую миграцию, не делает массовый обход;
  * не удаляет и не меняет legacy `projects/`, не трогает `comparison/`;
  * НЕТ общего `--force`: refresh допускается только при стабильном legacy;
  * перед перезаписью архивирует старую v2-копию в `_system/refresh_archive/`.

Использование:
  python scripts/projects_v2/refresh_migrated_snapshot.py --dry-run \
      --document "13АВ-РД-АР3-К6" --version v002 \
      --v2-root <projects_v2> --stable-seconds 120
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import sys
import time
from pathlib import Path
from typing import Callable, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v2lib  # noqa: E402


class RefreshError(Exception):
    pass


def find_record(map_obj: dict, document: str, version: str) -> Optional[dict]:
    for m in map_obj.get("migrations", []):
        if m.get("document_code") == document and m.get("version_id") == version:
            return m
    return None


def snapshot_dir(folder: Path) -> dict:
    """{relpath: (sha256, size, mtime)} для всех файлов под folder."""
    snap: dict[str, tuple] = {}
    if not folder.is_dir():
        return snap
    for p in sorted(folder.rglob("*")):
        if p.is_file():
            try:
                snap[str(p.relative_to(folder))] = (
                    v2lib.sha256_file(p), p.stat().st_size, int(p.stat().st_mtime))
            except OSError:
                pass
    return snap


def stability_check(legacy_folder: Path, stable_seconds: int,
                    between_hook: Optional[Callable[[], None]] = None) -> tuple[bool, dict, dict]:
    """Два снимка legacy с паузой. True если совпали (стабилен).

    between_hook — для тестов (имитирует изменение legacy между снимками);
    в проде None → обычная пауза time.sleep(stable_seconds).
    """
    snap1 = snapshot_dir(legacy_folder)
    if between_hook is not None:
        between_hook()
    elif stable_seconds > 0:
        time.sleep(stable_seconds)
    snap2 = snapshot_dir(legacy_folder)
    return (snap1 == snap2), snap1, snap2


def compute_diffs(rec: dict) -> list[dict]:
    """Сравнивает recorded sha с текущим legacy/v2 для каждого tracked-файла.

    'modified' = sha записан И текущий legacy-sha != записанному (это то, что
    validate помечает LEGACY CHANGED). Untracked (sha=None) файлы validate не
    проверяет — их не трогаем.
    """
    diffs: list[dict] = []
    for f in rec.get("files", []):
        old_sha = f.get("sha256")
        if old_sha is None:
            continue  # validate такие не проверяет
        old_path = Path(f["old_path"])
        new_path = Path(f["new_path"])
        cur_legacy = v2lib.sha256_file(old_path) if old_path.exists() else None
        cur_v2 = v2lib.sha256_file(new_path) if new_path.exists() else None
        if cur_legacy is None:
            status = "missing_legacy"
        elif cur_legacy != old_sha:
            status = "modified"
        elif cur_v2 != old_sha:
            status = "v2_drift"
        else:
            status = "unchanged"
        if status in ("modified", "v2_drift", "missing_legacy"):
            diffs.append({
                "old_path": str(old_path), "new_path": str(new_path),
                "role": f.get("role", ""), "old_sha": old_sha,
                "current_legacy_sha": cur_legacy, "current_v2_sha": cur_v2,
                "status": status,
            })
    return diffs


def apply_refresh(rec: dict, diffs: list[dict], archive_dir: Path,
                  stamp: str) -> list[dict]:
    """Архивирует старую v2-копию и пере-копирует legacy→v2 для modified файлов.

    Обновляет sha256/bytes в rec['files'] in-place. Возвращает применённые записи.
    """
    by_new = {f["new_path"]: f for f in rec.get("files", [])}
    applied: list[dict] = []
    for d in diffs:
        if d["status"] != "modified":
            continue  # обновляем только реально изменившийся legacy
        old_path = Path(d["old_path"])
        new_path = Path(d["new_path"])
        # архив старой v2-копии
        if new_path.exists():
            arch = archive_dir / stamp / new_path.name
            arch.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(new_path, arch)
        # пере-копировать legacy → v2
        new_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(old_path, new_path)
        new_sha = v2lib.sha256_file(new_path)
        # обновить запись(и) карты с этим new_path
        entry = by_new.get(str(new_path))
        if entry is not None:
            entry["sha256"] = new_sha
            entry["bytes"] = new_path.stat().st_size
        applied.append({**d, "refreshed_sha": new_sha,
                        "archived_to": str(archive_dir / stamp / new_path.name)})
    return applied


def run_refresh(*, v2_root: Path, document: str, version: str,
                stable_seconds: int, execute: bool,
                map_path: Optional[Path] = None,
                between_hook: Optional[Callable[[], None]] = None) -> dict:
    map_path = map_path or (v2_root / "_system" / "old_to_new_map.json")
    if not map_path.exists():
        raise RefreshError(f"old_to_new_map.json not found: {map_path}")
    map_obj = v2lib.load_old_to_new_map(map_path)
    rec = find_record(map_obj, document, version)
    if rec is None:
        raise RefreshError(f"unknown document/version in map: {document} / {version}")

    legacy_folder = Path(rec["legacy_folder_path"])
    if not legacy_folder.is_dir():
        raise RefreshError(f"legacy folder missing: {legacy_folder}")

    stable, snap1, snap2 = stability_check(legacy_folder, stable_seconds, between_hook)
    diffs = compute_diffs(rec)

    summary = {
        "document_code": document, "version_id": version,
        "legacy_folder": str(legacy_folder), "v2_document_dir": rec.get("v2_document_dir"),
        "stable": stable, "stable_seconds": stable_seconds,
        "diff_count": len(diffs), "mode": "execute" if execute else "dry_run",
        "applied_count": 0,
    }

    result = {"summary": summary, "diffs": diffs, "applied": [], "stable": stable}

    if not stable:
        summary["error"] = "legacy_unstable_during_stability_check"
        return result

    if not execute:
        return result

    if diffs:
        stamp = time.strftime("%Y%m%dT%H%M%S")
        archive_dir = v2_root / "_system" / "refresh_archive" / v2lib.safe_component(document) / version
        applied = apply_refresh(rec, diffs, archive_dir, stamp)
        result["applied"] = applied
        summary["applied_count"] = len(applied)
        v2lib.save_old_to_new_map(map_obj, map_path)

    # отчёт
    sys_dir = v2_root / "_system"
    sys_dir.mkdir(parents=True, exist_ok=True)
    (sys_dir / "refresh_report.json").write_text(json.dumps({
        "schema_version": 1, "generated_at": v2lib.utc_now_iso(),
        "summary": summary, "diffs": diffs, "applied": result["applied"],
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    fields = ["status", "role", "old_path", "new_path", "old_sha",
              "current_legacy_sha", "current_v2_sha"]
    with open(sys_dir / "refresh_report.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for d in diffs:
            w.writerow({k: d.get(k, "") for k in fields})
    return result


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Refresh one migrated projects_v2 snapshot (safe)")
    ap.add_argument("--document", required=True)
    ap.add_argument("--version", required=True)
    ap.add_argument("--v2-root", default=None)
    ap.add_argument("--stable-seconds", type=int, default=120)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args(argv)

    if args.execute and args.dry_run:
        print("[ERROR] нельзя одновременно --execute и --dry-run", file=sys.stderr)
        return 2

    v2_root = Path(args.v2_root).resolve() if args.v2_root else v2lib.projects_v2_root()
    try:
        res = run_refresh(v2_root=v2_root, document=args.document, version=args.version,
                          stable_seconds=args.stable_seconds, execute=args.execute)
    except RefreshError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2

    s = res["summary"]
    print(f"=== refresh snapshot ({s['mode']}) {s['document_code']} / {s['version_id']} ===")
    print(f"legacy folder: {s['legacy_folder']}")
    print(f"v2 document:   {s['v2_document_dir']}")
    print(f"stable:        {s['stable']} (waited {s['stable_seconds']}s)")
    if not res["stable"]:
        print("[ABORT] legacy менялся во время stability-check — ничего не обновлено")
        return 1
    print(f"differing files (validate-relevant): {s['diff_count']}")
    for d in res["diffs"]:
        print(f"  [{d['status']:<14}] {Path(d['new_path']).name}")
        print(f"      old_sha={d['old_sha'][:16] if d['old_sha'] else None} "
              f"legacy={d['current_legacy_sha'][:16] if d['current_legacy_sha'] else None} "
              f"v2={d['current_v2_sha'][:16] if d['current_v2_sha'] else None}")
    if not args.execute:
        print("\n[DRY-RUN] ничего не скопировано. Повторите с --execute.")
        return 0
    print(f"\n[EXECUTE] обновлено файлов: {s['applied_count']}")
    for a in res["applied"]:
        print(f"  refreshed {Path(a['new_path']).name} -> {a['refreshed_sha'][:16]}")
    print(f"archive: {v2_root/'_system'/'refresh_archive'/v2lib.safe_component(args.document)/args.version}")
    print(f"report:  {v2_root/'_system'/'refresh_report.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
