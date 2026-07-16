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


# --- новые legacy analysis-файлы (legacy_new_file_not_in_map) ---

# whitelist допустимых НОВЫХ файлов (относительно папки версии legacy)
NEW_FILE_WHITELIST = (
    "_output/02_text_analysis.json",
    "_output/01_blocks_analysis.json",
    "_output/03_findings.json",
    "_output/03_findings_review.json",
    "_output/norm_checks.json",
    "_output/03a_norms_verified.json",
    "_output/optimization.json",
    "_output/optimization_review.json",
    "_output/pipeline_log.json",
    "_output/audit_log.jsonl",
)
# какие из них дублируются в 03_analysis/latest
_LATEST_NAMES = {
    "02_text_analysis.json", "01_blocks_analysis.json", "03_findings.json",
    "norm_checks.json", "03a_norms_verified.json", "optimization.json",
}
_CRITICAL_NAMES = ("02_text_analysis.json", "01_blocks_analysis.json", "03_findings.json")


def detect_new_files(rec: dict) -> list[dict]:
    """Whitelist-файлы, появившиеся в legacy и отсутствующие в old_to_new_map."""
    legacy_folder = Path(rec["legacy_folder_path"])
    known_old = {f.get("old_path") for f in rec.get("files", [])}
    found = []
    for rel in NEW_FILE_WHITELIST:
        src = legacy_folder / rel
        if src.is_file() and str(src) not in known_old:
            found.append({
                "rel": rel, "basename": src.name, "legacy_path": str(src),
                "current_legacy_sha": v2lib.sha256_file(src),
                "drift_type": "legacy_new_file_not_in_map",
            })
    return found


def _analysis_status(latest_dir: Path) -> str:
    present = sum(1 for n in _CRITICAL_NAMES if (latest_dir / n).exists())
    if present == len(_CRITICAL_NAMES):
        return "complete"
    if present > 0:
        return "partial"
    return "none"


def _update_version_json(version_root: Path, status: str) -> None:
    vjson = version_root / "version.json"
    if not vjson.exists():
        return
    try:
        meta = json.loads(vjson.read_text(encoding="utf-8"))
    except Exception:
        return
    meta["analysis_status"] = status
    meta["analysis_refreshed_at"] = v2lib.utc_now_iso()
    meta["analysis_refresh_reason"] = "legacy_new_analysis_artifacts"
    vjson.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def apply_new_files(rec: dict, new_files: list[dict], version_root: Path,
                    run_dir: Path, archive_dir: Path, stamp: str) -> list[dict]:
    """Копирует новые whitelist-файлы в run_refresh + (для analysis) в latest.

    Добавляет записи в rec['files']. Возвращает применённые записи.
    """
    applied: list[dict] = []
    for nf in new_files:
        src = Path(nf["legacy_path"])
        name = nf["basename"]
        dests = [run_dir / name]
        if name in _LATEST_NAMES:
            dests.append(version_root / "03_analysis" / "latest" / name)
        copied_to = []
        for dest in dests:
            if dest.exists():  # на всякий — архивируем (обычно новых файлов в v2 ещё нет)
                arch = archive_dir / stamp / dest.name
                arch.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(dest, arch)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest)
            rec["files"].append({
                "old_path": str(src), "new_path": str(dest),
                "sha256": v2lib.sha256_file(dest), "bytes": dest.stat().st_size,
                "role": "run_refresh_new",
            })
            copied_to.append(str(dest))
        applied.append({**nf, "copied_to": copied_to})
    return applied


def run_refresh(*, v2_root: Path, document: str, version: str,
                stable_seconds: int, execute: bool,
                map_path: Optional[Path] = None,
                between_hook: Optional[Callable[[], None]] = None,
                include_new_files: bool = False) -> dict:
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
    new_files = detect_new_files(rec) if include_new_files else []

    summary = {
        "document_code": document, "version_id": version,
        "legacy_folder": str(legacy_folder), "v2_document_dir": rec.get("v2_document_dir"),
        "stable": stable, "stable_seconds": stable_seconds,
        "diff_count": len(diffs), "new_file_count": len(new_files),
        "mode": "execute" if execute else "dry_run",
        "include_new_files": include_new_files,
        "applied_count": 0, "new_files_added": 0,
        "run_refresh_dir": None, "analysis_status": None,
    }

    result = {"summary": summary, "diffs": diffs, "new_files": new_files,
              "applied": [], "new_applied": [], "stable": stable}

    if not stable:
        summary["error"] = "legacy_unstable_during_stability_check"
        return result

    if not execute:
        return result

    changed = False
    stamp = time.strftime("%Y%m%dT%H%M%S")
    archive_dir = v2_root / "_system" / "refresh_archive" / v2lib.safe_component(document) / version

    if diffs:
        applied = apply_refresh(rec, diffs, archive_dir, stamp)
        result["applied"] = applied
        summary["applied_count"] = len(applied)
        changed = changed or bool(applied)

    if include_new_files and new_files:
        version_root = Path(rec["v2_document_dir"]) / "versions" / version
        run_dir = version_root / "03_analysis" / "runs" / f"run_refresh_{stamp}"
        new_applied = apply_new_files(rec, new_files, version_root, run_dir, archive_dir, stamp)
        result["new_applied"] = new_applied
        summary["new_files_added"] = len(new_applied)
        summary["run_refresh_dir"] = str(run_dir)
        changed = changed or bool(new_applied)

    if include_new_files:
        version_root = Path(rec["v2_document_dir"]) / "versions" / version
        status = _analysis_status(version_root / "03_analysis" / "latest")
        summary["analysis_status"] = status
        _update_version_json(version_root, status)

    if changed:
        v2lib.save_old_to_new_map(map_obj, map_path)

    # отчёт
    sys_dir = v2_root / "_system"
    sys_dir.mkdir(parents=True, exist_ok=True)
    (sys_dir / "refresh_report.json").write_text(json.dumps({
        "schema_version": 1, "generated_at": v2lib.utc_now_iso(),
        "summary": summary, "diffs": diffs, "applied": result["applied"],
        "new_files": new_files, "new_applied": result["new_applied"],
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
    ap.add_argument("--include-new-files", action="store_true",
                    help="добавлять whitelist analysis-файлы, появившиеся в legacy после миграции")
    args = ap.parse_args(argv)

    if args.execute and args.dry_run:
        print("[ERROR] нельзя одновременно --execute и --dry-run", file=sys.stderr)
        return 2

    v2_root = Path(args.v2_root).resolve() if args.v2_root else v2lib.projects_v2_root()
    try:
        res = run_refresh(v2_root=v2_root, document=args.document, version=args.version,
                          stable_seconds=args.stable_seconds, execute=args.execute,
                          include_new_files=args.include_new_files)
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
    if args.include_new_files:
        print(f"new legacy files (not in map): {s['new_file_count']}")
        for nf in res["new_files"]:
            print(f"  [legacy_new_file ] {nf['basename']}")
    if not args.execute:
        print("\n[DRY-RUN] ничего не скопировано. Повторите с --execute.")
        return 0
    print(f"\n[EXECUTE] обновлено существующих: {s['applied_count']}  добавлено новых: {s['new_files_added']}")
    for a in res["applied"]:
        print(f"  refreshed {Path(a['new_path']).name} -> {a['refreshed_sha'][:16]}")
    for a in res["new_applied"]:
        print(f"  added {a['basename']} -> {len(a['copied_to'])} dest")
    if s["run_refresh_dir"]:
        print(f"run_refresh: {s['run_refresh_dir']}")
    if s["analysis_status"]:
        print(f"analysis_status: {s['analysis_status']}")
    print(f"archive: {v2_root/'_system'/'refresh_archive'/v2lib.safe_component(args.document)/args.version}")
    print(f"report:  {v2_root/'_system'/'refresh_report.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
