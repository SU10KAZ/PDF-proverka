#!/usr/bin/env python3
"""
batch_migrate_projects_v2.py — пакетная миграция legacy-проектов в projects_v2.

Читает `projects_v2/_system/migration_readiness_report.json` и мигрирует
проекты выбранного класса готовности через `v2lib.migrate_project`.

БЕЗОПАСНОСТЬ (по умолчанию):
  * без `--execute` — DRY-RUN, ничего не копируется;
  * `--class` обязателен (иначе ошибка);
  * класс != AUTO_SAFE требует `--allow-warnings`;
  * MANUAL_REVIEW_REQUIRED не мигрируется НИКОГДА;
  * SKIP_EMPTY_OR_INVALID не мигрируется;
  * already-migrated пропускаются (`--skip-already-migrated`);
  * перед копированием проверяется существование legacy-path;
  * перезапись существующего document/version запрещена (нужен `--force`);
  * `--force` намеренно НЕ реализован (всегда ошибка).

READ-ONLY к legacy. Пишет только в projects_v2/.

Использование:
  python scripts/projects_v2/batch_migrate_projects_v2.py \
      --dry-run --class AUTO_SAFE --limit 5 --skip-already-migrated \
      --legacy-root <projects> --v2-root <projects_v2>
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Callable, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v2lib       # noqa: E402
import readiness   # noqa: E402


class BatchRequestError(Exception):
    """Невалидный запрос (нарушение safety-правил)."""


# ---------------------------------------------------------------------------
# Валидация запроса (чистая, тестируемая)
# ---------------------------------------------------------------------------


def validate_request(klass: Optional[str], *, execute: bool, dry_run: bool,
                     allow_warnings: bool, force: bool,
                     warning_policy: Optional[str] = None) -> None:
    """Бросает BatchRequestError при нарушении safety-инвариантов."""
    if force:
        raise BatchRequestError("--force не реализован и запрещён на этом этапе")
    if execute and dry_run:
        raise BatchRequestError("нельзя одновременно --execute и --dry-run")
    if not klass:
        raise BatchRequestError("--class обязателен (например AUTO_SAFE)")
    if klass not in readiness.READINESS_GROUPS:
        raise BatchRequestError(f"неизвестный --class: {klass}")
    if klass == readiness.MANUAL_REVIEW_REQUIRED:
        raise BatchRequestError("MANUAL_REVIEW_REQUIRED мигрировать нельзя")
    if klass == readiness.SKIP_EMPTY_OR_INVALID:
        raise BatchRequestError("SKIP_EMPTY_OR_INVALID мигрировать нельзя")
    if klass == readiness.ALREADY_MIGRATED:
        raise BatchRequestError("ALREADY_MIGRATED мигрировать повторно нельзя")

    # --- warning-policy ---
    if klass == readiness.CAN_MIGRATE_WITH_WARNINGS:
        if not warning_policy:
            raise BatchRequestError(
                "--class CAN_MIGRATE_WITH_WARNINGS требует --warning-policy")
        if warning_policy == readiness.WARNINGS_BLOCKED:
            raise BatchRequestError("WARNINGS_BLOCKED мигрировать нельзя никогда")
        if warning_policy == readiness.WARNINGS_NEED_POLICY:
            raise BatchRequestError(
                "WARNINGS_NEED_POLICY требует отдельного флага (пока не реализован)")
        if warning_policy != readiness.WARNINGS_AUTO_CANDIDATE:
            raise BatchRequestError(f"неподдерживаемый --warning-policy: {warning_policy}")
    elif warning_policy:
        raise BatchRequestError(
            "--warning-policy допустим только с --class CAN_MIGRATE_WITH_WARNINGS")

    if klass != readiness.AUTO_SAFE and not allow_warnings:
        raise BatchRequestError(
            f"класс {klass} требует явного --allow-warnings")


# ---------------------------------------------------------------------------
# Выбор кандидатов (чистая, тестируемая)
# ---------------------------------------------------------------------------


def select_candidates(report_projects: list[dict], klass: str, *,
                      limit: Optional[int], skip_already_migrated: bool,
                      is_migrated: Callable[[dict], bool],
                      group_field: str = "group",
                      extra_predicate: Optional[Callable[[dict], bool]] = None
                      ) -> tuple[list[dict], list[dict]]:
    """Возвращает (to_migrate, skipped_already_migrated).

    Берёт проекты, у которых `group_field == klass` (и `extra_predicate`, если
    задан), в порядке отчёта; при skip_already_migrated пропускает уже
    мигрированные; останавливается, набрав `limit` к миграции.
    """
    to_migrate: list[dict] = []
    skipped: list[dict] = []
    for p in report_projects:
        if p.get(group_field) != klass:
            continue
        if extra_predicate is not None and not extra_predicate(p):
            continue
        if skip_already_migrated and is_migrated(p):
            skipped.append(p)
            continue
        to_migrate.append(p)
        if limit is not None and len(to_migrate) >= limit:
            break
    return to_migrate, skipped


def _live_is_migrated(v2_root: Path) -> Callable[[dict], bool]:
    """Проверяет фактическое наличие document.json в projects_v2 (не доверяя отчёту)."""
    def _check(p: dict) -> bool:
        doc_dir = v2lib.document_dir_in_v2(
            v2_root, p["object_id"], p["discipline"], p["document_code"],
            display_name=p.get("object"))
        return (doc_dir / "document.json").exists()
    return _check


# ---------------------------------------------------------------------------
# Выполнение
# ---------------------------------------------------------------------------

REPORT_FIELDS = [
    "status", "old_path", "new_path", "object_id", "discipline", "document_code",
    "version_count", "copied_files_count", "checksum_checked_count", "error_message",
]


def _migrate_and_record(project: dict, v2_root: Path, objects_map: dict,
                        map_obj: dict, *, execute: bool) -> dict:
    """Мигрирует один проект (или планирует при dry-run). Возвращает строку отчёта."""
    legacy_path = Path(project["legacy_path"])
    base = {
        "old_path": str(legacy_path),
        "new_path": "",
        "object_id": project["object_id"],
        "discipline": project["discipline"],
        "document_code": project["document_code"],
        "version_count": project.get("version_count", 0),
        "copied_files_count": 0,
        "checksum_checked_count": 0,
        "error_message": "",
    }

    # pre-copy: legacy существует?
    if not legacy_path.is_dir():
        return {**base, "status": "error", "error_message": "legacy_path_missing"}

    # pre-copy: целевой документ уже существует? (перезапись запрещена без --force)
    doc_dir = v2lib.document_dir_in_v2(
        v2_root, project["object_id"], project["discipline"], project["document_code"],
        display_name=project.get("object"))
    if (doc_dir / "document.json").exists():
        return {**base, "new_path": str(doc_dir), "status": "error",
                "error_message": "target_exists_without_force"}

    if not execute:
        return {**base, "new_path": str(doc_dir), "status": "planned"}

    try:
        result = v2lib.migrate_project(legacy_path, v2_root,
                                       objects_map=objects_map, run_id=None)
    except Exception as exc:  # fail-soft: одна ошибка не валит весь батч
        return {**base, "new_path": str(doc_dir), "status": "error",
                "error_message": f"{type(exc).__name__}: {exc}"}

    copied = 0
    checked = 0
    for vrec in result["versions"]:
        files = vrec["files"]
        copied += len(files)
        checked += sum(1 for f in files if f.get("sha256") is not None)
        v2lib.upsert_migration(map_obj, {
            "object_id": result["object_id"],
            "object_name": result["object_name"],
            "discipline": result["discipline"],
            "document_code": result["document_code"],
            "kind": result["kind"],
            "version_id": vrec["version_id"],
            "version_no": vrec["version_no"],
            "legacy_folder_name": vrec["legacy_folder_name"],
            "legacy_folder_path": vrec["legacy_folder_path"],
            "analysis_run_id": vrec["analysis_run_id"],
            "v2_document_dir": result["v2_document_dir"],
            "files": files,
        })
    return {
        **base,
        "new_path": result["v2_document_dir"],
        "version_count": len(result["versions"]),
        "copied_files_count": copied,
        "checksum_checked_count": checked,
        "status": "migrated",
        "error_message": "",
    }


def run_batch(*, report_path: Path, v2_root: Path, klass: str,
              limit: Optional[int], skip_already_migrated: bool,
              execute: bool, objects_map: Optional[dict] = None,
              warning_policy: Optional[str] = None) -> dict:
    """Выполняет (или планирует) батч. Пишет batch_migration_report.{json,csv}.

    Без warning_policy читает readiness report и фильтрует по `group == klass`.
    С warning_policy читает warning-policy report и берёт только
    `policy_group == warning_policy` И `recommendation == can_batch_migrate`.
    """
    report = json.loads(report_path.read_text(encoding="utf-8"))
    projects = report.get("projects", [])

    if warning_policy:
        to_migrate, skipped = select_candidates(
            projects, warning_policy, limit=limit,
            skip_already_migrated=skip_already_migrated,
            is_migrated=_live_is_migrated(v2_root),
            group_field="policy_group",
            extra_predicate=lambda p: p.get("recommendation") == readiness.REC_CAN_BATCH)
    else:
        to_migrate, skipped = select_candidates(
            projects, klass, limit=limit,
            skip_already_migrated=skip_already_migrated,
            is_migrated=_live_is_migrated(v2_root))

    if objects_map is None:
        objects_map = v2lib.load_objects_map()
    map_path = v2_root / "_system" / "old_to_new_map.json"
    map_obj = v2lib.load_old_to_new_map(map_path) if execute else {}

    rows: list[dict] = []
    for p in to_migrate:
        rows.append(_migrate_and_record(p, v2_root, objects_map, map_obj, execute=execute))

    if execute:
        v2lib.save_old_to_new_map(map_obj, map_path)

    migrated = sum(1 for r in rows if r["status"] == "migrated")
    planned = sum(1 for r in rows if r["status"] == "planned")
    errors = sum(1 for r in rows if r["status"] == "error")
    total_copied = sum(r["copied_files_count"] for r in rows)
    total_checked = sum(r["checksum_checked_count"] for r in rows)

    summary = {
        "mode": "execute" if execute else "dry_run",
        "class": klass,
        "warning_policy": warning_policy,
        "limit": limit,
        "skip_already_migrated": skip_already_migrated,
        "selected": len(to_migrate),
        "skipped_already_migrated": len(skipped),
        "migrated": migrated,
        "planned": planned,
        "errors": errors,
        "copied_files_total": total_copied,
        "checksum_checked_total": total_checked,
    }

    out_dir = v2_root / "_system"
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "batch_migration_report.json"
    csv_path = out_dir / "batch_migration_report.csv"
    json_path.write_text(json.dumps({
        "schema_version": 1,
        "generated_at": v2lib.utc_now_iso(),
        "summary": summary,
        "skipped_already_migrated": [
            {"document_code": s["document_code"], "discipline": s["discipline"],
             "legacy_path": s["legacy_path"]} for s in skipped],
        "projects": rows,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=REPORT_FIELDS)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    return {"summary": summary, "rows": rows, "skipped": skipped,
            "json_path": json_path, "csv_path": csv_path}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Batch migrate legacy projects to projects_v2")
    parser.add_argument("--class", dest="klass", default=None,
                        help="readiness class (AUTO_SAFE | CAN_MIGRATE_WITH_WARNINGS)")
    parser.add_argument("--warning-policy", dest="warning_policy", default=None,
                        help="warning-policy подгруппа (только WARNINGS_AUTO_CANDIDATE)")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--skip-already-migrated", action="store_true")
    parser.add_argument("--allow-warnings", action="store_true")
    parser.add_argument("--force", action="store_true",
                        help="НЕ реализован — всегда ошибка (safety)")
    parser.add_argument("--legacy-root", default=None)
    parser.add_argument("--v2-root", default=None)
    parser.add_argument("--report-path", default=None,
                        help="override readiness report path")
    args = parser.parse_args(argv)

    try:
        validate_request(args.klass, execute=args.execute, dry_run=args.dry_run,
                         allow_warnings=args.allow_warnings, force=args.force,
                         warning_policy=args.warning_policy)
    except BatchRequestError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2

    v2_root = Path(args.v2_root).resolve() if args.v2_root else v2lib.projects_v2_root()
    # warning-policy режим читает warning-policy report, иначе — readiness report
    default_report = ("migration_warning_policy_report.json" if args.warning_policy
                      else "migration_readiness_report.json")
    report_path = (Path(args.report_path).resolve() if args.report_path
                   else v2_root / "_system" / default_report)
    if not report_path.exists():
        print(f"[ERROR] report not found: {report_path}\n"
              f"        run report_migration_readiness.py first", file=sys.stderr)
        return 2

    execute = args.execute  # dry-run по умолчанию (если не --execute)
    result = run_batch(report_path=report_path, v2_root=v2_root, klass=args.klass,
                       limit=args.limit, skip_already_migrated=args.skip_already_migrated,
                       execute=execute, warning_policy=args.warning_policy)
    s = result["summary"]

    print(f"=== batch migration ({s['mode']}) class={s['class']} "
          f"policy={s.get('warning_policy')} limit={s['limit']} ===")
    print(f"selected:                 {s['selected']}")
    print(f"skipped already_migrated: {s['skipped_already_migrated']}")
    if execute:
        print(f"migrated:                 {s['migrated']}")
    else:
        print(f"planned (dry-run):        {s['planned']}")
    print(f"errors:                   {s['errors']}")
    print(f"copied files total:       {s['copied_files_total']}")
    print(f"checksum checked total:   {s['checksum_checked_total']}")
    print()
    for r in result["rows"]:
        print(f"  [{r['status']:<8}] {r['discipline']}/{r['document_code']} "
              f"-> v{r['version_count']} files={r['copied_files_count']} "
              f"{r['error_message']}")
    print()
    print(f"-> {result['json_path']}")
    print(f"-> {result['csv_path']}")
    if not execute:
        print("\n[DRY-RUN] ничего не скопировано. Повторите с --execute для реальной миграции.")
    return 1 if s["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
