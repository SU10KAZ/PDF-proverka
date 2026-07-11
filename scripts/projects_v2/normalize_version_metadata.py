#!/usr/bin/env python3
"""
normalize_version_metadata.py — METADATA-ONLY нормализация version.json в
projects_v2 после финальной миграции.

Зачем: часть version.json (ранняя схема миграции) не содержит поля
`analysis_status`. Перед read-only storage adapter metadata надо привести к
единому виду. Поле `analysis_status` определяется ПО ФАКТИЧЕСКИ существующим
файлам в `versions/<vid>/03_analysis/latest/`.

Что инструмент ДЕЛАЕТ:
  * проходит по всем `version.json` в projects_v2;
  * вычисляет `analysis_status` из наличия 02_text_analysis.json /
    01_blocks_analysis.json / 03_findings.json в 03_analysis/latest;
  * в `--execute` ЗАПОЛНЯЕТ отсутствующий `analysis_status` (+ `missing_analysis_files`)
    в самом version.json.

Что инструмент НЕ делает:
  * не читает legacy projects/ (классификация только по projects_v2);
  * не копирует и не удаляет файлы;
  * не трогает analysis/output артефакты (только version.json);
  * по умолчанию НЕ перезаписывает уже выставленный `analysis_status`
    (см. --correct-existing) — чтобы не регрессировать осознанные пометки
    (напр. legacy_partial у документа с KB-findings, но без файлов в latest).

Правила классификации (по файлам в 03_analysis/latest):
  has = {02_text_analysis.json, 01_blocks_analysis.json, 03_findings.json}
  is_legacy = migration_kind == "legacy_findings_preserve"
              ИЛИ "legacy" в preserve_reason
  - is_legacy + есть analysis-файлы   -> legacy_partial
  - is_legacy + нет analysis-файлов   -> source_only
  - не legacy + все три               -> complete
  - не legacy + часть                 -> partial
  - не legacy + ничего (есть вход)    -> none

READ-ONLY для всего, кроме version.json. legacy projects/ и comparison/ не
трогает, в git не пишет (отчёты — runtime в projects_v2/_system).
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v2lib  # noqa: E402

CRITICAL = ("02_text_analysis.json", "01_blocks_analysis.json", "03_findings.json")


def is_legacy_preserve(vd: dict) -> bool:
    if vd.get("migration_kind") == "legacy_findings_preserve":
        return True
    pr = str(vd.get("preserve_reason") or "").lower()
    return "legacy" in pr


def classify_status(present: dict, is_legacy: bool, input_present: bool) -> str:
    """present = {имя critical -> bool}. Возвращает analysis_status."""
    n = sum(1 for c in CRITICAL if present.get(c))
    if is_legacy:
        return "legacy_partial" if n > 0 else "source_only"
    if n == len(CRITICAL):
        return "complete"
    if n > 0:
        return "partial"
    return "none"  # есть вход, но нет анализа


def _version_meta_from_path(vj_path: Path, objects_root: Path) -> dict:
    try:
        rel = vj_path.relative_to(objects_root).parts
        # <obj>/disciplines/<disc>/documents/<code>/versions/<vid>/version.json
        return {"object": rel[0], "discipline": rel[2], "document_code": rel[4],
                "version_id": rel[6]}
    except Exception:
        return {"object": "?", "discipline": "?", "document_code": "?", "version_id": "?"}


def plan_one(vj_path: Path, objects_root: Path) -> dict:
    meta = _version_meta_from_path(vj_path, objects_root)
    try:
        vd = json.loads(vj_path.read_text(encoding="utf-8"))
    except Exception as e:
        return {**meta, "error": f"unreadable version.json: {e}",
                "action": "error", "existing": None, "proposed": None}

    vroot = vj_path.parent
    latest = vroot / "03_analysis" / "latest"
    present = {c: (latest / c).exists() for c in CRITICAL}
    input_dir = vroot / "01_input"
    input_present = input_dir.is_dir() and any(p.is_file() for p in input_dir.rglob("*"))
    legacy = is_legacy_preserve(vd)
    proposed = classify_status(present, legacy, input_present)
    missing = [c for c in CRITICAL if not present.get(c)]

    existing = vd.get("analysis_status")
    has_missing_field = "missing_analysis_files" in vd
    if existing is None:
        action = "fill"
    elif existing == proposed:
        action = "unchanged_match" if has_missing_field else "unchanged_match_add_missing"
    else:
        action = "kept_existing_divergent"

    return {
        **meta,
        "is_legacy_preserve": legacy,
        "has_01": present[CRITICAL[0]], "has_02": present[CRITICAL[1]],
        "has_03": present[CRITICAL[2]],
        "input_present": input_present,
        "existing": existing,
        "proposed": proposed,
        "missing_analysis_files": missing,
        "action": action,
        "_path": str(vj_path),
        "_vd": vd,  # для записи (не сериализуем в отчёт)
    }


def apply_one(row: dict, *, correct_existing: bool) -> Optional[str]:
    """Пишет version.json при необходимости. Возвращает применённый статус или None."""
    action = row["action"]
    vd = row["_vd"]
    path = Path(row["_path"])
    write = False
    new_status = None

    if action == "fill":
        vd["analysis_status"] = row["proposed"]
        vd["missing_analysis_files"] = row["missing_analysis_files"]
        new_status = row["proposed"]
        write = True
    elif action == "unchanged_match_add_missing":
        # статус совпадает, но не хватает поля missing_analysis_files — добавим
        vd["missing_analysis_files"] = row["missing_analysis_files"]
        new_status = row["existing"]
        write = True
    elif action == "kept_existing_divergent" and correct_existing:
        vd["analysis_status"] = row["proposed"]
        vd["missing_analysis_files"] = row["missing_analysis_files"]
        new_status = row["proposed"]
        write = True

    if write:
        vd["metadata_normalized_at"] = v2lib.utc_now_iso()
        path.write_text(json.dumps(vd, ensure_ascii=False, indent=2), encoding="utf-8")
    return new_status


def effective_status(row: dict, *, correct_existing: bool) -> str:
    a = row["action"]
    if a == "fill":
        return row["proposed"]
    if a == "kept_existing_divergent":
        return row["proposed"] if correct_existing else row["existing"]
    return row["existing"]


def gather(v2_root: Path) -> list[dict]:
    objects_root = v2_root / "objects"
    rows = []
    if objects_root.is_dir():
        for vj in sorted(objects_root.rglob("versions/*/version.json")):
            rows.append(plan_one(vj, objects_root))
    return rows


CSV_FIELDS = ["object", "discipline", "document_code", "version_id",
              "is_legacy_preserve", "has_01", "has_02", "has_03", "input_present",
              "existing", "proposed", "action", "missing_analysis_files"]


def write_reports(rows: list[dict], summary: dict, sys_dir: Path) -> tuple[Path, Path]:
    sys_dir.mkdir(parents=True, exist_ok=True)
    clean = [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]
    jp = sys_dir / "version_metadata_normalization_report.json"
    jp.write_text(json.dumps({"schema_version": 1, "generated_at": v2lib.utc_now_iso(),
                              "summary": summary, "versions": clean},
                             ensure_ascii=False, indent=2), encoding="utf-8")
    cp = sys_dir / "version_metadata_normalization_report.csv"
    with open(cp, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        w.writeheader()
        for r in clean:
            row = {k: r.get(k, "") for k in CSV_FIELDS}
            row["missing_analysis_files"] = ";".join(r.get("missing_analysis_files") or [])
            w.writerow(row)
    return jp, cp


def build_summary(rows: list[dict], *, executed: bool, correct_existing: bool) -> dict:
    actions = Counter(r["action"] for r in rows)
    missing_before = sum(1 for r in rows if r.get("existing") is None)
    after = Counter(effective_status(r, correct_existing=correct_existing)
                    for r in rows if r["action"] != "error")
    divergent = [
        {"document_code": r["document_code"], "version_id": r["version_id"],
         "existing": r["existing"], "proposed": r["proposed"],
         "is_legacy_preserve": r["is_legacy_preserve"]}
        for r in rows if r["action"] == "kept_existing_divergent"
    ]
    # updated = записи, которые реально будут/были записаны
    updated = actions.get("fill", 0) + actions.get("unchanged_match_add_missing", 0)
    if correct_existing:
        updated += actions.get("kept_existing_divergent", 0)
    return {
        "executed": executed,
        "correct_existing": correct_existing,
        "total_version_json": len(rows),
        "missing_analysis_status_before": missing_before,
        "filled": actions.get("fill", 0),
        "added_missing_field_only": actions.get("unchanged_match_add_missing", 0),
        "unchanged": actions.get("unchanged_match", 0),
        "kept_existing_divergent": actions.get("kept_existing_divergent", 0),
        "errors": actions.get("error", 0),
        "updated": updated,
        "status_distribution_after": dict(after),
        "divergent_details": divergent,
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Metadata-only нормализация version.json (projects_v2)")
    ap.add_argument("--v2-root", default=None)
    ap.add_argument("--dry-run", action="store_true", help="ничего не менять (default)")
    ap.add_argument("--execute", action="store_true", help="записать version.json")
    ap.add_argument("--correct-existing", action="store_true",
                    help="перезаписывать уже выставленный analysis_status при расхождении (default OFF)")
    args = ap.parse_args(argv)

    v2_root = Path(args.v2_root).resolve() if args.v2_root else v2lib.projects_v2_root()
    execute = args.execute and not args.dry_run

    rows = gather(v2_root)

    applied = Counter()
    if execute:
        for r in rows:
            st = apply_one(r, correct_existing=args.correct_existing)
            if st is not None:
                applied[st] += 1

    summary = build_summary(rows, executed=execute, correct_existing=args.correct_existing)
    jp, cp = write_reports(rows, summary, v2_root / "_system")

    mode = "EXECUTE" if execute else "DRY-RUN"
    print(f"=== normalize_version_metadata [{mode}] ===")
    print(f"total version.json:            {summary['total_version_json']}")
    print(f"missing analysis_status before:{summary['missing_analysis_status_before']}")
    print(f"filled:                        {summary['filled']}")
    print(f"added missing_field only:      {summary['added_missing_field_only']}")
    print(f"unchanged:                     {summary['unchanged']}")
    print(f"kept_existing_divergent:       {summary['kept_existing_divergent']}"
          + (" (will rewrite: correct-existing ON)" if args.correct_existing else " (preserved)"))
    print(f"errors:                        {summary['errors']}")
    print(f"updated (written):             {summary['updated'] if execute else '(dry-run: 0)'}")
    print(f"status distribution after:     {summary['status_distribution_after']}")
    if summary["divergent_details"]:
        print("divergent (existing != proposed, preserved by default):")
        for d in summary["divergent_details"]:
            print(f"  {d['document_code']} {d['version_id']}: existing={d['existing']} "
                  f"proposed={d['proposed']} legacy={d['is_legacy_preserve']}")
    print(f"-> {jp}")
    print(f"-> {cp}")
    if execute:
        print(f"applied status counts: {dict(applied)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
