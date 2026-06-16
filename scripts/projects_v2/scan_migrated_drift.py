#!/usr/bin/env python3
"""
scan_migrated_drift.py — READ-ONLY глобальная диагностика drift по уже
мигрированным документам projects_v2.

Сравнивает по `old_to_new_map.json` для каждого tracked-файла:
recorded sha (момент миграции) ↔ текущий legacy sha ↔ текущий v2 sha.

drift_type:
  * legacy_changed_v2_old — legacy ушёл вперёд, v2 = recorded (классический
    живой-аудит drift; безопасно refresh-нуть, если legacy стабилен);
  * v2_changed           — изменилась копия в projects_v2 (нужен ручной разбор);
  * missing_legacy       — legacy-файл пропал;
  * missing_v2           — копия в projects_v2 пропала.

Для каждого drift-документа выполняется stability-check (два снимка legacy с
паузой `--stable-seconds`). Рекомендация:
  * refresh_safe  — весь drift = legacy_changed_v2_old И legacy стабилен;
  * wait_backend  — legacy_changed_v2_old, но legacy ещё меняется (аудит идёт);
  * manual_review — есть v2_changed / missing_* / смешанные случаи.

НИЧЕГО не копирует и не меняет. legacy `projects/` и `comparison/` не трогаются.

Использование:
  python scripts/projects_v2/scan_migrated_drift.py \
      --v2-root <projects_v2> --stable-seconds 120
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Callable, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v2lib                          # noqa: E402
import refresh_migrated_snapshot as rms  # noqa: E402

DRIFT_TYPES = ("legacy_changed_v2_old", "legacy_new_file_not_in_map",
               "v2_changed", "missing_legacy", "missing_v2")
# типы, которые refresh умеет безопасно закрыть
_SAFE_DRIFT_TYPES = {"legacy_changed_v2_old", "legacy_new_file_not_in_map"}
REC_REFRESH_SAFE = "refresh_safe"
REC_WAIT_BACKEND = "wait_backend"
REC_MANUAL_REVIEW = "manual_review"


def classify_file(f: dict) -> Optional[dict]:
    """Возвращает drift-запись или None если файл не дрейфнул / не tracked."""
    recorded = f.get("sha256")
    if recorded is None:
        return None  # validate такие не проверяет
    old_path = Path(f["old_path"])
    new_path = Path(f["new_path"])

    if not old_path.exists():
        cur_v2 = v2lib.sha256_file(new_path) if new_path.exists() else None
        return {"drift_type": "missing_legacy", "current_legacy_sha": None,
                "current_v2_sha": cur_v2}

    cur_legacy = v2lib.sha256_file(old_path)
    if cur_legacy == recorded:
        # legacy не менялся — проверим копию v2
        if not new_path.exists():
            return {"drift_type": "missing_v2", "current_legacy_sha": cur_legacy,
                    "current_v2_sha": None}
        cur_v2 = v2lib.sha256_file(new_path)
        if cur_v2 != recorded:
            return {"drift_type": "v2_changed", "current_legacy_sha": cur_legacy,
                    "current_v2_sha": cur_v2}
        return None  # всё совпадает

    # legacy изменился
    if not new_path.exists():
        return {"drift_type": "missing_v2", "current_legacy_sha": cur_legacy,
                "current_v2_sha": None}
    cur_v2 = v2lib.sha256_file(new_path)
    if cur_v2 == recorded:
        return {"drift_type": "legacy_changed_v2_old", "current_legacy_sha": cur_legacy,
                "current_v2_sha": cur_v2}
    # и legacy, и v2 разошлись с recorded -> ручной разбор
    return {"drift_type": "v2_changed", "current_legacy_sha": cur_legacy,
            "current_v2_sha": cur_v2}


def _recommendation(drift_types: set, stable: bool) -> str:
    if drift_types - _SAFE_DRIFT_TYPES:
        return REC_MANUAL_REVIEW  # есть v2_changed / missing_*
    # только safe-типы (legacy_changed_v2_old / legacy_new_file_not_in_map)
    return REC_REFRESH_SAFE if stable else REC_WAIT_BACKEND


def run_scan(*, v2_root: Path, stable_seconds: int, map_path: Optional[Path] = None,
             between_hook: Optional[Callable[[], None]] = None) -> dict:
    map_path = map_path or (v2_root / "_system" / "old_to_new_map.json")
    map_obj = v2lib.load_old_to_new_map(map_path)

    documents = []
    rows = []
    for rec in map_obj.get("migrations", []):
        drift_files = []
        for f in rec.get("files", []):
            d = classify_file(f)
            if d is None:
                continue
            drift_files.append({
                "file": Path(f["new_path"]).name,
                "old_path": f["old_path"], "new_path": f["new_path"],
                "recorded_sha": f.get("sha256"),
                "current_legacy_sha": d["current_legacy_sha"],
                "current_v2_sha": d["current_v2_sha"],
                "drift_type": d["drift_type"],
            })
        # новые whitelist-файлы в legacy, которых нет в карте
        for nf in rms.detect_new_files(rec):
            drift_files.append({
                "file": nf["basename"],
                "old_path": nf["legacy_path"], "new_path": "",
                "recorded_sha": None,
                "current_legacy_sha": nf["current_legacy_sha"],
                "current_v2_sha": None,
                "drift_type": "legacy_new_file_not_in_map",
            })
        if not drift_files:
            continue

        legacy_folder = Path(rec["legacy_folder_path"])
        stable = False
        if legacy_folder.is_dir():
            stable, _, _ = rms.stability_check(legacy_folder, stable_seconds, between_hook)
        drift_types = {d["drift_type"] for d in drift_files}
        rec_label = _recommendation(drift_types, stable)

        doc_entry = {
            "document_code": rec.get("document_code"),
            "version_id": rec.get("version_id"),
            "legacy_path": str(legacy_folder),
            "v2_path": rec.get("v2_document_dir"),
            "stable": stable,
            "drift_types": sorted(drift_types),
            "drift_file_count": len(drift_files),
            "recommendation": rec_label,
        }
        documents.append(doc_entry)
        for d in drift_files:
            rows.append({
                "document_code": rec.get("document_code"),
                "version_id": rec.get("version_id"),
                "legacy_path": str(legacy_folder),
                "v2_path": rec.get("v2_document_dir"),
                "drift_type": d["drift_type"],
                "file": d["file"],
                "recorded_sha": d["recorded_sha"],
                "current_legacy_sha": d["current_legacy_sha"],
                "current_v2_sha": d["current_v2_sha"],
                "stable": stable,
                "recommendation": rec_label,
            })

    summary = {
        "drift_documents": len(documents),
        "stable": sum(1 for d in documents if d["stable"]),
        "unstable": sum(1 for d in documents if not d["stable"]),
        "refresh_safe": [f"{d['document_code']}/{d['version_id']}"
                         for d in documents if d["recommendation"] == REC_REFRESH_SAFE],
        "wait_backend": [f"{d['document_code']}/{d['version_id']}"
                         for d in documents if d["recommendation"] == REC_WAIT_BACKEND],
        "manual_review": [f"{d['document_code']}/{d['version_id']}"
                          for d in documents if d["recommendation"] == REC_MANUAL_REVIEW],
        "stable_seconds": stable_seconds,
    }
    return {"summary": summary, "documents": documents, "rows": rows}


def write_reports(result: dict, v2_root: Path) -> tuple[Path, Path]:
    sys_dir = v2_root / "_system"
    sys_dir.mkdir(parents=True, exist_ok=True)
    json_path = sys_dir / "migrated_drift_scan_report.json"
    csv_path = sys_dir / "migrated_drift_scan_report.csv"
    json_path.write_text(json.dumps({
        "schema_version": 1, "generated_at": v2lib.utc_now_iso(),
        "summary": result["summary"], "documents": result["documents"],
        "files": result["rows"],
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    fields = ["document_code", "version_id", "drift_type", "file", "recorded_sha",
              "current_legacy_sha", "current_v2_sha", "stable", "recommendation",
              "legacy_path", "v2_path"]
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in result["rows"]:
            w.writerow({k: r.get(k, "") for k in fields})
    return json_path, csv_path


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Read-only drift scan of migrated projects_v2 documents")
    ap.add_argument("--v2-root", default=None)
    ap.add_argument("--stable-seconds", type=int, default=120)
    args = ap.parse_args(argv)

    v2_root = Path(args.v2_root).resolve() if args.v2_root else v2lib.projects_v2_root()
    result = run_scan(v2_root=v2_root, stable_seconds=args.stable_seconds)
    json_path, csv_path = write_reports(result, v2_root)
    s = result["summary"]

    print("=== migrated drift scan ===")
    print(f"drift documents:  {s['drift_documents']}")
    print(f"  stable:         {s['stable']}")
    print(f"  unstable:       {s['unstable']}")
    print(f"refresh_safe:     {s['refresh_safe']}")
    print(f"wait_backend:     {s['wait_backend']}")
    print(f"manual_review:    {s['manual_review']}")
    print()
    for d in result["documents"]:
        print(f"  [{d['recommendation']:<13}] {d['document_code']}/{d['version_id']} "
              f"stable={d['stable']} types={d['drift_types']} files={d['drift_file_count']}")
    print()
    print(f"-> {json_path}")
    print(f"-> {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
