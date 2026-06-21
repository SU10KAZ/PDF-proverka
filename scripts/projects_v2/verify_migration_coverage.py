#!/usr/bin/env python3
"""
verify_migration_coverage.py — READ-ONLY проверка покрытия legacy → projects_v2.

Сканирует legacy `projects/`, `projects_v2/` и `old_to_new_map.json`, относит
каждый реальный legacy-проект к одной из категорий и сообщает, есть ли
ДЕЙСТВИТЕЛЬНЫЙ backlog на миграцию (отсутствует v2-документ).

Скрипт НИЧЕГО не пишет по умолчанию и НИКОГДА не пишет в projects_v2/_system.
Он не вызывает refresh/batch-migrate и не трогает old_to_new_map.json.

Категории (см. CATEGORIES):
  * mapped                                — есть в old_to_new_map по пути;
  * dual_write_present_but_not_in_ledger  — v2 document.json есть, но записи в
                                            old_to_new_map нет (создан dual-write
                                            shadow, который ledger не пополняет);
  * container_moved_needs_map_repair      — document_code есть в map, но путь уехал
                                            (V1 переехал в `<база>(main)/`) — нужен
                                            repair пути, не миграция;
  * experiment_sandbox_junk               — папка под `_experiments/`/`_`-сегментом
                                            (как iter_project_dirs: `_`-исключения);
  * orphan_pdf_named_folder               — папка с именем `… .pdf` (битый артефакт
                                            версионирования);
  * missing_v2_real_backlog               — НЕТ v2 document.json → реальный backlog.

Дополнительно (warning, не влияет на exit-код):
  * snapshot_drift_candidates             — legacy и v2 findings оба есть, но
                                            размеры заметно расходятся → кандидат
                                            на точечный refresh.

Использование:
  python scripts/projects_v2/verify_migration_coverage.py
  python scripts/projects_v2/verify_migration_coverage.py --json
  python scripts/projects_v2/verify_migration_coverage.py --output /tmp/coverage.json
  python scripts/projects_v2/verify_migration_coverage.py \
      --legacy-root <projects> --v2-root <projects_v2> --map-file <old_to_new_map.json>

Exit codes:
  0 — coverage ok / нет реального backlog;
  1 — найден missing_v2_real_backlog;
  2 — ошибка скрипта / неверные пути.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Optional

CATEGORIES = (
    "mapped",
    "dual_write_present_but_not_in_ledger",
    "container_moved_needs_map_repair",
    "experiment_sandbox_junk",
    "orphan_pdf_named_folder",
    "missing_v2_real_backlog",
)

# Доля/абсолют расхождения размера findings, ниже которых дрейф не отмечается.
_DRIFT_MIN_ABS_BYTES = 1024
_DRIFT_MIN_RATIO = 0.02


def _read_json(path: Path) -> Optional[dict]:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _is_sandbox_path(rel: str) -> bool:
    """True, если в относительном пути есть `_`-сегмент (как iter_project_dirs:
    `_experiments`, `_output`, `_smoke*` и пр. исключаются из реальных проектов)."""
    parts = Path(rel).parts
    # последний сегмент — это сама папка проекта; junk = `_`-сегмент ВЫШЕ по пути
    return any(p.startswith("_") for p in parts[:-1])


def _legacy_findings_size(legacy_dir: Path) -> int:
    f = legacy_dir / "_output" / "03_findings.json"
    try:
        return f.stat().st_size if f.is_file() else -1
    except Exception:
        return -1


def _v2_doc_dir(v2_root: Path, discipline: str, doc_code: str) -> Optional[Path]:
    """Найти v2-документную папку по (discipline, document_code) глобом по объектам.

    Не зависит от формата object_id: ищем
    objects/*/disciplines/<disc>/documents/<doc_code>/document.json.
    """
    base = v2_root / "objects"
    if not base.is_dir():
        return None
    for obj in base.iterdir():
        cand = obj / "disciplines" / discipline / "documents" / doc_code
        if (cand / "document.json").is_file():
            return cand
    return None


def _v2_findings_size(doc_dir: Path) -> int:
    """Размер 03_findings.json последней версии (по имени) в v2-документе."""
    vroot = doc_dir / "versions"
    if not vroot.is_dir():
        return -1
    best = -1
    for v in sorted(vroot.iterdir()):
        f = v / "03_analysis" / "latest" / "03_findings.json"
        try:
            if f.is_file():
                best = f.stat().st_size
        except Exception:
            pass
    return best


def classify_coverage(legacy_root: Path, v2_root: Path, map_file: Path) -> dict:
    """Собрать read-only отчёт о покрытии. Ничего не пишет на диск."""
    legacy_root = Path(legacy_root)
    v2_root = Path(v2_root)

    migrations = []
    mp = _read_json(map_file)
    if isinstance(mp, dict):
        migrations = mp.get("migrations", []) or []

    mapped_real_paths = set()
    mapped_doc_codes = set()
    for e in migrations:
        p = e.get("legacy_folder_path")
        if p:
            mapped_real_paths.add(os.path.realpath(p))
        dc = e.get("document_code")
        if dc:
            mapped_doc_codes.add(dc)

    buckets = {c: [] for c in CATEGORIES}
    drift = []

    for dp, dns, fns in os.walk(legacy_root):
        if "project_info.json" not in fns:
            continue
        folder = Path(dp)
        rel = os.path.relpath(dp, legacy_root)

        if _is_sandbox_path(rel):
            buckets["experiment_sandbox_junk"].append(rel)
            continue

        info = _read_json(folder / "project_info.json") or {}
        doc_code = info.get("document_code") or info.get("name") or folder.name
        discipline = info.get("section") or (Path(rel).parts[1] if len(Path(rel).parts) > 1 else "")

        real = os.path.realpath(dp)
        doc_dir = _v2_doc_dir(v2_root, discipline, doc_code)

        if real in mapped_real_paths:
            cat = "mapped"
        elif doc_code in mapped_doc_codes:
            cat = "container_moved_needs_map_repair"
        elif doc_dir is not None:
            cat = "dual_write_present_but_not_in_ledger"
        elif folder.name.lower().endswith(".pdf"):
            # `.pdf`-именованная папка БЕЗ v2-присутствия — битый артефакт
            # версионирования (gotcha `… (1).pdf`), а не реальный backlog.
            # (Если такая папка ЕСТЬ в map / есть v2-док — она уже отнесена выше.)
            cat = "orphan_pdf_named_folder"
        else:
            cat = "missing_v2_real_backlog"
        buckets[cat].append(rel)

        # drift проверяется только когда v2-документ существует
        if doc_dir is not None:
            ls = _legacy_findings_size(folder)
            vs = _v2_findings_size(doc_dir)
            if ls > 0 and vs > 0 and ls != vs:
                diff = abs(ls - vs)
                if diff >= _DRIFT_MIN_ABS_BYTES and diff >= ls * _DRIFT_MIN_RATIO:
                    drift.append({
                        "project": rel, "category": cat,
                        "legacy_findings_bytes": ls, "v2_findings_bytes": vs,
                    })

    counts = {c: len(buckets[c]) for c in CATEGORIES}
    real_projects = (
        counts["mapped"]
        + counts["dual_write_present_but_not_in_ledger"]
        + counts["container_moved_needs_map_repair"]
        + counts["missing_v2_real_backlog"]
    )
    return {
        "legacy_root": str(legacy_root),
        "v2_root": str(v2_root),
        "map_file": str(map_file),
        "map_entries": len(migrations),
        "legacy_real_projects": real_projects,
        "counts": counts,
        "snapshot_drift_candidates": drift,
        "categories": buckets,
        "real_backlog": counts["missing_v2_real_backlog"] > 0,
    }


def _print_summary(report: dict) -> None:
    c = report["counts"]
    print("=== projects_v2 migration coverage (read-only) ===")
    print(f"legacy_root : {report['legacy_root']}")
    print(f"v2_root     : {report['v2_root']}")
    print(f"map_file    : {report['map_file']} ({report['map_entries']} entries)")
    print(f"legacy real projects                 : {report['legacy_real_projects']}")
    print(f"  mapped                             : {c['mapped']}")
    print(f"  dual_write_present_but_not_in_ledger: {c['dual_write_present_but_not_in_ledger']}")
    print(f"  container_moved_needs_map_repair    : {c['container_moved_needs_map_repair']}")
    print(f"  missing_v2_real_backlog            : {c['missing_v2_real_backlog']}")
    print(f"excluded experiment_sandbox_junk     : {c['experiment_sandbox_junk']}")
    print(f"orphan_pdf_named_folder              : {c['orphan_pdf_named_folder']}")
    print(f"snapshot_drift_candidates (warning)  : {len(report['snapshot_drift_candidates'])}")
    if c["missing_v2_real_backlog"]:
        print("\n[BACKLOG] реальные проекты без v2-документа:")
        for r in report["categories"]["missing_v2_real_backlog"]:
            print(f"   - {r}")
    if report["snapshot_drift_candidates"]:
        print("\n[DRIFT] кандидаты на точечный refresh (warning):")
        for d in report["snapshot_drift_candidates"]:
            print(f"   - {d['project']}: legacy={d['legacy_findings_bytes']}B v2={d['v2_findings_bytes']}B")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Read-only проверка покрытия legacy → projects_v2")
    ap.add_argument("--legacy-root", default=None, help="legacy projects/ (default: <repo>/projects)")
    ap.add_argument("--v2-root", default=None, help="projects_v2/ (default: <repo>/projects_v2)")
    ap.add_argument("--map-file", default=None,
                    help="old_to_new_map.json (default: <v2-root>/_system/old_to_new_map.json)")
    ap.add_argument("--json", action="store_true", help="печатать полный JSON в stdout")
    ap.add_argument("--output", default=None,
                    help="записать JSON в указанный путь (например /tmp/coverage.json)")
    args = ap.parse_args(argv)

    # Резолв путей. Дефолты — через v2lib, но только если roots не заданы явно
    # (тесты передают tmp-roots и от v2lib не зависят).
    if args.legacy_root:
        legacy_root = Path(args.legacy_root).resolve()
    else:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import v2lib  # noqa: E402
        legacy_root = v2lib.legacy_projects_root()
    if args.v2_root:
        v2_root = Path(args.v2_root).resolve()
    else:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import v2lib  # noqa: E402
        v2_root = v2lib.projects_v2_root()

    map_file = Path(args.map_file).resolve() if args.map_file else (v2_root / "_system" / "old_to_new_map.json")

    if not legacy_root.is_dir():
        print(f"[ERROR] legacy root not found: {legacy_root}", file=sys.stderr)
        return 2
    if not v2_root.is_dir():
        print(f"[ERROR] v2 root not found: {v2_root}", file=sys.stderr)
        return 2

    try:
        report = classify_coverage(legacy_root, v2_root, map_file)
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] coverage scan failed: {exc}", file=sys.stderr)
        return 2

    # Guard: НИКОГДА не писать в projects_v2/_system.
    if args.output:
        out = Path(args.output).resolve()
        system_dir = (v2_root / "_system").resolve()
        if system_dir == out.parent or system_dir in out.parents:
            print(f"[ERROR] отказ писать в projects_v2/_system: {out}", file=sys.stderr)
            return 2
        try:
            out.parent.mkdir(parents=True, exist_ok=True)
            with open(out, "w", encoding="utf-8") as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            print(f"[OK] coverage report -> {out}")
        except Exception as exc:  # noqa: BLE001
            print(f"[ERROR] cannot write output: {exc}", file=sys.stderr)
            return 2

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        _print_summary(report)

    return 1 if report["real_backlog"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
