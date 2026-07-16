#!/usr/bin/env python3
"""
migrate_legacy_findings_preserve.py — миграция legacy-проектов King&Sons
(blocked/manual) в projects_v2 как LEGACY SNAPSHOT по политике
`POLICY_READY_LEGACY_FINDINGS_PRESERVE`.

Главная цель — НЕ добиваться идеальной структуры входных файлов, а сохранить:
  * найденные замечания (03_findings.json и сопутствующие артефакты);
  * полный legacy `_output/` (контекст аудита);
  * связь с knowledge_base/decisions_log.json (отдельным metadata-файлом).

Целевая раскладка (одна версия v001):

  versions/v001/
    01_input/legacy_bundle/<rel>     <- все исходники (pdf/md/ocr/result/…) как есть
    03_analysis/latest/<name>        <- 03_findings.json / 01 / 02 / pipeline_log /
                                        norm_checks / optimization — если есть
    03_analysis/runs/run_legacy_preserve_<ts>/<rel>  <- значимые analysis-файлы (.json/.jsonl)
    04_review/kb_decisions_link.json <- если есть связь с базой знаний
    99_service/legacy_output/<rel>   <- ПОЛНАЯ копия каждого legacy _output/
    version.json

version.json:
  legacy_partial (есть findings/анализ):
    analysis_status=legacy_partial, analysis_generation=legacy,
    preserve_reason=king_sons_legacy_findings_preserve,
    source_files_strategy=legacy_bundle, primary_goal=preserve_findings_and_kb_links
  source_only (только исходники, без анализа):
    analysis_status=source_only, analysis_generation=legacy,
    preserve_reason=king_sons_source_only_legacy_bundle,
    source_files_strategy=legacy_bundle

Правила:
  * НЕ создаём фейковые _ocr.html / 03_findings.json / иные отсутствующие файлы;
  * неоднозначные исходники сохраняем в legacy_bundle (ничего не выкидываем);
  * весь _output сохраняем в 99_service/legacy_output;
  * 03_findings.json, если есть — обязательно в 03_analysis/latest;
  * саму knowledge_base НЕ меняем (только ссылку);
  * old_to_new_map.json обновляем по всем скопированным файлам + checksum.

READ-ONLY по отношению к `projects/` и `knowledge_base/`. Без `--execute` —
только dry-run (ничего не копирует).

Безопасность: мигрирует ТОЛЬКО проекты, попадающие под King&Sons legacy-политику
(см. analyze_blocked_manual_projects.is_king_sons_legacy). Иначе — отказ.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v2lib  # noqa: E402
import analyze_blocked_manual_projects as abm  # noqa: E402

POLICY = abm.POLICY_READY_LEGACY_FINDINGS_PRESERVE
MIGRATION_KIND = "legacy_findings_preserve"

# артефакты, которые тянем в 03_analysis/latest при наличии (без фейков)
LATEST_PICK = (
    "03_findings.json",
    "02_text_analysis.json",
    "01_blocks_analysis.json",
    "pipeline_log.json",
    "norm_checks.json",
    "optimization.json",
)
# наличие любого из них => legacy_partial; иначе source_only
ANALYSIS_MARKERS = ("03_findings.json", "02_text_analysis.json", "01_blocks_analysis.json")
# каталоги, которые НЕ попадают в legacy_bundle (бэкапы/кеши вне _output)
_SKIP_BUNDLE_MARKERS = ("_bench_backup", ".bak_", "_backup")
# для значимой копии в runs/ берём только лёгкие текстовые артефакты
_RUN_SIGNIFICANT_EXT = {".json", ".jsonl"}
# sha считаем только для текстовых артефактов (png-блоки трекаем без sha)
_SHA_EXT = {".json", ".md", ".txt", ".html", ".csv", ".xlsx", ".jsonl", ".log"}


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _under_skip_bundle(rel_parts: tuple[str, ...]) -> bool:
    return any(any(m in part for m in _SKIP_BUNDLE_MARKERS) for part in rel_parts)


def _role_for_source(name: str) -> str:
    low = name.lower()
    if low.endswith(".pdf"):
        return "input:pdf"
    if low.endswith("_document.md"):
        return "input:document_md"
    if low.endswith("_ocr.html"):
        return "input:ocr_html"
    if low.endswith("_result.json"):
        return "input:result_json"
    if low.endswith("_annotation.json"):
        return "input:annotation_json"
    if name == "project_info.json":
        return "input:project_info"
    if name == v2lib.VERSION_GROUP_FILENAME:
        return "input:version_group"
    return "bundle:other"


def _copy(src: Path, dst: Path, role: str) -> dict:
    with_sha = src.suffix.lower() in _SHA_EXT
    return v2lib._copy_file_tracked(src, dst, role, with_sha=with_sha)


def find_output_dirs(project_path: Path) -> list[Path]:
    """Все каталоги `_output` в дереве проекта (любой глубины)."""
    return sorted(d for d in project_path.rglob("*")
                  if d.is_dir() and d.name == "_output")


def plan_project(project_path: Path) -> dict:
    """Read-only анализ того, что будет скопировано (для dry-run и execute)."""
    project_path = project_path.resolve()
    output_dirs = find_output_dirs(project_path)
    output_dir_parts = [od.relative_to(project_path).parts for od in output_dirs]

    def _in_output(rel_parts: tuple[str, ...]) -> bool:
        return "_output" in rel_parts

    bundle: list[Path] = []
    skipped_bundle: list[Path] = []
    for f in sorted(project_path.rglob("*")):
        if not f.is_file():
            continue
        rel = f.relative_to(project_path)
        if _in_output(rel.parts):
            continue  # _output обрабатывается отдельно (legacy_output)
        if _under_skip_bundle(rel.parts):
            skipped_bundle.append(f)
            continue
        bundle.append(f)

    # latest-кандидаты: для каждого имени — все вхождения в _output (depth 1)
    latest_candidates: dict[str, list[Path]] = {}
    for od in output_dirs:
        for name in LATEST_PICK:
            cand = od / name
            if cand.is_file():
                latest_candidates.setdefault(name, []).append(cand)

    analysis_present = {m: (m in latest_candidates) for m in ANALYSIS_MARKERS}
    has_analysis = any(analysis_present.values())

    return {
        "project_path": project_path,
        "output_dirs": output_dirs,
        "output_dir_parts": output_dir_parts,
        "bundle_files": bundle,
        "skipped_bundle_files": skipped_bundle,
        "latest_candidates": latest_candidates,
        "analysis_present": analysis_present,
        "has_analysis": has_analysis,
    }


def _pick_latest(cands: list[Path]) -> Path:
    """Из нескольких вхождений выбираем наиболее полное (по размеру), детерминированно."""
    return sorted(cands, key=lambda p: (p.stat().st_size, str(p)))[-1]


def migrate_one(project_path: Path, v2_root: Path, *, objects_map: dict,
                decisions: list, execute: bool) -> dict:
    project_path = project_path.resolve()
    discipline = project_path.parent.name
    object_dir = project_path.parent.parent
    object_name = object_dir.name

    if abm.classify_blocked_manual(object_name) != POLICY:
        raise SystemExit(f"REFUSED: {object_name} не подпадает под {POLICY} "
                         f"(мигрируем только King&Sons legacy)")

    object_id = v2lib.object_id_for(object_dir, objects_map)
    document_code = v2lib.document_code_for(project_path)
    kind = "container" if v2lib.is_version_container(project_path) else "plain"
    plan = plan_project(project_path)

    has_analysis = plan["has_analysis"]
    run_id = "run_legacy_preserve_" + _utc_stamp()
    kb_items = abm.kb_entries_for(decisions, document_code)

    obj_root = v2lib.allocate_object_folder(v2_root, object_id, object_name)
    doc_dir = (obj_root / "disciplines" / v2lib.safe_component(discipline)
               / "documents" / v2lib.safe_component(document_code))
    vroot = doc_dir / "versions" / "v001"

    summary = {
        "object_id": object_id,
        "object_name": object_name,
        "discipline": discipline,
        "document_code": document_code,
        "kind": kind,
        "legacy_project_path": str(project_path),
        "v2_document_dir": str(doc_dir),
        "version_id": "v001",
        "migration_kind": MIGRATION_KIND,
        "policy": POLICY,
        "has_03_findings": plan["analysis_present"]["03_findings.json"],
        "has_01_text_analysis": plan["analysis_present"]["02_text_analysis.json"],
        "has_02_blocks_analysis": plan["analysis_present"]["01_blocks_analysis.json"],
        "kb_linked": bool(kb_items),
        "kb_entries": len(kb_items),
        "analysis_status": "legacy_partial" if has_analysis else "source_only",
        "bundle_files_count": len(plan["bundle_files"]),
        "legacy_output_dirs": [str(p) for p in plan["output_dirs"]],
        "skipped_bundle_files": [str(p) for p in plan["skipped_bundle_files"]],
        "run_id": run_id,
        "executed": execute,
    }

    if not execute:
        summary["latest_will_copy"] = sorted(plan["latest_candidates"].keys())
        return summary

    # ---------------- EXECUTE ----------------
    for sub in v2lib.VERSION_SUBDIRS:
        (vroot / sub).mkdir(parents=True, exist_ok=True)

    files: list[dict] = []

    # 1) legacy_bundle — все исходники как есть (сохраняя относительный путь)
    bundle_root = vroot / "01_input" / "legacy_bundle"
    for src in plan["bundle_files"]:
        rel = src.relative_to(project_path)
        files.append(_copy(src, bundle_root / rel, _role_for_source(src.name)))

    # 2) 99_service/legacy_output — ПОЛНАЯ копия каждого _output (verbatim, вкл. бэкапы)
    legacy_output_root = vroot / "99_service" / "legacy_output"
    for od in plan["output_dirs"]:
        od_rel = od.relative_to(project_path)  # напр. 133_23-ГК-ЭМ2/_output
        for src in sorted(od.rglob("*")):
            if src.is_file():
                rel = src.relative_to(project_path)
                files.append(_copy(src, legacy_output_root / rel, "legacy_output"))

    # 3) 03_analysis/latest — ключевые артефакты (без фейков)
    latest_dir = vroot / "03_analysis" / "latest"
    latest_copied: dict[str, str] = {}
    for name, cands in plan["latest_candidates"].items():
        chosen = _pick_latest(cands)
        files.append(_copy(chosen, latest_dir / name, "analysis:latest"))
        latest_copied[name] = str(chosen)

    # 4) 03_analysis/runs/<run_id> — значимые analysis-файлы (.json/.jsonl, depth 1 _output)
    run_dir = vroot / "03_analysis" / "runs" / run_id
    for od in plan["output_dirs"]:
        for src in sorted(od.iterdir()):
            if src.is_file() and src.suffix.lower() in _RUN_SIGNIFICANT_EXT:
                rel = src.relative_to(project_path)
                files.append(_copy(src, run_dir / rel, "run"))

    # 5) 04_review/kb_decisions_link.json — связь с базой знаний (БЕЗ изменения KB)
    if kb_items:
        kb_link = {
            "schema_version": 1,
            "document_code": document_code,
            "object_id": object_id,
            "source_project": document_code,
            "knowledge_base_file": "knowledge_base/decisions_log.json",
            "entry_count": len(kb_items),
            "entries": kb_items,
            "note": ("Ссылка на knowledge_base/decisions_log.json. "
                     "База знаний НЕ изменялась."),
            "generated_at": v2lib.utc_now_iso(),
        }
        kb_link_path = vroot / "04_review" / "kb_decisions_link.json"
        kb_link_path.parent.mkdir(parents=True, exist_ok=True)
        kb_link_path.write_text(json.dumps(kb_link, ensure_ascii=False, indent=2),
                                encoding="utf-8")
        # сгенерированная metadata-ссылка (НЕ копия legacy-файла): sha256=None,
        # чтобы validate не считал её legacy-копией и не сверял с decisions_log.
        files.append({
            "old_path": str(v2_root.parent / "knowledge_base" / "decisions_log.json"),
            "new_path": str(kb_link_path),
            "sha256": None,
            "bytes": kb_link_path.stat().st_size,
            "role": "review:kb_link",
            "generated": True,
        })

    # 6) version.json
    if has_analysis:
        version_json = {
            "analysis_status": "legacy_partial",
            "analysis_generation": "legacy",
            "preserve_reason": "king_sons_legacy_findings_preserve",
            "source_files_strategy": "legacy_bundle",
            "primary_goal": "preserve_findings_and_kb_links",
        }
    else:
        version_json = {
            "analysis_status": "source_only",
            "analysis_generation": "legacy",
            "preserve_reason": "king_sons_source_only_legacy_bundle",
            "source_files_strategy": "legacy_bundle",
        }
    version_json.update({
        "schema_version": 1,
        "version_id": "v001",
        "version_no": 1,
        "label": "V1",
        "migration_kind": MIGRATION_KIND,
        "legacy_folder_name": project_path.name,
        "legacy_folder_path": str(project_path),
        "kind": kind,
        "analysis_run_id": run_id,
        "analysis_latest_present": sorted(latest_copied.keys()),
        "kb_linked": bool(kb_items),
        "kb_entries": len(kb_items),
        "legacy_output_dirs": [str(p.relative_to(project_path)) for p in plan["output_dirs"]],
        "migrated_at": v2lib.utc_now_iso(),
    })
    (vroot / "version.json").write_text(
        json.dumps(version_json, ensure_ascii=False, indent=2), encoding="utf-8")

    # 7) document.json + current_version.txt
    document_json = {
        "schema_version": 1,
        "document_code": document_code,
        "object_id": object_id,
        "discipline": discipline,
        "kind": kind,
        "migration_kind": MIGRATION_KIND,
        "legacy_project_name": project_path.name,
        "legacy_project_path": str(project_path),
        "versions": [{"version_id": "v001", "version_no": 1, "label": "V1",
                      "legacy_folder_name": project_path.name}],
        "current_version": "v001",
        "migrated_at": v2lib.utc_now_iso(),
    }
    (doc_dir / "document.json").write_text(
        json.dumps(document_json, ensure_ascii=False, indent=2), encoding="utf-8")
    (doc_dir / "current_version.txt").write_text("v001\n", encoding="utf-8")

    # 8) object.json (если ещё нет)
    obj_json = obj_root / "object.json"
    if not obj_json.exists():
        obj_json.write_text(json.dumps({
            "schema_version": 1,
            "object_id": object_id,
            "display_name": object_name,
            "folder_name": obj_root.name,
            "legacy_name": object_name,
            "legacy_path": str(object_dir),
            "created_at": v2lib.utc_now_iso(),
        }, ensure_ascii=False, indent=2), encoding="utf-8")

    # 9) checksum verify (re-sha каждой копии, сверка с записанным)
    checksum_checked = 0
    checksum_errors: list[str] = []
    for f in files:
        if f.get("sha256") is None:
            continue
        actual = v2lib.sha256_file(Path(f["new_path"]))
        checksum_checked += 1
        if actual != f["sha256"]:
            checksum_errors.append(f["new_path"])

    summary.update({
        "files_copied": len(files),
        "checksum_checked": checksum_checked,
        "checksum_errors": checksum_errors,
        "latest_copied": latest_copied,
        "legacy_bundle_dir": str(bundle_root),
        "legacy_output_dir": str(legacy_output_root),
        "version_json": version_json,
        "map_record": {
            "object_id": object_id,
            "object_name": object_name,
            "discipline": discipline,
            "document_code": document_code,
            "kind": kind,
            "migration_kind": MIGRATION_KIND,
            "legacy_project_path": str(project_path),
            "legacy_folder_path": str(project_path),
            "v2_document_dir": str(doc_dir),
            "version_id": "v001",
            "version_no": 1,
            "analysis_run_id": run_id,
            "analysis_status": version_json["analysis_status"],
            "files": files,
            "migrated_at": v2lib.utc_now_iso(),
        },
    })
    return summary


TARGET_LEGACY_PATHS = (
    'EOM/133_23-ГК-ЭМ2(main)',
    'EOM/Фасадное освещение',
    'ITP/133_23-ГК-ИТП.ТМ',
    'SS/133_23-ГК-АК(main)',
)


def default_targets(legacy_root: Path) -> list[Path]:
    base = legacy_root / '213. Мосфильмовская 31А "King&Sons"'
    return [base / rel for rel in TARGET_LEGACY_PATHS]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Мигрировать King&Sons blocked/manual проекты как legacy snapshot")
    ap.add_argument("--v2-root", default=None)
    ap.add_argument("--legacy-root", default=None)
    ap.add_argument("--project", action="append", default=None,
                    help="конкретный legacy-путь проекта (можно несколько); "
                         "по умолчанию — 4 целевых King&Sons")
    ap.add_argument("--legacy-findings-preserve", action="store_true",
                    help="ОБЯЗАТЕЛЬНЫЙ явный флаг политики (без него миграция запрещена)")
    ap.add_argument("--execute", action="store_true",
                    help="реально копировать (без флага — только dry-run)")
    args = ap.parse_args(argv)

    if not args.legacy_findings_preserve:
        print("[REFUSED] нужен явный флаг --legacy-findings-preserve", file=sys.stderr)
        return 2

    v2_root = Path(args.v2_root).resolve() if args.v2_root else v2lib.projects_v2_root()
    legacy_root = (Path(args.legacy_root).resolve() if args.legacy_root
                   else v2lib.legacy_projects_root())
    v2lib.ensure_v2_skeleton(v2_root)

    targets = ([Path(p).resolve() for p in args.project] if args.project
               else default_targets(legacy_root))

    objects_map = v2lib.load_objects_map()
    kb_file = v2_root.parent / "knowledge_base" / "decisions_log.json"
    decisions = []
    if kb_file.exists():
        try:
            decisions = json.loads(kb_file.read_text(encoding="utf-8")).get("entries", [])
        except Exception:
            decisions = []

    map_path = v2_root / "_system" / "old_to_new_map.json"
    map_obj = v2lib.load_old_to_new_map(map_path)

    mode = "EXECUTE" if args.execute else "DRY-RUN"
    print(f"=== migrate_legacy_findings_preserve [{mode}] ===")
    print(f"v2_root={v2_root}")
    print(f"targets={len(targets)}\n")

    results = []
    total_files = total_checksum = 0
    for t in targets:
        if not t.is_dir():
            print(f"  MISSING legacy path: {t}")
            return 1
        res = migrate_one(t, v2_root, objects_map=objects_map,
                          decisions=decisions, execute=args.execute)
        results.append(res)
        if args.execute:
            v2lib.upsert_migration(map_obj, res["map_record"])
            total_files += res["files_copied"]
            total_checksum += res["checksum_checked"]
            if res["checksum_errors"]:
                print(f"  CHECKSUM ERRORS in {res['document_code']}: {res['checksum_errors']}")
                return 1
        print(f"  [{res['analysis_status']}] {res['discipline']}/{res['document_code']} "
              f"({res['kind']})")
        print(f"      03_findings={res['has_03_findings']} 01={res['has_01_text_analysis']} "
              f"02={res['has_02_blocks_analysis']} kb={res['kb_entries']}")
        print(f"      bundle_files={res['bundle_files_count']} "
              f"_output_dirs={len(res['legacy_output_dirs'])} "
              + (f"files_copied={res.get('files_copied','-')} "
                 f"checksum={res.get('checksum_checked','-')}" if args.execute
                 else f"latest_will_copy={res.get('latest_will_copy')}"))

    if args.execute:
        v2lib.save_old_to_new_map(map_obj, map_path)
        report = {
            "schema_version": 1,
            "generated_at": v2lib.utc_now_iso(),
            "policy": POLICY,
            "migrated": len(results),
            "files_copied_total": total_files,
            "checksum_checked_total": total_checksum,
            "projects": [{k: v for k, v in r.items() if k != "map_record"} for r in results],
        }
        rep_path = v2_root / "_system" / "legacy_findings_preserve_report.json"
        rep_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\nmigrated={len(results)} files_copied={total_files} "
              f"checksum_checked={total_checksum}")
        print(f"-> {map_path}")
        print(f"-> {rep_path}")
    else:
        print("\n(dry-run: ничего не скопировано; добавьте --execute)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
