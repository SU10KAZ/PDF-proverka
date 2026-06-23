#!/usr/bin/env python3
"""
analyze_blocked_manual_projects.py — READ-ONLY разбор оставшихся manual/blocked
проектов (WARNINGS_BLOCKED / MANUAL_REVIEW_REQUIRED) с применением политики
сохранения legacy-замечаний King&Sons.

Политика `POLICY_READY_LEGACY_FINDINGS_PRESERVE`:
  * не блокировать перенос из-за multiple PDF/MD/result / incomplete quad;
  * не угадывать «основной» PDF, если рискованно;
  * сохранить ВСЕ исходные файлы как legacy_bundle;
  * сохранить найденные замечания (03_findings.json и т.п.);
  * сохранить связь с knowledge_base/decisions_log.json;
  * сохранить полный legacy `_output/` (legacy_output) для восстановления контекста.

Целевая структура (для будущей миграции, СЕЙЧАС НЕ выполняется):
  versions/v001/
    01_input/legacy_bundle/   <- все pdf/md/ocr/result как есть
    03_analysis/latest/       <- 03_findings.json / 01 / 02 / pipeline_log если есть
    99_service/legacy_output/ <- полная копия legacy _output/
  version.json:
    analysis_status=legacy_partial, analysis_generation=legacy,
    preserve_reason=king_sons_legacy_findings_preserve,
    source_files_strategy=legacy_bundle,
    primary_goal=preserve_findings_and_kb_links

Пишет ТОЛЬКО:
  projects_v2/_system/blocked_manual_analysis_report.json
  projects_v2/_system/blocked_manual_analysis_report.csv

Ничего не копирует и не мигрирует. legacy/comparison не трогает.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v2lib  # noqa: E402

POLICY_READY_LEGACY_FINDINGS_PRESERVE = "POLICY_READY_LEGACY_FINDINGS_PRESERVE"
POLICY_STILL_MANUAL = "POLICY_STILL_MANUAL"

# исходные файлы (legacy bundle) распознаём по суффиксам
_SOURCE_SUFFIXES = {
    "pdf": ".pdf",
    "document_md": "_document.md",
    "ocr_html": "_ocr.html",
    "result_json": "_result.json",
    "annotation_json": "_annotation.json",
}
# артефакты анализа, которые тянем в 03_analysis/latest при наличии
_ANALYSIS_PICK = ("03_findings.json", "01_text_analysis.json",
                  "02_blocks_analysis.json", "pipeline_log.json")
# каталоги, которые НЕ тащим в bundle (бэкапы/кеши)
_SKIP_DIR_MARKERS = ("_bench_backup", ".bak_", "_backup", "cache", "/raw", "/prompts")


def is_king_sons_legacy(object_name: str) -> bool:
    o = object_name or ""
    return o.strip().startswith("213") or "King&Sons" in o or "Мосфильмов" in o


def kb_entries_for(decisions: list, document_code: str) -> list[dict]:
    """Записи decisions_log по document_code или его logical base."""
    base = document_code
    # срез хвостовых V<n>/.pdf уже в document_code не нужен, но подстрахуемся
    candidates = {document_code, document_code.rstrip()}
    out = []
    for e in decisions:
        sp = str(e.get("source_project") or "")
        if sp and (sp in candidates or sp == base):
            out.append({
                "item_id": e.get("item_id"), "section": e.get("section"),
                "severity": e.get("severity"), "summary": (e.get("summary") or "")[:160],
                "expert_decision": e.get("expert_decision"),
            })
    return out


def _under_skip_dir(p: Path) -> bool:
    s = str(p)
    return any(m in s for m in _SKIP_DIR_MARKERS)


def gather_legacy_inventory(project_path: Path) -> dict:
    """Собирает legacy-инвентарь (read-only): исходные файлы, _output, анализ."""
    source_files: list[str] = []
    by_role: dict[str, list[str]] = {k: [] for k in _SOURCE_SUFFIXES}
    other_files: list[str] = []
    output_dirs: list[str] = []
    analysis_present: dict[str, bool] = {a: False for a in _ANALYSIS_PICK}

    if not project_path.is_dir():
        return {"source_files": [], "by_role": by_role, "unclassified_files": [],
                "output_dirs": [], "analysis_present": analysis_present}

    for p in sorted(project_path.rglob("*")):
        rel = str(p.relative_to(project_path))
        if p.is_dir():
            if p.name == "_output":
                output_dirs.append(rel)
            continue
        if _under_skip_dir(p):
            continue
        # анализ-артефакты (в любом _output)
        if "_output" in p.parts and p.name in analysis_present:
            analysis_present[p.name] = True
        # исходные файлы вне _output
        if "_output" in p.parts:
            continue
        name = p.name
        matched = None
        for role, suf in _SOURCE_SUFFIXES.items():
            if name.lower().endswith(suf):
                matched = role
                break
        if matched:
            by_role[matched].append(rel)
            source_files.append(rel)
        elif name in ("project_info.json", "version_group.json", "client.log"):
            source_files.append(rel)  # служебные — тоже в bundle
        else:
            other_files.append(rel)
            source_files.append(rel)

    return {
        "source_files": source_files, "by_role": by_role,
        "unclassified_files": other_files, "output_dirs": output_dirs,
        "analysis_present": analysis_present,
    }


def classify_blocked_manual(object_name: str) -> str:
    """King&Sons legacy -> preserve-as-legacy-bundle; иначе остаётся manual."""
    return (POLICY_READY_LEGACY_FINDINGS_PRESERVE if is_king_sons_legacy(object_name)
            else POLICY_STILL_MANUAL)


def proposed_version_json() -> dict:
    return {
        "analysis_status": "legacy_partial",
        "analysis_generation": "legacy",
        "preserve_reason": "king_sons_legacy_findings_preserve",
        "source_files_strategy": "legacy_bundle",
        "primary_goal": "preserve_findings_and_kb_links",
    }


def analyze(v2_root: Path) -> dict:
    wp = json.loads((v2_root / "_system" / "migration_warning_policy_report.json").read_text(encoding="utf-8"))
    targets = [p for p in wp.get("projects", [])
               if p.get("policy_group") == "WARNINGS_BLOCKED"
               or p.get("readiness_group") == "MANUAL_REVIEW_REQUIRED"]

    kb_file = v2_root.parent / "knowledge_base" / "decisions_log.json"
    decisions = []
    if kb_file.exists():
        try:
            decisions = json.loads(kb_file.read_text(encoding="utf-8")).get("entries", [])
        except Exception:
            decisions = []

    rows = []
    for p in targets:
        legacy_path = Path(p.get("legacy_path") or "")
        document_code = p.get("document_code") or ""
        object_name = p.get("object") or ""
        inv = gather_legacy_inventory(legacy_path)
        kb_items = kb_entries_for(decisions, document_code)
        ambiguous = {role: len(files) for role, files in inv["by_role"].items() if len(files) > 1}
        policy = classify_blocked_manual(object_name)
        rows.append({
            "object": object_name, "discipline": p.get("discipline"),
            "document_code": document_code, "kind": p.get("kind"),
            "legacy_path": str(legacy_path), "blockers": p.get("blockers", []),
            "has_03_findings": inv["analysis_present"]["03_findings.json"],
            "has_01_text_analysis": inv["analysis_present"]["01_text_analysis.json"],
            "has_02_blocks_analysis": inv["analysis_present"]["02_blocks_analysis.json"],
            "has_pipeline_log": inv["analysis_present"]["pipeline_log.json"],
            "kb_entries": len(kb_items), "kb_linked": bool(kb_items),
            "kb_items": kb_items,
            "legacy_source_files_count": len(inv["source_files"]),
            "legacy_source_files": inv["source_files"],
            "ambiguous_roles": ambiguous,           # напр. {"pdf":4}
            "unclassified_files": inv["unclassified_files"],
            "legacy_output_dirs": inv["output_dirs"],
            "proposed_policy": policy,
            "proposed_analysis_status": "legacy_partial" if policy == POLICY_READY_LEGACY_FINDINGS_PRESERVE else None,
            "proposed_version_json": proposed_version_json() if policy == POLICY_READY_LEGACY_FINDINGS_PRESERVE else None,
            "target_structure": ("versions/v001/{01_input/legacy_bundle/, "
                                 "03_analysis/latest/, 99_service/legacy_output/}"),
            "migrate_now": False,  # миграция только после отдельного подтверждения
        })
    return {"rows": rows, "kb_total": len(decisions)}


CSV_FIELDS = [
    "proposed_policy", "object", "discipline", "document_code", "kind",
    "has_03_findings", "has_01_text_analysis", "has_02_blocks_analysis", "has_pipeline_log",
    "kb_entries", "kb_linked", "legacy_source_files_count", "ambiguous_roles",
    "unclassified_files", "legacy_output_dirs", "blockers", "proposed_analysis_status",
    "migrate_now", "legacy_path",
]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Read-only analysis of blocked/manual King&Sons legacy projects")
    ap.add_argument("--v2-root", default=None)
    args = ap.parse_args(argv)
    v2_root = Path(args.v2_root).resolve() if args.v2_root else v2lib.projects_v2_root()

    result = analyze(v2_root)
    rows = result["rows"]
    from collections import Counter
    counts = Counter(r["proposed_policy"] for r in rows)
    preserve = [r for r in rows if r["proposed_policy"] == POLICY_READY_LEGACY_FINDINGS_PRESERVE]

    summary = {
        "total_blocked_manual": len(rows),
        "policy_counts": dict(counts),
        "legacy_findings_preserve": len(preserve),
        "with_03_findings": sum(1 for r in rows if r["has_03_findings"]),
        "with_kb_links": sum(1 for r in rows if r["kb_linked"]),
        "migrate_now": False,
    }

    sys_dir = v2_root / "_system"
    sys_dir.mkdir(parents=True, exist_ok=True)
    (sys_dir / "blocked_manual_analysis_report.json").write_text(json.dumps({
        "schema_version": 1, "generated_at": v2lib.utc_now_iso(),
        "summary": summary, "projects": rows,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    with open(sys_dir / "blocked_manual_analysis_report.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        w.writeheader()
        for r in rows:
            row = {k: r.get(k, "") for k in CSV_FIELDS}
            row["ambiguous_roles"] = ";".join(f"{k}:{v}" for k, v in r["ambiguous_roles"].items())
            row["unclassified_files"] = ";".join(r["unclassified_files"])
            row["legacy_output_dirs"] = ";".join(r["legacy_output_dirs"])
            row["blockers"] = ";".join(r["blockers"])
            w.writerow(row)

    print("=== blocked/manual legacy-preserve analysis ===")
    print(f"total: {summary['total_blocked_manual']}  | policy: {summary['policy_counts']}")
    print(f"legacy_findings_preserve: {summary['legacy_findings_preserve']}  "
          f"with_03_findings: {summary['with_03_findings']}  with_kb_links: {summary['with_kb_links']}")
    print(f"migrate_now: {summary['migrate_now']} (миграция только после подтверждения)")
    print()
    for r in rows:
        print(f"  [{r['proposed_policy']}] {r['discipline']}/{r['document_code']}")
        print(f"      blockers={r['blockers']} ambiguous={r['ambiguous_roles']}")
        print(f"      03_findings={r['has_03_findings']} 01={r['has_01_text_analysis']} "
              f"02={r['has_02_blocks_analysis']} pipeline_log={r['has_pipeline_log']}")
        print(f"      kb_entries={r['kb_entries']} kb_linked={r['kb_linked']} "
              f"source_files={r['legacy_source_files_count']} _output_dirs={len(r['legacy_output_dirs'])} "
              f"unclassified={len(r['unclassified_files'])}")
    print()
    print(f"-> {sys_dir / 'blocked_manual_analysis_report.json'}")
    print(f"-> {sys_dir / 'blocked_manual_analysis_report.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
