#!/usr/bin/env python3
"""
analyze_need_policy_projects.py — READ-ONLY разбор проектов
`WARNINGS_NEED_POLICY` и подготовка политики их будущей миграции.

Читает (не меняет):
  projects_v2/_system/migration_warning_policy_report.json
  projects_v2/_system/migration_readiness_report.json
  knowledge_base/decisions_log.json (для KB-связи legacy King&Sons)
  legacy projects/ — только чтение (siblings, наличие analysis-файлов)

Пишет ТОЛЬКО:
  projects_v2/_system/need_policy_analysis_report.json
  projects_v2/_system/need_policy_analysis_report.csv

Ничего не копирует, не мигрирует, legacy/comparison не трогает.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v2lib  # noqa: E402

# подгруппы
POLICY_READY_MISSING_OCR_HTML = "POLICY_READY_MISSING_OCR_HTML"
POLICY_READY_NO_ANALYSIS = "POLICY_READY_NO_ANALYSIS"
POLICY_READY_PARTIAL_ANALYSIS = "POLICY_READY_PARTIAL_ANALYSIS"
POLICY_READY_SINGLE_PDF_NAMED_FOLDER = "POLICY_READY_SINGLE_PDF_NAMED_FOLDER"
POLICY_READY_GROUPED_VERSIONS_WITHOUT_MAIN = "POLICY_READY_GROUPED_VERSIONS_WITHOUT_MAIN"
POLICY_READY_LEGACY_KB_PRESERVE = "POLICY_READY_LEGACY_KB_PRESERVE"
POLICY_NEEDS_MANUAL_VERSION_GROUPING = "POLICY_NEEDS_MANUAL_VERSION_GROUPING"
POLICY_RECHECK_AS_BLOCKED = "POLICY_RECHECK_AS_BLOCKED"

SUBGROUPS = (
    POLICY_READY_MISSING_OCR_HTML,
    POLICY_READY_NO_ANALYSIS,
    POLICY_READY_PARTIAL_ANALYSIS,
    POLICY_READY_SINGLE_PDF_NAMED_FOLDER,
    POLICY_READY_GROUPED_VERSIONS_WITHOUT_MAIN,
    POLICY_READY_LEGACY_KB_PRESERVE,
    POLICY_NEEDS_MANUAL_VERSION_GROUPING,
    POLICY_RECHECK_AS_BLOCKED,
)

_READY = {
    POLICY_READY_MISSING_OCR_HTML, POLICY_READY_NO_ANALYSIS,
    POLICY_READY_PARTIAL_ANALYSIS, POLICY_READY_SINGLE_PDF_NAMED_FOLDER,
    POLICY_READY_GROUPED_VERSIONS_WITHOUT_MAIN, POLICY_READY_LEGACY_KB_PRESERVE,
}

_ANALYSIS_FILES = (
    "02_text_analysis.json", "01_blocks_analysis.json", "03_findings.json",
    "norm_checks.json", "optimization.json", "pipeline_log.json", "audit_log.jsonl",
)


def logical_base(name: str) -> tuple[str, Optional[int]]:
    """`X V1.pdf` -> ('X', 1); `X.pdf` -> ('X', None)."""
    n = re.sub(r"\.pdf$", "", name, flags=re.IGNORECASE)
    m = re.match(r"^(.*?)[\s_]*[Vv](\d+)\s*$", n)
    if m:
        return m.group(1).strip(" _-"), int(m.group(2))
    return n.strip(), None


# ---------------------------------------------------------------------------
# чистая классификация
# ---------------------------------------------------------------------------


def classify_need_policy(s: dict) -> dict:
    """Возвращает {subgroup, proposed_analysis_status, can_migrate_auto,
    proposed_next_class, legacy_kb_preserve}."""
    has_quad = s.get("has_pdf") and s.get("has_document_md") and s.get("has_result_json")
    incomplete = not has_quad

    blockers = []
    if s.get("multiple_pdf"):
        blockers.append("multiple_pdf")
    if s.get("multiple_document_md"):
        blockers.append("multiple_document_md")
    if s.get("multiple_result_json"):
        blockers.append("multiple_result_json")
    if not s.get("has_project_info"):
        blockers.append("missing_project_info")
    if s.get("document_code_conflict"):
        blockers.append("document_code_conflict")
    if incomplete:
        blockers.append("incomplete_input_quad")

    h01, h02, h03 = s.get("has_01"), s.get("has_02"), s.get("has_03")
    if h01 and h02 and h03:
        analysis_status = "complete"
    elif h01 or h02 or h03:
        analysis_status = "partial"
    else:
        analysis_status = "none"

    has_any_legacy_analysis = bool(h01 or h02 or h03 or s.get("has_pipeline_log"))
    legacy_kb_preserve = bool(
        s.get("legacy_generation")
        and (s.get("kb_linked") or has_any_legacy_analysis)
        and analysis_status != "complete"
    )

    def ready(subgroup, status):
        return {"subgroup": subgroup, "proposed_analysis_status": status,
                "can_migrate_auto": True, "proposed_next_class": "WARNINGS_AUTO_CANDIDATE",
                "legacy_kb_preserve": legacy_kb_preserve}

    def manual(subgroup):
        return {"subgroup": subgroup, "proposed_analysis_status": analysis_status,
                "can_migrate_auto": False, "proposed_next_class": "MANUAL_REVIEW_REQUIRED",
                "legacy_kb_preserve": legacy_kb_preserve}

    # 1. blockers -> вернуть в blocked
    if blockers:
        return {"subgroup": POLICY_RECHECK_AS_BLOCKED, "proposed_analysis_status": analysis_status,
                "can_migrate_auto": False, "proposed_next_class": "WARNINGS_BLOCKED",
                "blockers": blockers, "legacy_kb_preserve": legacy_kb_preserve}

    # 2. legacy King&Sons с KB-данными — сохранить как legacy-снимок
    if legacy_kb_preserve:
        return ready(POLICY_READY_LEGACY_KB_PRESERVE, "legacy_partial")

    # 3. структурная группировка .pdf-папок без version_group
    if s.get("pdf_named") and not s.get("has_version_group"):
        sib = int(s.get("sibling_count") or 1)
        if sib <= 1:
            return ready(POLICY_READY_SINGLE_PDF_NAMED_FOLDER, analysis_status)
        if s.get("sibling_unambiguous"):
            return ready(POLICY_READY_GROUPED_VERSIONS_WITHOUT_MAIN, analysis_status)
        return manual(POLICY_NEEDS_MANUAL_VERSION_GROUPING)

    # 4. отсутствует только _ocr.html (входной комплект полный)
    if not s.get("has_ocr_html"):
        return ready(POLICY_READY_MISSING_OCR_HTML, analysis_status)

    # 5/6. анализ
    if analysis_status == "none":
        return ready(POLICY_READY_NO_ANALYSIS, "none")
    if analysis_status == "partial":
        return ready(POLICY_READY_PARTIAL_ANALYSIS, "partial")

    # 7. fallback — что-то ещё (например object_not_in_registry) -> manual
    return manual(POLICY_NEEDS_MANUAL_VERSION_GROUPING)


# ---------------------------------------------------------------------------
# сбор сигналов (read-only filesystem)
# ---------------------------------------------------------------------------


def load_kb_source_projects(root: Path) -> set:
    f = root / "knowledge_base" / "decisions_log.json"
    out: set = set()
    if not f.exists():
        return out
    try:
        data = json.loads(f.read_text(encoding="utf-8"))
    except Exception:
        return out
    rows = data.get("entries", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
    for r in rows:
        if isinstance(r, dict) and r.get("source_project"):
            out.add(str(r["source_project"]).strip())
    return out


def detect_version_siblings(legacy_path: Path) -> tuple[int, bool, str]:
    """(sibling_count_incl_self, unambiguous, base). Для .pdf-named папок."""
    folder = legacy_path
    parent = folder.parent
    base, _ = logical_base(folder.name)
    sibs = []
    if parent.is_dir():
        for d in sorted(parent.iterdir()):
            if not d.is_dir():
                continue
            b, n = logical_base(d.name)
            if b == base and (n is not None or d.name == folder.name):
                sibs.append((d.name, n))
    if not sibs:
        sibs = [(folder.name, logical_base(folder.name)[1])]
    vnums = [n for _, n in sibs if n is not None]
    unambiguous = len(sibs) >= 2 and len(vnums) == len(sibs) and len(vnums) == len(set(vnums))
    return len(sibs), unambiguous, base


def missing_analysis_files(legacy_path: Path) -> list[str]:
    out_dir = legacy_path / "_output"
    return [n for n in _ANALYSIS_FILES if not (out_dir / n).exists()]


def is_legacy_generation(object_name: str) -> bool:
    o = object_name or ""
    return o.strip().startswith("213") or "King&Sons" in o or "Мосфильмов" in o


def build_signal(wp_row: dict, rd_row: dict, projects_root: Path, kb_set: set) -> dict:
    legacy_path = Path(rd_row.get("legacy_path") or wp_row.get("legacy_path") or "")
    document_code = rd_row.get("document_code") or wp_row.get("document_code") or ""
    object_name = rd_row.get("object") or wp_row.get("object") or ""

    base_code, _ = logical_base(document_code)
    kb_linked = document_code in kb_set or base_code in kb_set

    pdf_named = bool(rd_row.get("pdf_named_version_folder"))
    has_vg = bool(rd_row.get("has_version_group"))
    sibling_count, sibling_unambiguous = 1, False
    if pdf_named and not has_vg and legacy_path:
        sibling_count, sibling_unambiguous, _ = detect_version_siblings(legacy_path)

    miss_analysis = missing_analysis_files(legacy_path) if legacy_path else list(_ANALYSIS_FILES)

    signal = {
        "has_pdf": rd_row.get("has_pdf"),
        "has_document_md": rd_row.get("has_document_md"),
        "has_result_json": rd_row.get("has_result_json"),
        "has_ocr_html": rd_row.get("has_ocr_html"),
        "has_project_info": rd_row.get("has_project_info"),
        "has_01": rd_row.get("has_01_text_analysis"),
        "has_02": rd_row.get("has_02_blocks_analysis"),
        "has_03": rd_row.get("has_03_findings"),
        "has_pipeline_log": rd_row.get("has_pipeline_log"),
        "pdf_named": pdf_named,
        "has_version_group": has_vg,
        "multiple_pdf": rd_row.get("multiple_pdf"),
        "multiple_document_md": rd_row.get("multiple_document_md"),
        "multiple_result_json": rd_row.get("multiple_result_json"),
        "document_code_conflict": rd_row.get("document_code_conflict"),
        "sibling_count": sibling_count,
        "sibling_unambiguous": sibling_unambiguous,
        "legacy_generation": is_legacy_generation(object_name),
        "kb_linked": kb_linked,
    }
    # обогащённые поля для отчёта
    meta = {
        "object": object_name, "discipline": rd_row.get("discipline") or wp_row.get("discipline"),
        "document_code": document_code, "legacy_path": str(legacy_path),
        "kind": rd_row.get("kind") or wp_row.get("kind"),
        "version_count": rd_row.get("version_count"),
        "warning_tags": wp_row.get("warning_tags", []),
        "blockers": wp_row.get("blockers", []),
        "has_pdf": signal["has_pdf"], "has_document_md": signal["has_document_md"],
        "has_ocr_html": signal["has_ocr_html"], "has_result_json": signal["has_result_json"],
        "has_project_info": signal["has_project_info"], "has_output": rd_row.get("has_output"),
        "has_01_text_analysis": signal["has_01"], "has_02_blocks_analysis": signal["has_02"],
        "has_03_findings": signal["has_03"], "has_pipeline_log": signal["has_pipeline_log"],
        "kb_linked": kb_linked, "sibling_count": sibling_count,
        "sibling_unambiguous": sibling_unambiguous,
        "missing_analysis_files": miss_analysis,
        "missing_optional_files": ([] if signal["has_ocr_html"] else ["ocr_html"]),
    }
    return signal, meta


def analyze(projects_root: Path, v2_root: Path) -> dict:
    wp = json.loads((v2_root / "_system" / "migration_warning_policy_report.json").read_text(encoding="utf-8"))
    rd = json.loads((v2_root / "_system" / "migration_readiness_report.json").read_text(encoding="utf-8"))
    rd_index = {(r.get("object_id"), r.get("document_code")): r for r in rd.get("projects", [])}
    kb_set = load_kb_source_projects(v2_root.parent)

    need = [p for p in wp.get("projects", [])
            if p.get("policy_group") == "WARNINGS_NEED_POLICY"
            and p.get("recommendation") == "needs_policy"]

    rows = []
    for p in need:
        rd_row = rd_index.get((p.get("object_id"), p.get("document_code")), {})
        signal, meta = build_signal(p, rd_row, projects_root, kb_set)
        verdict = classify_need_policy(signal)
        rows.append({**meta,
                     "subgroup": verdict["subgroup"],
                     "proposed_policy": verdict["subgroup"],
                     "proposed_next_class": verdict["proposed_next_class"],
                     "proposed_analysis_status": verdict["proposed_analysis_status"],
                     "can_migrate_auto_after_policy": verdict["can_migrate_auto"],
                     "legacy_kb_preserve": verdict["legacy_kb_preserve"],
                     "blockers_detected": verdict.get("blockers", [])})
    return {"rows": rows, "kb_count": len(kb_set)}


# ---------------------------------------------------------------------------


CSV_FIELDS = [
    "subgroup", "object", "discipline", "document_code", "kind", "version_count",
    "can_migrate_auto_after_policy", "proposed_next_class", "proposed_analysis_status",
    "legacy_kb_preserve", "kb_linked",
    "has_pdf", "has_document_md", "has_ocr_html", "has_result_json", "has_project_info",
    "has_output", "has_01_text_analysis", "has_02_blocks_analysis", "has_03_findings",
    "has_pipeline_log", "sibling_count", "sibling_unambiguous",
    "missing_optional_files", "missing_analysis_files", "warning_tags",
    "blockers_detected", "legacy_path",
]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Read-only analysis of WARNINGS_NEED_POLICY projects")
    ap.add_argument("--projects-root", default=None)
    ap.add_argument("--v2-root", default=None)
    args = ap.parse_args(argv)

    projects_root = Path(args.projects_root).resolve() if args.projects_root else v2lib.legacy_projects_root()
    v2_root = Path(args.v2_root).resolve() if args.v2_root else v2lib.projects_v2_root()

    result = analyze(projects_root, v2_root)
    rows = result["rows"]

    from collections import Counter
    counts = Counter(r["subgroup"] for r in rows)
    ready = [r for r in rows if r["can_migrate_auto_after_policy"]]
    manual = [r for r in rows if not r["can_migrate_auto_after_policy"] and r["proposed_next_class"] == "MANUAL_REVIEW_REQUIRED"]
    blocked = [r for r in rows if r["proposed_next_class"] == "WARNINGS_BLOCKED"]
    kb_preserve = [r for r in rows if r["legacy_kb_preserve"]]

    summary = {
        "total_need_policy": len(rows),
        "subgroup_counts": {g: counts.get(g, 0) for g in SUBGROUPS},
        "migratable_next_pilot": len(ready),
        "remain_manual": len(manual),
        "recheck_blocked": len(blocked),
        "legacy_kb_preserve_count": len(kb_preserve),
        "kb_source_projects_total": result["kb_count"],
        "king_sons_legacy_preserve": [
            {"document_code": r["document_code"], "discipline": r["discipline"],
             "has_03_findings": r["has_03_findings"], "kb_linked": r["kb_linked"],
             "missing_analysis_files": r["missing_analysis_files"],
             "proposed_analysis_status": r["proposed_analysis_status"]}
            for r in kb_preserve
        ],
    }

    sys_dir = v2_root / "_system"
    sys_dir.mkdir(parents=True, exist_ok=True)
    (sys_dir / "need_policy_analysis_report.json").write_text(json.dumps({
        "schema_version": 1, "generated_at": v2lib.utc_now_iso(),
        "summary": summary, "projects": rows,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    with open(sys_dir / "need_policy_analysis_report.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        w.writeheader()
        for r in rows:
            row = {k: r.get(k, "") for k in CSV_FIELDS}
            row["missing_optional_files"] = ";".join(r.get("missing_optional_files", []))
            row["missing_analysis_files"] = ";".join(r.get("missing_analysis_files", []))
            row["warning_tags"] = ";".join(r.get("warning_tags", []))
            row["blockers_detected"] = ";".join(r.get("blockers_detected", []))
            w.writerow(row)

    print("=== WARNINGS_NEED_POLICY analysis ===")
    print(f"total: {summary['total_need_policy']}")
    for g in SUBGROUPS:
        print(f"  {g:<42} {counts.get(g, 0)}")
    print()
    print(f"migratable next pilot (after policy): {summary['migratable_next_pilot']}")
    print(f"remain manual:                        {summary['remain_manual']}")
    print(f"recheck as blocked:                   {summary['recheck_blocked']}")
    print(f"legacy_kb_preserve (King&Sons etc.):  {summary['legacy_kb_preserve_count']}")
    for k in summary["king_sons_legacy_preserve"]:
        print(f"    KB-preserve: {k['discipline']}/{k['document_code']} "
              f"03_findings={k['has_03_findings']} kb_linked={k['kb_linked']}")
    print()
    print(f"-> {sys_dir / 'need_policy_analysis_report.json'}")
    print(f"-> {sys_dir / 'need_policy_analysis_report.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
