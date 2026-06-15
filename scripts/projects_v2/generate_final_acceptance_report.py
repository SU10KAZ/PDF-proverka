#!/usr/bin/env python3
"""
generate_final_acceptance_report.py — финальная приёмка миграции projects_v2.

READ-ONLY. Собирает фактическое состояние projects_v2 (с диска + из готовых
report-JSON: readiness / warning-policy / drift), при желании прогоняет
validate_migration и формирует:

  projects_v2/_system/final_migration_acceptance_report.json
  projects_v2/_system/final_migration_acceptance_report.md

Ничего не мигрирует, legacy `projects/` и `comparison/` не трогает, в git не
пишет (отчёты — runtime-данные в projects_v2/_system).
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v2lib  # noqa: E402

# 4 King&Sons legacy-preserve документа (object 213)
KING_SONS_OBJECT_FOLDER = "213_Mosfilmovskaya_31A_KingSons"
KING_SONS_TARGETS = [
    ("EOM", "133_23-ГК-ЭМ2", "legacy_partial", True),
    ("SS", "133_23-ГК-АК", "legacy_partial", True),
    ("EOM", "Фасадное освещение", "source_only", False),
    ("ITP", "133_23-ГК-ИТП.ТМ", "source_only", False),
]


def _read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def run_validate(v2_root: Path) -> dict:
    """Прогоняет validate_migration как subprocess, парсит результат."""
    script = Path(__file__).resolve().parent / "validate_migration.py"
    try:
        proc = subprocess.run(
            [sys.executable, str(script), "--v2-root", str(v2_root)],
            capture_output=True, text=True, timeout=600)
    except Exception as e:  # pragma: no cover
        return {"ran": False, "error": str(e), "result": "UNKNOWN"}
    out = proc.stdout + proc.stderr
    m_pass = re.search(r"\[PASS\] all checks passed \((\d+) ok\)", out)
    m_fail = re.search(r"\[FAIL\] (\d+) error", out)
    if m_pass:
        return {"ran": True, "result": "PASS", "ok": int(m_pass.group(1)),
                "errors": 0, "exit": proc.returncode}
    return {"ran": True, "result": "FAIL",
            "errors": int(m_fail.group(1)) if m_fail else None,
            "exit": proc.returncode, "tail": out.strip().splitlines()[-5:]}


def gather(v2_root: Path, projects_root: Path, *, do_validate: bool = True) -> dict:
    obj_root = v2_root / "objects"
    sys_dir = v2_root / "_system"

    # ---- объекты ----
    object_dirs = sorted([d for d in obj_root.iterdir() if d.is_dir()]) if obj_root.is_dir() else []
    obj_star = [d.name for d in object_dirs if d.name.startswith("obj_")]
    readable_objects = [d.name for d in object_dirs if not d.name.startswith("obj_")]

    # ---- документы / версии / статусы (с диска) ----
    doc_jsons = sorted(obj_root.rglob("document.json")) if obj_root.is_dir() else []
    kind_counter: Counter = Counter()
    migkind_counter: Counter = Counter()
    status_counter: Counter = Counter()
    versions_total = 0
    multi_version_docs = 0
    legacy_preserve_docs = []
    source_only_docs = []
    for dj in doc_jsons:
        d = _read_json(dj)
        kind_counter[d.get("kind", "?")] += 1
        mk = d.get("migration_kind") or "(normal)"
        migkind_counter[mk] += 1
        vers = d.get("versions", [])
        if len(vers) > 1:
            multi_version_docs += 1
        doc_dir = dj.parent
        doc_label = f"{d.get('discipline')}/{d.get('document_code')}"
        if d.get("migration_kind") == "legacy_findings_preserve":
            legacy_preserve_docs.append(doc_label)
        for v in vers:
            versions_total += 1
            vd = _read_json(doc_dir / "versions" / v["version_id"] / "version.json")
            st = vd.get("analysis_status")
            if st is None:
                status_counter["(no_status_field_legacy_schema)"] += 1
            else:
                status_counter[st] += 1
                if st == "source_only":
                    source_only_docs.append(doc_label)

    # ---- old_to_new_map ----
    map_obj = _read_json(sys_dir / "old_to_new_map.json")
    migs = map_obj.get("migrations", [])
    unique_docs = {(m.get("object_id"), m.get("document_code")) for m in migs}

    # ---- готовые report-JSON ----
    readiness = _read_json(sys_dir / "migration_readiness_report.json").get("summary", {})
    warnpol = _read_json(sys_dir / "migration_warning_policy_report.json").get("summary", {})
    drift = _read_json(sys_dir / "migrated_drift_scan_report.json").get("summary", {})
    group_counts = readiness.get("group_counts", {})

    # ---- validate ----
    validate = run_validate(v2_root) if do_validate else {"ran": False, "result": "SKIPPED"}

    # ---- King&Sons preserve checks ----
    ks_root = obj_root / KING_SONS_OBJECT_FOLDER / "disciplines"
    ks_checks = []
    for disc, code, want_status, want_findings in KING_SONS_TARGETS:
        vroot = ks_root / disc / "documents" / code / "versions" / "v001"
        vd = _read_json(vroot / "version.json")
        findings = vroot / "03_analysis" / "latest" / "03_findings.json"
        kb_link = vroot / "04_review" / "kb_decisions_link.json"
        bundle = vroot / "01_input" / "legacy_bundle"
        legout = vroot / "99_service" / "legacy_output"
        kb_data = _read_json(kb_link) if kb_link.exists() else {}
        ok = (vroot.exists()
              and vd.get("analysis_status") == want_status
              and findings.exists() == want_findings)
        ks_checks.append({
            "document": f"{disc}/{code}",
            "migrated": vroot.exists(),
            "analysis_status": vd.get("analysis_status"),
            "expected_status": want_status,
            "findings_present": findings.exists(),
            "expected_findings": want_findings,
            "kb_linked": kb_link.exists(),
            "kb_entries": kb_data.get("entry_count", 0),
            "legacy_bundle_files": sum(1 for p in bundle.rglob("*") if p.is_file()) if bundle.exists() else 0,
            "legacy_output_files": sum(1 for p in legout.rglob("*") if p.is_file()) if legout.exists() else 0,
            "preserve_reason": vd.get("preserve_reason"),
            "ok": ok,
        })
    # АК спец-условие: findings + KB
    ak = next((c for c in ks_checks if c["document"] == "SS/133_23-ГК-АК"), None)
    if ak:
        ak["ok"] = ak["ok"] and ak["kb_linked"] and ak["kb_entries"] >= 4

    # ---- остаточные риски ----
    no_status = status_counter.get("(no_status_field_legacy_schema)", 0)
    risks = []
    if no_status:
        risks.append(
            f"{no_status} version.json созданы ранней версией кода и не содержат "
            f"поля analysis_status/missing_analysis_files (старая схема). validate и "
            f"readiness от этого поля НЕ зависят (проверяют артефакты напрямую); "
            f"потери данных нет. Можно добить metadata-проходом при необходимости.")
    risks.append(
        "backend/UI/deploy продолжают работать со старой projects/ — projects_v2 "
        "пока НЕ подключена к backend (storage adapter — будущий этап).")
    risks.append(
        "Source-only документы King&Sons (Фасадное освещение, ИТП.ТМ) не имеют "
        "анализа и имели пустой legacy _output → legacy_output не создавался "
        "(нечего сохранять; не потеря данных).")
    def _count(x):
        return len(x) if isinstance(x, (list, tuple)) else x
    rd_top = {
        "pdf_named_version_folders": _count(readiness.get("pdf_named_version_folders")),
        "no_analysis": _count(readiness.get("no_analysis")),
        "multiple_pdf_md_json": _count(readiness.get("multiple_pdf_md_json")),
        "incomplete_input_quad": _count(readiness.get("incomplete_input_quad")),
    }
    risks.append(
        "readiness top-warnings описывают форму legacy-источников уже мигрированных "
        f"документов (информативно, не блокеры): {rd_top}.")

    total_documents = len(doc_jsons)
    acceptance_ok = (
        validate.get("result") == "PASS"
        and drift.get("drift_documents") == 0
        and drift.get("unstable") == 0
        and group_counts.get("MANUAL_REVIEW_REQUIRED") == 0
        and group_counts.get("CAN_MIGRATE_WITH_WARNINGS") == 0
        and not obj_star
        and total_documents == 184
        and len(unique_docs) == 184
        and all(c["ok"] for c in ks_checks)
    )

    return {
        "schema_version": 1,
        "generated_at": v2lib.utc_now_iso(),
        "acceptance_ok": acceptance_ok,
        "totals": {
            "total_legacy_projects_before": warnpol.get("total_legacy_projects") or readiness.get("total_projects"),
            "documents_in_v2": total_documents,
            "migrated_documents": len(unique_docs),
            "version_level_migration_records": len(migs),
            "versions_total": versions_total,
            "plain_documents": kind_counter.get("plain", 0),
            "container_documents": kind_counter.get("container", 0),
            "multi_version_documents": multi_version_docs,
            "legacy_findings_preserve_documents": migkind_counter.get("legacy_findings_preserve", 0),
            "source_only_versions": status_counter.get("source_only", 0),
        },
        "analysis_status_distribution": dict(status_counter),
        "objects": {
            "count": len(object_dirs),
            "readable_folders": readable_objects,
            "obj_star_folders": obj_star,
        },
        "kind_distribution": dict(kind_counter),
        "migration_kind_distribution": dict(migkind_counter),
        "validate": validate,
        "drift": {
            "drift_documents": drift.get("drift_documents"),
            "stable": drift.get("stable"),
            "unstable": drift.get("unstable"),
            "result": "PASS" if (drift.get("drift_documents") == 0 and drift.get("unstable") == 0) else "FAIL",
        },
        "readiness_group_counts": group_counts,
        "warning_policy_counts": warnpol.get("policy_counts", {}),
        "king_sons_legacy_preserve": ks_checks,
        "legacy_preserve_documents": legacy_preserve_docs,
        "remaining_risks": risks,
        "backend_note": ("Backend/UI/deploy продолжают работать со старой projects/. "
                         "projects_v2 — параллельное хранилище, к backend не подключено."),
    }


def render_md(rep: dict) -> str:
    t = rep["totals"]
    v = rep["validate"]
    d = rep["drift"]
    g = rep["readiness_group_counts"]
    lines = []
    A = lines.append
    A("# Финальная приёмка миграции projects_v2")
    A("")
    A(f"**Сгенерировано:** {rep['generated_at']}  ")
    A(f"**Итог приёмки:** {'✅ PASS' if rep['acceptance_ok'] else '❌ FAIL'}")
    A("")
    A("## Сводка")
    A("")
    A("| Метрика | Значение |")
    A("|---|---|")
    A(f"| Всего legacy-проектов (было) | {t['total_legacy_projects_before']} |")
    A(f"| Документов в projects_v2 | {t['documents_in_v2']} |")
    A(f"| Перенесено документов | {t['migrated_documents']} |")
    A(f"| Записей миграции (по версиям) | {t['version_level_migration_records']} |")
    A(f"| Версий всего | {t['versions_total']} |")
    A(f"| Обычных (plain) | {t['plain_documents']} |")
    A(f"| Versioned (контейнеры) | {t['container_documents']} (многоверсионных: {t['multi_version_documents']}) |")
    A(f"| Legacy-findings-preserve | {t['legacy_findings_preserve_documents']} |")
    A(f"| Source-only версий | {t['source_only_versions']} |")
    A("")
    A("## analysis_status (по версиям)")
    A("")
    for k, n in sorted(rep["analysis_status_distribution"].items()):
        A(f"- `{k}`: {n}")
    A("")
    A("## validate / drift / readiness")
    A("")
    A(f"- **validate:** {v.get('result')}" + (f" ({v.get('ok')} ok)" if v.get('ok') is not None else ""))
    A(f"- **drift:** {d.get('result')} (drift_documents={d.get('drift_documents')}, "
      f"stable={d.get('stable')}, unstable={d.get('unstable')})")
    A(f"- **readiness groups:** {g}")
    A(f"- **warning-policy:** {rep['warning_policy_counts']}")
    A("")
    A("## Объекты")
    A("")
    A(f"- obj_* папок (ожидается 0): **{len(rep['objects']['obj_star_folders'])}** {rep['objects']['obj_star_folders']}")
    A(f"- читаемые папки объектов: {rep['objects']['readable_folders']}")
    A("")
    A("## King&Sons legacy preserve")
    A("")
    A("| Документ | status | findings | KB | bundle | legacy_output | ok |")
    A("|---|---|---|---|---|---|---|")
    for c in rep["king_sons_legacy_preserve"]:
        A(f"| {c['document']} | {c['analysis_status']} | "
          f"{'✅' if c['findings_present'] else '—'} | "
          f"{('✅ '+str(c['kb_entries'])) if c['kb_linked'] else '—'} | "
          f"{c['legacy_bundle_files']} | {c['legacy_output_files']} | "
          f"{'✅' if c['ok'] else '❌'} |")
    A("")
    A("## Остаточные риски")
    A("")
    for r in rep["remaining_risks"]:
        A(f"- {r}")
    A("")
    A("## Backend")
    A("")
    A(f"- {rep['backend_note']}")
    A("")
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Финальная приёмка миграции projects_v2 (read-only)")
    ap.add_argument("--v2-root", default=None)
    ap.add_argument("--projects-root", default=None)
    ap.add_argument("--no-validate", action="store_true",
                    help="не прогонять validate_migration (быстрее)")
    args = ap.parse_args(argv)

    v2_root = Path(args.v2_root).resolve() if args.v2_root else v2lib.projects_v2_root()
    projects_root = (Path(args.projects_root).resolve() if args.projects_root
                     else v2lib.legacy_projects_root())

    rep = gather(v2_root, projects_root, do_validate=not args.no_validate)
    sys_dir = v2_root / "_system"
    sys_dir.mkdir(parents=True, exist_ok=True)
    json_path = sys_dir / "final_migration_acceptance_report.json"
    md_path = sys_dir / "final_migration_acceptance_report.md"
    json_path.write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_md(rep), encoding="utf-8")

    t = rep["totals"]
    print("=== final acceptance ===")
    print(f"acceptance_ok: {rep['acceptance_ok']}")
    print(f"documents={t['documents_in_v2']} migrated={t['migrated_documents']} "
          f"records={t['version_level_migration_records']} versions={t['versions_total']}")
    print(f"plain={t['plain_documents']} containers={t['container_documents']} "
          f"legacy_preserve={t['legacy_findings_preserve_documents']} source_only={t['source_only_versions']}")
    print(f"validate={rep['validate'].get('result')} drift={rep['drift'].get('result')} "
          f"obj_star={len(rep['objects']['obj_star_folders'])}")
    print(f"king_sons_ok={all(c['ok'] for c in rep['king_sons_legacy_preserve'])}")
    print(f"-> {json_path}")
    print(f"-> {md_path}")
    return 0 if rep["acceptance_ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
