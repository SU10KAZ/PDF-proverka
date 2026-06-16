#!/usr/bin/env python3
"""
check_ui_contract_parity.py — READ-ONLY проверка соответствия UI/API-контракта
между legacy `projects/` и тем, что сможет отдать `projects_v2` (через adapter).

Это НЕ cutover и НЕ переключение backend. Скрипт по выборке документов разных
типов сравнивает поля, важные для UI/API, и классифицирует каждое поле:

  MATCH | EXPECTED_DIFFERENCE | MISMATCH | MISSING_IN_V2 | MISSING_IN_LEGACY

v2-сторона читается ТОЛЬКО через ProjectsV2Adapter; legacy читается напрямую
(read-only «сверка»). Ничего не пишется, кроме runtime-отчёта в
`projects_v2/_system/`.

Отчёты:
  projects_v2/_system/ui_contract_parity_report.json
  projects_v2/_system/ui_contract_parity_report.md
  projects_v2/_system/ui_contract_parity_report.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Optional

_SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_SCRIPT_DIR))            # v2lib, check_backend_parity
sys.path.insert(0, str(_SCRIPT_DIR.parents[1]))  # repo root (backend import)
import v2lib  # noqa: E402
import check_backend_parity as BP  # noqa: E402  (reuse legacy helpers)
from backend.app.services.storage.projects_v2_adapter import (  # noqa: E402
    ProjectsV2Adapter, _FINDINGS_PRIORITY,
)

MATCH = "MATCH"
EXPECTED = "EXPECTED_DIFFERENCE"
MISMATCH = "MISMATCH"
MISSING_V2 = "MISSING_IN_V2"
MISSING_LEGACY = "MISSING_IN_LEGACY"
STATUS_ORDER = [MATCH, EXPECTED, MISSING_LEGACY, MISSING_V2, MISMATCH]  # worst-last

_LEGACY_PRESERVE_STATUSES = {"legacy_partial", "source_only"}


def _read_json(p: Path) -> Optional[dict]:
    try:
        return json.loads(Path(p).read_text(encoding="utf-8"))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# field comparison
# ---------------------------------------------------------------------------


def cmp_field(name, legacy, v2, *, expected=False, soft=False, na=False) -> dict:
    if na:
        status = MATCH  # поле неприменимо к этому типу документа
    elif legacy == v2:
        status = MATCH
    elif expected:
        status = EXPECTED
    elif v2 is None and legacy is not None:
        status = MISSING_V2
    elif legacy is None and v2 is not None:
        status = MISSING_LEGACY
    else:
        status = MISMATCH
    return {"field": name, "legacy": legacy, "v2": v2, "status": status, "soft": soft}


def _severity_in_dir(d: Optional[Path]) -> dict:
    """Severity-счётчики из лучшего findings-файла каталога (тот же приоритет)."""
    if not d or not Path(d).is_dir():
        return {}
    for n in _FINDINGS_PRIORITY:
        p = Path(d) / n
        if p.is_file():
            data = _read_json(p) or {}
            items = data if isinstance(data, list) else data.get("findings", data.get("items", []))
            out: dict = {}
            for f in items or []:
                if isinstance(f, dict):
                    sev = str(f.get("severity") or f.get("category") or "unknown")
                    out[sev] = out.get(sev, 0) + 1
            return out
    return {}


def _pipeline_stage_count(p: Optional[Path]) -> Optional[int]:
    if not p:
        return None
    data = _read_json(p) or {}
    stages = data.get("stages")
    if isinstance(stages, dict):
        return len(stages)
    if isinstance(stages, list):
        return len(stages)
    return 0


def _legacy_has(d: Optional[Path], name: str) -> bool:
    return bool(d) and (Path(d) / name).is_file()


def compare_document(adapter: ProjectsV2Adapter, doc: dict, migrations: list,
                     decisions: list, projects_root: Path) -> dict:
    doc_dir = Path(doc["doc_dir"])
    snap = adapter.document_snapshot(doc["object_folder"], doc["discipline"],
                                     doc["document_code"])
    dtype = BP.doc_type(snap, adapter)
    dj = adapter.read_document_json(doc_dir) or {}
    is_kingsons = snap.get("migration_kind") == "legacy_findings_preserve"

    recs = BP.map_records_for(migrations, snap["object_id"], snap["document_code"])
    cur = snap["current_version"]
    cur_meta = adapter.version_metadata(doc_dir, cur)
    cur_rec = recs.get(cur)

    # ---- object / discipline / code (из legacy_project_path) ----
    lp = dj.get("legacy_project_path") or (cur_rec or {}).get("legacy_folder_path")
    legacy_object = legacy_discipline = None
    if lp:
        try:
            rel = Path(lp).resolve().relative_to(projects_root.resolve())
            legacy_object = rel.parts[0] if len(rel.parts) > 0 else None
            legacy_discipline = rel.parts[1] if len(rel.parts) > 1 else None
        except Exception:
            pass
    v2_obj = next((o for o in adapter.list_objects()
                   if o["folder_name"] == snap["object_folder"]), {})
    v2_object_name = v2_obj.get("display_name")

    # ---- versions ----
    v2_vcount = snap["version_count"]
    legacy_vcount = BP.legacy_actual_version_count(dj)
    v2_cur_no = cur_meta.get("version_no")
    legacy_cur_no = _legacy_current_version_no(dj)

    # ---- analysis (current version) ----
    v2_latest = adapter.latest_dir(doc_dir, cur)
    legacy_out = None
    if cur_rec and cur_rec.get("legacy_folder_path"):
        legacy_out = BP.legacy_output_with_findings(Path(cur_rec["legacy_folder_path"]))

    v2_status = cur_meta.get("analysis_status")
    legacy_status = _derive_status(legacy_out)
    status_expected = v2_status in _LEGACY_PRESERVE_STATUSES  # v2-специфичные статусы

    crit = ("01_text_analysis.json", "02_blocks_analysis.json", "03_findings.json")
    v2_has = {n: (v2_latest / n).is_file() for n in crit}
    legacy_has = {n: _legacy_has(legacy_out, n) for n in crit}

    v2_fc = BP.findings_count_in_dir(v2_latest)
    legacy_fc = BP.findings_count_in_dir(legacy_out)
    v2_sev = _severity_in_dir(v2_latest)
    legacy_sev = _severity_in_dir(legacy_out)

    v2_plog = adapter.pipeline_log_path(doc_dir, cur)
    legacy_plog = _legacy_pipeline_log(legacy_out)

    # ---- flags / KB ----
    v2_preserve_flag = bool(is_kingsons) or None  # v2-only концепт
    v2_source_only = True if v2_status == "source_only" else None

    fields = [
        cmp_field("object_display_name", legacy_object, v2_object_name),
        cmp_field("discipline", legacy_discipline, snap["discipline"]),
        cmp_field("document_code", v2lib.document_code_for(Path(lp)) if lp else None,
                  snap["document_code"]),
        cmp_field("current_version_no", legacy_cur_no, v2_cur_no, expected=is_kingsons),
        cmp_field("version_count", legacy_vcount, v2_vcount, expected=is_kingsons),
        cmp_field("analysis_status", legacy_status, v2_status, expected=status_expected),
        cmp_field("has_01_text_analysis", legacy_has[crit[0]], v2_has[crit[0]]),
        cmp_field("has_02_blocks_analysis", legacy_has[crit[1]], v2_has[crit[1]]),
        cmp_field("has_blocks_analysis", legacy_has[crit[1]], v2_has[crit[1]]),
        cmp_field("has_03_findings", legacy_has[crit[2]], v2_has[crit[2]]),
        cmp_field("findings_count", legacy_fc, v2_fc),
        cmp_field("findings_by_severity", legacy_sev, v2_sev, soft=True),
        cmp_field("pipeline_log_present", legacy_plog is not None, v2_plog is not None),
        cmp_field("pipeline_log_stage_count", _pipeline_stage_count(legacy_plog),
                  _pipeline_stage_count(v2_plog), soft=True),
        cmp_field("v2_legacy_preserve_flag", None, v2_preserve_flag,
                  expected=is_kingsons),
        cmp_field("source_only_flag", None, v2_source_only,
                  expected=(v2_status == "source_only")),
    ]
    # KB-link только для King&Sons legacy preserve
    if is_kingsons:
        legacy_kb = sum(1 for e in decisions
                        if str(e.get("source_project") or "") == snap["document_code"])
        v2_kb = 0
        kb_link = doc_dir / "versions" / cur / "04_review" / "kb_decisions_link.json"
        if kb_link.is_file():
            v2_kb = (_read_json(kb_link) or {}).get("entry_count", 0)
        fields.append(cmp_field("kb_link_entry_count", legacy_kb, v2_kb))

    # rollup (hard fields only)
    hard = [f for f in fields if not f["soft"]]
    soft = [f for f in fields if f["soft"]]
    doc_status = MATCH
    for f in hard:
        if STATUS_ORDER.index(f["status"]) > STATUS_ORDER.index(doc_status):
            doc_status = f["status"]

    findings_field = next(f for f in fields if f["field"] == "findings_count")
    vcount_field = next(f for f in fields if f["field"] == "version_count")
    return {
        "document_code": snap["document_code"],
        "object_folder": snap["object_folder"],
        "discipline": snap["discipline"],
        "type": dtype,
        "is_kingsons_preserve": is_kingsons,
        "doc_status": doc_status,
        "fields": fields,
        "soft_mismatches": [f["field"] for f in soft if f["status"] == MISMATCH],
        "findings_loss": findings_field["status"] == MISMATCH and (v2_fc < legacy_fc),
        "findings_legacy": legacy_fc, "findings_v2": v2_fc,
        "version_loss": (vcount_field["status"] == MISMATCH),
        "version_legacy": legacy_vcount, "version_v2": v2_vcount,
    }


def _legacy_current_version_no(dj: dict) -> Optional[int]:
    lp = dj.get("legacy_project_path")
    if not lp:
        return None
    p = Path(lp)
    if p.name.endswith("(main)"):
        vg = _read_json(p / "version_group.json") or {}
        latest = str(vg.get("latest_version_id") or "").strip()
        import re
        m = re.match(r"v(\d+)$", latest)
        if m:
            return int(m.group(1))
        return len(vg.get("versions", []) or []) or 1
    return 1


def _derive_status(legacy_out: Optional[Path]) -> Optional[str]:
    """Legacy-эквивалент analysis_status из наличия 01/02/03 в _output."""
    if legacy_out is None:
        return "none"
    crit = ("01_text_analysis.json", "02_blocks_analysis.json", "03_findings.json")
    n = sum(1 for c in crit if (Path(legacy_out) / c).is_file())
    if n == 3:
        return "complete"
    if n > 0:
        return "partial"
    return "none"


def _legacy_pipeline_log(legacy_out: Optional[Path]) -> Optional[Path]:
    if not legacy_out:
        return None
    p = Path(legacy_out) / "pipeline_log.json"
    return p if p.is_file() else None


# ---------------------------------------------------------------------------
# orchestration
# ---------------------------------------------------------------------------


def run_contract_parity(adapter: ProjectsV2Adapter, *, per_type: int = 3,
                        explicit_codes: Optional[list[str]] = None,
                        projects_root: Optional[Path] = None) -> dict:
    migrations = (_read_json(adapter.v2_root / "_system" / "old_to_new_map.json")
                  or {}).get("migrations", [])
    kb_file = adapter.v2_root.parent / "knowledge_base" / "decisions_log.json"
    decisions = (_read_json(kb_file) or {}).get("entries", []) if kb_file.exists() else []
    projects_root = Path(projects_root) if projects_root else v2lib.legacy_projects_root()

    if explicit_codes:
        docs = [d for c in explicit_codes for d in [adapter.find_document(c)] if d]
    else:
        docs = BP.select_documents(adapter, per_type)

    results = [compare_document(adapter, d, migrations, decisions, projects_root)
               for d in docs]

    # field-level breakdown
    field_counts = {s: 0 for s in STATUS_ORDER}
    for r in results:
        for f in r["fields"]:
            if not f["soft"]:
                field_counts[f["status"]] = field_counts.get(f["status"], 0) + 1
    # doc-level breakdown
    doc_counts = {s: 0 for s in STATUS_ORDER}
    for r in results:
        doc_counts[r["doc_status"]] = doc_counts.get(r["doc_status"], 0) + 1
    by_type: dict[str, int] = {}
    for r in results:
        by_type[r["type"]] = by_type.get(r["type"], 0) + 1

    findings_losses = [r["document_code"] for r in results if r["findings_loss"]]
    version_losses = [r["document_code"] for r in results if r["version_loss"]]
    hard_mismatch_docs = [r["document_code"] for r in results if r["doc_status"] == MISMATCH]

    return {
        "schema_version": 1,
        "generated_at": v2lib.utc_now_iso(),
        "storage_backend_default": "legacy",
        "documents_checked": len(results),
        "by_type": by_type,
        "doc_status_counts": doc_counts,
        "field_status_counts": field_counts,
        "findings_losses": findings_losses,
        "version_losses": version_losses,
        "any_findings_loss": bool(findings_losses),
        "any_version_loss": bool(version_losses),
        "contract_ok": not hard_mismatch_docs,
        "hard_mismatch_documents": hard_mismatch_docs,
        "results": results,
    }


def render_md(rep: dict) -> str:
    out = []
    A = out.append
    A("# UI/API contract parity — legacy projects/ ↔ projects_v2 (read-only)")
    A("")
    A(f"**Сгенерировано:** {rep['generated_at']}  ")
    A(f"**Итог:** {'✅ CONTRACT OK' if rep['contract_ok'] else '❌ MISMATCH'}  ")
    A(f"**Storage backend (default):** `{rep['storage_backend_default']}` (НЕ cutover, backend не переключён)")
    A("")
    A(f"- Документов: **{rep['documents_checked']}** (по типам: {rep['by_type']})")
    A(f"- Документы по статусу: {rep['doc_status_counts']}")
    A(f"- Поля по статусу: {rep['field_status_counts']}")
    A(f"- Потери findings: {'❌ ' + str(rep['findings_losses']) if rep['any_findings_loss'] else '✅ нет'}")
    A(f"- Потери versions: {'❌ ' + str(rep['version_losses']) if rep['any_version_loss'] else '✅ нет'}")
    A("")
    A("| Документ | тип | статус | findings L/v2 | versions L/v2 |")
    A("|---|---|---|---|---|")
    for r in rep["results"]:
        A(f"| {r['document_code']} | {r['type']} | {r['doc_status']} | "
          f"{r['findings_legacy']}/{r['findings_v2']} | {r['version_legacy']}/{r['version_v2']} |")
    A("")
    if not rep["contract_ok"]:
        A("## Hard mismatch documents")
        for r in rep["results"]:
            if r["doc_status"] == MISMATCH:
                bad = [f"{f['field']}(L={f['legacy']},v2={f['v2']})"
                       for f in r["fields"] if f["status"] == MISMATCH and not f["soft"]]
                A(f"- **{r['document_code']}**: {bad}")
        A("")
    A("## Expected differences (разрешены, не блокируют cutover)")
    A("")
    A("- King&Sons legacy preserve: v2 хранит legacy snapshot (version_count/"
      "current_version отличаются; analysis_status=legacy_partial/source_only; "
      "флаги legacy/source_only — v2-only).")
    A("- source_only / проекты без анализа: отсутствие 01/02/03 — норма (с обеих сторон).")
    A("- legacy version container иной формы, но v2 нормализован: при равном "
      "version_count → MATCH; расхождение допускается только для King&Sons.")
    A("")
    A("## Что блокирует будущий cutover")
    A("")
    A("- MISMATCH по `findings_count` (потеря/искажение замечаний);")
    A("- MISMATCH по `version_count` вне King&Sons (потеря версий);")
    A("- MISMATCH по `analysis_status` для обычных документов;")
    A("- MISMATCH по наличию `01/02/03` или `object/discipline/code`;")
    A("- `MISSING_IN_V2` для KB-link у King&Sons (потеря связи с базой знаний).")
    return "\n".join(out)


def write_reports(rep: dict, v2_root: Path) -> tuple[Path, Path, Path]:
    sys_dir = v2_root / "_system"
    sys_dir.mkdir(parents=True, exist_ok=True)
    jp = sys_dir / "ui_contract_parity_report.json"
    mp = sys_dir / "ui_contract_parity_report.md"
    cp = sys_dir / "ui_contract_parity_report.csv"
    jp.write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8")
    mp.write_text(render_md(rep), encoding="utf-8")
    with open(cp, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["document_code", "type", "doc_status", "findings_legacy",
                    "findings_v2", "findings_loss", "version_legacy", "version_v2",
                    "version_loss", "hard_mismatch_fields"])
        for r in rep["results"]:
            bad = ";".join(f["field"] for f in r["fields"]
                           if f["status"] == MISMATCH and not f["soft"])
            w.writerow([r["document_code"], r["type"], r["doc_status"],
                        r["findings_legacy"], r["findings_v2"], r["findings_loss"],
                        r["version_legacy"], r["version_v2"], r["version_loss"], bad])
    return jp, mp, cp


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Read-only UI/API contract parity legacy ↔ projects_v2")
    ap.add_argument("--v2-root", default=None)
    ap.add_argument("--per-type", type=int, default=3)
    ap.add_argument("--documents", default=None, help="явные document_code через запятую")
    args = ap.parse_args(argv)

    v2_root = Path(args.v2_root).resolve() if args.v2_root else v2lib.projects_v2_root()
    adapter = ProjectsV2Adapter(v2_root)
    codes = [c.strip() for c in args.documents.split(",")] if args.documents else None

    rep = run_contract_parity(adapter, per_type=args.per_type, explicit_codes=codes)
    jp, mp, cp = write_reports(rep, v2_root)

    print("=== UI/API contract parity (legacy ↔ projects_v2) ===")
    print(f"checked: {rep['documents_checked']}  by_type: {rep['by_type']}")
    print(f"doc_status: {rep['doc_status_counts']}")
    print(f"field_status: {rep['field_status_counts']}")
    print(f"findings_loss: {rep['any_findings_loss']} {rep['findings_losses']}")
    print(f"version_loss: {rep['any_version_loss']} {rep['version_losses']}")
    print(f"contract_ok: {rep['contract_ok']}")
    for r in rep["results"]:
        print(f"  [{r['doc_status']:<19}] {r['type']:<26} {r['document_code']} "
              f"findings={r['findings_legacy']}/{r['findings_v2']}")
    print(f"-> {jp}\n-> {mp}\n-> {cp}")
    return 0 if rep["contract_ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
