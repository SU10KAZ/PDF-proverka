#!/usr/bin/env python3
"""
check_backend_parity.py — READ-ONLY parity-проверка legacy `projects/` ↔
`projects_v2` через read-only adapter.

Сравнивает по выборке документов разных типов (complete / partial / none /
source_only / legacy_partial / versioned / King&Sons legacy-preserve):

  * document exists (в v2);
  * version count (v2 vs фактические версии legacy-контейнера);
  * current/latest version;
  * analysis_status;
  * наличие 01/02/03 (v2 latest vs legacy _output);
  * количество findings в 03_findings.json (симметрично: один и тот же
    приоритет 03a_norms_verified > 03_findings и в legacy, и в v2);
  * наличие pipeline_log;
  * ОТСУТСТВИЕ потери findings (главный инвариант: v2 >= legacy, в норме ==).

Ничего не пишет в `projects/` и `projects_v2/` (кроме runtime-отчёта в
`projects_v2/_system/`). Legacy читается напрямую (read-only) — это разрешённая
«сверка». v2 читается ТОЛЬКО через adapter (без legacy-fallback).

Отчёт:
  projects_v2/_system/backend_parity_report.json
  projects_v2/_system/backend_parity_report.md
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Optional

_SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_SCRIPT_DIR))           # v2lib
sys.path.insert(0, str(_SCRIPT_DIR.parents[1]))  # repo root (backend import)
import v2lib  # noqa: E402
from backend.app.services.storage.projects_v2_adapter import (  # noqa: E402
    ProjectsV2Adapter, _FINDINGS_PRIORITY,
)

TYPE_ORDER = ["complete", "partial", "none", "source_only", "legacy_partial",
              "versioned", "king_sons_legacy_preserve"]


def _read_json(p: Path) -> Optional[dict]:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def doc_type(doc_snapshot: dict, adapter: ProjectsV2Adapter) -> str:
    if doc_snapshot.get("migration_kind") == "legacy_findings_preserve":
        return "king_sons_legacy_preserve"
    if doc_snapshot.get("version_count", 0) > 1:
        return "versioned"
    # по текущей версии
    cur = doc_snapshot.get("current_version")
    for v in doc_snapshot.get("versions", []):
        if v["version_id"] == cur:
            return v.get("analysis_status") or "none"
    return "unknown"


def findings_count_in_dir(d: Optional[Path]) -> int:
    """Симметричный подсчёт: приоритет 03a_norms_verified > 03_findings > pre_merge."""
    if not d or not Path(d).is_dir():
        return 0
    for name in _FINDINGS_PRIORITY:
        p = Path(d) / name
        if p.is_file():
            data = _read_json(p) or {}
            if isinstance(data, list):
                return len(data)
            return len(data.get("findings", data.get("items", [])) or [])
    return 0


def legacy_output_with_findings(version_legacy_folder: Path) -> Optional[Path]:
    """Находит _output (любой глубины) в legacy-папке версии, где есть findings."""
    folder = Path(version_legacy_folder)
    if not folder.is_dir():
        return None
    outs = [d for d in folder.rglob("_output") if d.is_dir()]
    # сначала тот, где есть приоритетный findings-файл
    for d in sorted(outs):
        if any((d / n).is_file() for n in _FINDINGS_PRIORITY):
            return d
    # иначе любой _output (для 01/02/pipeline_log)
    return sorted(outs)[0] if outs else None


def legacy_actual_version_count(document_json: dict) -> Optional[int]:
    """Фактическое число версий legacy: version_group.json контейнера, иначе 1."""
    lp = document_json.get("legacy_project_path")
    if not lp:
        return None
    p = Path(lp)
    if p.name.endswith("(main)"):
        vg = _read_json(p / "version_group.json")
        if vg and isinstance(vg.get("versions"), list):
            return len(vg["versions"])
    return 1


def map_records_for(migrations: list, object_id: str, document_code: str) -> dict:
    out = {}
    for r in migrations:
        if r.get("object_id") == object_id and r.get("document_code") == document_code:
            out[r.get("version_id")] = r
    return out


def compare_document(adapter: ProjectsV2Adapter, doc: dict, migrations: list) -> dict:
    """Сравнивает один документ v2 ↔ legacy. Возвращает результат с checks[]."""
    snap = adapter.document_snapshot(doc["object_folder"], doc["discipline"],
                                     doc["document_code"])
    dtype = doc_type(snap, adapter)
    dj = adapter.read_document_json(Path(snap["doc_dir"])) or {}
    recs = map_records_for(migrations, snap["object_id"], snap["document_code"])

    checks: list[dict] = []

    def add(name, ok, detail, severity="hard", expected=False):
        checks.append({"check": name, "ok": bool(ok), "detail": detail,
                       "severity": severity, "expected_difference": expected})

    # 1) document exists
    add("document_exists", True, f"v2 doc_dir={Path(snap['doc_dir']).name}")

    # 2) version count (v2 vs legacy actual container)
    legacy_versions = legacy_actual_version_count(dj)
    v2_versions = snap["version_count"]
    if legacy_versions is None:
        add("version_count", True, f"v2={v2_versions} (legacy unknown)", severity="soft")
    elif v2_versions == legacy_versions:
        add("version_count", True, f"v2={v2_versions} == legacy={legacy_versions}")
    else:
        is_lp = snap.get("migration_kind") == "legacy_findings_preserve"
        add("version_count", is_lp,
            f"v2={v2_versions} != legacy={legacy_versions}"
            + (" (legacy snapshot collapse — ожидаемо)" if is_lp else ""),
            expected=is_lp)

    # 3) current version present + analysis_status present
    cur = snap["current_version"]
    cur_v = next((v for v in snap["versions"] if v["version_id"] == cur), None)
    add("current_version", cur_v is not None, f"current={cur}")
    add("analysis_status_present", bool(cur_v and cur_v["analysis_status"]),
        f"status={cur_v['analysis_status'] if cur_v else None}")

    # per-version: 01/02/03 parity + findings parity (no loss) + pipeline_log
    version_results = []
    for v in snap["versions"]:
        vid = v["version_id"]
        rec = recs.get(vid)
        legacy_out = None
        if rec and rec.get("legacy_folder_path"):
            legacy_out = legacy_output_with_findings(Path(rec["legacy_folder_path"]))
        v2_latest = adapter.latest_dir(Path(snap["doc_dir"]), vid)

        # findings (symmetric)
        v2_fc = findings_count_in_dir(v2_latest)
        legacy_fc = findings_count_in_dir(legacy_out)
        # 01/02/03 presence
        def has(name, d):
            return bool(d) and (Path(d) / name).is_file()
        v2_arts = {n: (v2_latest / n).is_file() for n in
                   ("02_text_analysis.json", "01_blocks_analysis.json", "03_findings.json")}
        legacy_arts = {n: has(n, legacy_out) for n in v2_arts}

        version_results.append({
            "version_id": vid, "is_current": v["is_current"],
            "analysis_status": v["analysis_status"],
            "legacy_output": str(legacy_out) if legacy_out else None,
            "v2_findings": v2_fc, "legacy_findings": legacy_fc,
            "findings_no_loss": v2_fc >= legacy_fc,
            "findings_exact": v2_fc == legacy_fc,
            "v2_artifacts": v2_arts, "legacy_artifacts": legacy_arts,
            "artifacts_match": v2_arts == legacy_arts,
            "v2_pipeline_log": v["has_pipeline_log"],
        })

    # aggregate per-version into checks
    no_loss = all(vr["findings_no_loss"] for vr in version_results)
    exact = all(vr["findings_exact"] for vr in version_results)
    add("findings_no_loss", no_loss,
        "; ".join(f"{vr['version_id']}: v2={vr['v2_findings']} legacy={vr['legacy_findings']}"
                  for vr in version_results))
    add("findings_exact_match", exact,
        "все версии совпали" if exact else "есть расхождение количества (см. per-version)",
        severity="soft" if exact else "hard")
    arts_ok = all(vr["artifacts_match"] for vr in version_results)
    add("artifacts_01_02_03_parity", arts_ok,
        "; ".join(f"{vr['version_id']}: {'ok' if vr['artifacts_match'] else 'diff'}"
                  for vr in version_results), severity="soft")
    # pipeline_log: для версий с анализом ожидаем наличие
    plog_versions = [vr for vr in version_results if vr["analysis_status"] in
                     ("complete", "partial", "legacy_partial")]
    plog_ok = all(vr["v2_pipeline_log"] for vr in plog_versions) if plog_versions else True
    add("pipeline_log_present", plog_ok,
        "; ".join(f"{vr['version_id']}: {vr['v2_pipeline_log']}" for vr in plog_versions)
        or "n/a (нет версий с анализом)", severity="soft")

    hard_fail = [c for c in checks if not c["ok"] and c["severity"] == "hard"
                 and not c["expected_difference"]]
    return {
        "document_code": snap["document_code"],
        "object_folder": snap["object_folder"],
        "discipline": snap["discipline"],
        "type": dtype,
        "kind": snap["kind"],
        "current_version": cur,
        "v2_version_count": v2_versions,
        "legacy_version_count": legacy_versions,
        "checks": checks,
        "versions": version_results,
        "ok": not hard_fail,
        "hard_failures": [c["check"] for c in hard_fail],
    }


def select_documents(adapter: ProjectsV2Adapter, per_type: int) -> list[dict]:
    buckets: dict[str, list[dict]] = {t: [] for t in TYPE_ORDER}
    for d in adapter.list_documents():
        snap = adapter.document_snapshot(d["object_folder"], d["discipline"],
                                         d["document_code"])
        t = doc_type(snap, adapter)
        if t in buckets and len(buckets[t]) < per_type:
            buckets[t].append(d)
    selected = []
    for t in TYPE_ORDER:
        selected.extend(buckets[t])
    return selected


def run_parity(adapter: ProjectsV2Adapter, per_type: int = 3,
               explicit_codes: Optional[list[str]] = None) -> dict:
    migrations = (_read_json(adapter.v2_root / "_system" / "old_to_new_map.json")
                  or {}).get("migrations", [])
    if explicit_codes:
        docs = []
        for code in explicit_codes:
            d = adapter.find_document(code)
            if d:
                docs.append(d)
    else:
        docs = select_documents(adapter, per_type)

    results = [compare_document(adapter, d, migrations) for d in docs]
    by_type: dict[str, int] = {}
    for r in results:
        by_type[r["type"]] = by_type.get(r["type"], 0) + 1
    failures = [r for r in results if not r["ok"]]
    total_v2_findings = sum(sum(v["v2_findings"] for v in r["versions"]) for r in results)
    total_legacy_findings = sum(sum(v["legacy_findings"] for v in r["versions"]) for r in results)
    return {
        "schema_version": 1,
        "generated_at": v2lib.utc_now_iso(),
        "storage_backend_default": "legacy",
        "documents_checked": len(results),
        "by_type": by_type,
        "passed": len(results) - len(failures),
        "failed": len(failures),
        "parity_ok": not failures,
        "total_v2_findings": total_v2_findings,
        "total_legacy_findings": total_legacy_findings,
        "findings_no_loss_overall": total_v2_findings >= total_legacy_findings,
        "results": results,
    }


def render_md(rep: dict) -> str:
    L = []
    A = L.append
    A("# Backend parity report — legacy projects/ ↔ projects_v2 (read-only adapter)")
    A("")
    A(f"**Сгенерировано:** {rep['generated_at']}  ")
    A(f"**Итог:** {'✅ PARITY OK' if rep['parity_ok'] else '❌ MISMATCH'}  ")
    A(f"**Storage backend (default):** `{rep['storage_backend_default']}` (production не изменён)")
    A("")
    A(f"- Документов проверено: **{rep['documents_checked']}** (по типам: {rep['by_type']})")
    A(f"- Прошло: **{rep['passed']}** · Не прошло: **{rep['failed']}**")
    A(f"- Findings: v2={rep['total_v2_findings']} / legacy={rep['total_legacy_findings']} "
      f"→ потери нет: {'✅' if rep['findings_no_loss_overall'] else '❌'}")
    A("")
    A("| Документ | тип | v2/legacy версий | current | findings v2/legacy | артефакты | ok |")
    A("|---|---|---|---|---|---|---|")
    for r in rep["results"]:
        fc = "; ".join(f"{v['version_id']}:{v['v2_findings']}/{v['legacy_findings']}"
                       for v in r["versions"])
        arts = "ok" if all(v["artifacts_match"] for v in r["versions"]) else "diff"
        A(f"| {r['document_code']} | {r['type']} | {r['v2_version_count']}/"
          f"{r['legacy_version_count']} | {r['current_version']} | {fc} | {arts} | "
          f"{'✅' if r['ok'] else '❌ '+','.join(r['hard_failures'])} |")
    A("")
    if rep["failed"]:
        A("## Hard failures")
        for r in rep["results"]:
            if not r["ok"]:
                A(f"- **{r['document_code']}**: {r['hard_failures']}")
        A("")
    A("> Расхождение числа версий у King&Sons legacy-preserve (v2=1 vs legacy-контейнер)"
      " помечено `expected_difference` и НЕ считается ошибкой (осознанный snapshot).")
    return "\n".join(L)


def write_reports(rep: dict, v2_root: Path) -> tuple[Path, Path]:
    sys_dir = v2_root / "_system"
    sys_dir.mkdir(parents=True, exist_ok=True)
    jp = sys_dir / "backend_parity_report.json"
    mp = sys_dir / "backend_parity_report.md"
    jp.write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8")
    mp.write_text(render_md(rep), encoding="utf-8")
    # CSV-сводка по документам
    cp = sys_dir / "backend_parity_report.csv"
    with open(cp, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["document_code", "type", "v2_versions", "legacy_versions",
                    "current", "ok", "hard_failures"])
        for r in rep["results"]:
            w.writerow([r["document_code"], r["type"], r["v2_version_count"],
                        r["legacy_version_count"], r["current_version"], r["ok"],
                        ";".join(r["hard_failures"])])
    return jp, mp


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Read-only parity check legacy ↔ projects_v2")
    ap.add_argument("--v2-root", default=None)
    ap.add_argument("--per-type", type=int, default=3, help="документов на тип (default 3)")
    ap.add_argument("--documents", default=None,
                    help="явные document_code через запятую (override авто-выборки)")
    args = ap.parse_args(argv)

    v2_root = Path(args.v2_root).resolve() if args.v2_root else v2lib.projects_v2_root()
    adapter = ProjectsV2Adapter(v2_root)
    codes = [c.strip() for c in args.documents.split(",")] if args.documents else None

    rep = run_parity(adapter, per_type=args.per_type, explicit_codes=codes)
    jp, mp = write_reports(rep, v2_root)

    print("=== backend parity (legacy ↔ projects_v2) ===")
    print(f"checked: {rep['documents_checked']}  by_type: {rep['by_type']}")
    print(f"passed: {rep['passed']}  failed: {rep['failed']}  parity_ok: {rep['parity_ok']}")
    print(f"findings v2/legacy: {rep['total_v2_findings']}/{rep['total_legacy_findings']} "
          f"(no loss: {rep['findings_no_loss_overall']})")
    for r in rep["results"]:
        mark = "OK " if r["ok"] else "FAIL"
        print(f"  [{mark}] {r['type']:<26} {r['document_code']}  "
              f"v2/legacy versions={r['v2_version_count']}/{r['legacy_version_count']}")
    print(f"-> {jp}")
    print(f"-> {mp}")
    return 0 if rep["parity_ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
