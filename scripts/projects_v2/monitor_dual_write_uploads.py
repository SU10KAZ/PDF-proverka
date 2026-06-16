#!/usr/bin/env python3
"""
monitor_dual_write_uploads.py — мониторинг controlled reopen инженерских загрузок
под `dual_write_shadow` (Step 10/10).

READ-ONLY относительно `projects/`. В `projects_v2/_system/` пишет только
runtime-отчёт. Гоняет три проверки (validate / parity / drift), читает их
артефакты + `dual_write_shadow_errors.jsonl` + счётчики, выводит сводку и код
выхода (0 = всё PASS, 1 = есть проблема → остановить новые загрузки).

Назначение: после каждой партии инженерских загрузок запускать ОДНОЙ командой
вместо трёх, получать единый вердикт PASS/FAIL и (опц.) обновлять отчёт
`projects_v2/_system/engineer_uploads_monitoring_report.{json,md}`.

Использование (в проде data разнесена с кодом — пути передаём явно):

    python scripts/projects_v2/monitor_dual_write_uploads.py \
        --v2-root /home/coder/projects/PDF-proverka/projects_v2 \
        --legacy-root /home/coder/projects/PDF-proverka/projects \
        --write-report

Флаги: --no-drift (пропустить медленный drift), --write-report (обновить отчёт).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent


def _run(cmd: list[str], timeout: int = 600) -> tuple[int, str]:
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, (p.stdout or "") + (p.stderr or "")
    except Exception as exc:  # noqa: BLE001
        return 1, f"{type(exc).__name__}: {exc}"


def _read_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _count_docs(v2_root: Path) -> int:
    objs = v2_root / "objects"
    return sum(1 for _ in objs.rglob("document.json")) if objs.is_dir() else 0


def _map_count(v2_root: Path) -> int:
    m = _read_json(v2_root / "_system" / "old_to_new_map.json") or {}
    return len(m.get("migrations", []))


def _shadow_errors(v2_root: Path) -> int:
    f = v2_root / "_system" / "dual_write_shadow_errors.jsonl"
    if not f.is_file():
        return 0
    try:
        return sum(1 for ln in f.read_text(encoding="utf-8").splitlines() if ln.strip())
    except Exception:
        return -1  # unreadable → treat as problem


def main() -> int:
    ap = argparse.ArgumentParser(description="dual_write_shadow uploads monitor")
    ap.add_argument("--v2-root", required=True)
    ap.add_argument("--legacy-root", required=True)
    ap.add_argument("--no-drift", action="store_true")
    ap.add_argument("--write-report", action="store_true")
    ap.add_argument("--stable-seconds", default="120")
    args = ap.parse_args()

    v2 = Path(args.v2_root).resolve()
    legacy = Path(args.legacy_root).resolve()
    py = sys.executable

    result: dict = {"v2_root": str(v2), "legacy_root": str(legacy), "checks": {}}

    # 1. validate
    rc, out = _run([py, str(_SCRIPTS / "validate_migration.py"), "--v2-root", str(v2)])
    validate_pass = ("[PASS]" in out) and rc == 0
    result["checks"]["validate"] = {"pass": validate_pass,
                                    "summary": next((l for l in out.splitlines()
                                                     if "[PASS]" in l or "[FAIL]" in l), "")}

    # 2. parity (читаем артефакт)
    _run([py, str(_SCRIPTS / "check_ui_contract_parity.py"),
          "--legacy-root", str(legacy), "--v2-root", str(v2), "--all"])
    par = _read_json(v2 / "_system" / "full_corpus_parity_report.json") or {}
    parity_ok = bool(par.get("contract_ok"))
    result["checks"]["parity"] = {
        "contract_ok": parity_ok,
        "mismatch": len(par.get("hard_mismatch_documents", []) or []),
        "missing_real": len(par.get("missing_in_legacy_real", []) or []),
        "findings_loss": bool(par.get("any_findings_loss")),
        "version_loss": bool(par.get("any_version_loss")),
        "documents_checked": par.get("documents_checked"),
    }

    # 3. drift (опц.)
    drift_ok = True
    if not args.no_drift:
        _run([py, str(_SCRIPTS / "scan_migrated_drift.py"), "--v2-root", str(v2),
              "--stable-seconds", str(args.stable_seconds)])
        dr = _read_json(v2 / "_system" / "migrated_drift_scan_report.json") or {}
        summ = dr.get("summary", {}) if isinstance(dr.get("summary"), dict) else dr
        docs = dr.get("documents", []) or []
        unstable = sum(1 for d in docs if not d.get("stable", True))
        # drift OK = нет нестабильных и нет потери (известный .pdf-naming артефакт = stable, ок)
        drift_ok = (unstable == 0)
        result["checks"]["drift"] = {
            "drift_documents": len(docs), "unstable": unstable,
            "items": [{"document_code": d.get("document_code"),
                       "types": d.get("drift_types"), "stable": d.get("stable")}
                      for d in docs],
            "ok": drift_ok,
        }
    else:
        result["checks"]["drift"] = {"skipped": True}

    # 4. shadow errors + counts
    se = _shadow_errors(v2)
    result["shadow_write_errors"] = se
    result["projects_v2_documents"] = _count_docs(v2)
    result["old_to_new_map_migrations"] = _map_count(v2)

    # overall verdict
    overall = (validate_pass and parity_ok
               and result["checks"]["parity"]["mismatch"] == 0
               and not result["checks"]["parity"]["findings_loss"]
               and not result["checks"]["parity"]["version_loss"]
               and se == 0 and drift_ok)
    result["overall"] = "PASS" if overall else "FAIL"
    result["recommendation"] = ("продолжать инженерские загрузки под мониторингом"
                                if overall else
                                "ОСТАНОВИТЬ новые загрузки, разобрать причину (см. checks)")

    # report
    if args.write_report:
        rep = v2 / "_system" / "engineer_uploads_monitoring_report.json"
        rep.parent.mkdir(parents=True, exist_ok=True)
        rep.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        result["report_path"] = str(rep)

    print(json.dumps(result, ensure_ascii=False, indent=2))
    print(f"\nOVERALL: {result['overall']} — {result['recommendation']}")
    return 0 if overall else 1


if __name__ == "__main__":
    raise SystemExit(main())
