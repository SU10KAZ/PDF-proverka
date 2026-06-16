#!/usr/bin/env python3
"""
check_cutover_readiness.py — READ-ONLY сводка готовности `projects_v2` к будущему
read-only cutover.

Собирает:
  * validate (запускает validate_migration.py как subprocess) → PASS/FAIL;
  * drift (из последнего runtime-отчёта migrated_drift_scan_report.json);
  * backend parity (из backend_parity_report.json);
  * UI contract parity (из ui_contract_parity_report.json);
  * dual-read sample (live, через DualReadService);
  * рекомендацию: not_ready / ready_for_shadow_prod / ready_for_read_only_canary.

Логика рекомендации единая с backend (`projects_v2_dual_read.cutover_readiness`),
чтобы CLI и shadow endpoint /cutover-readiness совпадали.

Отчёты:
  projects_v2/_system/cutover_readiness_report.json
  projects_v2/_system/cutover_readiness_report.md

READ-ONLY (кроме своего отчёта в `_system`). legacy `projects/` и `comparison/`
не трогает; миграцию/refresh не выполняет.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_SCRIPT_DIR))            # v2lib
sys.path.insert(0, str(_SCRIPT_DIR.parents[1]))  # repo root (backend import)
import v2lib  # noqa: E402
from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter  # noqa: E402
from backend.app.services.storage.projects_v2_dual_read import cutover_readiness  # noqa: E402


def run_validate(v2_root: Path) -> dict:
    script = Path(__file__).resolve().parents[0] / "validate_migration.py"
    try:
        proc = subprocess.run(
            [sys.executable, str(script), "--v2-root", str(v2_root)],
            capture_output=True, text=True, timeout=900)
    except Exception as e:  # pragma: no cover
        return {"status": "UNKNOWN", "error": str(e)}
    out = proc.stdout + proc.stderr
    m = re.search(r"\[PASS\] all checks passed \((\d+) ok\)", out)
    if m:
        return {"status": "PASS", "ok_count": int(m.group(1))}
    mf = re.search(r"\[FAIL\] (\d+) error", out)
    return {"status": "FAIL", "errors": int(mf.group(1)) if mf else None,
            "tail": out.strip().splitlines()[-5:]}


def build(v2_root: Path, *, per_type: int = 3, run_validate_flag: bool = True) -> dict:
    adapter = ProjectsV2Adapter(v2_root)
    validate = run_validate(v2_root) if run_validate_flag else {"status": None}
    readiness = cutover_readiness(adapter, validate_status=validate.get("status"),
                                  per_type=per_type)
    readiness["validate"] = {**readiness.get("validate", {}), **validate}
    readiness["schema_version"] = 1
    readiness["generated_at"] = v2lib.utc_now_iso()
    return readiness


def render_md(rep: dict) -> str:
    out = []
    A = out.append
    A("# Cutover readiness — projects_v2 (read-only)")
    A("")
    A(f"**Сгенерировано:** {rep.get('generated_at')}  ")
    A(f"**Рекомендация:** `{rep['recommendation']}`  ")
    A(f"**Storage backend (default):** `{rep['storage_backend_default']}` (НЕ cutover)")
    A("")
    A("| Проверка | Статус |")
    A("|---|---|")
    A(f"| validate | {rep['validate'].get('status')} |")
    A(f"| drift | docs={rep['drift'].get('drift_documents')} unstable={rep['drift'].get('unstable')} ok={rep['drift'].get('ok')} |")
    A(f"| backend parity | ok={rep['backend_parity'].get('ok')} no_loss={rep['backend_parity'].get('findings_no_loss')} |")
    A(f"| UI contract parity | ok={rep['ui_contract_parity'].get('ok')} checked={rep['ui_contract_parity'].get('documents_checked')} full_corpus={rep['ui_contract_parity'].get('full_corpus')} |")
    A(f"| dual-read sample | ok={rep['dual_read_sample'].get('ok')} checked={rep['dual_read_sample'].get('documents_checked')} counts={rep['dual_read_sample'].get('status_counts')} |")
    A(f"| v2 documents | {rep['v2_documents']} |")
    A(f"| total mismatches | {rep['total_mismatches']} |")
    A("")
    A(f"- Потери findings (dual-read): {rep['dual_read_sample'].get('findings_losses')}")
    A(f"- Потери versions (dual-read): {rep['dual_read_sample'].get('version_losses')}")
    A("")
    A("## Рекомендации (значения)")
    A("")
    A("- `not_ready` — есть hard-проблема (validate FAIL / drift>0 / mismatch / потеря findings|versions) или validate/drift не определены;")
    A("- `ready_for_shadow_prod` — базово зелено (validate PASS, drift 0, парити без mismatch), но contract parity покрыт по ВЫБОРКЕ, не по всему корпусу → можно включить shadow API в prod для наблюдения;")
    A("- `ready_for_read_only_canary` — всё зелено И contract parity покрыт по ВСЕМУ корпусу И нет потерь → можно read-only канарейку.")
    A("")
    A("> Это не cutover. Backend/UI не переключаются; `AUDIT_STORAGE_BACKEND` "
      "остаётся `legacy`. Порядок будущего cutover — см. docs/projects_v2_migration_plan.md.")
    return "\n".join(out)


def write_reports(rep: dict, v2_root: Path) -> tuple[Path, Path]:
    sysd = v2_root / "_system"
    sysd.mkdir(parents=True, exist_ok=True)
    jp = sysd / "cutover_readiness_report.json"
    mp = sysd / "cutover_readiness_report.md"
    jp.write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8")
    mp.write_text(render_md(rep), encoding="utf-8")
    return jp, mp


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Read-only cutover readiness для projects_v2")
    ap.add_argument("--v2-root", default=None)
    ap.add_argument("--per-type", type=int, default=3)
    ap.add_argument("--no-validate", action="store_true")
    args = ap.parse_args(argv)

    v2_root = Path(args.v2_root).resolve() if args.v2_root else v2lib.projects_v2_root()
    rep = build(v2_root, per_type=args.per_type, run_validate_flag=not args.no_validate)
    jp, mp = write_reports(rep, v2_root)

    print("=== cutover readiness ===")
    print(f"recommendation: {rep['recommendation']}")
    print(f"validate: {rep['validate'].get('status')}  "
          f"drift: docs={rep['drift'].get('drift_documents')} ok={rep['drift'].get('ok')}")
    print(f"backend_parity ok={rep['backend_parity'].get('ok')}  "
          f"ui_contract ok={rep['ui_contract_parity'].get('ok')} "
          f"(checked={rep['ui_contract_parity'].get('documents_checked')})")
    print(f"dual_read ok={rep['dual_read_sample'].get('ok')} "
          f"counts={rep['dual_read_sample'].get('status_counts')}")
    print(f"v2_documents={rep['v2_documents']}  total_mismatches={rep['total_mismatches']}")
    print(f"-> {jp}\n-> {mp}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
