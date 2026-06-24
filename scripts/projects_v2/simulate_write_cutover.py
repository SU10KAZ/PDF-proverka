#!/usr/bin/env python3
"""
simulate_write_cutover.py — DRY-RUN симуляция write/upload cutover в projects_v2.

Step 8/10 «prepare write/upload cutover». Скрипт прогоняет StorageWriteFacade на
ВРЕМЕННЫХ фикстурах (tempfile), демонстрируя поведение трёх режимов записи без
единой записи в production `projects/` или `projects_v2/`.

ГАРАНТИИ БЕЗОПАСНОСТИ:
  * НИКОГДА не пишет в реальные projects/ или projects_v2/ — только в свежую
    temp-директорию, создаваемую на каждый прогон;
  * не читает/не меняет AUDIT_STORAGE_BACKEND и не трогает env процесса
    (режим записи выставляется ЛОКАЛЬНО через monkeypatch get_write_mode);
  * на проде ничего не включает — это оффлайн-симулятор.

Что проверяется:
  1. legacy            → v2 не трогается, legacy авторитетна;
  2. dual_write_shadow → legacy ПЕРВОЙ, затем v2-тень; сбой v2 не ломает legacy;
  3. projects_v2_primary → v2 primary, legacy как архив;
  4. деструктив в v2 заблокирован.

Использование:
    python scripts/projects_v2/simulate_write_cutover.py            # человекочитаемо
    python scripts/projects_v2/simulate_write_cutover.py --json     # машинный отчёт
    python scripts/projects_v2/simulate_write_cutover.py --keep     # не удалять temp

Exit code: 0 — все инварианты выполнены; 1 — нарушение инварианта.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
from pathlib import Path

# repo root on sys.path для импорта backend.*
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.app.services.storage import storage_write_facade as swf  # noqa: E402


def _scenario(mode: str, v2_root: Path, *, fail_v2: bool = False) -> dict:
    """Прогнать один сценарий записи в заданном режиме на temp v2_root."""
    # локальный режим без изменения env процесса
    orig_get_mode = swf.get_write_mode
    swf.get_write_mode = lambda: mode  # type: ignore[assignment]
    try:
        facade = swf.StorageWriteFacade(v2_root=v2_root)
        if fail_v2:
            facade.v2_root = lambda: (_ for _ in ()).throw(RuntimeError("simulated v2 root failure"))  # type: ignore[assignment]

        target = swf.V2Target(
            object_folder="214_Alia_ASTERUS",
            discipline="EOM",
            document_code="13АВ-РД-ЭО-К3",
            version_id="v1",
        )

        order: list[str] = []
        results = []

        # имитация загрузки новой версии: метаданные + входной бандл
        results.append(facade.save_version_metadata(
            target,
            {"version_no": 1, "label": "V1", "analysis_status": "source_only"},
            legacy_write=lambda: order.append("legacy:version_meta"),
        ).to_dict())

        results.append(facade.save_input_bundle(
            target,
            [("document.pdf", b"%PDF-1.4 simulated"),
             ("document_document.md", b"# simulated markdown")],
            legacy_write=lambda: order.append("legacy:input_bundle"),
        ).to_dict())

        # имитация завершения аудита: analysis-артефакт
        results.append(facade.save_analysis_artifact(
            target,
            "03_findings.json",
            {"findings": [{"id": "F-001", "severity": "Критическое"}], "meta": {"audit_completed": "2026-06-16"}},
            run_id="run_sim",
            legacy_write=lambda: order.append("legacy:analysis"),
        ).to_dict())

        # деструктив всегда заблокирован
        destructive_blocked = False
        try:
            facade.block_destructive("clean_project_data")
        except swf.DestructiveWriteBlocked:
            destructive_blocked = True

        return {
            "mode": mode,
            "fail_v2": fail_v2,
            "legacy_call_order": order,
            "results": results,
            "destructive_blocked": destructive_blocked,
        }
    finally:
        swf.get_write_mode = orig_get_mode  # type: ignore[assignment]


def _check(report: list[dict]) -> list[str]:
    """Проверить инварианты. Вернуть список нарушений (пусто = всё ок)."""
    violations: list[str] = []
    by = {(r["mode"], r["fail_v2"]): r for r in report}

    # 1) legacy: v2 не трогается, legacy авторитетна
    leg = by[("legacy", False)]
    for res in leg["results"]:
        if res["v2_attempted"] or res["v2_ok"] is not None:
            violations.append("legacy mode attempted v2 write")
        if res["legacy_ok"] is not True:
            violations.append("legacy mode legacy_ok != True")
    if not leg["destructive_blocked"]:
        violations.append("legacy mode destructive not blocked")

    # 2) shadow (ok): legacy ПЕРВОЙ, затем v2 ok
    sh = by[("dual_write_shadow", False)]
    if sh["legacy_call_order"] != ["legacy:version_meta", "legacy:input_bundle", "legacy:analysis"]:
        violations.append(f"shadow legacy order wrong: {sh['legacy_call_order']}")
    for res in sh["results"]:
        if not (res["legacy_ok"] and res["v2_ok"] and res["legacy_authoritative"]):
            violations.append(f"shadow result not legacy-first+v2-ok: {res}")
        if not res["v2_paths"]:
            violations.append("shadow produced no v2 paths")

    # 3) shadow (v2 fails): legacy ok, v2 fail-soft
    shf = by[("dual_write_shadow", True)]
    for res in shf["results"]:
        if res["legacy_ok"] is not True:
            violations.append("shadow-fail broke legacy write")
        if res["v2_ok"] is not False:
            violations.append("shadow-fail v2_ok should be False")
        if not res["v2_error"]:
            violations.append("shadow-fail missing v2_error diagnostic")

    # 4) v2_primary: v2 ok, legacy archived, not authoritative
    pr = by[("projects_v2_primary", False)]
    for res in pr["results"]:
        if not (res["v2_ok"] and res["legacy_ok"]):
            violations.append("v2_primary result incomplete")
        if res["legacy_authoritative"]:
            violations.append("v2_primary should not mark legacy authoritative")

    return violations


def main() -> int:
    ap = argparse.ArgumentParser(description="DRY-RUN projects_v2 write cutover simulation")
    ap.add_argument("--json", action="store_true", help="машинный JSON-отчёт")
    ap.add_argument("--keep", action="store_true", help="не удалять temp-директорию")
    args = ap.parse_args()

    tmp = Path(tempfile.mkdtemp(prefix="v2_write_sim_"))
    # ЖЁСТКАЯ страховка: temp обязан быть под системным tmp, не под репозиторием
    if _REPO_ROOT in tmp.parents:
        print(f"[FATAL] temp dir {tmp} inside repo — abort", file=sys.stderr)
        return 1

    try:
        report = [
            _scenario("legacy", tmp / "legacy"),
            _scenario("dual_write_shadow", tmp / "shadow_ok"),
            _scenario("dual_write_shadow", tmp / "shadow_fail", fail_v2=True),
            _scenario("projects_v2_primary", tmp / "v2_primary"),
        ]
        violations = _check(report)

        if args.json:
            print(json.dumps({
                "temp_root": str(tmp),
                "scenarios": report,
                "violations": violations,
                "ok": not violations,
            }, ensure_ascii=False, indent=2))
        else:
            print(f"=== projects_v2 write-cutover DRY-RUN (temp={tmp}) ===\n")
            for r in report:
                tag = f"{r['mode']}" + ("  [v2 forced-fail]" if r["fail_v2"] else "")
                print(f"▸ {tag}")
                print(f"    legacy call order : {r['legacy_call_order']}")
                for res in r["results"]:
                    print(f"    {res['op']:<24} legacy_ok={res['legacy_ok']} "
                          f"v2_attempted={res['v2_attempted']} v2_ok={res['v2_ok']}"
                          + (f" v2_error={res['v2_error']}" if res['v2_error'] else ""))
                print(f"    destructive_blocked: {r['destructive_blocked']}\n")
            if violations:
                print("INVARIANT VIOLATIONS:")
                for v in violations:
                    print(f"  ✗ {v}")
            else:
                print("✓ all write-mode invariants hold; production projects/ & projects_v2/ untouched")

        return 0 if not violations else 1
    finally:
        if not args.keep:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
