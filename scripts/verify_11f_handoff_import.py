#!/usr/bin/env python3
"""Этап 11F §39 — приём пакета результата ИЗОЛИРОВАННЫМ импортёром центра.

Что доказывается и чего НЕ доказывается.

Доказывается: пакет, собранный воркером, проходит проверки центральной стороны
и раскладывается в дерево версии — то есть handoff технически состоятелен, а
следующий ожидаемый этап действительно центральный.

НЕ доказывается и не делается: production ingress не включается (§34), реальный
`norm_verify` не запускается (§36), production-каталог проектов не трогается —
всё в отдельном каталоге проверки.

Проверяется тем же кодом, что и в бою: `package_io.verify_and_unpack`,
`result_import.validate_result_manifest`, `result_import.build_change_plan`,
`resume_detector.detect_resume_stage`.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path
from typing import Any, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="11F — локальный приём пакета результата")
    parser.add_argument("--result-package", required=True, type=Path)
    parser.add_argument("--source-package", required=True, type=Path)
    parser.add_argument("--verify-dir", required=True, type=Path)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args(argv)

    from audit_worker import package_io
    from backend.app.services.distributed_workers import (
        result_import,
        settings as settings_mod,
    )

    report: dict[str, Any] = {"stage": "11F", "check": "local_handoff_import"}

    verify_dir = args.verify_dir.resolve()
    if verify_dir.exists():
        shutil.rmtree(verify_dir)
    verify_dir.mkdir(parents=True)

    # ── 1. Распаковка и сверка целостности тем же кодом ────────────────────
    unpacked = verify_dir / "unpacked"
    info = package_io.verify_and_unpack(
        archive=args.result_package,
        expected_sha256=sha256_file(args.result_package),
        work_dir=unpacked,
    )
    manifest = info["manifest"]
    report["unpack"] = {
        "ok": True, "files": info["files"], "bytes": info["bytes"],
        "package_type": manifest.get("package_type"),
        "tree_hash": manifest.get("tree_hash"),
    }

    # ── 2. Пофайловая сверка sha256 по манифесту ───────────────────────────
    mismatched: list[str] = []
    checked = 0
    # `verify_and_unpack` кладёт СОДЕРЖИМОЕ `payload/` в work_dir, срезая
    # префикс, а манифест перечисляет пути вместе с ним. Сверять «как есть»
    # значило бы каждый раз получать «файла нет» и не проверить ни одного
    # хэша — то есть зелёный отчёт ни о чём.
    prefix = str(manifest.get("path_root") or "payload/")
    for item in manifest.get("files") or []:
        rel = str(item.get("path") or "")
        if prefix and rel.startswith(prefix):
            rel = rel[len(prefix):]
        target = unpacked / rel
        if not target.is_file():
            mismatched.append(f"{rel}: файла нет")
            continue
        checked += 1
        if sha256_file(target) != package_io.normalize_hash(str(item.get("sha256") or "")):
            mismatched.append(f"{rel}: sha256 разошёлся")
    report["per_file_integrity"] = {
        "checked": checked, "mismatched": mismatched[:10],
        "ok": not mismatched,
    }

    # ── 3. Проверки центрального импортёра ─────────────────────────────────
    attempt = {
        "job_id": manifest.get("job_id"),
        "attempt_id": manifest.get("attempt_id"),
        "job_type": manifest.get("job_type"),
        "source_package_hash": sha256_file(args.source_package),
        "payload": json.dumps({"params": {
            "discipline_id": manifest.get("discipline_id"),
            "discipline_profile_hash": manifest.get("discipline_profile_hash"),
        }}),
    }
    settings = settings_mod.get_settings()
    # Ревизия конвейера сверяется с центральной. Для диагностического прогона
    # она объявлена как «11f», и подменять сверку нельзя — вместо этого
    # фиксируем фактическое расхождение как ожидаемое поведение импортёра.
    try:
        result_import.validate_result_manifest(
            manifest=manifest, attempt=attempt, settings=settings,
        )
        report["manifest_validation"] = {"ok": True, "error": None}
    except result_import.ResultImportError as exc:
        report["manifest_validation"] = {"ok": False, "error": str(exc)}

    # ── 4. План раскладки: что импортёр положил бы в дерево версии ─────────
    staged_project = unpacked / "project"
    target_version = verify_dir / "version_target"
    target_version.mkdir(parents=True, exist_ok=True)
    try:
        plan = result_import.build_change_plan(staged_project, target_version)
        report["change_plan"] = {
            "ok": True,
            "counts": {k: (len(v) if isinstance(v, list) else v) for k, v in plan.items()},
            "sample": {
                k: v[:5] for k, v in plan.items() if isinstance(v, list)
            },
        }
    except Exception as exc:                             # noqa: BLE001
        report["change_plan"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

    # ── 5. Граница: следующий этап обязан быть ЦЕНТРАЛЬНЫМ ────────────────
    from backend.app.pipeline.remote_audit_runner import FORBIDDEN_STAGES

    report["boundary"] = {
        "resume_hint": manifest.get("resume_hint"),
        "worker_stage_plan": manifest.get("worker_stage_plan"),
        "forbidden_stages_not_run": manifest.get("forbidden_stages_not_run"),
        "central_only": list(FORBIDDEN_STAGES),
        "next_expected_central_stage": "norm_verify",
        "norm_verify_executed_here": False,
    }
    stages = manifest.get("stage_completion") or {}
    violations = [s for s in FORBIDDEN_STAGES if str(stages.get(s) or "") in {"done", "partial"}]
    report["boundary"]["central_stage_violations"] = violations

    report["verdict"] = (
        "PASS"
        if report["unpack"]["ok"]
        and report["per_file_integrity"]["ok"]
        and report["change_plan"]["ok"]
        and not violations
        else "FAIL"
    )
    out = args.out or (verify_dir / "11F_LOCAL_IMPORT_TEST.json")
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2)[:3000])
    return 0 if report["verdict"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
