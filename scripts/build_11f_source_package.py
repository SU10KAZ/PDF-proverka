#!/usr/bin/env python3
"""Этап 11F — собрать пакет исходников версии для пилотного воркера.

Делает ровно то, что делает центр в `audit_job_service._build_source_package`,
и тем же кодом (`project_package.build_project_source_package`), но без базы
заданий и без сетевого ingress: 11F проверяет worker-участок, а не транспорт
центра (§34 задания прямо запрещает включать production ingress).

Читает дерево версии в ПЕРЕНОСИМОЙ раскладке `projects_v2/objects/…` — то есть
из диагностической песочницы, а не из production-каталога. Ничего в источнике
не меняет.

Пример:

    python scripts/build_11f_source_package.py \\
        --version-dir /home/coder/11f_diagnostic/sandbox/projects_v2/objects/…/versions/v001 \\
        --out /home/coder/11f_diagnostic/package/source.tar.gz
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Таблица моделей одна на этап: скрипт прогона и сборщик пакета обязаны
# объявлять одно и то же, иначе снимок разошёлся бы с фактической конфигурацией.
import importlib.util as _ilu  # noqa: E402
_spec = _ilu.spec_from_file_location(
    "_run_11f_worker_slice", Path(__file__).with_name("run_11f_worker_slice.py"),
)
_mod = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
WORKER_STAGE_MODELS = _mod.WORKER_STAGE_MODELS


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="11F — сборка пакета исходников")
    parser.add_argument("--version-dir", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--job-id", default="11f-job")
    parser.add_argument("--attempt-id", default="11f-a1")
    parser.add_argument("--discipline", default="")
    parser.add_argument("--provider-mode", choices=("fake", "real"), default="real")
    args = parser.parse_args(argv)

    from backend.app.services.common import discipline_service
    from backend.app.services.distributed_workers import (
        discipline_profile,
        project_package,
    )

    version_dir = args.version_dir.resolve()
    if not version_dir.is_dir():
        raise SystemExit(f"нет каталога версии: {version_dir}")

    identity = project_package.resolve_portable_identity(version_dir)
    discipline_id = args.discipline or identity.discipline

    # Снимки центра. Промпты и модели — те же функции, что у боевого сборщика.
    # Снимок ВСЕГО каталога промптов, как на центре: ключи там уже вида
    # `prompts/<rel>`, и внутри пакета они ложатся в `snapshot/`. Брать только
    # `prompts/pipeline` нельзя — тогда в снимок не попадает
    # `disciplines/_registry.json`, а `AUDIT_PROMPTS_DIR` на воркере уводит
    # PROMPTS_DIR целиком в снимок, и реестр дисциплин становится недостижим.
    prompts = project_package.collect_prompt_snapshot(REPO_ROOT / "prompts")
    # Модели этапов едут В ПАКЕТЕ, как на центре, но значения — worker-профиля
    # (см. `WORKER_STAGE_MODELS`). Подсунуть воркеру центральный
    # `stage_models.json` нельзя: там ensemble с ногой в OpenRouter, транспорта
    # для которой у воркера нет. Писать файл ПОСЛЕ распаковки тоже нельзя —
    # `verify_snapshot` сверяет снимок с хэшем из манифеста и отвергает
    # дописанное задним числом.
    models: dict[str, bytes] = {
        "stage_models.json": json.dumps(
            WORKER_STAGE_MODELS, ensure_ascii=False, indent=2,
        ).encode("utf-8"),
    }
    snapshot_files = {**prompts, **models}
    feature_flags: dict[str, Any] = {}

    # Профиль дисциплины — ОБЯЗАТЕЛЕН. Отправить задание без него значит
    # получить многочасовой прогон, выполненный не тем экспертом; сам сборщик
    # это и утверждает исключением, и глушить его здесь нельзя.
    from backend.app.services.common import discipline_identity

    profile_snapshot = discipline_profile.collect_profile_snapshot(
        discipline_identity.resolve_from_version_dir(version_dir),
        prompts_dir=REPO_ROOT / "prompts",
        app_data_dir=REPO_ROOT / "backend" / "app" / "data",
    )

    manifest_base = {
        "manifest_version": 1,
        "package_id": f"11f-{identity.document_code}",
        "job_id": args.job_id,
        "attempt_id": args.attempt_id,
        "project_id": identity.document_code,
        "project_external_id": identity.project_external_id or identity.document_code,
        "version_id": identity.version_id,
        "job_type": "audit_pipeline_v1",
        "execution_profile": "remote_audit_pilot_v1",
        "pipeline_revision": "11f",
        "worker_protocol_version": 1,
        "protocol_version": 1,
        "created_by": {"role": "operator", "stage": "11F"},
        "prompt_bundle_hash": project_package.hash_files(prompts),
        "model_config_hash": project_package.hash_files(models),
        "required_inputs": [],
        "discipline_id": discipline_id,
        "discipline_profile_hash": (
            profile_snapshot.tree_hash if profile_snapshot is not None else None
        ),
    }

    # Снимок runtime-конфигурации. Без него `remote_audit_runner` отказывается
    # стартовать, и правильно: режим записи хранилища иначе взялся бы с ХОСТА
    # воркера, а результат прогона зависел бы от машины.
    from backend.app.services.distributed_workers import runtime_config

    runtime_snapshot = runtime_config.build_snapshot(
        pipeline_revision="11f",
        protocol_version=1,
        package_manifest_version=1,
        execution_profile="remote_audit_pilot_v1",
        project_layout_version=project_package.PROJECT_LAYOUT_VERSION,
        projects_v2_write_mode="projects_v2_primary",
        provider_mode=args.provider_mode,
        discipline_id=discipline_id,
        discipline_profile_hash=(
            profile_snapshot.tree_hash if profile_snapshot is not None else ""
        ),
        stage_model_mapping=WORKER_STAGE_MODELS,
        prompt_bundle_hash=manifest_base["prompt_bundle_hash"],
        model_config_hash=manifest_base["model_config_hash"],
        feature_flags=feature_flags,
        feature_flags_hash=project_package.hash_json(feature_flags),
        created_at=time.time(),
    )
    runtime_config.assert_no_secrets(runtime_snapshot)
    manifest_base["runtime_snapshot_hash"] = runtime_snapshot.snapshot_hash()

    leaks = project_package.find_secrets_in_files(list(snapshot_files.items()))
    if leaks:
        raise SystemExit("в снимке найдены секреты, пакет не собран: " + "; ".join(leaks[:5]))

    manifest = project_package.build_project_source_package(
        dest_path=args.out,
        version_dir=version_dir,
        manifest_base=manifest_base,
        snapshot_files=snapshot_files,
        feature_flags=feature_flags,
        runtime_config=runtime_snapshot.to_package_bytes(),
        discipline_profile_entries=(
            profile_snapshot.package_entries() if profile_snapshot is not None else None
        ),
    )
    digest = hashlib.sha256(args.out.read_bytes()).hexdigest()
    summary = {
        "archive": str(args.out),
        "runtime_snapshot_hash": runtime_snapshot.snapshot_hash(),
        "provider_mode": args.provider_mode,
        "sha256": digest,
        "bytes": args.out.stat().st_size,
        "project_id": manifest.get("project_id"),
        "version_id": manifest.get("version_id"),
        "discipline_id": discipline_id,
        "files": len(manifest.get("files") or []),
        "version_relative_path": manifest.get("version_relative_path"),
        "excluded": manifest.get("excluded", [])[:20],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
