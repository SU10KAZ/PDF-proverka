"""Создание и совместимость заданий `audit_pipeline_v1`.

Отдельный модуль, а не ветка в `job_service`: у реального аудита свои
предусловия (совместимость ревизий, снимки конфигурации, отдельный слот), и
смешивать их с тестовым контуром — верный способ однажды выдать реальный аудит
воркеру, который его не тянет.

Границы этапа зафиксированы здесь же, в коде, а не только в документе:

  * профиль ровно один — `remote_audit_pilot_v1`;
  * нормативный этап на воркере не выполняется (`include_norms=False` —
    литеральный тип, не bool);
  * одновременных реальных аудитов на воркере — не больше одного;
  * `test_pipeline_v1` и `audit_pipeline_v1` на одном воркере не смешиваются.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

from backend.app.models.distributed_workers import (
    CENTRAL_ONLY_STAGES,
    REMOTE_AUDIT_PILOT_V1,
    AuditPipelineParams,
    JobState,
    JobType,
)
from backend.app.services.distributed_workers import (
    auth,
    identifiers,
    job_service,
    package_service,
    project_package,
    repositories,
    runtime_config,
    slots,
    worker_registry,
)
from backend.app.services.distributed_workers.settings import DistributedWorkersSettings

#: Артефакты, без которых результат реального аудита неполон. Список ведётся
#: ЗДЕСЬ и дублируется на воркере: каждый рубеж держит оборону сам.
AUDIT_REQUIRED_ARTIFACTS: list[str] = [
    "work/pipeline_log.json",
    "result/03_findings.json",
    "result/audit_manifest.json",
    "usage/usage_report.json",
]

#: Артефакты, которые обязаны отсутствовать: их делает ТОЛЬКО центр.
AUDIT_FORBIDDEN_ARTIFACTS: tuple[str, ...] = (
    "result/norm_checks.json",
    "result/03a_norms_verified.json",
    "result/decision_carryover_report.json",
)

#: Максимум одновременных РЕАЛЬНЫХ аудитов на одном воркере. Доказанный
#: предел этапа — 1. Два тестовых задания ничего не говорят о двух аудитах:
#: у реального другой профиль RAM, диска и длительности.
REAL_AUDIT_MAX_SLOTS = 1


class AuditJobError(job_service.JobError):
    """Нарушение предусловия реального аудита."""


# ─── Совместимость воркера ───────────────────────────────────────────────────
def center_pipeline_revision() -> str:
    from backend.app.core import config

    return str(getattr(config, "AUDIT_PIPELINE_REVISION", "") or "").strip()


def compatibility_report(
    worker: dict[str, Any],
    *,
    settings: DistributedWorkersSettings,
    active_attempts: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    """Почему воркер (не)годится для реального аудита. Причина всегда точная.

    Молчаливое «недоступен» — худший из ответов: оператор не может ни понять,
    ни исправить. Поэтому каждая проверка возвращает свой код.
    """
    caps = job_service.worker_capabilities(worker)
    reasons: list[dict[str, str]] = []

    revision = center_pipeline_revision()
    worker_revision = str(
        worker.get("pipeline_revision") or caps.get("pipeline_revision") or ""
    ).strip()
    if not revision:
        reasons.append({
            "code": "center_revision_missing",
            "message": "AUDIT_PIPELINE_REVISION не задана на центре — "
                       "сверять ревизии не с чем",
        })
    elif worker_revision != revision:
        reasons.append({
            "code": "code_revision_mismatch",
            "message": f"Ревизия кода: воркер {worker_revision or '—'}, центр {revision}",
        })

    if int(worker.get("protocol_version") or 0) != settings.protocol_version:
        reasons.append({
            "code": "protocol_mismatch",
            "message": (
                f"Версия протокола: воркер {worker.get('protocol_version')}, "
                f"центр {settings.protocol_version}"
            ),
        })

    job_types = caps.get("job_types") or []
    if JobType.AUDIT_PIPELINE_V1.value not in job_types:
        reasons.append({
            "code": "missing_capability",
            "message": "Воркер не объявляет audit_pipeline_v1 "
                       "(AUDIT_WORKER_AUDIT_PIPELINE_ENABLED=false)",
        })

    if worker.get("registration_status") != "approved":
        reasons.append({
            "code": "not_approved",
            "message": f"Регистрация: {worker.get('registration_status')}",
        })
    if worker.get("connection_status") != "online":
        reasons.append({
            "code": "agent_offline",
            "message": f"Связь с агентом: {worker.get('connection_status')}",
        })

    executor_status = slots._executor_status(worker)          # noqa: SLF001
    if executor_status in ("offline", "interrupted"):
        reasons.append({
            "code": "executor_offline",
            "message": f"Локальный исполнитель: {executor_status}",
        })
    disk_level = slots._disk_level(worker)                    # noqa: SLF001
    if disk_level == "critical":
        reasons.append({
            "code": "disk_critical",
            "message": "Критически мало места на диске воркера",
        })

    attempts = active_attempts if active_attempts is not None else []
    audit_busy = sum(1 for a in attempts if _is_audit_attempt(a))
    test_busy = sum(
        1 for a in attempts
        if not _is_audit_attempt(a) and slots.attempt_occupies_execution_slot(a)
    )
    if audit_busy >= REAL_AUDIT_MAX_SLOTS:
        reasons.append({
            "code": "audit_slot_busy",
            "message": f"Реальный аудит уже идёт ({audit_busy}/{REAL_AUDIT_MAX_SLOTS})",
        })
    if test_busy:
        reasons.append({
            "code": "test_jobs_running",
            "message": (
                f"На воркере идут тестовые задания ({test_busy}). Реальный аудит "
                "занимает VPS целиком и стартует только после их завершения."
            ),
        })

    provider_mode = str(caps.get("provider_mode") or "unknown")
    real_llm = bool(caps.get("real_llm_enabled"))
    return {
        "worker_id": worker.get("worker_id"),
        "display_name": worker.get("display_name"),
        "compatible": not reasons,
        "reasons": reasons,
        "pipeline_revision": worker_revision or None,
        "center_pipeline_revision": revision or None,
        "protocol_version": worker.get("protocol_version"),
        "provider_mode": provider_mode,
        "real_llm_enabled": real_llm,
        "audit_slot_used": audit_busy,
        "audit_slot_limit": REAL_AUDIT_MAX_SLOTS,
        "audit_slot_label": f"{audit_busy}/{REAL_AUDIT_MAX_SLOTS}",
        "executor_status": executor_status,
        "disk_level": disk_level,
    }


def _is_audit_attempt(attempt: dict[str, Any]) -> bool:
    return str(attempt.get("job_type") or "") == JobType.AUDIT_PIPELINE_V1.value


def list_compatible_workers(*, settings: DistributedWorkersSettings) -> list[dict[str, Any]]:
    """Воркеры с пояснением, годится каждый или нет и почему."""
    out: list[dict[str, Any]] = []
    for worker in repositories.list_workers(settings=settings):
        attempts = repositories.attempts_for_worker_nonterminal(
            worker["worker_id"], settings=settings
        )
        out.append(
            compatibility_report(worker, settings=settings, active_attempts=attempts)
        )
    return out


# ─── Снимки конфигурации ─────────────────────────────────────────────────────
def build_snapshot(*, feature_flags: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """Immutable-снимок промптов, моделей и флагов на КОНКРЕТНУЮ попытку."""
    from backend.app.core import config

    prompts = project_package.collect_prompt_snapshot(Path(config.PROMPTS_DIR))
    models = project_package.collect_model_config_snapshot(
        Path(getattr(config, "STAGE_MODELS_FILE", "stage_models.json"))
    )
    flags = (
        feature_flags
        if feature_flags is not None
        else project_package.collect_feature_flags_snapshot()
    )
    files = {**prompts, **models}
    return {
        "files": files,
        "feature_flags": flags,
        "prompt_bundle_hash": project_package.hash_files(prompts),
        "model_config_hash": project_package.hash_files(models),
        "feature_flags_hash": project_package.hash_json(flags),
    }


def build_runtime_snapshot(
    *,
    snapshot: dict[str, Any],
    revision: str,
    settings: DistributedWorkersSettings,
    provider_mode: str = "fake",
) -> runtime_config.AuditRuntimeConfigSnapshot:
    """Снимок runtime-конфигурации попытки.

    **Режим записи хранилища читается ЯВНО у фасада центра.** До этого этапа он
    не передавался вовсе, воркер дефолтил в `legacy`, а центр работает в
    `projects_v2_primary`: раскладка результата зависела от машины, на которой
    шёл прогон. Значение берётся здесь и один раз — на момент создания попытки;
    смена конфигурации центра позже на эту попытку уже не влияет.

    `provider_mode` — свойство ЗАДАНИЯ, но окончательное решение остаётся за
    воркером: снимок с `real` на воркере без `AUDIT_WORKER_ALLOW_REAL_LLM`
    отвергается (`runtime_config.assert_compatible`), а не тихо понижается.
    """
    from backend.app.core import config
    from backend.app.services.storage import storage_write_facade

    stage_models: dict[str, str] = {}
    try:
        stage_models = {
            str(k): str(v)
            for k, v in (getattr(config, "STAGE_MODEL_CONFIG", {}) or {}).items()
        }
    except Exception:                                # noqa: BLE001 — снимок не блокер
        stage_models = {}

    return runtime_config.build_snapshot(
        pipeline_revision=revision,
        protocol_version=settings.protocol_version,
        package_manifest_version=settings.manifest_version,
        execution_profile=REMOTE_AUDIT_PILOT_V1,
        project_layout_version=project_package.PROJECT_LAYOUT_VERSION,
        projects_v2_write_mode=storage_write_facade.get_write_mode(),
        provider_mode=provider_mode,
        stage_model_mapping=stage_models,
        prompt_bundle_hash=snapshot["prompt_bundle_hash"],
        model_config_hash=snapshot["model_config_hash"],
        feature_flags=snapshot["feature_flags"],
        feature_flags_hash=snapshot["feature_flags_hash"],
        output_schema_versions={
            "package_manifest": settings.manifest_version,
            "project_layout": project_package.PROJECT_LAYOUT_VERSION,
            "runtime_snapshot": runtime_config.RUNTIME_SNAPSHOT_VERSION,
        },
        created_at=time.time(),
    )


# ─── Создание задания ────────────────────────────────────────────────────────
def create_audit_job(
    *,
    worker_id: str,
    project_id: str,
    version_id: Optional[str],
    version_dir: Path,
    action: str = "full",
    include_optimization: bool = True,
    retry_stage: Optional[str] = None,
    actor: str,
    display_name: str = "",
    settings: DistributedWorkersSettings,
    feature_flags: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Создать задание реального аудита и закрепить его за воркером.

    Порядок операций значим: сначала ВСЕ проверки, потом создание записи, и
    только потом сборка пакета. Обратный порядок оставлял бы на диске пакеты
    заданий, которые так и не были созданы.
    """
    worker = repositories.get_worker(worker_id, settings=settings)
    if worker is None:
        raise AuditJobError("Воркер не найден")

    attempts = repositories.attempts_for_worker_nonterminal(worker_id, settings=settings)
    report = compatibility_report(worker, settings=settings, active_attempts=attempts)
    if not report["compatible"]:
        why = "; ".join(r["message"] for r in report["reasons"])
        raise AuditJobError(f"Воркер не готов к реальному аудиту: {why}")

    ok, why = worker_registry.can_receive_jobs(worker)
    if not ok:
        raise AuditJobError(f"Воркер не может принять задание: {why}")

    external_id = identifiers.normalize_external_id(project_id, field="project_id")
    external_version = (
        identifiers.normalize_external_id(version_id, field="version_id")
        if version_id
        else None
    )

    snapshot = build_snapshot(feature_flags=feature_flags)
    revision = center_pipeline_revision()
    # Снимок runtime-конфигурации строится ДО задания: его хэш — обязательное
    # поле нагрузки, а само значение режима записи берётся у ЦЕНТРА явным
    # чтением, а не «как-нибудь на воркере».
    runtime_snapshot = build_runtime_snapshot(
        snapshot=snapshot, revision=revision, settings=settings,
    )
    params = AuditPipelineParams(
        execution_profile=REMOTE_AUDIT_PILOT_V1,
        action=action if action in ("full", "audit", "resume") else "full",
        retry_stage=retry_stage,
        include_optimization=include_optimization,
        include_norms=False,
        project_layout_version=project_package.PROJECT_LAYOUT_VERSION,
        pipeline_revision=revision,
        expected_source_tree_hash="sha256:" + "0" * 64,   # заполняется после сборки
        prompt_bundle_hash=snapshot["prompt_bundle_hash"],
        model_config_hash=snapshot["model_config_hash"],
        feature_flags_hash=snapshot["feature_flags_hash"],
        runtime_snapshot_hash=runtime_snapshot.snapshot_hash(),
        required_result_artifacts=list(AUDIT_REQUIRED_ARTIFACTS),
    )

    job = repositories.create_job(
        job_type=JobType.AUDIT_PIPELINE_V1.value,
        project_id=external_id,
        version_id=external_version,
        payload={"params": params.model_dump()},
        display_name=identifiers.normalize_display_name(
            display_name, fallback=external_id
        ),
        created_by=actor,
        settings=settings,
    )
    repositories.update_logical_job(
        job["job_id"],
        {"execution_profile": REMOTE_AUDIT_PILOT_V1, "pipeline_revision": revision},
        settings=settings,
    )

    caps = job_service.worker_capabilities(worker)
    compression = package_service.pick_compression(caps.get("compressions"))
    manifest = build_audit_source_package(
        job=job,
        version_dir=Path(version_dir),
        params=params,
        snapshot=snapshot,
        runtime_snapshot=runtime_snapshot,
        compression=compression,
        settings=settings,
    )
    # Хэш дерева известен только ПОСЛЕ сборки — дописываем его в нагрузку,
    # чтобы воркер мог сверить распакованное с заявленным.
    params = params.model_copy(
        update={"expected_source_tree_hash": manifest["source_tree_hash"]}
    )
    repositories.update_logical_job(
        job["job_id"], {"payload": json.dumps({"params": params.model_dump()},
                                              ensure_ascii=False)},
        settings=settings,
    )

    token = auth.generate_execution_token()
    updated = job_service.transition(
        job_id=job["job_id"],
        to_state=JobState.ASSIGNED,
        actor=actor,
        reason=f"ручное назначение реального аудита на {worker_id}",
        fields={
            "assigned_worker_id": worker_id,
            "assigned_at": time.time(),
            "execution_token_sha256": auth.hash_token(token),
            "package_id": manifest["package_id"],
            "source_package_hash": manifest["archive"]["sha256"],
        },
        settings=settings,
    )
    updated["_execution_token_plain"] = token
    updated["_manifest"] = manifest
    updated["_params"] = params.model_dump()
    return updated


def build_audit_source_package(
    *,
    job: dict[str, Any],
    version_dir: Path,
    params: AuditPipelineParams,
    snapshot: dict[str, Any],
    compression: str,
    settings: DistributedWorkersSettings,
    runtime_snapshot: Optional[runtime_config.AuditRuntimeConfigSnapshot] = None,
) -> dict[str, Any]:
    """Собрать переносимый пакет версии проекта."""
    package_id = repositories.new_id("pkg")
    dest_dir = identifiers.attempt_dir(
        settings.source_packages_dir, job["job_id"], job["attempt_id"]
    )
    dest_path = dest_dir / f"{package_id}{package_service.archive_suffix(compression)}"

    snapshot_files = dict(snapshot["files"])
    snapshot_files["job.json"] = json.dumps(
        {
            "job_type": JobType.AUDIT_PIPELINE_V1.value,
            "job_id": job["job_id"],
            "attempt_id": job["attempt_id"],
            "project_id": job["project_id"],
            "version_id": job.get("version_id"),
            "execution_profile": REMOTE_AUDIT_PILOT_V1,
            "params": params.model_dump(),
            "required_result_artifacts": list(AUDIT_REQUIRED_ARTIFACTS),
            "forbidden_result_artifacts": list(AUDIT_FORBIDDEN_ARTIFACTS),
            "central_only_stages": list(CENTRAL_ONLY_STAGES),
        },
        ensure_ascii=False,
        indent=2,
    ).encode("utf-8")

    manifest_base = {
        "manifest_version": settings.manifest_version,
        "package_id": package_id,
        "job_id": job["job_id"],
        "attempt_id": job["attempt_id"],
        "project_id": job["project_id"],
        "project_external_id": job["project_id"],
        "version_id": job.get("version_id"),
        "job_type": JobType.AUDIT_PIPELINE_V1.value,
        "execution_profile": REMOTE_AUDIT_PILOT_V1,
        "pipeline_revision": params.pipeline_revision,
        "worker_protocol_version": settings.protocol_version,
        "protocol_version": settings.protocol_version,
        "created_by": {"role": "center"},
        "prompt_bundle_hash": params.prompt_bundle_hash,
        "model_config_hash": params.model_config_hash,
        "global_snapshot_hash": project_package.hash_json(
            {
                "prompts": params.prompt_bundle_hash,
                "models": params.model_config_hash,
                "flags": params.feature_flags_hash,
            }
        ),
        "required_inputs": list(AUDIT_REQUIRED_ARTIFACTS),
        "runtime_snapshot_hash": params.runtime_snapshot_hash,
        "limits": {
            "max_package_bytes": settings.max_package_bytes,
        },
    }

    # Рубеж, а не «на всякий случай»: пакет уезжает на чужой VPS. Проверка идёт
    # ДО записи архива — иначе рядом остаётся sidecar-манифест уже удалённого
    # пакета. Сканируется и блоб флагов: он попадает в архив отдельной записью,
    # а фильтр `collect_feature_flags_snapshot` смотрит только на ИМЕНА ключей.
    scan_targets = [(name, blob) for name, blob in snapshot_files.items()]
    scan_targets.append(
        (
            "snapshot/feature_flags.json",
            json.dumps(snapshot["feature_flags"], ensure_ascii=False).encode("utf-8"),
        )
    )
    leaks = project_package.find_secrets_in_files(scan_targets)
    if leaks:
        raise AuditJobError(
            "В снимке конфигурации найдены секреты — пакет не собран: "
            + "; ".join(leaks[:5])
        )

    return project_package.build_project_source_package(
        dest_path=dest_path,
        version_dir=version_dir,
        manifest_base=manifest_base,
        snapshot_files=snapshot_files,
        feature_flags=snapshot["feature_flags"],
        runtime_config=(
            runtime_snapshot.to_package_bytes() if runtime_snapshot is not None else None
        ),
        compression=compression,
    )
