"""Жизненный цикл удалённого задания: машина состояний и её единственный писатель.

`JobStateStore` техпроекта здесь — модуль-функции `transition()` и таблица
ALLOWED_TRANSITIONS. Правила, которые обеспечиваются машинно, а не намерением:

  * переход, отсутствующий в таблице, отвергается исключением, а не «просто
    присваивается» полю;
  * центр НЕ имеет перехода `running → failed` по молчанию — такого ребра в
    таблице нет вовсе (инварианты I-01, I-02);
  * `completed` достижим только из `validating` (I-07);
  * авто-переназначения нет: ребра `running → assigned` не существует (I-03).

Этап 0 выдаёт единственный тип задания — `test_pipeline_v1`. Полезная нагрузка
проходит валидацию pydantic на центре и ПОВТОРНО зажимается на воркере.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

from backend.app.models.distributed_workers import (
    ConnectivityState,
    JobState,
    JobType,
    RetentionState,
    TestJobParams,
)
from backend.app.services.distributed_workers import (
    auth,
    database,
    package_service,
    repositories,
    worker_registry,
)
from backend.app.services.distributed_workers.settings import DistributedWorkersSettings

# Артефакты, без которых результат тестового задания считается неполным.
TEST_JOB_REQUIRED_ARTIFACTS = ["result/summary.json", "result/run_log.txt"]

ASSIGN_TTL_SEC = 1800
RETENTION_DAYS = 30


class JobError(RuntimeError):
    """Ошибка бизнес-правила задания (не 500)."""


class IllegalTransition(JobError):
    """Переход не описан в машине состояний."""


# ─── Машина состояний ────────────────────────────────────────────────────────
# from_state -> {to_state: (кто вправе инициировать, ...)}
_W, _C, _O = "worker", "center", "operator"

ALLOWED_TRANSITIONS: dict[JobState, dict[JobState, tuple[str, ...]]] = {
    JobState.CREATED: {
        # Таблица §10.3 техпроекта называет инициатором центр (preflight +
        # выпуск попытки). На этапе 0 назначение РУЧНОЕ, и оператор запускает
        # ровно ту же последовательность одним действием, поэтому роль
        # operator допущена явно. Роль worker сюда не допущена никогда.
        JobState.ASSIGNED: (_C, _O),
        JobState.CANCELLED: (_O,),
    },
    JobState.ASSIGNED: {
        JobState.SOURCE_UPLOADING: (_W,),
        JobState.FAILED: (_C, _O),
        JobState.CANCELLED: (_O,),
    },
    JobState.SOURCE_UPLOADING: {
        JobState.SOURCE_READY: (_W,),
        JobState.SOURCE_UPLOADING: (_W,),
        JobState.FAILED: (_W, _O),
        JobState.CANCELLED: (_O,),
    },
    JobState.SOURCE_READY: {
        JobState.ACCEPTED_BY_WORKER: (_W,),
        JobState.FAILED: (_W, _O),
        JobState.CANCELLED: (_O,),
    },
    JobState.ACCEPTED_BY_WORKER: {
        JobState.RUNNING: (_W,),
        JobState.FAILED: (_W, _O),
        JobState.CANCEL_REQUESTED: (_O,),
    },
    JobState.RUNNING: {
        JobState.RUNNING: (_W,),
        JobState.COMPLETED_LOCALLY: (_W,),
        # ВНИМАНИЕ: инициатор только worker. Центр не вправе объявить провал
        # по молчанию — этого ребра для center здесь нет намеренно.
        JobState.FAILED: (_W, _O),
        JobState.CANCEL_REQUESTED: (_O,),
    },
    JobState.CANCEL_REQUESTED: {
        JobState.CANCELLED: (_W,),
        JobState.COMPLETED_LOCALLY: (_W,),   # гонка: воркер успел закончить
        JobState.CANCEL_REQUESTED: (_O,),
        JobState.FAILED: (_O,),
    },
    JobState.COMPLETED_LOCALLY: {
        JobState.RESULT_UPLOADING: (_W,),
        JobState.FAILED: (_W, _O),
    },
    JobState.RESULT_UPLOADING: {
        JobState.RESULT_UPLOADING: (_W,),
        JobState.RESULT_RECEIVED: (_W,),
        JobState.FAILED: (_C, _O),
    },
    JobState.RESULT_RECEIVED: {
        JobState.VALIDATING: (_C,),
        JobState.FAILED: (_C, _O),
    },
    JobState.VALIDATING: {
        JobState.COMPLETED: (_C,),
        JobState.FAILED: (_C,),
    },
    # Терминальные: выходов нет. «Повторить» = НОВАЯ попытка (новый attempt_id).
    JobState.COMPLETED: {},
    JobState.CANCELLED: {},
    JobState.FAILED: {
        # Единственное ребро из failed: вернувшийся старый воркер сдаёт
        # результат отозванной попытки на хранение. Публикации нет.
        JobState.SUPERSEDED_RESULT_RECEIVED: (_W,),
    },
    JobState.SUPERSEDED_RESULT_RECEIVED: {},
}


def transition(
    *,
    job_id: str,
    to_state: JobState,
    actor: str,
    reason: str = "",
    fields: Optional[dict[str, Any]] = None,
    event_seq: Optional[int] = None,
    settings: DistributedWorkersSettings,
) -> dict[str, Any]:
    """Единственная точка смены состояния задания.

    `actor` — 'worker' | 'center' | 'operator:<login>'. Роль извлекается до
    двоеточия: журнал хранит конкретного оператора, а таблица проверяет роль.
    """
    role = actor.split(":", 1)[0]
    with database.write_txn(settings) as conn:
        row = conn.execute(
            "SELECT * FROM remote_jobs WHERE job_id = ?", (job_id,)
        ).fetchone()
        if row is None:
            raise JobError(f"Задание {job_id} не найдено")
        job = dict(row)
        current = JobState(job["state"])
        allowed = ALLOWED_TRANSITIONS.get(current, {})
        if to_state not in allowed:
            raise IllegalTransition(
                f"Переход {current.value} → {to_state.value} не разрешён"
            )
        if role not in allowed[to_state]:
            raise IllegalTransition(
                f"Переход {current.value} → {to_state.value} недоступен роли {role!r} "
                f"(разрешено: {', '.join(allowed[to_state])})"
            )
        payload = dict(fields or {})
        payload["state"] = to_state.value
        assignments = ", ".join(f"{k} = ?" for k in payload)
        conn.execute(
            f"UPDATE remote_jobs SET {assignments} WHERE job_id = ?",
            (*payload.values(), job_id),
        )
        repositories.insert_transition(
            conn,
            job_id=job_id,
            attempt_id=job["attempt_id"],
            from_state=current.value,
            to_state=to_state.value,
            actor=actor,
            reason=reason,
            event_seq=event_seq,
        )
        job.update(payload)
    return job


# ─── Создание тестового задания ──────────────────────────────────────────────
def create_test_job(
    *,
    worker_id: str,
    project_id: str,
    version_id: Optional[str],
    params: TestJobParams,
    actor: str,
    settings: DistributedWorkersSettings,
) -> dict[str, Any]:
    """Создать и сразу закрепить за воркером безопасное тестовое задание.

    Ручное назначение — единственный режим этапа 0 (ADR-004: автовыбора и
    авто-переназначения нет).
    """
    worker = repositories.get_worker(worker_id, settings=settings)
    if worker is None:
        raise JobError("Воркер не найден")
    ok, why = worker_registry.can_receive_jobs(worker)
    if not ok:
        raise JobError(f"Воркер не может принять задание: {why}")

    caps = _loads(worker.get("capabilities"), {})
    supported = caps.get("job_types") or [JobType.TEST_PIPELINE_V1.value]
    if JobType.TEST_PIPELINE_V1.value not in supported:
        raise JobError(
            f"Воркер не поддерживает тип задания {JobType.TEST_PIPELINE_V1.value}"
        )

    total_seconds = params.steps * params.step_seconds
    if total_seconds > settings.test_job_max_sec:
        raise JobError(
            f"Тестовое задание длиннее потолка: {total_seconds:.0f} с > "
            f"{settings.test_job_max_sec} с (DISTRIBUTED_WORKERS_TEST_JOB_MAX_SEC)"
        )

    job = repositories.create_job(
        job_type=JobType.TEST_PIPELINE_V1.value,
        project_id=project_id,
        version_id=version_id,
        payload={"params": params.model_dump()},
        settings=settings,
    )

    compression = package_service.pick_compression(caps.get("compressions"))
    manifest = build_source_package(
        job=job, params=params, compression=compression, settings=settings
    )

    token = auth.generate_execution_token()
    updated = transition(
        job_id=job["job_id"],
        to_state=JobState.ASSIGNED,
        actor=actor,
        reason=f"ручное назначение на {worker_id}",
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
    return updated


def build_source_package(
    *,
    job: dict[str, Any],
    params: TestJobParams,
    compression: str,
    settings: DistributedWorkersSettings,
) -> dict[str, Any]:
    """Собрать исходный пакет тестового задания.

    Этап 0: содержимое СИНТЕТИЧЕСКОЕ — описание задания и человекочитаемая
    записка. Реальные деревья projects_v2 здесь не собираются: сборка
    настоящего пакета проекта — отдельный шаг (см. §25 техпроекта, шаг 2).

    В пакете нет и не может быть shell-команды, argv, кода или путей: воркер
    строит фиксированный argv сам (§4 задания).
    """
    job_id = job["job_id"]
    attempt_id = job["attempt_id"]
    package_id = repositories.new_id("pkg")
    dest_dir = settings.source_packages_dir / job_id / attempt_id
    dest_path = dest_dir / f"{package_id}{package_service.archive_suffix(compression)}"

    job_descriptor = {
        "job_type": JobType.TEST_PIPELINE_V1.value,
        "job_id": job_id,
        "attempt_id": attempt_id,
        "project_id": job["project_id"],
        "version_id": job.get("version_id"),
        "params": params.model_dump(),
        "required_result_artifacts": TEST_JOB_REQUIRED_ARTIFACTS,
    }
    readme = (
        "Тестовый пакет распределённого аудита (этап 0).\n"
        "Реальных данных проекта здесь нет.\n"
        f"job_id={job_id}\nattempt_id={attempt_id}\n"
        "Воркер обязан построить argv тестового процесса самостоятельно:\n"
        "в пакете нет ни команды, ни аргументов, ни путей.\n"
    )
    files = {
        "job.json": json.dumps(job_descriptor, ensure_ascii=False, indent=2).encode("utf-8"),
        "README.txt": readme.encode("utf-8"),
    }

    manifest = {
        "manifest_version": settings.manifest_version,
        "package_id": package_id,
        "package_type": "source",
        "job_id": job_id,
        "attempt_id": attempt_id,
        "project_id": job["project_id"],
        "version_id": job.get("version_id"),
        "job_type": JobType.TEST_PIPELINE_V1.value,
        "created_at": time.time(),
        "created_by": {"role": "center"},
        "pipeline_revision": None,
        "worker_version": None,
        "protocol_version": settings.protocol_version,
        "project_layout_version": 0,   # синтетический пакет: раскладки проекта нет
        "prompt_bundle_hash": None,
        "model_config_hash": None,
        "feature_flags_hash": None,
        "norm_snapshot_hash": None,
        "required_artifacts": ["payload/job.json"],
        "excluded_recoverable": [],
        "hardlink_groups": {},
        "path_rules": {"absolute_paths_present": False, "rewrite_on_unpack": []},
    }
    return package_service.build_package(
        dest_path=dest_path, files=files, manifest=manifest, compression=compression
    )


def source_package_path(
    job: dict[str, Any], *, settings: DistributedWorkersSettings
) -> Optional[Path]:
    directory = settings.source_packages_dir / job["job_id"] / job["attempt_id"]
    if not directory.is_dir():
        return None
    for candidate in sorted(directory.iterdir()):
        if candidate.is_file() and candidate.name != package_service.MANIFEST_NAME:
            return candidate
    return None


# ─── Финализация результата ──────────────────────────────────────────────────
def finalize_result(
    *,
    job: dict[str, Any],
    archive: Path,
    expected_hash: str,
    expected_size: int,
    settings: DistributedWorkersSettings,
) -> tuple[dict[str, Any], package_service.ValidationReport]:
    """Провалидировать и принять результат. Публикация — только после 4 проверок."""
    job_id = job["job_id"]
    attempt_id = job["attempt_id"]

    transition(
        job_id=job_id,
        to_state=JobState.VALIDATING,
        actor="center",
        reason="запуск проверки результата",
        settings=settings,
    )
    report = package_service.validate_result_package(
        archive=archive,
        expected_hash=expected_hash,
        expected_size=expected_size,
        job_id=job_id,
        attempt_id=attempt_id,
        required_artifacts=TEST_JOB_REQUIRED_ARTIFACTS,
        max_bytes=settings.max_package_bytes,
    )

    if not report.ok:
        target_dir = settings.rejected_results_dir / job_id / attempt_id
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / archive.name
        archive.replace(target)
        (target_dir / "rejection_report.json").write_text(
            json.dumps(report.as_dict(), ensure_ascii=False, indent=2), encoding="utf-8"
        )
        updated = transition(
            job_id=job_id,
            to_state=JobState.FAILED,
            actor="center",
            reason=f"валидация не пройдена: {report.error}",
            fields={
                "error": json.dumps(
                    {"code": report.error, "checks": report.checks}, ensure_ascii=False
                )
            },
            settings=settings,
        )
        return updated, report

    target_dir = settings.validated_results_dir / job_id / attempt_id
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / archive.name
    archive.replace(target)
    (target_dir / "validation_report.json").write_text(
        json.dumps(report.as_dict(), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    if report.manifest:
        (target_dir / package_service.MANIFEST_NAME).write_text(
            json.dumps(report.manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    now = time.time()
    updated = transition(
        job_id=job_id,
        to_state=JobState.COMPLETED,
        actor="center",
        reason="результат принят и проверен",
        fields={
            "validated_at": now,
            # retention_until выставляется ТОЛЬКО здесь — после подтверждённой
            # проверки. До этого у воркера он NULL и авто-удаление невозможно
            # (инвариант I-08 и признак retention_unconfirmed).
            "retention_until": now + RETENTION_DAYS * 86400,
            "retention_state": RetentionState.RETAINED.value,
            "result_package_hash": package_service.normalize_hash(expected_hash),
        },
        settings=settings,
    )
    return updated, report


def validated_result_path(
    job: dict[str, Any], *, settings: DistributedWorkersSettings
) -> Optional[Path]:
    directory = settings.validated_results_dir / job["job_id"] / job["attempt_id"]
    if not directory.is_dir():
        return None
    for candidate in sorted(directory.iterdir()):
        if candidate.is_file() and candidate.suffix not in (".json",):
            return candidate
    return None


# ─── Представление задания ───────────────────────────────────────────────────
def retention_unconfirmed(job: dict[str, Any]) -> bool:
    """Вычисляемый признак «результат есть, приём не подтверждён».

    НЕ состояние: у такого задания state может быть completed_locally,
    result_uploading или failed. Определение из §10.6 техпроекта:
    (на воркере есть материализованный результат) AND (retention_until IS NULL).
    """
    has_result = bool(job.get("completed_locally_at")) or bool(job.get("result_package_hash"))
    return has_result and job.get("retention_until") is None


def display_status(job: dict[str, Any]) -> str:
    """Вычисляемое состояние для UI: пара «что происходит + видно ли это»."""
    state = job.get("state")
    conn = job.get("connectivity_state", ConnectivityState.ONLINE.value)
    if state == JobState.RUNNING.value:
        if conn == ConnectivityState.OFFLINE.value:
            return "Выполняется, связь потеряна"
        if conn == ConnectivityState.STALE.value:
            return "Выполняется, связь нестабильна"
        return "Выполняется"
    return {
        JobState.CREATED.value: "Создано",
        JobState.ASSIGNED.value: "Назначено, ожидает воркер",
        JobState.SOURCE_UPLOADING.value: "Пакет передаётся воркеру",
        JobState.SOURCE_READY.value: "Пакет получен воркером",
        JobState.ACCEPTED_BY_WORKER.value: "Принято воркером",
        JobState.COMPLETED_LOCALLY.value: "Завершён на воркере, ожидается передача",
        JobState.RESULT_UPLOADING.value: "Результат передаётся",
        JobState.RESULT_RECEIVED.value: "Результат принят, идёт проверка",
        JobState.VALIDATING.value: "Результат принят, идёт проверка",
        JobState.COMPLETED.value: "Результат принят и проверен",
        JobState.CANCEL_REQUESTED.value: "Запрошена отмена",
        JobState.CANCELLED.value: "Отменено",
        JobState.FAILED.value: "Ошибка",
        JobState.SUPERSEDED_RESULT_RECEIVED.value: "Результат отозванной попытки",
    }.get(state or "", state or "")


def to_view(
    job: dict[str, Any], *, settings: DistributedWorkersSettings
) -> dict[str, Any]:
    view = dict(job)
    view["error"] = _loads(job.get("error"), None)
    view["progress_snapshot"] = _loads(job.get("progress_snapshot"), None)
    view["payload"] = _loads(job.get("payload"), {})
    view["retention_unconfirmed"] = retention_unconfirmed(job)
    view["display_status"] = display_status(job)
    if view["retention_unconfirmed"]:
        view["retention_warning"] = (
            "Центр не подтвердил приём — автоматическое удаление запрещено"
        )
    view.pop("execution_token_sha256", None)
    return view


def _loads(raw: Optional[str], default: Any) -> Any:
    if not raw:
        return default
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return default
