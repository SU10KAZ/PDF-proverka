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
import logging
import time
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

from backend.app.models.distributed_workers import (
    AuditPipelineParams,
    TERMINAL_JOB_STATES,
    ConnectivityState,
    JobState,
    JobType,
    RetentionState,
    TestJobParams,
)
from backend.app.services.distributed_workers import (
    auth,
    database,
    identifiers,
    package_service,
    repositories,
    worker_registry,
)
from backend.app.services.distributed_workers.settings import DistributedWorkersSettings

# Артефакты, без которых результат тестового задания считается неполным.
TEST_JOB_REQUIRED_ARTIFACTS = ["result/summary.json", "result/run_log.txt"]


def required_artifacts_for(job: dict[str, Any]) -> list[str]:
    """Обязательные артефакты результата — по ТИПУ задания.

    Раньше список был один на всё. Для реального аудита он другой, и подстановка
    тестового означала бы «пакет без 03_findings.json считается полным».
    """
    if str(job.get("job_type") or "") == JobType.AUDIT_PIPELINE_V1.value:
        from backend.app.services.distributed_workers import audit_job_service

        # Список зависит не только от типа задания, но и от ДЕЙСТВИЯ: у
        # `provider_selfcheck` нет и не может быть `03_findings.json`, потому
        # что аудита он не выполняет. Подстановка общего списка означала бы
        # «пакет синтетической проверки неполон всегда» — то есть проверка
        # была бы обречена независимо от того, что произошло на воркере.
        payload = job.get("payload")
        if isinstance(payload, str):
            payload = _loads(payload, {}) or {}
        params = (payload or {}).get("params") if isinstance(payload, dict) else {}
        action = str((params or {}).get("action") or "")
        return audit_job_service.required_artifacts_for(action)
    return list(TEST_JOB_REQUIRED_ARTIFACTS)

ASSIGN_TTL_SEC = 1800
RETENTION_DAYS = 30


class JobError(RuntimeError):
    """Ошибка бизнес-правила задания (не 500)."""


class AttemptNoLongerActive(JobError):
    """Попытку отозвали, пока центр собирал и проверял её архив.

    Между «проверили, что попытка активна» и «записали результат» проходят
    минуты: распаковка и валидация многосотмегабайтного пакета. Оператор
    успевает нажать «признать потерянной». Без этой проверки результат
    отозванной попытки публиковался как актуальный (нарушение I-07).
    """


class IllegalTransition(JobError):
    """Переход не описан в машине состояний."""


# ─── Машина состояний ────────────────────────────────────────────────────────
# from_state -> {to_state: (кто вправе инициировать, ...)}
_W, _C, _O = "worker", "center", "operator"

# Ребро «сдать результат на хранение без публикации». Доступно почти из любого
# состояния, потому что отозванную попытку оператор мог застать где угодно —
# но ТОЛЬКО для попытки, чей disposition уже не 'active' (проверка в
# transition()). Публикации на этом ребре нет по построению.
_STORE_ONLY = {JobState.SUPERSEDED_RESULT_RECEIVED: ("worker",)}

ALLOWED_TRANSITIONS: dict[JobState, dict[JobState, tuple[str, ...]]] = {
    JobState.CREATED: {
        # Таблица §10.3 техпроекта называет инициатором центр (preflight +
        # выпуск попытки). На этапе 0 назначение РУЧНОЕ, и оператор запускает
        # ровно ту же последовательность одним действием, поэтому роль
        # operator допущена явно. Роль worker сюда не допущена никогда.
        JobState.ASSIGNED: (_C, _O),
        JobState.CANCEL_REQUESTED: (_O,),
        JobState.CANCELLED: (_O,),
    },
    # ВНИМАНИЕ: начиная с `source_uploading` прямого операторского ребра
    # `→cancelled` нет. Пакет уже едет к воркеру, и «отменено» без
    # подтверждения исполнителя — это ровно то враньё, которое запрещает
    # критерий готовности 6 и I-16. Путь один: `cancel_requested` → команда →
    # ACK воркера.
    #
    # У `assigned` ребро ЕСТЬ и это не отступление от правила, а его точное
    # применение. `assigned` означает «лежит в очереди центра»: единственный
    # способ передать работу воркеру — `claim_next_job_for_worker`, а он
    # атомарно переводит `assigned → source_uploading` в той же транзакции, что
    # и выборка. Значит попытка в `assigned` воркеру НЕ выдавалась ни разу:
    # процесса нет, команду посылать некому, и «отменено» здесь — факт, а не
    # предположение. Гонка с `/jobs/next` разрешается транзакционно: обе
    # стороны пишут под `BEGIN IMMEDIATE`, и проигравший видит уже изменённое
    # состояние (см. attempt_service.request_cancel и §32.1 п.24 отчёта 05).
    JobState.ASSIGNED: {
        JobState.SOURCE_UPLOADING: (_W,),
        JobState.FAILED: (_C, _O),
        JobState.CANCEL_REQUESTED: (_O,),
        JobState.CANCELLED: (_O,),
        **_STORE_ONLY,
    },
    JobState.SOURCE_UPLOADING: {
        JobState.SOURCE_READY: (_W,),
        JobState.SOURCE_UPLOADING: (_W,),
        # Ре-предложение ЕЩЁ НЕ НАЧАТОЙ работы (см. reoffer_unknown_jobs).
        # Это не нарушение I-03: I-03 запрещает переназначать РАБОТУ, которая
        # может выполняться; здесь воркер на сверке доказал, что о задании не
        # знает вовсе, значит выполнять его некому.
        JobState.ASSIGNED: (_C,),
        JobState.FAILED: (_W, _O),
        JobState.CANCEL_REQUESTED: (_O,),
        **_STORE_ONLY,
    },
    JobState.SOURCE_READY: {
        JobState.ACCEPTED_BY_WORKER: (_W,),
        JobState.ASSIGNED: (_C,),
        JobState.FAILED: (_W, _O),
        JobState.CANCEL_REQUESTED: (_O,),
        **_STORE_ONLY,
    },
    JobState.ACCEPTED_BY_WORKER: {
        JobState.RUNNING: (_W,),
        JobState.ASSIGNED: (_C,),
        JobState.FAILED: (_W, _O),
        JobState.CANCEL_REQUESTED: (_O,),
        **_STORE_ONLY,
    },
    JobState.RUNNING: {
        JobState.RUNNING: (_W,),
        JobState.COMPLETED_LOCALLY: (_W,),
        # ВНИМАНИЕ: инициатор только worker. Центр не вправе объявить провал
        # по молчанию — этого ребра для center здесь нет намеренно.
        JobState.FAILED: (_W, _O),
        JobState.CANCEL_REQUESTED: (_O,),
        **_STORE_ONLY,
    },
    JobState.CANCEL_REQUESTED: {
        # `cancelled` ставит ТОЛЬКО воркер, подтвердивший, что исполнять
        # больше нечего. Роли center здесь нет: офлайн-VPS не должен
        # превращаться в «отменено» по молчанию (критерий готовности 6).
        JobState.CANCELLED: (_W,),
        JobState.COMPLETED_LOCALLY: (_W,),   # гонка: воркер успел закончить
        JobState.CANCEL_REQUESTED: (_O,),
        JobState.FAILED: (_W, _O),
        **_STORE_ONLY,
    },
    JobState.COMPLETED_LOCALLY: {
        JobState.RESULT_UPLOADING: (_W,),
        JobState.FAILED: (_W, _O),
        JobState.CANCEL_REQUESTED: (_O,),
        **_STORE_ONLY,
    },
    JobState.RESULT_UPLOADING: {
        JobState.RESULT_UPLOADING: (_W,),
        JobState.RESULT_RECEIVED: (_W,),
        JobState.FAILED: (_C, _O),
        **_STORE_ONLY,
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
    JobState.CANCELLED: {
        # Отменённая попытка тоже может вернуться с готовым архивом: работа
        # была сделана до того, как отмена доехала. Результат сохраняем.
        **_STORE_ONLY,
    },
    JobState.FAILED: {
        # Вернувшийся старый воркер сдаёт результат отозванной попытки на
        # хранение. Публикации нет.
        **_STORE_ONLY,
    },
    JobState.SUPERSEDED_RESULT_RECEIVED: {},
}


# Терминальное состояние исполнения → «расположение» попытки, если оператор
# ещё не распорядился ей сам. Ось disposition ортогональна execution_state:
# `failed` — это законченная своим ходом попытка, а не «отозванная».
_TERMINAL_DISPOSITION: dict[JobState, str] = {
    JobState.COMPLETED: "completed",
    JobState.FAILED: "completed",
    JobState.CANCELLED: "cancelled",
    JobState.SUPERSEDED_RESULT_RECEIVED: "superseded",
}

# Расположения, назначенные ОПЕРАТОРОМ. Их автоматика не перебивает: иначе
# «признана потерянной» превратилось бы в «завершена» задним числом.
OPERATOR_DISPOSITIONS = frozenset({"operator_declared_lost", "cancelled", "superseded"})


def transition(
    *,
    job_id: Optional[str] = None,
    attempt_id: Optional[str] = None,
    to_state: JobState,
    actor: str,
    reason: str = "",
    fields: Optional[dict[str, Any]] = None,
    event_seq: Optional[int] = None,
    settings: DistributedWorkersSettings,
) -> dict[str, Any]:
    """Единственная точка смены состояния ПОПЫТКИ.

    `job_id` означает «текущая попытка задания», `attempt_id` — конкретную,
    в том числе уже отозванную (её события и результат продолжают жить своей
    жизнью, но актуальную попытку не трогают, I-07).

    `actor` — 'worker' | 'center' | 'operator:<login>'. Роль извлекается до
    двоеточия: журнал хранит конкретного оператора, а таблица проверяет роль.
    """
    if not job_id and not attempt_id:
        raise JobError("transition требует job_id или attempt_id")
    role = actor.split(":", 1)[0]
    with database.write_txn(settings) as conn:
        if attempt_id:
            row = conn.execute(
                f"{repositories.ATTEMPT_PROJECTION} WHERE a.attempt_id = ?",
                (attempt_id,),
            ).fetchone()
            missing = f"Попытка {attempt_id} не найдена"
        else:
            row = conn.execute(
                "SELECT * FROM remote_jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
            missing = f"Задание {job_id} не найдено"
        if row is None:
            raise JobError(missing)
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
        if (
            to_state is JobState.SUPERSEDED_RESULT_RECEIVED
            and (job.get("attempt_disposition") or "active") == "active"
        ):
            # Машинный запрет: «результат отозванной попытки» у АКТИВНОЙ
            # попытки невозможен. Иначе этим широким ребром можно было бы
            # похоронить нормальный результат текущей работы.
            raise IllegalTransition(
                "Складывать результат без публикации можно только у попытки, "
                "которая уже не активна"
            )
        payload = dict(fields or {})
        payload["state"] = to_state.value

        disposition = job.get("attempt_disposition") or "active"
        if to_state in _TERMINAL_DISPOSITION and disposition not in OPERATOR_DISPOSITIONS:
            payload["attempt_disposition"] = _TERMINAL_DISPOSITION[to_state]
        if to_state is JobState.CANCELLED and not job.get("cancelled_at"):
            payload.setdefault("cancelled_at", time.time())

        columns = repositories._attempt_columns(payload)  # noqa: SLF001 — общий словарь алиасов
        assignments = ", ".join(f"{k} = ?" for k in columns)
        conn.execute(
            f"UPDATE job_attempts SET {assignments} WHERE attempt_id = ?",
            (*columns.values(), job["attempt_id"]),
        )
        # Сводное состояние логического задания ведём только по ТЕКУЩЕЙ попытке:
        # хвост отозванной попытки не вправе объявить задание завершённым (I-07).
        if job.get("current_attempt_id") == job["attempt_id"]:
            overall = _overall_for(to_state, payload.get("attempt_disposition", disposition))
            if overall:
                conn.execute(
                    "UPDATE logical_jobs SET overall_state = ?, updated_at = ? "
                    "WHERE job_id = ?",
                    (overall, time.time(), job["job_id"]),
                )
        repositories.insert_transition(
            conn,
            job_id=job["job_id"],
            attempt_id=job["attempt_id"],
            from_state=current.value,
            to_state=to_state.value,
            actor=actor,
            reason=reason,
            event_seq=event_seq,
        )
        job.update(payload)
    return job


def _overall_for(state: JobState, disposition: str) -> Optional[str]:
    """Сводное состояние логического задания по состоянию текущей попытки."""
    if state is JobState.COMPLETED:
        return "completed"
    if state in TERMINAL_JOB_STATES or disposition in OPERATOR_DISPOSITIONS:
        return "needs_operator"
    return None


# ─── Создание тестового задания ──────────────────────────────────────────────
def worker_capabilities(worker: dict[str, Any]) -> dict[str, Any]:
    return _loads(worker.get("capabilities"), {}) or {}


def assignment_params(job: dict[str, Any], payload_obj: dict[str, Any]):
    """Shared strict payload parser used by polling and Agent Gateway."""
    raw = payload_obj.get("params") or {}
    if str(job.get("job_type") or "") == JobType.AUDIT_PIPELINE_V1.value:
        return AuditPipelineParams(**raw)
    return TestJobParams(**raw)


def job_params(logical_job: dict[str, Any]) -> TestJobParams:
    """Параметры задания из полезной нагрузки логического задания.

    Нужны при создании НОВОЙ попытки: исходный пакет собирается заново, но с
    теми же параметрами и новым attempt_id в манифесте.
    """
    payload = _loads(logical_job.get("payload"), {}) or {}
    return TestJobParams(**(payload.get("params") or {}))


def create_test_job(
    *,
    worker_id: str,
    project_id: str,
    version_id: Optional[str],
    params: TestJobParams,
    actor: str,
    display_name: str = "",
    settings: DistributedWorkersSettings,
    resume_existing: bool = False,
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

    caps = worker_capabilities(worker)
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

    external_id = identifiers.normalize_external_id(project_id, field="project_id")
    external_version = (
        identifiers.normalize_external_id(version_id, field="version_id")
        if version_id
        else None
    )
    resumed_created = False
    try:
        job = repositories.create_job(
            job_type=JobType.TEST_PIPELINE_V1.value,
            project_id=external_id,
            version_id=external_version,
            payload={"params": params.model_dump()},
            display_name=identifiers.normalize_display_name(
                display_name, fallback=external_id
            ),
            created_by=actor,
            settings=settings,
        )
    except repositories.ActiveJobExists:
        if not resume_existing:
            raise
        candidates = [
            item
            for item in repositories.list_jobs(limit=1000, settings=settings)
            if item.get("project_id") == external_id
            and (item.get("version_id") or None) == external_version
            and (item.get("attempt_disposition") or "active") == "active"
        ]
        if not candidates:
            raise JobError("Активное тестовое задание не найдено для resume")
        job = max(candidates, key=lambda item: float(item.get("created_at") or 0.0))
        if job.get("state") != JobState.CREATED.value:
            assigned_worker = job.get("assigned_worker_id")
            if assigned_worker not in {None, worker_id}:
                raise JobError("Существующее тестовое задание назначено другому воркеру")
            return job
        resumed_created = True

    compression = package_service.pick_compression(caps.get("compressions"))
    existing_archive = (
        source_package_path(job, settings=settings) if resumed_created else None
    )
    if existing_archive is not None:
        # A previous process can die after atomically publishing the package
        # but before persisting CREATED -> ASSIGNED.  Reuse the exact archive
        # selected by the download endpoint; rebuilding here would leave two
        # archives and make the stored SHA refer to a different file.
        manifest = package_service.read_manifest(existing_archive)
        manifest["archive"] = {
            "sha256": package_service.sha256_file(existing_archive)
        }
    else:
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
    # Путь строится ТОЛЬКО из UUID. Внешний код проекта («13АВ/РД-АР3-К7»)
    # сюда не попадает и попасть не может: identifiers отвергнет не-UUID (I-11).
    dest_dir = identifiers.attempt_dir(settings.source_packages_dir, job_id, attempt_id)
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
        # Обязательные файлы источника: воркер проверяет их наличие
        # после распаковки и отказывается работать без них.
        "required_files": ["payload/job.json", "payload/README.txt"],
        "excluded_recoverable": [],
        "hardlink_groups": {},
        "path_rules": {"absolute_paths_present": False, "rewrite_on_unpack": []},
    }
    return package_service.build_package(
        dest_path=dest_path, files=files, manifest=manifest, compression=compression
    )


def _read_dir(root: Path, job: dict[str, Any]) -> Optional[Path]:
    """Каталог попытки ДЛЯ ЧТЕНИЯ. Мигрированные ключи этапа 0 допускаются."""
    try:
        return identifiers.attempt_dir(
            root, job["job_id"], job["attempt_id"], allow_legacy=True
        )
    except identifiers.UnsafeIdentifier:
        return None


def _archive_in(directory: Optional[Path], *, skip: tuple[str, ...]) -> Optional[Path]:
    if directory is None or not directory.is_dir():
        return None
    for candidate in sorted(directory.iterdir()):
        if candidate.is_file() and candidate.name not in skip:
            return candidate
    return None


def result_package_path(
    job: dict[str, Any], *, settings: DistributedWorkersSettings
) -> Optional[Path]:
    """Принятый (провалидированный) архив результата, если он есть на диске."""
    return _archive_in(
        _read_dir(settings.validated_results_dir, job),
        skip=("validation_report.json", package_service.MANIFEST_NAME),
    )


def superseded_result_path(
    job: dict[str, Any], *, settings: DistributedWorkersSettings
) -> Optional[Path]:
    """Архив ОТОЗВАННОЙ попытки. Актуальным результатом задания не является."""
    return _archive_in(
        _read_dir(settings.superseded_results_dir, job),
        skip=("unpublished_reason.json", package_service.MANIFEST_NAME),
    )


def source_package_path(
    job: dict[str, Any], *, settings: DistributedWorkersSettings
) -> Optional[Path]:
    return _archive_in(
        _read_dir(settings.source_packages_dir, job),
        skip=(package_service.MANIFEST_NAME,),
    )


# Состояния «задание выдано, но работа ещё не начиналась». Из них безопасно
# вернуть задание в очередь: результата нет, процесса нет, терять нечего.
_PRE_RUN_STATES = (
    JobState.SOURCE_UPLOADING,
    JobState.SOURCE_READY,
    JobState.ACCEPTED_BY_WORKER,
)


def reoffer_unknown_jobs(
    *,
    worker_id: str,
    known_job_ids: set[str],
    settings: DistributedWorkersSettings,
) -> list[str]:
    """Вернуть в очередь задания, о которых воркер на сверке не заявил.

    Единственный источник такой ситуации — потерянный ответ на `/jobs/next`
    (или падение центра сразу после выдачи): у центра задание уже не
    `assigned`, поэтому опрос его не выдаст, а воркер о нём не знает и в
    reconcile не упомянет. Без этого прохода пара (project_id, version_id)
    блокировалась навсегда уникальным индексом.

    Трогаем ТОЛЬКО состояния до начала работы: `running` сюда не попадает
    никогда — там I-03 и возможный живой процесс.
    """
    reoffered: list[str] = []
    for state in _PRE_RUN_STATES:
        for job in repositories.list_jobs(
            worker_id=worker_id, state=state.value, settings=settings
        ):
            if job["job_id"] in known_job_ids:
                continue
            if (job.get("attempt_disposition") or "active") != "active":
                # Попытку уже забрал оператор (отмена, «потеряна», вытеснение).
                # Возвращать её в очередь — значит выдать заново то, чем
                # оператор распорядился вручную.
                continue
            try:
                transition(
                    attempt_id=job["attempt_id"],
                    to_state=JobState.ASSIGNED,
                    actor="center",
                    reason="воркер на сверке не знает о задании — возврат в очередь",
                    settings=settings,
                )
            except JobError:
                continue
            reoffered.append(job["job_id"])
    return reoffered


def catch_up_to_result_received(
    *, attempt_id: str, settings: DistributedWorkersSettings
) -> dict[str, Any]:
    """Догнать состояние до `result_received`, когда события отстали от пакета.

    Реальный случай: связь пропала во время аудита, воркер доработал офлайн, а
    после возврата связи первым доехал АРХИВ, а не события. Центр при этом ещё
    считает задание `running`, и прямой переход `running → result_received` в
    таблице отсутствует — раньше это давало HTTP 500, и доставить готовый
    результат было невозможно вообще.

    Здесь центр не «догадывается» о завершении по молчанию (это запрещено
    I-01/I-02): у него на руках собранный архив с сошедшимся sha256 — прямое
    доказательство, что воркер работу закончил. Поэтому проходим ровно теми
    рёбрами, что описаны в таблице, от имени worker, помечая причину.
    """
    path = {
        JobState.RUNNING: [JobState.COMPLETED_LOCALLY, JobState.RESULT_UPLOADING],
        JobState.ACCEPTED_BY_WORKER: [
            JobState.RUNNING, JobState.COMPLETED_LOCALLY, JobState.RESULT_UPLOADING,
        ],
        JobState.COMPLETED_LOCALLY: [JobState.RESULT_UPLOADING],
        JobState.RESULT_UPLOADING: [],
        # Гонка отмены: команда доехала, а воркер уже закончил работу. Ребро
        # `cancel_requested → completed_locally` заведено в таблице именно под
        # этот случай, но раньше догон им не пользовался — и готовый результат
        # не доставлялся вообще (409 по кругу).
        JobState.CANCEL_REQUESTED: [
            JobState.COMPLETED_LOCALLY, JobState.RESULT_UPLOADING,
        ],
    }
    # RESULT_RECEIVED и VALIDATING обрабатываются отдельно: перехода в
    # result_received из них нет и не нужно — финализацию надо просто
    # повторить (см. finalize_result, он идемпотентен по состоянию).
    job = repositories.get_attempt(attempt_id, settings=settings)
    if job is None:
        raise JobError(f"Попытка {attempt_id} не найдена")
    current = JobState(job["state"])
    if current in (JobState.RESULT_RECEIVED, JobState.VALIDATING):
        # Центр упал посреди финализации. Раньше отсюда выхода не было вовсе:
        # повторная попытка давала validating → validating, роутер отвечал 409,
        # а воркер бесконечно перезаливал архив. Возвращаем как есть —
        # finalize_result сам увидит, что переход уже сделан.
        return job
    if current not in path:
        return job
    for step in path[current]:
        job = transition(
            attempt_id=attempt_id,
            to_state=step,
            actor="worker",
            reason="догон состояния: архив результата получен раньше событий",
            settings=settings,
        )
    updated = transition(
        attempt_id=attempt_id,
        to_state=JobState.RESULT_RECEIVED,
        actor="worker",
        reason="архив собран, sha256 сошёлся",
        fields={"returned_at": time.time()},
        settings=settings,
    )
    _advance_handoff(attempt_id, "result_received", settings=settings)
    return updated


def store_unpublished_result(
    *,
    job: dict[str, Any],
    archive: Path,
    settings: DistributedWorkersSettings,
    reason: str = "результат получен после провала/отмены попытки",
    expected_hash: Optional[str] = None,
    expected_size: Optional[int] = None,
) -> dict[str, Any]:
    """Принять на ХРАНЕНИЕ результат попытки, которая уже не публикуется.

    Три случая ведут сюда: попытка провалена, отменена или отозвана оператором
    (§5.5, §15.2). Общее у них одно — публикации быть не может, а терять
    готовую работу нельзя: пакет складывается в отдельное хранилище с явной
    пометкой «не является актуальным результатом», и воркеру есть что
    подтвердить — иначе он вечно висел бы в retention_unconfirmed (I-08).

    Автоматического сравнения со свежим результатом и продвижения в актуальный
    здесь НЕТ и на этом этапе не планируется.
    """
    # allow_legacy: этап 0 выдавал attempt_id вида `att_1a2b3c4d`, миграция 3
    # перенесла их как есть. Без послабления на ПУТИ ЗАПИСИ любое мигрированное
    # незавершённое задание не могло сдать результат вообще — UnsafeIdentifier
    # это ValueError, его не ловил даже `except JobError` в роутере (HTTP 500).
    target_dir = identifiers.attempt_dir(
        settings.superseded_results_dir,
        job["job_id"],
        job["attempt_id"],
        allow_legacy=True,
    )
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / archive.name
    # Содержимое проверяется и на этом пути тоже. Публикации оно не даёт, но
    # воркеру разрешают удалить локальную копию — значит центр обязан знать,
    # что именно он сохранил, а не только что sha256 передачи сошёлся.
    stored_report: Optional[dict[str, Any]] = None
    if expected_hash is not None and expected_size is not None:
        try:
            probe = package_service.validate_result_package(
                archive=archive,
                expected_hash=expected_hash,
                expected_size=expected_size,
                job_id=job["job_id"],
                attempt_id=job["attempt_id"],
                required_artifacts=required_artifacts_for(job),
                max_bytes=settings.max_package_bytes,
            )
            stored_report = probe.as_dict()
        except Exception as exc:                      # noqa: BLE001 — fail-soft
            stored_report = {"ok": False, "error": f"проверка не выполнена: {exc}"}
    archive.replace(target)
    if stored_report is not None:
        (target_dir / "stored_validation_report.json").write_text(
            json.dumps(stored_report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    (target_dir / "unpublished_reason.json").write_text(
        json.dumps(
            {
                "reason": reason,
                "state_before": job["state"],
                "attempt_disposition": job.get("attempt_disposition"),
                "attempt_no": job.get("attempt_no"),
                "stored_at": time.time(),
                "published": False,
                "content_valid": None if stored_report is None else stored_report.get("ok"),
                "note": "Результат устаревшей попытки — автоматически не используется",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    now = time.time()
    fields = {
        "returned_at": now,
        "retention_until": now + RETENTION_DAYS * 86400,
        "result_storage_class": "superseded",
        "result_package_path": str(target),
        "result_acknowledged_at": now,
    }
    current = JobState(job["state"])
    if current is JobState.SUPERSEDED_RESULT_RECEIVED:
        # Повторная доставка того же архива: состояние уже конечное.
        repositories.update_attempt_fields(
            job["attempt_id"], fields, settings=settings
        )
        return {**job, **fields}
    return transition(
        attempt_id=job["attempt_id"],
        to_state=JobState.SUPERSEDED_RESULT_RECEIVED,
        actor="worker",
        reason="результат принят на хранение без публикации",
        fields=fields,
        settings=settings,
    )


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

    # Перепроверка НЕПОСРЕДСТВЕННО перед записью, а не только на входе в
    # обработчик: сборка архива занимает минуты, и решение оператора,
    # принятое в это время, обязано перевесить.
    fresh = repositories.get_attempt(attempt_id, settings=settings)
    if fresh is None:
        raise JobError(f"Попытка {attempt_id} исчезла во время приёмки результата")
    if (fresh.get("attempt_disposition") or "active") != "active":
        raise AttemptNoLongerActive(
            "Попытка отозвана оператором во время приёмки результата "
            f"({fresh.get('attempt_disposition')})"
        )
    job = fresh

    if job.get("state") != JobState.VALIDATING.value:
        transition(
            attempt_id=attempt_id,
            to_state=JobState.VALIDATING,
            actor="center",
            reason="запуск проверки результата",
            settings=settings,
        )
    _advance_handoff(attempt_id, "result_validating", settings=settings)
    # Список обязательных артефактов — ПО ТИПУ задания. Хардкод тестового
    # списка означал буквально «результат реального аудита не публикуется
    # никогда»: у него нет ни `result/summary.json`, ни `result/run_log.txt`,
    # и проверка 4 отвергала КАЖДЫЙ удалённый аудит с `missing_artifacts`.
    # Дефект не видел ни один тест: `finalize_result` вызывался только с
    # тестовыми пакетами, а сквозной прогон центрального хвоста не делался.
    report = package_service.validate_result_package(
        archive=archive,
        expected_hash=expected_hash,
        expected_size=expected_size,
        job_id=job_id,
        attempt_id=attempt_id,
        required_artifacts=required_artifacts_for(job),
        max_bytes=settings.max_package_bytes,
    )

    if not report.ok:
        target_dir = identifiers.attempt_dir(
            settings.rejected_results_dir, job_id, attempt_id, allow_legacy=True
        )
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / archive.name
        archive.replace(target)
        (target_dir / "rejection_report.json").write_text(
            json.dumps(report.as_dict(), ensure_ascii=False, indent=2), encoding="utf-8"
        )
        updated = transition(
            attempt_id=attempt_id,
            to_state=JobState.FAILED,
            actor="center",
            reason=f"валидация не пройдена: {report.error}",
            fields={
                "error": json.dumps(
                    {"code": report.error, "checks": report.checks}, ensure_ascii=False
                ),
                "result_storage_class": "rejected",
                "result_package_path": str(target),
            },
            settings=settings,
        )
        _advance_handoff(
            attempt_id, "failed", settings=settings,
            detail={"stage": "result_validation", "error": report.error},
        )
        return updated, report

    target_dir = identifiers.attempt_dir(
        settings.validated_results_dir, job_id, attempt_id, allow_legacy=True
    )
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
        attempt_id=attempt_id,
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
            "result_storage_class": "validated",
            "result_package_path": str(target),
            # Момент, с которого у воркера начинает течь срок хранения (§12.1).
            "result_acknowledged_at": now,
        },
        settings=settings,
    )
    # Архив принят и проверен. Центральный хвост при этом ещё НЕ начинался —
    # ровно эту разницу и хранит отдельная ось: `JobState.COMPLETED` здесь
    # означает «воркер отработал», а не «аудит завершён».
    _advance_handoff(attempt_id, "result_validated", settings=settings)
    return updated, report


def _advance_handoff(
    attempt_id: str,
    state: str,
    *,
    settings: DistributedWorkersSettings,
    detail: Optional[dict[str, Any]] = None,
) -> None:
    """Продвинуть ось центрального хвоста, не роняя приём результата.

    Ось диагностическая и восстановительная: её отказ не должен превращать
    успешно принятый архив в потерянный. Поэтому fail-soft — но с записью в
    лог, а не молча.
    """
    try:
        from backend.app.services.distributed_workers import central_handoff

        central_handoff.advance(
            attempt_id,
            central_handoff.HandoffState(state),
            settings=settings,
            detail=detail,
        )
    except Exception as exc:                       # noqa: BLE001 — ось не блокер
        logger.warning(
            "Ось центрального хвоста не продвинулась (%s → %s): %s",
            attempt_id, state, exc,
        )


def validated_result_path(
    job: dict[str, Any], *, settings: DistributedWorkersSettings
) -> Optional[Path]:
    directory = _read_dir(settings.validated_results_dir, job)
    if directory is None or not directory.is_dir():
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
    # Реквизиты принятого пакета: оператор должен видеть, ЧТО именно принято,
    # а не только слово «завершено». Размер берём с диска — отдельной колонки
    # для него нет, а дублировать факт в БД ради экрана незачем.
    result_path = result_package_path(job, settings=settings)
    view["result_package_size"] = result_path.stat().st_size if result_path else None
    view["result_package_name"] = result_path.name if result_path else None

    # Результат ОТОЗВАННОЙ попытки — отдельная сущность и отдельная подпись:
    # автоматически он не используется никогда (§5.5).
    superseded = superseded_result_path(job, settings=settings)
    view["superseded_result"] = (
        {
            "name": superseded.name,
            "size": superseded.stat().st_size,
            "sha256": job.get("result_package_hash"),
            "stored_at": job.get("returned_at"),
            "warning": "Результат устаревшей попытки — автоматически не используется",
        }
        if superseded
        else None
    )
    view["result_storage_class"] = job.get("result_storage_class") or "none"
    view["attempt_disposition"] = job.get("attempt_disposition")
    view["result_acknowledged"] = bool(job.get("result_acknowledged_at"))
    view["deleted_from_worker"] = bool(job.get("deleted_from_worker_at"))
    # Фактический этап ЦЕНТРАЛЬНОГО хвоста. Без него оператор видит «завершено»
    # уже в момент приёма архива, тогда как нормативный этап и Excel впереди —
    # то есть общий `running`/`completed` вводит в заблуждение ровно там, где
    # цена ошибки максимальна.
    try:
        from backend.app.services.distributed_workers import central_handoff

        view.update(central_handoff.describe(job))
    except Exception:                              # noqa: BLE001 — экран не блокер
        pass
    view.pop("execution_token_sha256", None)
    return view


def _loads(raw: Optional[str], default: Any) -> Any:
    if not raw:
        return default
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return default
