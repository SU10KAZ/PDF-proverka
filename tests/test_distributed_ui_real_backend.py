"""12I: real, read-only data projections for the distributed UI."""
from __future__ import annotations

import json
import time
import uuid

import pytest


REAL_JOB_ID = "f4f2f214-3ab4-431b-894a-de75813f0326"
FAILED_JOB_ID = "507f8151-0000-4000-8000-000000000001"
WORKER_ID = "wrk_19c87718"
SECRET_VALUE = "sk-test-value-that-must-never-leave-the-api"


@pytest.fixture()
def center_env(tmp_path, monkeypatch):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "center"))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ALLOW_INSECURE_ADMIN", "false")
    from tests.distributed_workers_helpers import enable_portal_roles

    enable_portal_roles(monkeypatch)
    from backend.app.services.distributed_workers import database
    from backend.app.services.distributed_workers.settings import get_settings

    database.reset_state_for_tests()
    settings = get_settings()
    database.ensure_ready(settings)
    yield settings
    database.reset_state_for_tests()


def _insert_job(
    settings,
    *,
    job_id: str,
    project: str,
    job_type: str = "audit_pipeline_v1",
    state: str = "assigned",
    overall: str = "active",
    central: str = "worker_running",
    result_import: str | None = None,
    worker_id: str | None = WORKER_ID,
    params: dict | None = None,
    error: dict | None = None,
    created_at: float | None = None,
) -> None:
    from backend.app.services.distributed_workers import database

    created = created_at if created_at is not None else time.time() - 1800
    attempt_id = str(uuid.uuid4())
    disposition = "active" if overall == "active" else (
        "completed" if overall == "completed" else "failed"
    )
    central_completed = created + 10 if central == "completed" else None
    with database.write_txn(settings) as conn:
        conn.execute(
            "INSERT INTO logical_jobs (job_id,project_external_id,project_display_name,"
            "project_version_id,job_type,payload,current_attempt_id,overall_state,created_at,"
            "created_by,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                job_id, project, project, "v001", job_type,
                json.dumps({"params": params or {"discipline_id": "KM"}}, ensure_ascii=False),
                attempt_id, overall, created, "test", created,
            ),
        )
        conn.execute(
            "INSERT INTO job_attempts (attempt_id,job_id,attempt_number,assignment_generation,"
            "assigned_worker_id,execution_state,attempt_disposition,connectivity_state,"
            "retention_state,created_at,assigned_at,started_at,validated_at,error_json,"
            "progress_json,central_handoff_state,central_handoff_at,central_completed_at,"
            "result_import_state) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                attempt_id, job_id, 1, 1, worker_id, state, disposition, "online",
                "retained", created, created + 10, created + 20,
                created + 8 if state == "completed" else None,
                json.dumps(error, ensure_ascii=False) if error else None,
                json.dumps({
                    "processed": 25, "total": 100, "percent_reliable": True,
                    "stage": "audit", "received_at": created + 300,
                }) if state == "running" else None,
                central, created + 10, central_completed, result_import,
            ),
        )
        conn.execute(
            "INSERT INTO job_state_transitions "
            "(job_id,attempt_id,from_state,to_state,actor,reason,at) VALUES (?,?,?,?,?,?,?)",
            (
                job_id, attempt_id, None, state, "worker",
                f"OPENROUTER_API_KEY={SECRET_VALUE}", created + 30,
            ),
        )


@pytest.fixture()
def seeded(center_env, monkeypatch):
    from backend.app.services.distributed_workers import (
        database,
        distributed_ui,
        provider_accounts,
        registration_service,
        repositories,
        worker_registry,
    )

    row = repositories.create_worker(
        display_name="Москва real", instance_id="inst-real-12i",
        worker_version="1.12.4", protocol_version=1, pipeline_revision="rev-real",
        capabilities={"job_types": ["audit_pipeline_v1"], "secret": SECRET_VALUE},
        configured_max_slots=1, settings=center_env,
    )
    # Stable production-evidence identifier; no dependent rows exist yet.
    with database.write_txn(center_env) as conn:
        conn.execute(
            "UPDATE workers SET worker_id=? WHERE worker_id=?", (WORKER_ID, row["worker_id"])
        )
    registration_service.approve_worker(
        worker_id=WORKER_ID, display_name=None, configured_max_slots=1,
        settings=center_env,
    )
    worker_registry.record_heartbeat(
        worker_id=WORKER_ID, instance_id="inst-real-12i", worker_state="idle",
        configured_max_slots=1, calculated_free_slots=1, active_jobs=[],
        resource_snapshot={
            "at": time.time(),
            "cpu": {"cores": 8, "la1": 1.5, "la5": 1.2},
            "ram": {"total_gb": 32, "available_gb": 16},
            "disk": {"total_gb": 100, "free_gb": 75},
        },
        warnings=[], settings=center_env,
        executor={"status": "online", "version": "executor-12i"},
    )
    # The projection computes connectivity without persisting it on GET.
    repositories.update_worker_fields(
        WORKER_ID, {"connection_status": "offline"}, settings=center_env
    )

    now = time.time()
    provider_accounts.record_worker_providers(
        worker_id=WORKER_ID,
        snapshots=[
            {
                "provider": "codex", "installation_status": "installed",
                "auth_state": "logged_in", "policy_state": "allowed",
                "inference_allowed": True, "credential_present": True,
                "account_fingerprint": SECRET_VALUE,
                "observed_at": now,
                "quota": {
                    "provider": "codex", "quota_state": "ready", "observed_at": now,
                    "source": "official_app_server_rpc", "confidence": "high",
                    "source_stability": "stable", "next_reset_at": now + 3600,
                    "estimated_remaining_pct": 63, "raw_remaining_supported": True,
                },
            },
            {
                "provider": "claude", "installation_status": "installed",
                "auth_state": "logged_in", "policy_state": "allowed",
                "inference_allowed": True, "credential_present": True,
                "observed_at": now,
                "quota": {
                    "provider": "claude", "quota_state": "ready", "observed_at": now,
                    "source": "unavailable", "confidence": "none",
                    "estimated_remaining_pct": 88, "raw_remaining_supported": True,
                },
            },
        ],
        settings=center_env,
        now=now,
    )

    _insert_job(
        center_env, job_id=REAL_JOB_ID, project="13АВ-РД-КМ-К2",
        state="completed", overall="completed", central="central_resume_pending",
        result_import="applied",
        params={"discipline_id": "KM", "project_layout_version": 2},
        created_at=now - 60,
    )
    _insert_job(
        center_env, job_id=FAILED_JOB_ID, project="Историческая ошибка",
        state="failed", overall="needs_operator", central="failed",
        error={"code": "WORKER_FAILED", "message": f"token={SECRET_VALUE}"},
        created_at=now - 7200,
    )
    _insert_job(
        center_env, job_id=str(uuid.uuid4()), project="Текущая проверка",
        state="running", overall="active", central="worker_running",
        created_at=now - 900,
    )
    _insert_job(
        center_env, job_id=str(uuid.uuid4()), project="Очередь",
        state="assigned", overall="active", central="worker_running",
        created_at=now - 300,
    )
    _insert_job(
        center_env, job_id=str(uuid.uuid4()), project="Canary",
        state="assigned", overall="active", central="worker_running",
        params={"discipline_id": "KM", "canary": True}, created_at=now - 200,
    )
    _insert_job(
        center_env, job_id=str(uuid.uuid4()), project="Test pipeline",
        job_type="test_pipeline_v1", state="assigned", overall="active",
        central="worker_running", created_at=now - 100,
    )

    monkeypatch.setattr(
        distributed_ui,
        "finding_count",
        lambda job: 21 if job.get("job_id") == REAL_JOB_ID else None,
    )
    return center_env


@pytest.fixture()
def viewer(seeded):
    from tests.distributed_workers_helpers import VIEWER_USER, make_center_app, portal_client

    return portal_client(make_center_app(), username=VIEWER_USER)


def test_overview_surfaces_real_12h_history_and_honest_kpis(viewer, seeded):
    from backend.app.services.distributed_workers import repositories

    response = viewer.get("/api/workers/distributed/overview")
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["metadata"] == {
        **data["metadata"],
        "mode": "real",
        "readOnly": True,
        "schedulerEnabled": False,
        "autoDispatchEnabled": False,
        "mutationsEnabled": False,
    }
    assert data["kpis"]["online"] == 1
    assert data["kpis"]["active"] == 1
    assert data["kpis"]["queued"] == 1
    assert data["kpis"]["errors"] == 0
    assert data["kpis"]["historicalNeedsOperator"] == 1
    assert data["kpis"]["completedToday"] == 1
    assert data["recommendation"]["available"] is False
    assert data["recommendation"]["source"] == "unavailable"
    assert data["recommendation"]["schedulerEnabled"] is False
    assert all("Canary" not in json.dumps(item) for item in data["queuePreview"])

    worker = data["workers"][0]
    assert worker["id"] == WORKER_ID
    assert worker["status"] == "busy"
    assert worker["connectionStatus"] == "online"
    assert worker["acceptsNewTasks"] is False
    assert worker["workerState"] == "idle"
    assert worker["resources"]["cpu"] is None
    assert worker["resources"]["gpu"] is None
    assert worker["resources"]["ram"] == 50.0
    assert worker["resources"]["disk"] == 25.0
    # A GET computes the displayed connectivity but does not refresh the DB.
    assert repositories.get_worker(WORKER_ID, settings=seeded)["connection_status"] == "offline"


def test_tasks_keep_completed_evidence_and_historical_needs_operator_separate(viewer):
    response = viewer.get("/api/workers/distributed/tasks")
    assert response.status_code == 200, response.text
    tasks = response.json()["tasks"]
    completed = next(task for task in tasks["completed"] if task["id"] == REAL_JOB_ID)
    assert completed["project"] == "13АВ-РД-КМ-К2"
    assert completed["packageName"] == "KM / v001"
    assert completed["findingCount"] == 21
    assert completed["result"] == "21 замечаний"
    assert completed["progressPercent"] == 100
    assert completed["progressKind"] == "exact"

    running = next(task for task in tasks["active"] if task["project"] == "Текущая проверка")
    assert running["stage"] == "auditing"
    assert running["progressPercent"] == 25
    assert running["progressKind"] == "exact"

    failed = next(task for task in tasks["errors"] if task["id"] == FAILED_JOB_ID)
    assert failed["stage"] == "error"
    assert failed["needsOperator"] is True
    assert failed["isActive"] is False
    assert failed["progressPercent"] is None
    assert failed["progressKind"] == "unavailable"
    assert SECRET_VALUE not in json.dumps(response.json(), ensure_ascii=False)


def test_finding_count_reads_only_the_12h_resolved_same_version(monkeypatch, tmp_path):
    from backend.app.services.distributed_workers import distributed_ui, result_import

    resolved_version = tmp_path / "documents" / "13АВ-РД-КМ-К2" / "versions" / "v001"
    latest = resolved_version / "03_analysis" / "latest"
    latest.mkdir(parents=True)
    (latest / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": index} for index in range(21)]}),
        encoding="utf-8",
    )
    seen = []

    def resolve(job):
        seen.append((job["job_id"], job["project_id"], job["version_id"]))
        return resolved_version

    monkeypatch.setattr(result_import, "_resolve_version_dir", resolve)
    job = {
        "job_id": REAL_JOB_ID,
        "project_id": "13АВ-РД-КМ-К2",
        "version_id": "v001",
        "payload": json.dumps({"params": {"project_layout_version": 2}}),
    }

    assert distributed_ui.finding_count(job) == 21
    assert seen == [(REAL_JOB_ID, "13АВ-РД-КМ-К2", "v001")]


def test_limits_and_diagnostics_are_nullable_and_credential_free(viewer):
    limits_response = viewer.get("/api/workers/distributed/limits")
    assert limits_response.status_code == 200
    row = limits_response.json()["limits"][0]
    assert row["codex"]["percentageRemaining"] == 63
    assert row["codex"]["resetAt"] is not None
    assert row["codex"]["usedToday"] is None
    assert row["claude"]["percentageRemaining"] is None
    assert row["claude"]["resetAt"] is None

    diagnostics_response = viewer.get("/api/workers/distributed/diagnostics")
    assert diagnostics_response.status_code == 200
    diagnostic = diagnostics_response.json()["diagnostics"][0]["diagnostic"]
    # Раньше здесь стояло `== "unavailable"`, и тест закреплял жёстко зашитую
    # строку из кода: поле ВСЕГДА сообщало «неизвестно», хотя реестр
    # сертификатов хранит серийник, отпечаток, срок и статус. Тест на константу
    # защищал дефект. Теперь проверяется СЕМАНТИКА: значение из допустимого
    # набора и согласовано с транспортом. В этой фикстуре сертификата нет и
    # поток не gRPC, поэтому честный ответ — «неприменимо».
    assert diagnostic["mtls"] in {
        "verified", "enrolled", "unknown", "not_applicable",
    }
    if diagnostic["transport"] != "grpc_stream":
        assert diagnostic["mtls"] == "not_applicable"
    # Срок действия отдаётся только вместе с сертификатом — числа без
    # источника здесь так же недопустимы, как и в квотах.
    if diagnostic["mtls"] in {"unknown", "not_applicable"}:
        assert diagnostic["certExpiry"] is None
        assert diagnostic["certSerial"] is None
    # Воркер этой фикстуры эксплуатационную сводку не присылал, поэтому адреса
    # шлюза нет — и пустое поле обязано быть объяснено, иначе оно неотличимо от
    # недоделки. С 12I.2 адрес приезжает от воркера, а не «не передаётся вовсе».
    assert diagnostic["gatewayTarget"] is None
    assert diagnostic["gatewayTargetNote"]
    # Числа журнала событий без источника недопустимы ровно так же, как числа
    # квот: состояние называется словом, а `null` пользователю не показывают.
    assert diagnostic["eventOutbox"]["status"] == "unavailable"
    assert diagnostic["eventOutbox"]["lastWrittenSeq"] is None
    assert diagnostic["eventOutbox"]["lastAckedSeq"] is None
    assert diagnostic["eventOutbox"]["pending"] is None
    # Релизы не выдумываются: без настроенных манифестов вердикт «неизвестно».
    assert diagnostic["releases"]["status"] == "unknown"
    assert diagnostic["releases"]["centerRelease"] is None
    assert diagnostic["releases"]["gatewayRelease"] is None
    assert diagnostic["workerRelease"] is None
    serialized = json.dumps(
        {"limits": limits_response.json(), "diagnostics": diagnostics_response.json()},
        ensure_ascii=False,
    ).lower()
    assert SECRET_VALUE.lower() not in serialized
    for forbidden in (
        "credential_present", "credential_mode", "account_fingerprint",
        "token_sha256", "execution_token", "capabilities",
    ):
        assert forbidden not in serialized


@pytest.mark.parametrize(
    ("state", "central", "expected"),
    [
        ("created", "worker_running", "queued"),
        ("source_uploading", "worker_running", "transfer"),
        ("source_ready", "worker_running", "preparing"),
        ("running", "worker_running", "auditing"),
        ("completed_locally", "worker_completed_locally", "collecting"),
        ("result_uploading", "result_uploading", "returning"),
        ("completed", "result_importing", "importing"),
        ("completed", "completed", "done"),
        ("failed", "failed", "error"),
    ],
)
def test_human_stage_mapping(state, central, expected):
    from backend.app.services.distributed_workers import distributed_ui

    assert distributed_ui.human_stage({
        "state": state, "central_handoff_state": central, "overall_state": "active",
    }) == expected


def test_human_stage_treats_12h_imported_success_as_done():
    from backend.app.services.distributed_workers import distributed_ui

    assert distributed_ui.human_stage({
        "state": "completed",
        "overall_state": "completed",
        "central_handoff_state": "central_resume_pending",
        "result_import_state": "applied",
    }) == "done"


@pytest.mark.parametrize(
    ("job", "expected"),
    [
        (
            {
                "state": "completed", "overall_state": "completed",
                "central_handoff_state": "central_resume_running",
                "result_import_state": "applied",
            },
            "done",
        ),
        (
            {
                "state": "completed", "overall_state": "active",
                "central_handoff_state": "result_imported",
            },
            "done",
        ),
        (
            {
                "state": "completed", "overall_state": "active",
                "central_handoff_state": "result_importing",
                "result_import_state": "applied",
            },
            "done",
        ),
        (
            {
                "state": "completed", "overall_state": "active",
                "central_handoff_state": "central_resume_pending",
            },
            "importing",
        ),
        (
            {
                "state": "running", "overall_state": "active",
                "central_handoff_state": "central_resume_running",
            },
            "importing",
        ),
    ],
)
def test_human_stage_import_completion_rules(job, expected):
    from backend.app.services.distributed_workers import distributed_ui

    assert distributed_ui.human_stage(job) == expected


def test_12h_like_fixture_lands_in_completed_with_finding_count(
    center_env, monkeypatch, tmp_path,
):
    from backend.app.services.distributed_workers import distributed_ui, result_import

    now = time.time()
    _insert_job(
        center_env, job_id=REAL_JOB_ID, project="13АВ-РД-КМ-К2",
        state="completed", overall="completed", central="central_resume_pending",
        result_import="applied",
        worker_id=None,
        params={"discipline_id": "KM", "project_layout_version": 2},
        created_at=now - 60,
    )
    resolved_version = tmp_path / "documents" / "13АВ-РД-КМ-К2" / "versions" / "v001"
    latest = resolved_version / "03_analysis" / "latest"
    latest.mkdir(parents=True)
    (latest / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": index} for index in range(21)]}),
        encoding="utf-8",
    )
    monkeypatch.setattr(result_import, "_resolve_version_dir", lambda job: resolved_version)

    snapshot = distributed_ui.build_snapshot(settings=center_env, now=now)
    completed = snapshot["tasks"]["completed"]
    match = next(task for task in completed if task["id"] == REAL_JOB_ID)
    assert match["stage"] == "done"
    assert match["findingCount"] == 21
    assert match["isActive"] is False
    assert all(task["id"] != REAL_JOB_ID for task in snapshot["tasks"]["errors"])
    assert all(task["id"] != REAL_JOB_ID for task in snapshot["tasks"]["active"])


def test_all_12i_routes_are_viewer_only_gets_and_no_mutation_surface(seeded):
    from backend.app.api.routers import audit_workers_admin
    from tests.distributed_workers_helpers import (
        STRANGER_USER,
        VIEWER_USER,
        make_center_app,
        portal_client,
    )

    routes = [
        route for route in audit_workers_admin.router.routes
        if getattr(route, "path", "").startswith("/api/workers/distributed/")
    ]
    assert {route.path for route in routes} == {
        "/api/workers/distributed/snapshot",
        "/api/workers/distributed/overview",
        "/api/workers/distributed/workers",
        "/api/workers/distributed/tasks",
        "/api/workers/distributed/queue",
        "/api/workers/distributed/limits",
        "/api/workers/distributed/diagnostics",
        "/api/workers/distributed/recommendation",
    }
    assert all(route.methods == {"GET"} for route in routes)

    app = make_center_app()
    viewer = portal_client(app, username=VIEWER_USER)
    stranger = portal_client(app, username=STRANGER_USER)
    assert viewer.get("/api/workers/distributed/recommendation").status_code == 200
    assert stranger.get("/api/workers/distributed/recommendation").status_code == 403

def test_worker_slot_semantics_intake_off(seeded):
    from backend.app.services.distributed_workers import database, distributed_ui

    with database.write_txn(seeded) as conn:
        conn.execute("UPDATE workers SET intake_enabled=0 WHERE worker_id=?", (WORKER_ID,))
    snap = distributed_ui.build_snapshot(settings=seeded)
    worker = next(item for item in snap["workers"] if item["id"] == WORKER_ID)
    slots = worker["slots"]
    assert slots["intakeEnabled"] is False
    assert slots["dispatchableSlots"] == 0
    assert slots["physicalFreeSlots"] == max(0, slots["totalSlots"] - slots["occupiedSlots"])


def test_resource_view_exposes_cpu_ram_gpu():
    from backend.app.services.distributed_workers import distributed_ui

    view = distributed_ui._resource_view({
        "at": 1.0,
        "cpu": {"utilization_pct": 12.5, "cores": 8, "la1": 0.4},
        "ram": {"total_gb": 32, "available_gb": 16, "used_pct": 50.0},
        "gpu": {"utilization_pct": 7.0, "used_gb": 4.0, "total_gb": 8.0},
    })
    assert view["cpu"] == 12.5
    assert view["ram"] == 50.0
    assert view["gpu"] == 7.0
    assert view["vramUsedGb"] == 4.0



def test_resource_view_reports_no_gpu_instead_of_zeroes():
    """Машина без видеокарты не должна выглядеть как машина с пустой картой.

    `nvidia-smi` на таком хосте не отвечает, и по проводу приезжают нули.
    Показать «0.0 / 0.0 ГБ VRAM» значит выдать отсутствие данных за измерение:
    на экране это неотличимо от настоящего замера. Нулевой ОБЪЁМ памяти у
    существующей карты физически невозможен, поэтому он и служит признаком
    отсутствия устройства.
    """
    from backend.app.services.distributed_workers import distributed_ui

    view = distributed_ui._resource_view({
        "at": 1.0,
        "cpu": {"utilization_pct": 5.1, "cores": 8},
        "ram": {"total_gb": 11.58, "available_gb": 7.76, "used_pct": 33.0},
        "gpu": {"used_gb": 0.0, "total_gb": 0.0},
    })
    assert view["gpuPresent"] is False
    assert view["gpu"] is None
    assert view["vramUsedGb"] is None
    assert view["vramTotalGb"] is None
    # CPU и RAM при этом обязаны остаться настоящими.
    assert view["cpu"] == 5.1
    assert view["ram"] == 33.0


def test_resource_view_reads_disk_from_heartbeat_report():
    """Диск приходит в `disk_report` в БАЙТАХ, ключа `disk` в снимке нет.

    Чтение только из `disk` давало пустые «Диск: —» при полностью исправной
    телеметрии — то есть теряло реальные сведения.
    """
    from backend.app.services.distributed_workers import distributed_ui

    view = distributed_ui._resource_view({
        "at": 1.0,
        "disk_report": {
            "total_bytes": 126711623680.0,
            "free_bytes": 55454093312.0,
            "used_bytes": None,
        },
    })
    assert view["diskTotalGb"] == 118.01
    assert view["diskFreeGb"] == 51.65
    assert view["disk"] == 56.2


def test_resource_view_keeps_unknown_distinct_from_zero():
    """Отсутствие сведений — это `None`, а не ноль."""
    from backend.app.services.distributed_workers import distributed_ui

    view = distributed_ui._resource_view({"at": 1.0})
    for key in ("cpu", "ram", "disk", "diskTotalGb", "diskFreeGb",
                "gpu", "vramUsedGb", "vramTotalGb"):
        assert view[key] is None, key


def test_disabled_intake_is_not_a_slot_count_mismatch():
    """Выключенный приём — это политика центра, а не расхождение в счёте.

    Раньше число воркера сравнивалось с `center_free`, куда уже вложены
    политические запреты. Оператор, выключивший приём, немедленно получал
    предупреждение «одна из сторон считает не то» — при полном согласии сторон
    о том, СКОЛЬКО слотов существует. Ровно это состояние (физически свободен
    1, к назначению 0) этап 12I.1 делает штатным.
    """
    from backend.app.services.distributed_workers import slots

    worker = {
        "worker_id": "wrk_test", "registration_status": "approved",
        "intake_enabled": 0, "connection_status": "online",
        "configured_max_slots": 1, "worker_reported_max_slots": 1,
        "calculated_free_slots": 1,
    }
    limit = slots.effective_limit(worker)
    usage = slots.SlotUsage(occupied=0, awaiting=0, unproven=0)
    view = slots.build_slot_view(worker, usage, limit)

    assert view["physical_free_slots"] == 1      # слот физически есть
    assert view["effective_free_slots"] == 0     # но назначать нельзя
    assert view["slot_count_mismatch"] is False  # и это НЕ расхождение
    assert view["limit_binding"] == "operator_intake"


def test_real_slot_count_mismatch_is_still_reported():
    """Настоящее расхождение обязано остаться видимым.

    Проверка нужна, чтобы исправление предыдущего теста не превратилось в
    «молчать всегда»: воркер, обещающий больше слотов, чем есть физически, —
    по-прежнему повод для предупреждения.
    """
    from backend.app.services.distributed_workers import slots

    worker = {
        "worker_id": "wrk_test", "registration_status": "approved",
        "intake_enabled": 1, "connection_status": "online",
        "configured_max_slots": 1, "worker_reported_max_slots": 1,
        "calculated_free_slots": 2,
    }
    limit = slots.effective_limit(worker)
    usage = slots.SlotUsage(occupied=0, awaiting=0, unproven=0)
    view = slots.build_slot_view(worker, usage, limit)

    assert view["slot_count_mismatch"] is True
    assert view["slot_count_mismatch_direction"] == "worker_claims_more"
    assert view["slot_count_mismatch_hint"]


def test_idle_worker_provider_is_available_not_unknown():
    """Простой воркер не делает исправного провайдера «неизвестным».

    Это вторая половина противоречия 12H: инференс в реальном аудите работал, а
    экран показывал провайдера недоступным. Причина — доступность выводилась из
    `inference_allowed`, то есть из наличия ВЫДАННОГО ГРАНТА на вызов модели.
    Гранты выпускаются под конкретную попытку и по её завершении истекают, так
    что у простаивающего воркера их нет никогда.
    """
    from backend.app.services.distributed_workers import distributed_ui
    from backend.app.services.distributed_workers.settings import get_settings

    state = {
        "installation_status": "installed",
        "auth_state": "logged_in",
        "policy_state": "allowed",
        "inference_allowed": False,          # грантов нет: воркер простаивает
        "observed_at": time.time(),
        "quota": {"quota_state": "unknown", "source": "unavailable",
                  "confidence": "none", "observed_at": time.time()},
    }
    view = distributed_ui._provider_quota(
        state, settings=get_settings(), now=time.time()
    )
    assert view["availability"] == "available"
    # Отсутствие разрешения на вызов при этом остаётся видимым отдельно.
    assert view["inferenceCapable"] is False


def test_blocked_provider_is_still_unavailable():
    """Обратная сторона: настоящая блокировка обязана остаться видимой."""
    from backend.app.services.distributed_workers import distributed_ui
    from backend.app.services.distributed_workers.settings import get_settings

    for broken in (
        {"auth_state": "logged_out"},
        {"policy_state": "denied"},
        {"installation_status": "broken"},
    ):
        state = {
            "installation_status": "installed", "auth_state": "logged_in",
            "policy_state": "allowed", "inference_allowed": True,
            "observed_at": time.time(),
            "quota": {"quota_state": "ready", "source": "unavailable",
                      "confidence": "none", "observed_at": time.time()},
            **broken,
        }
        view = distributed_ui._provider_quota(
            state, settings=get_settings(), now=time.time()
        )
        assert view["availability"] == "unavailable", broken


def test_reported_quota_is_not_marked_as_estimate():
    """Число от провайдера — не оценка.

    Контракт квот запрещает процент без `raw_remaining_supported`, поэтому
    любой дошедший до экрана процент сообщён самим провайдером. Пометка
    «оценка» занижала доверие к самому надёжному источнику из имеющихся.
    """
    from backend.app.services.distributed_workers import distributed_ui
    from backend.app.services.distributed_workers.settings import get_settings

    now = time.time()
    view = distributed_ui._provider_quota(
        {
            "installation_status": "installed", "auth_state": "logged_in",
            "policy_state": "allowed", "inference_allowed": False,
            "observed_at": now,
            "quota": {
                "quota_state": "ready", "source": "official_app_server_rpc",
                "confidence": "high", "estimated_remaining_pct": 11.0,
                "raw_remaining_supported": True, "next_reset_at": now + 3600,
                "observed_at": now,
            },
        },
        settings=get_settings(), now=now,
    )
    assert view["percentageRemaining"] == 11.0
    assert view["isEstimated"] is False
    assert view["resetAt"] is not None


def test_every_supported_provider_reaches_the_limits_row(seeded):
    """openrouter доезжал до центра, но терялся на выходе из проекции.

    И строка лимитов, и словарь quotas перечисляли claude с codex жёстко, хотя
    сам расчёт шёл по PROVIDERS. Провайдер, про которого центр всё знает, на
    экране не существовал.
    """
    from backend.app.services.distributed_workers import distributed_ui

    snapshot = distributed_ui.build_snapshot(settings=seeded)
    row = snapshot["limits"][0]
    for provider in distributed_ui.PROVIDERS:
        assert provider in row, provider
        assert isinstance(row[provider], dict), provider
    for worker in snapshot["workers"]:
        assert set(worker["quotas"]) == set(distributed_ui.PROVIDERS)
