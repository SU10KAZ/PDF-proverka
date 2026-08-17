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
    assert diagnostic["mtls"] == "unavailable"
    assert diagnostic["gatewayTarget"] is None
    assert diagnostic["eventOutbox"] == {
        "lastWrittenSeq": None, "lastAckedSeq": None, "pending": None,
    }
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
