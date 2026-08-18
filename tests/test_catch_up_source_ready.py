"""catch_up must heal SOURCE_READY when result archive arrives first."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


@pytest.fixture()
def center_env(tmp_path, monkeypatch):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "center"))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_UPLOAD_CHUNK_BYTES", "4096")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_LONG_POLL_SEC", "1")
    from tests.distributed_workers_helpers import enable_portal_roles

    enable_portal_roles(monkeypatch)

    from backend.app.services.distributed_workers import database
    from backend.app.services.distributed_workers.settings import get_settings

    database.reset_state_for_tests()
    settings = get_settings()
    database.ensure_ready(settings)
    yield settings
    database.reset_state_for_tests()


def test_catch_up_from_source_ready_to_result_received(center_env):
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service, repositories

    job = repositories.create_job(
        project_id="p-source-ready-catchup",
        version_id="v1",
        job_type="audit_pipeline_v1",
        payload={"params": {}},
        settings=center_env,
    )
    attempt_id = job["attempt_id"]
    job_service.transition(
        attempt_id=attempt_id,
        to_state=JobState.ASSIGNED,
        actor="operator:test",
        settings=center_env,
    )
    job_service.transition(
        attempt_id=attempt_id,
        to_state=JobState.SOURCE_UPLOADING,
        actor="worker",
        settings=center_env,
    )
    job_service.transition(
        attempt_id=attempt_id,
        to_state=JobState.SOURCE_READY,
        actor="worker",
        settings=center_env,
    )
    caught = job_service.catch_up_to_result_received(
        attempt_id=attempt_id, settings=center_env
    )
    assert caught["state"] == JobState.RESULT_RECEIVED.value
    states = [
        t["to_state"]
        for t in repositories.list_transitions(job["job_id"], settings=center_env)
    ]
    assert "accepted_by_worker" in states
    assert "completed_locally" in states
    assert states[-1] == "result_received"
