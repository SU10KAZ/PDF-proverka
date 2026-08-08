"""
test_distributed_workers_step35.py
----------------------------------
Этап 3.5 — усиление вертикального среза. Проверяется то, чего на этапе 0 не
было вовсе и из-за чего застрявшее задание чинилось правкой БД:

  * миграция разделённой модели «логическое задание ↔ попытка» без потери
    данных, транзакционность и повторный прогон;
  * операторские отмена / признание потерянной / новая попытка, их
    идемпотентность и гонки между ними;
  * вернувшаяся отозванная попытка: события в свою историю, результат — в
    отдельное хранилище, актуальная попытка не меняется;
  * persistent WorkerCommand: адресность, повторная доставка, конфликт ACK,
    срок годности, строгая схема нагрузки;
  * agent/executor двумя НАСТОЯЩИМИ процессами: убийство агента не трогает ни
    исполнителя, ни процесс аудита, и второго процесса не появляется;
  * безопасная отмена процесса по доказанной принадлежности;
  * внешние коды проектов с кириллицей и «/» при путях только из UUID;
  * RetentionManager: сухой прогон по умолчанию, запреты удаления, tombstone;
  * отсутствие XSS в новых полях экрана.

Run: python -m pytest tests/test_distributed_workers_step35.py -v
"""
from __future__ import annotations

import json
import os
import re
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

httpx = pytest.importorskip("httpx")

BOOTSTRAP = "test-bootstrap-secret-0123456789abcdef"
INTENT = {"X-Requested-With": "audit-workers"}


# ─── Фикстуры центра ─────────────────────────────────────────────────────────
@pytest.fixture()
def center_env(tmp_path, monkeypatch):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "center"))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET", BOOTSTRAP)
    monkeypatch.setenv("DISTRIBUTED_WORKERS_UPLOAD_CHUNK_BYTES", "4096")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_LONG_POLL_SEC", "1")
    monkeypatch.setenv("PORTAL_AUTH_ENABLED", "false")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ALLOW_INSECURE_ADMIN", "true")

    from backend.app.services.distributed_workers import database
    from backend.app.services.distributed_workers.settings import get_settings

    database.reset_state_for_tests()
    settings = get_settings()
    database.ensure_ready(settings)
    yield settings
    database.reset_state_for_tests()


@pytest.fixture()
def client(center_env):
    from tests.distributed_workers_helpers import SyncASGITransport, make_center_app

    return httpx.Client(
        transport=SyncASGITransport(make_center_app()),
        base_url="http://center",
        headers={"X-Requested-With": "audit-workers"},
    )


def _approved_worker(client, instance_id="inst_step35_0001", name="VPS-35"):
    registered = client.post(
        "/api/v1/worker/register",
        json={"instance_id": instance_id, "protocol_version": 1,
              "display_name_hint": name},
        headers={"Authorization": f"Bearer {BOOTSTRAP}", "X-Protocol-Version": "1"},
    ).json()
    worker_id = registered["worker_id"]
    client.post(f"/api/workers/{worker_id}/approve", json={"configured_max_slots": 1})
    token = client.post(
        "/api/v1/worker/claim",
        json={"worker_id": worker_id, "instance_id": instance_id,
              "claim_secret": registered["claim_secret"]},
    ).json()["worker_token"]
    headers = {"Authorization": f"Bearer {token}", "X-Worker-Id": worker_id,
               "X-Protocol-Version": "1"}
    client.post(
        "/api/v1/worker/heartbeat",
        json={"instance_id": instance_id, "sent_at": time.time()},
        headers=headers,
    )
    return worker_id, headers


def _create_job(client, worker_id, project="ПРОЕКТ/тест 1"):
    response = client.post(
        "/api/workers/jobs",
        json={"worker_id": worker_id, "project_id": project,
              "params": {"label": "s35", "steps": 1, "step_seconds": 0.0}},
    )
    assert response.status_code == 200, response.text
    return response.json()["job"]


def _take_job(client, headers):
    response = client.post(
        "/api/v1/worker/jobs/next", json={"free_slots": 1, "wait_sec": 0},
        headers=headers,
    )
    assert response.status_code == 200, response.text
    return response.json()


def _running_attempt(client, worker_id, headers, project="ПРОЕКТ/тест 1"):
    """Довести задание до running и вернуть (job_id, attempt_id, exec_token)."""
    _create_job(client, worker_id, project=project)
    assignment = _take_job(client, headers)
    token = assignment["execution_token"]
    auth = {**headers, "X-Execution-Token": token}
    client.post(
        f"/api/v1/worker/jobs/{assignment['job_id']}/accept",
        json={"attempt_id": assignment["attempt_id"], "accepted_at": time.time(),
              "source_verified": {"sha256_ok": True, "manifest_version": 1}},
        headers=auth,
    )
    client.post(
        "/api/v1/worker/events",
        json={
            "job_id": assignment["job_id"], "attempt_id": assignment["attempt_id"],
            "first_seq": 1, "count": 1,
            "events": [{"seq": 1, "event_id": "e1", "event_type": "job_started",
                        "occurred_at": time.time(), "payload": {}}],
        },
        headers=auth,
    )
    return assignment["job_id"], assignment["attempt_id"], token


# ═══ §18.1 Миграции ══════════════════════════════════════════════════════════
def _build_step0_database(path: Path) -> tuple[str, str]:
    """Создать базу ровно в состоянии этапа 0 (схема версии 2) с данными."""
    from backend.app.services.distributed_workers import schema

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    schema.apply_pragmas(conn)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations "
        "(version INTEGER PRIMARY KEY, applied_at REAL NOT NULL)"
    )
    for version in (1, 2):
        for statement in schema.MIGRATIONS[version]:
            conn.execute(statement)
        conn.execute("INSERT INTO schema_migrations VALUES (?, 0.0)", (version,))
    conn.execute(
        "INSERT INTO workers (worker_id, display_name, instance_id, worker_version,"
        " protocol_version, capabilities, registration_status, worker_state,"
        " configured_max_slots, created_at, updated_at)"
        " VALUES ('wrk_old', 'старый VPS', 'inst_old00000001', '0.1.0', 1, '{}',"
        " 'approved', 'idle', 1, 1.0, 1.0)"
    )
    job_id = str(uuid.uuid4())
    attempt_id = "att_legacy1"
    conn.execute(
        "INSERT INTO remote_jobs (job_id, job_type, project_id, version_id, attempt_id,"
        " attempt_no, state, connectivity_state, retention_state, payload, created_at,"
        " assigned_worker_id, result_package_hash, validated_at, retention_until,"
        " last_event_seq)"
        " VALUES (?, 'test_pipeline_v1', 'СТАРЫЙ/проект', 'v1', ?, 1, 'completed',"
        " 'online', 'retained', '{\"params\": {}}', 100.0, 'wrk_old', 'abc123',"
        " 200.0, 300.0, 42)",
        (job_id, attempt_id),
    )
    conn.execute(
        "INSERT INTO job_state_transitions (job_id, attempt_id, from_state, to_state,"
        " actor, reason, at) VALUES (?, ?, NULL, 'created', 'center', 'seed', 1.0)",
        (job_id, attempt_id),
    )
    conn.commit()
    conn.close()
    return job_id, attempt_id


def test_step0_database_migrates_without_data_loss(tmp_path):
    """База этапа 0 доводится до v3: задания, результаты и история целы."""
    from backend.app.services.distributed_workers import schema

    db = tmp_path / "workers.db"
    job_id, attempt_id = _build_step0_database(db)

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    schema.apply_pragmas(conn)
    assert schema.current_version(conn) == 2

    assert schema.migrate(conn) == schema.SCHEMA_VERSION == 3

    logical = conn.execute(
        "SELECT * FROM logical_jobs WHERE job_id = ?", (job_id,)
    ).fetchone()
    assert logical["project_external_id"] == "СТАРЫЙ/проект"
    assert logical["current_attempt_id"] == attempt_id
    assert logical["overall_state"] == "completed"

    attempt = conn.execute(
        "SELECT * FROM job_attempts WHERE attempt_id = ?", (attempt_id,)
    ).fetchone()
    assert attempt["attempt_number"] == 1
    assert attempt["execution_state"] == "completed"
    assert attempt["attempt_disposition"] == "completed"
    assert attempt["result_package_hash"] == "abc123"
    assert attempt["result_storage_class"] == "validated"
    assert attempt["retention_until"] == 300.0
    assert attempt["last_event_seq"] == 42

    # Историческое имя осталось читаемым — представлением «текущая попытка».
    view = conn.execute("SELECT * FROM remote_jobs WHERE job_id = ?", (job_id,)).fetchone()
    assert view["project_id"] == "СТАРЫЙ/проект"
    assert view["state"] == "completed"
    # Журнал переходов не тронут.
    assert conn.execute(
        "SELECT COUNT(*) FROM job_state_transitions WHERE job_id = ?", (job_id,)
    ).fetchone()[0] == 1
    conn.close()


def test_migration_is_idempotent(tmp_path):
    """Повторный прогон миграций ничего не ломает и не дублирует."""
    from backend.app.services.distributed_workers import schema

    db = tmp_path / "workers.db"
    job_id, _ = _build_step0_database(db)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    schema.apply_pragmas(conn)
    schema.migrate(conn)
    assert schema.migrate(conn) == 3
    assert schema.migrate(conn) == 3
    assert conn.execute("SELECT COUNT(*) FROM job_attempts").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM logical_jobs").fetchone()[0] == 1
    conn.close()


def test_failed_migration_leaves_no_half_applied_schema(tmp_path, monkeypatch):
    """Сбой посреди миграции откатывается целиком: отметки версии нет."""
    from backend.app.services.distributed_workers import schema

    db = tmp_path / "workers.db"
    _build_step0_database(db)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    schema.apply_pragmas(conn)

    broken = list(schema.MIGRATIONS[3])
    broken.insert(2, "SELECT ЭТО_НЕ_SQL(")
    monkeypatch.setitem(schema.MIGRATIONS, 3, tuple(broken))

    with pytest.raises(sqlite3.Error):
        schema.migrate(conn)

    assert schema.current_version(conn) == 2
    tables = {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    # Первые операторы миграции откатились вместе со всем шагом.
    assert "logical_jobs" not in tables
    assert "remote_jobs" in tables      # старая таблица на месте
    conn.close()


def test_backup_is_taken_before_migration(tmp_path, monkeypatch):
    """Перед миграцией снимается согласованная копия базы."""
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET", BOOTSTRAP)

    from backend.app.services.distributed_workers import database
    from backend.app.services.distributed_workers.settings import get_settings

    database.reset_state_for_tests()
    _build_step0_database(tmp_path / "workers.db")
    database.ensure_ready(get_settings())

    backups = list(tmp_path.glob("workers.db.before_v*"))
    assert backups, "резервная копия перед миграцией не снята"
    # Копия читается и содержит СТАРУЮ схему.
    old = sqlite3.connect(backups[0])
    tables = {r[0] for r in old.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "remote_jobs" in tables and "job_attempts" not in tables
    old.close()
    database.reset_state_for_tests()


def test_only_one_active_attempt_per_job(center_env):
    """Частичный уникальный индекс не даёт завести вторую активную попытку."""
    from backend.app.services.distributed_workers import repositories

    job = repositories.create_job(
        job_type="test_pipeline_v1", project_id="п/1", version_id=None,
        payload={}, settings=center_env,
    )
    with pytest.raises(repositories.ActiveAttemptExists):
        repositories.create_next_attempt(
            job_id=job["job_id"], worker_id=None, settings=center_env
        )


def test_attempt_numbers_are_never_reused(center_env):
    """UNIQUE(job_id, attempt_number): номер попытки не переиспользуется."""
    from backend.app.services.distributed_workers import repositories

    worker = repositories.create_worker(
        display_name="VPS", instance_id="inst_reuse_0001", worker_version="1",
        protocol_version=1, pipeline_revision=None, capabilities={},
        configured_max_slots=1, settings=center_env,
    )
    job = repositories.create_job(
        job_type="test_pipeline_v1", project_id="п/2", version_id=None,
        payload={}, settings=center_env,
    )
    repositories.update_attempt_fields(
        job["attempt_id"], {"attempt_disposition": "cancelled"}, settings=center_env
    )
    second = repositories.create_next_attempt(
        job_id=job["job_id"], worker_id=worker["worker_id"], settings=center_env
    )
    assert second["attempt_no"] == 2
    attempts = repositories.list_attempts(job["job_id"], settings=center_env)
    assert [a["attempt_no"] for a in attempts] == [1, 2]
    # Старая строка не перезаписана.
    assert attempts[0]["attempt_id"] == job["attempt_id"]


def test_view_columns_match_attempt_projection(center_env):
    """Представление remote_jobs и проекция попытки не разъезжаются."""
    from backend.app.services.distributed_workers import database, repositories

    job = repositories.create_job(
        job_type="test_pipeline_v1", project_id="п/3", version_id=None,
        payload={}, settings=center_env,
    )
    with database.read_conn(center_env) as conn:
        view = conn.execute("SELECT * FROM remote_jobs LIMIT 1").fetchone()
    attempt = repositories.get_attempt(job["attempt_id"], settings=center_env)
    assert set(view.keys()) == set(attempt.keys())


# ═══ §18.2 Операторские действия ═════════════════════════════════════════════
def test_cancel_requires_reason_and_confirmation(client, center_env):
    worker_id, headers = _approved_worker(client)
    job_id, attempt_id, _ = _running_attempt(client, worker_id, headers)
    base = f"/api/workers/jobs/{job_id}/attempts/{attempt_id}/cancel"

    # Без заголовка намерения — 403 (CSRF-эквивалент). Клиент фикстуры шлёт
    # его по умолчанию (как настоящая страница), поэтому здесь снимаем явно.
    client.headers.pop("X-Requested-With", None)
    assert client.post(base, json={"reason": "r", "confirmation": "ОТМЕНИТЬ"},
                       headers={"Idempotency-Key": "k1"}).status_code == 403
    client.headers["X-Requested-With"] = "audit-workers"
    # Без Idempotency-Key — 400.
    assert client.post(base, json={"reason": "r", "confirmation": "ОТМЕНИТЬ"},
                       headers=INTENT).status_code == 400
    # Неверная подтверждающая фраза — 400.
    assert client.post(base, json={"reason": "r", "confirmation": "да"},
                       headers={**INTENT, "Idempotency-Key": "k2"}).status_code == 400
    # Пустая причина отбивается схемой.
    assert client.post(base, json={"reason": "", "confirmation": "ОТМЕНИТЬ"},
                       headers={**INTENT, "Idempotency-Key": "k3"}).status_code == 422


def test_cancel_creates_persistent_command_and_does_not_fake_cancelled(client, center_env):
    """Отмена — просьба: состояние cancel_requested, а не cancelled."""
    from backend.app.services.distributed_workers import repositories

    worker_id, headers = _approved_worker(client)
    job_id, attempt_id, _ = _running_attempt(client, worker_id, headers)
    response = client.post(
        f"/api/workers/jobs/{job_id}/attempts/{attempt_id}/cancel",
        json={"reason": "оператор передумал", "confirmation": "ОТМЕНИТЬ"},
        headers={**INTENT, "Idempotency-Key": "cancel-1"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["outcome"] == "cancel_requested"
    assert body["state"] == "cancel_requested"
    assert body["command_id"]

    attempt = repositories.get_attempt(attempt_id, settings=center_env)
    assert attempt["state"] == "cancel_requested"
    assert attempt["attempt_disposition"] == "active"
    # Команда персистентна и адресована этой попытке.
    command = repositories.get_command(body["command_id"], settings=center_env)
    assert command["worker_id"] == worker_id
    assert command["attempt_id"] == attempt_id
    assert command["command_type"] == "cancel_attempt"


def test_cancel_is_idempotent(client, center_env):
    worker_id, headers = _approved_worker(client)
    job_id, attempt_id, _ = _running_attempt(client, worker_id, headers)
    payload = {"reason": "дважды", "confirmation": "ОТМЕНИТЬ"}
    key = {**INTENT, "Idempotency-Key": "same-key"}
    first = client.post(
        f"/api/workers/jobs/{job_id}/attempts/{attempt_id}/cancel",
        json=payload, headers=key).json()
    second = client.post(
        f"/api/workers/jobs/{job_id}/attempts/{attempt_id}/cancel",
        json=payload, headers=key).json()
    assert second.get("replayed") is True
    assert second["command_id"] == first["command_id"]


def test_cancel_of_completed_attempt_keeps_result(client, center_env):
    """Задача успела закончиться — отмена не переписывает историю (§15.1)."""
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service, repositories

    worker_id, headers = _approved_worker(client)
    job_id, attempt_id, _ = _running_attempt(client, worker_id, headers)
    for state, actor in (("completed_locally", "worker"), ("result_uploading", "worker"),
                         ("result_received", "worker"), ("validating", "center"),
                         ("completed", "center")):
        job_service.transition(attempt_id=attempt_id, to_state=JobState(state),
                               actor=actor, reason="тест", settings=center_env)
    response = client.post(
        f"/api/workers/jobs/{job_id}/attempts/{attempt_id}/cancel",
        json={"reason": "поздно", "confirmation": "ОТМЕНИТЬ"},
        headers={**INTENT, "Idempotency-Key": "late-cancel"},
    )
    assert response.json()["outcome"] == "already_final"
    assert repositories.get_attempt(attempt_id, settings=center_env)["state"] == "completed"


def test_cancel_of_foreign_attempt_is_rejected(client, center_env):
    worker_id, headers = _approved_worker(client)
    job_id, attempt_id, _ = _running_attempt(client, worker_id, headers)
    other = _create_job(client, worker_id, project="другой/проект")
    response = client.post(
        f"/api/workers/jobs/{other['job_id']}/attempts/{attempt_id}/cancel",
        json={"reason": "чужая", "confirmation": "ОТМЕНИТЬ"},
        headers={**INTENT, "Idempotency-Key": "foreign"},
    )
    assert response.status_code == 409
    assert "не найдена" in response.json()["detail"]


def test_mark_lost_does_not_claim_process_stopped(client, center_env):
    """Признание потерянной меняет ТОЛЬКО ось disposition (I-06)."""
    from backend.app.services.distributed_workers import repositories

    worker_id, headers = _approved_worker(client)
    job_id, attempt_id, _ = _running_attempt(client, worker_id, headers)
    response = client.post(
        f"/api/workers/jobs/{job_id}/attempts/{attempt_id}/mark-lost",
        json={"mandatory_reason": "VPS молчит сутки",
              "typed_confirmation": "ПОПЫТКА ПОТЕРЯНА",
              "observed_worker_state": "offline",
              "optional_operator_note": ""},
        headers={**INTENT, "Idempotency-Key": "lost-1"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["attempt_disposition"] == "operator_declared_lost"
    # execution_state НЕ подменён выдуманным failed.
    assert body["execution_state"] == "running"
    assert "может продолжать работу" in body["message"]

    attempt = repositories.get_attempt(attempt_id, settings=center_env)
    assert attempt["state"] == "running"
    assert attempt["attempt_disposition"] == "operator_declared_lost"
    assert attempt["declared_lost_at"]
    logical = repositories.get_logical_job(job_id, settings=center_env)
    assert logical["overall_state"] == "needs_operator"


def test_mark_lost_wrong_confirmation_and_replay(client, center_env):
    worker_id, headers = _approved_worker(client)
    job_id, attempt_id, _ = _running_attempt(client, worker_id, headers)
    url = f"/api/workers/jobs/{job_id}/attempts/{attempt_id}/mark-lost"
    bad = client.post(url, json={"mandatory_reason": "r", "typed_confirmation": "ага"},
                      headers={**INTENT, "Idempotency-Key": "bad"})
    assert bad.status_code == 400

    payload = {"mandatory_reason": "r", "typed_confirmation": "ПОПЫТКА ПОТЕРЯНА"}
    key = {**INTENT, "Idempotency-Key": "lost-same"}
    assert client.post(url, json=payload, headers=key).status_code == 200
    again = client.post(url, json=payload, headers=key).json()
    assert again["replayed"] is True


def test_new_attempt_is_forbidden_over_running(client, center_env):
    worker_id, headers = _approved_worker(client)
    job_id, attempt_id, _ = _running_attempt(client, worker_id, headers)
    response = client.post(
        f"/api/workers/jobs/{job_id}/attempts",
        json={"worker_id": worker_id, "reason": "хочу заново",
              "source_attempt_id": attempt_id, "confirmation": "НОВАЯ ПОПЫТКА"},
        headers={**INTENT, "Idempotency-Key": "new-over-running"},
    )
    assert response.status_code == 409
    assert "работающей" in response.json()["detail"]


def test_new_attempt_after_mark_lost(client, center_env):
    """Новая попытка получает свой номер, токен и поколение; старая цела."""
    from backend.app.services.distributed_workers import repositories

    worker_id, headers = _approved_worker(client)
    job_id, attempt_id, old_token = _running_attempt(client, worker_id, headers)
    client.post(
        f"/api/workers/jobs/{job_id}/attempts/{attempt_id}/mark-lost",
        json={"mandatory_reason": "молчит", "typed_confirmation": "ПОПЫТКА ПОТЕРЯНА"},
        headers={**INTENT, "Idempotency-Key": "lost"},
    )
    created = client.post(
        f"/api/workers/jobs/{job_id}/attempts",
        json={"worker_id": worker_id, "reason": "повтор",
              "source_attempt_id": attempt_id, "confirmation": "НОВАЯ ПОПЫТКА"},
        headers={**INTENT, "Idempotency-Key": "new-1"},
    )
    assert created.status_code == 200, created.text
    body = created.json()
    assert body["attempt_number"] == 2
    assert body["assignment_generation"] == 2
    assert body["superseded_attempt_id"] == attempt_id

    attempts = repositories.list_attempts(job_id, settings=center_env)
    assert len(attempts) == 2
    old = next(a for a in attempts if a["attempt_id"] == attempt_id)
    assert old["attempt_disposition"] == "operator_declared_lost"
    assert old["superseded_by_attempt"] == body["attempt_id"]
    new = next(a for a in attempts if a["attempt_id"] == body["attempt_id"])
    assert new["state"] == "assigned"
    assert new["execution_token_sha256"] != old["execution_token_sha256"]


def test_two_identical_create_attempt_requests_make_one(client, center_env):
    worker_id, headers = _approved_worker(client)
    job_id, attempt_id, _ = _running_attempt(client, worker_id, headers)
    client.post(
        f"/api/workers/jobs/{job_id}/attempts/{attempt_id}/mark-lost",
        json={"mandatory_reason": "молчит", "typed_confirmation": "ПОПЫТКА ПОТЕРЯНА"},
        headers={**INTENT, "Idempotency-Key": "lost"},
    )
    payload = {"worker_id": worker_id, "reason": "повтор",
               "source_attempt_id": attempt_id, "confirmation": "НОВАЯ ПОПЫТКА"}
    key = {**INTENT, "Idempotency-Key": "double-click"}
    first = client.post(f"/api/workers/jobs/{job_id}/attempts",
                        json=payload, headers=key).json()
    second = client.post(f"/api/workers/jobs/{job_id}/attempts",
                         json=payload, headers=key).json()
    assert second["replayed"] is True
    assert second["attempt_id"] == first["attempt_id"]

    from backend.app.services.distributed_workers import repositories

    assert len(repositories.list_attempts(job_id, settings=center_env)) == 2


def test_two_different_create_attempt_requests_conflict(client, center_env):
    """Разные ключи, но активная попытка одна: второй получает 409."""
    worker_id, headers = _approved_worker(client)
    job_id, attempt_id, _ = _running_attempt(client, worker_id, headers)
    client.post(
        f"/api/workers/jobs/{job_id}/attempts/{attempt_id}/mark-lost",
        json={"mandatory_reason": "молчит", "typed_confirmation": "ПОПЫТКА ПОТЕРЯНА"},
        headers={**INTENT, "Idempotency-Key": "lost"},
    )
    payload = {"worker_id": worker_id, "reason": "повтор",
               "source_attempt_id": attempt_id, "confirmation": "НОВАЯ ПОПЫТКА"}
    first = client.post(f"/api/workers/jobs/{job_id}/attempts", json=payload,
                        headers={**INTENT, "Idempotency-Key": "k1"})
    second = client.post(f"/api/workers/jobs/{job_id}/attempts", json=payload,
                         headers={**INTENT, "Idempotency-Key": "k2"})
    assert first.status_code == 200
    assert second.status_code == 409


def test_attempt_history_endpoint(client, center_env):
    worker_id, headers = _approved_worker(client)
    job_id, attempt_id, _ = _running_attempt(client, worker_id, headers)
    client.post(
        f"/api/workers/jobs/{job_id}/attempts/{attempt_id}/cancel",
        json={"reason": "стоп", "confirmation": "ОТМЕНИТЬ"},
        headers={**INTENT, "Idempotency-Key": "c1"},
    )
    data = client.get(f"/api/workers/jobs/{job_id}/attempts").json()
    assert data["job"]["project_external_id"] == "ПРОЕКТ/тест 1"
    attempt = data["attempts"][0]
    assert attempt["is_current"] is True
    assert attempt["attempt_number"] == 1
    assert attempt["attempt_disposition"] == "active"
    assert attempt["commands"] and attempt["commands"][0]["command_type"] == "cancel_attempt"
    assert any(a["action_type"] == "cancel_attempt" for a in attempt["operator_actions"])


def test_admin_action_log_is_append_only(client, center_env):
    """Журнал пишется и НЕ имеет ручки удаления."""
    from backend.app.api.routers import audit_workers_admin

    worker_id, headers = _approved_worker(client)
    job_id, attempt_id, _ = _running_attempt(client, worker_id, headers)
    client.post(
        f"/api/workers/jobs/{job_id}/attempts/{attempt_id}/mark-lost",
        json={"mandatory_reason": "нет связи", "typed_confirmation": "ПОПЫТКА ПОТЕРЯНА"},
        headers={**INTENT, "Idempotency-Key": "lost"},
    )
    actions = client.get(f"/api/workers/admin-actions?job_id={job_id}").json()["actions"]
    kinds = {a["action_type"] for a in actions}
    assert {"create_job", "mark_attempt_lost"} <= kinds
    # Действия над воркером к заданию не привязаны — видны в общем списке.
    everything = {a["action_type"]
                  for a in client.get("/api/workers/admin-actions").json()["actions"]}
    assert "approve_worker" in everything
    entry = next(a for a in actions if a["action_type"] == "mark_attempt_lost")
    # Actor берётся из аутентификации, а не из тела запроса.
    assert entry["actor_id"].startswith("operator:")
    assert entry["reason"] == "нет связи"
    assert entry["previous_state"]["execution_state"] == "running"

    methods = {
        (route.path, method)
        for route in audit_workers_admin.router.routes
        for method in getattr(route, "methods", set())
    }
    assert not [p for p, m in methods if m == "DELETE"], "в API нет DELETE и быть не должно"


def test_dangerous_endpoints_absent_without_portal_auth(tmp_path, monkeypatch):
    """PORTAL_AUTH_ENABLED=false → операторский контур не монтируется (fail-closed)."""
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "c"))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET", BOOTSTRAP)
    monkeypatch.setenv("PORTAL_AUTH_ENABLED", "false")
    monkeypatch.delenv("DISTRIBUTED_WORKERS_ALLOW_INSECURE_ADMIN", raising=False)

    from backend.app.core import portal_auth
    from backend.app.services.distributed_workers.settings import get_settings

    settings = get_settings()
    assert settings.allow_insecure_admin is False
    assert portal_auth.get_settings().enabled is False
    # Условие монтирования из main.py: оба ложны → роутер не поднимается.
    assert not (portal_auth.get_settings().enabled or settings.allow_insecure_admin)


# ═══ §18.3 Результат отозванной попытки ══════════════════════════════════════
def test_old_attempt_events_do_not_touch_the_new_one(client, center_env):
    """События вернувшейся потерянной попытки идут в ЕЁ историю (I-07)."""
    from backend.app.services.distributed_workers import repositories

    worker_id, headers = _approved_worker(client)
    job_id, old_attempt, old_token = _running_attempt(client, worker_id, headers)
    client.post(
        f"/api/workers/jobs/{job_id}/attempts/{old_attempt}/mark-lost",
        json={"mandatory_reason": "молчит", "typed_confirmation": "ПОПЫТКА ПОТЕРЯНА"},
        headers={**INTENT, "Idempotency-Key": "lost"},
    )
    new = client.post(
        f"/api/workers/jobs/{job_id}/attempts",
        json={"worker_id": worker_id, "reason": "повтор",
              "source_attempt_id": old_attempt, "confirmation": "НОВАЯ ПОПЫТКА"},
        headers={**INTENT, "Idempotency-Key": "new"},
    ).json()
    new_attempt = new["attempt_id"]

    # Старый воркер возвращается со СВОИМ токеном.
    response = client.post(
        "/api/v1/worker/events",
        json={
            "job_id": job_id, "attempt_id": old_attempt, "first_seq": 2, "count": 1,
            "events": [{"seq": 2, "event_id": "old-1", "event_type": "job_failed",
                        "occurred_at": time.time(),
                        "payload": {"code": "late", "message": "поздний отчёт"}}],
        },
        headers={**headers, "X-Execution-Token": old_token},
    )
    assert response.status_code == 200, response.text

    old_row = repositories.get_attempt(old_attempt, settings=center_env)
    new_row = repositories.get_attempt(new_attempt, settings=center_env)
    assert old_row["state"] == "failed"          # изменилась ТОЛЬКО старая
    assert new_row["state"] == "assigned"
    assert new_row["error"] is None
    events = repositories.list_events(job_id, attempt_id=old_attempt, settings=center_env)
    assert any(e["event_type"] == "job_failed" for e in events)
    assert repositories.list_events(
        job_id, attempt_id=new_attempt, settings=center_env) == []


def test_old_attempt_result_goes_to_superseded_storage(client, center_env, tmp_path):
    """Результат отозванной попытки сохраняется отдельно и не публикуется."""
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service, repositories

    worker_id, headers = _approved_worker(client)
    job_id, old_attempt, _ = _running_attempt(client, worker_id, headers)
    client.post(
        f"/api/workers/jobs/{job_id}/attempts/{old_attempt}/mark-lost",
        json={"mandatory_reason": "молчит", "typed_confirmation": "ПОПЫТКА ПОТЕРЯНА"},
        headers={**INTENT, "Idempotency-Key": "lost"},
    )
    new = client.post(
        f"/api/workers/jobs/{job_id}/attempts",
        json={"worker_id": worker_id, "reason": "повтор",
              "source_attempt_id": old_attempt, "confirmation": "НОВАЯ ПОПЫТКА"},
        headers={**INTENT, "Idempotency-Key": "new"},
    ).json()

    archive = tmp_path / "late.tar.gz"
    archive.write_bytes("работа старой попытки".encode("utf-8"))
    attempt_row = repositories.get_attempt(old_attempt, settings=center_env)
    stored = job_service.store_unpublished_result(
        job=attempt_row, archive=archive, settings=center_env)

    assert stored["state"] == JobState.SUPERSEDED_RESULT_RECEIVED.value
    kept = center_env.superseded_results_dir / job_id / old_attempt / "late.tar.gz"
    assert kept.is_file()
    marker = json.loads(
        (kept.parent / "unpublished_reason.json").read_text(encoding="utf-8"))
    assert marker["published"] is False
    assert "не используется" in marker["note"]

    # Актуальная попытка не тронута, задание не «завершено».
    new_row = repositories.get_attempt(new["attempt_id"], settings=center_env)
    assert new_row["state"] == "assigned"
    logical = repositories.get_logical_job(job_id, settings=center_env)
    assert logical["overall_state"] == "active"
    assert logical["current_attempt_id"] == new["attempt_id"]

    # Скачать старый пакет можно, и он подписан как устаревший.
    download = client.get(
        f"/api/workers/jobs/{job_id}/attempts/{old_attempt}/result")
    assert download.status_code == 200
    assert "УСТАРЕВШАЯ" in download.headers.get("content-disposition", "").upper() \
        or "attachment" in download.headers.get("content-disposition", "")


def test_active_attempt_cannot_be_stored_as_superseded(center_env):
    """Широкое ребро «на хранение» закрыто для АКТИВНОЙ попытки."""
    from backend.app.services.distributed_workers import job_service, repositories

    job = repositories.create_job(
        job_type="test_pipeline_v1", project_id="п/актив", version_id=None,
        payload={}, settings=center_env,
    )
    with pytest.raises(job_service.IllegalTransition):
        job_service.transition(
            attempt_id=job["attempt_id"],
            to_state=__import__(
                "backend.app.models.distributed_workers", fromlist=["JobState"]
            ).JobState.SUPERSEDED_RESULT_RECEIVED,
            actor="worker", reason="попытка обойти", settings=center_env,
        )


def test_old_execution_token_cannot_touch_new_attempt(client, center_env):
    """Старый токен не даёт прав на новую попытку."""
    worker_id, headers = _approved_worker(client)
    job_id, old_attempt, old_token = _running_attempt(client, worker_id, headers)
    client.post(
        f"/api/workers/jobs/{job_id}/attempts/{old_attempt}/mark-lost",
        json={"mandatory_reason": "молчит", "typed_confirmation": "ПОПЫТКА ПОТЕРЯНА"},
        headers={**INTENT, "Idempotency-Key": "lost"},
    )
    new = client.post(
        f"/api/workers/jobs/{job_id}/attempts",
        json={"worker_id": worker_id, "reason": "повтор",
              "source_attempt_id": old_attempt, "confirmation": "НОВАЯ ПОПЫТКА"},
        headers={**INTENT, "Idempotency-Key": "new"},
    ).json()
    # Старым токеном пытаемся отчитаться от имени НОВОЙ попытки.
    response = client.post(
        "/api/v1/worker/events",
        json={"job_id": job_id, "attempt_id": new["attempt_id"], "first_seq": 1,
              "count": 1,
              "events": [{"seq": 1, "event_id": "x", "event_type": "job_started",
                          "occurred_at": time.time(), "payload": {}}]},
        headers={**headers, "X-Execution-Token": old_token},
    )
    assert response.status_code == 409
    assert response.json()["detail"]["error"] == "attempt_superseded"


# ═══ §18.4 WorkerCommand ═════════════════════════════════════════════════════
def test_command_delivered_only_to_owner_and_survives_redelivery(client, center_env):
    from backend.app.services.distributed_workers import repositories

    worker_id, headers = _approved_worker(client)
    other_id, other_headers = _approved_worker(client, "inst_other_000002", "VPS-2")
    command = repositories.enqueue_command(
        worker_id=worker_id, command_type="cancel_attempt",
        payload={"job_id": "j", "attempt_id": "a"},
        idempotency_key="c-1", job_id="j", attempt_id="a", settings=center_env,
    )
    # Чужому не выдаётся.
    assert client.get("/api/v1/worker/commands", headers=other_headers).json()["commands"] == []
    # Своему выдаётся и ПОВТОРНО тоже (доставка ≠ исполнение).
    first = client.post("/api/v1/worker/commands/next", json={"wait_sec": 0},
                        headers=headers).json()["commands"]
    assert [c["command_id"] for c in first] == [command["command_id"]]
    second = client.post("/api/v1/worker/commands/next", json={"wait_sec": 0},
                         headers=headers).json()["commands"]
    assert [c["command_id"] for c in second] == [command["command_id"]]


def test_command_ack_conflict_and_replay(client, center_env):
    from backend.app.services.distributed_workers import repositories

    worker_id, headers = _approved_worker(client)
    command = repositories.enqueue_command(
        worker_id=worker_id, command_type="cancel_attempt",
        payload={"job_id": "j", "attempt_id": "a"},
        idempotency_key="c-2", job_id="j", attempt_id="a", settings=center_env,
    )
    url = f"/api/v1/worker/commands/{command['command_id']}/ack"
    result = {"status": "ok", "detail": {"outcome": "not_running_locally"}}
    assert client.post(url, json={"result": result, "acknowledged_at": time.time()},
                       headers=headers).status_code == 200
    replay = client.post(url, json={"result": result, "acknowledged_at": time.time()},
                         headers=headers)
    assert replay.status_code == 200 and replay.json()["replayed"] is True
    conflict = client.post(
        url,
        json={"result": {"status": "ok", "detail": {"outcome": "cancelled"}},
              "acknowledged_at": time.time()},
        headers=headers,
    )
    assert conflict.status_code == 409


def test_expired_command_is_not_delivered(client, center_env):
    from backend.app.services.distributed_workers import repositories

    worker_id, headers = _approved_worker(client)
    repositories.enqueue_command(
        worker_id=worker_id, command_type="cancel_attempt",
        payload={"job_id": "j", "attempt_id": "a"},
        idempotency_key="c-3", job_id="j", attempt_id="a",
        ttl_sec=-1, settings=center_env,
    )
    assert client.get("/api/v1/worker/commands", headers=headers).json()["commands"] == []


def test_unknown_command_type_and_extra_fields_are_rejected(client, center_env):
    """Незнакомый тип и лишнее поле не доезжают до воркера, но и не висят вечно."""
    from backend.app.services.distributed_workers import repositories

    worker_id, headers = _approved_worker(client)
    bad_type = repositories.enqueue_command(
        worker_id=worker_id, command_type="run_shell", payload={"cmd": "rm -rf /"},
        idempotency_key="bad-1", settings=center_env,
    )
    bad_payload = repositories.enqueue_command(
        worker_id=worker_id, command_type="cancel_attempt",
        payload={"job_id": "j", "attempt_id": "a", "sudo": True},
        idempotency_key="bad-2", job_id="j", attempt_id="a", settings=center_env,
    )
    delivered = client.get("/api/v1/worker/commands", headers=headers).json()["commands"]
    assert delivered == []
    for command_id in (bad_type["command_id"], bad_payload["command_id"]):
        row = repositories.get_command(command_id, settings=center_env)
        assert row["acknowledged_at"] is not None
        assert "outcome" in (row["result"] or "")


def test_commands_survive_center_restart(client, center_env):
    """Команда персистентна: пересоздание соединений её не теряет."""
    from backend.app.services.distributed_workers import database, repositories

    worker_id, headers = _approved_worker(client)
    command = repositories.enqueue_command(
        worker_id=worker_id, command_type="delete_attempt_data",
        payload={"job_id": "j", "attempt_id": "a"},
        idempotency_key="c-4", job_id="j", attempt_id="a", settings=center_env,
    )
    database.reset_state_for_tests()          # «перезапуск центра»
    again = repositories.get_command(command["command_id"], settings=center_env)
    assert again is not None and again["acknowledged_at"] is None


def test_cancel_ack_moves_attempt_to_cancelled_only_on_proof(client, center_env):
    """`cancelled` появляется только когда воркер доказал: исполнять нечего."""
    from backend.app.services.distributed_workers import repositories

    worker_id, headers = _approved_worker(client)
    job_id, attempt_id, _ = _running_attempt(client, worker_id, headers)
    body = client.post(
        f"/api/workers/jobs/{job_id}/attempts/{attempt_id}/cancel",
        json={"reason": "стоп", "confirmation": "ОТМЕНИТЬ"},
        headers={**INTENT, "Idempotency-Key": "c1"},
    ).json()
    url = f"/api/v1/worker/commands/{body['command_id']}/ack"

    # Неоднозначный ответ состояние не меняет.
    client.post(url, json={"result": {"status": "error",
                                      "detail": {"outcome": "ownership_mismatch"}},
                           "acknowledged_at": time.time()}, headers=headers)
    assert repositories.get_attempt(
        attempt_id, settings=center_env)["state"] == "cancel_requested"

    # Новая команда с доказательством — переводит.
    second = client.post(
        f"/api/workers/jobs/{job_id}/attempts/{attempt_id}/cancel",
        json={"reason": "ещё раз", "confirmation": "ОТМЕНИТЬ"},
        headers={**INTENT, "Idempotency-Key": "c2"},
    ).json()
    client.post(
        f"/api/v1/worker/commands/{second['command_id']}/ack",
        json={"result": {"status": "ok", "detail": {"outcome": "cancelled"}},
              "acknowledged_at": time.time()},
        headers=headers,
    )
    assert repositories.get_attempt(
        attempt_id, settings=center_env)["state"] == "cancelled"


# ═══ §18.7 Идентификаторы проектов ═══════════════════════════════════════════
REAL_PROJECT_CODES = [
    "13АВ/РД-АР3-К7",
    "АР — 001 план потолка",
    "ЖК «Событие 6.2» / корпус 3",
    "проект с пробелами   и   табами",
    'кавычки "двойные" и \'одинарные\'',
    "<script>alert(1)</script>",
    "../../etc/passwd",
    "Ё" * 199,
]


@pytest.mark.parametrize("code", REAL_PROJECT_CODES)
def test_external_project_codes_are_accepted_and_never_hit_paths(client, center_env, code):
    from backend.app.services.distributed_workers import repositories

    worker_id, headers = _approved_worker(client)
    job = _create_job(client, worker_id, project=code)
    row = repositories.get_logical_job(job["job_id"], settings=center_env)
    assert row["project_external_id"] == code.strip()

    # Пакет лежит строго по UUID: имени проекта в пути нет.
    package_dir = center_env.source_packages_dir / job["job_id"] / job["attempt_id"]
    assert package_dir.is_dir()
    assert uuid.UUID(job["job_id"]) and uuid.UUID(job["attempt_id"])
    for part in package_dir.relative_to(center_env.source_packages_dir).parts:
        assert uuid.UUID(part)
    # И нигде под корнем данных не появилось каталога с внешним кодом.
    leaked = [p for p in center_env.data_dir.rglob("*") if code[:12] in p.name]
    assert leaked == []


@pytest.mark.parametrize("bad", ["", "  ", "\x00нуль", "упр\x07символ", "Ё" * 500])
def test_bad_project_codes_are_rejected(client, center_env, bad):
    worker_id, headers = _approved_worker(client)
    response = client.post(
        "/api/workers/jobs",
        json={"worker_id": worker_id, "project_id": bad,
              "params": {"label": "x", "steps": 1}},
    )
    assert response.status_code == 422, response.text


def test_unicode_normalization_makes_one_project(center_env):
    """«й» в NFC и NFD — один и тот же проект, а не два разных."""
    import unicodedata

    from backend.app.services.distributed_workers import identifiers

    nfc = "Зданий"
    nfd = unicodedata.normalize("NFD", nfc)
    assert nfc != nfd
    assert identifiers.normalize_external_id(nfd) == nfc


def test_storage_keys_must_be_uuid():
    from backend.app.services.distributed_workers import identifiers
    from audit_worker import paths

    for bad in ("../../etc", "13АВ/РД", "job_1", "", "..", "a" * 40):
        with pytest.raises(identifiers.UnsafeIdentifier):
            identifiers.require_storage_key(bad, field="job_id")
        with pytest.raises(paths.UnsafeStorageKey):
            paths.require_storage_key(bad, field="job_id", allow_legacy=False)
    # Послабление для каталогов этапа 0 НЕ открывает обход: «/», пробелы,
    # «..» и пустая строка отвергаются при любом значении флага.
    for never in ("../../etc", "13АВ/РД", "", "..", "с пробелом", "a" * 80):
        with pytest.raises(identifiers.UnsafeIdentifier):
            identifiers.require_storage_key(never, field="job_id", allow_legacy=True)
        with pytest.raises(paths.UnsafeStorageKey):
            paths.require_storage_key(never, field="job_id", allow_legacy=True)
    # А вот ключ этапа 0 при чтении проходит — иначе результат мигрированного
    # задания стал бы недоступен.
    assert identifiers.require_storage_key(
        "att_1a2b3c4d", field="attempt_id", allow_legacy=True) == "att_1a2b3c4d"
    good = str(uuid.uuid4())
    assert identifiers.require_storage_key(good, field="job_id") == good
    assert paths.require_storage_key(good, field="job_id") == good


def test_download_filename_is_sanitized():
    from backend.app.services.distributed_workers import identifiers

    name = identifiers.safe_download_filename(
        "13АВ/РД-АР3-К7 ../../secret", fallback="attempt", suffix=".tar.gz")
    assert "/" not in name and "\\" not in name
    assert ".." not in name
    assert name.endswith(".tar.gz")


def test_no_module_builds_path_from_project_id():
    """Грепом: никто не склеивает путь из внешнего кода проекта (I-11)."""
    # Ищем склейку пути: «что-то / project_id» или Path(project_id).
    suspicious = re.compile(
        r"(?:/\s*(?:job\[)?[\"']?project_(?:id|external_id|display_name)"
        r"|Path\(\s*[^)\n]*project_(?:id|external_id|display_name)"
        r"|joinpath\(\s*[^)\n]*project_(?:id|external_id|display_name))"
    )
    offenders = []
    files = [
        *(_ROOT / "backend/app/services/distributed_workers").rglob("*.py"),
        *(_ROOT / "audit_worker").rglob("*.py"),
        _ROOT / "backend/app/api/routers/audit_worker_agent.py",
        _ROOT / "backend/app/api/routers/audit_workers_admin.py",
    ]
    if True:
        for path in files:
            for number, line in enumerate(
                path.read_text(encoding="utf-8").splitlines(), start=1
            ):
                if suspicious.search(line):
                    offenders.append(f"{path.name}:{number}: {line.strip()}")
    assert not offenders, offenders


# ═══ §18.8 RetentionManager ══════════════════════════════════════════════════
@pytest.fixture()
def worker_env(tmp_path):
    """Локальное окружение воркера без центра."""
    from audit_worker.config import WorkerConfig
    from audit_worker.local_db import LocalDB

    config = WorkerConfig(
        dispatcher_url="http://center",
        root=tmp_path / "worker",
        display_name="VPS-ret",
    )
    config.ensure_dirs()
    return config, LocalDB(config.local_db_path)


def _finished_attempt(config, *, retention_until, result_hash="deadbeef"):
    """Создать на диске завершённую попытку с результатом."""
    from audit_worker.local_store import LocalJobStore

    job_id, attempt_id = str(uuid.uuid4()), str(uuid.uuid4())
    store = LocalJobStore(config.jobs_dir)
    job_dir = store.job_dir(job_id, attempt_id)
    (job_dir / "result").mkdir(parents=True, exist_ok=True)
    (job_dir / "events").mkdir(parents=True, exist_ok=True)
    (job_dir / "result" / "summary.json").write_text("{}", encoding="utf-8")
    store.save({
        "job_id": job_id, "attempt_id": attempt_id, "local_state": "finished",
        "result_hash": result_hash, "result_size": 2,
        "retention_until": retention_until,
    })
    return job_id, attempt_id


def test_retention_dry_run_is_default(worker_env):
    config, db = worker_env
    from audit_worker.retention import RetentionManager

    assert config.retention_delete_enabled is False
    job_id, attempt_id = _finished_attempt(config, retention_until=time.time() - 10)
    manager = RetentionManager(config, db)
    report = manager.sweep()

    assert report["dry_run"] is True
    assert report["candidate_count"] == 1
    assert report["reclaimable_bytes"] > 0
    # Ничего не удалено.
    assert config.job_dir(job_id, attempt_id).is_dir()


def test_retention_never_deletes_unconfirmed_result(worker_env):
    config, db = worker_env
    from audit_worker.retention import RetentionManager

    config.retention_delete_enabled = True
    job_id, attempt_id = _finished_attempt(config, retention_until=None)
    manager = RetentionManager(config, db)

    assert manager.candidates() == []
    outcome = manager.delete_attempt(job_id=job_id, attempt_id=attempt_id, manual=True)
    assert outcome["status"] == "error"
    assert "retention_unconfirmed" in outcome["detail"]["reason"]
    assert config.job_dir(job_id, attempt_id).is_dir()


def test_retention_refuses_active_attempt_and_pending_outbox(worker_env):
    config, db = worker_env
    from audit_worker.event_outbox import EventOutbox
    from audit_worker.retention import RetentionManager

    config.retention_delete_enabled = True
    manager = RetentionManager(config, db)

    # Активная в очереди.
    running_job, running_attempt = _finished_attempt(
        config, retention_until=time.time() - 10)
    db.enqueue(job_id=running_job, attempt_id=running_attempt,
               job_type="test_pipeline_v1", params={})
    allowed, reason = manager.deletion_allowed(running_job, running_attempt)
    assert not allowed and "в работе" in reason

    # Неотправленные события.
    outbox_job, outbox_attempt = _finished_attempt(
        config, retention_until=time.time() - 10)
    outbox = EventOutbox(config.job_dir(outbox_job, outbox_attempt) / "events")
    outbox.append("job_completed", {})
    allowed, reason = manager.deletion_allowed(outbox_job, outbox_attempt)
    assert not allowed and "outbox" in reason


def test_retention_physical_delete_with_tombstone(worker_env):
    config, db = worker_env
    from audit_worker.retention import RetentionManager

    config.retention_delete_enabled = True
    job_id, attempt_id = _finished_attempt(config, retention_until=time.time() - 10)
    manager = RetentionManager(config, db)

    outcome = manager.delete_attempt(job_id=job_id, attempt_id=attempt_id)
    assert outcome["detail"]["outcome"] == "deleted"
    assert not config.job_dir(job_id, attempt_id).exists()

    # Запись о попытке и hash сохранены.
    tombstones = manager.tombstones()
    assert len(tombstones) == 1
    assert tombstones[0]["result_hash"] == "deadbeef"
    # Повтор безопасен.
    repeat = manager.delete_attempt(job_id=job_id, attempt_id=attempt_id)
    assert repeat["detail"]["outcome"] == "already_deleted"


def test_retention_symlink_cannot_escape_data_root(worker_env, tmp_path):
    """Симлинк наружу удаляется как ссылка, цель остаётся нетронутой."""
    config, db = worker_env
    from audit_worker.retention import RetentionManager

    config.retention_delete_enabled = True
    outside = tmp_path / "чужие-данные"
    outside.mkdir()
    (outside / "важное.txt").write_text("не трогать", encoding="utf-8")

    job_id, attempt_id = _finished_attempt(config, retention_until=time.time() - 10)
    target = config.job_dir(job_id, attempt_id)
    import shutil

    shutil.rmtree(target)
    target.symlink_to(outside, target_is_directory=True)

    RetentionManager(config, db)._safe_remove(
        target, job_id=job_id, attempt_id=attempt_id)
    assert not target.exists()
    assert (outside / "важное.txt").is_file(), "удаление ушло за пределы каталога данных"


def test_disk_snapshot_levels(worker_env):
    config, db = worker_env
    from audit_worker.retention import RetentionManager

    manager = RetentionManager(config, db)
    _finished_attempt(config, retention_until=None)
    snapshot = manager.disk_snapshot()
    assert snapshot["level"] in ("ok", "warning", "critical")
    assert snapshot["unconfirmed_results_bytes"] > 0

    config.disk_critical_free_bytes = 10 ** 18      # заведомо больше свободного
    assert manager.disk_snapshot()["level"] == "critical"


def test_disk_critical_blocks_new_jobs_but_not_running(client, center_env):
    from backend.app.services.distributed_workers import repositories, worker_registry

    worker_id, headers = _approved_worker(client)
    job_id, attempt_id, _ = _running_attempt(client, worker_id, headers)
    client.post(
        "/api/v1/worker/heartbeat",
        json={
            "instance_id": "inst_step35_0001", "sent_at": time.time(),
            "disk": {"level": "critical", "free_bytes": 1000.0},
        },
        headers=headers,
    )
    row = repositories.get_worker(worker_id, settings=center_env)
    ok, why = worker_registry.can_receive_jobs(row)
    assert not ok and "диск" in why.lower()
    # Текущая попытка продолжает жить.
    assert repositories.get_attempt(attempt_id, settings=center_env)["state"] == "running"
    # Новую работу воркер не получит.
    assert client.post("/api/v1/worker/jobs/next", json={"free_slots": 1, "wait_sec": 0},
                       headers=headers).status_code == 409


def test_manual_deletion_request_requires_acknowledged_result(client, center_env):
    worker_id, headers = _approved_worker(client)
    job_id, attempt_id, _ = _running_attempt(client, worker_id, headers)
    response = client.post(
        f"/api/workers/jobs/{job_id}/attempts/{attempt_id}/request-deletion",
        json={"reason": "почистить", "confirmation": "УДАЛИТЬ ДАННЫЕ"},
        headers={**INTENT, "Idempotency-Key": "del-1"},
    )
    assert response.status_code == 409
    assert "Активную попытку" in response.json()["detail"]


# ═══ §18.9 Безопасность интерфейса ═══════════════════════════════════════════
XSS = "<img src=x onerror=alert(1)>"


def test_worker_snapshot_strips_html_and_non_numbers(client, center_env):
    """Отравленный снимок ресурсов не доезжает до экрана сырым."""
    worker_id, headers = _approved_worker(client)
    client.post(
        "/api/v1/worker/heartbeat",
        json={
            "instance_id": "inst_step35_0001", "sent_at": time.time(),
            "resource_snapshot": {
                "at": time.time(),
                "ram": {"total_gb": XSS, "available_gb": 4.5},
                "cpu": {"cores": "<script>1</script>"},
                "disk": {"free_gb": 10.0},
                "slots": {"binding_constraint": XSS + "x" * 500},
            },
            "warnings": [{"code": XSS, "severity": "warn", "message": XSS * 50}],
            "executor": {"status": XSS, "executor_instance_id": XSS,
                         "running_processes": "много"},
            "disk": {"level": XSS, "free_bytes": "мало"},
        },
        headers=headers,
    )
    shown = client.get("/api/workers").json()["workers"][0]
    snapshot = shown["resource_snapshot"]
    assert "total_gb" not in snapshot["ram"]         # не число — выброшено
    assert snapshot["ram"]["available_gb"] == 4.5
    assert snapshot["cpu"] == {}
    assert len(snapshot["slots"]["binding_constraint"]) <= 200
    assert shown["executor"]["status"] == "unknown"  # не из закрытого набора
    assert shown["executor"]["running_processes"] == 0
    assert shown["disk"]["level"] == "unknown"
    assert shown["disk"]["free_bytes"] is None
    for warning in shown["warnings"]:
        assert len(warning["message"]) <= 300
        assert len(warning["code"]) <= 64


def test_operator_text_fields_are_stored_verbatim_not_executed(client, center_env):
    """Причина оператора хранится как ТЕКСТ и уходит в JSON, а не в разметку."""
    worker_id, headers = _approved_worker(client)
    job_id, attempt_id, _ = _running_attempt(client, worker_id, headers)
    client.post(
        f"/api/workers/jobs/{job_id}/attempts/{attempt_id}/mark-lost",
        json={"mandatory_reason": XSS, "typed_confirmation": "ПОПЫТКА ПОТЕРЯНА",
              "observed_worker_state": XSS, "optional_operator_note": XSS},
        headers={**INTENT, "Idempotency-Key": "xss"},
    )
    actions = client.get(f"/api/workers/admin-actions?job_id={job_id}").json()["actions"]
    entry = next(a for a in actions if a["action_type"] == "mark_attempt_lost")
    assert entry["reason"] == XSS          # хранится дословно
    attempts = client.get(f"/api/workers/jobs/{job_id}/attempts").json()["attempts"]
    assert attempts[0]["operator_actions"][0]["reason"] == XSS


def test_frontend_builds_dom_without_innerhtml():
    """Экран не собирает разметку склейкой строк: innerHTML в скрипте нет."""
    source = (_ROOT / "frontend/static/js/audit-workers.js").read_text(encoding="utf-8")
    assert "innerHTML" not in source, "данные воркера обязаны идти через textContent"
    assert "outerHTML" not in source
    assert "insertAdjacentHTML" not in source
    # Все четыре опасных действия требуют подтверждающую фразу.
    for phrase in ("ОТМЕНИТЬ", "ПОПЫТКА ПОТЕРЯНА", "НОВАЯ ПОПЫТКА", "УДАЛИТЬ ДАННЫЕ"):
        assert phrase in source
    # И честный текст про офлайн-VPS.
    assert "Мгновенная остановка НЕ гарантируется" in source
    assert "может продолжать работу" in source
    # Опасные вызовы уходят с заголовком намерения.
    assert "'X-Requested-With': 'audit-workers'" in source
    assert "Idempotency-Key" in source


def test_frontend_marks_superseded_result_explicitly():
    source = (_ROOT / "frontend/static/js/audit-workers.js").read_text(encoding="utf-8")
    assert "Не является актуальным результатом задания" in source
    assert "Результат устаревшей попытки" in source
