"""
test_distributed_workers_e2e.py
-------------------------------
Сквозной вертикальный срез этапа 0: НАСТОЯЩИЙ агент против НАСТОЯЩЕГО
FastAPI-приложения (через httpx ASGITransport — без сокетов и портов).

Цепочка: регистрация → одобрение → heartbeat → ручная выдача тестового
задания → скачивание пакета → проверка sha256 и манифеста → безопасная
распаковка → фиксированный тестовый процесс → события и логи → сборка
результата → чанкованная загрузка → четыре проверки → скачивание результата.

Реального аудита, Claude Code, Codex и норм-этапа здесь нет и быть не должно.

Run: python -m pytest tests/test_distributed_workers_e2e.py -v
"""
from __future__ import annotations

import contextlib
import json
import os
import sys
import threading
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

httpx = pytest.importorskip("httpx")


BOOTSTRAP = "test-bootstrap-secret-0123456789abcdef"


@pytest.fixture()
def center(tmp_path, monkeypatch):
    """Поднять приложение центра с включённой подсистемой и изолированной БД."""
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "center"))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET", BOOTSTRAP)
    monkeypatch.setenv("DISTRIBUTED_WORKERS_UPLOAD_CHUNK_BYTES", "4096")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_LONG_POLL_SEC", "1")
    monkeypatch.setenv("PORTAL_AUTH_ENABLED", "false")

    from backend.app.services.distributed_workers import database
    from tests.distributed_workers_helpers import make_center_app

    database.reset_state_for_tests()
    yield make_center_app()
    database.reset_state_for_tests()


@pytest.fixture()
def transport(center):
    from tests.distributed_workers_helpers import SyncASGITransport

    return SyncASGITransport(center)


@pytest.fixture()
def admin(transport):
    return httpx.Client(transport=transport, base_url="http://center")


def _make_worker_config(tmp_path, transport):
    from audit_worker.config import WorkerConfig

    return WorkerConfig(
        dispatcher_url="http://center",
        root=tmp_path / "worker",
        display_name="VPS-e2e",
        heartbeat_interval_sec=5.0,
        poll_wait_sec=1,
        event_flush_interval_sec=0.2,
        max_slots=2,
        test_max_total_sec=60.0,
        transport=transport,
    )


@contextlib.contextmanager
def running_executor(config, *, max_jobs=1):
    """Поднять ЛОКАЛЬНОГО исполнителя рядом с агентом.

    Агент с этапа 3.5 процессы не запускает: он ставит попытку в worker.db,
    а работу делает исполнитель. Здесь он живёт в отдельном потоке того же
    процесса; настоящее разделение процессов проверяется в
    tests/test_distributed_workers_executor.py.
    """
    from audit_worker.executor import Executor

    executor = Executor(config)
    thread = threading.Thread(
        target=executor.run_forever, kwargs={"max_jobs": max_jobs}, daemon=True
    )
    thread.start()
    try:
        yield executor
    finally:
        executor.shutdown()
        thread.join(timeout=15)


def test_vertical_slice_full_cycle(tmp_path, transport, admin):
    from audit_worker.agent import WorkerAgent
    from audit_worker.registration import ensure_registered

    config = _make_worker_config(tmp_path, transport)
    config.ensure_dirs()

    # 1. Регистрация — только ЗАЯВКА. Токена ещё нет: выдаётся одноразовый
    #    claim-secret, который сработает лишь после одобрения оператором.
    identity = ensure_registered(config, bootstrap_secret=BOOTSTRAP)
    assert identity["worker_id"].startswith("wrk_")
    assert identity.get("token") is None
    assert identity["registration_status"] == "pending"
    claim_path = config.token_path.with_name("claim_secret")
    assert claim_path.is_file()
    assert oct(claim_path.stat().st_mode)[-3:] == "600"
    assert not config.token_path.exists()

    worker_id = identity["worker_id"]

    # 2. Оператор одобряет.
    response = admin.post(
        f"/api/workers/{worker_id}/approve",
        json={"display_name": "VPS-e2e", "configured_max_slots": 2},
    )
    assert response.status_code == 200, response.text
    assert response.json()["worker"]["registration_status"] == "approved"

    # 3. Только теперь воркер забирает токен — и ровно один раз.
    identity = ensure_registered(config)
    assert identity["token"].startswith("wtk_")
    assert oct(config.token_path.stat().st_mode)[-3:] == "600"
    assert not claim_path.exists()          # одноразовый секрет удалён
    agent = WorkerAgent(config, identity)

    # 4. Heartbeat проходит и приносит ресурсы.
    beat = agent.heartbeat.beat_once()
    assert beat["connection_status"] == "online"
    listing = admin.get("/api/workers").json()
    assert listing["summary"]["online"] == 1
    shown = listing["workers"][0]
    assert shown["display_name"] == "VPS-e2e"
    assert shown["resource_snapshot"]["slots"]["binding_constraint"]

    # 5. Оператор выдаёт тестовое задание вручную.
    created = admin.post(
        "/api/workers/jobs",
        json={
            "worker_id": worker_id,
            "project_id": "e2e-project",
            "params": {"label": "e2e", "steps": 3, "step_seconds": 0.02,
                       "result_bytes": 2048},
        },
    )
    assert created.status_code == 200, created.text
    job_id = created.json()["job"]["job_id"]
    assert created.json()["job"]["state"] == "assigned"
    # Токен попытки наружу оператору не отдаётся.
    assert "_execution_token_plain" not in created.json()["job"]

    # 6. Агент забирает задание, ИСПОЛНИТЕЛЬ его выполняет, агент передаёт.
    agent._start_sender()
    try:
        with running_executor(config, max_jobs=1):
            assignment = agent.poller.poll(free_slots=2, compressions=["gzip"])
            assert assignment is not None
            assert assignment["job_type"] == "test_pipeline_v1"
            assert assignment["package"]["sha256"]
            outcome = agent.execute_job(assignment)
    finally:
        agent.shutdown()
    assert outcome["ok"], outcome
    # Процесс аудита запускал ИСПОЛНИТЕЛЬ: запись о нём есть в его реестре.
    process = agent.db.process_row(assignment["attempt_id"])
    assert process is not None and process["executor_instance_id"]

    # 7. Центр: задание принято и проверено.
    final = admin.get(f"/api/workers/jobs/{job_id}").json()
    assert final["job"]["state"] == "completed"
    assert final["job"]["display_status"] == "Результат принят и проверен"
    assert final["job"]["validated_at"] is not None
    assert final["job"]["retention_until"] is not None
    # Результат подтверждён → признак «не подтверждён» снят.
    assert final["job"]["retention_unconfirmed"] is False

    # 8. Журнал переходов содержит полный путь без пропусков.
    states = [t["to_state"] for t in final["transitions"]]
    assert states == [
        "created", "assigned", "source_uploading", "source_ready",
        "accepted_by_worker", "running", "completed_locally",
        "result_uploading", "result_received", "validating", "completed",
    ]

    # 9. События дошли, порядок сохранён, дыр нет.
    events = admin.get(f"/api/workers/jobs/{job_id}/events").json()["events"]
    seqs = [e["sequence"] for e in events]
    # ORDER BY sequence гарантирует сортировку сам по себе, поэтому проверяем
    # то, что зависит от кода: дублей нет, а дыры в таблице объясняются ровно
    # строками лога (они по проекту уходят в файл, а не в таблицу).
    assert len(seqs) == len(set(seqs)), "дубли событий"
    log_seqs = [
        l["seq"] for l in admin.get(f"/api/workers/jobs/{job_id}/logs").json()["lines"]
    ]
    assert not set(seqs) & set(log_seqs), "событие попало и в таблицу, и в файл"
    united = sorted(set(seqs) | set(log_seqs))
    assert united == list(range(1, len(united) + 1)), (
        f"дыра в последовательности: {united[:20]}"
    )
    types = {e["event_type"] for e in events}
    assert {"source_verified", "job_accepted", "job_started", "stage_progress",
            "stage_completed", "artifact_created", "job_completed_locally",
            "result_upload_started", "job_completed"} <= types
    # Артефакты объявлены поимённо, с размером и хэшем.
    artifacts = {e["payload"]["path_rel"] for e in events
                 if e["event_type"] == "artifact_created"}
    assert {"result/summary.json", "result/run_log.txt"} <= artifacts
    # log_line в таблицу не попадают — они в файле.
    assert "log_line" not in types

    # 10. Полные stdout-логи доступны отдельно.
    logs = admin.get(f"/api/workers/jobs/{job_id}/logs").json()["lines"]
    assert any("test_pipeline_v1" in line["message"] for line in logs)
    assert all("seq" in line for line in logs)

    # 11. Прогресс честный: процент только при достоверном total.
    progress = final["job"]["progress"] if "progress" in final["job"] else None
    listed = admin.get("/api/workers/jobs/list").json()["jobs"]
    prog = next(j["progress"] for j in listed if j["job_id"] == job_id)
    assert prog["percent_reliable"] is True
    assert prog["processed"] == 3 and prog["total"] == 3
    assert prog["indeterminate"] is False

    # 12. Результат скачивается и содержит обязательные артефакты.
    downloaded = admin.get(f"/api/workers/jobs/{job_id}/result")
    assert downloaded.status_code == 200
    assert len(downloaded.content) > 0

    result_path = tmp_path / "downloaded.tar.gz"
    result_path.write_bytes(downloaded.content)
    import tarfile

    with tarfile.open(result_path, "r:gz") as tar:
        names = tar.getnames()
    assert "package_manifest.json" in names
    assert "payload/result/summary.json" in names
    assert "payload/result/run_log.txt" in names


def test_offline_run_then_late_delivery(tmp_path, transport, admin):
    """Аудит завершается без связи → события копятся → досылаются после."""
    from audit_worker.agent import WorkerAgent
    from audit_worker.registration import ensure_registered

    config = _make_worker_config(tmp_path, transport)
    config.ensure_dirs()
    identity = ensure_registered(config, bootstrap_secret=BOOTSTRAP)
    admin.post(
        f"/api/workers/{identity['worker_id']}/approve",
        json={"configured_max_slots": 1},
    )
    identity = ensure_registered(config)      # claim после одобрения
    agent = WorkerAgent(config, identity)
    admin.post(
        "/api/workers/jobs",
        json={
            "worker_id": identity["worker_id"],
            "project_id": "offline-project",
            "params": {"label": "offline", "steps": 2, "step_seconds": 0.01},
        },
    )
    assignment = agent.poller.poll(free_slots=1, compressions=["gzip"])
    assert assignment is not None

    # Рвём связь: любой POST /events падает, но конвейер обязан доработать.
    original_post_events = agent.client.post_events
    calls = {"n": 0}

    def broken_post_events(*args, **kwargs):
        calls["n"] += 1
        raise httpx.ConnectError("сеть недоступна")

    agent.client.post_events = broken_post_events  # type: ignore[assignment]
    job_id = assignment["job_id"]
    attempt_id = assignment["attempt_id"]

    agent._download_and_verify(assignment, {
        "job_id": job_id, "attempt_id": attempt_id,
        "execution_token": assignment["execution_token"],
        "outbox": __import__("audit_worker.event_outbox", fromlist=["EventOutbox"])
        .EventOutbox(config.job_dir(job_id, attempt_id) / "events"),
        "stage": "download",
    }, config.job_dir(job_id, attempt_id))

    # Прогон задания при мёртвой сети событий.
    ctx_outbox = __import__("audit_worker.event_outbox", fromlist=["EventOutbox"]).EventOutbox(
        config.job_dir(job_id, attempt_id) / "events"
    )
    ctx = {
        "job_id": job_id, "attempt_id": attempt_id,
        "execution_token": assignment["execution_token"],
        "outbox": ctx_outbox, "stage": "run",
    }
    agent._accept(assignment, ctx)          # accept идёт отдельным вызовом (он ОК)
    with running_executor(config, max_jobs=1):
        result = agent._dispatch_and_wait(assignment, ctx)
    assert result["ok"], result
    assert calls["n"] > 0, "должны были быть неудачные попытки отправки"

    # События целы на диске и ждут отправки.
    ctx_outbox.reload()
    assert ctx_outbox.has_pending
    pending_before = ctx_outbox.last_written_seq - ctx_outbox.last_acked_seq
    assert pending_before >= 4

    # Связь вернулась — досылаем накопленное.
    agent.client.post_events = original_post_events  # type: ignore[assignment]
    agent._flush_outbox(ctx)
    assert not ctx_outbox.has_pending

    events = admin.get(f"/api/workers/jobs/{job_id}/events").json()["events"]
    seqs = [e["sequence"] for e in events]
    # ORDER BY sequence гарантирует сортировку сам по себе, поэтому проверяем
    # то, что зависит от кода: дублей нет, а дыры в таблице объясняются ровно
    # строками лога (они по проекту уходят в файл, а не в таблицу).
    assert len(seqs) == len(set(seqs)), "дубли событий"
    log_seqs = [
        l["seq"] for l in admin.get(f"/api/workers/jobs/{job_id}/logs").json()["lines"]
    ]
    assert not set(seqs) & set(log_seqs), "событие попало и в таблицу, и в файл"
    united = sorted(set(seqs) | set(log_seqs))
    assert united == list(range(1, len(united) + 1)), (
        f"дыра в последовательности: {united[:20]}"
    )
    agent.shutdown()


def test_worker_api_requires_token(transport):
    """Контуры аутентификации разделены: без bearer-токена воркерский API закрыт."""
    client = httpx.Client(transport=transport, base_url="http://center")
    response = client.post(
        "/api/v1/worker/heartbeat",
        json={"instance_id": "inst_x", "sent_at": 0.0},
    )
    assert response.status_code == 401

    # Регистрация без bootstrap-секрета тоже закрыта.
    response = client.post(
        "/api/v1/worker/register",
        json={"instance_id": "inst_x", "protocol_version": 1},
    )
    assert response.status_code == 401
