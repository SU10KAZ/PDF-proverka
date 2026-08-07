"""
test_distributed_workers_hardening.py
-------------------------------------
Тесты на пробелы, найденные повторной сверкой этапа 0 и закрытые отдельно:

  * двухэтапная выдача токена (register → approve → claim), а не токен сразу;
  * Idempotency-Key с проверкой хэша тела (повтор с другим телом → 409);
  * запрет HTTP на внешний хост и отсутствие глобального verify=false;
  * защита TAR от дублей путей, «бомбы» по степени сжатия, подмены файла
    при совпавшем хэше архива, отсутствия обязательных файлов;
  * порядок «файл на диск → отметка в БД» при приёме чанка;
  * раздельные stdout/stderr;
  * состав результирующего пакета: input/ + work/ + result/;
  * command fingerprint и completed.marker.

Run: python -m pytest tests/test_distributed_workers_hardening.py -v
"""
from __future__ import annotations

import io
import json
import os
import sys
import tarfile
import time
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

httpx = pytest.importorskip("httpx")

BOOTSTRAP = "test-bootstrap-secret-0123456789abcdef"


@pytest.fixture()
def center_env(tmp_path, monkeypatch):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "center"))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET", BOOTSTRAP)
    monkeypatch.setenv("DISTRIBUTED_WORKERS_UPLOAD_CHUNK_BYTES", "4096")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_LONG_POLL_SEC", "1")

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
        transport=SyncASGITransport(make_center_app()), base_url="http://center"
    )


def _register(client, instance_id="inst_hardening01"):
    return client.post(
        "/api/v1/worker/register",
        json={"instance_id": instance_id, "protocol_version": 1,
              "display_name_hint": "VPS-h"},
        headers={"Authorization": f"Bearer {BOOTSTRAP}", "X-Protocol-Version": "1"},
    )


# ─── §4 Двухэтапная выдача токена ────────────────────────────────────────────
def test_register_does_not_issue_token(client):
    """На регистрации выдаётся claim-secret, а НЕ токен доступа."""
    response = _register(client)
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["registration_status"] == "pending"
    assert body["claim_secret"].startswith("clm_")
    assert "worker_token" not in body


def test_claim_before_approval_is_rejected(client):
    body = _register(client).json()
    response = client.post(
        "/api/v1/worker/claim",
        json={"worker_id": body["worker_id"], "instance_id": "inst_hardening01",
              "claim_secret": body["claim_secret"]},
    )
    assert response.status_code == 409
    assert "не одобрена" in response.json()["detail"]


def test_claim_is_single_use(client):
    body = _register(client).json()
    worker_id = body["worker_id"]
    client.post(f"/api/workers/{worker_id}/approve", json={"configured_max_slots": 1})

    first = client.post(
        "/api/v1/worker/claim",
        json={"worker_id": worker_id, "instance_id": "inst_hardening01",
              "claim_secret": body["claim_secret"]},
    )
    assert first.status_code == 200
    assert first.json()["worker_token"].startswith("wtk_")

    second = client.post(
        "/api/v1/worker/claim",
        json={"worker_id": worker_id, "instance_id": "inst_hardening01",
              "claim_secret": body["claim_secret"]},
    )
    assert second.status_code == 409
    assert "уже использован" in second.json()["detail"]


def test_wrong_claim_secret_rejected(client):
    body = _register(client).json()
    client.post(f"/api/workers/{body['worker_id']}/approve", json={"configured_max_slots": 1})
    response = client.post(
        "/api/v1/worker/claim",
        json={"worker_id": body["worker_id"], "instance_id": "inst_hardening01",
              "claim_secret": "clm_" + "z" * 40},
    )
    assert response.status_code == 409


def test_rejected_worker_cannot_claim(client):
    body = _register(client).json()
    assert client.post(f"/api/workers/{body['worker_id']}/reject").status_code == 200
    response = client.post(
        "/api/v1/worker/claim",
        json={"worker_id": body["worker_id"], "instance_id": "inst_hardening01",
              "claim_secret": body["claim_secret"]},
    )
    assert response.status_code == 409
    assert "отклонена" in response.json()["detail"]


def test_token_is_not_recoverable_from_db(client, center_env):
    """Потерянный токен нельзя достать обратно: в БД только sha256."""
    from backend.app.services.distributed_workers import database

    body = _register(client).json()
    worker_id = body["worker_id"]
    client.post(f"/api/workers/{worker_id}/approve", json={"configured_max_slots": 1})
    token = client.post(
        "/api/v1/worker/claim",
        json={"worker_id": worker_id, "instance_id": "inst_hardening01",
              "claim_secret": body["claim_secret"]},
    ).json()["worker_token"]

    with database.read_conn(center_env) as conn:
        dump = "".join(
            json.dumps(dict(row), default=str)
            for table in ("workers", "worker_tokens")
            for row in conn.execute(f"SELECT * FROM {table}")
        )
    assert token not in dump
    assert body["claim_secret"] not in dump
    # Хэш claim-secret после использования стирается.
    with database.read_conn(center_env) as conn:
        row = conn.execute(
            "SELECT claim_secret_sha256, claim_used_at FROM workers WHERE worker_id = ?",
            (worker_id,),
        ).fetchone()
    assert row["claim_secret_sha256"] is None
    assert row["claim_used_at"] is not None


def test_revoked_token_is_refused(client):
    body = _register(client).json()
    worker_id = body["worker_id"]
    client.post(f"/api/workers/{worker_id}/approve", json={"configured_max_slots": 1})
    token = client.post(
        "/api/v1/worker/claim",
        json={"worker_id": worker_id, "instance_id": "inst_hardening01",
              "claim_secret": body["claim_secret"]},
    ).json()["worker_token"]

    headers = {"Authorization": f"Bearer {token}", "X-Worker-Id": worker_id,
               "X-Protocol-Version": "1"}
    ok = client.post("/api/v1/worker/heartbeat",
                     json={"instance_id": "inst_hardening01", "sent_at": time.time()},
                     headers=headers)
    assert ok.status_code == 200

    client.post(f"/api/workers/{worker_id}/revoke")
    after = client.post("/api/v1/worker/heartbeat",
                        json={"instance_id": "inst_hardening01", "sent_at": time.time()},
                        headers=headers)
    assert after.status_code == 401


def test_repeat_registration_does_not_multiply_workers(client, center_env):
    from backend.app.services.distributed_workers import repositories

    for _ in range(3):
        _register(client)
    workers = repositories.list_workers(settings=center_env)
    assert len(workers) == 1


# ─── §5 Idempotency-Key ──────────────────────────────────────────────────────
def _approved_worker(client):
    body = _register(client).json()
    worker_id = body["worker_id"]
    client.post(f"/api/workers/{worker_id}/approve", json={"configured_max_slots": 2})
    token = client.post(
        "/api/v1/worker/claim",
        json={"worker_id": worker_id, "instance_id": "inst_hardening01",
              "claim_secret": body["claim_secret"]},
    ).json()["worker_token"]
    headers = {"Authorization": f"Bearer {token}", "X-Worker-Id": worker_id,
               "X-Protocol-Version": "1"}
    client.post("/api/v1/worker/heartbeat",
                json={"instance_id": "inst_hardening01", "sent_at": time.time(),
                      "calculated_free_slots": 2, "configured_max_slots": 2},
                headers=headers)
    return worker_id, headers


def test_idempotency_key_same_body_replays(client):
    worker_id, headers = _approved_worker(client)
    client.post("/api/workers/jobs",
                json={"worker_id": worker_id, "project_id": "idem-1",
                      "params": {"steps": 1, "step_seconds": 0.0}})
    body = {"free_slots": 1, "accepts": {}, "wait_sec": 0}
    key = {"Idempotency-Key": "abc-123"}

    first = client.post("/api/v1/worker/jobs/next", json=body, headers={**headers, **key})
    assert first.status_code == 200
    second = client.post("/api/v1/worker/jobs/next", json=body, headers={**headers, **key})
    assert second.status_code == 200
    # Повтор вернул ТО ЖЕ задание, а не второе.
    assert second.json()["job_id"] == first.json()["job_id"]
    assert second.json()["attempt_id"] == first.json()["attempt_id"]


def test_idempotency_key_different_body_conflicts(client):
    worker_id, headers = _approved_worker(client)
    client.post("/api/workers/jobs",
                json={"worker_id": worker_id, "project_id": "idem-2",
                      "params": {"steps": 1, "step_seconds": 0.0}})
    assignment = client.post(
        "/api/v1/worker/jobs/next",
        json={"free_slots": 1, "accepts": {}, "wait_sec": 0}, headers=headers,
    ).json()
    job_id = assignment["job_id"]
    exec_headers = {**headers, "X-Execution-Token": assignment["execution_token"],
                    "Idempotency-Key": "accept-1"}

    payload = {"attempt_id": assignment["attempt_id"], "accepted_at": time.time(),
               "source_verified": {"sha256_ok": True, "manifest_version": 1},
               "planned_stages": ["test_pipeline_v1"]}
    first = client.post(f"/api/v1/worker/jobs/{job_id}/accept",
                        json=payload, headers=exec_headers)
    assert first.status_code == 200

    # Тот же ключ, но другое тело — это ошибка клиента, а не повтор.
    changed = {**payload, "planned_stages": ["что-то другое"]}
    conflict = client.post(f"/api/v1/worker/jobs/{job_id}/accept",
                           json=changed, headers=exec_headers)
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["error"] == "idempotency_key_reuse"


def test_foreign_worker_cannot_touch_job(client):
    """Чужой воркер не получает и не меняет задание, даже зная job_id."""
    owner_id, owner_headers = _approved_worker(client)
    client.post("/api/workers/jobs",
                json={"worker_id": owner_id, "project_id": "owned",
                      "params": {"steps": 1, "step_seconds": 0.0}})
    assignment = client.post(
        "/api/v1/worker/jobs/next",
        json={"free_slots": 1, "accepts": {}, "wait_sec": 0}, headers=owner_headers,
    ).json()

    other = _register(client, instance_id="inst_other0001").json()
    client.post(f"/api/workers/{other['worker_id']}/approve", json={"configured_max_slots": 1})
    other_token = client.post(
        "/api/v1/worker/claim",
        json={"worker_id": other["worker_id"], "instance_id": "inst_other0001",
              "claim_secret": other["claim_secret"]},
    ).json()["worker_token"]
    foreign = {"Authorization": f"Bearer {other_token}",
               "X-Worker-Id": other["worker_id"], "X-Protocol-Version": "1",
               "X-Execution-Token": assignment["execution_token"]}

    assert client.get(
        f"/api/v1/worker/jobs/{assignment['job_id']}/source", headers=foreign
    ).status_code == 403
    assert client.post(
        f"/api/v1/worker/jobs/{assignment['job_id']}/accept",
        json={"attempt_id": assignment["attempt_id"], "accepted_at": time.time(),
              "source_verified": {"sha256_ok": True, "manifest_version": 1}},
        headers=foreign,
    ).status_code == 403


# ─── §23 TLS ─────────────────────────────────────────────────────────────────
def test_no_global_verify_disable(monkeypatch, tmp_path):
    """Переменной, отключающей проверку сертификата, не существует."""
    from audit_worker import config as cfg

    monkeypatch.setenv("AUDIT_WORKER_DISPATCHER_URL", "https://center.example")
    monkeypatch.setenv("AUDIT_WORKER_VERIFY_TLS", "false")   # старая лазейка
    conf = cfg.load_config(str(tmp_path))
    assert conf.verify_tls is True


@pytest.mark.parametrize(
    "url,allow,ok",
    [
        ("https://center.example", False, True),
        ("http://localhost:8081", True, True),
        ("http://127.0.0.1:8081", True, True),
        ("http://localhost:8081", False, False),
        ("http://center.example", True, False),
        ("ftp://center.example", True, False),
    ],
)
def test_transport_security_rules(monkeypatch, tmp_path, url, allow, ok):
    from audit_worker import config as cfg

    monkeypatch.setenv("AUDIT_WORKER_DISPATCHER_URL", url)
    monkeypatch.setenv(
        "AUDIT_WORKER_ALLOW_INSECURE_LOCALHOST", "true" if allow else "false"
    )
    if ok:
        assert cfg.load_config(str(tmp_path)).dispatcher_url == url.rstrip("/")
    else:
        with pytest.raises(SystemExit):
            cfg.load_config(str(tmp_path))


# ─── §11 Защита TAR ──────────────────────────────────────────────────────────
def _tar_with(entries, path, manifest=None):
    with tarfile.open(path, "w:gz") as tar:
        data = json.dumps(manifest or {"manifest_version": 1}).encode()
        info = tarfile.TarInfo("package_manifest.json")
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
        for name, payload in entries:
            item = tarfile.TarInfo(name)
            item.size = len(payload)
            tar.addfile(item, io.BytesIO(payload))


def test_duplicate_paths_rejected(tmp_path, center_env):
    from backend.app.services.distributed_workers import package_service

    archive = tmp_path / "dup.tar.gz"
    _tar_with([("payload/a.txt", b"one"), ("payload/a.txt", b"two")], archive)
    with pytest.raises(package_service.PackageError) as info:
        package_service.safe_extract(archive, tmp_path / "out")
    assert "Повторяющийся путь" in str(info.value)


def test_compression_bomb_ratio_rejected(tmp_path, center_env):
    from backend.app.services.distributed_workers import package_service

    archive = tmp_path / "bomb.tar.gz"
    # 8 МБ нулей сжимаются в килобайты — соотношение много выше потолка.
    _tar_with([("payload/zeros.bin", b"\0" * (8 * 1024 * 1024))], archive)
    with pytest.raises(package_service.PackageError) as info:
        package_service.safe_extract(archive, tmp_path / "out")
    assert "степень сжатия" in str(info.value)


def test_worker_detects_file_hash_mismatch(tmp_path):
    """Манифест, лгущий о хэше файла, отвергается при распаковке."""
    from audit_worker import package_io

    archive = tmp_path / "lying.tar.gz"
    manifest = {
        "manifest_version": 1,
        "archive": {"uncompressed_bytes": 100, "entries": 2},
        "files": [{"path": "payload/a.txt", "sha256": "0" * 64}],
    }
    _tar_with([("payload/a.txt", b"real content")], archive, manifest)
    digest = package_io.sha256_file(archive)
    with pytest.raises(package_io.BundleError) as info:
        package_io.verify_and_unpack(
            archive=archive, expected_sha256=digest, work_dir=tmp_path / "work"
        )
    assert "SHA-256 файла" in str(info.value)
    assert not (tmp_path / "work").exists()


def test_worker_requires_required_files(tmp_path):
    from audit_worker import package_io

    archive = tmp_path / "incomplete.tar.gz"
    manifest = {
        "manifest_version": 1,
        "archive": {"uncompressed_bytes": 10, "entries": 2},
        "required_files": ["payload/job.json"],
    }
    _tar_with([("payload/other.txt", b"x")], archive, manifest)
    digest = package_io.sha256_file(archive)
    with pytest.raises(package_io.BundleError) as info:
        package_io.verify_and_unpack(
            archive=archive, expected_sha256=digest, work_dir=tmp_path / "work"
        )
    assert "обязательных файлов" in str(info.value)


def test_source_manifest_declares_required_files(center_env):
    from backend.app.models.distributed_workers import TestJobParams
    from backend.app.services.distributed_workers import (
        job_service, package_service, registration_service, repositories, worker_registry,
    )

    worker = repositories.create_worker(
        display_name="w", instance_id="inst_manifest01", worker_version="0.1.0",
        protocol_version=1, pipeline_revision=None,
        capabilities={"job_types": ["test_pipeline_v1"], "compressions": ["gzip"]},
        configured_max_slots=1, settings=center_env,
    )
    registration_service.approve_worker(
        worker_id=worker["worker_id"], display_name=None,
        configured_max_slots=1, settings=center_env)
    worker_registry.record_heartbeat(
        worker_id=worker["worker_id"], instance_id="inst_heartbeat1", worker_state="idle",
        configured_max_slots=1, calculated_free_slots=1, active_jobs=[],
        resource_snapshot={"at": time.time()}, warnings=[], settings=center_env)

    job = job_service.create_test_job(
        worker_id=worker["worker_id"], project_id="manifest-proj", version_id=None,
        params=TestJobParams(steps=1, step_seconds=0.0), actor="operator:t",
        settings=center_env)
    manifest = package_service.read_manifest(
        job_service.source_package_path(job, settings=center_env)
    )
    assert manifest["package_type"] == "source"
    assert manifest["job_type"] == "test_pipeline_v1"
    assert manifest["required_files"] == ["payload/job.json", "payload/README.txt"]
    assert all(f["path"].startswith("payload/") for f in manifest["files"])
    assert all("\\\\" not in f["path"] for f in manifest["files"])


# ─── §19 Порядок записи чанка ────────────────────────────────────────────────
def test_chunk_not_marked_received_when_write_fails(center_env, monkeypatch, tmp_path):
    """Сбой записи на диск не должен оставлять чанк «принятым»."""
    from backend.app.models.distributed_workers import TestJobParams
    from backend.app.services.distributed_workers import (
        job_service, registration_service, repositories, upload_service, worker_registry,
    )

    worker = repositories.create_worker(
        display_name="w", instance_id="inst_chunk0001", worker_version="0.1.0",
        protocol_version=1, pipeline_revision=None,
        capabilities={"job_types": ["test_pipeline_v1"]},
        configured_max_slots=1, settings=center_env)
    registration_service.approve_worker(
        worker_id=worker["worker_id"], display_name=None,
        configured_max_slots=1, settings=center_env)
    worker_registry.record_heartbeat(
        worker_id=worker["worker_id"], instance_id="inst_heartbeat1", worker_state="idle",
        configured_max_slots=1, calculated_free_slots=1, active_jobs=[],
        resource_snapshot={"at": time.time()}, warnings=[], settings=center_env)
    job = job_service.create_test_job(
        worker_id=worker["worker_id"], project_id="chunk-proj", version_id=None,
        params=TestJobParams(steps=1, step_seconds=0.0), actor="operator:t",
        settings=center_env)

    session, _ = upload_service.open_or_create_session(
        job=job, package_type="result", expected_size=4096,
        expected_hash="d" * 64, settings=center_env)

    original = Path.write_bytes

    def failing_write(self, data):
        raise OSError("диск переполнен")

    monkeypatch.setattr(Path, "write_bytes", failing_write)
    with pytest.raises(OSError):
        upload_service.store_chunk(
            session=session, idx=0, data=b"x" * 100,
            declared_sha256=None, settings=center_env)
    monkeypatch.setattr(Path, "write_bytes", original)

    # Чанк НЕ должен числиться принятым.
    assert repositories.received_chunks(session["upload_id"], settings=center_env) == []


# ─── §18 Состав результирующего пакета ───────────────────────────────────────
def test_result_package_has_all_sections(tmp_path):
    from audit_worker import package_io

    job_dir = tmp_path / "job"
    (job_dir / "work").mkdir(parents=True)
    (job_dir / "result").mkdir(parents=True)
    (job_dir / "work" / "job.json").write_text('{"job_type":"test_pipeline_v1"}')
    (job_dir / "work" / "test_params.json").write_text('{"steps":1}')
    (job_dir / "work" / "completed.marker").write_text('{"exit_code":0}')
    (job_dir / "result" / "summary.json").write_text('{"status":"ok"}')
    (job_dir / "result" / "run_log.txt").write_text("ok\n")

    manifest = package_io.build_result_package(
        dest_path=tmp_path / "res.tar.gz", job_dir=job_dir,
        job_id="job-1", attempt_id="att_1", project_id="p", version_id=None,
        worker_id="wrk_abc", worker_version="0.1.0", protocol_version=1,
        manifest_version=1, source_package_hash="sha256:" + "a" * 64, exit_code=0,
    )
    paths = {f["path"] for f in manifest["files"]}
    assert "payload/input/job.json" in paths
    assert "payload/work/test_params.json" in paths
    assert "payload/work/completed.marker" in paths
    assert "payload/result/summary.json" in paths

    assert manifest["package_type"] == "result"
    assert manifest["worker_id"] == "wrk_abc"
    assert manifest["source_package_hash"] == "a" * 64
    assert manifest["exit_code"] == 0
    assert manifest["sections"] == ["input", "work", "result"]
    assert manifest["archive"]["sha256"]


# ─── §14 Fingerprint и маркер завершения ─────────────────────────────────────
def test_command_fingerprint_distinguishes_commands(tmp_path):
    from audit_worker import test_runner

    a = test_runner.command_fingerprint(["python", "-u", "/a.py", "/p.json"])
    b = test_runner.command_fingerprint(["python", "-u", "/b.py", "/p.json"])
    assert a != b and len(a) == 32


def test_registry_rejects_foreign_pid_by_fingerprint(tmp_path):
    """Чужой процесс, занявший наш pid, не признаётся своим."""
    from audit_worker.process_registry import ProcessRegistry

    registry = ProcessRegistry(tmp_path)
    registry.register(os.getpid(), job_id="j", attempt_id="a",
                      command_fingerprint="ours")
    assert registry.alive_for_job("j", "a", command_fingerprint="ours")
    assert not registry.alive_for_job("j", "a", command_fingerprint="theirs")


def test_completed_marker_written(tmp_path):
    from audit_worker.agent import _write_completed_marker
    from audit_worker.test_runner import RunOutcome

    job_dir = tmp_path / "job"
    _write_completed_marker(
        job_dir,
        RunOutcome(exit_code=0, duration_sec=1.5, steps_done=3, steps_total=3,
                   stdout_lines=7, stderr_lines=0),
    )
    marker = json.loads((job_dir / "work" / "completed.marker").read_text())
    assert marker["exit_code"] == 0 and marker["steps_done"] == 3
    assert marker["finished_at"] > 0


# ─── §17 Redaction: расширенные правила ──────────────────────────────────────
@pytest.mark.parametrize(
    "text,leaked",
    [
        ("Cookie: session=deadbeefsecret", "deadbeefsecret"),
        ("Set-Cookie: portal=abc123; HttpOnly", "abc123"),
        ("https://admin:hunter2@center.example/x", "hunter2"),
        ("clm_oneTimeClaimSecretValue123456", "oneTimeClaimSecretValue123456"),
        ("-----BEGIN PRIVATE KEY-----\nMIIabc\n-----END PRIVATE KEY-----", "MIIabc"),
    ],
)
def test_extended_redaction(text, leaked):
    from audit_worker import redaction

    assert leaked not in redaction.redact(text)


def test_center_and_worker_redaction_identical():
    """Редакторы центра и воркера — один и тот же модуль по содержанию."""
    center = (_ROOT / "backend/app/services/distributed_workers/redaction.py").read_text()
    worker = (_ROOT / "audit_worker/redaction.py").read_text()
    assert center == worker


# ─── §11.3 Сверка после рестарта ─────────────────────────────────────────────
def _running_job(client, center_env):
    """Довести задание до состояния running и вернуть (job_id, attempt, headers)."""
    worker_id, headers = _approved_worker(client)
    client.post("/api/workers/jobs",
                json={"worker_id": worker_id, "project_id": "reconcile-proj",
                      "params": {"steps": 1, "step_seconds": 0.0}})
    assignment = client.post(
        "/api/v1/worker/jobs/next",
        json={"free_slots": 1, "accepts": {}, "wait_sec": 0}, headers=headers,
    ).json()
    exec_headers = {**headers, "X-Execution-Token": assignment["execution_token"]}
    job_id, attempt_id = assignment["job_id"], assignment["attempt_id"]
    client.post(f"/api/v1/worker/jobs/{job_id}/accept",
                json={"attempt_id": attempt_id, "accepted_at": time.time(),
                      "source_verified": {"sha256_ok": True, "manifest_version": 1}},
                headers=exec_headers)
    client.post(
        "/api/v1/worker/events",
        json={"job_id": job_id, "attempt_id": attempt_id, "first_seq": 1, "count": 1,
              "events": [{"seq": 1, "event_id": "ev-1", "occurred_at": time.time(),
                          "event_type": "job_started",
                          "payload": {"stage": "test_pipeline_v1"}}]},
        headers=exec_headers,
    )
    # Токен попытки нужен вызывающим: события и загрузки без него — 409.
    headers = {**headers, "_exec": assignment["execution_token"]}
    return job_id, attempt_id, headers


def test_reconcile_dead_process_is_not_told_to_continue(client, center_env):
    """«continue» для мёртвого процесса — ложь: продолжать нечего."""
    job_id, attempt_id, headers = _running_job(client, center_env)

    alive = client.post(
        "/api/v1/worker/reconcile",
        json={"instance_id": "inst_new00000001", "restarted_at": time.time(),
              "known_jobs": [{"job_id": job_id, "attempt_id": attempt_id,
                              "local_state": "running", "processes_alive": True}]},
        headers=headers,
    ).json()
    assert alive["jobs"][0]["action"] == "continue"

    dead = client.post(
        "/api/v1/worker/reconcile",
        json={"instance_id": "inst_new00000002", "restarted_at": time.time(),
              "known_jobs": [{"job_id": job_id, "attempt_id": attempt_id,
                              "local_state": "running", "processes_alive": False}]},
        headers=headers,
    ).json()
    assert dead["jobs"][0]["action"] == "await_operator"
    # Центр НЕ объявил провал сам (I-01/I-02): состояние осталось running.
    assert dead["jobs"][0]["center_state"] == "running"


def test_reconcile_returns_retention_after_offline_acceptance(client, center_env, tmp_path):
    """Воркер, пропустивший подтверждение приёма, узнаёт retention_until."""
    from backend.app.services.distributed_workers import job_service, repositories

    job_id, attempt_id, headers = _running_job(client, center_env)
    # Доводим задание до completed «мимо» воркера — как будто он был офлайн.
    for state, actor in (("completed_locally", "worker"), ("result_uploading", "worker"),
                         ("result_received", "worker"), ("validating", "center"),
                         ("completed", "center")):
        from backend.app.models.distributed_workers import JobState
        job_service.transition(job_id=job_id, to_state=JobState(state), actor=actor,
                               reason="тест", settings=center_env)
    repositories.update_job_fields(job_id,
                            {"validated_at": time.time(),
                             "retention_until": time.time() + 86400},
                            settings=center_env)

    verdict = client.post(
        "/api/v1/worker/reconcile",
        json={"instance_id": "inst_new00000003", "restarted_at": time.time(),
              "known_jobs": [{"job_id": job_id, "attempt_id": attempt_id,
                              "local_state": "completed_locally", "result_ready": True,
                              "retention_until": None}]},
        headers=headers,
    ).json()["jobs"][0]
    assert verdict["result_accepted"] is True
    assert verdict["retention_until"] is not None
    assert verdict["execution_token_valid"] is False   # задание терминально


def test_reconcile_reports_superseded_jobs(client, center_env):
    job_id, attempt_id, headers = _running_job(client, center_env)
    response = client.post(
        "/api/v1/worker/reconcile",
        json={"instance_id": "inst_new00000004", "restarted_at": time.time(),
              "known_jobs": [{"job_id": job_id, "attempt_id": "att_старая",
                              "local_state": "running", "processes_alive": True}]},
        headers=headers,
    ).json()
    assert response["jobs"][0]["action"] == "stop_superseded"
    assert response["superseded_jobs"] == [job_id]
    assert response["jobs"][0]["execution_token_valid"] is False


def test_reconcile_ignores_foreign_job(client, center_env):
    job_id, attempt_id, headers = _running_job(client, center_env)
    other = _register(client, instance_id="inst_other0002").json()
    client.post(f"/api/workers/{other['worker_id']}/approve", json={"configured_max_slots": 1})
    token = client.post(
        "/api/v1/worker/claim",
        json={"worker_id": other["worker_id"], "instance_id": "inst_other0002",
              "claim_secret": other["claim_secret"]},
    ).json()["worker_token"]
    response = client.post(
        "/api/v1/worker/reconcile",
        json={"instance_id": "inst_other0002", "restarted_at": time.time(),
              "known_jobs": [{"job_id": job_id, "attempt_id": attempt_id,
                              "local_state": "running"}]},
        headers={"Authorization": f"Bearer {token}",
                 "X-Worker-Id": other["worker_id"], "X-Protocol-Version": "1"},
    ).json()
    assert response["unknown_jobs"] == [job_id]
    assert response["jobs"] == []


def test_worker_reports_lost_process_after_restart(tmp_path):
    """Агент сам сообщает о смерти своего процесса, а не ждёт вердикта центра."""
    from audit_worker import reconciliation
    from audit_worker.local_store import LocalJobStore
    from audit_worker.process_registry import ProcessRegistry

    store = LocalJobStore(tmp_path / "jobs")
    store.create({"job_id": "job-lost", "attempt_id": "att_1",
                  "job_type": "test_pipeline_v1", "project_id": "p"})
    store.update("job-lost", "att_1", local_state="running", pid=999999,
                 command_fingerprint="abc")
    registry = ProcessRegistry(tmp_path / "runtime")
    registry.register(999999, job_id="job-lost", attempt_id="att_1",
                      command_fingerprint="abc")

    assert reconciliation.survived_processes(store, registry) == []
    lost = reconciliation.lost_processes(store, registry)
    assert [m["job_id"] for m in lost] == ["job-lost"]


def test_known_jobs_payload_carries_disk_state(tmp_path):
    from audit_worker import reconciliation
    from audit_worker.local_store import LocalJobStore

    store = LocalJobStore(tmp_path / "jobs")
    store.create({"job_id": "job-disk", "attempt_id": "att_1",
                  "job_type": "test_pipeline_v1", "project_id": "p"})
    store.update("job-disk", "att_1", local_state="completed_locally",
                 result_hash="a" * 64, upload_id="upl_1")
    (store.job_dir("job-disk", "att_1") / "result" / "summary.json").write_text("{}")

    known = reconciliation.collect_known_jobs(store, store.jobs_dir)[0]
    assert known["result_present"] is True
    assert known["source_present"] is False
    assert known["upload_id"] == "upl_1"
    assert known["processes_alive"] is False


# ─── §21 Экран оператора: контракт данных ────────────────────────────────────
def test_pending_count_in_summary(client):
    _register(client)
    summary = client.get("/api/workers").json()["summary"]
    assert summary["pending"] == 1
    assert summary["total"] == 1


def test_result_details_exposed_after_acceptance(client, center_env, tmp_path):
    """Оператор видит, ЧТО принято: хэш, размер, дату приёма, срок хранения."""
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service, repositories

    job_id, attempt_id, _ = _running_job(client, center_env)
    for state, actor in (("completed_locally", "worker"), ("result_uploading", "worker"),
                         ("result_received", "worker")):
        job_service.transition(job_id=job_id, to_state=JobState(state), actor=actor,
                               reason="тест", settings=center_env)

    # Кладём «принятый» архив на диск и финализируем поля, как это делает приём.
    target = center_env.validated_results_dir / job_id / attempt_id
    target.mkdir(parents=True, exist_ok=True)
    (target / "result.tar.gz").write_bytes(b"x" * 4096)
    (target / "validation_report.json").write_text("{}")
    for state, actor in (("validating", "center"), ("completed", "center")):
        job_service.transition(job_id=job_id, to_state=JobState(state), actor=actor,
                               reason="тест", settings=center_env)
    accepted_at = time.time()
    repositories.update_job_fields(
        job_id,
        {"validated_at": accepted_at, "retention_until": accepted_at + 86400,
         "result_package_hash": "b" * 64},
        settings=center_env,
    )

    view = client.get(f"/api/workers/jobs/{job_id}").json()["job"]
    assert view["result_package_hash"] == "b" * 64
    assert view["result_package_size"] == 4096
    assert view["result_package_name"] == "result.tar.gz"
    assert view["validated_at"] == pytest.approx(accepted_at)
    assert view["retention_unconfirmed"] is False


def test_ui_offers_reject_for_pending_only():
    """Кнопка «Отклонить» рисуется рядом с «Одобрить» и бьёт в /reject."""
    js = (_ROOT / "frontend/static/js/audit-workers.js").read_text(encoding="utf-8")
    assert "data-reject=" in js
    assert "/reject" in js
    # Отклонение и отзыв — оба через подтверждение.
    assert js.count("window.confirm") >= 2
    # Секция ожидающих существует и отделена от рабочих карточек.
    assert "pendingBlock" in js
    html = (_ROOT / "frontend/audit-workers.html").read_text(encoding="utf-8")
    assert 'id="pendingBlock"' in html


def test_ui_shows_result_credentials():
    js = (_ROOT / "frontend/static/js/audit-workers.js").read_text(encoding="utf-8")
    for field in ("result_package_hash", "result_package_size", "validated_at",
                  "retention_until"):
        assert field in js, field


# ─── §18 Обрыв связи ПРИ ПЕРЕДАЧЕ готового результата ────────────────────────
def test_upload_failure_is_not_a_job_failure(tmp_path, monkeypatch):
    """Аудит выполнен, канал упал: это отложенная передача, а не провал.

    Регресс живого прогона: раньше сюда прилетало job_failed и local_state
    становился `failed` — готовая работа помечалась потерянной.
    """
    from audit_worker import agent as agent_module
    from audit_worker.agent import UploadDeferred, WorkerAgent

    calls: dict[str, Any] = {}

    def boom(**kwargs):
        calls["tried"] = True
        raise ConnectionRefusedError("[Errno 111] Connection refused")

    monkeypatch.setattr(agent_module, "upload_result", boom)

    instance = object.__new__(WorkerAgent)
    instance.jobs = __import__(
        "audit_worker.local_store", fromlist=["LocalJobStore"]
    ).LocalJobStore(tmp_path / "jobs")
    instance.token = "wtk_" + "a" * 30
    instance.worker_id = "wrk_x"
    instance.client = object()          # до сети дело не дойдёт: upload_result подменён
    instance._flush_outbox = lambda ctx: None

    job_dir = instance.jobs.job_dir("job-up", "att_1")
    instance.jobs.create({"job_id": "job-up", "attempt_id": "att_1",
                          "job_type": "test_pipeline_v1", "project_id": "p"})
    (job_dir / "work").mkdir(parents=True, exist_ok=True)
    (job_dir / "result").mkdir(parents=True, exist_ok=True)
    (job_dir / "result" / "summary.json").write_text('{"status":"ok"}')
    (job_dir / "result" / "run_log.txt").write_text("ok\n")

    from audit_worker.event_outbox import EventOutbox
    ctx = {"job_id": "job-up", "attempt_id": "att_1", "execution_token": "etk_x",
           "outbox": EventOutbox(job_dir / "events"), "stage": "package"}
    assignment = {"project_id": "p", "version_id": None,
                  "package": {"manifest_version": 1, "sha256": "a" * 64}}

    with pytest.raises(UploadDeferred):
        WorkerAgent._package_and_upload(instance, assignment, ctx, job_dir)

    assert calls.get("tried")
    meta = instance.jobs.load("job-up", "att_1")
    assert meta["local_state"] == "completed_locally"   # НЕ failed
    assert meta["result_hash"]
    types = [e["event_type"] for e in ctx["outbox"].pending_batch(limit=100)]
    assert "job_completed_locally" in types
    assert "job_failed" not in types                    # ключевое


def test_pending_results_are_retried_without_restart(tmp_path, monkeypatch):
    """Досылка не должна ждать перезапуска агента."""
    from audit_worker.agent import WorkerAgent
    from audit_worker.local_store import LocalJobStore

    instance = object.__new__(WorkerAgent)
    instance.jobs = LocalJobStore(tmp_path / "jobs")
    tried: list[tuple[str, str]] = []
    instance._resume_upload = lambda j, a: tried.append((j, a))

    for job_id, state, digest in (
        ("j1", "completed_locally", "a" * 64),
        ("j2", "completed_locally", None),     # архива нет — нечего слать
        ("j3", "finished", "b" * 64),          # уже принят
        ("j4", "running", None),
    ):
        instance.jobs.create({"job_id": job_id, "attempt_id": "att_1",
                              "job_type": "test_pipeline_v1", "project_id": "p"})
        instance.jobs.update(job_id, "att_1", local_state=state, result_hash=digest)

    WorkerAgent._deliver_pending_results(instance)
    assert tried == [("j1", "att_1")]


def test_center_catches_up_when_archive_arrives_before_events(client, center_env):
    """Пакет доехал раньше событий: центр обязан догнать состояние, а не 500."""
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service, repositories

    job_id, attempt_id, _ = _running_job(client, center_env)
    assert repositories.get_job(job_id, settings=center_env)["state"] == "running"

    job = job_service.catch_up_to_result_received(job_id=job_id, settings=center_env)
    assert job["state"] == JobState.RESULT_RECEIVED.value
    states = [t["to_state"] for t in
              repositories.list_transitions(job_id, settings=center_env)]
    # Прошли законными рёбрами, ни одно не перепрыгнули.
    assert states[-3:] == ["completed_locally", "result_uploading", "result_received"]


def test_result_of_failed_attempt_is_stored_not_published(client, center_env, tmp_path):
    """Результат провалившейся попытки не теряется и не публикуется."""
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service, repositories

    job_id, attempt_id, _ = _running_job(client, center_env)
    job_service.transition(job_id=job_id, to_state=JobState.FAILED, actor="worker",
                           reason="тест", settings=center_env)
    job = repositories.get_job(job_id, settings=center_env)

    archive = tmp_path / "late.tar.gz"
    archive.write_bytes("результат опоздавшей попытки".encode("utf-8"))
    stored = job_service.store_unpublished_result(
        job=job, archive=archive, settings=center_env)

    assert stored["state"] == JobState.SUPERSEDED_RESULT_RECEIVED.value
    assert stored["retention_until"] is not None      # воркеру есть что чистить
    kept = center_env.rejected_results_dir / job_id / attempt_id
    assert (kept / "late.tar.gz").is_file()
    reason = json.loads((kept / "unpublished_reason.json").read_text())
    assert reason["published"] is False
    # В валидированные результаты не попал.
    assert not (center_env.validated_results_dir / job_id).exists()
    assert job_service.display_status(stored) == "Результат отозванной попытки"


# ─── §19 Миграции: обновление без удаления базы ──────────────────────────────
def test_migration_2_upgrades_existing_database(tmp_path):
    """Старая база версии 1 доводится до 2 без потери данных и без пересоздания."""
    import sqlite3

    from backend.app.services.distributed_workers import schema

    db = tmp_path / "workers.db"
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    schema.apply_pragmas(conn)

    # Ставим ТОЛЬКО первую миграцию — имитируем базу, созданную до claim-потока.
    conn.executescript(schema.MIGRATIONS[1])
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations "
        "(version INTEGER PRIMARY KEY, applied_at REAL NOT NULL)"
    )
    conn.execute("INSERT INTO schema_migrations VALUES (1, 0.0)")
    conn.execute(
        "INSERT INTO workers (worker_id, display_name, instance_id, worker_version,"
        " protocol_version, capabilities, registration_status, worker_state,"
        " configured_max_slots, created_at, updated_at)"
        " VALUES ('wrk_old', 'старый', 'inst_old00000001', '0.1.0', 1, '{}',"
        " 'approved', 'idle', 1, 1.0, 1.0)"
    )
    conn.commit()
    assert schema.current_version(conn) == 1
    columns = {r["name"] for r in conn.execute("PRAGMA table_info(workers)")}
    assert "claim_secret_sha256" not in columns

    version = schema.migrate(conn)

    assert version == schema.SCHEMA_VERSION == 2
    columns = {r["name"] for r in conn.execute("PRAGMA table_info(workers)")}
    assert {"claim_secret_sha256", "claim_issued_at", "claim_used_at",
            "rejected_at"} <= columns
    # Данные на месте — база не пересоздавалась.
    row = conn.execute("SELECT display_name FROM workers WHERE worker_id='wrk_old'").fetchone()
    assert row["display_name"] == "старый"
    # Повторный прогон миграций ничего не ломает.
    assert schema.migrate(conn) == 2
    conn.close()


def test_no_plaintext_token_column_exists(tmp_path):
    """В схеме нет колонки, куда токен можно было бы положить открытым текстом."""
    import sqlite3

    from backend.app.services.distributed_workers import schema

    conn = sqlite3.connect(tmp_path / "w.db")
    conn.row_factory = sqlite3.Row
    schema.apply_pragmas(conn)
    schema.migrate(conn)
    for table in ("workers", "worker_tokens", "remote_jobs"):
        names = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
        plain = {n for n in names
                 if ("token" in n or "secret" in n) and not n.endswith("sha256")}
        # Допускаются отметки времени и идентификатор записи, но не значения.
        allowed = {"token_id"}
        assert not {n for n in plain
                    if not n.endswith("_at") and n not in allowed}, (table, plain)
    conn.close()


# ─── §14 Потоки вывода, артефакты, восстановление связи ──────────────────────
def test_stdout_and_stderr_are_separate_streams(tmp_path):
    """Потоки не сливаются: у каждой строки известен источник."""
    from audit_worker import test_runner

    lines: list[tuple[str, str, str]] = []
    params = test_runner.validate_params(
        {"label": "streams", "steps": 2, "step_seconds": 0.0, "result_bytes": 64},
        max_total_sec=60.0,
    )
    outcome = test_runner.run_test_job(
        params=params, job_dir=tmp_path / "job",
        on_progress=lambda *a: None,
        on_log=lambda stream, level, line: lines.append((stream, level, line)),
    )
    assert outcome.exit_code == 0
    assert {s for s, _, _ in lines} <= {"stdout", "stderr"}
    # Счётчик считает ВСЕ строки потока, а в on_log уходят только те, что не
    # были разобраны как прогресс: строк прочитано не меньше, чем передано.
    assert outcome.stdout_lines >= sum(1 for s, _, _ in lines if s == "stdout") > 0
    assert outcome.stderr_lines == sum(1 for s, _, _ in lines if s == "stderr")


def test_artifact_created_declares_every_result_file(tmp_path):
    """Каждый файл результата объявляется событием с размером и хэшем.

    Событие порождается в `_run` сразу после процесса — раньше, чем собран
    архив: центр должен знать, ЧТО создано, ещё до получения пакета.
    """
    from audit_worker import package_io, test_runner
    from audit_worker.event_outbox import EventOutbox

    job_dir = tmp_path / "job"
    params = test_runner.validate_params(
        {"label": "art", "steps": 1, "step_seconds": 0.0, "result_bytes": 128},
        max_total_sec=60.0,
    )
    test_runner.run_test_job(params=params, job_dir=job_dir,
                             on_progress=lambda *a: None, on_log=lambda *a: None)

    outbox = EventOutbox(job_dir / "events")
    for artifact in sorted((job_dir / "result").rglob("*")):
        if artifact.is_file():
            outbox.append("artifact_created", {
                "name": artifact.relative_to(job_dir / "result").as_posix(),
                "path_rel": "result/" + artifact.relative_to(job_dir / "result").as_posix(),
                "bytes": artifact.stat().st_size,
                "sha256": package_io.sha256_file(artifact),
            })
    declared = {e["payload"]["path_rel"] for e in outbox.pending_batch(limit=50)}
    assert "result/summary.json" in declared
    assert "result/run_log.txt" in declared
    assert all(e["payload"]["sha256"] and e["payload"]["bytes"] is not None
               for e in outbox.pending_batch(limit=50))


def test_reconnect_event_is_sent_in_the_same_pass(tmp_path):
    """worker_reconnected не должен зависать в outbox до следующего цикла."""
    from audit_worker.agent import WorkerAgent
    from audit_worker.event_outbox import EventOutbox

    sent: list[list[dict]] = []

    class FakeClient:
        def post_events(self, job_id, attempt_id, first_seq, batch, token):
            sent.append(list(batch))
            return {"last_seen_seq": batch[-1]["seq"]}

    instance = object.__new__(WorkerAgent)
    instance.client = FakeClient()
    instance.instance_id = "inst_zzz00000001"
    instance._stop = __import__("threading").Event()

    class Cfg:
        event_batch_max = 50
    instance.config = Cfg()

    outbox = EventOutbox(tmp_path / "events")
    outbox.append("job_started", {"stage": "t"})
    ctx = {"job_id": "j", "attempt_id": "a", "outbox": outbox,
           "execution_token": "etk", "last_send_error": "обрыв связи"}

    WorkerAgent._flush_outbox(instance, ctx)

    types = [e["event_type"] for batch in sent for e in batch]
    assert "worker_reconnected" in types
    assert not outbox.has_pending          # ничего не осталось «на потом»


# ─── Находки состязательных проверок ─────────────────────────────────────────
def test_poisoned_resource_snapshot_cannot_reach_screen(client, center_env):
    """HTML и строки вместо чисел не доезжают до панели оператора."""
    worker_id, headers = _approved_worker(client)
    payload = {
        "instance_id": "inst_hardening01", "sent_at": time.time(),
        "configured_max_slots": 1, "calculated_free_slots": 1,
        "resource_snapshot": {
            "at": time.time(),
            "ram": {"total_gb": "<img src=x onerror=alert(1)>", "available_gb": 4.5},
            "cpu": {"cores": {"вложенный": "объект"}, "la5": 1.2},
            "disk": {"free_gb": "нет данных"},
            "slots": {"binding_constraint": "ram", "explanation": "я" * 5000},
        },
    }
    assert client.post("/api/v1/worker/heartbeat", json=payload,
                       headers=headers).status_code == 200

    shown = client.get("/api/workers").json()["workers"][0]["resource_snapshot"]
    assert "onerror" not in json.dumps(shown, ensure_ascii=False)
    assert "total_gb" not in shown["ram"]          # строка отброшена целиком
    assert shown["ram"]["available_gb"] == 4.5     # число сохранено
    assert "cores" not in shown["cpu"]
    assert len(shown["slots"]["explanation"]) <= 200


def test_poisoned_progress_does_not_kill_the_jobs_screen(client, center_env):
    """Строка в elapsed_sec роняла ВЕСЬ список заданий, и навсегда."""
    job_id, attempt_id, headers = _running_job(client, center_env)
    exec_headers = {**headers, "X-Execution-Token": headers.pop("_exec")}
    response = client.post(
        "/api/v1/worker/events",
        json={"job_id": job_id, "attempt_id": attempt_id, "first_seq": 2, "count": 1,
              "events": [{"seq": 2, "event_id": "ev-2", "occurred_at": time.time(),  # noqa
                          "event_type": "stage_progress",
                          "payload": {"stage": "t", "processed": "много",
                                      "total": "все", "elapsed_sec": "долго",
                                      "percent_reliable": True,
                                      "completed_operations": [1, 2]}}]},
        headers=exec_headers,
    )
    assert response.status_code == 200
    listing = client.get("/api/workers/jobs/list")
    assert listing.status_code == 200
    progress = next(j["progress"] for j in listing.json()["jobs"] if j["job_id"] == job_id)
    assert progress["percent"] is None
    assert progress["indeterminate"] is True
    assert isinstance(progress["elapsed_sec"], (int, float))
    assert client.get(f"/api/workers/jobs/{job_id}").status_code == 200


def test_repeat_registration_does_not_reissue_claim_secret(client):
    """Чужой instance_id не перехватывает выдачу токена."""
    first = _register(client).json()
    assert first["claim_secret"]

    # Атакующий приходит с тем же instance_id.
    second = _register(client).json()
    assert second["worker_id"] == first["worker_id"]
    assert second["claim_secret"] is None
    assert "НЕ" in second["message"] or "не перевыпуск" in second["message"].lower()

    # Токен получает владелец ПЕРВОГО секрета, и только он.
    client.post(f"/api/workers/{first['worker_id']}/approve", json={"configured_max_slots": 1})
    ok = client.post("/api/v1/worker/claim",
                     json={"worker_id": first["worker_id"],
                           "instance_id": "inst_hardening01",
                           "claim_secret": first["claim_secret"]})
    assert ok.status_code == 200


def test_foreign_worker_cannot_read_upload_session(client, center_env):
    owner_id, owner_headers = _approved_worker(client)
    client.post("/api/workers/jobs",
                json={"worker_id": owner_id, "project_id": "upl-own",
                      "params": {"steps": 1, "step_seconds": 0.0}})
    assignment = client.post("/api/v1/worker/jobs/next",
                             json={"free_slots": 1, "accepts": {}, "wait_sec": 0},
                             headers=owner_headers).json()
    exec_headers = {**owner_headers, "X-Execution-Token": assignment["execution_token"]}
    session = client.post(
        "/api/v1/worker/uploads",
        json={"job_id": assignment["job_id"], "attempt_id": assignment["attempt_id"],
              "package_type": "result", "expected_size": 1024, "expected_hash": "c" * 64},
        headers=exec_headers,
    ).json()

    other = _register(client, instance_id="inst_other0003").json()
    client.post(f"/api/workers/{other['worker_id']}/approve", json={"configured_max_slots": 1})
    token = client.post("/api/v1/worker/claim",
                        json={"worker_id": other["worker_id"],
                              "instance_id": "inst_other0003",
                              "claim_secret": other["claim_secret"]}).json()["worker_token"]
    foreign = {"Authorization": f"Bearer {token}", "X-Worker-Id": other["worker_id"],
               "X-Protocol-Version": "1"}
    assert client.get(f"/api/v1/worker/uploads/{session['upload_id']}",
                      headers=foreign).status_code == 403


def test_foreign_ack_does_not_swallow_the_command(client, center_env):
    """403 не должен гасить команду: адресат обязан её получить."""
    from backend.app.services.distributed_workers import repositories

    owner_id, owner_headers = _approved_worker(client)
    command = repositories.enqueue_command(
        worker_id=owner_id, command_type="drain", payload={},
        idempotency_key="drain-1", settings=center_env,
    )
    command_id = command["command_id"]

    other = _register(client, instance_id="inst_other0004").json()
    client.post(f"/api/workers/{other['worker_id']}/approve", json={"configured_max_slots": 1})
    token = client.post("/api/v1/worker/claim",
                        json={"worker_id": other["worker_id"],
                              "instance_id": "inst_other0004",
                              "claim_secret": other["claim_secret"]}).json()["worker_token"]
    foreign = {"Authorization": f"Bearer {token}", "X-Worker-Id": other["worker_id"],
               "X-Protocol-Version": "1"}

    assert client.post(
        f"/api/v1/worker/commands/{command_id}/ack",
        json={"result": {"status": "ok"}, "acknowledged_at": time.time()},
        headers=foreign,
    ).status_code == 403
    # Команда всё ещё ждёт настоящего адресата.
    pending = client.get("/api/v1/worker/commands", headers=owner_headers).json()
    assert [c["command_id"] for c in pending["commands"]] == [command_id]


def test_log_path_traversal_is_rejected(client, center_env):
    job_id, _, _ = _running_job(client, center_env)
    bad = client.get(f"/api/workers/jobs/{job_id}/logs",
                     params={"attempt": "../../секреты"})
    assert bad.status_code == 400
    assert "Недопустимый" in bad.json()["detail"]


def test_register_accepts_only_bearer_scheme(client):
    response = client.post(
        "/api/v1/worker/register",
        json={"instance_id": "inst_scheme00001", "protocol_version": 1},
        headers={"Authorization": f"Foo {BOOTSTRAP}"},
    )
    assert response.status_code == 401


def test_execution_token_is_not_cached_in_plaintext(client, center_env):
    """Кэш идемпотентности не должен хранить секрет попытки открытым текстом."""
    from backend.app.services.distributed_workers import database

    worker_id, headers = _approved_worker(client)
    client.post("/api/workers/jobs",
                json={"worker_id": worker_id, "project_id": "cache-token",
                      "params": {"steps": 1, "step_seconds": 0.0}})
    body = {"free_slots": 1, "accepts": {}, "wait_sec": 0}
    key = {"Idempotency-Key": "tok-1"}
    first = client.post("/api/v1/worker/jobs/next", json=body,
                        headers={**headers, **key}).json()
    token = first["execution_token"]

    with database.read_conn(center_env) as conn:
        dump = "".join(json.dumps(dict(r), default=str)
                       for r in conn.execute("SELECT * FROM idempotency_keys"))
    assert dump                       # запись есть
    assert token not in dump          # а секрета в ней нет

    # Повтор возвращает ТО ЖЕ задание и рабочий токен (уже свежий).
    second = client.post("/api/v1/worker/jobs/next", json=body,
                         headers={**headers, **key}).json()
    assert second["job_id"] == first["job_id"]
    assert second["execution_token"]
    ok = client.post(
        f"/api/v1/worker/jobs/{second['job_id']}/accept",
        json={"attempt_id": second["attempt_id"], "accepted_at": time.time(),
              "source_verified": {"sha256_ok": True, "manifest_version": 1}},
        headers={**headers, "X-Execution-Token": second["execution_token"]},
    )
    assert ok.status_code == 200


def test_reconcile_reoffers_job_the_worker_never_received(client, center_env):
    """Потерянный ответ /jobs/next больше не блокирует проект навсегда."""
    from backend.app.services.distributed_workers import repositories

    worker_id, headers = _approved_worker(client)
    client.post("/api/workers/jobs",
                json={"worker_id": worker_id, "project_id": "lost-answer",
                      "params": {"steps": 1, "step_seconds": 0.0}})
    assignment = client.post("/api/v1/worker/jobs/next",
                             json={"free_slots": 1, "accepts": {}, "wait_sec": 0},
                             headers=headers).json()
    job_id = assignment["job_id"]
    # Ответ «потерялся»: воркер о задании не знает, состояние уже не assigned.
    assert repositories.get_job(job_id, settings=center_env)["state"] == "source_uploading"
    assert client.post("/api/v1/worker/jobs/next",
                       json={"free_slots": 1, "accepts": {}, "wait_sec": 0},
                       headers=headers).status_code == 204

    verdict = client.post(
        "/api/v1/worker/reconcile",
        json={"instance_id": "inst_hardening01", "restarted_at": time.time(),
              "known_jobs": []},
        headers=headers,
    ).json()
    assert verdict["reoffered_jobs"] == [job_id]
    assert repositories.get_job(job_id, settings=center_env)["state"] == "assigned"

    # И следующий обычный опрос его выдаёт.
    again = client.post("/api/v1/worker/jobs/next",
                        json={"free_slots": 1, "accepts": {}, "wait_sec": 0},
                        headers=headers)
    assert again.status_code == 200
    assert again.json()["job_id"] == job_id


def test_running_job_is_never_reoffered(client, center_env):
    """Работающее задание в очередь не возвращается ни при каких условиях."""
    from backend.app.services.distributed_workers import job_service, repositories

    job_id, attempt_id, headers = _running_job(client, center_env)
    assert repositories.get_job(job_id, settings=center_env)["state"] == "running"
    reoffered = job_service.reoffer_unknown_jobs(
        worker_id=repositories.get_job(job_id, settings=center_env)["assigned_worker_id"],
        known_job_ids=set(), settings=center_env,
    )
    assert reoffered == []
    assert repositories.get_job(job_id, settings=center_env)["state"] == "running"


def test_heartbeat_carries_retention_updates(client, center_env):
    """Канал подтверждения хранения не должен быть пустым всегда."""
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service, repositories

    job_id, attempt_id, headers = _running_job(client, center_env)
    for state, actor in (("completed_locally", "worker"), ("result_uploading", "worker"),
                         ("result_received", "worker"), ("validating", "center"),
                         ("completed", "center")):
        job_service.transition(job_id=job_id, to_state=JobState(state), actor=actor,
                               reason="тест", settings=center_env)
    repositories.update_job_fields(
        job_id, {"validated_at": time.time(), "retention_until": time.time() + 60},
        settings=center_env)

    beat = client.post("/api/v1/worker/heartbeat",
                       json={"instance_id": "inst_hardening01", "sent_at": time.time()},
                       headers=headers).json()
    assert [u["job_id"] for u in beat["retention_updates"]] == [job_id]
    assert beat["retention_updates"][0]["retention_until"] is not None


def test_finalize_survives_center_restart_inside_validation(client, center_env, tmp_path):
    """Состояние validating перестало быть тупиком."""
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service, repositories

    job_id, attempt_id, _ = _running_job(client, center_env)
    for state, actor in (("completed_locally", "worker"), ("result_uploading", "worker"),
                         ("result_received", "worker"), ("validating", "center")):
        job_service.transition(job_id=job_id, to_state=JobState(state), actor=actor,
                               reason="тест", settings=center_env)
    # Центр «перезапустился» ровно здесь. Догон обязан не падать...
    job = job_service.catch_up_to_result_received(job_id=job_id, settings=center_env)
    assert job["state"] == "validating"

    # ...и повторная финализация обязана пройти, а не упереться в 409 навсегда.
    from audit_worker import package_io
    job_dir = tmp_path / "job"
    (job_dir / "result").mkdir(parents=True)
    (job_dir / "work").mkdir(parents=True)
    (job_dir / "result" / "summary.json").write_text('{"status":"ok"}')
    (job_dir / "result" / "run_log.txt").write_text("ok\n")
    archive = tmp_path / "res.tar.gz"
    manifest = package_io.build_result_package(
        dest_path=archive, job_dir=job_dir, job_id=job_id, attempt_id=attempt_id,
        project_id="p", version_id=None, worker_id="wrk_x", worker_version="0.1.0",
        protocol_version=1, manifest_version=1,
        source_package_hash="sha256:" + "a" * 64, exit_code=0,
    )
    updated, report = job_service.finalize_result(
        job=repositories.get_job(job_id, settings=center_env),
        archive=archive,
        expected_hash=manifest["archive"]["sha256"],
        expected_size=manifest["archive"]["compressed_bytes"],
        settings=center_env,
    )
    assert report.ok, report.as_dict()
    assert updated["state"] == "completed"


def test_outbox_append_is_thread_safe(tmp_path):
    """Три потока пишут в один outbox: seq не должен дублироваться."""
    import threading

    from audit_worker.event_outbox import EventOutbox

    outbox = EventOutbox(tmp_path / "events")

    def spam(n):
        for i in range(150):
            outbox.append("log_line", {"message": f"{n}-{i}"})

    threads = [threading.Thread(target=spam, args=(k,)) for k in range(4)]
    [t.start() for t in threads]
    [t.join() for t in threads]

    seqs = []
    for path in sorted((tmp_path / "events").glob("outbox-*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                seqs.append(json.loads(line)["seq"])
    assert len(seqs) == 600
    assert len(set(seqs)) == 600                       # ни одного дубля
    assert sorted(seqs) == list(range(1, 601))


def test_outbox_repairs_cursor_ahead_of_segments(tmp_path):
    """Курсор впереди файлов останавливал поток событий насовсем."""
    from audit_worker.event_outbox import EventOutbox

    events = tmp_path / "events"
    outbox = EventOutbox(events)
    for i in range(5):
        outbox.append("stage_progress", {"processed": i})

    # Имитируем потерю хвоста сегмента при жёстком отказе машины.
    cursor = json.loads((events / "cursor.json").read_text())
    cursor["last_written_seq"] = 99
    (events / "cursor.json").write_text(json.dumps(cursor))

    revived = EventOutbox(events)
    assert revived.last_written_seq == 5
    assert revived.has_pending
    batch = revived.pending_batch(limit=10)
    assert [e["seq"] for e in batch] == [1, 2, 3, 4, 5]


def test_finished_work_is_packaged_not_declared_lost(tmp_path):
    """Рестарт между выходом процесса и сборкой архива не уничтожает работу."""
    from audit_worker import reconciliation, test_runner
    from audit_worker.agent import _write_completed_marker
    from audit_worker.local_store import LocalJobStore
    from audit_worker.process_registry import ProcessRegistry

    store = LocalJobStore(tmp_path / "jobs")
    registry = ProcessRegistry(tmp_path / "runtime")
    store.create({"job_id": "job-done", "attempt_id": "att_1",
                  "job_type": "test_pipeline_v1", "project_id": "p"})
    store.update("job-done", "att_1", local_state="running", pid=999999)
    job_dir = store.job_dir("job-done", "att_1")
    (job_dir / "result").mkdir(parents=True, exist_ok=True)
    (job_dir / "result" / "summary.json").write_text('{"status":"ok"}')
    _write_completed_marker(
        job_dir,
        test_runner.RunOutcome(exit_code=0, duration_sec=1.0, steps_done=2,
                               steps_total=2, stdout_lines=4, stderr_lines=0),
    )

    ready = reconciliation.finished_but_unpackaged(store, registry)
    assert [m["job_id"] for m in ready] == ["job-done"]
    # И такое задание НЕ попадает в «процесс потерян».
    assert reconciliation.lost_processes(store, registry) == []


def test_crashed_run_without_marker_is_still_reported_lost(tmp_path):
    """Обратная сторона: без маркера это по-прежнему потеря процесса."""
    from audit_worker import reconciliation
    from audit_worker.local_store import LocalJobStore
    from audit_worker.process_registry import ProcessRegistry

    store = LocalJobStore(tmp_path / "jobs")
    registry = ProcessRegistry(tmp_path / "runtime")
    store.create({"job_id": "job-crash", "attempt_id": "att_1",
                  "job_type": "test_pipeline_v1", "project_id": "p"})
    store.update("job-crash", "att_1", local_state="running", pid=999999)
    assert reconciliation.finished_but_unpackaged(store, registry) == []
    assert [m["job_id"] for m in reconciliation.lost_processes(store, registry)] == ["job-crash"]


def test_agent_sends_idempotency_key(tmp_path):
    """Заголовок должен реально уходить: иначе защита от повтора не включается."""
    from audit_worker.client import CenterClient

    sent: list[dict] = []

    class Recorder(CenterClient):
        def request(self, method, path, **kwargs):
            sent.append({"path": path, "headers": kwargs.get("headers") or {}})
            return {}

    client = Recorder("https://center.example", token="wtk_x", worker_id="wrk_x",
                      instance_id="inst_x")
    client.accept_job("job-1", {"attempt_id": "att_1"}, "etk_1")
    client.create_upload({"job_id": "job-1", "attempt_id": "att_1", "sha256": "a" * 64},
                         "etk_1")
    client.close()

    keys = [c["headers"].get("Idempotency-Key") for c in sent]
    assert all(keys), keys
    assert keys[0] == "accept:job-1:att_1"
    assert "a" * 64 in keys[1]
