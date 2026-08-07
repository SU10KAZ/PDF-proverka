"""
test_distributed_workers_center.py
----------------------------------
Центральная часть подсистемы: машина состояний, идемпотентность событий,
формат пакета, безопасность распаковки, чанкованная загрузка, разделение
контуров аутентификации.

Ключевые инварианты техпроекта, которые здесь закреплены тестами:
  I-01/I-02  молчание воркера НЕ переводит задание в failed;
  I-04       повторная отправка события не применяет последствия дважды;
  I-05       одно задание не исполняется дважды;
  I-06       повторная загрузка не создаёт дубликат результата;
  I-07       результат не публикуется до четырёх проверок;
  I-10/I-11  канала произвольных команд нет.

Run: python -m pytest tests/test_distributed_workers_center.py -v
"""
from __future__ import annotations

import json
import sys
import tarfile
import time
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

BOOTSTRAP = "test-bootstrap-secret-0123456789abcdef"


@pytest.fixture()
def settings(tmp_path, monkeypatch):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "center"))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET", BOOTSTRAP)
    monkeypatch.setenv("DISTRIBUTED_WORKERS_UPLOAD_CHUNK_BYTES", "4096")

    from backend.app.services.distributed_workers import database
    from backend.app.services.distributed_workers.settings import get_settings

    database.reset_state_for_tests()
    st = get_settings()
    database.ensure_ready(st)
    yield st
    database.reset_state_for_tests()


@pytest.fixture()
def approved_worker(settings):
    from backend.app.services.distributed_workers import registration_service, repositories, worker_registry

    worker = repositories.create_worker(
        display_name="VPS-1", instance_id="inst_test0001",
        worker_version="0.1.0", protocol_version=1, pipeline_revision=None,
        capabilities={"job_types": ["test_pipeline_v1"], "compressions": ["gzip"]},
        configured_max_slots=2, settings=settings,
    )
    registration_service.approve_worker(
        worker_id=worker["worker_id"], display_name=None,
        configured_max_slots=2, settings=settings,
    )
    worker_registry.record_heartbeat(
        worker_id=worker["worker_id"], instance_id="inst_test0001",
        worker_state="idle", configured_max_slots=2, calculated_free_slots=2,
        active_jobs=[], resource_snapshot={"at": time.time()}, warnings=[],
        settings=settings,
    )
    return repositories.get_worker(worker["worker_id"], settings=settings)


def _make_job(settings, worker, **overrides):
    from backend.app.models.distributed_workers import TestJobParams
    from backend.app.services.distributed_workers import job_service

    return job_service.create_test_job(
        worker_id=worker["worker_id"],
        project_id=overrides.get("project_id", "proj-1"),
        version_id=overrides.get("version_id"),
        params=TestJobParams(steps=2, step_seconds=0.0),
        actor="operator:test",
        settings=settings,
    )


# ─── Машина состояний ────────────────────────────────────────────────────────
def test_state_machine_rejects_undeclared_transition(settings, approved_worker):
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service

    job = _make_job(settings, approved_worker)
    with pytest.raises(job_service.IllegalTransition):
        job_service.transition(
            job_id=job["job_id"], to_state=JobState.COMPLETED,
            actor="center", settings=settings,
        )


def test_center_cannot_fail_running_job(settings, approved_worker):
    """I-01/I-02: центр не вправе объявить провал — только воркер или оператор.

    Ребра `running → failed` для роли center в таблице нет намеренно: именно
    через него молчание превращалось бы в потерю результатов.
    """
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service

    allowed = job_service.ALLOWED_TRANSITIONS[JobState.RUNNING][JobState.FAILED]
    assert "center" not in allowed
    assert set(allowed) == {"worker", "operator"}


def test_no_auto_reassign_edge(settings):
    """I-03: ребра «running → assigned» не существует вовсе."""
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service

    assert JobState.ASSIGNED not in job_service.ALLOWED_TRANSITIONS[JobState.RUNNING]


def test_completed_only_from_validating(settings):
    """I-07: единственный вход в completed — из validating."""
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service

    sources = [
        state
        for state, edges in job_service.ALLOWED_TRANSITIONS.items()
        if JobState.COMPLETED in edges
    ]
    assert sources == [JobState.VALIDATING]


def test_all_15_states_reachable_in_table(settings):
    """Число состояний совпадает с перечнем — и все они есть в таблице."""
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service

    assert len(list(JobState)) == 15
    assert set(job_service.ALLOWED_TRANSITIONS) == set(JobState)


def test_double_assignment_blocked_by_index(settings, approved_worker):
    """I-05: два активных задания на один (project_id, version_id) невозможны."""
    from backend.app.services.distributed_workers import repositories

    _make_job(settings, approved_worker, project_id="dup-proj")
    with pytest.raises(repositories.ActiveJobExists):
        _make_job(settings, approved_worker, project_id="dup-proj")


def test_claim_is_single_flight(settings, approved_worker):
    from backend.app.services.distributed_workers import repositories

    _make_job(settings, approved_worker)
    first = repositories.claim_next_job_for_worker(
        approved_worker["worker_id"], settings=settings
    )
    second = repositories.claim_next_job_for_worker(
        approved_worker["worker_id"], settings=settings
    )
    assert first is not None and second is None


# ─── События ─────────────────────────────────────────────────────────────────
def _event(seq: int, etype: str, payload=None):
    return {
        "seq": seq, "event_id": f"ev{seq}", "event_type": etype,
        "occurred_at": time.time(), "schema_version": 1, "payload": payload or {},
    }


def test_event_batch_idempotent(settings, approved_worker):
    """I-04: повторная доставка не применяет последствия дважды."""
    from backend.app.services.distributed_workers import event_service, repositories

    job = _make_job(settings, approved_worker)
    repositories.claim_next_job_for_worker(approved_worker["worker_id"], settings=settings)
    row = repositories.get_job(job["job_id"], settings=settings)

    batch = [_event(1, "source_verified"), _event(2, "job_accepted")]
    first = event_service.ingest_batch(
        job=row, worker_id=approved_worker["worker_id"], first_seq=1,
        events=batch, settings=settings,
    )
    second = event_service.ingest_batch(
        job=row, worker_id=approved_worker["worker_id"], first_seq=1,
        events=batch, settings=settings,
    )
    assert first["accepted"] == 2 and first["skipped_duplicates"] == 0
    assert second["accepted"] == 0 and second["skipped_duplicates"] == 2
    assert second["replayed"] is True
    stored = repositories.list_events(job["job_id"], settings=settings)
    assert len(stored) == 2


def test_event_gap_rejected_with_expected_seq(settings, approved_worker):
    from backend.app.services.distributed_workers import event_service, repositories

    job = _make_job(settings, approved_worker)
    repositories.claim_next_job_for_worker(approved_worker["worker_id"], settings=settings)
    row = repositories.get_job(job["job_id"], settings=settings)
    with pytest.raises(event_service.SequenceGap) as info:
        event_service.ingest_batch(
            job=row, worker_id=approved_worker["worker_id"], first_seq=5,
            events=[_event(5, "log_line")], settings=settings,
        )
    assert info.value.expected_seq == 1


def test_non_contiguous_batch_rejected(settings, approved_worker):
    from backend.app.services.distributed_workers import event_service, repositories

    job = _make_job(settings, approved_worker)
    repositories.claim_next_job_for_worker(approved_worker["worker_id"], settings=settings)
    row = repositories.get_job(job["job_id"], settings=settings)
    with pytest.raises(ValueError):
        event_service.ingest_batch(
            job=row, worker_id=approved_worker["worker_id"], first_seq=1,
            events=[_event(1, "log_line"), _event(3, "log_line")], settings=settings,
        )


def test_log_lines_go_to_file_not_table(settings, approved_worker):
    """Строки лога не раздувают БД; курсор при этом ОДИН на оба потока."""
    from backend.app.services.distributed_workers import event_service, repositories

    job = _make_job(settings, approved_worker)
    repositories.claim_next_job_for_worker(approved_worker["worker_id"], settings=settings)
    row = repositories.get_job(job["job_id"], settings=settings)
    result = event_service.ingest_batch(
        job=row, worker_id=approved_worker["worker_id"], first_seq=1,
        events=[
            _event(1, "log_line", {"level": "info", "message": "строка один"}),
            _event(2, "log_line", {"level": "info", "message": "строка два"}),
            _event(3, "source_verified"),
        ],
        settings=settings,
    )
    assert result["last_seen_seq"] == 3
    table = repositories.list_events(job["job_id"], settings=settings)
    assert [e["event_type"] for e in table] == ["source_verified"]
    lines = event_service.read_log_lines(
        job["job_id"], row["attempt_id"], settings=settings
    )
    assert [line["message"] for line in lines] == ["строка один", "строка два"]


def test_secrets_redacted_on_ingest(settings, approved_worker):
    from backend.app.services.distributed_workers import event_service, repositories

    job = _make_job(settings, approved_worker)
    repositories.claim_next_job_for_worker(approved_worker["worker_id"], settings=settings)
    row = repositories.get_job(job["job_id"], settings=settings)
    event_service.ingest_batch(
        job=row, worker_id=approved_worker["worker_id"], first_seq=1,
        events=[_event(1, "resource_warning",
                       {"message": "OPENROUTER_API_KEY=sk-secret-value-1234567890abcdef"})],
        settings=settings,
    )
    stored = repositories.list_events(job["job_id"], settings=settings)
    assert "sk-secret-value" not in json.dumps(stored[0]["payload"])


# ─── Пакет ───────────────────────────────────────────────────────────────────
def test_source_package_roundtrip(settings, approved_worker):
    from backend.app.services.distributed_workers import job_service, package_service

    job = _make_job(settings, approved_worker)
    archive = job_service.source_package_path(job, settings=settings)
    assert archive is not None and archive.is_file()

    manifest = package_service.read_manifest(archive)
    assert manifest["package_type"] == "source"
    assert manifest["job_id"] == job["job_id"]
    assert manifest["tree_hash"].startswith("sha256:")
    with tarfile.open(archive, "r:gz") as tar:
        names = tar.getnames()
    assert "package_manifest.json" in names
    assert all(n == "package_manifest.json" or n.startswith("payload/") for n in names)


def test_safe_extract_blocks_traversal(tmp_path, settings):
    from backend.app.services.distributed_workers import package_service

    evil = tmp_path / "evil.tar"
    with tarfile.open(evil, "w") as tar:
        info = tarfile.TarInfo("../../etc/passwd")
        info.size = 3
        tar.addfile(info, __import__("io").BytesIO(b"bad"))
    with pytest.raises(package_service.PackageError):
        package_service.safe_extract(evil, tmp_path / "out")
    assert not (tmp_path / "out").exists()


def test_safe_extract_blocks_symlink(tmp_path, settings):
    from backend.app.services.distributed_workers import package_service

    evil = tmp_path / "link.tar"
    with tarfile.open(evil, "w") as tar:
        info = tarfile.TarInfo("payload/escape")
        info.type = tarfile.SYMTYPE
        info.linkname = "/etc/passwd"
        tar.addfile(info)
    with pytest.raises(package_service.PackageError):
        package_service.safe_extract(evil, tmp_path / "out")


def test_safe_extract_entry_cap(tmp_path, settings):
    from backend.app.services.distributed_workers import package_service

    archive = tmp_path / "many.tar"
    import io

    with tarfile.open(archive, "w") as tar:
        for i in range(10):
            info = tarfile.TarInfo(f"payload/f{i}")
            info.size = 1
            tar.addfile(info, io.BytesIO(b"x"))
    with pytest.raises(package_service.PackageError):
        package_service.safe_extract(archive, tmp_path / "out", max_entries=3)


# ─── Валидация результата (четыре проверки) ──────────────────────────────────
def _build_result(tmp_path, job, *, files=None, package_type="result", attempt=None):
    from backend.app.services.distributed_workers import package_service

    payload = files if files is not None else {
        "result/summary.json": b'{"status":"ok"}',
        "result/run_log.txt": b"ok\n",
    }
    dest = tmp_path / "result.tar.gz"
    manifest = {
        "manifest_version": 1,
        "package_id": "pkg_test",
        "package_type": package_type,
        "job_id": job["job_id"],
        "attempt_id": attempt or job["attempt_id"],
        "project_id": job["project_id"],
        "created_at": time.time(),
    }
    full = package_service.build_package(
        dest_path=dest, files=payload, manifest=manifest, compression="gzip"
    )
    return dest, full


def test_validation_accepts_good_result(tmp_path, settings, approved_worker):
    from backend.app.services.distributed_workers import job_service, package_service

    job = _make_job(settings, approved_worker)
    archive, manifest = _build_result(tmp_path, job)
    report = package_service.validate_result_package(
        archive=archive,
        expected_hash=manifest["archive"]["sha256"],
        expected_size=archive.stat().st_size,
        job_id=job["job_id"], attempt_id=job["attempt_id"],
        required_artifacts=job_service.TEST_JOB_REQUIRED_ARTIFACTS,
    )
    assert report.ok, report.checks


def test_validation_rejects_hash_mismatch(tmp_path, settings, approved_worker):
    from backend.app.services.distributed_workers import job_service, package_service

    job = _make_job(settings, approved_worker)
    archive, manifest = _build_result(tmp_path, job)
    report = package_service.validate_result_package(
        archive=archive,
        expected_hash="0" * 64,
        expected_size=archive.stat().st_size,
        job_id=job["job_id"], attempt_id=job["attempt_id"],
        required_artifacts=job_service.TEST_JOB_REQUIRED_ARTIFACTS,
    )
    assert not report.ok and report.error == "hash_mismatch"


def test_validation_rejects_missing_artifacts(tmp_path, settings, approved_worker):
    from backend.app.services.distributed_workers import job_service, package_service

    job = _make_job(settings, approved_worker)
    archive, manifest = _build_result(
        tmp_path, job, files={"result/run_log.txt": b"only log\n"}
    )
    report = package_service.validate_result_package(
        archive=archive,
        expected_hash=manifest["archive"]["sha256"],
        expected_size=archive.stat().st_size,
        job_id=job["job_id"], attempt_id=job["attempt_id"],
        required_artifacts=job_service.TEST_JOB_REQUIRED_ARTIFACTS,
    )
    assert not report.ok and report.error == "artifacts_missing"


def test_validation_rejects_forbidden_paths(tmp_path, settings, approved_worker):
    """Пакет с 04_review/ отвергается целиком, а не «аккуратно пропускается»."""
    from backend.app.services.distributed_workers import job_service, package_service

    job = _make_job(settings, approved_worker)
    archive, manifest = _build_result(
        tmp_path, job,
        files={
            "result/summary.json": b"{}",
            "result/run_log.txt": b"x",
            "04_review/expert_review.json": b'{"verdicts": []}',
        },
    )
    report = package_service.validate_result_package(
        archive=archive,
        expected_hash=manifest["archive"]["sha256"],
        expected_size=archive.stat().st_size,
        job_id=job["job_id"], attempt_id=job["attempt_id"],
        required_artifacts=job_service.TEST_JOB_REQUIRED_ARTIFACTS,
    )
    assert not report.ok and report.error == "forbidden_path"


def test_failed_validation_does_not_publish(tmp_path, settings, approved_worker):
    """I-07: провал проверки → rejected_results/, validated_results/ пуст."""
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service, repositories

    job = _make_job(settings, approved_worker)
    repositories.claim_next_job_for_worker(approved_worker["worker_id"], settings=settings)
    for target, actor in (
        (JobState.SOURCE_READY, "worker"),
        (JobState.ACCEPTED_BY_WORKER, "worker"),
        (JobState.RUNNING, "worker"),
        (JobState.COMPLETED_LOCALLY, "worker"),
        (JobState.RESULT_UPLOADING, "worker"),
        (JobState.RESULT_RECEIVED, "worker"),
    ):
        job = job_service.transition(
            job_id=job["job_id"], to_state=target, actor=actor, settings=settings
        )
    archive, manifest = _build_result(
        tmp_path, job, files={"result/run_log.txt": b"no summary\n"}
    )
    updated, report = job_service.finalize_result(
        job=job, archive=archive,
        expected_hash=manifest["archive"]["sha256"],
        expected_size=archive.stat().st_size,
        settings=settings,
    )
    assert not report.ok
    assert updated["state"] == JobState.FAILED.value
    assert not list(settings.validated_results_dir.rglob("*.tar.gz"))
    assert list(settings.rejected_results_dir.rglob("*.tar.gz"))


# ─── retention_unconfirmed ───────────────────────────────────────────────────
def test_retention_unconfirmed_is_computed_not_a_state(settings, approved_worker):
    """Признак вычисляемый: у задания при этом обычное состояние исполнения."""
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service, repositories

    job = _make_job(settings, approved_worker)
    repositories.claim_next_job_for_worker(approved_worker["worker_id"], settings=settings)
    for target in (JobState.SOURCE_READY, JobState.ACCEPTED_BY_WORKER,
                   JobState.RUNNING, JobState.COMPLETED_LOCALLY):
        job = job_service.transition(
            job_id=job["job_id"], to_state=target, actor="worker",
            fields={"completed_locally_at": time.time()}
            if target is JobState.COMPLETED_LOCALLY else None,
            settings=settings,
        )
    view = job_service.to_view(job, settings=settings)
    assert view["state"] == JobState.COMPLETED_LOCALLY.value
    assert view["retention_unconfirmed"] is True
    assert "автоматическое удаление запрещено" in view["retention_warning"]
    # И это не значение enum состояний.
    assert "retention_unconfirmed" not in {s.value for s in JobState}


# ─── Загрузка ────────────────────────────────────────────────────────────────
def test_upload_session_is_idempotent(settings, approved_worker):
    """I-06: повтор создания сессии возвращает ту же с уже принятыми чанками."""
    from backend.app.services.distributed_workers import repositories, upload_service

    job = _make_job(settings, approved_worker)
    first, replayed_first = upload_service.open_or_create_session(
        job=job, package_type="result", expected_size=10_000,
        expected_hash="a" * 64, settings=settings,
    )
    assert replayed_first is False
    upload_service.store_chunk(
        session=first, idx=0, data=b"x" * 4096, declared_sha256=None, settings=settings
    )
    second, replayed_second = upload_service.open_or_create_session(
        job=job, package_type="result", expected_size=10_000,
        expected_hash="a" * 64, settings=settings,
    )
    assert replayed_second is True
    assert second["upload_id"] == first["upload_id"]
    assert repositories.received_chunks(first["upload_id"], settings=settings) == [0]


def test_chunk_replay_and_conflict(settings, approved_worker):
    from backend.app.services.distributed_workers import upload_service

    job = _make_job(settings, approved_worker)
    session, _ = upload_service.open_or_create_session(
        job=job, package_type="result", expected_size=8192,
        expected_hash="b" * 64, settings=settings,
    )
    data = b"y" * 4096
    assert upload_service.store_chunk(
        session=session, idx=0, data=data, declared_sha256=None, settings=settings
    ) == "inserted"
    assert upload_service.store_chunk(
        session=session, idx=0, data=data, declared_sha256=None, settings=settings
    ) == "replayed"
    with pytest.raises(upload_service.ChunkConflict):
        upload_service.store_chunk(
            session=session, idx=0, data=b"z" * 4096,
            declared_sha256=None, settings=settings,
        )


def test_chunk_declared_hash_checked(settings, approved_worker):
    from backend.app.services.distributed_workers import upload_service

    job = _make_job(settings, approved_worker)
    session, _ = upload_service.open_or_create_session(
        job=job, package_type="result", expected_size=4096,
        expected_hash="c" * 64, settings=settings,
    )
    with pytest.raises(upload_service.UploadError):
        upload_service.store_chunk(
            session=session, idx=0, data=b"q" * 100,
            declared_sha256="0" * 64, settings=settings,
        )


# ─── Команды и безопасность ──────────────────────────────────────────────────
def test_command_enum_has_no_shell(settings):
    """I-10: канала произвольных команд нет и появиться незаметно не может."""
    from backend.app.models.distributed_workers import WorkerCommandType

    values = {c.value for c in WorkerCommandType}
    assert values == {"cancel_job", "drain", "undrain"}
    forbidden = {"run_shell", "exec", "eval", "shell", "run", "command"}
    assert not (values & forbidden)


def test_job_type_enum_is_closed(settings):
    from backend.app.models.distributed_workers import JobType

    assert {t.value for t in JobType} == {"test_pipeline_v1"}


def test_token_stored_as_hash_only(settings, approved_worker):
    """Колонки с открытым токеном в схеме нет."""
    from backend.app.services.distributed_workers import auth, database, repositories

    token = auth.generate_token()
    repositories.insert_token(
        approved_worker["worker_id"], auth.hash_token(token), settings=settings
    )
    with database.read_conn(settings) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(worker_tokens)")}
        rows = conn.execute("SELECT * FROM worker_tokens").fetchall()
    assert "token_sha256" in columns
    assert not any("token" == c or c == "plain_token" or c == "secret" for c in columns)
    assert all(token not in json.dumps(dict(r), default=str) for r in rows)


def test_execution_token_mismatch_rejected(settings, approved_worker):
    from fastapi import HTTPException

    from backend.app.services.distributed_workers import auth

    job = _make_job(settings, approved_worker)
    row = dict(job)
    row["execution_token_sha256"] = auth.hash_token("etk_correct")
    auth.require_execution_token(row, "etk_correct")           # не бросает
    with pytest.raises(HTTPException) as info:
        auth.require_execution_token(row, "etk_wrong")
    assert info.value.status_code == 409
    assert info.value.detail["error"] == "attempt_superseded"


def test_bootstrap_secret_required(monkeypatch, settings):
    """Отсутствие секрета — понятная ошибка конфигурации, а не «пустить всех»."""
    from fastapi import HTTPException

    from backend.app.services.distributed_workers import auth
    from backend.app.services.distributed_workers.settings import (
        DistributedWorkersConfigError,
        get_settings,
    )

    monkeypatch.setenv("DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET", "")
    with pytest.raises(DistributedWorkersConfigError):
        get_settings().require_bootstrap_secret()
    with pytest.raises(HTTPException) as info:
        auth.verify_bootstrap_secret("что-угодно")
    assert info.value.status_code == 503

    monkeypatch.setenv("DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET", "короткий")
    with pytest.raises(DistributedWorkersConfigError):
        get_settings().require_bootstrap_secret()


def test_connectivity_axis_never_touches_job_state(settings, approved_worker):
    """I-02: пересчёт связи не меняет ни одного состояния задания."""
    from backend.app.services.distributed_workers import repositories, worker_registry

    job = _make_job(settings, approved_worker)
    repositories.update_worker_fields(
        approved_worker["worker_id"], {"last_seen_at": time.time() - 100_000},
        settings=settings,
    )
    before = repositories.get_job(job["job_id"], settings=settings)["state"]
    workers = worker_registry.refresh_connectivity(settings=settings)
    after = repositories.get_job(job["job_id"], settings=settings)["state"]
    assert workers[0]["connection_status"] == "offline"
    assert before == after
