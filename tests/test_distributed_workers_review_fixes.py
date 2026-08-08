"""Дефекты, найденные адверсариальной проверкой этапа 3.5, и их закрытие.

Отдельный файл намеренно: каждый тест здесь назван по проблеме, а не по
функции, и падение прямо говорит, какая гарантия сломалась обратно. Все
проверки — только на подтверждённые по коду находки; предположения проверяющих,
не воспроизведённые на коде, сюда не попали.

Разделы:
  §1 принадлежность процесса (I-17);
  §2 журнал событий: счётчик и починка курсора;
  §3 центр: адресация попыткой вместо задания;
  §4 центр: доставка результата и приёмка;
  §5 операторские действия и идемпотентность;
  §6 воркер: очередь, команды, хранение.
"""
from __future__ import annotations

import json
import os
import sqlite3
import stat
import time
import uuid
from pathlib import Path

import httpx
import pytest

from tests.test_distributed_workers_step35 import (  # noqa: F401 — фикстуры
    INTENT,
    _approved_worker,
    _create_job,
    _running_attempt,
    _take_job,
    center_env,
    client,
)


# ═══ §1 Принадлежность процесса (I-17) ═══════════════════════════════════════
def test_pid_without_start_time_is_not_proof_of_life():
    """Голый pid не доказывает ничего — даже того, что процесс жив.

    Раньше `is_alive(pid, None)` возвращал True: незаписанная метка старта
    превращалась в «считаем живым», то есть в разрешение слать сигнал.
    """
    from audit_worker import process_registry

    assert process_registry.is_alive(os.getpid(), None) is False
    # А с настоящей меткой — да, это мы.
    identity = process_registry.process_start_time(os.getpid())
    assert process_registry.is_alive(os.getpid(), identity) is True


def test_ownership_refused_without_start_identity():
    from audit_worker import process_control, process_registry

    row = {
        "job_id": "j", "attempt_id": "a", "pid": os.getpid(),
        "process_start_identity": None,
        "command_fingerprint": process_registry.live_command_fingerprint(os.getpid()),
    }
    ok, why = process_control.verify_ownership(row, job_id="j", attempt_id="a")
    assert ok is False
    assert "метка времени старта" in why


def test_ownership_checks_command_of_the_live_process_not_our_own_copy():
    """Второй источник — ядро, а не вторая копия нашей же записи.

    Сверка `local_db` с `metadata.json` совпадала всегда: оба поля пишет один
    вызов из одной переменной. Настоящая проверка спрашивает /proc.
    """
    from audit_worker import process_control, process_registry

    pid = os.getpid()
    row = {
        "job_id": "j", "attempt_id": "a", "pid": pid,
        "process_start_identity": process_registry.process_start_time(pid),
        "command_fingerprint": "0" * 32,          # запись врёт
    }
    ok, why = process_control.verify_ownership(row, job_id="j", attempt_id="a")
    assert ok is False
    assert "команда живого процесса" in why

    row["command_fingerprint"] = process_registry.live_command_fingerprint(pid)
    ok, why = process_control.verify_ownership(row, job_id="j", attempt_id="a")
    assert ok is True, why


def test_live_fingerprint_matches_the_recorded_formula():
    """Отпечаток из /proc считается так же, как при запуске процесса."""
    from audit_worker import process_registry, test_runner

    pid = os.getpid()
    argv = process_registry.process_cmdline(pid)
    assert argv
    assert process_registry.live_command_fingerprint(pid) == (
        test_runner.command_fingerprint(argv)
    )


# ═══ §2 Журнал событий ═══════════════════════════════════════════════════════
def test_cursor_behind_segments_is_repaired_upward(tmp_path):
    """Потерян cursor.json, сегменты целы → номера НЕ переиспользуются.

    Чинилось только «курсор впереди файлов». Обратный случай приводил к тому,
    что новые события получали уже занятые seq, и центр молча отбрасывал их
    как дубли.
    """
    from audit_worker.event_outbox import EventOutbox

    events = tmp_path / "events"
    first = EventOutbox(events)
    for _ in range(5):
        first.append("log_line", {"text": "x"})
    assert first.last_written_seq == 5

    (events / "cursor.json").unlink()
    second = EventOutbox(events)
    assert second.last_written_seq == 5, "курсор обязан подтянуться до файлов"
    assert second.append("job_started", {}) == 6


def test_two_processes_never_hand_out_the_same_seq(tmp_path):
    """Исполнитель и агент пишут в один каталог — номер обязан быть уникальным.

    Два объекта EventOutbox моделируют два процесса: у каждого свой
    `last_written_seq` в памяти, и без межпроцессного замка оба выдавали
    одному номеру два события.
    """
    from audit_worker.event_outbox import EventOutbox

    events = tmp_path / "events"
    executor_side = EventOutbox(events)
    agent_side = EventOutbox(events)

    seqs = []
    for _ in range(10):
        seqs.append(executor_side.append("log_line", {"t": "e"}))
        seqs.append(agent_side.append("worker_reconnected", {"t": "a"}))

    assert len(set(seqs)) == len(seqs), f"номера повторились: {seqs}"
    assert sorted(seqs) == list(range(1, 21))

    written = []
    for path in sorted(events.glob("outbox-*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                written.append(int(json.loads(line)["seq"]))
    assert sorted(written) == list(range(1, 21))


def test_pending_batch_finds_events_moved_to_acked(tmp_path):
    """После 409 центр просит повторить с номера, уже уехавшего в acked/."""
    from audit_worker.event_outbox import EventOutbox

    outbox = EventOutbox(tmp_path / "events")
    for _ in range(3):
        outbox.append("log_line", {"t": "x"})
    outbox.ack(3)                     # уплотнение уносит сегмент в acked/
    outbox.append("job_started", {})

    outbox.rewind_to(2)               # «повтори с seq=2»
    batch = outbox.pending_batch(limit=10)
    assert [item["seq"] for item in batch] == [2, 3, 4]


# ═══ §3 Центр: адресация попыткой, а не заданием ═════════════════════════════
def test_stale_idempotency_key_cannot_reissue_token_of_another_attempt(
    client, center_env
):
    """Повтор /jobs/next старым ключом НЕ трогает токен новой попытки.

    Запись шла по job_id, то есть в «текущую попытку». Вернувшийся воркер
    повтором старого ключа перевыпускал токен ЧУЖОЙ активной попытки, и её
    законный исполнитель получал 409 на всех ручках.
    """
    from backend.app.services.distributed_workers import repositories

    worker_id, headers = _approved_worker(client)
    _create_job(client, worker_id, project="ПРОЕКТ/ключи")
    first = client.post(
        "/api/v1/worker/jobs/next", json={"free_slots": 1, "wait_sec": 0},
        headers={**headers, "Idempotency-Key": "reuse-me"},
    ).json()
    old_attempt = first["attempt_id"]

    client.post(
        f"/api/workers/jobs/{first['job_id']}/attempts/{old_attempt}/mark-lost",
        json={"mandatory_reason": "VPS молчит",
              "typed_confirmation": "ПОПЫТКА ПОТЕРЯНА",
              "observed_worker_state": "offline"},
        headers={**INTENT, "Idempotency-Key": "lost-1"},
    )
    created = client.post(
        f"/api/workers/jobs/{first['job_id']}/attempts",
        json={"worker_id": worker_id, "reason": "повтор",
              "source_attempt_id": old_attempt, "confirmation": "НОВАЯ ПОПЫТКА"},
        headers={**INTENT, "Idempotency-Key": "new-1"},
    ).json()
    new_attempt = created["attempt_id"]
    token_before = repositories.get_attempt(
        new_attempt, settings=center_env
    )["execution_token_sha256"]

    replay = client.post(
        "/api/v1/worker/jobs/next", json={"free_slots": 1, "wait_sec": 0},
        headers={**headers, "Idempotency-Key": "reuse-me"},
    )
    assert replay.status_code == 409
    assert replay.json()["detail"]["error"] == "idempotency_key_stale"

    token_after = repositories.get_attempt(
        new_attempt, settings=center_env
    )["execution_token_sha256"]
    assert token_after == token_before, "токен чужой попытки перевыпущен"


def test_lost_attempt_is_never_offered_to_the_worker_again(client, center_env):
    """Признанную потерянной попытку центр не выдаёт повторно (I-03)."""
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service, repositories

    worker_id, headers = _approved_worker(client)
    job_id, attempt_id, _ = _running_attempt(
        client, worker_id, headers, project="ПРОЕКТ/повторная выдача"
    )
    client.post(
        f"/api/workers/jobs/{job_id}/attempts/{attempt_id}/mark-lost",
        json={"mandatory_reason": "молчит", "typed_confirmation": "ПОПЫТКА ПОТЕРЯНА",
              "observed_worker_state": "offline"},
        headers={**INTENT, "Idempotency-Key": "lost-2"},
    )
    # Возвращаем состояние в assigned, как это сделал бы reoffer.
    repositories.update_attempt_fields(
        attempt_id, {"state": JobState.ASSIGNED.value}, settings=center_env
    )
    assert repositories.claim_next_job_for_worker(worker_id, settings=center_env) is None

    # И сам reoffer её тоже не трогает.
    assert job_service.reoffer_unknown_jobs(
        worker_id=worker_id, known_job_ids=set(), settings=center_env
    ) == []


def test_operator_cannot_declare_cancelled_once_worker_has_the_package(center_env):
    """`source_*` → `cancelled` напрямую нет (критерий 6).

    Граница уточнена на этапе ExecutionBackend (§2.1 задания). Раньше в этот
    список входило и `assigned`, но проверка была шире, чем обоснование:
    «пакет уже у воркера» верно начиная с `source_uploading`, а `assigned`
    означает «лежит в очереди центра». Единственный путь передачи работы —
    `claim_next_job_for_worker`, и он атомарно уводит попытку из `assigned` в
    той же транзакции, что и выборка. То есть попытка в `assigned` воркеру не
    выдавалась ни разу: процесса нет, подтверждать нечего, а WorkerCommand,
    который создавался раньше, был мусорным и держал слот (§32.1 п.24
    отчёта 05 — «3/2»).
    """
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service

    allowed = job_service.ALLOWED_TRANSITIONS
    for state in (JobState.SOURCE_UPLOADING, JobState.SOURCE_READY):
        assert JobState.CANCELLED not in allowed[state], (
            f"{state.value} → cancelled без подтверждения воркера"
        )
        assert JobState.CANCEL_REQUESTED in allowed[state]
    # Пакет ещё не выдавался — прямая отмена законна и является фактом.
    assert JobState.CANCELLED in allowed[JobState.CREATED]
    assert JobState.CANCELLED in allowed[JobState.ASSIGNED]
    assert JobState.CANCEL_REQUESTED in allowed[JobState.ASSIGNED]
    # Работающая попытка по-прежнему отменяется только через подтверждение.
    assert JobState.CANCELLED not in allowed[JobState.RUNNING]
    assert JobState.CANCELLED not in allowed[JobState.ACCEPTED_BY_WORKER]


# ═══ §4 Центр: доставка результата ═══════════════════════════════════════════
def test_result_is_deliverable_while_cancel_is_pending(center_env):
    """Догон состояния знает `cancel_requested`.

    Воркер доработал офлайн, отмена доехала позже. Раньше архив было не сдать:
    словарь путей догона не содержал `cancel_requested`, и попытка бесконечно
    получала 409.
    """
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service, repositories

    job = repositories.create_job(
        project_id="ПРОЕКТ/гонка отмены", version_id=None,
        job_type="test_pipeline_v1", payload={"params": {}},
        settings=center_env,
    )
    for state in (JobState.ASSIGNED, JobState.SOURCE_UPLOADING, JobState.SOURCE_READY,
                  JobState.ACCEPTED_BY_WORKER, JobState.RUNNING):
        repositories.update_attempt_fields(
            job["attempt_id"], {"state": state.value}, settings=center_env
        )
    repositories.update_attempt_fields(
        job["attempt_id"],
        {"state": JobState.CANCEL_REQUESTED.value},
        settings=center_env,
    )
    caught = job_service.catch_up_to_result_received(
        attempt_id=job["attempt_id"], settings=center_env
    )
    assert caught["state"] == JobState.RESULT_RECEIVED.value


def test_attempt_revoked_during_validation_is_not_published(center_env, tmp_path):
    """Оператор отозвал попытку, пока центр проверял её архив (I-07)."""
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service, repositories

    job = repositories.create_job(
        project_id="ПРОЕКТ/toctou", version_id=None,
        job_type="test_pipeline_v1", payload={"params": {}},
        settings=center_env,
    )
    repositories.update_attempt_fields(
        job["attempt_id"],
        {"state": JobState.VALIDATING.value,
         "attempt_disposition": "operator_declared_lost"},
        settings=center_env,
    )
    stale = repositories.get_attempt(job["attempt_id"], settings=center_env)
    archive = tmp_path / "result.tar.gz"
    archive.write_bytes("неважно".encode("utf-8"))
    with pytest.raises(job_service.AttemptNoLongerActive):
        job_service.finalize_result(
            job=stale, archive=archive, expected_hash="0" * 64,
            expected_size=archive.stat().st_size, settings=center_env,
        )
    assert archive.exists(), "архив не должен исчезнуть при отказе публикации"


def test_migrated_legacy_attempt_can_still_store_its_result(center_env, tmp_path):
    """Мигрированный `att_legacy1` — валидный ключ пути ЗАПИСИ, а не 500."""
    from backend.app.services.distributed_workers import job_service

    from backend.app.services.distributed_workers import identifiers

    # Все три пути ЗАПИСИ обязаны принимать ключ этапа 0.
    for root in (center_env.superseded_results_dir, center_env.rejected_results_dir,
                 center_env.validated_results_dir):
        target = identifiers.attempt_dir(
            root, str(uuid.uuid4()), "att_legacy1", allow_legacy=True
        )
        assert target.name == "att_legacy1"
    # А внешний код проекта не проходит ни при каком послаблении.
    with pytest.raises(identifiers.UnsafeIdentifier):
        identifiers.attempt_dir(
            center_env.validated_results_dir, str(uuid.uuid4()),
            "13АВ/РД-АР3-К7", allow_legacy=True,
        )
    assert job_service.store_unpublished_result is not None


def test_poisoned_resource_snapshot_via_resources_endpoint(client, center_env):
    """`POST /resources` тоже обязан санировать снимок, а не только heartbeat."""
    worker_id, headers = _approved_worker(client)
    for poison in ({"executor": "PWNED"}, {"warnings": 5},
                   {"executor": {"status": "online", "seen_at": "не число"}}):
        assert client.post(
            "/api/v1/worker/resources", json=poison, headers=headers
        ).status_code == 200
        listing = client.get("/api/workers")
        assert listing.status_code == 200, listing.text
        single = client.get(f"/api/workers/{worker_id}")
        assert single.status_code == 200, single.text


# ═══ §5 Операторские действия ════════════════════════════════════════════════
def test_idempotency_key_is_bound_to_the_attempt(client, center_env):
    """Тот же ключ на ДРУГОЙ попытке не выдаёт чужой результат за свой."""
    worker_id, headers = _approved_worker(client)
    job_a, attempt_a, _ = _running_attempt(
        client, worker_id, headers, project="ПРОЕКТ/ключ A"
    )
    first = client.post(
        f"/api/workers/jobs/{job_a}/attempts/{attempt_a}/mark-lost",
        json={"mandatory_reason": "нет связи",
              "typed_confirmation": "ПОПЫТКА ПОТЕРЯНА",
              "observed_worker_state": "offline"},
        headers={**INTENT, "Idempotency-Key": "one-key"},
    )
    assert first.status_code == 200
    assert first.json().get("replayed") is not True

    job_b, attempt_b, _ = _running_attempt(
        client, worker_id, headers, project="ПРОЕКТ/ключ B"
    )
    second = client.post(
        f"/api/workers/jobs/{job_b}/attempts/{attempt_b}/mark-lost",
        json={"mandatory_reason": "нет связи",
              "typed_confirmation": "ПОПЫТКА ПОТЕРЯНА",
              "observed_worker_state": "offline"},
        headers={**INTENT, "Idempotency-Key": "one-key"},
    )
    assert second.status_code == 200
    assert second.json().get("replayed") is not True, "выдан ответ ЧУЖОЙ попытки"
    # Действие обязано было ВЫПОЛНИТЬСЯ, а не вернуть чужой ответ.
    fresh = client.get(f"/api/workers/jobs/{job_b}/attempts").json()["attempts"]
    assert fresh[0]["attempt_disposition"] == "operator_declared_lost"


def test_cancel_ack_effect_applies_even_on_replay(client, center_env):
    """Центр упал между записью ACK и применением эффекта — отмена не зависает."""
    from backend.app.services.distributed_workers import repositories

    worker_id, headers = _approved_worker(client)
    job_id, attempt_id, token = _running_attempt(
        client, worker_id, headers, project="ПРОЕКТ/повтор ACK"
    )
    client.post(
        f"/api/workers/jobs/{job_id}/attempts/{attempt_id}/cancel",
        json={"reason": "хватит", "confirmation": "ОТМЕНИТЬ",
              "grace_period_sec": 5},
        headers={**INTENT, "Idempotency-Key": "cancel-replay"},
    )
    command = client.post(
        "/api/v1/worker/commands/next", json={"wait_sec": 0}, headers=headers
    ).json()["commands"][0]
    body = {"result": {"status": "ok", "detail": {"outcome": "cancelled"}},
            "acknowledged_at": time.time()}

    first_raw = client.post(
        f"/api/v1/worker/commands/{command['command_id']}/ack",
        json=body, headers=headers,
    )
    assert first_raw.status_code == 200, first_raw.text
    first = first_raw.json()
    assert first["attempt_state"] == "cancelled", first

    # Имитируем «эффект не применился»: возвращаем попытку в cancel_requested.
    repositories.update_attempt_fields(
        attempt_id, {"state": "cancel_requested"}, settings=center_env
    )
    second = client.post(
        f"/api/v1/worker/commands/{command['command_id']}/ack",
        json=body, headers=headers,
    ).json()
    assert second["replayed"] is True
    assert second["attempt_state"] == "cancelled", (
        "повторный ACK обязан довести эффект: иначе попытка застревает навсегда"
    )


def test_repeat_deletion_request_enqueues_a_fresh_command(client, center_env):
    """Ключ команды удаления со счётчиком, а не фиксированный."""
    from backend.app.services.distributed_workers import repositories

    worker_id, headers = _approved_worker(client)
    job_id, attempt_id, _ = _running_attempt(
        client, worker_id, headers, project="ПРОЕКТ/удаление"
    )
    client.post(
        f"/api/workers/jobs/{job_id}/attempts/{attempt_id}/cancel",
        json={"reason": "стоп", "confirmation": "ОТМЕНИТЬ"},
        headers={**INTENT, "Idempotency-Key": "del-cancel"},
    )
    command = client.post(
        "/api/v1/worker/commands/next", json={"wait_sec": 0}, headers=headers
    ).json()["commands"][0]
    client.post(
        f"/api/v1/worker/commands/{command['command_id']}/ack",
        json={"result": {"status": "ok", "detail": {"outcome": "cancelled"}},
              "acknowledged_at": time.time()},
        headers=headers,
    )

    def _request(key: str):
        return client.post(
            f"/api/workers/jobs/{job_id}/attempts/{attempt_id}/request-deletion",
            json={"reason": "чистим диск",
                  "confirmation": "УДАЛИТЬ ДАННЫЕ"},
            headers={**INTENT, "Idempotency-Key": key},
        )

    first = _request("del-1")
    assert first.status_code == 200, first.text
    first_command = first.json()["command_id"]
    client.post(
        f"/api/v1/worker/commands/{first_command}/ack",
        json={"result": {"status": "ok", "detail": {"outcome": "deleted"}},
              "acknowledged_at": time.time()},
        headers=headers,
    )
    second = _request("del-2")
    assert second.status_code == 200, second.text
    assert second.json()["command_id"] != first_command, (
        "повтор вернул старую команду под видом свежепоставленной"
    )
    keys = {
        c["idempotency_key"]
        for c in repositories.commands_for_job(job_id, settings=center_env)
        if c["command_type"] == "delete_attempt_data"
    }
    assert len(keys) == 2


def test_worker_management_endpoints_require_intent_header(client, center_env):
    """Одобрить/отклонить/отозвать/создать задание — тоже за CSRF-рубежом."""
    worker_id, _ = _approved_worker(client)
    client.headers.pop("X-Requested-With", None)
    try:
        for path, body in (
            (f"/api/workers/{worker_id}/approve", {"configured_max_slots": 1}),
            (f"/api/workers/{worker_id}/reject", {}),
            (f"/api/workers/{worker_id}/revoke", {}),
            ("/api/workers/jobs", {"worker_id": worker_id, "project_id": "x",
                                   "params": {"label": "l", "steps": 1,
                                              "step_seconds": 0.0}}),
        ):
            assert client.post(path, json=body).status_code == 403, path
    finally:
        client.headers["X-Requested-With"] = "audit-workers"


def test_second_active_job_for_project_is_409_not_500(client, center_env):
    """Конфликт по индексу проекта — тоже ответ оператору, а не 500."""
    from backend.app.services.distributed_workers import repositories

    worker_id, headers = _approved_worker(client)
    job = _create_job(client, worker_id, project="ПРОЕКТ/один активный")
    with pytest.raises(repositories.ActiveAttemptExists):
        repositories.create_next_attempt(
            job_id=job["job_id"], worker_id=worker_id, settings=center_env
        )


# ═══ §6 Воркер: очередь, команды, хранение ═══════════════════════════════════
def test_grace_period_is_clamped(tmp_path):
    """`grace_period_sec: 1e9` не должен останавливать главный цикл навсегда."""
    from audit_worker.executor import MAX_GRACE_SEC, _grace_period

    assert _grace_period(1e9) == MAX_GRACE_SEC
    assert _grace_period("не число") == 30.0
    assert _grace_period(None) == 30.0
    assert _grace_period(float("inf")) == 30.0
    assert _grace_period(-5) == 0.0
    assert _grace_period(12) == 12.0


def test_orphan_local_commands_return_to_the_queue(tmp_path):
    """Команда, застрявшая в `processing` после смерти исполнителя."""
    from audit_worker import local_db

    db = local_db.LocalDB(tmp_path / "worker.db")
    db.enqueue_local_command(
        command_type="cancel_attempt", job_id="j", attempt_id="a",
        payload={"job_id": "j", "attempt_id": "a"}, central_command_id="cmd_1",
    )
    claimed = db.claim_local_command()
    assert claimed is not None
    assert db.claim_local_command() is None          # вторая выдача запрещена

    assert db.requeue_orphan_commands() == 1
    again = db.claim_local_command()
    assert again is not None
    assert again["local_command_id"] == claimed["local_command_id"]


def test_recovery_does_not_steal_attempts_of_a_live_executor(tmp_path):
    """Второй исполнитель не становится вторым наблюдателем чужого процесса."""
    from audit_worker import local_db

    db = local_db.LocalDB(tmp_path / "worker.db")
    alive = db.register_executor(version="1.0")
    # Строка «этого» воплощения указывает на живой pid (наш собственный).
    assert db.executor_alive(alive) is False, "сам себя живым чужим не считает"

    other = local_db.LocalDB(tmp_path / "worker.db")
    assert other.executor_alive(alive) is True, (
        "чужой исполнитель с живым pid обязан считаться живым"
    )
    assert other.executor_alive("exe_never_existed") is False


def test_claimed_but_never_started_attempt_returns_to_the_queue(tmp_path):
    from audit_worker import local_db

    db = local_db.LocalDB(tmp_path / "worker.db")
    instance = db.register_executor(version="1.0")
    db.enqueue(job_id="j", attempt_id="a", job_type="test_pipeline_v1", params={})
    claimed = db.claim_next(instance)
    assert claimed is not None
    assert db.queue_item("a")["state"] == local_db.QUEUE_CLAIMED

    db.release_claim("a")
    row = db.queue_item("a")
    assert row["state"] == local_db.QUEUE_QUEUED
    assert row["claimed_by_executor"] is None


def test_retention_refuses_to_delete_the_jobs_root(tmp_path):
    """`target == root` было в РАЗРЕШАЮЩЕЙ части условия."""
    from audit_worker import local_db
    from audit_worker.config import WorkerConfig
    from audit_worker.retention import DeletionRefused, RetentionManager

    root = tmp_path / "worker"
    (root / "jobs").mkdir(parents=True)
    config = WorkerConfig(root=root, dispatcher_url="http://x", display_name="VPS")
    config.ensure_dirs()
    manager = RetentionManager(config, local_db.LocalDB(config.local_db_path))

    with pytest.raises(DeletionRefused):
        manager._safe_remove(config.jobs_dir, job_id="j", attempt_id="a")
    assert config.jobs_dir.is_dir(), "каталог заданий уцелел"


def test_job_metadata_with_execution_token_is_not_group_readable(tmp_path):
    """В metadata.json лежит execution_token — файл обязан быть 0600."""
    from audit_worker.local_store import LocalJobStore

    store = LocalJobStore(tmp_path / "jobs")
    job_id, attempt_id = str(uuid.uuid4()), str(uuid.uuid4())
    store.create({"job_id": job_id, "attempt_id": attempt_id,
                  "execution_token": "etk_секрет", "job_type": "test_pipeline_v1"})
    mode = stat.S_IMODE(store.meta_path(job_id, attempt_id).stat().st_mode)
    assert mode == 0o600, oct(mode)


def test_local_job_dir_rejects_unsafe_key(tmp_path):
    """Путь строится только из безопасного ключа — даже во внутреннем store."""
    from audit_worker.local_store import LocalJobStore
    from audit_worker.paths import UnsafeStorageKey

    store = LocalJobStore(tmp_path / "jobs")
    with pytest.raises(UnsafeStorageKey):
        store.job_dir("../../etc", "a")
    with pytest.raises(UnsafeStorageKey):
        store.job_dir(str(uuid.uuid4()), "13АВ/РД-АР3-К7")


def test_stuck_assembly_session_can_be_reclaimed(center_env):
    """Сборщик умер вместе с процессом — сессия не залипает навсегда."""
    from backend.app.services.distributed_workers import repositories

    job = repositories.create_job(
        project_id="ПРОЕКТ/аренда", version_id=None,
        job_type="test_pipeline_v1", payload={"params": {}},
        settings=center_env,
    )
    upload_id = str(uuid.uuid4())
    repositories.create_upload_session(
        upload_id=upload_id, job_id=job["job_id"], attempt_id=job["attempt_id"],
        package_type="result", expected_size=10, chunk_size=10,
        expected_hash="0" * 64, ttl_sec=3600, settings=center_env,
    )
    assert repositories.claim_upload_for_assembly(upload_id, settings=center_env)
    assert repositories.claim_upload_for_assembly(upload_id, settings=center_env) is None

    # Аренда истекла — «сборщик» не подаёт признаков жизни.
    with sqlite3.connect(center_env.db_path) as conn:
        conn.execute(
            "UPDATE upload_sessions SET assembly_started_at = ? WHERE upload_id = ?",
            (time.time() - repositories.ASSEMBLY_LEASE_SEC - 1, upload_id),
        )
    assert repositories.claim_upload_for_assembly(upload_id, settings=center_env)
