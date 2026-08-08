"""
test_distributed_workers_execution_backend.py
---------------------------------------------
Этап ExecutionBackend. Разделы файла соответствуют §31 задания:

  §1  предварительные исправления (§2 задания):
      отмена невыданной попытки, свежесть эффективного лимита,
      старт агента при недоступном центре, лимит частоты регистрации;
  §2  контракт ExecutionBackend и оба его воплощения;
  §3  интеграция с PipelineManager;
  §4  исходный пакет проекта и снимки конфигурации;
  §5  строгий audit_pipeline_v1 на воркере;
  §6  приём результата: staging, откат, идемпотентность;
  §7  безопасность.

Run: python -m pytest tests/test_distributed_workers_execution_backend.py -v
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

httpx = pytest.importorskip("httpx")

BOOTSTRAP = "exec-backend-bootstrap-secret-0123456789"
INTENT = {"X-Requested-With": "audit-workers"}


# ═══ Фикстуры ════════════════════════════════════════════════════════════════
@pytest.fixture()
def center_env(tmp_path, monkeypatch):
    from tests.distributed_workers_helpers import enable_portal_roles

    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "center"))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET", BOOTSTRAP)
    monkeypatch.setenv("DISTRIBUTED_WORKERS_UPLOAD_CHUNK_BYTES", "4096")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_LONG_POLL_SEC", "1")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ALLOW_INSECURE_ADMIN", "false")
    enable_portal_roles(monkeypatch)

    from backend.app.services.distributed_workers import database
    from backend.app.services.distributed_workers.settings import get_settings

    database.reset_state_for_tests()
    settings = get_settings()
    database.ensure_ready(settings)
    yield settings
    database.reset_state_for_tests()


def _client(username: str):
    from tests.distributed_workers_helpers import make_center_app, portal_client

    return portal_client(make_center_app(), username=username)


@pytest.fixture()
def admin(center_env):
    from tests.distributed_workers_helpers import ADMIN_USER

    return _client(ADMIN_USER)


@pytest.fixture()
def operator(center_env):
    from tests.distributed_workers_helpers import OPERATOR_USER

    return _client(OPERATOR_USER)


def _register(client, instance_id: str, *, secret: str = BOOTSTRAP):
    return client.post(
        "/api/v1/worker/register",
        json={"instance_id": instance_id, "protocol_version": 1,
              "display_name_hint": "VPS-exec"},
        headers={"Authorization": f"Bearer {secret}", "X-Protocol-Version": "1"},
    )


def _approved_worker(admin_client, instance_id="inst_exec_00001", max_slots=1):
    registered = _register(admin_client, instance_id).json()
    worker_id = registered["worker_id"]
    approved = admin_client.post(
        f"/api/workers/{worker_id}/approve",
        json={"configured_max_slots": max_slots},
    )
    assert approved.status_code == 200, approved.text
    token = admin_client.post(
        "/api/v1/worker/claim",
        json={"worker_id": worker_id, "instance_id": instance_id,
              "claim_secret": registered["claim_secret"]},
    ).json()["worker_token"]
    headers = {"Authorization": f"Bearer {token}", "X-Worker-Id": worker_id,
               "X-Protocol-Version": "1"}
    admin_client.post(
        "/api/v1/worker/heartbeat",
        json={"instance_id": instance_id, "sent_at": time.time(),
              "configured_max_slots": max_slots, "calculated_free_slots": max_slots,
              "max_verified_slots": 2,
              "executor": {"status": "online", "running_processes": 0}},
        headers=headers,
    )
    return worker_id, headers


def _create_job(client, worker_id, project="ИСП/проект 1"):
    response = client.post(
        "/api/workers/jobs",
        json={"worker_id": worker_id, "project_id": project,
              "params": {"label": "exec", "steps": 1, "step_seconds": 0.0}},
    )
    assert response.status_code == 200, response.text
    return response.json()["job"]


def _key() -> dict[str, str]:
    return {**INTENT, "Idempotency-Key": f"exec-{uuid.uuid4().hex[:12]}"}


def _cancel(client, job, key=None):
    return client.post(
        f"/api/workers/jobs/{job['job_id']}/attempts/{job['attempt_id']}/cancel",
        json={"reason": "проверка", "confirmation": "ОТМЕНИТЬ"},
        headers=key or _key(),
    )


# ═══ §1.1 Отмена ещё не выданной попытки (§2.1 задания) ══════════════════════
def test_assigned_cancel_is_terminal_and_creates_no_command(admin, operator, center_env):
    """`assigned` → `cancelled` напрямую, без WorkerCommand и без занятия слота."""
    from backend.app.services.distributed_workers import repositories

    worker_id, _ = _approved_worker(admin, instance_id="inst_exec_cancel1")
    job = _create_job(admin, worker_id, project="ИСП/отмена до выдачи")

    response = _cancel(operator, job)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["outcome"] == "cancelled_before_dispatch"
    assert body["state"] == "cancelled"
    assert body["command_id"] is None
    assert body["command_status"] == "not_required"

    assert repositories.commands_for_job(job["job_id"], settings=center_env) == []
    attempt = repositories.get_attempt(job["attempt_id"], settings=center_env)
    assert attempt["state"] == "cancelled"
    assert attempt["attempt_disposition"] == "cancelled"


def test_assigned_cancel_frees_the_slot(admin, operator, center_env):
    """Отменённая до выдачи попытка слот не занимает — счётчик остаётся честным."""
    from backend.app.services.distributed_workers import repositories, slots

    worker_id, _ = _approved_worker(admin, instance_id="inst_exec_cancel2", max_slots=2)
    for idx in range(3):
        job = _create_job(admin, worker_id, project=f"ИСП/слот {idx}")
        assert _cancel(operator, job).status_code == 200

    usage = repositories.worker_slot_snapshot(worker_id, settings=center_env)
    row = repositories.get_worker(worker_id, settings=center_env)
    view = slots.build_slot_view(row, usage, slots.effective_limit(row))
    assert usage.occupied == 0
    assert view["occupancy_label"] == "0/2"
    assert view["center_free_slots"] == 2


def test_assigned_cancel_is_idempotent(admin, operator, center_env):
    """Повтор с тем же ключом возвращает записанный результат, второй раз ничего не делает."""
    from backend.app.services.distributed_workers import repositories

    worker_id, _ = _approved_worker(admin, instance_id="inst_exec_cancel3")
    job = _create_job(admin, worker_id, project="ИСП/идемпотентная отмена")
    key = _key()

    first = _cancel(operator, job, key=key)
    second = _cancel(operator, job, key=key)
    assert first.status_code == 200 and second.status_code == 200
    assert second.json().get("replayed") is True
    assert second.json()["state"] == "cancelled"
    assert repositories.commands_for_job(job["job_id"], settings=center_env) == []


def test_dispatched_attempt_still_uses_cancel_requested(admin, operator, center_env):
    """Как только пакет выдан воркеру, отмена снова становится просьбой."""
    from backend.app.services.distributed_workers import repositories

    worker_id, headers = _approved_worker(admin, instance_id="inst_exec_cancel4")
    job = _create_job(admin, worker_id, project="ИСП/выдано")
    claimed = repositories.claim_next_job_for_worker(worker_id, settings=center_env)
    assert claimed is not None and claimed["state"] == "source_uploading"

    response = _cancel(operator, job)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["outcome"] == "cancel_requested"
    assert body["state"] == "cancel_requested"
    assert body["command_id"]
    assert len(repositories.commands_for_job(job["job_id"], settings=center_env)) == 1


def test_cancel_loses_race_to_dispatch_and_does_not_lie(admin, operator, center_env):
    """Гонка «отмена против выдачи» разрешается транзакционно, а не догадкой.

    Выдача успевает первой → отмена видит уже выданную попытку и уходит на
    обычный путь `cancel_requested` с командой воркеру. «Отменено» без
    подтверждения не появляется ни в одном из исходов.
    """
    from backend.app.services.distributed_workers import repositories

    worker_id, _ = _approved_worker(admin, instance_id="inst_exec_race1")
    job = _create_job(admin, worker_id, project="ИСП/гонка")
    repositories.claim_next_job_for_worker(worker_id, settings=center_env)

    body = _cancel(operator, job).json()
    assert body["state"] == "cancel_requested"


def test_dispatch_loses_race_and_returns_nothing(admin, operator, center_env):
    """Отмена успела первой → выдача не отдаёт отменённую попытку воркеру."""
    from backend.app.services.distributed_workers import repositories

    worker_id, _ = _approved_worker(admin, instance_id="inst_exec_race2")
    job = _create_job(admin, worker_id, project="ИСП/гонка 2")
    assert _cancel(operator, job).status_code == 200

    assert repositories.claim_next_job_for_worker(
        worker_id, settings=center_env
    ) is None
    attempt = repositories.get_attempt(job["attempt_id"], settings=center_env)
    assert attempt["state"] == "cancelled"


# ═══ §1.2 Свежесть эффективного лимита (§2.2 задания) ════════════════════════
def test_revoked_worker_gets_no_work_inside_claim(admin, center_env):
    """Отзыв доступа виден захвату немедленно, а не со следующего вызова."""
    from backend.app.services.distributed_workers import repositories

    worker_id, _ = _approved_worker(admin, instance_id="inst_exec_revoke")
    _create_job(admin, worker_id, project="ИСП/отзыв")
    assert admin.post(
        f"/api/workers/{worker_id}/revoke", json={}, headers=INTENT
    ).status_code == 200

    with pytest.raises(repositories.SlotLimitReached) as excinfo:
        repositories.claim_next_job_for_worker(worker_id, settings=center_env)
    assert "отозван" in str(excinfo.value).lower()


def test_offline_agent_gets_no_work_by_current_time(admin, center_env, monkeypatch):
    """Связь считается по ТЕКУЩЕМУ времени центра, а не по колонке heartbeat."""
    from backend.app.services.distributed_workers import repositories

    worker_id, _ = _approved_worker(admin, instance_id="inst_exec_offline")
    _create_job(admin, worker_id, project="ИСП/офлайн")
    # Двигаем последний heartbeat в прошлое: колонка connection_status
    # по-прежнему говорит «online», а фактически воркер молчит.
    repositories.update_worker_fields(
        worker_id, {"last_seen_at": time.time() - 10_000}, settings=center_env
    )
    row = repositories.get_worker(worker_id, settings=center_env)
    assert row["connection_status"] == "online"      # снимок устарел

    with pytest.raises(repositories.SlotLimitReached):
        repositories.claim_next_job_for_worker(worker_id, settings=center_env)


def test_executor_offline_hint_blocks_claim(admin, center_env):
    """Состояние исполнителя из ЗАПРОСА участвует в решении внутри транзакции."""
    from backend.app.services.distributed_workers import repositories

    worker_id, _ = _approved_worker(admin, instance_id="inst_exec_exoff")
    _create_job(admin, worker_id, project="ИСП/исполнитель офлайн")

    with pytest.raises(repositories.SlotLimitReached):
        repositories.claim_next_job_for_worker(
            worker_id, executor_status_hint="offline", settings=center_env
        )
    # Без подсказки тот же вызов работу отдаёт.
    assert repositories.claim_next_job_for_worker(
        worker_id, settings=center_env
    ) is not None


def test_jobs_next_no_longer_passes_stale_limit(center_env):
    """Маршрут `/jobs/next` не передаёт заранее посчитанный лимит.

    Проверка машинная: `limit_override` в вызове захвата — это ровно тот
    снимок, который жил до конца long-poll (§32.1 п.26 отчёта 05).
    """
    source = (
        _ROOT / "backend" / "app" / "api" / "routers" / "audit_worker_agent.py"
    ).read_text(encoding="utf-8")
    head, _, tail = source.partition("async def jobs_next(")
    body = tail.split("\n@router.")[0]
    assert "claim_next_job_for_worker" in body
    assert "limit_override" not in body, (
        "jobs_next снова передаёт устаревший лимит в захват"
    )
    assert "executor_status_hint" in body


# ═══ §1.3 Старт агента при недоступном центре (§2.3 задания) ═════════════════
def test_center_failures_are_classified_separately():
    """TLS, авторизация и протокол не маскируются под обычный обрыв."""
    import ssl

    from audit_worker import registration
    from audit_worker.client import CenterError

    assert registration.classify_center_failure(
        httpx.ConnectError("connection refused")
    ) == registration.CENTER_UNREACHABLE
    assert registration.classify_center_failure(
        ssl.SSLError("certificate verify failed")
    ) == registration.CENTER_TLS_ERROR
    assert registration.classify_center_failure(
        CenterError(401, "revoked")
    ) == registration.CENTER_AUTH_ERROR
    assert registration.classify_center_failure(
        CenterError(426, "protocol")
    ) == registration.CENTER_PROTOCOL_ERROR
    assert registration.classify_center_failure(
        CenterError(500, "boom")
    ) == registration.CENTER_UNREACHABLE


def test_unknown_exception_is_not_swallowed_as_network():
    """Чужая ошибка пробрасывается: классификатор не глотает всё подряд."""
    from audit_worker import registration

    with pytest.raises(ValueError):
        registration.classify_center_failure(ValueError("не сетевая"))


def test_agent_starts_when_center_is_unreachable(tmp_path):
    """`ensure_registered` с токеном на диске не падает при мёртвом центре.

    Это и есть закрытие §32.10 отчёта 05: раньше `httpx.ConnectError` уходил
    наружу, процесс завершался с traceback, и под systemd Restart=always это
    давало крэш-луп при живых процессах аудита.
    """
    from audit_worker.config import WorkerConfig
    from audit_worker.local_store import WorkerStateStore
    from audit_worker.registration import CENTER_UNREACHABLE, ensure_registered

    root = tmp_path / "worker"
    root.mkdir()
    config = WorkerConfig(
        dispatcher_url="http://127.0.0.1:1",     # заведомо мёртвый порт
        root=root,
        display_name="dead-center",
        request_timeout_sec=1.0,
        allow_insecure_localhost=True,
    )
    config.ensure_dirs()
    store = WorkerStateStore(config.state_path, config.token_path)
    store.save({"worker_id": "wrk_dead0001"})
    store.write_token("wtk_test_token_value")

    identity = ensure_registered(config)

    assert identity["worker_id"] == "wrk_dead0001"
    assert identity["token"] == "wtk_test_token_value"
    assert identity["center_state"] == CENTER_UNREACHABLE
    # Состояние сохранено на диск — следующий запуск знает, что было.
    assert store.load()["center_state"] == CENTER_UNREACHABLE


def test_agent_records_recovery_of_connection(tmp_path):
    """Агент отличает восстановление связи от продолжающегося обрыва."""
    from audit_worker.agent import WorkerAgent

    agent = object.__new__(WorkerAgent)
    agent.center_state = "online"
    agent._on_center_error(httpx.ConnectError("down"))
    assert agent.center_state == "center_unreachable"
    agent._on_center_ok()
    assert agent.center_state == "online"


# ═══ §1.4 Лимит частоты регистрации (§2.4 задания) ═══════════════════════════
def test_registration_rate_limit_returns_429_with_retry_after(admin, center_env, monkeypatch):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_INSTANCE", "2")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_IP", "100")

    ok1 = _register(admin, "inst_rate_0001")
    ok2 = _register(admin, "inst_rate_0001")
    blocked = _register(admin, "inst_rate_0001")

    assert ok1.status_code == 201 and ok2.status_code == 201
    assert blocked.status_code == 429
    assert int(blocked.headers["Retry-After"]) > 0


def test_registration_rate_limit_has_separate_ip_budget(admin, center_env, monkeypatch):
    """Смена instance_id не обходит лимит: отдельный счётчик по адресу."""
    monkeypatch.setenv("DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_INSTANCE", "100")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_IP", "3")

    codes = [_register(admin, f"inst_ip_{i:05d}").status_code for i in range(5)]
    assert codes[:3] == [201, 201, 201]
    assert codes[3:] == [429, 429]


def test_registration_rate_limit_counts_wrong_secret(admin, center_env, monkeypatch):
    """Неверный секрет тоже списывает попытку — иначе перебор не ограничен."""
    monkeypatch.setenv("DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_INSTANCE", "2")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_IP", "100")

    first = _register(admin, "inst_rate_brute", secret="wrong-secret-0123456789")
    second = _register(admin, "inst_rate_brute", secret="wrong-secret-0123456789")
    third = _register(admin, "inst_rate_brute")

    assert first.status_code == 401 and second.status_code == 401
    assert third.status_code == 429


def test_registration_rate_limit_does_not_leak_existence(admin, center_env, monkeypatch):
    """Ответ 429 одинаков для известного и неизвестного instance_id."""
    monkeypatch.setenv("DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_INSTANCE", "1")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_IP", "100")

    known = _register(admin, "inst_known_0001")
    assert known.status_code == 201
    blocked_known = _register(admin, "inst_known_0001")
    blocked_unknown = _register(admin, "inst_unknown_9999")
    _register(admin, "inst_unknown_9999")
    blocked_unknown2 = _register(admin, "inst_unknown_9999")

    assert blocked_known.status_code == 429
    assert blocked_unknown.status_code == 201       # первая попытка своей корзины
    assert blocked_unknown2.status_code == 429
    assert blocked_known.json()["detail"] == blocked_unknown2.json()["detail"]


def test_registration_rate_limit_survives_restart(center_env, admin, monkeypatch):
    """Счётчик персистентный: сброс кэшей соединений его не обнуляет."""
    from backend.app.services.distributed_workers import database, rate_limit

    monkeypatch.setenv("DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_INSTANCE", "1")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_IP", "100")

    assert _register(admin, "inst_persist_001").status_code == 201
    stored = rate_limit.snapshot(settings=center_env)
    assert stored, "счётчик не сохранился в workers.db"

    # Эмуляция рестарта центра: соединения закрыты, отметки миграций сброшены.
    database.reset_state_for_tests()
    fresh = _client_admin()
    assert _register(fresh, "inst_persist_001").status_code == 429


def _client_admin():
    from tests.distributed_workers_helpers import ADMIN_USER

    return _client(ADMIN_USER)


def test_registration_rate_limit_stores_only_hashes(center_env, admin, monkeypatch):
    """В базе не лежит ни IP, ни instance_id открытым текстом."""
    from backend.app.services.distributed_workers import rate_limit

    monkeypatch.setenv("DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_INSTANCE", "5")
    assert _register(admin, "inst_secret_name_42").status_code == 201
    rows = rate_limit.snapshot(settings=center_env)
    blob = json.dumps(rows, ensure_ascii=False)
    assert "inst_secret_name_42" not in blob
    assert all(len(row["key"]) == 64 for row in rows)


def test_registration_rate_limit_can_be_disabled(center_env, admin, monkeypatch):
    """Оба нуля выключают ограничитель явно — молчаливого «выключено» нет."""
    monkeypatch.setenv("DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_INSTANCE", "0")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_IP", "0")

    codes = [_register(admin, "inst_nolimit_01").status_code for _ in range(6)]
    assert set(codes) == {201}


def test_registration_rate_limit_does_not_double_charge_one_request(center_env, monkeypatch):
    """Отказ по второй корзине не списывает первую: проверка идёт до инкремента."""
    from backend.app.services.distributed_workers import rate_limit

    monkeypatch.setenv("DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_INSTANCE", "5")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_REGISTRATION_RATE_MAX_PER_IP", "1")
    from backend.app.services.distributed_workers.settings import get_settings

    settings = get_settings()
    assert rate_limit.check_and_consume(
        source_ip="10.0.0.1", instance_id="a", settings=settings
    ).allowed
    blocked = rate_limit.check_and_consume(
        source_ip="10.0.0.1", instance_id="b", settings=settings
    )
    assert not blocked.allowed and blocked.scope == rate_limit.SCOPE_IP

    rows = {(r["scope"], r["key"]): r["count"] for r in rate_limit.snapshot(settings=settings)}
    per_instance = [c for (scope, _), c in rows.items() if scope == rate_limit.SCOPE_IP_INSTANCE]
    # Ровно одна корзина пары и ровно одно списание: отказ по IP не должен
    # оставлять «съеденную» квоту instance_id.
    assert per_instance == [1]


# ═══ §2 Контракт ExecutionBackend ════════════════════════════════════════════
CONTRACT_METHODS = (
    "prepare", "start", "status", "wait", "cancel", "liveness", "reattach",
    "collect_result",
)


def test_contract_declares_eight_operations():
    from backend.app.pipeline.execution.contracts import ExecutionBackend

    for name in CONTRACT_METHODS:
        assert callable(getattr(ExecutionBackend, name, None)), name


def test_both_backends_implement_the_contract():
    from backend.app.pipeline.execution.local import LocalExecutionBackend
    from backend.app.pipeline.execution.remote import RemoteWorkerExecutionBackend

    for cls in (LocalExecutionBackend, RemoteWorkerExecutionBackend):
        for name in CONTRACT_METHODS:
            own = getattr(cls, name, None)
            base = getattr(
                __import__(
                    "backend.app.pipeline.execution.contracts",
                    fromlist=["ExecutionBackend"],
                ).ExecutionBackend,
                name,
            )
            assert own is not base, f"{cls.__name__}.{name} не реализован"


def test_request_forbids_command_and_path_fields():
    """Через контракт нельзя передать команду, argv, env или путь."""
    import pydantic

    from backend.app.pipeline.execution.contracts import ExecutionRequest

    for field in ("command", "argv", "env", "cwd", "executable", "script", "module"):
        with pytest.raises(pydantic.ValidationError):
            ExecutionRequest(project_id="p", job_id="j", **{field: "x"})


def test_audit_options_forbid_unknown_fields():
    import pydantic

    from backend.app.pipeline.execution.contracts import AuditExecutionOptions

    with pytest.raises(pydantic.ValidationError):
        AuditExecutionOptions(action="full", shell="rm -rf /")


class _FakeJob:
    def __init__(self):
        from backend.app.models.audit import AuditStage, JobStatus

        self.job_id = "job-1"
        self.project_id = "ПРО/ект 1"
        self.version_id = "v002"
        self.object_id = None
        self.status = JobStatus.COMPLETED
        self.stage = AuditStage.PREPARE
        self.progress_current = 0
        self.progress_total = 0
        self.error_message = None
        self.completed_at = None


class _RecordingManager:
    """Минимальный двойник менеджера: фиксирует, чем его позвали."""

    def __init__(self):
        self.calls: list[dict] = []
        self.active_jobs: dict = {}
        self._tasks: dict = {}
        self._batch_queue = None

    async def _dispatch_action(self, item, job, *, default_action="full",
                               action_override=None):
        self.calls.append(
            {
                "item": item, "job": job,
                "default_action": default_action,
                "action_override": action_override,
            }
        )

    def _persist_queue(self):
        return None

    async def cancel(self, project_id):
        self.calls.append({"cancel": project_id})
        return True


def test_local_backend_delegates_with_identical_arguments():
    """LocalExecutionBackend вызывает прежний `_dispatch_action` один раз и как раньше."""
    import asyncio

    from backend.app.models.audit import BatchQueueItem
    from backend.app.pipeline.execution.contracts import (
        ExecutionContext,
        ExecutionRequest,
    )
    from backend.app.pipeline.execution.local import LocalExecutionBackend

    manager = _RecordingManager()
    backend = LocalExecutionBackend(manager)
    item = BatchQueueItem(project_id="ПРО/ект 1", version_id="v002", action="full")
    job = _FakeJob()
    ctx = ExecutionContext(item=item, job=job, default_action="audit",
                           action_override="resume")
    request = ExecutionRequest(project_id=job.project_id, version_id="v002",
                               job_id=job.job_id)

    result = asyncio.run(backend.run(request, ctx))

    assert len(manager.calls) == 1
    call = manager.calls[0]
    assert call["item"] is item and call["job"] is job
    assert call["default_action"] == "audit"
    assert call["action_override"] == "resume"
    assert result.success is True and result.cancelled is False


def test_local_backend_prepare_has_no_side_effects():
    import asyncio

    from backend.app.models.audit import BatchQueueItem
    from backend.app.pipeline.execution.contracts import (
        ExecutionContext,
        ExecutionMode,
        ExecutionRequest,
    )
    from backend.app.pipeline.execution.local import LocalExecutionBackend

    manager = _RecordingManager()
    backend = LocalExecutionBackend(manager)
    item = BatchQueueItem(project_id="p")
    ctx = ExecutionContext(item=item, job=_FakeJob())
    handle = asyncio.run(
        backend.prepare(ExecutionRequest(project_id="p", job_id="j"), ctx)
    )
    assert handle.backend_type is ExecutionMode.LOCAL
    assert manager.calls == []
    assert item.execution_handle == {}


def test_local_backend_reattach_is_noop():
    import asyncio

    from backend.app.pipeline.execution.contracts import (
        ExecutionHandle,
        ExecutionMode,
    )
    from backend.app.pipeline.execution.local import LocalExecutionBackend

    backend = LocalExecutionBackend(_RecordingManager())
    handle = ExecutionHandle(
        backend_type=ExecutionMode.LOCAL, handle_id="j", project_id="p"
    )
    assert asyncio.run(backend.reattach(handle)) is None


def test_remote_backend_never_calls_local_dispatch():
    """E-02 машинно: в remote.py нет ВЫЗОВА `_dispatch_action` и запуска процессов.

    Проверяется дерево разбора, а не текст: упоминание в докстринге («этот
    backend не зовёт `_dispatch_action`») — это объяснение границы, а не её
    нарушение, и текстовый греп на нём ложно срабатывал.
    """
    import ast

    tree = ast.parse(
        (_ROOT / "backend" / "app" / "pipeline" / "execution" / "remote.py")
        .read_text(encoding="utf-8")
    )
    banned_attrs = {"_dispatch_action", "system", "Popen", "run_script"}
    banned_names = {"subprocess", "paramiko", "pexpect"}
    offenders: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr in banned_attrs:
            offenders.append(f"{node.lineno}: .{node.attr}")
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = [a.name.split(".")[0] for a in (node.names or [])]
            module = (getattr(node, "module", "") or "").split(".")[0]
            for candidate in [*names, module]:
                if candidate in banned_names:
                    offenders.append(f"{node.lineno}: import {candidate}")
    assert not offenders, offenders
    # Токен воркера remote backend не читает: этим занимается только агент.
    for banned in ("read_token", "worker_token", "claim_secret"):
        assert banned not in ast.dump(tree), banned


def test_remote_backend_reuses_existing_handle(center_env, admin, monkeypatch):
    """Повторный prepare не создаёт второе задание (E-04, E-05)."""
    import asyncio

    from backend.app.models.audit import BatchQueueItem
    from backend.app.pipeline.execution.contracts import (
        ExecutionContext,
        ExecutionHandle,
        ExecutionMode,
        ExecutionRequest,
    )
    from backend.app.pipeline.execution.remote import RemoteWorkerExecutionBackend
    from backend.app.services.distributed_workers import repositories

    monkeypatch.setenv("DISTRIBUTED_AUDIT_EXECUTION_ENABLED", "true")
    worker_id, _ = _approved_worker(admin, instance_id="inst_exec_reuse")
    job = _create_job(admin, worker_id, project="ИСП/повторный prepare")

    item = BatchQueueItem(
        project_id="ИСП/повторный prepare",
        execution_mode="remote_worker",
        worker_id=worker_id,
        execution_handle=ExecutionHandle(
            backend_type=ExecutionMode.REMOTE_WORKER,
            handle_id=job["attempt_id"],
            project_id="ИСП/повторный prepare",
            attempt_id=job["attempt_id"],
            remote_job_id=job["job_id"],
            worker_id=worker_id,
        ).model_dump(),
    )
    backend = RemoteWorkerExecutionBackend(_RecordingManager())
    ctx = ExecutionContext(item=item, job=_FakeJob())
    handle = asyncio.run(
        backend.prepare(
            ExecutionRequest(
                project_id="ИСП/повторный prepare", job_id="job-1",
                execution_mode=ExecutionMode.REMOTE_WORKER,
                assigned_worker_id=worker_id,
            ),
            ctx,
        )
    )
    assert handle.attempt_id == job["attempt_id"]
    assert len(repositories.list_jobs(worker_id=worker_id, settings=center_env)) == 1


def test_remote_backend_requires_explicit_worker(center_env, monkeypatch):
    import asyncio

    from backend.app.models.audit import BatchQueueItem
    from backend.app.pipeline.execution.contracts import (
        ExecutionContext,
        ExecutionError,
        ExecutionMode,
        ExecutionRequest,
    )
    from backend.app.pipeline.execution.remote import RemoteWorkerExecutionBackend

    monkeypatch.setenv("DISTRIBUTED_AUDIT_EXECUTION_ENABLED", "true")
    backend = RemoteWorkerExecutionBackend(_RecordingManager())
    ctx = ExecutionContext(item=BatchQueueItem(project_id="p"), job=_FakeJob())
    with pytest.raises(ExecutionError):
        asyncio.run(
            backend.prepare(
                ExecutionRequest(
                    project_id="p", job_id="j",
                    execution_mode=ExecutionMode.REMOTE_WORKER,
                ),
                ctx,
            )
        )


def test_remote_liveness_never_reports_dead_on_offline(center_env, admin):
    """E-08: потеря связи не превращается в «мертво»."""
    import asyncio

    from backend.app.pipeline.execution.contracts import (
        ExecutionHandle,
        ExecutionMode,
        Liveness,
    )
    from backend.app.pipeline.execution.remote import RemoteWorkerExecutionBackend
    from backend.app.services.distributed_workers import repositories

    worker_id, _ = _approved_worker(admin, instance_id="inst_exec_live")
    job = _create_job(admin, worker_id, project="ИСП/живость")
    repositories.claim_next_job_for_worker(worker_id, settings=center_env)
    # Связь берётся из строки ВОРКЕРА (`last_seen_at`), а не из колонки
    # `job_attempts.connectivity_state`: у той нет ни одного писателя, и
    # опираться на неё значило бы всегда рапортовать «связь есть».
    repositories.update_worker_fields(
        worker_id,
        {"last_seen_at": time.time() - 86_400},
        settings=center_env,
    )
    handle = ExecutionHandle(
        backend_type=ExecutionMode.REMOTE_WORKER,
        handle_id=job["attempt_id"], project_id="ИСП/живость",
        attempt_id=job["attempt_id"], remote_job_id=job["job_id"],
        worker_id=worker_id,
    )
    backend = RemoteWorkerExecutionBackend(_RecordingManager())
    verdict = asyncio.run(backend.liveness(handle))
    assert verdict.state is Liveness.UNKNOWN
    assert not verdict.may_be_reclaimed


def test_remote_liveness_dead_only_on_terminal_state(center_env, admin, operator):
    import asyncio

    from backend.app.pipeline.execution.contracts import (
        ExecutionHandle,
        ExecutionMode,
        Liveness,
    )
    from backend.app.pipeline.execution.remote import RemoteWorkerExecutionBackend

    worker_id, _ = _approved_worker(admin, instance_id="inst_exec_live2")
    job = _create_job(admin, worker_id, project="ИСП/терминал")
    assert _cancel(operator, job).status_code == 200
    handle = ExecutionHandle(
        backend_type=ExecutionMode.REMOTE_WORKER,
        handle_id=job["attempt_id"], project_id="ИСП/терминал",
        attempt_id=job["attempt_id"], remote_job_id=job["job_id"],
        worker_id=worker_id,
    )
    verdict = asyncio.run(
        RemoteWorkerExecutionBackend(_RecordingManager()).liveness(handle)
    )
    assert verdict.state is Liveness.DEAD and verdict.may_be_reclaimed


def test_remote_reattach_finds_attempt_and_creates_nothing(center_env, admin):
    import asyncio

    from backend.app.pipeline.execution.contracts import (
        ExecutionHandle,
        ExecutionMode,
    )
    from backend.app.pipeline.execution.remote import RemoteWorkerExecutionBackend
    from backend.app.services.distributed_workers import repositories

    worker_id, _ = _approved_worker(admin, instance_id="inst_exec_reattach")
    job = _create_job(admin, worker_id, project="ИСП/переподключение")
    handle = ExecutionHandle(
        backend_type=ExecutionMode.REMOTE_WORKER,
        handle_id=job["attempt_id"], project_id="ИСП/переподключение",
        attempt_id=job["attempt_id"], remote_job_id=job["job_id"],
        worker_id=worker_id,
    )
    before = len(repositories.list_jobs(worker_id=worker_id, settings=center_env))
    snapshot = asyncio.run(
        RemoteWorkerExecutionBackend(_RecordingManager()).reattach(handle)
    )
    after = len(repositories.list_jobs(worker_id=worker_id, settings=center_env))
    assert snapshot is not None
    assert before == after == 1


def test_remote_reattach_returns_none_for_missing_attempt(center_env):
    import asyncio

    from backend.app.pipeline.execution.contracts import (
        ExecutionHandle,
        ExecutionMode,
    )
    from backend.app.pipeline.execution.remote import RemoteWorkerExecutionBackend

    handle = ExecutionHandle(
        backend_type=ExecutionMode.REMOTE_WORKER, handle_id="нет",
        project_id="p", attempt_id="00000000-0000-4000-8000-000000000000",
        remote_job_id="00000000-0000-4000-8000-000000000001",
    )
    assert asyncio.run(
        RemoteWorkerExecutionBackend(_RecordingManager()).reattach(handle)
    ) is None


# ═══ §3 Интеграция с PipelineManager ═════════════════════════════════════════
def test_local_path_goes_straight_to_dispatch_action():
    """Локальный режим не создаёт ни одного объекта backend'а.

    `_execute_item` для локального элемента вызывает прежний `_dispatch_action`
    напрямую — это и есть «поведение прежнее», а не «эквивалентное».
    """
    import asyncio

    from backend.app.models.audit import BatchQueueItem
    from backend.app.pipeline.manager import PipelineManager

    manager = object.__new__(PipelineManager)
    calls: list[dict] = []

    async def fake_dispatch(item, job, *, default_action="full", action_override=None):
        calls.append({"default_action": default_action,
                      "action_override": action_override})

    manager._dispatch_action = fake_dispatch          # type: ignore[assignment]
    item = BatchQueueItem(project_id="p")
    job = _FakeJob()
    asyncio.run(
        manager._execute_item(item, job, default_action="full", action_override=None)
    )
    assert calls == [{"default_action": "full", "action_override": None}]


def test_remote_item_without_flag_is_refused_not_run_locally(monkeypatch):
    """E-03: remote-элемент при выключенном флаге НЕ исполняется локально."""
    import asyncio

    from backend.app.models.audit import BatchQueueItem
    from backend.app.pipeline.execution.contracts import ExecutionError
    from backend.app.pipeline.manager import PipelineManager

    monkeypatch.setenv("DISTRIBUTED_AUDIT_EXECUTION_ENABLED", "false")
    manager = object.__new__(PipelineManager)
    called: list[str] = []

    async def fake_dispatch(item, job, **kwargs):
        called.append("dispatch")

    manager._dispatch_action = fake_dispatch          # type: ignore[assignment]
    item = BatchQueueItem(
        project_id="p", execution_mode="remote_worker", worker_id="wrk_1"
    )
    with pytest.raises(ExecutionError):
        asyncio.run(manager._execute_item(item, _FakeJob(), default_action="full"))
    assert called == [], "remote-элемент исполнился локально — это двойной запуск"


def test_remote_mode_without_worker_falls_back_to_local():
    """Персистентный элемент без воркера трактуется как локальный, а не падает."""
    from backend.app.models.audit import BatchQueueItem
    from backend.app.pipeline.execution import registry
    from backend.app.pipeline.execution.contracts import ExecutionMode

    item = BatchQueueItem(project_id="p", execution_mode="remote_worker")
    assert registry.item_execution_mode(item) is ExecutionMode.LOCAL


def test_old_queue_json_reads_as_local():
    """Старый batch_queue.json без новых полей — локальный (совместимость)."""
    from backend.app.models.audit import BatchQueueStatus
    from backend.app.pipeline.execution import registry
    from backend.app.pipeline.execution.contracts import ExecutionMode

    legacy = {
        "queue_id": "q1", "action": "full", "current_index": 0, "total": 1,
        "items": [{"project_id": "ПРО/ект", "action": "full", "status": "pending"}],
    }
    queue = BatchQueueStatus(**legacy)
    item = queue.items[0]
    assert registry.item_execution_mode(item) is ExecutionMode.LOCAL
    assert item.execution_handle == {}
    assert item.worker_id is None


def test_queue_item_roundtrips_execution_handle(tmp_path):
    """Ссылка на удалённое исполнение переживает сериализацию очереди (E-05)."""
    from backend.app.models.audit import BatchQueueItem, BatchQueueStatus
    from backend.app.pipeline.execution import registry

    item = BatchQueueItem(
        project_id="ПРО/ект",
        execution_mode="remote_worker",
        worker_id="wrk_abcd1234",
        execution_profile="remote_audit_pilot_v1",
        execution_handle={
            "backend_type": "remote_worker",
            "handle_id": "att-1",
            "project_id": "ПРО/ект",
            "attempt_id": "att-1",
            "remote_job_id": "job-1",
            "worker_id": "wrk_abcd1234",
        },
    )
    queue = BatchQueueStatus(queue_id="q", items=[item], total=1)
    blob = json.dumps(queue.model_dump(), ensure_ascii=False)
    restored = BatchQueueStatus(**json.loads(blob))
    handle = registry.handle_from_item(restored.items[0])
    assert handle is not None
    assert handle.attempt_id == "att-1" and handle.remote_job_id == "job-1"


def test_broken_handle_does_not_break_the_queue():
    from backend.app.models.audit import BatchQueueItem
    from backend.app.pipeline.execution import registry

    item = BatchQueueItem(project_id="p", execution_handle={"garbage": True})
    assert registry.handle_from_item(item) is None


def test_cleanup_zombies_never_touches_remote_items():
    """E-07: у remote-задания нет локального процесса, и это не делает его зомби."""
    from backend.app.models.audit import (
        AuditJob,
        BatchQueueItem,
        BatchQueueStatus,
        JobStatus,
    )
    from backend.app.pipeline.manager import PipelineManager

    manager = object.__new__(PipelineManager)
    manager.active_jobs = {}
    manager._tasks = {}
    manager._heartbeat_tasks = {}
    manager._batch_queue = BatchQueueStatus(
        queue_id="q",
        items=[
            BatchQueueItem(
                project_id="ПРО/удалённый", status="running",
                execution_mode="remote_worker", worker_id="wrk_1",
                # Защищается ИМЕННО живое удалённое исполнение, а признак
                # такового — сохранённая ссылка на попытку. Элемент без неё
                # ничего на воркере не занимает.
                execution_handle={
                    "backend_type": "remote_worker", "handle_id": "att_1",
                    "project_id": "ПРО/удалённый", "attempt_id": "att_1",
                    "remote_job_id": "job_1", "worker_id": "wrk_1",
                },
            )
        ],
        total=1,
    )
    stale = AuditJob(
        job_id="j", project_id="ПРО/удалённый", status=JobStatus.RUNNING,
        started_at="2000-01-01T00:00:00", last_heartbeat="2000-01-01T00:00:00",
    )
    manager.active_jobs["ПРО/удалённый"] = stale

    assert "ПРО/удалённый" in manager._protected_pids()
    manager.cleanup_zombies()
    assert "ПРО/удалённый" in manager.active_jobs, (
        "удалённое задание снято как зомби по локальному таймауту"
    )
    # И очередь не демотирована в interrupted.
    assert manager._batch_queue.items[0].status == "running"


def test_local_zombie_detection_still_works():
    """Обратная сторона: локальный протухший job по-прежнему снимается."""
    from backend.app.models.audit import AuditJob, BatchQueueStatus, JobStatus
    from backend.app.pipeline.manager import PipelineManager

    manager = object.__new__(PipelineManager)
    manager.active_jobs = {}
    manager._tasks = {}
    manager._heartbeat_tasks = {}
    manager._batch_queue = BatchQueueStatus(queue_id="q", items=[], total=0)
    manager.active_jobs["ПРО/локальный"] = AuditJob(
        job_id="j", project_id="ПРО/локальный", status=JobStatus.RUNNING,
        started_at="2000-01-01T00:00:00", last_heartbeat="2000-01-01T00:00:00",
    )
    manager.cleanup_zombies()
    assert "ПРО/локальный" not in manager.active_jobs


def test_batch_stays_local_only(monkeypatch):
    """§9: batch-очередь остаётся локальной, remote — только одиночный запуск."""
    import inspect

    from backend.app.pipeline.manager import PipelineManager

    source = inspect.getsource(PipelineManager.add_to_batch)
    assert "execution_mode" not in source
    assert "worker_id" not in source
    # А одиночный удалённый запуск существует и требует воркера.
    signature = inspect.signature(PipelineManager.start_remote_audit)
    assert "worker_id" in signature.parameters


# ═══ §4 Исходный пакет проекта и снимки ══════════════════════════════════════
#: Физические сегменты переносимого дерева. Раскладка стала обязательной:
#: плоский `versions/<vid>` резолвером не находится (Б-3, отчёт 08).
_OBJ, _DISC, _DOC, _VID = "ОБЪЕКТ-1", "АР", "ПРОЕКТ-К1", "v002"
_PROJECT_REL = f"objects/{_OBJ}/disciplines/{_DISC}/documents/{_DOC}"
_VERSION_REL = f"{_PROJECT_REL}/versions/{_VID}"


def _make_v2_root(root: Path) -> Path:
    """Метаданные объекта и документа. Без document.json адаптер молчит."""
    doc_dir = root / _PROJECT_REL
    doc_dir.mkdir(parents=True, exist_ok=True)
    (doc_dir / "document.json").write_text(
        json.dumps({"schema_version": 1, "document_code": _DOC,
                    "object_id": "obj-1", "discipline": _DISC,
                    "current_version": _VID,
                    "versions": [{"version_id": _VID, "version_no": 2}]},
                   ensure_ascii=False),
        encoding="utf-8")
    (doc_dir / "current_version.txt").write_text(_VID, encoding="utf-8")
    obj_dir = root / "objects" / _OBJ
    (obj_dir / "object.json").write_text(
        json.dumps({"schema_version": 1, "object_id": "obj-1", "name": _OBJ},
                   ensure_ascii=False),
        encoding="utf-8")
    return doc_dir


def _make_version_tree(root: Path) -> Path:
    """Правдоподобное дерево версии projects_v2 с хардлинком и мусором."""
    _make_v2_root(root)
    version = root / _VERSION_REL
    (version / "01_input").mkdir(parents=True)
    (version / "02_work").mkdir(parents=True)
    (version / "03_analysis" / "latest" / "blocks_stage02_100").mkdir(parents=True)
    (version / "99_service").mkdir(parents=True)
    (version / "01_input" / "document.pdf").write_bytes(b"%PDF-1.7 fake\n" * 10)
    (version / "01_input" / "input_manifest.json").write_text('{"files": []}')
    (version / "02_work" / "document.pdf").write_bytes(b"%PDF-1.7 fake\n" * 10)
    (version / "02_work" / "document.md").write_text("# Лист 1\n", encoding="utf-8")
    (version / "03_analysis" / "latest" / "03_findings.json").write_text(
        '{"findings": []}', encoding="utf-8"
    )
    (version / "99_service" / "pipeline_log.json").write_text(
        '{"stages": {}}', encoding="utf-8"
    )
    (version / "version.json").write_text('{"version_id": "v002"}', encoding="utf-8")
    # Хардлинк: два пути, один инод — ровно то, из-за чего выбран TAR.
    crop = version / "03_analysis" / "latest" / "blocks_stage02_100" / "block_a.png"
    crop.write_bytes(b"\x89PNG fake crop")
    linked = version / "03_analysis" / "latest" / "block_a_dup.png"
    os_link(crop, linked)
    # То, что не должно попасть в пакет ни при каких обстоятельствах.
    (version / ".env").write_text("PORTAL_SESSION_SECRET=xxx\n", encoding="utf-8")
    (version / "token").write_text("wtk_abcdefghijklmnopqrstuvwxyz012345", encoding="utf-8")
    (version / "runtime.pid").write_text("123", encoding="utf-8")
    (version / ".git").mkdir()
    (version / ".git" / "config").write_text("[core]\n", encoding="utf-8")
    (version / "_stage02_paid_response_cache").mkdir()
    (version / "_stage02_paid_response_cache" / "c.json").write_text("{}")
    return version


def os_link(src: Path, dst: Path) -> None:
    import os as _os

    _os.link(src, dst)


def test_package_scan_excludes_secrets_and_regenerables(tmp_path):
    from backend.app.services.distributed_workers import project_package

    version = _make_version_tree(tmp_path)
    scan = project_package.scan_version_tree(version)
    names = {rel for _abs, rel in scan.files}

    assert "01_input/document.pdf" in names
    assert "02_work/document.md" in names
    assert "03_analysis/latest/03_findings.json" in names
    assert "99_service/pipeline_log.json" in names
    assert "version.json" in names

    for forbidden in (".env", "token", "runtime.pid", ".git/config",
                      "_stage02_paid_response_cache/c.json"):
        assert forbidden not in names, forbidden
    assert any(".env" in entry for entry in scan.excluded)


def test_package_preserves_hardlinks(tmp_path):
    """TAR обязан сохранять жёсткие ссылки: иначе пакет раздувается на 40 %."""
    import tarfile

    from backend.app.services.distributed_workers import project_package

    version = _make_version_tree(tmp_path)
    dest = tmp_path / "pkg.tar.gz"
    manifest = project_package.build_project_source_package(
        dest_path=dest,
        version_dir=version,
        manifest_base={"manifest_version": 1, "package_id": "pkg_1",
                       "job_id": "j", "attempt_id": "a"},
        snapshot_files={},
        feature_flags={},
    )
    assert manifest["hardlink_groups"] >= 1
    with tarfile.open(dest, "r:gz") as tar:
        links = [m for m in tar.getmembers() if m.islnk()]
    assert links, "хардлинков в архиве нет — они были потеряны"
    assert all(m.linkname.startswith("payload/projects_v2/") for m in links)


def test_package_manifest_has_required_fields(tmp_path):
    from backend.app.services.distributed_workers import project_package

    version = _make_version_tree(tmp_path)
    manifest = project_package.build_project_source_package(
        dest_path=tmp_path / "pkg.tar.gz",
        version_dir=version,
        manifest_base={
            "manifest_version": 1, "package_id": "pkg_1", "job_id": "j",
            "attempt_id": "a", "project_id": "ПРО/ект", "version_id": "v002",
            "execution_profile": "remote_audit_pilot_v1",
            "pipeline_revision": "rev-1",
            "prompt_bundle_hash": "sha256:aa", "model_config_hash": "sha256:bb",
        },
        snapshot_files={"stage_models.json": b"{}"},
        feature_flags={"AUDIT_X": "1"},
    )
    for field in (
        "manifest_version", "package_id", "package_type", "job_id", "attempt_id",
        "project_id", "version_id", "execution_profile", "pipeline_revision",
        "project_layout_version", "created_at", "compression", "source_tree_hash",
        "prompt_bundle_hash", "model_config_hash", "feature_flags_hash",
        "excluded_regenerable_paths", "files", "hardlinks", "total_size",
        "uncompressed_size", "limits", "archive",
    ):
        assert field in manifest, field
    assert manifest["package_type"] == "source"


def test_package_rejects_symlinks(tmp_path):
    from backend.app.services.distributed_workers import project_package

    version = _make_version_tree(tmp_path)
    (version / "outside.txt").symlink_to("/etc/passwd")
    scan = project_package.scan_version_tree(version)
    names = {rel for _abs, rel in scan.files}
    assert "outside.txt" not in names
    assert any("симлинк" in entry for entry in scan.excluded)


def test_feature_flags_snapshot_drops_secrets():
    from backend.app.services.distributed_workers import project_package

    flags = project_package.collect_feature_flags_snapshot(
        {
            "AUDIT_CROP_CACHE_SOURCE": "local_pdf",
            "AUDIT_WORKER_TOKEN": "wtk_secret",
            "PAID_API_ENABLED": "true",
            "AUDIT_BOOTSTRAP_SECRET": "s3cr3t",
            "OPENROUTER_API_KEY": "sk-xxx",
            "HOME": "/root",
        }
    )
    assert flags == {"AUDIT_CROP_CACHE_SOURCE": "local_pdf",
                     "PAID_API_ENABLED": "true"}


def test_secret_scanner_catches_known_forms():
    from backend.app.services.distributed_workers import project_package

    hits = project_package.find_secrets_in_files(
        [
            ("a.md", "текст без секретов".encode("utf-8")),
            ("b.env", b"PORTAL_SESSION_SECRET=abc"),
            ("c.json", b'{"t": "wtk_abcdefghijklmnopqrstuvwx"}'),
            ("d.txt", b"pbkdf2_sha256$29000$xyz"),
        ]
    )
    assert len(hits) == 3
    assert all(name in "b.env c.json d.txt" for name in (h.split(":")[0] for h in hits))


def test_prompt_snapshot_hash_is_stable_and_content_sensitive(tmp_path):
    from backend.app.services.distributed_workers import project_package

    prompts = tmp_path / "prompts"
    (prompts / "pipeline" / "ru").mkdir(parents=True)
    target = prompts / "pipeline" / "ru" / "task.md"
    target.write_text("шаблон", encoding="utf-8")

    first = project_package.collect_prompt_snapshot(prompts)
    hash_first = project_package.hash_files(first)
    assert project_package.hash_files(
        project_package.collect_prompt_snapshot(prompts)
    ) == hash_first

    target.write_text("другой шаблон", encoding="utf-8")
    assert project_package.hash_files(
        project_package.collect_prompt_snapshot(prompts)
    ) != hash_first


# ═══ §5 Строгий audit_pipeline_v1 на воркере ══════════════════════════════════
def _worker_config(tmp_path, **overrides):
    from audit_worker.config import WorkerConfig

    pipeline_root = tmp_path / "platform"
    pipeline_root.mkdir(parents=True, exist_ok=True)
    defaults = dict(
        dispatcher_url="https://center.example",
        root=tmp_path / "worker",
        display_name="vps-test",
        pipeline_revision="rev-abc123",
        pipeline_root=pipeline_root,
        audit_pipeline_enabled=True,
        allow_real_llm=False,
    )
    defaults.update(overrides)
    config = WorkerConfig(**defaults)
    config.ensure_dirs()
    return config


def _audit_params(**overrides):
    payload = {
        "execution_profile": "remote_audit_pilot_v1",
        "action": "full",
        "include_optimization": True,
        "include_norms": False,
        "pipeline_revision": "rev-abc123",
        "expected_source_tree_hash": "sha256:" + "a" * 64,
        "prompt_bundle_hash": "sha256:" + "b" * 64,
        "model_config_hash": "sha256:" + "c" * 64,
        "feature_flags_hash": "sha256:" + "d" * 64,
        "runtime_snapshot_hash": "sha256:" + "e" * 64,
    }
    payload.update(overrides)
    return payload


def test_worker_rejects_unknown_fields(tmp_path):
    from audit_worker import audit_runner

    config = _worker_config(tmp_path)
    with pytest.raises(audit_runner.AuditJobRejected) as excinfo:
        audit_runner.validate_params(
            _audit_params(command="rm -rf /"), config=config
        )
    assert "command" in str(excinfo.value)


def test_worker_rejects_norms_on_worker(tmp_path):
    from audit_worker import audit_runner

    config = _worker_config(tmp_path)
    with pytest.raises(audit_runner.AuditJobRejected):
        audit_runner.validate_params(
            _audit_params(include_norms=True), config=config
        )


def test_worker_rejects_unknown_profile_and_action(tmp_path):
    from audit_worker import audit_runner

    config = _worker_config(tmp_path)
    for payload in (
        _audit_params(execution_profile="anything_else"),
        _audit_params(action="delete_everything"),
    ):
        with pytest.raises(audit_runner.AuditJobRejected):
            audit_runner.validate_params(payload, config=config)


def test_worker_rejects_revision_mismatch(tmp_path):
    from audit_worker import audit_runner

    config = _worker_config(tmp_path)
    with pytest.raises(audit_runner.AuditJobRejected) as excinfo:
        audit_runner.validate_params(
            _audit_params(pipeline_revision="rev-other"), config=config
        )
    assert "Ревизия" in str(excinfo.value)


def test_worker_refuses_audit_when_capability_disabled(tmp_path):
    from audit_worker import audit_runner

    config = _worker_config(tmp_path, audit_pipeline_enabled=False)
    with pytest.raises(audit_runner.AuditJobRejected):
        audit_runner.validate_params(_audit_params(), config=config)


def test_worker_refuses_without_installed_platform(tmp_path):
    from audit_worker import audit_runner

    config = _worker_config(tmp_path, pipeline_root=None)
    with pytest.raises(audit_runner.AuditJobRejected):
        audit_runner.validate_params(_audit_params(), config=config)


def test_worker_cannot_shrink_required_artifacts(tmp_path):
    """Задание не может сократить список обязательных артефактов."""
    from audit_worker import audit_runner

    config = _worker_config(tmp_path)
    params = audit_runner.validate_params(
        _audit_params(required_result_artifacts=["result/anything.json"]),
        config=config,
    )
    assert set(audit_runner.REQUIRED_RESULT_ARTIFACTS) <= set(
        params.required_result_artifacts
    )
    assert "result/anything.json" not in params.required_result_artifacts


def test_worker_builds_fixed_argv(tmp_path):
    """argv фиксирован: интерпретатор + -u + -m + константный модуль + спека."""
    from audit_worker import audit_runner

    config = _worker_config(tmp_path)
    argv = audit_runner.build_argv(tmp_path / "spec.json", config=config)
    assert len(argv) == 5
    assert argv[1] == "-u" and argv[2] == "-m"
    assert argv[3] == audit_runner.PIPELINE_ENTRYPOINT_MODULE
    assert argv[4].endswith("spec.json")


def test_worker_env_is_an_allowlist_and_points_inside_job_dir(tmp_path, monkeypatch):
    from audit_worker import audit_runner

    monkeypatch.setenv("SECRET_LEAK", "must-not-pass")
    monkeypatch.setenv("AUDIT_WORKER_TOKEN", "wtk_leak")
    config = _worker_config(tmp_path)
    job_dir = tmp_path / "jobs" / "j" / "a"
    job_dir.mkdir(parents=True)
    env = audit_runner.build_env(config=config, job_dir=job_dir, provider_dir=None)

    assert "SECRET_LEAK" not in env
    assert "AUDIT_WORKER_TOKEN" not in env
    assert env["AUDIT_ROLE"] == "worker"
    for key in ("AUDIT_DATA_DIR", "AUDIT_APP_DATA_DIR", "AUDIT_PROJECTS_V2_DIR",
                "AUDIT_PROMPTS_DIR", "TMPDIR"):
        assert str(job_dir) in env[key], key


def test_worker_env_wires_fake_providers(tmp_path):
    from audit_worker import audit_runner

    config = _worker_config(tmp_path)
    provider_dir = tmp_path / "fakes"
    provider_dir.mkdir()
    env = audit_runner.build_env(
        config=config, job_dir=tmp_path / "jd", provider_dir=provider_dir
    )
    assert env["AUDIT_WORKER_PROVIDER_MODE"] == "fake"
    assert env["PATH"].startswith(str(provider_dir))


def test_fake_providers_are_marked_and_executable(tmp_path):
    import subprocess

    from backend.app.pipeline.execution import fake_providers

    target = fake_providers.materialize(tmp_path / "fakes")
    assert fake_providers.looks_like_fake_dir(target)
    for name in fake_providers.FAKE_BINARIES:
        binary = target / name
        assert binary.is_file()
        result = subprocess.run(
            [sys.executable, str(binary)], input="привет",
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0
        payload = json.loads(result.stdout)
        assert payload["provider"] == name
        assert payload["is_error"] is False


def test_fake_providers_can_simulate_failures(tmp_path):
    import subprocess

    from backend.app.pipeline.execution import fake_providers

    target = fake_providers.materialize(tmp_path / "fakes")
    binary = target / fake_providers.FAKE_BINARIES[0]
    for behaviour, expect_code in (("rate_limit", 1), ("auth_error", 1)):
        result = subprocess.run(
            [sys.executable, str(binary)], input="x", capture_output=True, text=True,
            env={**{"PATH": "/usr/bin:/bin"},
                 fake_providers.BEHAVIOUR_ENV: behaviour},
            timeout=30,
        )
        assert result.returncode == expect_code, behaviour
    broken = subprocess.run(
        [sys.executable, str(binary)], input="x", capture_output=True, text=True,
        env={**{"PATH": "/usr/bin:/bin"},
             fake_providers.BEHAVIOUR_ENV: "broken_json"},
        timeout=30,
    )
    with pytest.raises(ValueError):
        json.loads(broken.stdout)


def test_executor_fails_closed_without_fake_providers(tmp_path):
    """Настоящие модели запрещены, подделок нет → задание отвергается."""
    from audit_worker import audit_runner, local_db
    from audit_worker.executor import Executor

    config = _worker_config(tmp_path, fake_provider_dir=None, allow_real_llm=False)
    executor = Executor(config, db=local_db.LocalDB(config.local_db_path))
    with pytest.raises(audit_runner.AuditJobRejected):
        executor._provider_dir()


def test_executor_uses_real_providers_only_when_allowed(tmp_path):
    from audit_worker import local_db
    from audit_worker.executor import Executor

    config = _worker_config(tmp_path, allow_real_llm=True)
    executor = Executor(config, db=local_db.LocalDB(config.local_db_path))
    assert executor._provider_dir() is None


def test_real_audit_and_test_jobs_never_mix(tmp_path):
    """E-22, E-23, E-24: один аудит на воркер и никакого смешивания с тестами."""
    from audit_worker import local_db
    from audit_worker.executor import Executor

    config = _worker_config(tmp_path)
    db = local_db.LocalDB(config.local_db_path)
    executor = Executor(config, db=db)

    audit_a = str(uuid.uuid4())
    audit_b = str(uuid.uuid4())
    test_c = str(uuid.uuid4())
    job = str(uuid.uuid4())
    db.enqueue(job_id=job, attempt_id=audit_a, job_type="audit_pipeline_v1", params={})
    db.enqueue(job_id=job, attempt_id=audit_b, job_type="audit_pipeline_v1", params={})
    db.enqueue(job_id=job, attempt_id=test_c, job_type="test_pipeline_v1", params={})
    db.set_queue_state(audit_a, local_db.QUEUE_RUNNING)

    # Второй реальный аудит не стартует.
    assert executor.audit_slot_conflict(audit_b) is not None
    # Тестовое задание при идущем аудите тоже ждёт.
    assert executor.test_slot_conflict(test_c) is not None
    # А когда аудит закончился — оба свободны.
    db.set_queue_state(audit_a, local_db.QUEUE_FINISHED)
    assert executor.audit_slot_conflict(audit_b) is None
    assert executor.test_slot_conflict(test_c) is None


def test_running_test_job_blocks_real_audit(tmp_path):
    from audit_worker import local_db
    from audit_worker.executor import Executor

    config = _worker_config(tmp_path)
    db = local_db.LocalDB(config.local_db_path)
    executor = Executor(config, db=db)
    job = str(uuid.uuid4())
    test_a = str(uuid.uuid4())
    audit_b = str(uuid.uuid4())
    db.enqueue(job_id=job, attempt_id=test_a, job_type="test_pipeline_v1", params={})
    db.enqueue(job_id=job, attempt_id=audit_b, job_type="audit_pipeline_v1", params={})
    db.set_queue_state(test_a, local_db.QUEUE_RUNNING)
    conflict = executor.audit_slot_conflict(audit_b)
    assert conflict is not None and "тестовые" in conflict


def test_executor_rejects_unknown_job_type(tmp_path):
    from audit_worker import local_db
    from audit_worker.executor import Executor

    config = _worker_config(tmp_path)
    db = local_db.LocalDB(config.local_db_path)
    executor = Executor(config, db=db)
    job, attempt = str(uuid.uuid4()), str(uuid.uuid4())
    db.enqueue(job_id=job, attempt_id=attempt, job_type="run_shell", params={})
    outcome = executor.run_attempt(
        {"job_id": job, "attempt_id": attempt, "job_type": "run_shell",
         "params_json": "{}"}
    )
    assert outcome["ok"] is False and outcome["reason"] == "unknown_job_type"


def test_worker_job_layout_is_inside_attempt_dir(tmp_path):
    from audit_worker import audit_runner

    job_dir = tmp_path / "jobs" / "job" / "attempt"
    layout = audit_runner.prepare_job_dir(job_dir)
    for path in layout.values():
        assert job_dir in path.parents or path == job_dir


def test_worker_reports_provider_mode_in_capabilities(tmp_path):
    config_fake = _worker_config(tmp_path / "a")
    config_real = _worker_config(tmp_path / "b", allow_real_llm=True)
    caps_fake = config_fake.capabilities()
    caps_real = config_real.capabilities()
    assert caps_fake["provider_mode"] == "fake"
    assert caps_fake["real_llm_enabled"] is False
    assert caps_real["provider_mode"] == "real"
    assert "audit_pipeline_v1" in caps_fake["job_types"]
    assert caps_fake["real_audit_max_slots"] == 1


def test_worker_without_audit_flag_hides_capability(tmp_path):
    config = _worker_config(tmp_path, audit_pipeline_enabled=False)
    assert "audit_pipeline_v1" not in config.capabilities()["job_types"]


def test_runner_refuses_norms_and_unknown_profile(tmp_path):
    from backend.app.pipeline import remote_audit_runner

    spec = tmp_path / "spec.json"
    spec.write_text(json.dumps({"profile": "remote_audit_pilot_v1",
                                "include_norms": True}), encoding="utf-8")
    with pytest.raises(SystemExit):
        remote_audit_runner.load_spec(spec)

    spec.write_text(json.dumps({"profile": "other"}), encoding="utf-8")
    with pytest.raises(SystemExit):
        remote_audit_runner.load_spec(spec)


def test_runner_refuses_central_only_stage(tmp_path):
    from backend.app.pipeline import remote_audit_runner

    spec = tmp_path / "spec.json"
    spec.write_text(
        json.dumps({"profile": "remote_audit_pilot_v1", "include_norms": False,
                    "retry_stage": "norm_verify"}),
        encoding="utf-8",
    )
    with pytest.raises(SystemExit):
        remote_audit_runner.load_spec(spec)


def test_runner_refuses_paths_outside_attempt_dir(tmp_path, monkeypatch):
    from backend.app.pipeline import remote_audit_runner

    job_dir = tmp_path / "jobs" / "j" / "a"
    (job_dir / "project" / "objects").mkdir(parents=True)
    spec = {"paths": {"project": str(job_dir / "project")}}
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", "/etc")
    monkeypatch.setenv("AUDIT_DATA_DIR", str(job_dir / "work"))
    monkeypatch.setenv("AUDIT_APP_DATA_DIR", str(job_dir / "work"))
    with pytest.raises(SystemExit):
        remote_audit_runner.apply_runtime_paths(spec)


def test_runner_accepts_paths_inside_attempt_dir(tmp_path, monkeypatch):
    from backend.app.pipeline import remote_audit_runner

    job_dir = tmp_path / "jobs" / "j" / "a"
    (job_dir / "project" / "objects").mkdir(parents=True)
    spec = {"paths": {"project": str(job_dir / "project")}}
    # Корни берутся из ЕДИНСТВЕННОГО их источника — `isolated_roots`. Список
    # рос дважды (сначала `AUDIT_PROJECTS_DIR`, затем `COMPARISON_ROOT`, `HOME`,
    # `TMPDIR` и каталоги «чистой» cwd и рабочего каталога агента модели), и
    # перечисление их здесь копией означало бы, что тест отстаёт от кода.
    from audit_worker import audit_runner

    for name, value in audit_runner.isolated_roots(job_dir).items():
        monkeypatch.setenv(name, str(value))
    monkeypatch.delenv("AUDIT_ROOT_DIR", raising=False)
    monkeypatch.delenv("AUDIT_BASE_DIR", raising=False)
    remote_audit_runner.apply_runtime_paths(spec)      # не бросает


def test_runner_rejects_projects_dir_outside_attempt(tmp_path, monkeypatch):
    """`AUDIT_PROJECTS_DIR` наружу — отказ, а не тихая запись в чужой каталог."""
    from backend.app.pipeline import remote_audit_runner

    job_dir = tmp_path / "jobs" / "j" / "a"
    (job_dir / "project" / "objects").mkdir(parents=True)
    spec = {"paths": {"project": str(job_dir / "project")}}
    for name, value in (
        ("AUDIT_PROJECTS_DIR", tmp_path / "чужое" / "projects"),
        ("AUDIT_PROJECTS_V2_DIR", job_dir / "project"),
        ("AUDIT_DATA_DIR", job_dir / "work" / "data"),
        ("AUDIT_APP_DATA_DIR", job_dir / "work" / "app_data"),
        ("AUDIT_PROMPTS_DIR", job_dir / "snapshot" / "prompts"),
        ("AUDIT_ACTION_LOG_DIR", job_dir / "logs" / "actions"),
    ):
        monkeypatch.setenv(name, str(value))
    with pytest.raises(SystemExit) as excinfo:
        remote_audit_runner.apply_runtime_paths(spec)
    assert "AUDIT_PROJECTS_DIR" in str(excinfo.value)


def test_runner_detects_snapshot_tampering(tmp_path):
    from backend.app.pipeline import remote_audit_runner

    snapshot = tmp_path / "snapshot"
    (snapshot / "prompts").mkdir(parents=True)
    (snapshot / "prompts" / "task.md").write_text("шаблон", encoding="utf-8")
    spec = {
        "paths": {"snapshot": str(snapshot)},
        "prompt_bundle_hash": "sha256:" + "0" * 64,
    }
    with pytest.raises(SystemExit) as excinfo:
        remote_audit_runner.verify_snapshot(spec)
    assert "Снимок" in str(excinfo.value)


# ═══ §6 Приём результата: staging, откат, идемпотентность ═════════════════════
def _build_result_archive(
    tmp_path: Path,
    *,
    job_id: str,
    attempt_id: str,
    source_hash: str = "sha256:" + "1" * 64,
    revision: str = "rev-abc123",
    extra_project_files: dict[str, str] | None = None,
    omit_findings: bool = False,
) -> Path:
    """Собрать НАСТОЯЩИЙ пакет результата воркерским сборщиком."""
    from audit_worker import package_io

    job_dir = tmp_path / "jobs" / job_id / attempt_id
    for sub in ("project/03_analysis/latest", "work", "result", "usage", "logs"):
        (job_dir / sub).mkdir(parents=True, exist_ok=True)
    findings = '{"findings": [{"id": "F-001"}]}'
    if not omit_findings:
        (job_dir / "result" / "03_findings.json").write_text(findings, encoding="utf-8")
    (job_dir / "result" / "audit_manifest.json").write_text(
        json.dumps({"pipeline_revision": revision,
                    "stage_completion": {"findings_merge": "done"},
                    "resume_hint": "norm_verify"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (job_dir / "work" / "pipeline_log.json").write_text(
        '{"stages": {"findings_merge": {"status": "done"}}}', encoding="utf-8"
    )
    (job_dir / "usage" / "usage_report.json").write_text(
        json.dumps({"entries": [{"stage": "findings_merge", "model": "fake",
                                 "input_tokens": 10, "output_tokens": 5}]}),
        encoding="utf-8",
    )
    (job_dir / "logs" / "stdout.log").write_text("работа шла\n", encoding="utf-8")
    worker_version_dir = job_dir / "project" / _VERSION_REL
    (worker_version_dir / "03_analysis" / "latest").mkdir(parents=True, exist_ok=True)
    (worker_version_dir / "03_analysis" / "latest" / "03_findings.json").write_text(
        findings, encoding="utf-8"
    )
    for rel, content in (extra_project_files or {}).items():
        target = worker_version_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")

    archive = tmp_path / f"result-{attempt_id[:8]}.tar.gz"
    package_io.build_result_package(
        dest_path=archive,
        job_dir=job_dir,
        job_id=job_id,
        attempt_id=attempt_id,
        project_id="ПРО/ект",
        version_id="v002",
        worker_id="wrk_1",
        worker_version="0.0.1",
        protocol_version=1,
        manifest_version=1,
        source_package_hash=source_hash,
        exit_code=0,
        job_type="audit_pipeline_v1",
        required_artifacts=list(
            __import__("audit_worker.audit_runner", fromlist=["x"])
            .REQUIRED_RESULT_ARTIFACTS
        ),
        pipeline_revision=revision,
        stage_completion={"findings_merge": "done"},
        resume_hint="norm_verify",
    )
    return archive


def _center_attempt(center_env, tmp_path, *, source_hash="sha256:" + "1" * 64):
    """Создать в workers.db попытку реального аудита."""
    from backend.app.services.distributed_workers import repositories

    job = repositories.create_job(
        job_type="audit_pipeline_v1",
        project_id="ПРО/ект",
        version_id="v002",
        payload={"params": {}},
        display_name="ПРО/ект",
        created_by="operator:test",
        settings=center_env,
    )
    repositories.update_attempt_fields(
        job["attempt_id"], {"source_package_hash": source_hash}, settings=center_env
    )
    return repositories.get_attempt(job["attempt_id"], settings=center_env)


def _version_dir(tmp_path: Path) -> Path:
    _make_v2_root(tmp_path / "center_project")
    version = tmp_path / "center_project" / _VERSION_REL
    (version / "01_input").mkdir(parents=True, exist_ok=True)
    (version / "03_analysis" / "latest").mkdir(parents=True, exist_ok=True)
    (version / "01_input" / "document.pdf").write_bytes(b"%PDF original")
    (version / "03_analysis" / "latest" / "03_findings.json").write_text(
        '{"findings": []}', encoding="utf-8"
    )
    return version


def test_result_import_applies_only_generated_paths(center_env, tmp_path, monkeypatch):
    from backend.app.services.distributed_workers import result_import

    monkeypatch.setenv("AUDIT_PIPELINE_REVISION", "rev-abc123")
    import importlib

    from backend.app.core import config as core_config
    importlib.reload(core_config)

    attempt = _center_attempt(center_env, tmp_path)
    archive = _build_result_archive(
        tmp_path, job_id=attempt["job_id"], attempt_id=attempt["attempt_id"]
    )
    version = _version_dir(tmp_path)
    original_pdf = (version / "01_input" / "document.pdf").read_bytes()

    report = result_import.apply_result_package(
        archive=archive, attempt=attempt, version_dir=version, settings=center_env
    )

    assert "03_analysis/latest/03_findings.json" in report["applied_paths"]
    applied = json.loads(
        (version / "03_analysis" / "latest" / "03_findings.json").read_text("utf-8")
    )
    assert applied["findings"][0]["id"] == "F-001"
    # E-15: исходный PDF не тронут.
    assert (version / "01_input" / "document.pdf").read_bytes() == original_pdf
    assert Path(report["journal"]).is_file()


def test_worker_package_never_returns_source_files(tmp_path):
    """Первый рубеж: сборщик воркера физически не кладёт исходники в пакет."""
    import tarfile

    archive = _build_result_archive(
        tmp_path, job_id=str(uuid.uuid4()), attempt_id=str(uuid.uuid4()),
        extra_project_files={"01_input/document.pdf": "ПОДМЕНА",
                             "02_work/document.md": "ПОДМЕНА"},
    )
    with tarfile.open(archive, "r:gz") as tar:
        names = tar.getnames()
    assert not any("project/01_input/" in n for n in names)
    assert not any("project/02_work/" in n for n in names)
    assert any("project/03_analysis/" in n for n in names)


def test_center_plan_skips_source_and_rejects_unknown(tmp_path):
    """Второй рубеж: даже если исходник придёт, план его не применит."""
    from backend.app.services.distributed_workers import result_import

    staged = tmp_path / "staged"
    for rel in ("01_input/document.pdf", "02_work/document.md",
                "04_review/expert_review.json", "version.json",
                "03_analysis/latest/03_findings.json",
                "99_service/pipeline_log.json"):
        target = staged / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("x", encoding="utf-8")
    plan = result_import.build_change_plan(staged, tmp_path / "version")
    assert set(plan["apply"]) == {
        "03_analysis/latest/03_findings.json", "99_service/pipeline_log.json"
    }
    assert set(plan["skipped_source"]) == {
        "01_input/document.pdf", "02_work/document.md",
        "04_review/expert_review.json", "version.json",
    }
    assert plan["rejected"] == []

    (staged / "чужое.json").write_text("x", encoding="utf-8")
    plan2 = result_import.build_change_plan(staged, tmp_path / "version")
    assert [r["path"] for r in plan2["rejected"]] == ["чужое.json"]


def test_result_import_rejects_central_only_artifact(center_env, tmp_path, monkeypatch):
    """E-19: норм-артефакт из пакета воркера отклоняет ВЕСЬ пакет."""
    from backend.app.services.distributed_workers import result_import

    monkeypatch.setenv("AUDIT_PIPELINE_REVISION", "rev-abc123")
    attempt = _center_attempt(center_env, tmp_path)
    archive = _build_result_archive(
        tmp_path, job_id=attempt["job_id"], attempt_id=attempt["attempt_id"],
        extra_project_files={
            "03_analysis/latest/norm_checks.json": '{"checks": []}'
        },
    )
    version = _version_dir(tmp_path)
    before = (version / "03_analysis" / "latest" / "03_findings.json").read_text("utf-8")

    with pytest.raises(result_import.ResultImportError):
        result_import.apply_result_package(
            archive=archive, attempt=attempt, version_dir=version, settings=center_env
        )
    # Проект не тронут ни на один файл.
    assert (version / "03_analysis" / "latest" / "03_findings.json").read_text(
        "utf-8"
    ) == before


def test_result_import_rejects_wrong_source_package(center_env, tmp_path, monkeypatch):
    from backend.app.services.distributed_workers import result_import

    monkeypatch.setenv("AUDIT_PIPELINE_REVISION", "rev-abc123")
    attempt = _center_attempt(center_env, tmp_path, source_hash="sha256:" + "1" * 64)
    archive = _build_result_archive(
        tmp_path, job_id=attempt["job_id"], attempt_id=attempt["attempt_id"],
        source_hash="sha256:" + "9" * 64,
    )
    with pytest.raises(result_import.ResultImportError) as excinfo:
        result_import.apply_result_package(
            archive=archive, attempt=attempt,
            version_dir=_version_dir(tmp_path), settings=center_env,
        )
    assert "исходном пакете" in str(excinfo.value)


def test_result_import_rejects_revision_mismatch(center_env, tmp_path, monkeypatch):
    import importlib

    from backend.app.core import config as core_config
    from backend.app.services.distributed_workers import result_import

    monkeypatch.setenv("AUDIT_PIPELINE_REVISION", "rev-center")
    importlib.reload(core_config)
    try:
        attempt = _center_attempt(center_env, tmp_path)
        archive = _build_result_archive(
            tmp_path, job_id=attempt["job_id"], attempt_id=attempt["attempt_id"],
            revision="rev-worker",
        )
        with pytest.raises(result_import.ResultImportError) as excinfo:
            result_import.apply_result_package(
                archive=archive, attempt=attempt,
                version_dir=_version_dir(tmp_path), settings=center_env,
            )
        assert "Ревизия" in str(excinfo.value)
    finally:
        monkeypatch.delenv("AUDIT_PIPELINE_REVISION", raising=False)
        importlib.reload(core_config)


def test_result_import_is_idempotent_and_detects_conflict(center_env, tmp_path, monkeypatch):
    """Тот же пакет — `already_applied`; другой hash — конфликт (E-17)."""
    from backend.app.services.distributed_workers import repositories, result_import

    monkeypatch.setenv("AUDIT_PIPELINE_REVISION", "rev-abc123")
    attempt = _center_attempt(center_env, tmp_path)
    repositories.update_attempt_fields(
        attempt["attempt_id"],
        {
            "result_import_state": "applied",
            "result_import_hash": "a" * 64,
            "result_package_hash": "a" * 64,
            "result_import_report": json.dumps({"applied_paths": ["x"]}),
        },
        settings=center_env,
    )
    fresh = repositories.get_attempt(attempt["attempt_id"], settings=center_env)
    replayed = result_import.import_result_for_attempt(
        attempt=fresh, settings=center_env, version_dir=_version_dir(tmp_path)
    )
    assert replayed["replayed"] is True and replayed["applied"] is True

    repositories.update_attempt_fields(
        attempt["attempt_id"], {"result_package_hash": "b" * 64}, settings=center_env
    )
    conflicting = repositories.get_attempt(attempt["attempt_id"], settings=center_env)
    with pytest.raises(result_import.ResultImportConflict):
        result_import.import_result_for_attempt(
            attempt=conflicting, settings=center_env,
            version_dir=_version_dir(tmp_path),
        )


def test_result_import_rolls_back_on_failure(center_env, tmp_path, monkeypatch):
    """Сбой посреди применения откатывает ВСЁ и оставляет staging."""
    import shutil as _shutil

    from backend.app.services.distributed_workers import result_import

    monkeypatch.setenv("AUDIT_PIPELINE_REVISION", "rev-abc123")
    attempt = _center_attempt(center_env, tmp_path)
    archive = _build_result_archive(
        tmp_path, job_id=attempt["job_id"], attempt_id=attempt["attempt_id"],
        extra_project_files={
            "03_analysis/latest/a.json": "{}",
            "03_analysis/latest/b.json": "{}",
            "03_analysis/latest/c.json": "{}",
        },
    )
    version = _version_dir(tmp_path)
    before = (version / "03_analysis" / "latest" / "03_findings.json").read_text("utf-8")

    calls = {"n": 0}
    real_copy = _shutil.copy2

    def flaky_copy(src, dst, *args, **kwargs):
        # Ломаем ТОЛЬКО применение (копирование из staging). Восстановление из
        # резервной копии должно работать — иначе тест проверял бы не откат.
        if "unpacked" in str(src):
            calls["n"] += 1
            if calls["n"] > 2:
                raise OSError("диск кончился")
        return real_copy(src, dst, *args, **kwargs)

    monkeypatch.setattr(result_import.shutil, "copy2", flaky_copy)
    with pytest.raises(result_import.ResultImportError) as excinfo:
        result_import.apply_result_package(
            archive=archive, attempt=attempt, version_dir=version,
            settings=center_env,
        )
    assert "откачено" in str(excinfo.value)
    monkeypatch.undo()
    # Исходное состояние восстановлено, новых файлов не осталось.
    assert (version / "03_analysis" / "latest" / "03_findings.json").read_text(
        "utf-8"
    ) == before
    for name in ("a.json", "b.json", "c.json"):
        assert not (version / "03_analysis" / "latest" / name).exists(), name


def test_result_import_records_resume_stage_and_usage(center_env, tmp_path, monkeypatch):
    from backend.app.services.distributed_workers import result_import

    monkeypatch.setenv("AUDIT_PIPELINE_REVISION", "rev-abc123")
    attempt = _center_attempt(center_env, tmp_path)
    archive = _build_result_archive(
        tmp_path, job_id=attempt["job_id"], attempt_id=attempt["attempt_id"]
    )
    report = result_import.apply_result_package(
        archive=archive, attempt=attempt, version_dir=_version_dir(tmp_path),
        settings=center_env,
    )
    assert report["usage_report"]["entries"][0]["stage"] == "findings_merge"
    assert report["stage_completion"] == {"findings_merge": "done"}


def test_usage_report_applies_exactly_once(center_env, tmp_path):
    from backend.app.services.distributed_workers import repositories, result_import

    attempt = _center_attempt(center_env, tmp_path)
    usage = {"entries": [{"stage": "s", "model": "fake",
                          "input_tokens": 1, "output_tokens": 1}]}
    first = result_import.apply_usage_report(
        attempt=attempt, usage_report=usage, settings=center_env
    )
    fresh = repositories.get_attempt(attempt["attempt_id"], settings=center_env)
    second = result_import.apply_usage_report(
        attempt=fresh, usage_report=usage, settings=center_env
    )
    assert first["applied"] is True
    assert second["applied"] is False and second["reason"] == "already_applied"


def test_worker_package_omitting_required_artifact_is_not_successful(tmp_path):
    """Пакет без обязательного артефакта не считается полным (§24)."""
    from audit_worker import audit_runner

    job_dir = tmp_path / "jobs" / "j" / "a"
    audit_runner.prepare_job_dir(job_dir)
    (job_dir / "work" / "pipeline_log.json").write_text("{}", encoding="utf-8")
    missing = audit_runner.missing_required_artifacts(
        job_dir, audit_runner.REQUIRED_RESULT_ARTIFACTS
    )
    assert "result/03_findings.json" in missing
    assert "work/pipeline_log.json" not in missing


def test_path_classification_matches_the_contract():
    from backend.app.services.distributed_workers import result_import

    assert result_import.classify_path("01_input/document.pdf") == "source"
    assert result_import.classify_path("02_work/document.md") == "source"
    assert result_import.classify_path("04_review/expert_review.json") == "source"
    assert result_import.classify_path("version.json") == "source"
    assert result_import.classify_path("03_analysis/latest/03_findings.json") == "worker"
    assert result_import.classify_path("99_service/pipeline_log.json") == "worker"
    assert result_import.classify_path("03_analysis/latest/norm_checks.json") == "central"
    assert result_import.classify_path("что_то_чужое/файл.json") == "unknown"


# ═══ §7 Безопасность ══════════════════════════════════════════════════════════
def test_source_package_contains_no_secrets(tmp_path):
    """E-25: собранный пакет проверяется сканером секретов побайтово."""
    import tarfile

    from backend.app.services.distributed_workers import project_package

    version = _make_version_tree(tmp_path)
    dest = tmp_path / "pkg.tar.gz"
    project_package.build_project_source_package(
        dest_path=dest,
        version_dir=version,
        manifest_base={"manifest_version": 1, "package_id": "p", "job_id": "j",
                       "attempt_id": "a"},
        snapshot_files={},
        feature_flags=project_package.collect_feature_flags_snapshot(
            {"AUDIT_X": "1", "AUDIT_SECRET_TOKEN": "wtk_leak"}
        ),
    )
    blobs: list[tuple[str, bytes]] = []
    with tarfile.open(dest, "r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue
            fh = tar.extractfile(member)
            if fh is not None:
                blobs.append((member.name, fh.read()))
    assert project_package.find_secrets_in_files(blobs) == []


def test_result_package_extraction_rejects_traversal(tmp_path):
    """TAR с `..` отвергается до записи единого байта."""
    import io
    import tarfile

    from backend.app.services.distributed_workers import package_service

    archive = tmp_path / "evil.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        data = b"pwn"
        info = tarfile.TarInfo("payload/../../../etc/evil")
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
    with pytest.raises(package_service.PackageError):
        package_service.safe_extract(archive, tmp_path / "out")
    assert not (tmp_path / "out").exists()


def test_result_package_extraction_rejects_symlink(tmp_path):
    import tarfile

    from backend.app.services.distributed_workers import package_service

    archive = tmp_path / "link.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        info = tarfile.TarInfo("payload/link")
        info.type = tarfile.SYMTYPE
        info.linkname = "/etc/passwd"
        tar.addfile(info)
    with pytest.raises(package_service.PackageError):
        package_service.safe_extract(archive, tmp_path / "out")


def test_worker_unpacker_allows_hardlinks_but_only_inside_payload(tmp_path):
    """Хардлинки нужны (18 % корпуса), но только на уже распакованные записи."""
    import io
    import tarfile

    from audit_worker import package_io

    archive = tmp_path / "bad_link.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        manifest = json.dumps({"manifest_version": 1, "files": [],
                               "archive": {"uncompressed_bytes": 10,
                                           "entries": 2}}).encode()
        info = tarfile.TarInfo("package_manifest.json")
        info.size = len(manifest)
        tar.addfile(info, io.BytesIO(manifest))
        link = tarfile.TarInfo("payload/link")
        link.type = tarfile.LNKTYPE
        link.linkname = "payload/never_seen"
        tar.addfile(link)
    with pytest.raises(package_io.BundleError) as excinfo:
        package_io.verify_and_unpack(
            archive=archive,
            expected_sha256=package_io.sha256_file(archive),
            work_dir=tmp_path / "out",
        )
    assert "ссылка" in str(excinfo.value).lower()


def test_worker_token_gives_no_operator_rights_on_audit_routes(center_env, admin):
    """Машинный контур не открывает операторские маршруты аудита."""
    worker_id, headers = _approved_worker(admin, instance_id="inst_exec_sec")
    from tests.distributed_workers_helpers import SyncASGITransport, make_center_app

    client = httpx.Client(
        transport=SyncASGITransport(make_center_app()),
        base_url="http://center",
        headers={**headers, **INTENT},
    )
    assert client.get("/api/workers/audit/targets").status_code in (401, 403)
    assert client.post(
        "/api/workers/audit/launch",
        json={"worker_id": worker_id, "project_id": "p"},
        headers={**headers, **_key()},
    ).status_code in (401, 403)


def test_audit_launch_requires_operator_and_intent(center_env, admin, operator):
    """Право `operate`, гейт намерения и Idempotency-Key — все три обязательны."""
    from tests.distributed_workers_helpers import SyncASGITransport, make_center_app, session_cookie
    from backend.app.core import portal_auth
    from tests.distributed_workers_helpers import OPERATOR_USER, VIEWER_USER

    worker_id, _ = _approved_worker(admin, instance_id="inst_exec_launch")
    body = {"worker_id": worker_id, "project_id": "ПРО/ект", "action": "full"}

    # Наблюдателю запрещено при любых заголовках.
    viewer = _client(VIEWER_USER)
    assert viewer.post(
        "/api/workers/audit/launch", json=body, headers=_key()
    ).status_code == 403

    # Оператор БЕЗ заголовка намерения (клиент собран без него намеренно).
    bare = httpx.Client(
        transport=SyncASGITransport(make_center_app()), base_url="http://center"
    )
    bare.cookies.set(
        portal_auth.get_settings().cookie_name, session_cookie(OPERATOR_USER)
    )
    assert bare.post(
        "/api/workers/audit/launch", json=body,
        headers={"Idempotency-Key": "k1"},
    ).status_code == 403
    # С намерением, но без ключа идемпотентности.
    assert bare.post(
        "/api/workers/audit/launch", json=body, headers=INTENT
    ).status_code == 400


def test_audit_targets_explains_incompatibility(center_env, admin, monkeypatch):
    monkeypatch.setenv("DISTRIBUTED_AUDIT_EXECUTION_ENABLED", "true")
    _approved_worker(admin, instance_id="inst_exec_targets")
    response = admin.get("/api/workers/audit/targets")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["profile"] == "remote_audit_pilot_v1"
    assert body["norm_stage_location"] == "center"
    assert body["audit_slot_limit"] == 1
    worker = body["workers"][0]
    assert worker["compatible"] is False
    codes = {r["code"] for r in worker["reasons"]}
    # Воркер тестового контура не объявляет audit_pipeline_v1 — причина названа.
    assert "missing_capability" in codes


def test_audit_launch_rejects_incompatible_worker(center_env, admin, operator, monkeypatch):
    monkeypatch.setenv("DISTRIBUTED_AUDIT_EXECUTION_ENABLED", "true")
    worker_id, _ = _approved_worker(admin, instance_id="inst_exec_incompat")
    response = operator.post(
        "/api/workers/audit/launch",
        json={"worker_id": worker_id, "project_id": "ПРО/ект"},
        headers=_key(),
    )
    assert response.status_code == 409
    assert response.json()["detail"]["error"] == "worker_incompatible"


def test_launch_model_forbids_extra_fields():
    import pydantic

    from backend.app.models.distributed_workers import RemoteAuditLaunchRequest

    with pytest.raises(pydantic.ValidationError):
        RemoteAuditLaunchRequest(
            worker_id="w", project_id="p", command="rm -rf /"
        )


def test_audit_params_model_forbids_execution_fields():
    import pydantic

    from backend.app.models.distributed_workers import AuditPipelineParams

    base = dict(
        pipeline_revision="rev", expected_source_tree_hash="sha256:" + "a" * 64,
        prompt_bundle_hash="sha256:" + "b" * 64,
        model_config_hash="sha256:" + "c" * 64,
        feature_flags_hash="sha256:" + "d" * 64,
        runtime_snapshot_hash="sha256:" + "e" * 64,
    )
    AuditPipelineParams(**base)          # базовая форма валидна
    for field in ("command", "argv", "executable", "script", "module", "cwd", "env"):
        with pytest.raises(pydantic.ValidationError):
            AuditPipelineParams(**base, **{field: "x"})
    with pytest.raises(pydantic.ValidationError):
        AuditPipelineParams(**{**base, "include_norms": True})
    with pytest.raises(pydantic.ValidationError):
        AuditPipelineParams(**{**base, "retry_stage": "norm_verify"})


# ═══════════════════════════════════════════════════════════════════════════
# §8. Исправления по адверсариальным проверкам
#
# Каждый тест ниже закрепляет ОДИН подтверждённый дефект. Без них правки
# держатся на честном слове: все эти дефекты были в коде, который уже проходил
# 447 тестов.
# ═══════════════════════════════════════════════════════════════════════════
def test_central_stages_are_blocked_in_the_remote_process():
    """Норм-этап, долги, carryover и Excel на воркере не выполняются.

    Проверка `FORBIDDEN_STAGES` сверяла только явный `retry_stage`, поэтому
    `action="full"` спокойно гонял все четыре центральных этапа: `include_norms`
    из спеки в `BatchQueueItem` не переносился вовсе, а `_run_ocr_pipeline`
    решает по флагу платформы, а не по элементу очереди.
    """
    import asyncio

    from backend.app.models.audit import AuditJob
    from backend.app.pipeline.execution.registry import CENTRAL_STAGES_DISABLED_ENV
    from backend.app.pipeline.manager import PipelineManager

    manager = object.__new__(PipelineManager)
    manager.active_jobs = {}
    manager._tasks = {}
    logged: list[str] = []

    async def _log(job, message, level="info"):
        logged.append(message)

    manager._log = _log
    job = AuditJob(job_id="j", project_id="ПРО/удалённый")

    prev = os.environ.get(CENTRAL_STAGES_DISABLED_ENV)
    try:
        os.environ.pop(CENTRAL_STAGES_DISABLED_ENV, None)
        assert manager._central_stage_blocked("norm_verify") is False
        os.environ[CENTRAL_STAGES_DISABLED_ENV] = "1"
        assert manager._central_stage_blocked("norm_verify") is True
        # Все четыре метода выходят сразу и не зовут ни один runner.
        asyncio.run(manager._run_debt_control(job))
        asyncio.run(manager._run_decision_carryover(job))
        asyncio.run(manager._run_norm_verification(job, standalone=False))
    finally:
        if prev is None:
            os.environ.pop(CENTRAL_STAGES_DISABLED_ENV, None)
        else:
            os.environ[CENTRAL_STAGES_DISABLED_ENV] = prev
    assert len(logged) == 3, logged
    assert all("на центре" in m for m in logged), logged


def test_remote_runner_hardens_environment_before_config_import():
    """Гейты выставляются до импорта конфигурации, иначе они бесполезны."""
    import subprocess

    code = (
        "import sys, os\n"
        f"sys.path.insert(0, {str(_ROOT)!r})\n"
        "from backend.app.pipeline import remote_audit_runner as r\n"
        "r.harden_process_env()\n"
        "assert os.environ['AUDIT_DISABLE_DOTENV'] == '1'\n"
        "assert 'backend.app.core.config' not in sys.modules, 'конфиг импортирован раньше гейта'\n"
        "print('ok')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    assert "ok" in result.stdout


def test_config_honours_dotenv_kill_switch():
    """`AUDIT_DISABLE_DOTENV=1` не даёт `.env` вернуть окружение центра."""
    import subprocess

    env_file = _ROOT / ".env"
    if not env_file.is_file():
        pytest.skip(".env в этом окружении нет — проверять нечего")
    code = (
        "import sys, os\n"
        f"sys.path.insert(0, {str(_ROOT)!r})\n"
        "from backend.app.core import config\n"
        "print('LEAK' if os.environ.get('PAID_API_ENABLED') is not None else 'clean')\n"
    )
    clean_env = {
        "PATH": os.environ.get("PATH", ""),
        "HOME": os.environ.get("HOME", ""),
        "AUDIT_DISABLE_DOTENV": "1",
    }
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True,
        timeout=180, env=clean_env,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    assert "clean" in result.stdout, result.stdout


def test_fake_mode_removes_provider_keys_and_binds_cli():
    """Поддельный режим гасит платный HTTP и точки резолва мимо PATH."""
    from backend.app.pipeline import remote_audit_runner
    from backend.app.pipeline.execution import fake_providers

    with tempfile.TemporaryDirectory() as tmp:
        provider_dir = fake_providers.materialize(Path(tmp) / "providers")
        saved = {
            name: os.environ.get(name)
            for name in (
                "AUDIT_WORKER_FAKE_PROVIDER_DIR", "PAID_API_ENABLED",
                "OPENROUTER_API_KEY", "CLAUDE_CLI_BIN", "AUDIT_CODEX_CLI_PATH",
                "CODEX_CLI_PATH",
            )
        }
        try:
            os.environ["AUDIT_WORKER_FAKE_PROVIDER_DIR"] = str(provider_dir)
            os.environ["OPENROUTER_API_KEY"] = "sk-or-v1-" + "z" * 40
            report = remote_audit_runner.enforce_fake_providers(
                {"provider_mode": "fake"}
            )
            assert report["mode"] == "fake"
            assert os.environ["PAID_API_ENABLED"] == "false"
            assert "OPENROUTER_API_KEY" not in os.environ
            for name in ("CLAUDE_CLI_BIN", "AUDIT_CODEX_CLI_PATH", "CODEX_CLI_PATH"):
                assert os.environ[name].startswith(str(provider_dir))
        finally:
            for name, value in saved.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value


def test_fake_mode_refuses_directory_without_marker():
    """Существующего каталога недостаточно — нужен подтверждённый маркер."""
    from backend.app.pipeline import remote_audit_runner

    with tempfile.TemporaryDirectory() as tmp:
        saved = os.environ.get("AUDIT_WORKER_FAKE_PROVIDER_DIR")
        try:
            os.environ["AUDIT_WORKER_FAKE_PROVIDER_DIR"] = tmp   # пустой каталог
            with pytest.raises(SystemExit):
                remote_audit_runner.enforce_fake_providers({"provider_mode": "fake"})
        finally:
            if saved is None:
                os.environ.pop("AUDIT_WORKER_FAKE_PROVIDER_DIR", None)
            else:
                os.environ["AUDIT_WORKER_FAKE_PROVIDER_DIR"] = saved


def test_executor_rejects_provider_dir_that_is_not_fake():
    """Каталог с настоящими CLI не проходит как поддельный."""
    from audit_worker import audit_runner as worker_runner

    with tempfile.TemporaryDirectory() as tmp:
        real_like = Path(tmp)
        (real_like / "somebinary").write_text("#!/bin/sh\n", encoding="utf-8")
        assert worker_runner.provider_dir_is_fake(real_like) is False

    from backend.app.pipeline.execution import fake_providers

    with tempfile.TemporaryDirectory() as tmp:
        good = fake_providers.materialize(Path(tmp) / "p")
        assert worker_runner.provider_dir_is_fake(good) is True


def test_worker_env_disables_dotenv():
    """Белый список окружения не должен пробиваться `.env` из pipeline_root."""
    from audit_worker import audit_runner as worker_runner

    class _Cfg:
        pipeline_root = "/opt/audit-manager"
        pipeline_python = sys.executable

    env = worker_runner.build_env(
        config=_Cfg(), job_dir=Path("/var/lib/audit-worker/jobs/j/a"),
        provider_dir=None,
    )
    assert env["AUDIT_DISABLE_DOTENV"] == "1"


def test_project_id_from_job_is_validated_as_a_path():
    """`project_id` — часть пути; `..` и абсолютный путь отвергаются."""
    from backend.app.pipeline import remote_audit_runner

    assert remote_audit_runner.validate_project_id("АР/133-23-ГК-АР5") == "АР/133-23-ГК-АР5"
    for bad in (
        "", "   ", "../../../../etc", "/home/coder", "АР/../../../home",
        "~/секрет", "АР/./../..", "x" * 301,
    ):
        with pytest.raises(SystemExit):
            remote_audit_runner.validate_project_id(bad)


def test_runner_writes_process_exit_marker(tmp_path):
    """Второй источник «дошёл до конца сам» — иначе рестарт теряет результат."""
    from backend.app.pipeline import remote_audit_runner

    work = tmp_path / "work"
    work.mkdir()
    spec = {"paths": {"work": str(work)}, "job_id": "j", "attempt_id": "a"}
    remote_audit_runner.write_process_exit(spec, 0)
    payload = json.loads((work / "process_exit.json").read_text(encoding="utf-8"))
    assert payload["exit_code"] == 0
    assert payload["attempt_id"] == "a"


def test_pending_remote_item_does_not_freeze_the_queue():
    """Незапущенный удалённый элемент не считается живым аудитом."""
    from backend.app.models.audit import BatchQueueItem, BatchQueueStatus
    from backend.app.pipeline.manager import PipelineManager

    manager = object.__new__(PipelineManager)
    manager.active_jobs = {}
    manager._tasks = {}
    manager._batch_queue = BatchQueueStatus(
        queue_id="q",
        items=[
            BatchQueueItem(
                project_id="ПРО/ждёт", status="pending",
                execution_mode="remote_worker", worker_id="wrk_1",
            )
        ],
        total=1,
    )
    assert manager._has_live_project_audit() is False
    assert manager._remote_items() == {}


def test_interrupted_remote_item_is_protected_but_resumable():
    """`interrupted` защищён от зомби-уборки, но resume не блокирует."""
    from backend.app.models.audit import BatchQueueItem, BatchQueueStatus
    from backend.app.pipeline.manager import PipelineManager

    manager = object.__new__(PipelineManager)
    manager.active_jobs = {}
    manager._tasks = {}
    handle = {
        "backend_type": "remote_worker", "handle_id": "att", "project_id": "ПРО/жив",
        "attempt_id": "att", "remote_job_id": "job", "worker_id": "wrk_1",
    }
    manager._batch_queue = BatchQueueStatus(
        queue_id="q",
        items=[
            BatchQueueItem(
                project_id="ПРО/жив", status="interrupted",
                execution_mode="remote_worker", worker_id="wrk_1",
                execution_handle=handle,
            )
        ],
        total=1,
    )
    assert "ПРО/жив" in manager._remote_items()
    assert "ПРО/жив" in manager._protected_pids()
    assert manager._has_live_project_audit() is False


def test_cancel_does_not_hijack_a_live_local_audit():
    """Отмена живого локального аудита не уходит в удалённый элемент."""
    import asyncio

    from backend.app.models.audit import (
        AuditJob,
        BatchQueueItem,
        BatchQueueStatus,
        JobStatus,
    )
    from backend.app.pipeline.manager import PipelineManager

    manager = object.__new__(PipelineManager)
    manager._tasks = {}
    manager._heartbeat_tasks = {}
    local = AuditJob(job_id="j1", project_id="ПРО/оба", version_id="v1")
    local.status = JobStatus.RUNNING
    manager.active_jobs = {"ПРО/оба": local}
    remote = BatchQueueItem(
        project_id="ПРО/оба", version_id="v2", status="pending",
        execution_mode="remote_worker", worker_id="wrk_1",
    )
    manager._batch_queue = BatchQueueStatus(
        queue_id="q", items=[remote], total=1, status="running",
    )
    called: list[str] = []

    async def _fake_cancel_remote(item):
        called.append(item.project_id)
        return True

    manager._cancel_remote_item = _fake_cancel_remote
    manager._cleanup = lambda pid: None

    async def _noop_kill(pid):
        return 0

    import backend.app.pipeline.manager as manager_module

    saved_kill = manager_module.kill_all_processes
    manager_module.kill_all_processes = _noop_kill
    try:
        assert asyncio.run(manager.cancel("ПРО/оба")) is True
    finally:
        manager_module.kill_all_processes = saved_kill
    assert called == [], "отмена ушла в удалённый элемент вместо живого локального"
    assert local.status == JobStatus.CANCELLED
    assert remote.status == "pending", "удалённый элемент помечен отменённым напрасно"


def test_cancel_in_result_uploading_answers_instead_of_500(center_env, admin, operator):
    """Отмена во время возврата результата — понятный ответ, а не исключение."""
    from backend.app.services.distributed_workers import (
        attempt_service,
        job_service,
        repositories,
    )
    from backend.app.models.distributed_workers import JobState

    worker_id, _ = _approved_worker(admin, instance_id="inst_finishing")
    job = _create_job(admin, worker_id, project="ИСП/выгрузка")
    repositories.claim_next_job_for_worker(worker_id, settings=center_env)
    for state in (
        JobState.SOURCE_READY, JobState.ACCEPTED_BY_WORKER, JobState.RUNNING,
        JobState.COMPLETED_LOCALLY, JobState.RESULT_UPLOADING,
    ):
        job_service.transition(
            attempt_id=job["attempt_id"], to_state=state, actor="worker",
            reason="ход прогона", settings=center_env,
        )
    view = attempt_service.attempts_view(
        job_id=job["job_id"], settings=center_env,
    )
    assert view[0]["can_cancel"] is False, "интерфейс предлагает невозможную отмену"
    result = attempt_service.request_cancel(
        job_id=job["job_id"], attempt_id=job["attempt_id"],
        reason="передумал", confirmation=attempt_service.CONFIRM_CANCEL,
        actor="operator:test", idempotency_key="cancel-finishing-1",
        settings=center_env,
    )
    assert result["outcome"] == "already_finishing"
    assert result["command_id"] is None


def test_wait_stops_when_operator_declares_the_attempt_lost(center_env, admin, operator):
    """Ожидание не крутится вечно на попытке, признанной потерянной."""
    import asyncio

    from backend.app.pipeline.execution.contracts import (
        ExecutionContext,
        ExecutionHandle,
        ExecutionMode,
    )
    from backend.app.pipeline.execution.remote import RemoteWorkerExecutionBackend
    from backend.app.services.distributed_workers import attempt_service, repositories

    worker_id, _ = _approved_worker(admin, instance_id="inst_lost_wait")
    job = _create_job(admin, worker_id, project="ИСП/потеряна")
    repositories.claim_next_job_for_worker(worker_id, settings=center_env)
    attempt_service.mark_lost(
        job_id=job["job_id"], attempt_id=job["attempt_id"],
        reason="VPS не отвечает сутки",
        typed_confirmation=attempt_service.CONFIRM_MARK_LOST,
        actor="operator:test", idempotency_key="lost-wait-1", settings=center_env,
    )
    handle = ExecutionHandle(
        backend_type=ExecutionMode.REMOTE_WORKER, handle_id=job["attempt_id"],
        project_id="ИСП/потеряна", attempt_id=job["attempt_id"],
        remote_job_id=job["job_id"], worker_id=worker_id,
    )
    backend = RemoteWorkerExecutionBackend(_RecordingManager())
    ctx = ExecutionContext(item=None, job=None)
    result = asyncio.run(backend.wait(handle, ctx))
    assert result.success is False
    assert "потерянной" in (result.error or "")


def test_handle_is_not_reused_for_a_lost_attempt(center_env, admin, operator):
    """Ссылка на признанную потерянной попытку не переиспользуется."""
    from backend.app.pipeline.execution.remote import RemoteWorkerExecutionBackend

    assert RemoteWorkerExecutionBackend._handle_still_valid(
        {"attempt_disposition": "active"}
    ) is True
    assert RemoteWorkerExecutionBackend._handle_still_valid(
        {"attempt_disposition": "operator_declared_lost"}
    ) is False
    assert RemoteWorkerExecutionBackend._handle_still_valid(
        {"attempt_disposition": "active", "superseded_by_attempt": "att_2"}
    ) is False


def test_usage_report_is_recorded_with_the_real_signature(center_env, admin, monkeypatch):
    """Отчёт о расходе действительно ложится в трекер, а не теряется."""
    from backend.app.services.common import usage_service
    from backend.app.services.distributed_workers import repositories, result_import

    worker_id, _ = _approved_worker(admin, instance_id="inst_usage_sig")
    job = _create_job(admin, worker_id, project="ИСП/расход")
    attempt = repositories.get_attempt(job["attempt_id"], settings=center_env)

    seen: list[Any] = []
    monkeypatch.setattr(
        usage_service.usage_tracker, "record_usage", lambda record: seen.append(record)
    )
    report = result_import.apply_usage_report(
        attempt=attempt,
        usage_report={
            "entries": [
                {
                    "stage": "block_analysis", "model": "fake/model",
                    "input_tokens": 10, "output_tokens": 5, "calls": 2,
                    "duration_ms": 1200, "cost_usd": 0.0,
                }
            ]
        },
        settings=center_env,
    )
    assert report == {"applied": True, "entries": 1, "errors": []}
    assert len(seen) == 1
    assert seen[0].project_id == attempt["project_id"]
    assert seen[0].input_tokens == 10
    assert seen[0].stage == "block_analysis"


def test_usage_report_failure_does_not_mark_applied(center_env, admin, monkeypatch):
    """Провал записи не ставит отметку — иначе расход теряется навсегда."""
    from backend.app.services.common import usage_service
    from backend.app.services.distributed_workers import repositories, result_import

    worker_id, _ = _approved_worker(admin, instance_id="inst_usage_fail")
    job = _create_job(admin, worker_id, project="ИСП/расход2")
    attempt = repositories.get_attempt(job["attempt_id"], settings=center_env)

    def _boom(record):
        raise RuntimeError("трекер недоступен")

    monkeypatch.setattr(usage_service.usage_tracker, "record_usage", _boom)
    report = result_import.apply_usage_report(
        attempt=attempt,
        usage_report={"entries": [{"stage": "s", "model": "m"}]},
        settings=center_env,
    )
    assert report["applied"] is False
    assert report["reason"] == "record_failed"
    fresh = repositories.get_attempt(job["attempt_id"], settings=center_env)
    assert not fresh.get("usage_applied_at")


def test_central_artifacts_never_leave_the_center_in_the_source_package(tmp_path):
    """Асимметрия закрыта: центральные артефакты не уезжают на воркер."""
    from backend.app.services.distributed_workers import project_package

    version = tmp_path / "версия"
    (version / "03_analysis" / "latest").mkdir(parents=True)
    (version / "03_analysis" / "latest" / "03_findings.json").write_text(
        "{}", encoding="utf-8"
    )
    for name in (
        "norm_checks.json", "03a_norms_verified.json",
        "decision_carryover_report.json", "expert_review.json",
    ):
        (version / "03_analysis" / "latest" / name).write_text("{}", encoding="utf-8")
    scan = project_package.scan_version_tree(version)
    names = {Path(rel).name for _abs, rel in scan.files}
    assert "03_findings.json" in names
    for forbidden in (
        "norm_checks.json", "03a_norms_verified.json",
        "decision_carryover_report.json", "expert_review.json",
    ):
        assert forbidden not in names, f"{forbidden} уехал бы на воркер"


def test_expert_review_from_worker_is_treated_as_central():
    """Разметка эксперта не применяется из пакета, даже внутри 03_analysis/."""
    from backend.app.services.distributed_workers import result_import

    assert result_import.classify_path("03_analysis/latest/expert_review.json") == "central"
    assert result_import.classify_path("04_review/expert_review.json") == "source"
    assert result_import.classify_path("03_analysis/latest/03_findings.json") == "worker"


def test_secret_scanner_catches_modern_key_formats():
    """Сканер ловит формы, которые реально встречаются в ключах провайдеров."""
    from backend.app.services.distributed_workers import project_package

    samples = {
        "ant": b"ANTHROPIC_API_KEY=sk-ant-api03-" + b"q" * 40,
        "or": b'{"OPENROUTER_API_KEY": "sk-or-v1-' + b"w" * 40 + b'"}',
        "pem": b"-----BEGIN RSA PRIVATE KEY-----\nabc\n",
        "jwt": b"token: eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.sig",
        "aws": b"AKIAIOSFODNN7EXAMPLE",
        "dsn": b"postgres://user:secretpass@host/db",
        "bearer": b'Authorization: Bearer abcdefghij',
    }
    for label, blob in samples.items():
        hits = project_package.find_secrets_in_files([(label, blob)])
        assert hits, f"не поймано: {label}"
    clean = project_package.find_secrets_in_files(
        [("ok", "нормальный текст без секретов".encode("utf-8"))]
    )
    assert clean == []


def test_feature_flags_blob_is_scanned_for_secrets(center_env, admin, tmp_path, monkeypatch):
    """Блоб флагов тоже проверяется — фильтр по именам ловит не всё.

    `collect_feature_flags_snapshot` фильтрует ИМЕНА ключей, а значения не
    инспектирует; сам блоб уезжает в архив отдельной записью, и раньше он в
    сканирование не попадал вовсе.
    """
    from backend.app.models.distributed_workers import AuditPipelineParams
    from backend.app.services.distributed_workers import audit_job_service

    version = tmp_path / "версия"
    version.mkdir()
    (version / "project_info.json").write_text("{}", encoding="utf-8")
    params = AuditPipelineParams(
        pipeline_revision="rev",
        expected_source_tree_hash="sha256:" + "a" * 64,
        prompt_bundle_hash="sha256:" + "0" * 64,
        model_config_hash="sha256:" + "1" * 64,
        feature_flags_hash="sha256:" + "2" * 64,
        runtime_snapshot_hash="sha256:" + "3" * 64,
    )
    snapshot = {
        "files": {"prompts/task.md": "шаблон".encode("utf-8")},
        # Имя ключа фильтр не ловит, значение — настоящий ключ провайдера.
        "feature_flags": {"AUDIT_SOME_VALUE": "sk-ant-api03-" + "e" * 40},
        "prompt_bundle_hash": "sha256:" + "0" * 64,
        "model_config_hash": "sha256:" + "1" * 64,
        "feature_flags_hash": "sha256:" + "2" * 64,
    }
    job = {
        "job_id": str(uuid.uuid4()), "attempt_id": str(uuid.uuid4()),
        "project_id": "ПРО/скан", "version_id": "v1",
        "project_external_id": "ПРО/скан", "display_name": "ПРО/скан",
    }
    with pytest.raises(audit_job_service.AuditJobError) as excinfo:
        audit_job_service.build_audit_source_package(
            job=job, version_dir=version, params=params, snapshot=snapshot,
            compression="none", settings=center_env,
        )
    assert "секрет" in str(excinfo.value)
    # Архив не остался ни в каком виде: проверка идёт ДО записи.
    dest_dir = (
        Path(center_env.source_packages_dir) / job["job_id"] / job["attempt_id"]
    )
    leftovers = sorted(dest_dir.glob("*")) if dest_dir.is_dir() else []
    assert leftovers == [], leftovers


def test_worker_state_blocks_new_work():
    """Воркер в drain/degraded не получает новую работу."""
    from backend.app.services.distributed_workers import slots
    from backend.app.services.distributed_workers.settings import get_settings

    base = {
        "registration_status": "approved",
        "connection_status": "online",
        "configured_max_slots": 2,
        "reported_max_slots": 2,
        "max_verified_slots": 2,
        "executor_status": "idle",
        "disk_level": "ok",
    }
    settings = get_settings()
    ok = slots.effective_limit(base)
    assert ok.value >= 1
    for state in ("draining", "drained", "degraded", "revoked"):
        limited = slots.effective_limit({**base, "worker_state": state})
        assert limited.value == 0, state
        assert state in (limited.blocked_reason or "")


def test_precrop_skips_remote_items():
    """Центр не кропает проект, который поедет на воркер со своим пакетом."""
    from backend.app.models.audit import BatchQueueItem, BatchQueueStatus
    from backend.app.pipeline.manager import PipelineManager

    manager = object.__new__(PipelineManager)
    queue = BatchQueueStatus(
        queue_id="q",
        items=[
            BatchQueueItem(
                project_id="ПРО/удалённый", status="pending",
                execution_mode="remote_worker", worker_id="wrk_1", action="full",
            )
        ],
        total=1, action="full",
    )
    assert manager._select_precrop_candidate(queue, set()) is None


def test_second_remote_launch_after_restart_is_refused(monkeypatch):
    """Повторный запуск в окне рестарта не создаёт второй платный аудит."""
    import asyncio

    from backend.app.models.audit import BatchQueueItem, BatchQueueStatus
    from backend.app.pipeline.manager import PipelineManager

    manager = object.__new__(PipelineManager)
    manager.active_jobs = {}
    manager._tasks = {}
    manager._enqueue_lock = asyncio.Lock()
    handle = {
        "backend_type": "remote_worker", "handle_id": "att", "project_id": "ПРО/один",
        "attempt_id": "att", "remote_job_id": "job", "worker_id": "wrk_1",
    }
    manager._batch_queue = BatchQueueStatus(
        queue_id="q",
        items=[
            BatchQueueItem(
                project_id="ПРО/один", version_id="v1", status="interrupted",
                execution_mode="remote_worker", worker_id="wrk_1",
                execution_handle=handle,
            )
        ],
        total=1, status="interrupted",
    )

    import backend.app.pipeline.manager as manager_module

    class _VS:
        VersionNotFoundError = RuntimeError

        @staticmethod
        def resolve_effective_version_id(*_a, **_k):
            return "v1"

        @staticmethod
        def get_version_entry(*_a, **_k):
            return {}

    monkeypatch.setattr(
        manager_module, "resolve_project_dir", lambda pid: Path("/tmp/нет"),
    )
    monkeypatch.setitem(
        sys.modules, "backend.app.services.common.version_service", _VS,
    )
    with pytest.raises(RuntimeError) as excinfo:
        asyncio.run(
            manager._enqueue_single(
                "ПРО/один", action="full", version_id="v1",
                execution_mode="remote_worker", worker_id="wrk_1",
            )
        )
    assert "удалённое исполнение" in str(excinfo.value)


def test_clear_queue_history_refuses_while_remote_is_alive():
    """Историю нельзя стереть, пока в ней единственная ссылка на живую попытку."""
    from backend.app.models.audit import BatchQueueItem, BatchQueueStatus
    from backend.app.pipeline.manager import PipelineManager

    manager = object.__new__(PipelineManager)
    manager.active_jobs = {}
    manager._tasks = {}
    handle = {
        "backend_type": "remote_worker", "handle_id": "att", "project_id": "ПРО/жив",
        "attempt_id": "att", "remote_job_id": "job", "worker_id": "wrk_1",
    }
    manager._batch_queue = BatchQueueStatus(
        queue_id="q",
        items=[
            BatchQueueItem(
                project_id="ПРО/жив", status="interrupted",
                execution_mode="remote_worker", worker_id="wrk_1",
                execution_handle=handle,
            )
        ],
        total=1, status="interrupted",
    )
    with pytest.raises(RuntimeError) as excinfo:
        manager.clear_queue_history()
    assert "продолжаются на воркере" in str(excinfo.value)


def test_item_with_handle_is_never_run_locally():
    """Потерянный worker_id при живой ссылке не даёт локального дубля."""
    from backend.app.models.audit import BatchQueueItem
    from backend.app.pipeline.execution import registry
    from backend.app.pipeline.execution.contracts import ExecutionMode

    plain = BatchQueueItem(project_id="p", execution_mode="remote_worker")
    assert registry.item_execution_mode(plain) is ExecutionMode.LOCAL

    with_handle = BatchQueueItem(
        project_id="p", execution_mode="local",
        execution_handle={
            "backend_type": "remote_worker", "handle_id": "att", "project_id": "p",
            "attempt_id": "att", "remote_job_id": "job", "worker_id": "wrk_1",
        },
    )
    assert registry.item_execution_mode(with_handle) is ExecutionMode.REMOTE_WORKER


def test_expired_command_is_not_reused(center_env, admin, operator):
    """Протухшая команда не переиспользуется как «незавершённая»."""
    from backend.app.models.distributed_workers import JobState, WorkerCommandType
    from backend.app.services.distributed_workers import (
        attempt_service,
        job_service,
        repositories,
    )

    worker_id, _ = _approved_worker(admin, instance_id="inst_expired_cmd")
    job = _create_job(admin, worker_id, project="ИСП/протухла")
    repositories.claim_next_job_for_worker(worker_id, settings=center_env)
    for state in (JobState.SOURCE_READY, JobState.ACCEPTED_BY_WORKER, JobState.RUNNING):
        job_service.transition(
            attempt_id=job["attempt_id"], to_state=state, actor="worker",
            reason="ход прогона", settings=center_env,
        )
    first = attempt_service.request_cancel(
        job_id=job["job_id"], attempt_id=job["attempt_id"], reason="раз",
        confirmation=attempt_service.CONFIRM_CANCEL, actor="operator:test",
        idempotency_key="exp-cancel-1", settings=center_env,
    )
    assert first["command_id"]
    # Команда протухла, не будучи подтверждённой: истечение делает штатный
    # `expire_stale_commands`, которому достаточно сдвинуть «сейчас» вперёд.
    expired = repositories.expire_stale_commands(
        now=time.time() + 30 * 86_400, settings=center_env,
    )
    assert expired >= 1
    second = attempt_service.request_cancel(
        job_id=job["job_id"], attempt_id=job["attempt_id"], reason="два",
        confirmation=attempt_service.CONFIRM_CANCEL, actor="operator:test",
        idempotency_key="exp-cancel-2", settings=center_env,
    )
    assert second["command_id"] != first["command_id"], (
        "переиспользована протухшая команда — воркер её уже не получит"
    )
    commands = [
        c for c in repositories.commands_for_job(job["job_id"], settings=center_env)
        if c["command_type"] == WorkerCommandType.CANCEL_ATTEMPT.value
    ]
    assert len(commands) == 2


def test_result_import_resume_hint_uses_the_real_detector(monkeypatch):
    """Подсказка возобновления считается детектором, а не всегда None."""
    from backend.app.services.distributed_workers import result_import

    seen: dict[str, Any] = {}

    def _fake_detect(project_id, *, version_id=None):
        seen["project_id"] = project_id
        seen["version_id"] = version_id
        return {"stage": "norm_verify"}

    import backend.app.pipeline.resume_detector as detector

    monkeypatch.setattr(detector, "detect_resume_stage", _fake_detect)
    stage = result_import._detect_resume_stage(
        {"project_id": "ПРО/резюме", "version_id": "v3"}
    )
    assert stage == "norm_verify"
    assert seen == {"project_id": "ПРО/резюме", "version_id": "v3"}


def test_forbidden_prefixes_match_the_audit_layout(tmp_path):
    """Первый рубеж валидации видит вложенную раскладку пакета аудита.

    Пути аудита лежат под `payload/project/…`, а сравнение шло только с началом
    `rel` — ни одно совпадение не было возможно, и рубеж был мёртв.
    """
    import tarfile

    from backend.app.services.distributed_workers import package_service

    payload = tmp_path / "sample"
    payload.write_bytes(b"x")
    manifest = tmp_path / package_service.MANIFEST_NAME
    manifest.write_text(
        json.dumps(
            {
                "package_type": "result", "manifest_version": 1,
                "job_id": "j", "attempt_id": "a", "compression": "none",
                "archive": {"uncompressed_bytes": 10},
            }
        ),
        encoding="utf-8",
    )
    archive = tmp_path / "res.tar"
    with tarfile.open(archive, "w") as tar:
        tar.add(manifest, arcname=package_service.MANIFEST_NAME)
        tar.add(payload, arcname="payload/project/01_input/document.pdf")
        tar.add(payload, arcname="payload/project/03_analysis/latest/03_findings.json")
    report = package_service.validate_result_package(
        archive=archive,
        expected_hash=package_service.sha256_file(archive),
        expected_size=archive.stat().st_size,
        job_id="j", attempt_id="a", required_artifacts=[],
    )
    hits = report.checks["4_artifacts"]["forbidden_hits"]
    assert any("01_input" in h for h in hits), hits
