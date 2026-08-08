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
import sys
import time
import uuid
from pathlib import Path

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
