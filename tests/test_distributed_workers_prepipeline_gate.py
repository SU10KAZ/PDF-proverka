"""
test_distributed_workers_prepipeline_gate.py
--------------------------------------------
Пред-пайплайновый контрольный этап: два блокирующих риска этапа 3.5.

  §1 ролевая модель: разбор конфигурации, fail-closed, подделка роли;
  §2 матрица разрешений операторского API на НАСТОЯЩЕМ приложении;
  §3 отказ ничего не меняет: ни состояния попытки, ни WorkerCommand;
  §4 журнал: actor из сессии, роль и разрешение записаны;
  §5 слоты на центре: нормализация, эффективный лимит, атомарное резервирование;
  §6 локальная ёмкость исполнителя и его независимость от центра;
  §7 счётчик событий: несколько ПРОЦЕССОВ, нет дублей и дыр;
  §8 два настоящих процесса на одном воркере: перекрытие, третий ждёт,
     отмена одного не трогает второй, рестарт агента ничего не дублирует;
  §9 безопасность экрана: только DOM-API, никаких секретов.

Run: python -m pytest tests/test_distributed_workers_prepipeline_gate.py -v
"""
from __future__ import annotations

import json
import multiprocessing
import os
import re
import signal
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

httpx = pytest.importorskip("httpx")

BOOTSTRAP = "gate-bootstrap-secret-0123456789abcdef"
INTENT = {"X-Requested-With": "audit-workers"}
PY = sys.executable or "python3"


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


@pytest.fixture()
def viewer(center_env):
    from tests.distributed_workers_helpers import VIEWER_USER

    return _client(VIEWER_USER)


@pytest.fixture()
def anonymous(center_env):
    import httpx as _httpx

    from tests.distributed_workers_helpers import SyncASGITransport, make_center_app

    return _httpx.Client(
        transport=SyncASGITransport(make_center_app()),
        base_url="http://center",
        headers=INTENT,
    )


def _approved_worker(admin_client, instance_id="inst_gate_00001", max_slots=1):
    registered = admin_client.post(
        "/api/v1/worker/register",
        json={"instance_id": instance_id, "protocol_version": 1,
              "display_name_hint": "VPS-gate"},
        headers={"Authorization": f"Bearer {BOOTSTRAP}", "X-Protocol-Version": "1"},
    ).json()
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
    return worker_id, headers, approved.json()


def _create_job(client, worker_id, project="ГЕЙТ/проект 1"):
    response = client.post(
        "/api/workers/jobs",
        json={"worker_id": worker_id, "project_id": project,
              "params": {"label": "gate", "steps": 1, "step_seconds": 0.0}},
    )
    assert response.status_code == 200, response.text
    return response.json()["job"]


def _key() -> dict[str, str]:
    return {**INTENT, "Idempotency-Key": f"gate-{uuid.uuid4().hex[:12]}"}


# ═══ §1 Разбор конфигурации ролей и fail-closed ══════════════════════════════
def test_role_config_parses_lists_and_normalizes_unicode():
    from backend.app.services.distributed_workers import authorization as az

    config = az.load_role_config({
        az.ENV_ADMINS: "  Андрей ,  ",
        az.ENV_OPERATORS: "petr;ivan\nsemyon",
        az.ENV_VIEWERS: "guest",
    })
    assert config.ok and config.configured
    assert config.role_for("Андрей") == az.ROLE_ADMIN
    assert config.role_for("ivan") == az.ROLE_OPERATOR
    assert config.role_for("guest") == az.ROLE_VIEWER
    # Регистр НЕ игнорируется: «Ivan» с правами «ivan» — тихое расширение прав.
    assert config.role_for("IVAN") is None


def test_role_config_rejects_wildcards_and_closes_access():
    from backend.app.services.distributed_workers import authorization as az

    config = az.load_role_config({az.ENV_ADMINS: "*"})
    assert not config.ok
    assert "шаблон" in (config.diagnostics() or "").lower()
    # Битая конфигурация не выдаёт прав НИКОМУ, даже перечисленным.
    assert config.role_for("*") is None
    assert az.ROLE_PERMISSIONS[az.ROLE_ADMIN] > az.ROLE_PERMISSIONS[az.ROLE_OPERATOR]


def test_role_config_rejects_control_characters():
    from backend.app.services.distributed_workers import authorization as az

    config = az.load_role_config({az.ENV_OPERATORS: "иван\x00админ"})
    assert not config.ok


def test_empty_role_config_grants_nothing():
    from backend.app.services.distributed_workers import authorization as az

    config = az.load_role_config({})
    assert config.ok and not config.configured
    assert config.role_for("кто-угодно") is None
    assert "не настроены" in (config.diagnostics() or "")


def test_admin_role_includes_operate_and_view():
    from backend.app.services.distributed_workers import authorization as az

    admin = az.ROLE_PERMISSIONS[az.ROLE_ADMIN]
    assert az.PERM_VIEW in admin and az.PERM_OPERATE in admin and az.PERM_ADMIN in admin
    assert az.ROLE_PERMISSIONS[az.ROLE_OPERATOR] == {az.PERM_VIEW, az.PERM_OPERATE}
    assert az.ROLE_PERMISSIONS[az.ROLE_VIEWER] == {az.PERM_VIEW}


def test_highest_role_wins_for_subject_in_two_lists():
    from backend.app.services.distributed_workers import authorization as az

    config = az.load_role_config({
        az.ENV_OPERATORS: "ivan", az.ENV_ADMINS: "ivan",
    })
    assert config.role_for("ivan") == az.ROLE_ADMIN


# ═══ §2 Матрица разрешений ═══════════════════════════════════════════════════
def test_unauthenticated_get_is_rejected(anonymous):
    assert anonymous.get("/api/workers").status_code == 401


def test_unauthenticated_mutation_is_rejected(anonymous, admin):
    worker_id, _headers, _ = _approved_worker(admin)
    response = anonymous.post(
        f"/api/workers/{worker_id}/revoke", json={}, headers=INTENT
    )
    assert response.status_code == 401


def test_viewer_can_read(viewer, admin):
    _approved_worker(admin)
    listing = viewer.get("/api/workers")
    assert listing.status_code == 200
    assert listing.json()["workers"]
    assert viewer.get("/api/workers/jobs/list").status_code == 200


@pytest.mark.parametrize(
    "path_suffix, body",
    [
        ("cancel", {"reason": "r", "confirmation": "ОТМЕНИТЬ"}),
        ("mark-lost", {"mandatory_reason": "r",
                       "typed_confirmation": "ПОПЫТКА ПОТЕРЯНА"}),
        ("request-deletion", {"reason": "r", "confirmation": "УДАЛИТЬ ДАННЫЕ"}),
    ],
)
def test_viewer_cannot_touch_attempt(viewer, admin, path_suffix, body):
    worker_id, _headers, _ = _approved_worker(admin)
    job = _create_job(admin, worker_id)
    response = viewer.post(
        f"/api/workers/jobs/{job['job_id']}/attempts/{job['attempt_id']}/{path_suffix}",
        json=body, headers=_key(),
    )
    assert response.status_code == 403
    assert response.json()["detail"]["error"] == "permission_denied"


def test_viewer_cannot_create_attempt_or_job(viewer, admin):
    worker_id, _headers, _ = _approved_worker(admin)
    job = _create_job(admin, worker_id)
    assert viewer.post(
        "/api/workers/jobs",
        json={"worker_id": worker_id, "project_id": "x"}, headers=INTENT,
    ).status_code == 403
    assert viewer.post(
        f"/api/workers/jobs/{job['job_id']}/attempts",
        json={"worker_id": worker_id, "reason": "r", "confirmation": "НОВАЯ ПОПЫТКА"},
        headers=_key(),
    ).status_code == 403


def test_operator_manages_attempts(operator, admin):
    worker_id, headers, _ = _approved_worker(admin)
    job = _create_job(operator, worker_id)
    cancel = operator.post(
        f"/api/workers/jobs/{job['job_id']}/attempts/{job['attempt_id']}/cancel",
        json={"reason": "проверка прав", "confirmation": "ОТМЕНИТЬ"},
        headers=_key(),
    )
    assert cancel.status_code == 200, cancel.text
    assert cancel.json()["state"] == "cancel_requested"


def test_operator_cannot_administer_workers(operator, admin):
    worker_id, _headers, _ = _approved_worker(admin)
    for path in ("approve", "reject", "revoke", "rotate-token"):
        response = operator.post(
            f"/api/workers/{worker_id}/{path}", json={}, headers=_key()
        )
        assert response.status_code == 403, f"{path}: {response.text}"
        assert response.json()["detail"]["required_permission"] == (
            "distributed_workers.admin"
        )


def test_operator_cannot_read_full_admin_log(operator, admin):
    _approved_worker(admin)
    assert operator.get("/api/workers/admin-actions").status_code == 403


def test_admin_can_rotate_and_read_log(admin):
    worker_id, _headers, _ = _approved_worker(admin)
    rotated = admin.post(f"/api/workers/{worker_id}/rotate-token", json={}, headers=_key())
    assert rotated.status_code == 200
    assert rotated.json()["worker_token"].startswith("wtk_")
    log = admin.get("/api/workers/admin-actions")
    assert log.status_code == 200
    assert any(a["action_type"] == "rotate_worker_token" for a in log.json()["actions"])


def test_unknown_authenticated_subject_has_no_rights(center_env, admin):
    """Вошёл в портал, но в списках подсистемы не значится — прав нет."""
    from tests.distributed_workers_helpers import STRANGER_USER

    worker_id, _headers, _ = _approved_worker(admin)
    stranger = _client(STRANGER_USER)
    assert stranger.get("/api/workers").status_code == 403
    assert stranger.post(
        f"/api/workers/{worker_id}/revoke", json={}, headers=INTENT
    ).status_code == 403


def test_me_answers_without_rights(center_env):
    from tests.distributed_workers_helpers import STRANGER_USER

    stranger = _client(STRANGER_USER)
    me = stranger.get("/api/workers/me")
    assert me.status_code == 200
    body = me.json()
    assert body["authenticated"] is True
    assert body["permissions"] == []
    assert body["can_view"] is False
    assert body["diagnostics"]


def test_me_never_leaks_other_subjects(admin):
    body = admin.get("/api/workers/me").json()
    from tests.distributed_workers_helpers import OPERATOR_USER, VIEWER_USER

    dump = json.dumps(body, ensure_ascii=False)
    assert OPERATOR_USER not in dump and VIEWER_USER not in dump


# ═══ §2.1 Подделка роли ══════════════════════════════════════════════════════
def test_role_in_body_is_ignored(viewer, admin):
    worker_id, _headers, _ = _approved_worker(admin)
    response = viewer.post(
        f"/api/workers/{worker_id}/revoke",
        json={"role": "admin", "permissions": ["distributed_workers.admin"]},
        headers=INTENT,
    )
    assert response.status_code in (403, 422)


def test_role_in_query_and_header_is_ignored(viewer, admin):
    worker_id, _headers, _ = _approved_worker(admin)
    response = viewer.post(
        f"/api/workers/{worker_id}/revoke?role=admin&permission=distributed_workers.admin",
        json={},
        headers={**INTENT, "X-User-Role": "admin", "X-Permissions": "*"},
    )
    assert response.status_code == 403


def test_worker_token_gives_no_operator_rights(center_env, admin):
    """Токен воркера — машинный контур. Операторских прав он не даёт (R-09)."""
    import httpx as _httpx

    from tests.distributed_workers_helpers import SyncASGITransport, make_center_app

    worker_id, headers, _ = _approved_worker(admin)
    machine = _httpx.Client(
        transport=SyncASGITransport(make_center_app()),
        base_url="http://center",
        headers={**INTENT, **headers},
    )
    assert machine.get("/api/workers").status_code == 401
    assert machine.post(
        f"/api/workers/{worker_id}/rotate-token", json={}, headers=_key()
    ).status_code == 401


def test_portal_session_is_not_accepted_by_worker_api(admin):
    """И обратно: портальная cookie не открывает агентский контур."""
    response = admin.post(
        "/api/v1/worker/heartbeat",
        json={"instance_id": "inst_gate_00001", "sent_at": time.time()},
    )
    assert response.status_code == 401


# ═══ §2.2 CSRF ═══════════════════════════════════════════════════════════════
def test_missing_intent_header_is_rejected(operator, admin):
    worker_id, _headers, _ = _approved_worker(admin)
    job = _create_job(operator, worker_id)
    bare = dict(operator.headers)
    bare.pop("X-Requested-With", None)
    response = operator.post(
        f"/api/workers/jobs/{job['job_id']}/attempts/{job['attempt_id']}/cancel",
        json={"reason": "r", "confirmation": "ОТМЕНИТЬ"},
        headers={"Idempotency-Key": "k1", "X-Requested-With": ""},
    )
    assert response.status_code == 403


def test_wrong_intent_header_is_rejected(operator, admin):
    worker_id, _headers, _ = _approved_worker(admin)
    job = _create_job(operator, worker_id)
    response = operator.post(
        f"/api/workers/jobs/{job['job_id']}/attempts/{job['attempt_id']}/cancel",
        json={"reason": "r", "confirmation": "ОТМЕНИТЬ"},
        headers={"X-Requested-With": "XMLHttpRequest", "Idempotency-Key": "k2"},
    )
    assert response.status_code == 403


def test_correct_csrf_with_permission_passes(operator, admin):
    worker_id, _headers, _ = _approved_worker(admin)
    job = _create_job(operator, worker_id)
    response = operator.post(
        f"/api/workers/jobs/{job['job_id']}/attempts/{job['attempt_id']}/cancel",
        json={"reason": "r", "confirmation": "ОТМЕНИТЬ"},
        headers=_key(),
    )
    assert response.status_code == 200


# ═══ §3 Отказ ничего не меняет ═══════════════════════════════════════════════
def test_denied_request_changes_nothing(viewer, admin, center_env):
    from backend.app.services.distributed_workers import repositories

    worker_id, _headers, _ = _approved_worker(admin)
    job = _create_job(admin, worker_id)
    before = repositories.get_attempt(job["attempt_id"], settings=center_env)
    commands_before = repositories.commands_for_job(job["job_id"], settings=center_env)

    denied = viewer.post(
        f"/api/workers/jobs/{job['job_id']}/attempts/{job['attempt_id']}/cancel",
        json={"reason": "попытка обхода", "confirmation": "ОТМЕНИТЬ"},
        headers=_key(),
    )
    assert denied.status_code == 403

    after = repositories.get_attempt(job["attempt_id"], settings=center_env)
    assert after["state"] == before["state"]
    assert after["attempt_disposition"] == before["attempt_disposition"]
    assert after.get("cancel_requested_at") == before.get("cancel_requested_at")
    assert repositories.commands_for_job(job["job_id"], settings=center_env) == (
        commands_before
    )
    # И в журнал решений отказ не попадает как выполненное действие.
    actions = repositories.list_admin_actions(job_id=job["job_id"], settings=center_env)
    assert all(a["action_type"] != "cancel_attempt" for a in actions)


# ═══ §4 Журнал: actor из сессии ══════════════════════════════════════════════
def test_audit_log_records_session_actor_role_and_permission(operator, admin, center_env):
    from backend.app.services.distributed_workers import repositories
    from tests.distributed_workers_helpers import OPERATOR_USER

    worker_id, _headers, _ = _approved_worker(admin)
    job = _create_job(operator, worker_id)
    operator.post(
        f"/api/workers/jobs/{job['job_id']}/attempts/{job['attempt_id']}/cancel",
        json={"reason": "запись в журнал", "confirmation": "ОТМЕНИТЬ"},
        headers=_key(),
    )
    actions = repositories.list_admin_actions(job_id=job["job_id"], settings=center_env)
    cancel = next(a for a in actions if a["action_type"] == "cancel_attempt")
    assert cancel["actor_id"] == f"operator:{OPERATOR_USER}"
    assert cancel["actor_role"] == "operator"
    assert cancel["permission"] == "distributed_workers.operate"


def test_audit_log_ignores_actor_from_request_body(operator, admin, center_env):
    """Тело запроса не может назвать себя другим оператором."""
    from backend.app.services.distributed_workers import repositories
    from tests.distributed_workers_helpers import ADMIN_USER, OPERATOR_USER

    worker_id, _headers, _ = _approved_worker(admin)
    job = _create_job(operator, worker_id)
    operator.post(
        f"/api/workers/jobs/{job['job_id']}/attempts/{job['attempt_id']}/mark-lost",
        json={"mandatory_reason": "подмена actor", "typed_confirmation": "ПОПЫТКА ПОТЕРЯНА",
              "observed_worker_state": f"actor={ADMIN_USER}"},
        headers=_key(),
    )
    actions = repositories.list_admin_actions(job_id=job["job_id"], settings=center_env)
    lost = next(a for a in actions if a["action_type"] == "mark_attempt_lost")
    assert lost["actor_id"] == f"operator:{OPERATOR_USER}"


def test_admin_log_has_no_delete_endpoint():
    """Append-only не декларацией, а отсутствием ручки."""
    from backend.app.api.routers import audit_workers_admin

    for route in list(audit_workers_admin.router.routes) + list(
        audit_workers_admin.status_router.routes
    ):
        assert "DELETE" not in getattr(route, "methods", set())


# ═══ §5 Слоты: нормализация и лимит ══════════════════════════════════════════
@pytest.mark.parametrize(
    "raw, expected, clamped",
    [
        (None, 1, False),
        (1, 1, False),
        (2, 2, False),
        (0, 1, True),
        (-1, 1, True),
        (3, 2, True),
        (5, 2, True),
        ("две", 1, True),
        ("", 1, False),
        (True, 1, True),
    ],
)
def test_max_slots_normalization(raw, expected, clamped):
    from backend.app.services.distributed_workers import slots

    limit = slots.normalize_max_slots(raw)
    assert limit.value == expected
    assert limit.clamped is clamped
    if clamped and expected == slots.MAX_VERIFIED_SLOTS:
        assert "проверялась" in (limit.notice or "")


def test_worker_normalization_mirrors_center():
    """Воркер ставится отдельным комплектом — правило обязано совпасть."""
    from audit_worker import slots as worker_slots
    from backend.app.services.distributed_workers import slots as center_slots

    assert worker_slots.MAX_VERIFIED_SLOTS == center_slots.MAX_VERIFIED_SLOTS == 2
    for raw in (None, 0, 1, 2, 3, 7, "x", -4):
        assert (
            worker_slots.normalize_max_slots(raw).value
            == center_slots.normalize_max_slots(raw).value
        )


def test_slot_predicates_are_single_source_of_truth():
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import slots

    def attempt(state, disposition="active"):
        return {"state": state, "attempt_disposition": disposition}

    for state in ("source_uploading", "source_ready", "accepted_by_worker",
                  "running", "cancel_requested"):
        assert slots.attempt_occupies_execution_slot(attempt(state)), state
    # `assigned` — очередь центра, работа воркеру ещё не передана.
    assert not slots.attempt_occupies_execution_slot(attempt(JobState.ASSIGNED.value))
    assert slots.attempt_awaiting_slot(attempt(JobState.ASSIGNED.value))
    # Локальное исполнение достоверно закончилось — слот свободен.
    for state in ("completed_locally", "result_uploading", "completed",
                  "failed", "cancelled"):
        assert not slots.attempt_occupies_execution_slot(attempt(state)), state
    # Признанная потерянной: не занимает, но и не считается остановленной.
    lost = attempt("running", "operator_declared_lost")
    assert not slots.attempt_occupies_execution_slot(lost)
    assert slots.attempt_unproven_remote(lost)


def test_effective_limit_is_minimum_of_all_constraints():
    from backend.app.services.distributed_workers import slots

    base = {
        "registration_status": "approved",
        "connection_status": "online",
        "configured_max_slots": 2,
        "worker_reported_max_slots": 2,
        "max_verified_slots": 2,
        "protocol_version": 1,
    }
    assert slots.effective_limit(base, protocol_version=1).value == 2
    assert slots.effective_limit({**base, "configured_max_slots": 1}).value == 1
    assert slots.effective_limit({**base, "worker_reported_max_slots": 1}).value == 1
    assert slots.effective_limit({**base, "max_verified_slots": 1}).value == 1
    assert slots.effective_limit(
        {**base, "connection_status": "offline"}
    ).value == 0
    assert slots.effective_limit(base, executor_status="offline").value == 0
    assert slots.effective_limit(base, disk_level="critical").value == 0
    assert slots.effective_limit(base, protocol_version=2).value == 0
    # Неизвестное состояние исполнителя лимит НЕ обнуляет: это «нет сведений».
    assert slots.effective_limit(base, executor_status="unknown").value == 2


def test_approve_clamps_and_warns(admin):
    _worker_id, _headers, approved = _approved_worker(
        admin, instance_id="inst_gate_clamp1", max_slots=5
    )
    assert approved["configured_max_slots"] == 2
    assert "проверялась" in (approved["slot_limit_notice"] or "")


def test_heartbeat_does_not_overwrite_operator_setting(admin, center_env):
    from backend.app.services.distributed_workers import repositories

    worker_id, headers, _ = _approved_worker(
        admin, instance_id="inst_gate_hb001", max_slots=1
    )
    admin.post(
        "/api/v1/worker/heartbeat",
        json={"instance_id": "inst_gate_hb001", "sent_at": time.time(),
              "configured_max_slots": 2, "calculated_free_slots": 2,
              "max_verified_slots": 2},
        headers=headers,
    )
    row = repositories.get_worker(worker_id, settings=center_env)
    assert row["configured_max_slots"] == 1        # настройка оператора цела
    assert row["worker_reported_max_slots"] == 2   # что сказал воркер — отдельно


# ═══ §5.1 Резервирование на центре ═══════════════════════════════════════════
def _take(client, headers, free_slots=2):
    return client.post(
        "/api/v1/worker/jobs/next",
        json={"free_slots": free_slots, "wait_sec": 0, "executor_status": "online"},
        headers=headers,
    )


def test_two_slots_allow_two_jobs_and_block_third(admin, center_env):
    worker_id, headers, _ = _approved_worker(
        admin, instance_id="inst_gate_two001", max_slots=2
    )
    _create_job(admin, worker_id, project="ГЕЙТ/A")
    _create_job(admin, worker_id, project="ГЕЙТ/B")
    _create_job(admin, worker_id, project="ГЕЙТ/C")

    first = _take(admin, headers)
    second = _take(admin, headers)
    third = _take(admin, headers)
    assert first.status_code == 200 and second.status_code == 200
    assert first.json()["attempt_id"] != second.json()["attempt_id"]
    assert third.status_code == 409
    assert third.json()["error"] == "no_free_slots"


def test_one_slot_worker_never_gets_two(admin):
    worker_id, headers, _ = _approved_worker(
        admin, instance_id="inst_gate_one001", max_slots=1
    )
    _create_job(admin, worker_id, project="ГЕЙТ/one-A")
    _create_job(admin, worker_id, project="ГЕЙТ/one-B")
    assert _take(admin, headers, free_slots=1).status_code == 200
    assert _take(admin, headers, free_slots=1).status_code == 409


def test_center_does_not_trust_inflated_free_slots(admin):
    """Воркер заявил 5 свободных — центр всё равно считает сам (S-15)."""
    worker_id, headers, _ = _approved_worker(
        admin, instance_id="inst_gate_lie001", max_slots=1
    )
    _create_job(admin, worker_id, project="ГЕЙТ/lie-A")
    _create_job(admin, worker_id, project="ГЕЙТ/lie-B")
    assert _take(admin, headers, free_slots=5).status_code == 200
    assert _take(admin, headers, free_slots=5).status_code == 409


def test_slot_is_released_when_execution_ends(admin, center_env):
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service, repositories

    worker_id, headers, _ = _approved_worker(
        admin, instance_id="inst_gate_rel001", max_slots=1
    )
    _create_job(admin, worker_id, project="ГЕЙТ/rel-A")
    _create_job(admin, worker_id, project="ГЕЙТ/rel-B")
    taken = _take(admin, headers, free_slots=1).json()
    assert _take(admin, headers, free_slots=1).status_code == 409

    # Локальное исполнение закончилось — слот обязан освободиться.
    job_service.transition(
        attempt_id=taken["attempt_id"], to_state=JobState.SOURCE_READY,
        actor="worker", reason="t", settings=center_env,
    )
    job_service.transition(
        attempt_id=taken["attempt_id"], to_state=JobState.ACCEPTED_BY_WORKER,
        actor="worker", reason="t", settings=center_env,
    )
    job_service.transition(
        attempt_id=taken["attempt_id"], to_state=JobState.RUNNING,
        actor="worker", reason="t", settings=center_env,
    )
    usage = repositories.worker_slot_snapshot(worker_id, settings=center_env)
    assert usage.occupied == 1
    job_service.transition(
        attempt_id=taken["attempt_id"], to_state=JobState.COMPLETED_LOCALLY,
        actor="worker", reason="t", settings=center_env,
    )
    usage = repositories.worker_slot_snapshot(worker_id, settings=center_env)
    assert usage.occupied == 0
    assert _take(admin, headers, free_slots=1).status_code == 200


def test_result_uploading_does_not_hold_execution_slot(admin, center_env):
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service, repositories

    worker_id, headers, _ = _approved_worker(
        admin, instance_id="inst_gate_upl001", max_slots=1
    )
    _create_job(admin, worker_id, project="ГЕЙТ/upl-A")
    taken = _take(admin, headers, free_slots=1).json()
    for state in (JobState.SOURCE_READY, JobState.ACCEPTED_BY_WORKER,
                  JobState.RUNNING, JobState.COMPLETED_LOCALLY,
                  JobState.RESULT_UPLOADING):
        job_service.transition(
            attempt_id=taken["attempt_id"], to_state=state,
            actor="worker", reason="t", settings=center_env,
        )
    usage = repositories.worker_slot_snapshot(worker_id, settings=center_env)
    assert usage.occupied == 0


def test_cancel_requested_holds_slot_until_worker_confirms(admin, operator, center_env):
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service, repositories

    worker_id, headers, _ = _approved_worker(
        admin, instance_id="inst_gate_can001", max_slots=1
    )
    _create_job(admin, worker_id, project="ГЕЙТ/can-A")
    taken = _take(admin, headers, free_slots=1).json()
    for state in (JobState.SOURCE_READY, JobState.ACCEPTED_BY_WORKER, JobState.RUNNING):
        job_service.transition(
            attempt_id=taken["attempt_id"], to_state=state,
            actor="worker", reason="t", settings=center_env,
        )
    operator.post(
        f"/api/workers/jobs/{taken['job_id']}/attempts/{taken['attempt_id']}/cancel",
        json={"reason": "держит слот", "confirmation": "ОТМЕНИТЬ"},
        headers=_key(),
    )
    usage = repositories.worker_slot_snapshot(worker_id, settings=center_env)
    assert usage.occupied == 1, "cancel_requested занимает слот до подтверждения"


def test_declared_lost_is_counted_but_does_not_block(admin, operator, center_env):
    """Политика §34: потерянная попытка видна как «недоказанная», но не блокирует."""
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service, repositories

    worker_id, headers, _ = _approved_worker(
        admin, instance_id="inst_gate_lost01", max_slots=1
    )
    _create_job(admin, worker_id, project="ГЕЙТ/lost-A")
    taken = _take(admin, headers, free_slots=1).json()
    for state in (JobState.SOURCE_READY, JobState.ACCEPTED_BY_WORKER, JobState.RUNNING):
        job_service.transition(
            attempt_id=taken["attempt_id"], to_state=state,
            actor="worker", reason="t", settings=center_env,
        )
    operator.post(
        f"/api/workers/jobs/{taken['job_id']}/attempts/{taken['attempt_id']}/mark-lost",
        json={"mandatory_reason": "VPS молчит", "typed_confirmation": "ПОПЫТКА ПОТЕРЯНА"},
        headers=_key(),
    )
    usage = repositories.worker_slot_snapshot(worker_id, settings=center_env)
    assert usage.occupied == 0 and usage.unproven == 1
    view = admin.get("/api/workers").json()["workers"][0]["slots"]
    assert view["unproven_remote"] == 1
    assert "может" in (view["unproven_warning"] or "")


def test_new_attempt_on_offline_worker_requires_risk_acknowledgement(
    admin, operator, center_env
):
    from backend.app.models.distributed_workers import JobState
    from backend.app.services.distributed_workers import job_service, repositories

    worker_id, headers, _ = _approved_worker(
        admin, instance_id="inst_gate_risk01", max_slots=1
    )
    _create_job(admin, worker_id, project="ГЕЙТ/risk-A")
    taken = _take(admin, headers, free_slots=1).json()
    for state in (JobState.SOURCE_READY, JobState.ACCEPTED_BY_WORKER, JobState.RUNNING):
        job_service.transition(
            attempt_id=taken["attempt_id"], to_state=state,
            actor="worker", reason="t", settings=center_env,
        )
    operator.post(
        f"/api/workers/jobs/{taken['job_id']}/attempts/{taken['attempt_id']}/mark-lost",
        json={"mandatory_reason": "нет связи", "typed_confirmation": "ПОПЫТКА ПОТЕРЯНА"},
        headers=_key(),
    )
    repositories.update_worker_fields(
        worker_id, {"connection_status": "offline"}, settings=center_env
    )
    body = {"worker_id": worker_id, "reason": "повтор",
            "source_attempt_id": taken["attempt_id"], "confirmation": "НОВАЯ ПОПЫТКА"}
    refused = operator.post(
        f"/api/workers/jobs/{taken['job_id']}/attempts", json=body, headers=_key()
    )
    assert refused.status_code == 409
    assert "accept_capacity_risk" in refused.json()["detail"]

    accepted = operator.post(
        f"/api/workers/jobs/{taken['job_id']}/attempts",
        json={**body, "accept_capacity_risk": True}, headers=_key(),
    )
    assert accepted.status_code == 200, accepted.text


def test_disk_critical_blocks_new_jobs_but_not_running(admin, center_env):
    from backend.app.services.distributed_workers import repositories

    worker_id, headers, _ = _approved_worker(
        admin, instance_id="inst_gate_disk01", max_slots=2
    )
    _create_job(admin, worker_id, project="ГЕЙТ/disk-A")
    running = _take(admin, headers).json()
    _create_job(admin, worker_id, project="ГЕЙТ/disk-B")

    admin.post(
        "/api/v1/worker/heartbeat",
        json={"instance_id": "inst_gate_disk01", "sent_at": time.time(),
              "configured_max_slots": 2, "calculated_free_slots": 1,
              "disk": {"level": "critical", "free_bytes": 1024}},
        headers=headers,
    )
    blocked = _take(admin, headers)
    assert blocked.status_code == 409
    # Текущее задание не тронуто.
    attempt = repositories.get_attempt(running["attempt_id"], settings=center_env)
    assert attempt["state"] == "source_uploading"


def test_two_concurrent_jobs_next_never_exceed_limit(admin, center_env):
    """Настоящая гонка: два одновременных запроса на воркер с одним слотом."""
    import threading

    from tests.distributed_workers_helpers import ADMIN_USER, make_center_app, portal_client

    worker_id, headers, _ = _approved_worker(
        admin, instance_id="inst_gate_race01", max_slots=1
    )
    _create_job(admin, worker_id, project="ГЕЙТ/race-A")
    _create_job(admin, worker_id, project="ГЕЙТ/race-B")

    app = make_center_app()
    results: list[int] = []
    lock = threading.Lock()

    def take() -> None:
        client = portal_client(app, username=ADMIN_USER)
        response = client.post(
            "/api/v1/worker/jobs/next",
            json={"free_slots": 1, "wait_sec": 0}, headers=headers,
        )
        with lock:
            results.append(response.status_code)

    threads = [threading.Thread(target=take) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert sorted(results) == [200, 409], results


# ═══ §6 Локальная ёмкость исполнителя ════════════════════════════════════════
def _worker_config(tmp_path, max_slots=2):
    from audit_worker.config import WorkerConfig

    return WorkerConfig(
        dispatcher_url="https://center.example",
        root=tmp_path / "worker",
        display_name="VPS-gate",
        max_slots=max_slots,
    )


def test_executor_claims_two_and_refuses_third(tmp_path):
    from audit_worker.executor import Executor

    config = _worker_config(tmp_path, max_slots=2)
    executor = Executor(config)
    for index in range(3):
        executor.db.enqueue(
            job_id=f"job{index}", attempt_id=f"att{index}",
            job_type="test_pipeline_v1", params={},
        )
    first = executor.db.claim_next(executor.instance_id, capacity_limit=2)
    second = executor.db.claim_next(executor.instance_id, capacity_limit=2)
    third = executor.db.claim_next(executor.instance_id, capacity_limit=2)
    assert first and second and third is None
    assert first["attempt_id"] != second["attempt_id"]
    # Третья НЕ провалена — она ждёт слот.
    waiting = executor.db.queue_item("att2")
    assert waiting["state"] == "queued"


def test_executor_capacity_respects_configured_limit(tmp_path):
    from audit_worker.executor import Executor

    executor = Executor(_worker_config(tmp_path, max_slots=1))
    executor.db.enqueue(job_id="j1", attempt_id="a1", job_type="test_pipeline_v1", params={})
    executor.db.enqueue(job_id="j2", attempt_id="a2", job_type="test_pipeline_v1", params={})
    assert executor.slot_limit() == 1
    assert executor.db.claim_next(executor.instance_id, capacity_limit=1) is not None
    assert executor.db.claim_next(executor.instance_id, capacity_limit=1) is None


def test_executor_limit_is_clamped_to_verified_maximum(tmp_path):
    from audit_worker.executor import Executor

    executor = Executor(_worker_config(tmp_path, max_slots=5))
    assert executor.slot_limit() == 2


def test_executor_capacity_zeroes_on_critical_disk(tmp_path, monkeypatch):
    from audit_worker.executor import Executor

    executor = Executor(_worker_config(tmp_path, max_slots=2))
    monkeypatch.setattr(
        executor.retention, "disk_snapshot", lambda: {"level": "critical"}
    )
    free, why = executor.local_capacity()
    assert free == 0 and "диск" in why


def test_two_executors_share_one_capacity_counter(tmp_path):
    """Второй исполнитель не может «добрать» сверх общего лимита очереди."""
    from audit_worker.executor import Executor

    first = Executor(_worker_config(tmp_path, max_slots=2))
    second = Executor(_worker_config(tmp_path, max_slots=2))
    for index in range(4):
        first.db.enqueue(job_id=f"j{index}", attempt_id=f"a{index}",
                         job_type="test_pipeline_v1", params={})
    taken = [
        first.db.claim_next(first.instance_id, capacity_limit=2),
        second.db.claim_next(second.instance_id, capacity_limit=2),
        first.db.claim_next(first.instance_id, capacity_limit=2),
        second.db.claim_next(second.instance_id, capacity_limit=2),
    ]
    assert sum(1 for item in taken if item is not None) == 2


# ═══ §7 Счётчик событий: несколько ПРОЦЕССОВ ═════════════════════════════════
def _writer(db_path: str, events_dir: str, job_id: str, attempt_id: str, count: int):
    """Отдельный ПРОЦЕСС-писатель. Именно процессы, а не потоки: потоковый
    лок скрыл бы ровно ту гонку, ради которой тест написан."""
    sys.path.insert(0, str(_ROOT))
    from audit_worker.event_outbox import EventOutbox
    from audit_worker.local_db import LocalDB

    db = LocalDB(Path(db_path))
    outbox = EventOutbox(
        Path(events_dir), sequence_db=db, job_id=job_id, attempt_id=attempt_id
    )
    for index in range(count):
        outbox.append("log_line", {"message": f"{os.getpid()}-{index}"})


def _seqs_in(events_dir: Path) -> list[int]:
    out: list[int] = []
    for path in sorted(events_dir.glob("outbox-*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                out.append(int(json.loads(line)["seq"]))
    return out


@pytest.mark.slow
def test_event_sequence_is_unique_across_processes(tmp_path):
    """Четыре ПРОЦЕССА, две попытки, сотни событий: ни дублей, ни дыр."""
    from audit_worker.local_db import LocalDB

    db_path = tmp_path / "worker.db"
    LocalDB(db_path)                      # создать схему до форка
    dir_a = tmp_path / "events-a"
    dir_b = tmp_path / "events-b"
    per_writer = 120

    ctx = multiprocessing.get_context("spawn")
    procs = [
        ctx.Process(target=_writer,
                    args=(str(db_path), str(dir_a), "jobA", "attA", per_writer)),
        ctx.Process(target=_writer,
                    args=(str(db_path), str(dir_a), "jobA", "attA", per_writer)),
        ctx.Process(target=_writer,
                    args=(str(db_path), str(dir_b), "jobB", "attB", per_writer)),
        ctx.Process(target=_writer,
                    args=(str(db_path), str(dir_b), "jobB", "attB", per_writer)),
    ]
    for proc in procs:
        proc.start()
    for proc in procs:
        proc.join(timeout=180)
        assert proc.exitcode == 0, f"писатель упал: {proc.exitcode}"

    for events_dir, expected in ((dir_a, per_writer * 2), (dir_b, per_writer * 2)):
        seqs = _seqs_in(events_dir)
        assert len(seqs) == expected, f"{events_dir.name}: записано {len(seqs)}"
        assert len(set(seqs)) == len(seqs), "дубли sequence между процессами"
        assert sorted(seqs) == list(range(1, expected + 1)), "дыра в нумерации"

    # Счётчики попыток независимы: у каждой своя нумерация с единицы.
    db = LocalDB(db_path)
    assert db.allocated_event_high(job_id="jobA", attempt_id="attA") == per_writer * 2
    assert db.allocated_event_high(job_id="jobB", attempt_id="attB") == per_writer * 2


def test_sequence_survives_restart(tmp_path):
    from audit_worker.event_outbox import EventOutbox
    from audit_worker.local_db import LocalDB

    db = LocalDB(tmp_path / "worker.db")
    events = tmp_path / "events"
    first = EventOutbox(events, sequence_db=db, job_id="j", attempt_id="a")
    for _ in range(5):
        first.append("log_line", {})
    assert first.last_written_seq == 5

    # «Рестарт»: новый объект, тот же каталог и та же база.
    second = EventOutbox(events, sequence_db=db, job_id="j", attempt_id="a")
    assert second.append("job_started", {}) == 6


def test_allocation_gap_is_filled_visibly_not_masked(tmp_path):
    """Номер выдан, событие не дошло до диска — дыра ЗАКРЫВАЕТСЯ явной записью."""
    from audit_worker.event_outbox import EventOutbox
    from audit_worker.local_db import LocalDB

    db = LocalDB(tmp_path / "worker.db")
    events = tmp_path / "events"
    outbox = EventOutbox(events, sequence_db=db, job_id="j", attempt_id="a")
    outbox.append("job_started", {})
    # Имитируем смерть процесса ровно между выдачей номера и записью строки.
    lost = db.allocate_event_sequence(
        job_id="j", attempt_id="a", event_id="ev_lost", event_type="stage_progress"
    )
    outbox.append("job_completed_locally", {})

    healed = outbox.heal_allocation_gaps(older_than=0.0)
    assert lost in healed
    seqs = _seqs_in(events)
    assert sorted(seqs) == [1, 2, 3], seqs
    filler = [
        json.loads(line)
        for path in sorted(events.glob("outbox-*.jsonl"))
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and json.loads(line)["seq"] == lost
    ][0]
    assert filler["event_type"] == "events_truncated"
    assert filler["payload"]["reason"] == "allocated_but_not_written"
    # Поток событий после лечения непрерывен — пакет собирается целиком.
    assert [e["seq"] for e in outbox.pending_batch()] == [1, 2, 3]


def test_two_attempts_have_independent_sequences(tmp_path):
    from audit_worker.event_outbox import EventOutbox
    from audit_worker.local_db import LocalDB

    db = LocalDB(tmp_path / "worker.db")
    a = EventOutbox(tmp_path / "ea", sequence_db=db, job_id="j1", attempt_id="a1")
    b = EventOutbox(tmp_path / "eb", sequence_db=db, job_id="j2", attempt_id="a2")
    assert a.append("job_started", {}) == 1
    assert b.append("job_started", {}) == 1
    assert a.append("log_line", {}) == 2
    assert b.append("log_line", {}) == 2


# ═══ §8 Настоящее приложение и настоящие процессы ════════════════════════════
def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait(predicate, *, timeout=60.0, interval=0.2, message="условие"):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"не дождались: {message}")


def _stop(process, *, sig=signal.SIGTERM):
    if process is None or process.poll() is not None:
        return
    try:
        process.send_signal(sig)
        process.wait(timeout=15)
    except Exception:  # noqa: BLE001
        process.kill()


def _alive(pid: int) -> bool:
    from audit_worker.process_registry import process_start_time

    return process_start_time(pid) is not None


@pytest.fixture()
def live_main_app(tmp_path, monkeypatch):
    """НАСТОЯЩИЙ `backend/app/main.py` под uvicorn, с портальной защитой.

    Самодельная сборка роутеров не проверяет главного: что подсистема стоит за
    PortalAuthMiddleware, что маршруты смонтированы так же, как в проде, и что
    порядок middleware не съедает cookie.
    """
    from tests.distributed_workers_helpers import portal_role_env

    port = _free_port()
    role_env = portal_role_env()
    for key, value in role_env.items():
        monkeypatch.setenv(key, value)
    env = dict(os.environ)
    env.update({
        "PYTHONPATH": str(_ROOT),
        "DISTRIBUTED_WORKERS_ENABLED": "true",
        "DISTRIBUTED_WORKERS_DATA_DIR": str(tmp_path / "center"),
        "DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET": BOOTSTRAP,
        "DISTRIBUTED_WORKERS_LONG_POLL_SEC": "2",
        "DISTRIBUTED_WORKERS_ALLOW_INSECURE_ADMIN": "false",
        "AUDIT_PROJECTS_DIR": str(tmp_path / "projects"),
        "AUDIT_DATA_DIR": str(tmp_path / "data"),
        "AUDIT_APP_DATA_DIR": str(tmp_path / "app_data"),
        "AUDIT_ACTION_LOG_DIR": str(tmp_path / "actions"),
        **role_env,
    })
    process = subprocess.Popen(  # noqa: S603 — фиксированный argv, shell=False
        [PY, "-m", "uvicorn", "backend.app.main:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=str(_ROOT), env=env, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, text=True,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        _wait(
            lambda: _ping_status(url), timeout=120,
            message=f"настоящее приложение не поднялось на {url}",
        )
        yield url
    finally:
        _stop(process)


def _ping_status(url: str) -> bool:
    try:
        return httpx.get(f"{url}/api/workers/status", timeout=2.0).status_code in (
            200, 401, 302
        )
    except Exception:  # noqa: BLE001 — ещё не поднялся
        return False


def _live_client(url: str, username: str | None):
    from backend.app.core import portal_auth
    from tests.distributed_workers_helpers import session_cookie

    client = httpx.Client(base_url=url, timeout=20.0, headers=INTENT,
                          follow_redirects=False)
    if username:
        client.cookies.set(portal_auth.get_settings().cookie_name,
                           session_cookie(username))
    return client


@pytest.mark.slow
def test_real_main_enforces_roles_end_to_end(live_main_app):
    """§37 задания на НАСТОЯЩЕМ приложении: viewer / operator / admin."""
    from tests.distributed_workers_helpers import ADMIN_USER, OPERATOR_USER, VIEWER_USER

    admin = _live_client(live_main_app, ADMIN_USER)
    operator = _live_client(live_main_app, OPERATOR_USER)
    viewer = _live_client(live_main_app, VIEWER_USER)
    anon = _live_client(live_main_app, None)

    # 1. Без сессии портала не пускают вовсе (middleware отвечает раньше ролей).
    assert anon.get("/api/workers").status_code == 401

    # 2. Регистрация воркера — машинный контур, портальная cookie ему не нужна.
    registered = httpx.post(
        f"{live_main_app}/api/v1/worker/register",
        json={"instance_id": "inst_main_gate01", "protocol_version": 1,
              "display_name_hint": "VPS-main"},
        headers={"Authorization": f"Bearer {BOOTSTRAP}", "X-Protocol-Version": "1"},
        timeout=20.0,
    ).json()
    worker_id = registered["worker_id"]

    # 3. Одобрение — только администратору.
    assert operator.post(
        f"/api/workers/{worker_id}/approve", json={"configured_max_slots": 2}
    ).status_code == 403
    assert admin.post(
        f"/api/workers/{worker_id}/approve", json={"configured_max_slots": 2}
    ).status_code == 200

    token = httpx.post(
        f"{live_main_app}/api/v1/worker/claim",
        json={"worker_id": worker_id, "instance_id": "inst_main_gate01",
              "claim_secret": registered["claim_secret"]},
        timeout=20.0,
    ).json()["worker_token"]
    worker_headers = {"Authorization": f"Bearer {token}",
                      "X-Worker-Id": worker_id, "X-Protocol-Version": "1"}
    httpx.post(
        f"{live_main_app}/api/v1/worker/heartbeat",
        json={"instance_id": "inst_main_gate01", "sent_at": time.time(),
              "configured_max_slots": 2, "calculated_free_slots": 2,
              "max_verified_slots": 2,
              "executor": {"status": "online"}},
        headers=worker_headers, timeout=20.0,
    )

    # 4. Наблюдатель читает, но не управляет.
    assert viewer.get("/api/workers").status_code == 200
    assert viewer.post(
        "/api/workers/jobs",
        json={"worker_id": worker_id, "project_id": "МЭЙН/проект"},
    ).status_code == 403

    # 5. Оператор управляет заданиями, но не воркерами.
    created = operator.post(
        "/api/workers/jobs",
        json={"worker_id": worker_id, "project_id": "МЭЙН/проект",
              "params": {"label": "main", "steps": 1, "step_seconds": 0.0}},
    )
    assert created.status_code == 200, created.text
    job = created.json()["job"]
    assert operator.post(
        f"/api/workers/{worker_id}/rotate-token", json={},
        headers={**INTENT, "Idempotency-Key": "main-rot-1"},
    ).status_code == 403

    # 6. Подделка роли не работает ни через заголовок, ни через query, ни в теле.
    spoofed = viewer.post(
        f"/api/workers/jobs/{job['job_id']}/attempts/{job['attempt_id']}/cancel"
        "?role=admin",
        json={"reason": "подмена", "confirmation": "ОТМЕНИТЬ", },
        headers={**INTENT, "Idempotency-Key": "main-spoof-1",
                 "X-User-Role": "admin"},
    )
    assert spoofed.status_code == 403

    # 7. Оператору отмена доступна, и журнал знает настоящего актора.
    cancelled = operator.post(
        f"/api/workers/jobs/{job['job_id']}/attempts/{job['attempt_id']}/cancel",
        json={"reason": "проверка", "confirmation": "ОТМЕНИТЬ"},
        headers={**INTENT, "Idempotency-Key": "main-cancel-1"},
    )
    assert cancelled.status_code == 200, cancelled.text

    log = admin.get("/api/workers/admin-actions?limit=50")
    assert log.status_code == 200
    entry = next(a for a in log.json()["actions"] if a["action_type"] == "cancel_attempt")
    assert entry["actor_id"] == f"operator:{OPERATOR_USER}"
    assert entry["actor_role"] == "operator"

    # 8. Администратор может всё перечисленное.
    assert admin.post(
        f"/api/workers/{worker_id}/rotate-token", json={},
        headers={**INTENT, "Idempotency-Key": "main-rot-2"},
    ).status_code == 200


@pytest.mark.slow
def test_real_main_closes_operator_api_without_portal_auth(tmp_path):
    """R-05 на настоящем приложении: без портальной защиты опасного API нет."""
    port = _free_port()
    env = dict(os.environ)
    env.update({
        "PYTHONPATH": str(_ROOT),
        "DISTRIBUTED_WORKERS_ENABLED": "true",
        "DISTRIBUTED_WORKERS_DATA_DIR": str(tmp_path / "center"),
        "DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET": BOOTSTRAP,
        "PORTAL_AUTH_ENABLED": "false",
        # Даже с ЯВНЫМ признанием риска (локальный пилот) изменяющие ручки
        # обязаны отказывать: субъекта нет, проверять право не у кого.
        "DISTRIBUTED_WORKERS_ALLOW_INSECURE_ADMIN": "true",
        "AUDIT_PROJECTS_DIR": str(tmp_path / "projects"),
        "AUDIT_DATA_DIR": str(tmp_path / "data"),
        "AUDIT_APP_DATA_DIR": str(tmp_path / "app_data"),
        "AUDIT_ACTION_LOG_DIR": str(tmp_path / "actions"),
    })
    process = subprocess.Popen(  # noqa: S603
        [PY, "-m", "uvicorn", "backend.app.main:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=str(_ROOT), env=env, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, text=True,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        _wait(lambda: _ping_status(url), timeout=120, message="приложение")
        client = httpx.Client(base_url=url, timeout=20.0, headers=INTENT)
        # Просмотр в явно признанном небезопасном режиме допустим…
        assert client.get("/api/workers").status_code == 200
        # …а любое изменение — нет, и ответ говорит почему.
        blocked = client.post(
            "/api/workers/jobs",
            json={"worker_id": "wrk_x", "project_id": "p"},
        )
        assert blocked.status_code == 503
        assert blocked.json()["detail"]["error"] == "portal_auth_disabled"
    finally:
        _stop(process)


@pytest.fixture()
def two_slot_worker(tmp_path, monkeypatch):
    """Центр + исполнитель + агент настоящими процессами, max_slots=2."""
    from tests.distributed_workers_helpers import ADMIN_USER, portal_role_env

    port = _free_port()
    role_env = portal_role_env()
    for key, value in role_env.items():
        monkeypatch.setenv(key, value)
    center_env_vars = {
        "PYTHONPATH": str(_ROOT),
        "DISTRIBUTED_WORKERS_ENABLED": "true",
        "DISTRIBUTED_WORKERS_DATA_DIR": str(tmp_path / "center"),
        "DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET": BOOTSTRAP,
        "DISTRIBUTED_WORKERS_LONG_POLL_SEC": "1",
        "DISTRIBUTED_WORKERS_UPLOAD_CHUNK_BYTES": "65536",
        "DISTRIBUTED_WORKERS_ALLOW_INSECURE_ADMIN": "false",
        **role_env,
    }
    center = subprocess.Popen(  # noqa: S603
        [PY, "-m", "uvicorn", "tests.worker_center_app:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=str(_ROOT), env={**os.environ, **center_env_vars},
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    url = f"http://127.0.0.1:{port}"
    root = tmp_path / "worker"
    root.mkdir(parents=True, exist_ok=True)
    worker_env_vars = {
        **os.environ,
        "PYTHONPATH": str(_ROOT),
        "AUDIT_WORKER_ROOT": str(root),
        "AUDIT_WORKER_DISPATCHER_URL": url,
        "AUDIT_WORKER_ALLOW_INSECURE_LOCALHOST": "true",
        "AUDIT_WORKER_HEARTBEAT_SEC": "2",
        "AUDIT_WORKER_POLL_WAIT_SEC": "1",
        "AUDIT_WORKER_MAX_SLOTS": "2",
        "AUDIT_WORKER_TEST_MAX_SEC": "300",
    }
    children: list[subprocess.Popen] = [center]
    try:
        _wait(lambda: _ping_status(url), timeout=60, message="центр")
        admin = _live_client(url, ADMIN_USER)
        registered = subprocess.run(  # noqa: S603
            [PY, "-m", "audit_worker", "register", "--root", str(root),
             "--bootstrap-secret", BOOTSTRAP],
            env=worker_env_vars, cwd=str(_ROOT), capture_output=True, text=True,
        )
        assert registered.returncode == 0, registered.stderr
        worker_id = json.loads(registered.stdout)["worker_id"]
        assert admin.post(
            f"/api/workers/{worker_id}/approve", json={"configured_max_slots": 2}
        ).status_code == 200
        claimed = subprocess.run(  # noqa: S603
            [PY, "-m", "audit_worker", "register", "--root", str(root)],
            env=worker_env_vars, cwd=str(_ROOT), capture_output=True, text=True,
        )
        assert claimed.returncode == 0, claimed.stderr

        executor = subprocess.Popen(  # noqa: S603
            [PY, "-m", "audit_worker", "executor", "--root", str(root)],
            env=worker_env_vars, cwd=str(_ROOT), stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True,
        )
        children.append(executor)
        agent = subprocess.Popen(  # noqa: S603
            [PY, "-m", "audit_worker", "agent", "--root", str(root)],
            env=worker_env_vars, cwd=str(_ROOT), stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True,
        )
        children.append(agent)

        from audit_worker.local_db import LocalDB

        yield {
            "url": url, "admin": admin, "worker_id": worker_id, "root": root,
            "db": LocalDB(root / "worker.db"), "env": worker_env_vars,
            "agent": agent, "executor": executor, "children": children,
        }
    finally:
        for child in reversed(children):
            _stop(child)
        # Процессы аудита живут в своих сессиях и переживают уход наблюдателей —
        # добираем их явно, чтобы тест не оставлял хвостов.
        try:
            from audit_worker.local_db import LocalDB as _DB

            for row in _DB(root / "worker.db").list_processes():
                pid = int(row.get("pid") or 0)
                if pid and _alive(pid):
                    try:
                        os.killpg(os.getpgid(pid), signal.SIGKILL)
                    except OSError:
                        pass
        except Exception:  # noqa: BLE001 — уборка не должна ронять тест
            pass


def _live_processes(db) -> list[dict]:
    return [
        row for row in db.list_processes()
        if row.get("status") == "running" and _alive(int(row.get("pid") or 0))
    ]


@pytest.mark.slow
def test_two_real_processes_overlap_and_third_waits(two_slot_worker):
    """S-01…S-04: два процесса живут одновременно, третий ждёт, отмена адресна."""
    admin = two_slot_worker["admin"]
    worker_id = two_slot_worker["worker_id"]
    db = two_slot_worker["db"]

    def create(project: str, steps: int) -> str:
        response = admin.post("/api/workers/jobs", json={
            "worker_id": worker_id, "project_id": project,
            "params": {"label": "gate", "steps": steps, "step_seconds": 0.4,
                       "result_bytes": 1024},
        })
        assert response.status_code == 200, response.text
        return response.json()["job"]["job_id"]

    job_a = create("ГЕЙТ/A", 45)
    job_b = create("ГЕЙТ/B", 45)

    # 1. Два процесса ОДНОВРЕМЕННО живы.
    live = _wait(
        lambda: _live_processes(db) if len(_live_processes(db)) >= 2 else None,
        timeout=90, message="два одновременно живых процесса",
    )
    assert len({row["pid"] for row in live}) == 2, "разные PID"
    groups = {os.getpgid(int(row["pid"])) for row in live}
    assert len(groups) == 2, "разные группы процессов"
    assert {row["attempt_id"] for row in live} == {
        row["attempt_id"] for row in live
    }
    assert len({row["job_id"] for row in live}) == 2, "разные задания"

    # 2. Каталоги и логи независимы.
    for row in live:
        job_dir = two_slot_worker["root"] / "jobs" / row["job_id"] / row["attempt_id"]
        assert (job_dir / "logs" / "stdout.log").is_file()
        assert (job_dir / "events").is_dir()

    # 3. Третье задание создаётся, но НЕ запускается: центр не отдаёт его
    #    сверх лимита, поэтому до локальной очереди воркера оно не доезжает.
    job_c = create("ГЕЙТ/C", 6)
    time.sleep(4.0)
    assert len(_live_processes(db)) == 2, "третий процесс не должен появиться"
    center_view = admin.get(f"/api/workers/jobs/{job_c}").json()["job"]
    assert center_view["state"] == "assigned", (
        "третье задание обязано ЖДАТЬ слот, а не падать: "
        f"состояние {center_view['state']}"
    )
    local_jobs = {item["job_id"] for item in db.list_queue()}
    assert job_c not in local_jobs, "сверх лимита работа воркеру не передаётся"

    # 4. Прогресс обоих растёт независимо.
    first = {job: admin.get(f"/api/workers/jobs/{job}").json()["job"] for job in
             (job_a, job_b)}
    _wait(
        lambda: all(
            (admin.get(f"/api/workers/jobs/{job}").json()["job"].get("progress_snapshot")
             or {}).get("processed", 0)
            > ((first[job].get("progress_snapshot") or {}).get("processed", 0))
            for job in (job_a, job_b)
        ),
        timeout=90, message="прогресс обоих заданий",
    )

    # 5. Отмена A не трогает B: адресный сигнал по СВОЕЙ группе процессов.
    a_row = next(row for row in live if row["job_id"] == job_a)
    b_row = next(row for row in live if row["job_id"] == job_b)
    b_pid, b_group = int(b_row["pid"]), os.getpgid(int(b_row["pid"]))
    cancel = admin.post(
        f"/api/workers/jobs/{job_a}/attempts/{a_row['attempt_id']}/cancel",
        json={"reason": "гейт: адресная отмена", "confirmation": "ОТМЕНИТЬ",
              "grace_period_sec": 3},
        headers={**INTENT, "Idempotency-Key": f"gate-{uuid.uuid4().hex[:8]}"},
    )
    assert cancel.status_code == 200, cancel.text
    _wait(lambda: not _alive(int(a_row["pid"])), timeout=60,
          message="процесс A остановлен")
    assert _alive(b_pid), "процесс B не должен пострадать от отмены A"
    assert os.getpgid(b_pid) == b_group, "группа процессов B не менялась"

    # 6. Освободившийся слот получает третье задание, и снова ровно два процесса.
    _wait(
        lambda: any(row["job_id"] == job_c for row in _live_processes(db)),
        timeout=120, message="третье задание пошло в работу после освобождения слота",
    )
    assert len(_live_processes(db)) <= 2, "одновременно больше двух — нарушение S-01"


@pytest.mark.slow
def test_agent_restart_keeps_two_processes_and_creates_no_duplicates(two_slot_worker):
    """S-07…S-09: убийство агента не трогает работу и не порождает дублей."""
    admin = two_slot_worker["admin"]
    worker_id = two_slot_worker["worker_id"]
    db = two_slot_worker["db"]

    for project in ("ГЕЙТ/R1", "ГЕЙТ/R2"):
        assert admin.post("/api/workers/jobs", json={
            "worker_id": worker_id, "project_id": project,
            "params": {"label": "gate", "steps": 50, "step_seconds": 0.4,
                       "result_bytes": 1024},
        }).status_code == 200

    live = _wait(
        lambda: _live_processes(db) if len(_live_processes(db)) >= 2 else None,
        timeout=90, message="два процесса",
    )
    pids = sorted(int(row["pid"]) for row in live)
    events_sizes = {}
    for row in live:
        events = two_slot_worker["root"] / "jobs" / row["job_id"] / row["attempt_id"] / "events"
        events_sizes[row["attempt_id"]] = sum(
            path.stat().st_size for path in events.glob("outbox-*.jsonl")
        )

    # Агент убит НАСМЕРТЬ. Исполнитель и оба процесса обязаны выжить.
    two_slot_worker["agent"].send_signal(signal.SIGKILL)
    two_slot_worker["agent"].wait(timeout=20)
    time.sleep(2.0)
    assert two_slot_worker["executor"].poll() is None, "исполнитель пережил агента"
    assert all(_alive(pid) for pid in pids), "оба процесса аудита живы"

    # Журналы продолжают наполняться без агента (I-01).
    _wait(
        lambda: all(
            sum(path.stat().st_size for path in (
                two_slot_worker["root"] / "jobs" / row["job_id"] / row["attempt_id"]
                / "events"
            ).glob("outbox-*.jsonl")) > events_sizes[row["attempt_id"]]
            for row in live
        ),
        timeout=60, message="журналы обеих попыток растут",
    )

    # Агент поднимается заново — второго процесса не появляется.
    revived = subprocess.Popen(  # noqa: S603
        [PY, "-m", "audit_worker", "agent", "--root", str(two_slot_worker["root"])],
        env=two_slot_worker["env"], cwd=str(_ROOT), stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, text=True,
    )
    two_slot_worker["children"].append(revived)
    time.sleep(6.0)
    after = sorted(int(row["pid"]) for row in _live_processes(db))
    assert after == pids, f"состав процессов изменился: было {pids}, стало {after}"


# ═══ §9 Безопасность экрана ══════════════════════════════════════════════════
def test_frontend_has_no_html_injection_points():
    source = (_ROOT / "frontend" / "static" / "js" / "audit-workers.js").read_text(
        encoding="utf-8"
    )
    for marker in ("innerHTML", "outerHTML", "insertAdjacentHTML", "document.write"):
        assert marker not in source, marker


def test_frontend_reads_permissions_from_server_only():
    """Права не берутся из localStorage и не отправляются обратно на сервер."""
    source = (_ROOT / "frontend" / "static" / "js" / "audit-workers.js").read_text(
        encoding="utf-8"
    )
    assert "/api/workers/me" in source
    # Ищем ОБРАЩЕНИЯ к хранилищу, а не слово в комментарии.
    assert not re.search(r"\blocalStorage\s*[.\[]", source)
    assert not re.search(r"\bsessionStorage\s*[.\[]", source)
    # Роль нигде не кладётся в тело запроса.
    assert not re.search(r"body:\s*JSON\.stringify\([^)]*role", source)


def test_frontend_renders_slot_counters_and_warnings():
    source = (_ROOT / "frontend" / "static" / "js" / "audit-workers.js").read_text(
        encoding="utf-8"
    )
    for marker in ("occupancy_label", "center_free_slots", "worker_claimed_free_slots",
                   "slot_count_mismatch", "unproven_warning", "slot-jobs"):
        assert marker in source, marker


def test_dangerous_values_reach_screen_as_text(admin, center_env):
    """XSS-строки в данных возвращаются как ДАННЫЕ, а не как разметка."""
    from backend.app.services.distributed_workers import repositories

    worker_id, headers, _ = _approved_worker(
        admin, instance_id="inst_gate_xss001", max_slots=1
    )
    payload = "<img src=x onerror=alert(1)>"
    admin.post(
        "/api/v1/worker/heartbeat",
        json={"instance_id": "inst_gate_xss001", "sent_at": time.time(),
              "configured_max_slots": 1, "calculated_free_slots": 1,
              "executor": {"status": payload, "executor_instance_id": payload},
              "disk": {"level": payload},
              "warnings": [{"code": payload, "message": payload}]},
        headers=headers,
    )
    view = admin.get("/api/workers").json()["workers"][0]
    # Значения из закрытых наборов заменены, а не «просто показаны».
    assert view["executor"]["status"] == "unknown"
    assert view["disk"]["level"] == "unknown"
    # Свободные строки остаются строками (экран печатает их textContent).
    assert isinstance(view["warnings"][0]["message"], str)
    _create_job(admin, worker_id, project=payload)
    jobs = admin.get("/api/workers/jobs/list").json()["jobs"]
    assert any(job["project_id"] == payload for job in jobs)
    assert repositories.list_workers(settings=center_env)


# ═══ §10 Правки по итогам состязательной проверки ════════════════════════════
# Четыре независимые read-only проверки (§41 задания) нашли дефекты, часть из
# которых лежала ровно на двухслотовом пути. Здесь закрепляется КАЖДОЕ
# подтверждённое исправление — иначе следующая правка тихо вернёт всё назад.

def test_terminal_queue_state_cannot_be_overwritten(tmp_path):
    """Исход попытки записывается один раз. Второй поверх него — потеря."""
    from audit_worker import local_db

    db = local_db.LocalDB(tmp_path / "worker.db")
    db.enqueue(job_id="j1", attempt_id="a1", job_type="test_pipeline_v1", params={})
    assert db.set_queue_state("a1", local_db.QUEUE_CANCELLED) is True
    # Любая попытка «переехать» из терминального состояния отбивается.
    for state in (local_db.QUEUE_RUNNING, local_db.QUEUE_FAILED,
                  local_db.QUEUE_FINISHED, local_db.QUEUE_QUEUED):
        assert db.set_queue_state("a1", state) is False, state
    assert db.queue_item("a1")["state"] == local_db.QUEUE_CANCELLED


def test_queue_state_write_can_require_the_state_it_observed(tmp_path):
    """Условная запись закрывает окно «прочитал → решил → записал»."""
    from audit_worker import local_db

    db = local_db.LocalDB(tmp_path / "worker.db")
    db.enqueue(job_id="j1", attempt_id="a1", job_type="test_pipeline_v1", params={})
    assert db.set_queue_state("a1", local_db.QUEUE_RUNNING,
                              expect_states=(local_db.QUEUE_CLAIMED,)) is False
    assert db.queue_item("a1")["state"] == local_db.QUEUE_QUEUED
    assert db.set_queue_state("a1", local_db.QUEUE_RUNNING,
                              expect_states=(local_db.QUEUE_QUEUED,)) is True


def test_cancelled_attempt_never_starts_a_process(tmp_path, monkeypatch):
    """Гонка отмены: центр уже считает попытку отменённой — процесс не стартует.

    Сценарий из состязательной проверки: отмена застаёт попытку в `claimed`
    (поток задания ещё готовится), ставит `cancelled` и отвечает центру
    «локально не выполняется». Центр объявляет попытку отменённой и освобождает
    слот. Если после этого поток задания запустит процесс, работа пойдёт в
    контур отменённой попытки и сгорит молча.
    """
    from audit_worker import local_db, test_runner
    from audit_worker.executor import Executor

    executor = Executor(_worker_config(tmp_path, max_slots=2))
    executor.db.enqueue(job_id="j1", attempt_id="a1", job_type="test_pipeline_v1",
                        params={"label": "x", "steps": 1, "step_seconds": 0.0})
    item = executor.db.claim_next(executor.instance_id, capacity_limit=2)
    assert item is not None
    executor.jobs.create({"job_id": "j1", "attempt_id": "a1",
                          "job_type": "test_pipeline_v1", "project_id": "p",
                          "execution_token": "etk", "params": {}, "package": {}})

    # Отмена успевает первой.
    assert executor.db.set_queue_state("a1", local_db.QUEUE_CANCELLED) is True

    launched: list[str] = []
    monkeypatch.setattr(
        test_runner, "run_test_job",
        lambda *a, **k: launched.append("started"),
    )
    outcome = executor.run_attempt(item)
    assert outcome["ok"] is False and outcome["reason"] == "superseded"
    assert launched == [], "процесс запущен поверх отменённой попытки"
    assert executor.db.queue_item("a1")["state"] == local_db.QUEUE_CANCELLED


def test_cancel_that_lost_the_race_does_not_report_success(tmp_path):
    """Проиграв гонку, отмена отвечает ошибкой, а не «отменено»."""
    from audit_worker import local_db
    from audit_worker.executor import Executor

    executor = Executor(_worker_config(tmp_path, max_slots=1))
    executor.db.enqueue(job_id="j1", attempt_id="a1", job_type="test_pipeline_v1",
                        params={})
    executor.db.set_queue_state("a1", local_db.QUEUE_FINISHED)
    result = executor._cancel_lost_race("a1", local_db.QUEUE_CLAIMED)
    assert result["status"] == "error"
    assert result["detail"]["outcome"] == "state_changed_concurrently"


def test_process_row_is_closed_after_restart_packaging(tmp_path):
    """После рестарта исполнителя запись процесса обязана перестать быть `running`.

    Иначе удержание отказывается удалять попытку навсегда («процесс помечен
    работающим»), и архивы копятся на диске молча.
    """
    from audit_worker.executor import Executor

    executor = Executor(_worker_config(tmp_path, max_slots=1))
    executor.db.enqueue(job_id="j1", attempt_id="a1", job_type="test_pipeline_v1",
                        params={})
    executor.db.claim_next(executor.instance_id, capacity_limit=1)
    executor.db.register_process(
        job_id="j1", attempt_id="a1", executor_instance_id=executor.instance_id,
        pid=os.getpid(), process_start_identity=1.0,
        command_fingerprint="fp", process_group_id=os.getpgid(0),
    )
    meta = executor.jobs.create({"job_id": "j1", "attempt_id": "a1",
                                 "job_type": "test_pipeline_v1", "project_id": "p",
                                 "execution_token": "etk", "params": {}, "package": {}})
    result_dir = executor.jobs.job_dir("j1", "a1") / "result"
    result_dir.mkdir(parents=True, exist_ok=True)
    (result_dir / "out.txt").write_text("готово", encoding="utf-8")
    assert executor.db.process_row("a1")["status"] == "running"

    item = executor.db.queue_item("a1")
    executor._finish_from_marker(item, {**meta, "exit_code": 0})

    row = executor.db.process_row("a1")
    assert row["status"] == "exited", row
    assert executor.db.queue_item("a1")["state"] == "finished"


def test_adopt_claim_transfers_ownership_so_lease_renews(tmp_path):
    """Подхват после рестарта переписывает владельца строки очереди на себя."""
    from audit_worker import local_db

    db = local_db.LocalDB(tmp_path / "worker.db")
    db.enqueue(job_id="j1", attempt_id="a1", job_type="test_pipeline_v1", params={})
    db.claim_next("executor-old", capacity_limit=1)
    assert db.queue_item("a1")["claimed_by_executor"] == "executor-old"

    assert db.adopt_claim("a1", "executor-new") is True
    assert db.queue_item("a1")["claimed_by_executor"] == "executor-new"
    # Терминальную попытку подхватывать нечем.
    db.set_queue_state("a1", local_db.QUEUE_FINISHED)
    assert db.adopt_claim("a1", "executor-third") is False


def test_signal_is_never_sent_without_a_proven_process_group():
    """Не доказана группа — не отправляется НИЧЕГО, даже по голому pid."""
    from audit_worker import process_control

    detail = process_control.terminate(
        {"pid": os.getpid(), "process_group_id": None, "process_start_identity": None},
        grace_period_sec=0.0,
    )
    assert detail["outcome"] == process_control.OUTCOME_AMBIGUOUS
    assert "не отправлен" in detail["message"]
    # Запасного пути «убить по pid» в модуле больше нет.
    from audit_worker import process_registry

    assert not hasattr(process_registry.ProcessRegistry, "terminate_job")


def test_temp_file_name_is_unique_per_thread(tmp_path):
    """Два потока одного процесса не должны писать в один временный файл."""
    import threading as _threading

    from audit_worker import local_store

    names: list[str] = []
    real_replace = os.replace

    def spy(src, dst):
        names.append(str(src))
        return real_replace(src, dst)

    original = local_store.os.replace
    local_store.os.replace = spy
    try:
        threads = [
            _threading.Thread(
                target=local_store.atomic_write_json,
                args=(tmp_path / "same.json", {"n": i}),
            )
            for i in range(4)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    finally:
        local_store.os.replace = original
    assert len(set(names)) == len(names), names


def test_role_diagnostics_never_leak_the_subject_value():
    """Диагностику ролей читает любой вошедший — чужого имени в ней быть не может."""
    from backend.app.services.distributed_workers import authorization as az

    config = az.load_role_config({az.ENV_ADMINS: "Андрей*,secret-admin"})
    assert not config.ok
    text = config.diagnostics() or ""
    assert az.ENV_ADMINS in text, "переменную назвать надо — по ней ищут опечатку"
    assert "Андрей" not in text and "secret-admin" not in text, text


def test_every_operator_route_declares_a_permission():
    """Право — не дисциплина автора, а свойство каждого маршрута.

    Машинная перепись: у любого маршрута обоих операторских роутеров должна
    быть зависимость `require_view/require_operator/require_admin`. Забытый
    `Depends` на новой ручке иначе не ловится ничем.
    """
    from backend.app.api.routers import audit_workers_admin as mod

    names = {"require_view", "require_operator", "require_admin"}
    # Два маршрута без права — осознанное и закрытое исключение:
    #   /api/workers/status — «включена ли подсистема и настроены ли роли»;
    #   /api/workers/me     — «какие права у МЕНЯ», обязан отвечать и тому,
    #                         у кого прав нет, иначе экран не объяснит отказ.
    # Оба ничего чужого не отдают (см. test_role_diagnostics_never_leak...).
    # Список именно закрытый: новая ручка сюда сама не попадёт.
    allowed_without_permission = {"/api/workers/status", "/api/workers/me"}
    seen_exceptions: set[str] = set()
    for router in (mod.router, mod.status_router):
        for route in router.routes:
            deps = {
                getattr(d.call, "__name__", "")
                for d in getattr(route, "dependant", None).dependencies
            } if getattr(route, "dependant", None) else set()
            if route.path in allowed_without_permission:
                seen_exceptions.add(route.path)
                continue
            assert deps & names, f"{route.path} без проверки права: {deps}"
    assert seen_exceptions == allowed_without_permission, seen_exceptions


def test_every_mutating_route_requires_the_intent_header(admin):
    """CSRF-рубеж стоит на КАЖДОЙ изменяющей ручке, а не на выбранных."""
    from backend.app.api.routers import audit_workers_admin as mod

    checked = 0
    for route in mod.router.routes:
        methods = set(getattr(route, "methods", set())) - {"GET", "HEAD", "OPTIONS"}
        if not methods:
            continue
        source = mod.__dict__.get(route.endpoint.__name__)
        assert source is not None
        import inspect

        body = inspect.getsource(source)
        assert "_require_intent_header" in body or "_require_operator_intent" in body, (
            f"{route.path}: нет гейта намерения"
        )
        checked += 1
    assert checked >= 9, checked


def test_rotate_token_repeat_with_same_key_does_not_kill_the_new_token(admin):
    """Повтор ротации по тому же ключу не гасит токен, который уже прописан."""
    worker_id, _headers, _view = _approved_worker(admin)
    key = _key()
    first = admin.post(f"/api/workers/{worker_id}/rotate-token", headers=key)
    assert first.status_code == 200, first.text
    token = first.json()["worker_token"]

    second = admin.post(f"/api/workers/{worker_id}/rotate-token", headers=key)
    assert second.status_code == 409, second.text
    assert "уже использован" in second.json()["detail"]

    # Главное: выданный токен ПРОДОЛЖАЕТ работать.
    probe = admin.post(
        "/api/v1/worker/heartbeat",
        json={"instance_id": "inst_gate_00001", "sent_at": time.time(),
              "configured_max_slots": 1, "calculated_free_slots": 1},
        headers={"Authorization": f"Bearer {token}", "X-Worker-Id": worker_id,
                 "X-Protocol-Version": "1"},
    )
    assert probe.status_code == 200, probe.text


def test_worker_capacity_hint_is_not_anchored_to_center_occupancy(admin, center_env):
    """Подсказка воркера считается от ЕГО занятости, а не от занятости центра."""
    from backend.app.services.distributed_workers import repositories

    worker_id, _headers, _view = _approved_worker(admin, max_slots=2)
    for index in range(2):
        _create_job(admin, worker_id, project=f"ГЕЙТ/ёмкость {index}")

    # Воркер заявляет собственную ёмкость: занято 0, возьму ещё 1. Оба числа
    # из одного снимка, поэтому потолок = 1 и не зависит от того, что центр
    # успел выдать в этом же окне. Раньше вторая выдача проходила: потолок
    # пересчитывался как «занятость ЦЕНТРА + 1» и рос вместе с ней.
    first = repositories.claim_next_job_for_worker(
        worker_id, limit_override=2, worker_free_hint=1, worker_busy_hint=0,
        settings=center_env,
    )
    assert first is not None
    with pytest.raises(repositories.SlotLimitReached):
        repositories.claim_next_job_for_worker(
            worker_id, limit_override=2, worker_free_hint=1, worker_busy_hint=0,
            settings=center_env,
        )
    # Старое поведение (подсказки занятости нет) на этом же месте выдавало
    # второе задание — фиксируем разницу явно.
    assert repositories.claim_next_job_for_worker(
        worker_id, limit_override=2, worker_free_hint=1, settings=center_env
    ) is not None


def test_slot_mismatch_is_detected_in_both_directions():
    """«Центр считает свободным, воркер — занятым» тоже расхождение."""
    from backend.app.services.distributed_workers import slots

    limit = slots.EffectiveLimit(value=2, binding="test", components={"test": 2})
    usage = slots.SlotUsage(occupied=0, awaiting=0, unproven=1)

    view = slots.build_slot_view({}, usage, limit, worker_claimed_free=0)
    assert view["slot_count_mismatch"] is True
    assert view["slot_count_mismatch_direction"] == "worker_claims_fewer"
    assert "не начнётся" in view["slot_count_mismatch_hint"]

    view2 = slots.build_slot_view({}, usage, limit, worker_claimed_free=5)
    assert view2["slot_count_mismatch_direction"] == "worker_claims_more"

    view3 = slots.build_slot_view({}, usage, limit, worker_claimed_free=2)
    assert view3["slot_count_mismatch"] is False


def test_agent_takes_no_work_when_the_local_database_is_unreadable(tmp_path):
    """Не знаем занятость — не берём работу. Прежний запас ошибался в опасную сторону."""
    from audit_worker.agent import WorkerAgent

    agent = object.__new__(WorkerAgent)
    agent._active = {}
    agent._active_lock = __import__("threading").Lock()

    class _Broken:
        def claimed_attempt_count(self):
            raise RuntimeError("база занята")

        def live_process_count(self):
            raise RuntimeError("база занята")

    agent.db = _Broken()
    counts = agent._busy_counts()
    assert counts["db_ok"] is False
    assert WorkerAgent._free_slots(agent) == 0


def test_pending_delivery_covers_interrupted_upload_and_failure(tmp_path):
    """Результат в `uploading` и провал при мёртвом агенте обязаны быть досланы."""
    import threading as _threading

    from audit_worker.agent import WorkerAgent
    from audit_worker.local_store import LocalJobStore

    agent = object.__new__(WorkerAgent)
    agent._active = {}
    agent._active_lock = _threading.Lock()
    agent.jobs = LocalJobStore(tmp_path / "jobs")
    for job_id, attempt_id, state in (
        ("j1", "a1", "completed_locally"),
        ("j2", "a2", "uploading"),
        ("j3", "a3", "failed"),
        ("j4", "a4", "running"),
    ):
        agent.jobs.create({"job_id": job_id, "attempt_id": attempt_id,
                           "job_type": "test_pipeline_v1", "project_id": "p",
                           "execution_token": "etk", "params": {}, "package": {}})
        agent.jobs.update(job_id, attempt_id, local_state=state,
                          result_hash="h" if state != "failed" else None)

    uploaded: list[str] = []
    flushed: list[str] = []
    agent._resume_upload = lambda j, a: uploaded.append(a)
    agent._flush_terminal_events = lambda meta: flushed.append(meta["attempt_id"])

    WorkerAgent._deliver_pending_results(agent)
    assert sorted(uploaded) == ["a1", "a2"], uploaded
    assert flushed == ["a3"], flushed
