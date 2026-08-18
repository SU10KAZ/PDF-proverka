from __future__ import annotations

import stat
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


@pytest.fixture()
def settings(tmp_path, monkeypatch):
    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "state"))
    from backend.app.services.distributed_workers import database
    from backend.app.services.distributed_workers.settings import get_settings

    database.reset_state_for_tests()
    st = get_settings()
    database.ensure_ready(st)
    yield st
    database.reset_state_for_tests()


def _pending_worker(settings):
    from backend.app.services.distributed_workers import repositories

    return repositories.create_worker(
        display_name="candidate-worker",
        instance_id="inst_candidate_12f1b",
        worker_version="12f1b",
        protocol_version=1,
        pipeline_revision=None,
        capabilities={"job_types": ["test_pipeline_v1"]},
        configured_max_slots=1,
        settings=settings,
    )


def test_production_default_state_root_is_external(monkeypatch):
    monkeypatch.delenv("DISTRIBUTED_WORKERS_DATA_DIR", raising=False)
    from backend.app.services.distributed_workers.settings import get_settings

    assert str(get_settings().data_dir) == "/var/lib/auditmanager/distributed_workers"


def test_state_root_permissions_and_wal(settings):
    assert stat.S_IMODE(settings.data_dir.stat().st_mode) == 0o700
    assert stat.S_IMODE(settings.db_path.stat().st_mode) == 0o600
    from backend.app.services.distributed_workers import database

    from backend.app.services.distributed_workers import schema

    with database.read_conn(settings) as conn:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        # Инвариант: свежесозданная база доведена до КАНОНИЧЕСКОЙ версии кода,
        # а не до числа, записанного здесь однажды. Прежняя жёсткая «12»
        # проверяла возраст теста, а не миграцию: версия 13 приехала вместе с
        # переоформлением личности воркера (e6015d33), и тест начал падать на
        # исправном продакшене. Сравнение с SCHEMA_VERSION ловит настоящую
        # регрессию — недоехавшую или частично применённую миграцию.
        applied = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0]
        assert applied == schema.SCHEMA_VERSION
        # Ни один шаг не должен пропасть: подряд, начиная с первого.
        versions = [
            row[0]
            for row in conn.execute("SELECT version FROM schema_migrations ORDER BY version")
        ]
        assert versions == list(range(1, schema.SCHEMA_VERSION + 1))


def test_new_worker_is_durably_drained_and_heartbeat_cannot_resume(settings):
    from backend.app.services.distributed_workers import (
        registration_service,
        repositories,
        slots,
        worker_registry,
    )

    worker = _pending_worker(settings)
    registration_service.approve_worker(
        worker_id=worker["worker_id"],
        display_name=None,
        configured_max_slots=1,
        settings=settings,
    )
    approved = repositories.get_worker(worker["worker_id"], settings=settings)
    assert approved["intake_enabled"] == 0
    assert worker_registry.can_receive_jobs(approved)[0] is False
    assert slots.effective_limit(approved).value == 0

    worker_registry.record_heartbeat(
        worker_id=worker["worker_id"],
        instance_id=worker["instance_id"],
        worker_state="idle",
        configured_max_slots=1,
        calculated_free_slots=1,
        active_jobs=[],
        resource_snapshot={"at": 1.0},
        warnings=[],
        settings=settings,
    )
    after = repositories.get_worker(worker["worker_id"], settings=settings)
    assert after["intake_enabled"] == 0
    assert slots.effective_limit(after).value == 0


def test_operator_intake_toggle_is_persistent(settings):
    from backend.app.services.distributed_workers import repositories

    worker = _pending_worker(settings)
    before, resumed = repositories.set_worker_intake(
        worker["worker_id"],
        enabled=True,
        actor="operator:andrey",
        reason="approved canary",
        settings=settings,
    )
    assert before["intake_enabled"] == 0
    assert resumed["intake_enabled"] == 1
    assert resumed["intake_updated_by"] == "operator:andrey"
    _, drained = repositories.set_worker_intake(
        worker["worker_id"],
        enabled=False,
        actor="operator:andrey",
        reason="end canary",
        settings=settings,
    )
    assert drained["intake_enabled"] == 0


def test_human_intake_api_enforces_production_roles_and_appends_audit(
    settings, monkeypatch
):
    from backend.app.services.distributed_workers import database, repositories
    from tests.distributed_workers_helpers import (
        enable_portal_roles,
        make_center_app,
        portal_client,
    )

    enable_portal_roles(
        monkeypatch,
        admins=("andrey",),
        operators=(),
        viewers=("igor", "alexey", "filipp", "marina", "alexandra"),
        users=("andrey", "igor", "alexey", "filipp", "marina", "alexandra"),
    )
    worker = _pending_worker(settings)
    app = make_center_app()

    viewer = portal_client(app, username="igor")
    denied = viewer.post(
        f"/api/workers/{worker['worker_id']}/resume-intake",
        json={"reason": "viewer must not operate"},
    )
    assert denied.status_code == 403

    admin = portal_client(app, username="andrey")
    resumed = admin.post(
        f"/api/workers/{worker['worker_id']}/resume-intake",
        json={"reason": "approved canary"},
    )
    assert resumed.status_code == 200
    assert resumed.json()["worker"]["intake_enabled"] is True
    drained = admin.post(
        f"/api/workers/{worker['worker_id']}/drain",
        json={"reason": "canary complete"},
    )
    assert drained.status_code == 200
    assert repositories.get_worker(
        worker["worker_id"], settings=settings
    )["intake_enabled"] == 0

    with database.read_conn(settings) as conn:
        actions = conn.execute(
            "SELECT actor_id,actor_role,permission,action_type,reason "
            "FROM worker_admin_actions ORDER BY created_at",
        ).fetchall()
    assert [row["action_type"] for row in actions] == [
        "worker_intake_resumed",
        "worker_drained",
    ]
    assert all(row["actor_id"] == "operator:andrey" for row in actions)
    assert all(row["actor_role"] == "admin" for row in actions)
    assert all(row["permission"] == "distributed_workers.operate" for row in actions)


def test_human_intake_state_rolls_back_if_audit_append_fails(
    settings, monkeypatch
):
    import sqlite3

    from backend.app.services.distributed_workers import repositories

    worker = _pending_worker(settings)
    monkeypatch.setattr(repositories, "new_id", lambda *_args, **_kwargs: "act_collision")
    repositories.record_admin_action(
        actor_id="operator:andrey",
        actor_display_name="andrey",
        actor_role="admin",
        permission="operate",
        action_type="preexisting_test_action",
        worker_id=worker["worker_id"],
        settings=settings,
    )
    with pytest.raises(sqlite3.IntegrityError):
        repositories.set_worker_intake_with_audit(
            worker["worker_id"],
            enabled=True,
            actor_id="operator:andrey",
            actor_display_name="andrey",
            actor_role="admin",
            permission="operate",
            reason="must roll back",
            settings=settings,
        )
    unchanged = repositories.get_worker(worker["worker_id"], settings=settings)
    assert unchanged["intake_enabled"] == 0


def test_approved_portal_mapping_and_role_boundaries(monkeypatch):
    from backend.app.services.distributed_workers import authorization as az

    env = {
        az.ENV_VIEWERS: "igor,alexey,filipp,marina,alexandra",
        az.ENV_OPERATORS: "",
        az.ENV_ADMINS: "andrey",
    }
    roles = az.load_role_config(env)
    assert roles.role_for("andrey") == az.ROLE_ADMIN
    for subject in ("igor", "alexey", "filipp", "marina", "alexandra"):
        assert roles.role_for(subject) == az.ROLE_VIEWER
    assert az.ROLE_PERMISSIONS[az.ROLE_VIEWER] == {az.PERM_VIEW}
    assert az.PERM_OPERATE in az.ROLE_PERMISSIONS[az.ROLE_OPERATOR]
    assert az.PERM_ADMIN not in az.ROLE_PERMISSIONS[az.ROLE_OPERATOR]
    assert az.ROLE_PERMISSIONS[az.ROLE_ADMIN] >= {
        az.PERM_VIEW, az.PERM_OPERATE, az.PERM_ADMIN
    }


def test_registration_accepts_only_one_time_instance_scoped_token(settings):
    from backend.app.services.worker_bootstrap import store

    # A real persisted bootstrap session is required before a token can exist.
    from backend.app.services.worker_bootstrap.models import (
        BootstrapOperation,
        BootstrapRequest,
    )

    request = BootstrapRequest(
        host="worker.example",
        ssh_user="audit-worker",
        ssh_auth_ref="secret-store:worker",
        expected_host_fingerprint="SHA256:" + "A" * 32,
        install_root="/opt/audit-worker/releases/r1",
        center_url="https://auditmanager.app",
        display_name="candidate",
        bootstrap_instance_id="inst_candidate_12f1b",
    )
    session = store.create_session(
        operation=BootstrapOperation.INSTALL,
        request=request,
        idempotency_key="candidate-token-test",
        settings=settings,
    )
    token = store.issue_registration_token(
        session["session_id"],
        expected_instance_id="inst_candidate_12f1b",
        ttl_sec=60,
        settings=settings,
        now=100.0,
    )
    assert token.startswith(store.TOKEN_PREFIX)
    assert store.consume_registration_token(
        token,
        instance_id="inst_candidate_12f1b",
        settings=settings,
        now=101.0,
    ) == session["session_id"]
    with pytest.raises(store.RegistrationTokenRejected):
        store.consume_registration_token(
            token,
            instance_id="inst_candidate_12f1b",
            settings=settings,
            now=102.0,
        )


def test_no_reusable_bootstrap_fallback_in_runtime_source():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    checked = [
        root / "backend/app/core/config.py",
        root / "backend/app/services/distributed_workers/settings.py",
        root / "backend/app/services/distributed_workers/auth.py",
        root / "backend/app/api/routers/audit_worker_agent.py",
        root / ".env.example",
    ]
    joined = "\n".join(path.read_text(encoding="utf-8") for path in checked)
    assert "DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET" not in joined
    assert "verify_bootstrap_secret" not in joined


def test_candidate_service_definitions_pin_release_and_stable_endpoint():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    backend = (root / "deploy/systemd/auditmanager-backend@.service").read_text()
    agent = (root / "deploy/systemd/audit-worker-agent@.service").read_text()
    env = (root / "deploy/systemd/backend.env.example").read_text()
    assert "/opt/auditmanager/releases/%i" in backend
    assert "UMask=0077" in backend
    assert "/var/lib/auditmanager/distributed_workers" in backend
    assert "AUDIT_WORKER_DISPATCHER_URL=https://auditmanager.app" in agent
    assert "DISTRIBUTED_WORKERS_ENABLED=false" in env
    assert "DISTRIBUTED_AUDIT_EXECUTION_ENABLED=false" in env
    assert "AUDIT_WORKER_ALLOW_REAL_LLM=false" in env


def test_portal_exposes_role_gated_human_drain_controls():
    root = Path(__file__).resolve().parents[1]
    source = (root / "frontend/static/js/audit-workers.js").read_text()
    assert "data-drain" in source
    assert "data-resume-intake" in source
    assert "state.perms.canOperate" in source
    assert "resume-intake" in source
    assert "operator drain" in source
