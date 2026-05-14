"""
test_legacy_webapp_audit_version_id.py
--------------------------------------
Safety-gate: legacy webapp.main:app должен:

1. Принимать version_id query parameter в audit/optimization endpoints.
2. Если version_id отсутствует или 'v1' → старое legacy-поведение.
3. Если version_id != 'v1' → 409 Conflict без запуска legacy pipeline.

Это временный gate. Полноценный аудит V2 будет реализован отдельным проходом.

Run:
    python -m pytest tests/test_legacy_webapp_audit_version_id.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


# ─── Fixtures ───────────────────────────────────────────────────────────


@pytest.fixture
def legacy_client():
    """TestClient(webapp.main:app), без подмены pipeline_manager —
    проверяем именно gate, а не вызов manager'а."""
    from webapp.main import app
    return TestClient(app)


@pytest.fixture
def mocked_manager(monkeypatch):
    """Подменить legacy pipeline_manager на мок, чтобы детектировать вызовы.

    Возвращает мок-объект. Все start_* функции — AsyncMock, чтобы их можно
    было `await`. _check_project тоже патчим, чтобы не валидировать реальный
    проект (мы тестируем только gate)."""
    import webapp.routers.audit as audit_router
    import webapp.routers.optimization as opt_router

    manager_mock = MagicMock()

    # Все start_* должны быть awaitable и возвращать AuditJob-like объект.
    job_mock = MagicMock()
    job_mock.model_dump.return_value = {"job_id": "test", "status": "queued"}

    for name in [
        "start_prepare", "start_tile_audit", "start_main_audit",
        "start_smart_audit", "start_audit", "resume_pipeline",
        "start_from_stage", "start_norm_verify", "start_optimization",
    ]:
        setattr(manager_mock, name, AsyncMock(return_value=job_mock))

    manager_mock.detect_resume_stage = MagicMock(return_value={"resume_stage": None})

    monkeypatch.setattr(audit_router, "pipeline_manager", manager_mock)
    monkeypatch.setattr(opt_router, "pipeline_manager", manager_mock)

    # _check_project не должен падать на отсутствующих проектах.
    monkeypatch.setattr(audit_router, "_check_project", lambda pid: None)
    monkeypatch.setattr(opt_router, "_check_project", lambda pid: None)

    return manager_mock


# ─── 1. full-audit без version_id ───────────────────────────────────────


def test_full_audit_without_version_id_calls_manager(legacy_client, mocked_manager):
    """Старый legacy-сценарий: без ?version_id= manager вызывается как раньше."""
    r = legacy_client.post("/api/audit/SOMEPROJ/full-audit")
    assert r.status_code == 200
    mocked_manager.start_audit.assert_awaited_once_with("SOMEPROJ")


# ─── 2. full-audit?version_id=v1 ────────────────────────────────────────


def test_full_audit_with_v1_calls_manager(legacy_client, mocked_manager):
    """version_id=v1 — то же самое, что без version_id."""
    r = legacy_client.post("/api/audit/SOMEPROJ/full-audit?version_id=v1")
    assert r.status_code == 200
    mocked_manager.start_audit.assert_awaited_once_with("SOMEPROJ")


# ─── 3. full-audit?version_id=v2 → 409, manager не вызван ───────────────


def test_full_audit_with_v2_returns_409(legacy_client, mocked_manager):
    r = legacy_client.post("/api/audit/SOMEPROJ/full-audit?version_id=v2")
    assert r.status_code == 409
    body = r.json()
    assert "v2" in body["detail"]
    assert "legacy" in body["detail"].lower()
    mocked_manager.start_audit.assert_not_called()


def test_full_audit_with_v3_also_rejected(legacy_client, mocked_manager):
    r = legacy_client.post("/api/audit/SOMEPROJ/full-audit?version_id=v3")
    assert r.status_code == 409
    mocked_manager.start_audit.assert_not_called()


# ─── 4. Все остальные audit endpoints ───────────────────────────────────


@pytest.mark.parametrize("endpoint,manager_method", [
    ("/api/audit/X/prepare", "start_prepare"),
    ("/api/audit/X/tile-audit", "start_tile_audit"),
    ("/api/audit/X/main-audit", "start_main_audit"),
    ("/api/audit/X/smart-audit", "start_smart_audit"),
    ("/api/audit/X/full-audit", "start_audit"),
    ("/api/audit/X/standard-audit", "start_audit"),
    ("/api/audit/X/pro-audit", "start_audit"),
    ("/api/audit/X/resume", "resume_pipeline"),
    ("/api/audit/X/verify-norms", "start_norm_verify"),
])
def test_post_audit_endpoints_v2_gate(
    legacy_client, mocked_manager, endpoint, manager_method,
):
    """Каждый POST-endpoint аудита блокирует V2."""
    r = legacy_client.post(f"{endpoint}?version_id=v2")
    assert r.status_code == 409, r.text
    getattr(mocked_manager, manager_method).assert_not_called()


def test_start_from_v2_gate(legacy_client, mocked_manager):
    """start-from требует обязательный stage; v2 — gate срабатывает раньше."""
    r = legacy_client.post("/api/audit/X/start-from?stage=prepare&version_id=v2")
    assert r.status_code == 409
    mocked_manager.start_from_stage.assert_not_called()


def test_resume_info_v2_gate(legacy_client, mocked_manager):
    """resume-info?version_id=v2 не должен подтягивать V1 — возвращаем 409."""
    r = legacy_client.get("/api/audit/X/resume-info?version_id=v2")
    assert r.status_code == 409
    mocked_manager.detect_resume_stage.assert_not_called()


# ─── 5. optimization/run gate ───────────────────────────────────────────


def test_optimization_run_without_version_calls_manager(legacy_client, mocked_manager):
    r = legacy_client.post("/api/optimization/X/run")
    assert r.status_code == 200
    mocked_manager.start_optimization.assert_awaited_once_with("X")


def test_optimization_run_v1_calls_manager(legacy_client, mocked_manager):
    r = legacy_client.post("/api/optimization/X/run?version_id=v1")
    assert r.status_code == 200
    mocked_manager.start_optimization.assert_awaited_once_with("X")


def test_optimization_run_v2_returns_409(legacy_client, mocked_manager):
    r = legacy_client.post("/api/optimization/X/run?version_id=v2")
    assert r.status_code == 409
    body = r.json()
    assert "v2" in body["detail"]
    assert "оптимизации" in body["detail"].lower() or "legacy" in body["detail"].lower()
    mocked_manager.start_optimization.assert_not_called()


# ─── 6. Other version_id values (защита от обхода) ──────────────────────


def test_v2_with_uppercase_still_rejected(legacy_client, mocked_manager):
    """V2 в верхнем регистре тоже должен ловиться gate."""
    r = legacy_client.post("/api/audit/X/full-audit?version_id=V2")
    assert r.status_code == 409
    mocked_manager.start_audit.assert_not_called()


def test_v1_with_whitespace_normalized(legacy_client, mocked_manager):
    """v1 с пробелами вокруг — допустимо (lstrip/rstrip + lower)."""
    r = legacy_client.post("/api/audit/X/full-audit?version_id=%20v1%20")
    assert r.status_code == 200
    mocked_manager.start_audit.assert_awaited_once_with("X")


# ─── 7. Старые endpoints без version_id вообще не сломаны ───────────────


def test_old_endpoints_still_register_in_openapi(legacy_client):
    """Все endpoints из спецификации присутствуют в OpenAPI."""
    r = legacy_client.get("/openapi.json")
    paths = r.json().get("paths", {})
    expected = [
        "/api/audit/{project_id}/prepare",
        "/api/audit/{project_id}/tile-audit",
        "/api/audit/{project_id}/main-audit",
        "/api/audit/{project_id}/smart-audit",
        "/api/audit/{project_id}/full-audit",
        "/api/audit/{project_id}/standard-audit",
        "/api/audit/{project_id}/pro-audit",
        "/api/audit/{project_id}/resume",
        "/api/audit/{project_id}/resume-info",
        "/api/audit/{project_id}/start-from",
        "/api/audit/{project_id}/verify-norms",
        "/api/optimization/{project_id}/run",
    ]
    missing = [p for p in expected if p not in paths]
    assert not missing, f"missing endpoints: {missing}"


def test_full_audit_openapi_has_version_id_query(legacy_client):
    """Подтверждение: version_id виден в OpenAPI как query."""
    r = legacy_client.get("/openapi.json")
    op = r.json()["paths"]["/api/audit/{project_id}/full-audit"]["post"]
    params = [p for p in op.get("parameters", []) if p.get("name") == "version_id"]
    assert params, "version_id should be exposed as query parameter"
    assert params[0]["in"] == "query"
