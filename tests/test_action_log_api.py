"""Интеграционные тесты журнала действий на полном приложении.

Проверяют: middleware пишет события реальных запросов, /api/action-log отдаёт
их с фильтрами, /api/action-log/stats считает сводку. ACTION_LOG_DIR изолирован
autouse-фикстурой _isolate_action_log (tmp_path / "actions_log")."""
import json

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    from backend.app.main import app
    # Без context-manager → lifespan (pipeline manager) не запускается.
    return TestClient(app, raise_server_exceptions=False)


def _events(tmp_path):
    log_dir = tmp_path / "actions_log"
    events = []
    if log_dir.exists():
        for path in sorted(log_dir.glob("actions-*.jsonl")):
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    events.append(json.loads(line))
    return events


def test_middleware_logs_real_requests(client, tmp_path):
    resp = client.get("/api/projects")
    assert resp.status_code == 200
    api_events = [e for e in _events(tmp_path) if e["kind"] == "api"]
    assert any(e["path"] == "/api/projects" and e["status"] == 200 for e in api_events)


def test_middleware_skips_api_info_noise(client, tmp_path):
    assert client.get("/api/info").status_code == 200
    assert not [e for e in _events(tmp_path) if e.get("path") == "/api/info"]


def test_middleware_logs_404(client, tmp_path):
    assert client.get("/api/definitely-nonexistent-endpoint").status_code == 404
    events = [e for e in _events(tmp_path) if e.get("status") == 404]
    assert len(events) == 1
    assert events[0]["path"] == "/api/definitely-nonexistent-endpoint"


def test_action_log_api_roundtrip(client, tmp_path):
    from backend.app.core import action_log

    action_log.log_event("pipeline", project_id="ЭОМ/К1", stage="excel",
                         status="error", error="сбой отчёта")
    action_log.log_event("api", actor="ivan", method="POST",
                         path="/api/audit/x/full-audit", status=200, dur_ms=15)

    resp = client.get("/api/action-log", params={"limit": 100})
    assert resp.status_code == 200, resp.text
    data = resp.json()
    kinds = [e["kind"] for e in data["items"]]
    assert "pipeline" in kinds and "api" in kinds
    assert "persons" in data

    # Фильтр ошибок: pipeline error попадает, api 200 — нет.
    resp = client.get("/api/action-log", params={"errors_only": "true", "kind": "pipeline"})
    items = resp.json()["items"]
    assert len(items) == 1
    assert items[0]["error"] == "сбой отчёта"

    # Поиск подстрокой.
    resp = client.get("/api/action-log", params={"q": "full-audit"})
    assert len(resp.json()["items"]) == 1


def test_action_log_stats_endpoint(client, tmp_path):
    from backend.app.core import action_log

    action_log.log_event("api", actor="ivan", method="GET", path="/api/x", status=200)
    resp = client.get("/api/action-log/stats", params={"days": 3})
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["totals"]["events"] >= 1
    assert data["days"]


def test_action_log_query_validation(client):
    assert client.get("/api/action-log", params={"limit": 0}).status_code == 422
    assert client.get("/api/action-log", params={"limit": 5000}).status_code == 422
    # Кривые даты — явный 422, а не тихий пустой/неотфильтрованный результат.
    assert client.get("/api/action-log", params={"date_from": "2026-7-5"}).status_code == 422
    assert client.get("/api/action-log", params={"date_to": "вчера"}).status_code == 422
    assert client.get("/api/action-log", params={"date_from": "2026-07-05"}).status_code == 200
