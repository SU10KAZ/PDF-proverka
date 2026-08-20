"""Тесты ядра журнала действий (backend/app/core/action_log.py)."""
import json
import logging

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.core import action_log
from backend.app.core import config as cfg


def _events(log_dir):
    """Все события всех суточных файлов (старые → новые)."""
    events = []
    for path in sorted(log_dir.glob("actions-*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                events.append(json.loads(line))
    return events


@pytest.fixture
def log_dir(tmp_path, monkeypatch):
    d = tmp_path / "actions_log"
    monkeypatch.setattr(cfg, "ACTION_LOG_DIR", d, raising=False)
    monkeypatch.setattr(cfg, "ACTION_LOG_ENABLED", True, raising=False)
    return d


# ─── Писатель ────────────────────────────────────────────────────────────────
def test_log_event_roundtrip(log_dir):
    action_log.log_event("api", actor="ivan", status=200, skipped=None)
    events = _events(log_dir)
    assert len(events) == 1
    e = events[0]
    assert e["kind"] == "api"
    assert e["actor"] == "ivan"
    assert e["status"] == 200
    assert "skipped" not in e  # None-поля отбрасываются
    assert e["ts"]  # timestamp проставлен


def test_log_event_disabled(log_dir, monkeypatch):
    monkeypatch.setattr(cfg, "ACTION_LOG_ENABLED", False, raising=False)
    action_log.log_event("api", actor="ivan")
    assert not log_dir.exists() or not _events(log_dir)


def test_log_event_never_raises(tmp_path, monkeypatch):
    # Директория недоступна для записи (файл вместо папки) → fail-soft.
    bad = tmp_path / "not_a_dir"
    bad.write_text("x", encoding="utf-8")
    monkeypatch.setattr(cfg, "ACTION_LOG_DIR", bad / "sub", raising=False)
    action_log.log_event("api", actor="ivan")  # не должно бросить


def test_retention_cleanup(log_dir, monkeypatch):
    from datetime import date, timedelta
    monkeypatch.setattr(cfg, "ACTION_LOG_RETENTION_DAYS", 30, raising=False)
    log_dir.mkdir(parents=True)
    fresh = (date.today() - timedelta(days=3)).isoformat()
    (log_dir / "actions-2020-01-01.jsonl").write_text("{}\n", encoding="utf-8")
    (log_dir / "actions-2020-01-02.jsonl").write_text("{}\n", encoding="utf-8")
    (log_dir / f"actions-{fresh}.jsonl").write_text("{}\n", encoding="utf-8")
    # Смена (dir, day) → триггер чистки при первой записи.
    action_log.log_event("system", event="test")
    names = sorted(p.name for p in log_dir.glob("actions-*.jsonl"))
    # retention=30 дней: файлы 2020 года удалены, свежие (3 дня + сегодня) живут
    assert names == [f"actions-{fresh}.jsonl", f"actions-{date.today().isoformat()}.jsonl"]


def test_writer_self_heals_after_dir_removal(log_dir):
    """Пропажа директории посреди дня не должна убивать журнал до конца дня."""
    import shutil
    action_log.log_event("system", event="one")
    shutil.rmtree(log_dir)
    action_log.log_event("system", event="two")   # падает → сброс ключа
    action_log.log_event("system", event="three")  # самолечение: mkdir заново
    events = _events(log_dir)
    assert [e["event"] for e in events] == ["three"]


def test_day_cap_stops_writes(log_dir, monkeypatch):
    monkeypatch.setattr(cfg, "ACTION_LOG_MAX_DAY_BYTES", 200, raising=False)
    action_log.log_event("api", path="/api/x", status=200)  # ~60 байт — влезает
    action_log.log_event("api", path="/api/y" + "y" * 200, status=200)  # превысит
    action_log.log_event("api", path="/api/z", status=200)  # уже за потолком
    events = _events(log_dir)
    # первое событие + один маркер day_cap_reached; дальше — тишина
    assert len(events) == 2
    assert events[0]["kind"] == "api" and events[0]["path"] == "/api/x"
    assert events[1]["kind"] == "system" and events[1]["event"] == "day_cap_reached"


# ─── Шум-фильтр ──────────────────────────────────────────────────────────────
@pytest.mark.parametrize("path", [
    "/static/js/app.js",
    "/api/info",
    "/api/auth/me",
    "/api/audit/live-status",
    "/api/audit/batch/status",
    "/api/audit/pause/status",
    "/api/audit/ЭОМ/13АВ-РД-ЭМ-К1/status",
    "/api/audit/ЭОМ/13АВ-РД-ЭМ-К1/log",
    "/api/audit/prepare-data/queue",
    "/api/usage/counters",
    "/api/usage/global",
    "/api/lms/health",
    "/api/document/ЭОМ/К1/page/12",
    "/api/tiles/ЭОМ/К1/blocks/image/6L97",
    "/api/stage-comparison/sessions/s1/pairs/p1/page-svg",
])
def test_noise_paths(path):
    assert action_log.is_noise_path(path), path


@pytest.mark.parametrize("path", [
    "/",
    "/login",
    "/api/projects",
    "/api/audit/ЭОМ/13АВ-РД-ЭМ-К1/full-audit",
    "/api/findings/ЭОМ/13АВ-РД-ЭМ-К1",
    "/api/document/ЭОМ/К1/pdf",
    "/api/knowledge-base/expert-review/ЭОМ/К1",
    "/api/stage-comparison/sessions",
    "/api/action-log",
])
def test_non_noise_paths(path):
    assert not action_log.is_noise_path(path), path


def test_noise_extra_from_config(monkeypatch):
    monkeypatch.setattr(cfg, "ACTION_LOG_NOISE_EXTRA", [r"^/api/custom-poll$"], raising=False)
    assert action_log.is_noise_path("/api/custom-poll")
    monkeypatch.setattr(cfg, "ACTION_LOG_NOISE_EXTRA", [], raising=False)
    assert not action_log.is_noise_path("/api/custom-poll")


# ─── Middleware ──────────────────────────────────────────────────────────────
@pytest.fixture
def mw_client(log_dir):
    app = FastAPI()

    @app.get("/api/ok")
    async def ok():
        return {"ok": True}

    @app.post("/api/info")
    async def info_post():
        return {"ok": True}

    @app.get("/api/info")
    async def info_get():
        return {"ok": True}

    @app.get("/api/boom")
    async def boom():
        raise RuntimeError("взрыв в endpoint")

    @app.get("/api/projects/{project_id:path}/card")
    async def card(project_id: str):
        return {"project_id": project_id}

    app.add_middleware(action_log.ActionLogMiddleware)
    return TestClient(app, raise_server_exceptions=False)


def test_middleware_logs_ok_request(mw_client, log_dir):
    resp = mw_client.get("/api/ok", params={"x": "1"})
    assert resp.status_code == 200
    events = _events(log_dir)
    assert len(events) == 1
    e = events[0]
    assert e["kind"] == "api"
    assert e["method"] == "GET"
    assert e["path"] == "/api/ok"
    assert e["query"] == "x=1"
    assert e["status"] == 200
    assert isinstance(e["dur_ms"], int)


def test_middleware_skips_noisy_get_but_logs_post(mw_client, log_dir):
    assert mw_client.get("/api/info").status_code == 200
    assert not _events(log_dir)  # шумовой GET не пишется
    assert mw_client.post("/api/info").status_code == 200
    events = _events(log_dir)
    assert len(events) == 1  # мутирующий запрос пишется всегда
    assert events[0]["method"] == "POST"


def test_middleware_logs_error_status(mw_client, log_dir):
    resp = mw_client.get("/api/nonexistent")
    assert resp.status_code == 404
    events = _events(log_dir)
    assert len(events) == 1
    assert events[0]["status"] == 404


def test_middleware_logs_exception_with_traceback(mw_client, log_dir):
    resp = mw_client.get("/api/boom")
    assert resp.status_code == 500
    events = _events(log_dir)
    assert len(events) == 1
    e = events[0]
    assert "RuntimeError" in e["error"]
    assert "взрыв" in e["error"]
    assert "traceback" in e


def test_middleware_extracts_project_id(mw_client, log_dir):
    mw_client.get("/api/projects/ЭОМ/13АВ-РД-ЭМ-К1/card")
    events = _events(log_dir)
    assert events[0]["project_id"] == "ЭОМ/13АВ-РД-ЭМ-К1"


def test_middleware_resolves_actor_from_cookie(mw_client, log_dir, monkeypatch):
    from backend.app.core import portal_auth
    monkeypatch.setenv("PORTAL_AUTH_USERS", "ivan:x-hash")
    monkeypatch.setenv("PORTAL_SESSION_SECRET", "test-secret")
    token = portal_auth.issue_token("ivan", portal_auth.get_settings())
    mw_client.cookies.set("portal_session", token)
    mw_client.get("/api/ok")
    events = _events(log_dir)
    assert events[0]["actor"] == "ivan"


def test_middleware_disabled_by_flag(mw_client, log_dir, monkeypatch):
    monkeypatch.setattr(cfg, "ACTION_LOG_HTTP_ENABLED", False, raising=False)
    mw_client.get("/api/ok")
    assert not _events(log_dir)


# ─── Хук конвейера ───────────────────────────────────────────────────────────
def test_log_pipeline_event(log_dir):
    action_log.log_pipeline_event(
        "ЭОМ/К1", "block_analysis", "error", message="", error="упало", duration_sec=12,
    )
    events = _events(log_dir)
    assert len(events) == 1
    e = events[0]
    assert e["kind"] == "pipeline"
    assert e["project_id"] == "ЭОМ/К1"
    assert e["stage"] == "block_analysis"
    assert e["status"] == "error"
    assert e["error"] == "упало"
    assert e["duration_sec"] == 12
    assert "message" not in e  # пустая строка не пишется


def test_pipeline_hook_via_audit_logger(log_dir, tmp_path, monkeypatch):
    """update_pipeline_log → событие kind=pipeline в журнале."""
    from backend.app.services.common import audit_logger
    out_dir = tmp_path / "_output"
    monkeypatch.setattr(audit_logger, "_project_output_dir", lambda pid: out_dir)
    audit_logger.update_pipeline_log("ЭОМ/К1", "text_analysis", "running", message="старт")
    events = [e for e in _events(log_dir) if e["kind"] == "pipeline"]
    assert len(events) == 1
    assert events[0]["stage"] == "text_analysis"
    assert events[0]["status"] == "running"


def test_pipeline_disabled_by_flag(log_dir, monkeypatch):
    monkeypatch.setattr(cfg, "ACTION_LOG_PIPELINE_ENABLED", False, raising=False)
    action_log.log_pipeline_event("ЭОМ/К1", "excel", "done")
    assert not _events(log_dir)


# ─── Мост logging ────────────────────────────────────────────────────────────
def test_logging_bridge(log_dir):
    root = logging.getLogger()
    added_before = [h for h in root.handlers if isinstance(h, action_log._ActionLogHandler)]
    for h in added_before:
        root.removeHandler(h)
    try:
        action_log.install_logging_bridge()
        action_log.install_logging_bridge()  # идемпотентно
        handlers = [h for h in root.handlers if isinstance(h, action_log._ActionLogHandler)]
        assert len(handlers) == 1

        logging.getLogger("backend.test.module").warning("тестовое предупреждение %s", 42)
        logging.getLogger("backend.test.module").info("info не пишется")
        events = [e for e in _events(log_dir) if e["kind"] == "app_log"]
        assert len(events) == 1
        e = events[0]
        assert e["level"] == "WARNING"
        assert e["logger"] == "backend.test.module"
        assert e["message"] == "тестовое предупреждение 42"
    finally:
        for h in [h for h in root.handlers if isinstance(h, action_log._ActionLogHandler)]:
            root.removeHandler(h)


def test_uninstall_logging_bridge(log_dir):
    root = logging.getLogger()
    action_log.install_logging_bridge()
    assert any(isinstance(h, action_log._ActionLogHandler) for h in root.handlers)
    action_log.uninstall_logging_bridge()
    assert not any(isinstance(h, action_log._ActionLogHandler) for h in root.handlers)
    action_log.uninstall_logging_bridge()  # идемпотентно


def test_applog_rate_limit(log_dir, monkeypatch):
    monkeypatch.setattr(cfg, "ACTION_LOG_APPLOG_MAX_PER_MIN", 3, raising=False)
    monkeypatch.setattr(
        action_log, "_APPLOG_WINDOW", {"minute": None, "count": 0, "suppressed": 0},
    )
    handler = action_log._ActionLogHandler(level=logging.WARNING)
    logger = logging.getLogger("backend.test.flood")
    logger.addHandler(handler)
    logger.propagate = False
    try:
        for i in range(10):
            logger.warning("шторм %s", i)
        events = [e for e in _events(log_dir) if e["kind"] == "app_log"]
        # лимит 3/мин: первые 3 записаны, остальные 7 подавлены
        assert len(events) == 3
        assert action_log._APPLOG_WINDOW["suppressed"] == 7
    finally:
        logger.removeHandler(handler)
        logger.propagate = True


def test_logging_bridge_captures_exc_info(log_dir):
    handler = action_log._ActionLogHandler(level=logging.WARNING)
    logger = logging.getLogger("backend.test.exc")
    logger.addHandler(handler)
    logger.propagate = False
    try:
        try:
            raise ValueError("детали ошибки")
        except ValueError:
            logger.exception("поймано")
        events = [e for e in _events(log_dir) if e["kind"] == "app_log"]
        assert len(events) == 1
        assert events[0]["level"] == "ERROR"
        assert "ValueError" in events[0]["exc"]
    finally:
        logger.removeHandler(handler)
        logger.propagate = True


# ─── Чтение ──────────────────────────────────────────────────────────────────
def _write_day(log_dir, day, events):
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"actions-{day}.jsonl"
    with open(path, "a", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")


def test_read_events_filters(log_dir):
    _write_day(log_dir, "2026-07-14", [
        {"ts": "2026-07-14T10:00:00", "kind": "api", "actor": "ivan", "path": "/api/projects", "status": 200},
        {"ts": "2026-07-14T11:00:00", "kind": "api", "actor": "petr", "path": "/api/audit/x/full-audit", "status": 500},
    ])
    _write_day(log_dir, "2026-07-15", [
        {"ts": "2026-07-15T09:00:00", "kind": "pipeline", "project_id": "ЭОМ/К1", "stage": "excel", "status": "done"},
        {"ts": "2026-07-15T09:30:00", "kind": "pipeline", "project_id": "ЭОМ/К1", "stage": "norm_verify", "status": "error", "error": "сбой"},
    ])

    all_events = action_log.read_events(limit=100)
    assert len(all_events["items"]) == 4
    # новые → старые
    assert all_events["items"][0]["ts"] == "2026-07-15T09:30:00"
    assert all_events["items"][-1]["ts"] == "2026-07-14T10:00:00"

    assert len(action_log.read_events(kind="api")["items"]) == 2
    assert len(action_log.read_events(actor="ivan")["items"]) == 1

    errors = action_log.read_events(errors_only=True)["items"]
    assert {e.get("status") for e in errors} == {500, "error"}

    q = action_log.read_events(q="full-audit")["items"]
    assert len(q) == 1 and q[0]["actor"] == "petr"

    day1 = action_log.read_events(date_from="2026-07-15")["items"]
    assert len(day1) == 2
    day0 = action_log.read_events(date_to="2026-07-14")["items"]
    assert len(day0) == 2

    page = action_log.read_events(limit=2)
    assert page["truncated"] is True
    page2 = action_log.read_events(limit=2, offset=2)
    assert [e["ts"] for e in page2["items"]] == ["2026-07-14T11:00:00", "2026-07-14T10:00:00"]


def test_read_events_skips_broken_lines(log_dir):
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / "actions-2026-07-15.jsonl").write_text(
        '{"ts": "1", "kind": "api"}\nНЕ JSON\n{"ts": "2", "kind": "api"}\n',
        encoding="utf-8",
    )
    assert len(action_log.read_events()["items"]) == 2


def test_readers_survive_broken_utf8(log_dir):
    """Оборванный посреди многобайтового символа файл не кладёт чтение."""
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / "actions-2026-07-15.jsonl"
    with open(path, "wb") as f:
        f.write(b'{"ts": "1", "kind": "api", "status": 200}\n')
        f.write('{"ts": "2", "kind": "api", "path": "/про'.encode("utf-8")[:-1])
    assert len(action_log.read_events()["items"]) == 1
    assert action_log.stats(days=366)["totals"]["events"] == 1


def test_stats_days_are_calendar_days(log_dir):
    """stats(days=N) — последние N календарных дней, а не N файлов."""
    from datetime import date, timedelta
    old_day = (date.today() - timedelta(days=30)).isoformat()
    _write_day(log_dir, old_day, [{"ts": "t", "kind": "api", "status": 200}])
    _write_day(log_dir, date.today().isoformat(), [{"ts": "t", "kind": "api", "status": 200}])
    result = action_log.stats(days=3)
    assert [d["day"] for d in result["days"]] == [date.today().isoformat()]
    assert result["totals"]["events"] == 1


def test_stats(log_dir):
    _write_day(log_dir, "2026-07-15", [
        {"ts": "t", "kind": "api", "actor": "ivan", "method": "POST", "path": "/api/x", "status": 200},
        {"ts": "t", "kind": "api", "actor": "ivan", "method": "GET", "path": "/api/x", "status": 404},
        {"ts": "t", "kind": "pipeline", "project_id": "ЭОМ/К1", "stage": "excel", "status": "error"},
    ])
    result = action_log.stats(days=7)
    assert result["totals"]["events"] == 3
    assert result["totals"]["errors"] == 2
    day = result["days"][0]
    assert day["day"] == "2026-07-15"
    assert day["by_kind"] == {"api": 2, "pipeline": 1}
    assert day["actors"] == {"ivan": 2}
    assert day["pipeline_errors"] == {"ЭОМ/К1:excel": 1}
