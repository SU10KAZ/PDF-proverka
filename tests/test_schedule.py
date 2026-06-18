"""
test_schedule.py
----------------
Тесты графика производства работ (раздел «График работ»).

Покрывает:
* агрегатор aggregate_events: запись → событие; дедуп по (инженер, день, проект);
  несколько проектов в один день; фильтр по датам; фильтр по object_id;
  пропуск битой/пустой даты и пустого reviewer;
* короткое имя проекта short_name (номер объекта / длинная строка);
* стабильный engId (eng_slug);
* build_engineers: роль из users по имени, сортировка, без пустых инженеров;
* безопасное чтение лога: нет файла → пусто; битый JSON → пусто + warning;
* REST-роутер GET /api/schedule (TestClient, auth выключен в conftest):
  дефолт периода, фильтр дат, отсутствие лога не валит endpoint.

Run:
    python -m pytest tests/test_schedule.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import backend.app.services.common.schedule_service as schedule_service  # noqa: E402


def _entry(reviewer, date, project, section="AR", object_id="214", **extra):
    e = {
        "expert_reviewer": reviewer,
        "expert_date": date,
        "source_project": project,
        "section": section,
        "object_id": object_id,
        "item_type": "finding",
        "expert_decision": "accepted",
    }
    e.update(extra)
    return e


# ─── eng_slug ────────────────────────────────────────────────────────────────

def test_eng_slug_stable_and_hyphenated():
    assert schedule_service.eng_slug("Узун А. И.") == "uzun-a-i"
    # детерминированно: повтор даёт тот же id
    assert schedule_service.eng_slug("Узун А. И.") == schedule_service.eng_slug("Узун А.И.")
    assert schedule_service.eng_slug("Репников И. А.") == "repnikov-i-a"
    assert schedule_service.eng_slug("") == "unknown"


# ─── short_name ──────────────────────────────────────────────────────────────

def test_short_name_object_number_prefix():
    assert schedule_service.short_name("214. Alia (ASTERUS)") == "214. Alia"


def test_short_name_passthrough_and_truncation():
    # короткое имя без номера — как есть
    assert schedule_service.short_name("13АВ-РД-АР1.1-К5-К6") == "13АВ-РД-АР1.1-К5-К6"
    # длинное — обрезается с ellipsis
    long = "Очень длинное наименование проектной документации корпуса"
    out = schedule_service.short_name(long)
    assert out.endswith("…")
    assert len(out) <= 32
    assert schedule_service.short_name("") == ""


# ─── aggregate_events ────────────────────────────────────────────────────────

def test_one_record_one_event():
    entries = [_entry("Узун А. И.", "2026-06-18T06:00:00.000Z", "214. Alia (ASTERUS)")]
    events = schedule_service.aggregate_events(entries, from_day="2026-06-15", to_day="2026-06-21")
    assert len(events) == 1
    ev = events[0]
    assert ev["engId"] == "uzun-a-i"
    assert ev["engineerName"] == "Узун А. И."
    assert ev["date"] == ev["key"] == "2026-06-18"
    assert ev["short"] == "214. Alia"
    assert ev["full"] == ev["source_project"] == "214. Alia (ASTERUS)"
    assert ev["section"] == "AR"
    assert ev["object_id"] == "214"


def test_dedup_same_project_same_day_same_engineer():
    # три решения по одному проекту в один день → одно событие
    entries = [
        _entry("Узун А. И.", "2026-06-18T06:00:00Z", "214. Alia", item_id="F-1"),
        _entry("Узун А. И.", "2026-06-18T07:00:00Z", "214. Alia", item_id="F-2"),
        _entry("Узун А. И.", "2026-06-18T08:00:00Z", "214. Alia", item_id="F-3"),
    ]
    events = schedule_service.aggregate_events(entries, from_day="2026-06-15", to_day="2026-06-21")
    assert len(events) == 1


def test_multiple_projects_same_day_multiple_events():
    entries = [
        _entry("Узун А. И.", "2026-06-18T06:00:00Z", "214. Alia"),
        _entry("Узун А. И.", "2026-06-18T06:00:00Z", "213. Metromash"),
        _entry("Узун А. И.", "2026-06-18T06:00:00Z", "ДС3-АР"),
    ]
    events = schedule_service.aggregate_events(entries, from_day="2026-06-15", to_day="2026-06-21")
    assert len(events) == 3
    assert {e["date"] for e in events} == {"2026-06-18"}
    assert {e["source_project"] for e in events} == {"214. Alia", "213. Metromash", "ДС3-АР"}


def test_date_range_filter_inclusive():
    entries = [
        _entry("Узун А. И.", "2026-06-14T10:00:00Z", "P-before"),   # вне
        _entry("Узун А. И.", "2026-06-15T10:00:00Z", "P-from"),     # граница from
        _entry("Узун А. И.", "2026-06-18T10:00:00Z", "P-mid"),
        _entry("Узун А. И.", "2026-06-21T10:00:00Z", "P-to"),       # граница to
        _entry("Узун А. И.", "2026-06-22T10:00:00Z", "P-after"),    # вне
    ]
    events = schedule_service.aggregate_events(entries, from_day="2026-06-15", to_day="2026-06-21")
    got = {e["source_project"] for e in events}
    assert got == {"P-from", "P-mid", "P-to"}


def test_object_id_filter():
    entries = [
        _entry("Узун А. И.", "2026-06-18T10:00:00Z", "P-214", object_id="214"),
        _entry("Узун А. И.", "2026-06-18T10:00:00Z", "P-213", object_id="213"),
    ]
    events = schedule_service.aggregate_events(
        entries, from_day="2026-06-15", to_day="2026-06-21", object_id="214"
    )
    assert [e["source_project"] for e in events] == ["P-214"]


def test_bad_or_empty_date_skipped():
    entries = [
        _entry("Узун А. И.", "", "P-empty"),
        _entry("Узун А. И.", None, "P-none"),
        _entry("Узун А. И.", "не дата", "P-garbage"),
        _entry("Узун А. И.", "2026-13-40T10:00:00Z", "P-invalid"),
        _entry("Узун А. И.", "2026-06-18T10:00:00Z", "P-ok"),
    ]
    events = schedule_service.aggregate_events(entries, from_day="2026-06-01", to_day="2026-06-30")
    assert [e["source_project"] for e in events] == ["P-ok"]


def test_empty_reviewer_skipped():
    entries = [
        _entry("", "2026-06-18T10:00:00Z", "P-anon"),
        _entry("   ", "2026-06-18T10:00:00Z", "P-blank"),
        _entry("Узун А. И.", "2026-06-18T10:00:00Z", "P-named"),
    ]
    events = schedule_service.aggregate_events(entries, from_day="2026-06-01", to_day="2026-06-30")
    assert [e["source_project"] for e in events] == ["P-named"]


def test_system_reviewers_skipped():
    # служебные/импортные аккаунты не считаются инженерами
    entries = [
        _entry("su10_registry", "2026-06-18T10:00:00Z", "P-import"),
        _entry("SU10_REGISTRY", "2026-06-18T10:00:00Z", "P-import2"),  # регистронезависимо
        _entry("Репников И. А.", "2026-06-18T10:00:00Z", "P-real"),
    ]
    events = schedule_service.aggregate_events(entries, from_day="2026-06-01", to_day="2026-06-30")
    assert [e["source_project"] for e in events] == ["P-real"]


def test_aggregate_handles_non_dict_entries():
    entries = ["broken", 42, None, _entry("Узун А. И.", "2026-06-18T10:00:00Z", "P-ok")]
    events = schedule_service.aggregate_events(entries, from_day="2026-06-01", to_day="2026-06-30")
    assert len(events) == 1


# ─── build_engineers ─────────────────────────────────────────────────────────

def test_build_engineers_role_from_users_and_sorted():
    events = [
        {"engId": "repnikov-i-a", "engineerName": "Репников И. А."},
        {"engId": "uzun-a-i", "engineerName": "Узун А. И."},
        {"engId": "uzun-a-i", "engineerName": "Узун А. И."},  # дубль engId схлопывается
    ]
    users = [{"name": "Узун А. И.", "role": "admin"}]
    engs = schedule_service.build_engineers(events, users=users)
    assert [e["id"] for e in engs] == ["repnikov-i-a", "uzun-a-i"]  # сортировка по имени
    by_id = {e["id"]: e for e in engs}
    assert by_id["uzun-a-i"]["role"] == "admin"      # роль подтянута из users
    assert by_id["repnikov-i-a"]["role"] == "expert"  # дефолт


def test_build_engineers_no_empty_rows():
    # инженеры строятся только из событий — пустых строк нет
    assert schedule_service.build_engineers([], users=[{"name": "Узун А. И."}]) == []


# ─── build_schedule (pure) ───────────────────────────────────────────────────

def test_build_schedule_shape():
    entries = [_entry("Узун А. И.", "2026-06-18T06:00:00Z", "214. Alia (ASTERUS)")]
    payload = schedule_service.build_schedule(
        entries, from_day="2026-06-15", to_day="2026-06-21", users=[]
    )
    assert set(payload.keys()) == {"events", "engineers", "period"}
    assert payload["period"] == {"from": "2026-06-15", "to": "2026-06-21"}
    assert len(payload["events"]) == 1
    assert len(payload["engineers"]) == 1


# ─── load_decisions_log (IO, safe) ───────────────────────────────────────────

def test_load_missing_file_returns_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(schedule_service, "DECISIONS_LOG_FILE", tmp_path / "nope.json")
    entries, warning = schedule_service.load_decisions_log()
    assert entries == []
    assert warning is None


def test_load_broken_json_returns_warning(tmp_path, monkeypatch):
    f = tmp_path / "decisions_log.json"
    f.write_text("{ this is not json", encoding="utf-8")
    monkeypatch.setattr(schedule_service, "DECISIONS_LOG_FILE", f)
    entries, warning = schedule_service.load_decisions_log()
    assert entries == []
    assert warning and "повреждён" in warning


def test_load_supports_dict_and_list_shapes(tmp_path, monkeypatch):
    f = tmp_path / "decisions_log.json"
    # dict с entries
    f.write_text(json.dumps({"entries": [_entry("Узун А. И.", "2026-06-18", "P")]}), encoding="utf-8")
    monkeypatch.setattr(schedule_service, "DECISIONS_LOG_FILE", f)
    entries, _ = schedule_service.load_decisions_log()
    assert len(entries) == 1
    # plain list
    f.write_text(json.dumps([_entry("Узун А. И.", "2026-06-18", "P")]), encoding="utf-8")
    entries, _ = schedule_service.load_decisions_log()
    assert len(entries) == 1


# ─── REST-роутер ─────────────────────────────────────────────────────────────

@pytest.fixture
def client_with_log(tmp_path, monkeypatch):
    """TestClient + изолированные decisions_log.json и users.json."""
    from fastapi.testclient import TestClient
    import backend.app.main as main
    import backend.app.services.common.user_service as user_service

    log_file = tmp_path / "decisions_log.json"
    monkeypatch.setattr(schedule_service, "DECISIONS_LOG_FILE", log_file)
    monkeypatch.setattr(user_service, "USERS_FILE", tmp_path / "users.json")
    return TestClient(main.app), log_file


def test_endpoint_returns_events(client_with_log):
    client, log_file = client_with_log
    log_file.write_text(json.dumps([
        _entry("Узун А. И.", "2026-06-18T06:00:00Z", "214. Alia (ASTERUS)"),
        _entry("Узун А. И.", "2026-06-18T06:00:00Z", "213. Metromash"),
        _entry("Репников И. А.", "2026-06-16T09:00:00Z", "13АВ-РД-АР1.1"),
        _entry("", "2026-06-18T06:00:00Z", "P-anon"),  # пропускается
    ], ensure_ascii=False), encoding="utf-8")

    r = client.get("/api/schedule?from=2026-06-15&to=2026-06-21")
    assert r.status_code == 200
    data = r.json()
    assert data["period"] == {"from": "2026-06-15", "to": "2026-06-21"}
    assert len(data["events"]) == 3
    assert {e["id"] for e in data["engineers"]} == {"uzun-a-i", "repnikov-i-a"}
    assert data["warning"] is None


def test_endpoint_date_filter(client_with_log):
    client, log_file = client_with_log
    log_file.write_text(json.dumps([
        _entry("Узун А. И.", "2026-06-10T06:00:00Z", "P-out"),
        _entry("Узун А. И.", "2026-06-18T06:00:00Z", "P-in"),
    ], ensure_ascii=False), encoding="utf-8")
    r = client.get("/api/schedule?from=2026-06-15&to=2026-06-21")
    assert r.status_code == 200
    assert [e["source_project"] for e in r.json()["events"]] == ["P-in"]


def test_endpoint_missing_log_does_not_fail(client_with_log):
    client, log_file = client_with_log  # файл не создаём
    r = client.get("/api/schedule?from=2026-06-15&to=2026-06-21")
    assert r.status_code == 200
    data = r.json()
    assert data["events"] == []
    assert data["engineers"] == []


def test_endpoint_default_period_when_no_params(client_with_log):
    client, _ = client_with_log
    r = client.get("/api/schedule")
    assert r.status_code == 200
    period = r.json()["period"]
    # дефолт — текущая неделя (Пн ≤ Вс), формат YYYY-MM-DD
    assert period["from"] <= period["to"]
    assert len(period["from"]) == 10 and len(period["to"]) == 10


def test_endpoint_bad_date_param_400(client_with_log):
    client, _ = client_with_log
    r = client.get("/api/schedule?from=18-06-2026&to=2026-06-21")
    assert r.status_code == 400


# ──────────────────────────────────────────────────────────────────────────────
# План работ (work_plans.json): service + REST + admin-gate
# ──────────────────────────────────────────────────────────────────────────────

WEEK = dict(period_type="week", period_start="2026-06-15", period_end="2026-06-21")
WEEK2 = dict(period_type="week", period_start="2026-06-22", period_end="2026-06-28")
MONTH = dict(period_type="month", period_start="2026-06-01", period_end="2026-06-30")


@pytest.fixture
def plans_file(tmp_path, monkeypatch):
    f = tmp_path / "work_plans.json"
    monkeypatch.setattr(schedule_service, "WORK_PLANS_FILE", f)
    return f


def test_get_plans_missing_file_returns_empty(plans_file):
    out = schedule_service.get_plans(**WEEK)
    assert out["plans"] == []
    assert out["warning"] is None
    assert out["period"] == {"from": "2026-06-15", "to": "2026-06-21", "period_type": "week"}


def test_save_plans_creates_file(plans_file):
    schedule_service.save_plans(
        **WEEK, object_id=None,
        plans=[{"engineer_id": "kuldyaev-f-s", "engineer_name": "Кульдяев Ф. С.", "plan": 5}],
        updated_by="Узун А. И.")
    assert plans_file.exists()
    doc = json.loads(plans_file.read_text(encoding="utf-8"))
    assert doc["version"] == 1 and doc["updated_at"]
    assert any(p["engineer_id"] == "kuldyaev-f-s" and p["plan"] == 5 for p in doc["plans"])


def test_save_plans_updates_period_and_get_roundtrip(plans_file):
    schedule_service.save_plans(
        **WEEK, object_id=None,
        plans=[{"engineer_id": "rep", "engineer_name": "Репников", "plan": 4}], updated_by="a")
    out = schedule_service.get_plans(**WEEK)
    assert len(out["plans"]) == 1
    assert out["plans"][0]["engineer_id"] == "rep" and out["plans"][0]["plan"] == 4
    # повторный save того же периода — пересборка периода (новое значение)
    schedule_service.save_plans(
        **WEEK, object_id=None,
        plans=[{"engineer_id": "rep", "engineer_name": "Репников", "plan": 7}], updated_by="a")
    out2 = schedule_service.get_plans(**WEEK)
    assert len(out2["plans"]) == 1 and out2["plans"][0]["plan"] == 7


def test_save_does_not_clobber_other_period(plans_file):
    schedule_service.save_plans(**WEEK, object_id=None,
        plans=[{"engineer_id": "a", "plan": 5}], updated_by="x")
    schedule_service.save_plans(**WEEK2, object_id=None,
        plans=[{"engineer_id": "b", "plan": 6}], updated_by="x")
    w1 = schedule_service.get_plans(**WEEK)
    w2 = schedule_service.get_plans(**WEEK2)
    assert {p["engineer_id"] for p in w1["plans"]} == {"a"}
    assert {p["engineer_id"] for p in w2["plans"]} == {"b"}


def test_object_id_is_part_of_key(plans_file):
    schedule_service.save_plans(**WEEK, object_id=None,
        plans=[{"engineer_id": "a", "plan": 5}], updated_by="x")
    schedule_service.save_plans(**WEEK, object_id="214",
        plans=[{"engineer_id": "a", "plan": 9}], updated_by="x")
    glob = schedule_service.get_plans(**WEEK, object_id=None)
    obj = schedule_service.get_plans(**WEEK, object_id="214")
    assert glob["plans"][0]["plan"] == 5 and glob["plans"][0]["object_id"] is None
    assert obj["plans"][0]["plan"] == 9 and obj["plans"][0]["object_id"] == "214"


def test_week_and_month_do_not_conflict(plans_file):
    schedule_service.save_plans(**WEEK, object_id=None,
        plans=[{"engineer_id": "a", "plan": 5}], updated_by="x")
    schedule_service.save_plans(**MONTH, object_id=None,
        plans=[{"engineer_id": "a", "plan": 20}], updated_by="x")
    assert schedule_service.get_plans(**WEEK)["plans"][0]["plan"] == 5
    assert schedule_service.get_plans(**MONTH)["plans"][0]["plan"] == 20


def test_broken_json_backed_up_not_destroyed(plans_file):
    plans_file.write_text("{ this is broken", encoding="utf-8")
    out = schedule_service.get_plans(**WEEK)
    assert out["plans"] == [] and out["warning"]  # GET → пусто + warning, не падает
    res = schedule_service.save_plans(**WEEK, object_id=None,
        plans=[{"engineer_id": "a", "plan": 5}], updated_by="x")
    assert res["warning"] and "бэкап" in res["warning"].lower()
    # повреждённые байты сохранены в backup
    backups = list(plans_file.parent.glob("work_plans.json.broken-*"))
    assert backups and "broken" in backups[0].read_text(encoding="utf-8")
    # новый файл валиден и содержит период
    assert schedule_service.get_plans(**WEEK)["plans"][0]["plan"] == 5


# ─── REST + admin-gate ───

@pytest.fixture
def plan_client(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    import backend.app.main as main
    import backend.app.services.common.user_service as user_service
    monkeypatch.setattr(schedule_service, "WORK_PLANS_FILE", tmp_path / "work_plans.json")
    monkeypatch.setattr(user_service, "USERS_FILE", tmp_path / "users.json")
    return TestClient(main.app)


def test_endpoint_get_empty_when_no_file(plan_client):
    r = plan_client.get("/api/schedule/plan?from=2026-06-15&to=2026-06-21&period_type=week")
    assert r.status_code == 200
    assert r.json()["plans"] == []


def test_endpoint_put_then_get(plan_client):
    body = {**WEEK, "object_id": None,
            "plans": [{"engineer_id": "kuldyaev-f-s", "engineer_name": "Кульдяев Ф. С.", "plan": 5}]}
    rp = plan_client.put("/api/schedule/plan", json=body)
    assert rp.status_code == 200, rp.text  # dev-mode (auth off в conftest) → admin allowed
    rg = plan_client.get("/api/schedule/plan?from=2026-06-15&to=2026-06-21&period_type=week")
    assert rg.status_code == 200
    plans = rg.json()["plans"]
    assert len(plans) == 1 and plans[0]["plan"] == 5


def test_endpoint_plan_negative_rejected_422(plan_client):
    body = {**WEEK, "object_id": None, "plans": [{"engineer_id": "a", "plan": -1}]}
    assert plan_client.put("/api/schedule/plan", json=body).status_code == 422


def test_endpoint_plan_too_large_rejected_422(plan_client):
    body = {**WEEK, "object_id": None, "plans": [{"engineer_id": "a", "plan": 100000}]}
    assert plan_client.put("/api/schedule/plan", json=body).status_code == 422


def test_endpoint_plan_non_integer_rejected_422(plan_client):
    body = {**WEEK, "object_id": None, "plans": [{"engineer_id": "a", "plan": "five"}]}
    assert plan_client.put("/api/schedule/plan", json=body).status_code == 422


def test_admin_gate_blocks_non_admin_when_auth_enabled(monkeypatch):
    """_require_admin: auth включена + не-admin → 403; admin → имя."""
    import types
    import backend.app.api.routers.schedule as sched_router
    from backend.app.core import portal_auth
    import backend.app.services.common.user_service as user_service
    from fastapi import HTTPException

    monkeypatch.setattr(portal_auth, "get_settings",
                        lambda: types.SimpleNamespace(enabled=True, users={}))
    monkeypatch.setattr(portal_auth, "request_username", lambda req, st: "ivan")

    # не-admin → 403
    monkeypatch.setattr(user_service, "get_user_by_login", lambda u: {"role": "expert", "name": "Иван"})
    with pytest.raises(HTTPException) as ei:
        sched_router._require_admin(object())
    assert ei.value.status_code == 403

    # admin → возвращает имя
    monkeypatch.setattr(user_service, "get_user_by_login", lambda u: {"role": "admin", "name": "Админ"})
    assert sched_router._require_admin(object()) == "Админ"


def test_admin_gate_allows_in_dev_when_auth_disabled(monkeypatch):
    import types
    import backend.app.api.routers.schedule as sched_router
    from backend.app.core import portal_auth
    import backend.app.services.common.user_service as user_service
    monkeypatch.setattr(portal_auth, "get_settings",
                        lambda: types.SimpleNamespace(enabled=False, users={}))
    monkeypatch.setattr(user_service, "get_current_user", lambda: {"name": "DevUser"})
    assert sched_router._require_admin(object()) == "DevUser"  # dev → разрешено
