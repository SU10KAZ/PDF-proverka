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
