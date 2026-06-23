"""
test_schedule_plans.py
----------------------
Тесты редактируемого плана работ (раздел «График работ», этап 3).

Покрывает:
* сервис save_plans / get_plans / load_work_plans:
  - GET при отсутствии work_plans.json → пустой список, без ошибки;
  - PUT создаёт файл; PUT обновляет нужный период;
  - PUT не затирает другой период; object_id и period_type — часть ключа;
  - битый JSON: GET → warning + пусто; PUT → бэкап, файл не уничтожается;
  - атомарная запись (нет .tmp после записи);
* REST-роутер /api/schedule/plan (TestClient, auth выключен в conftest):
  - GET по периоду; PUT в dev-режиме; валидация plan (<0 / не int → 422);
* admin-гейт PUT (вызов view-функции напрямую, монипатч portal_auth):
  - auth включена + role expert → 403; role admin → ок; auth выключена → ок.

Run:
    python -m pytest tests/test_schedule_plans.py -v
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import backend.app.services.common.schedule_service as schedule_service  # noqa: E402
import backend.app.api.routers.schedule as schedule_router  # noqa: E402
import backend.app.services.common.user_service as user_service  # noqa: E402


@pytest.fixture
def tmp_plans(tmp_path, monkeypatch):
    """Изолированный work_plans.json на время теста."""
    f = tmp_path / "work_plans.json"
    monkeypatch.setattr(schedule_service, "WORK_PLANS_FILE", f)
    return f


WK = dict(period_type="week", period_start="2026-06-15", period_end="2026-06-21")
WK2 = dict(period_type="week", period_start="2026-06-22", period_end="2026-06-28")


def _items(*pairs):
    return [{"engineer_id": eid, "engineer_name": nm, "plan": pl} for eid, nm, pl in pairs]


# ─── сервис: чтение/запись ───────────────────────────────────────────────────

def test_get_missing_file_empty(tmp_plans):
    out = schedule_service.get_plans(**WK)
    assert out["plans"] == []
    assert out["warning"] is None
    assert out["period"] == {"from": "2026-06-15", "to": "2026-06-21", "period_type": "week"}
    assert not tmp_plans.exists()  # GET ничего не создаёт


def test_put_creates_file(tmp_plans):
    res = schedule_service.save_plans(
        object_id=None, plans=_items(("kuldyaev-f-s", "Кульдяев Ф. С.", 5)),
        updated_by="Узун А. И.", **WK,
    )
    assert tmp_plans.exists()
    assert res["plans"][0]["plan"] == 5
    doc = json.loads(tmp_plans.read_text(encoding="utf-8"))
    assert doc["version"] == 1
    assert doc["updated_at"]
    assert doc["plans"][0]["updated_by"] == "Узун А. И."
    # после записи временный файл не остаётся
    assert not tmp_plans.with_suffix(tmp_plans.suffix + ".tmp").exists()


def test_put_updates_period(tmp_plans):
    schedule_service.save_plans(object_id=None, plans=_items(("kuldyaev-f-s", "К", 5)), **WK)
    schedule_service.save_plans(object_id=None, plans=_items(("kuldyaev-f-s", "К", 8)), **WK)
    out = schedule_service.get_plans(**WK)
    assert len(out["plans"]) == 1
    assert out["plans"][0]["plan"] == 8


def test_put_does_not_overwrite_other_period(tmp_plans):
    schedule_service.save_plans(object_id=None, plans=_items(("a", "A", 5)), **WK)
    schedule_service.save_plans(object_id=None, plans=_items(("a", "A", 9)), **WK2)
    # повторно сохраняем первый период — второй не должен пострадать
    schedule_service.save_plans(object_id=None, plans=_items(("a", "A", 6)), **WK)
    assert schedule_service.get_plans(**WK)["plans"][0]["plan"] == 6
    assert schedule_service.get_plans(**WK2)["plans"][0]["plan"] == 9


def test_object_id_part_of_key(tmp_plans):
    schedule_service.save_plans(object_id="214", plans=_items(("a", "A", 5)), **WK)
    schedule_service.save_plans(object_id=None, plans=_items(("a", "A", 7)), **WK)
    assert schedule_service.get_plans(object_id="214", **WK)["plans"][0]["plan"] == 5
    assert schedule_service.get_plans(object_id=None, **WK)["plans"][0]["plan"] == 7
    # обновление по object_id="214" не трогает None-вариант
    schedule_service.save_plans(object_id="214", plans=_items(("a", "A", 1)), **WK)
    assert schedule_service.get_plans(object_id=None, **WK)["plans"][0]["plan"] == 7


def test_week_and_month_do_not_conflict(tmp_plans):
    # одинаковые даты, разный period_type → разные ключи
    common = dict(period_start="2026-06-15", period_end="2026-06-21", object_id=None)
    schedule_service.save_plans(period_type="week", plans=_items(("a", "A", 5)), **common)
    schedule_service.save_plans(period_type="month", plans=_items(("a", "A", 20)), **common)
    assert schedule_service.get_plans(period_type="week", period_start="2026-06-15", period_end="2026-06-21")["plans"][0]["plan"] == 5
    assert schedule_service.get_plans(period_type="month", period_start="2026-06-15", period_end="2026-06-21")["plans"][0]["plan"] == 20


def test_broken_json_get_warns_empty(tmp_plans):
    tmp_plans.write_text("{ broken json", encoding="utf-8")
    out = schedule_service.get_plans(**WK)
    assert out["plans"] == []
    assert out["warning"] and "повреждён" in out["warning"]


def test_broken_json_put_backs_up_and_survives(tmp_plans):
    tmp_plans.write_text("{ broken json", encoding="utf-8")
    res = schedule_service.save_plans(object_id=None, plans=_items(("a", "A", 5)), **WK)
    assert res["warning"] and "бэкап" in res["warning"]
    # бэкап с исходными байтами создан
    backups = list(tmp_plans.parent.glob("work_plans.json.broken-*"))
    assert backups, "ожидался бэкап повреждённого файла"
    assert "broken json" in backups[0].read_text(encoding="utf-8")
    # новый файл валиден и содержит сохранённый план
    doc = json.loads(tmp_plans.read_text(encoding="utf-8"))
    assert doc["plans"][0]["plan"] == 5


def test_dedup_same_engineer_last_wins(tmp_plans):
    schedule_service.save_plans(
        object_id=None,
        plans=_items(("a", "A", 3), ("a", "A", 7)),  # дубль engineer_id
        **WK,
    )
    out = schedule_service.get_plans(**WK)
    assert len(out["plans"]) == 1
    assert out["plans"][0]["plan"] == 7


def test_merge_preserves_unlisted_engineer_same_period(tmp_plans):
    # PUT по части инженеров не должен удалять план остальных в том же периоде
    schedule_service.save_plans(object_id=None, plans=_items(("a", "A", 5), ("b", "B", 3)), **WK)
    schedule_service.save_plans(object_id=None, plans=_items(("a", "A", 9)), **WK)
    out = {p["engineer_id"]: p["plan"] for p in schedule_service.get_plans(**WK)["plans"]}
    assert out == {"a": 9, "b": 3}


def test_plan_coercion_service_level(tmp_plans):
    # save_plans — публичная функция; нечисловой/большой plan не должен валить её
    schedule_service.save_plans(object_id=None, **WK, plans=[
        {"engineer_id": "a", "engineer_name": "A", "plan": "abc"},   # → 0
        {"engineer_id": "b", "engineer_name": "B", "plan": "5"},     # → 5
        {"engineer_id": "c", "engineer_name": "C", "plan": 1500},    # → clamp 999
    ])
    out = {p["engineer_id"]: p["plan"] for p in schedule_service.get_plans(**WK)["plans"]}
    assert out == {"a": 0, "b": 5, "c": 999}


def test_nonlist_plans_get_warns(tmp_plans):
    # структурно-валидный JSON, но plans не список → не молчим
    tmp_plans.write_text(json.dumps({"version": 1, "plans": {"x": 1}}), encoding="utf-8")
    out = schedule_service.get_plans(**WK)
    assert out["plans"] == []
    assert out["warning"] and "списком" in out["warning"]


def test_nonlist_plans_put_backs_up(tmp_plans):
    tmp_plans.write_text(json.dumps({"version": 1, "plans": "oops"}), encoding="utf-8")
    res = schedule_service.save_plans(object_id=None, plans=_items(("a", "A", 5)), **WK)
    assert res["warning"] and "бэкап" in res["warning"]
    assert list(tmp_plans.parent.glob("work_plans.json.broken-*"))
    assert schedule_service.get_plans(**WK)["plans"][0]["plan"] == 5


def test_concurrent_writes_no_crash_no_lost_update(tmp_plans):
    # Параллельные PUT по РАЗНЫМ периодам не должны падать и не терять записи
    # (общий tmp + lost-update — регресс на исходную реализацию).
    import threading
    errors = []

    def worker(i):
        try:
            schedule_service.save_plans(
                period_type="week",
                period_start=f"2026-07-{i:02d}", period_end=f"2026-07-{i:02d}",
                object_id=None, plans=_items((f"eng{i}", f"E{i}", i)),
            )
        except Exception as e:  # noqa: BLE001
            errors.append(repr(e))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(1, 9)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors
    for i in range(1, 9):
        out = schedule_service.get_plans(
            period_type="week", period_start=f"2026-07-{i:02d}", period_end=f"2026-07-{i:02d}")
        assert len(out["plans"]) == 1 and out["plans"][0]["plan"] == i
    # временные файлы не остаются
    assert not list(tmp_plans.parent.glob("work_plans.json*.tmp"))


# ─── REST-роутер: GET / PUT (dev-режим, auth выключен в conftest) ─────────────

@pytest.fixture
def client(tmp_plans, tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    import backend.app.main as main
    monkeypatch.setattr(user_service, "USERS_FILE", tmp_path / "users.json")
    return TestClient(main.app)


def test_endpoint_put_then_get(client):
    r = client.put("/api/schedule/plan", json={
        **WK, "object_id": None,
        "plans": [{"engineer_id": "kuldyaev-f-s", "engineer_name": "Кульдяев Ф. С.", "plan": 5}],
    })
    assert r.status_code == 200
    assert r.json()["plans"][0]["plan"] == 5

    r = client.get("/api/schedule/plan?from=2026-06-15&to=2026-06-21&period_type=week")
    assert r.status_code == 200
    data = r.json()
    assert data["period"] == {"from": "2026-06-15", "to": "2026-06-21", "period_type": "week"}
    assert data["plans"][0]["engineer_id"] == "kuldyaev-f-s"
    assert data["plans"][0]["plan"] == 5


def test_endpoint_get_missing_empty(client):
    r = client.get("/api/schedule/plan?from=2026-06-15&to=2026-06-21&period_type=week")
    assert r.status_code == 200
    assert r.json()["plans"] == []


def test_endpoint_plan_negative_422(client):
    r = client.put("/api/schedule/plan", json={
        **WK, "object_id": None,
        "plans": [{"engineer_id": "a", "engineer_name": "A", "plan": -1}],
    })
    assert r.status_code == 422


def test_endpoint_plan_not_integer_422(client):
    r = client.put("/api/schedule/plan", json={
        **WK, "object_id": None,
        "plans": [{"engineer_id": "a", "engineer_name": "A", "plan": "abc"}],
    })
    assert r.status_code == 422


def test_endpoint_plan_too_large_422(client):
    r = client.put("/api/schedule/plan", json={
        **WK, "object_id": None,
        "plans": [{"engineer_id": "a", "engineer_name": "A", "plan": 100000}],
    })
    assert r.status_code == 422


def test_endpoint_bad_period_type_400(client):
    r = client.get("/api/schedule/plan?from=2026-06-15&to=2026-06-21&period_type=year")
    assert r.status_code == 400


def test_endpoint_month_default_period(client):
    # без from/to и period_type=month дефолт должен быть месячным, не недельным
    r = client.get("/api/schedule/plan?period_type=month")
    assert r.status_code == 200
    p = r.json()["period"]
    assert p["period_type"] == "month"
    assert p["from"].endswith("-01")  # первый день месяца


# ─── admin-гейт PUT (прямой вызов view-функции) ──────────────────────────────

def _payload():
    return schedule_router.WorkPlanUpdate(
        period_type="week", period_start="2026-06-15", period_end="2026-06-21",
        object_id=None, plans=[{"engineer_id": "a", "engineer_name": "A", "plan": 5}],
    )


def test_put_forbidden_for_non_admin_when_auth_enabled(tmp_plans, monkeypatch):
    monkeypatch.setattr(schedule_router.portal_auth, "get_settings",
                        lambda: SimpleNamespace(enabled=True))
    monkeypatch.setattr(schedule_router.portal_auth, "request_username",
                        lambda req, s: "expertlogin")
    monkeypatch.setattr(user_service, "get_user_by_login",
                        lambda login: {"id": "exp", "login": "expertlogin", "name": "Эксперт", "role": "expert"})
    with pytest.raises(HTTPException) as ei:
        asyncio.run(schedule_router.put_plan(_payload(), SimpleNamespace()))
    assert ei.value.status_code == 403
    assert not tmp_plans.exists()  # ничего не записано


def test_put_allowed_for_admin_when_auth_enabled(tmp_plans, monkeypatch):
    monkeypatch.setattr(schedule_router.portal_auth, "get_settings",
                        lambda: SimpleNamespace(enabled=True))
    monkeypatch.setattr(schedule_router.portal_auth, "request_username",
                        lambda req, s: "adminlogin")
    monkeypatch.setattr(user_service, "get_user_by_login",
                        lambda login: {"id": "adm", "login": "adminlogin", "name": "Админ А. А.", "role": "admin"})
    res = asyncio.run(schedule_router.put_plan(_payload(), SimpleNamespace()))
    assert res["plans"][0]["plan"] == 5
    assert tmp_plans.exists()
    doc = json.loads(tmp_plans.read_text(encoding="utf-8"))
    assert doc["plans"][0]["updated_by"] == "Админ А. А."


def test_put_allowed_in_dev_when_auth_disabled(tmp_plans, monkeypatch):
    monkeypatch.setattr(schedule_router.portal_auth, "get_settings",
                        lambda: SimpleNamespace(enabled=False))
    monkeypatch.setattr(user_service, "get_current_user", lambda: {"name": "Dev"})
    res = asyncio.run(schedule_router.put_plan(_payload(), SimpleNamespace()))
    assert res["plans"][0]["plan"] == 5
    assert tmp_plans.exists()
