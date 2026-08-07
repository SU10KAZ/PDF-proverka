"""
Tests for PaidCostTracker daily break-down (по дням, моделям, проектам, этапам).

Контекст:
  paid_cost_tracker раньше хранил только два счётчика (display_usd /
  total_lifetime_usd) — невозможно было ответить на «куда уходят деньги
  с утра». Добавили daily_breakdown с разбивкой по моделям/проектам/этапам
  и эндпоинт GET /api/usage/paid-cost/daily.

Что проверяем:
  1) add() пишет в daily_breakdown[today] с буцкетами;
  2) суммы по моделям/проектам/этапам сходятся с total;
  3) get_daily(days=N) возвращает только окно последних N дней;
  4) reset_display() не трогает daily_breakdown (это исторический срез);
  5) совместимость со старым форматом файла (без daily_breakdown).
"""
from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest


@pytest.fixture
def fresh_tracker(tmp_path, monkeypatch):
    """Изолированный экземпляр PaidCostTracker с временным файлом."""
    _PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    if str(_PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(_PROJECT_ROOT))

    from backend.app.services.common import usage_service

    fake_file = tmp_path / "paid_cost.json"
    monkeypatch.setattr(usage_service, "PAID_COST_FILE", fake_file)

    return usage_service.PaidCostTracker()


def test_add_writes_daily_buckets(fresh_tracker):
    fresh_tracker.add(0.5, model="openai/gpt-5.4", project_id="proj-A", stage="block_analysis")
    fresh_tracker.add(0.25, model="google/gemini-2.5-flash", project_id="proj-A", stage="text_analysis")
    fresh_tracker.add(0.1, model="openai/gpt-5.4", project_id="proj-B", stage="block_analysis")

    daily = fresh_tracker.get_daily(days=1)
    today = datetime.now().date().isoformat()

    assert daily["window_total_usd"] == pytest.approx(0.85)
    assert len(daily["days"]) == 1
    day = daily["days"][0]
    assert day["date"] == today
    assert day["total"] == pytest.approx(0.85)
    assert day["n_calls"] == 3
    assert day["by_model"]["openai/gpt-5.4"] == pytest.approx(0.6)
    assert day["by_model"]["google/gemini-2.5-flash"] == pytest.approx(0.25)
    assert day["by_project"]["proj-A"] == pytest.approx(0.75)
    assert day["by_project"]["proj-B"] == pytest.approx(0.1)
    assert day["by_stage"]["block_analysis"] == pytest.approx(0.6)
    assert day["by_stage"]["text_analysis"] == pytest.approx(0.25)


def test_add_zero_or_negative_is_noop(fresh_tracker):
    fresh_tracker.add(0.0, model="m", project_id="p", stage="s")
    fresh_tracker.add(-1.5, model="m", project_id="p", stage="s")
    daily = fresh_tracker.get_daily(days=1)
    assert daily["window_total_usd"] == 0.0
    assert daily["days"] == []


def test_buckets_consistent_with_total(fresh_tracker):
    fresh_tracker.add(1.0, model="m1", project_id="p1", stage="s1")
    fresh_tracker.add(2.0, model="m2", project_id="p2", stage="s2")
    fresh_tracker.add(3.0, model="m1", project_id="p2", stage="s1")
    daily = fresh_tracker.get_daily(days=1)
    day = daily["days"][0]
    assert sum(day["by_model"].values()) == pytest.approx(day["total"])
    assert sum(day["by_project"].values()) == pytest.approx(day["total"])
    assert sum(day["by_stage"].values()) == pytest.approx(day["total"])


def test_window_filters_old_dates(fresh_tracker):
    fresh_tracker.add(1.0, model="m", project_id="p", stage="s")
    # Подмешиваем старую запись через файл (имитируя инкремент из subprocess'а
    # пару дней назад). Используем JSON-файл, потому что get_daily() перечитывает
    # его при каждом вызове.
    from backend.app.services.common import usage_service
    import json
    state = json.loads(usage_service.PAID_COST_FILE.read_text(encoding="utf-8"))
    yesterday = (datetime.now().date() - timedelta(days=2)).isoformat()
    state["daily_breakdown"][yesterday] = {
        "total": 5.0, "n_calls": 1,
        "by_model": {"m": 5.0}, "by_project": {"p": 5.0}, "by_stage": {"s": 5.0},
    }
    usage_service.PAID_COST_FILE.write_text(
        json.dumps(state), encoding="utf-8",
    )

    daily_1 = fresh_tracker.get_daily(days=1)
    daily_7 = fresh_tracker.get_daily(days=7)

    assert daily_1["window_total_usd"] == pytest.approx(1.0)
    assert daily_7["window_total_usd"] == pytest.approx(6.0)
    assert {d["date"] for d in daily_7["days"]} == {datetime.now().date().isoformat(), yesterday}


def test_reset_display_keeps_daily_breakdown(fresh_tracker):
    fresh_tracker.add(2.0, model="m", project_id="p", stage="s")
    assert fresh_tracker.get()["display_usd"] == pytest.approx(2.0)
    fresh_tracker.reset_display()
    assert fresh_tracker.get()["display_usd"] == 0.0
    daily = fresh_tracker.get_daily(days=1)
    assert daily["window_total_usd"] == pytest.approx(2.0)


def test_get_picks_up_external_writes(tmp_path, monkeypatch):
    """get()/get_daily() должны видеть инкременты от другого писателя (subprocess)."""
    _PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    if str(_PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(_PROJECT_ROOT))
    from backend.app.services.common import usage_service

    fake_file = tmp_path / "paid_cost.json"
    monkeypatch.setattr(usage_service, "PAID_COST_FILE", fake_file)

    tracker_a = usage_service.PaidCostTracker()
    tracker_b = usage_service.PaidCostTracker()

    tracker_a.add(0.5, model="m", project_id="p", stage="s")
    # tracker_b видит чужой инкремент через перечитывание файла:
    daily_b = tracker_b.get_daily(days=1)
    assert daily_b["window_total_usd"] == pytest.approx(0.5)
    assert tracker_b.get()["display_usd"] == pytest.approx(0.5)


def test_legacy_file_without_daily_breakdown(tmp_path, monkeypatch):
    """Старый paid_cost.json без daily_breakdown должен загружаться без ошибок."""
    _PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    if str(_PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(_PROJECT_ROOT))
    from backend.app.services.common import usage_service

    legacy_file = tmp_path / "paid_cost.json"
    legacy_file.write_text(json.dumps({
        "total_lifetime_usd": 100.0,
        "display_usd": 10.0,
        "reset_history": [],
    }), encoding="utf-8")
    monkeypatch.setattr(usage_service, "PAID_COST_FILE", legacy_file)

    tracker = usage_service.PaidCostTracker()
    snapshot = tracker.get()
    assert snapshot["total_lifetime_usd"] == pytest.approx(100.0)
    assert snapshot["display_usd"] == pytest.approx(10.0)
    assert snapshot["month_key"] == datetime.now().strftime("%Y-%m")
    assert snapshot["monthly_tracked_usd"] == 0.0
    assert snapshot["monthly_adjustment_usd"] == 0.0
    assert snapshot["monthly_spent_usd"] == 0.0
    assert snapshot["monthly_calibrated_to_usd"] is None
    assert snapshot["monthly_calibrated_at"] is None
    daily = tracker.get_daily(days=30)
    assert daily["days"] == []
    tracker.add(0.5, model="m", project_id="p", stage="s")
    assert tracker.get_daily(days=1)["window_total_usd"] == pytest.approx(0.5)


def test_month_calibration_is_overlay_and_future_add_increases_it(
    fresh_tracker, monkeypatch,
):
    from backend.app.services.common import usage_service

    monkeypatch.setattr(usage_service, "PAID_API_MONTHLY_LIMIT_USD", 250.0)
    now = datetime(2026, 8, 6, 12, 0, 0)
    fresh_tracker.add(63.1201, model="m", project_id="p", stage="s", now=now)

    calibrated = fresh_tracker.calibrate_month(61.43, now=now)
    assert calibrated["month_key"] == "2026-08"
    assert calibrated["monthly_tracked_usd"] == pytest.approx(63.1201)
    assert calibrated["monthly_adjustment_usd"] == pytest.approx(-1.6901)
    assert calibrated["monthly_spent_usd"] == pytest.approx(61.43)
    assert calibrated["monthly_calibrated_to_usd"] == pytest.approx(61.43)
    assert calibrated["monthly_calibrated_at"] == "2026-08-06T12:00:00"

    # Calibration is a fixed reconciliation overlay, not a frozen override.
    fresh_tracker.add(2.5, model="m", project_id="p", stage="s", now=now)
    after_add = fresh_tracker.get(now=now)
    assert after_add["monthly_tracked_usd"] == pytest.approx(65.6201)
    assert after_add["monthly_adjustment_usd"] == pytest.approx(-1.6901)
    assert after_add["monthly_spent_usd"] == pytest.approx(63.93)

    persisted = json.loads(usage_service.PAID_COST_FILE.read_text(encoding="utf-8"))
    record = persisted["monthly_calibrations"]["2026-08"]
    assert record["tracked_usd_at_calibration"] == pytest.approx(63.1201)
    assert record["calibrated_to_usd"] == pytest.approx(61.43)
    assert record["adjustment_usd"] == pytest.approx(-1.6901)
    assert persisted["monthly_calibration_history"][-1]["month_key"] == "2026-08"


def test_month_rollover_does_not_carry_calibration(fresh_tracker):
    january = datetime(2026, 1, 31, 23, 0, 0)
    february = datetime(2026, 2, 1, 1, 0, 0)

    fresh_tracker.add(5.0, model="m", project_id="p", stage="s", now=january)
    fresh_tracker.calibrate_month(7.0, now=january)
    assert fresh_tracker.get(now=january)["monthly_spent_usd"] == pytest.approx(7.0)

    rolled = fresh_tracker.get(now=february)
    assert rolled["month_key"] == "2026-02"
    assert rolled["monthly_tracked_usd"] == 0.0
    assert rolled["monthly_adjustment_usd"] == 0.0
    assert rolled["monthly_spent_usd"] == 0.0
    assert rolled["monthly_calibrated_to_usd"] is None
    assert rolled["monthly_calibrated_at"] is None

    fresh_tracker.add(1.25, model="m", project_id="p", stage="s", now=february)
    assert fresh_tracker.get(now=february)["monthly_spent_usd"] == pytest.approx(1.25)
    # January's reconciliation remains available when querying that month via clock.
    assert fresh_tracker.get(now=january)["monthly_spent_usd"] == pytest.approx(7.0)


def test_monthly_limit_reports_remaining_percent_and_overage(
    fresh_tracker, monkeypatch,
):
    from backend.app.services.common import usage_service

    monkeypatch.setattr(usage_service, "PAID_API_MONTHLY_LIMIT_USD", 10.0)
    now = datetime(2026, 7, 10, 12, 0, 0)
    fresh_tracker.add(12.5, model="m", project_id="p", stage="s", now=now)

    snapshot = fresh_tracker.get(now=now)
    assert snapshot["monthly_limit_usd"] == pytest.approx(10.0)
    assert snapshot["monthly_remaining_usd"] == 0.0
    assert snapshot["monthly_percent"] == pytest.approx(125.0)
    assert snapshot["monthly_over_limit_usd"] == pytest.approx(2.5)


def test_monthly_limit_override_applies_only_to_selected_month(
    fresh_tracker, monkeypatch,
):
    from backend.app.services.common import usage_service

    monkeypatch.setattr(usage_service, "PAID_API_MONTHLY_LIMIT_USD", 250.0)
    state = {"daily_breakdown": {}}
    state["monthly_limit_overrides_usd"] = {"2026-08": 200.0}
    usage_service.PAID_COST_FILE.write_text(json.dumps(state), encoding="utf-8")

    august = fresh_tracker.get(now=datetime(2026, 8, 31, 23, 59))
    september = fresh_tracker.get(now=datetime(2026, 9, 1, 0, 1))

    assert august["monthly_limit_usd"] == pytest.approx(200.0)
    assert august["monthly_limit_is_override"] is True
    assert september["monthly_limit_usd"] == pytest.approx(250.0)
    assert september["monthly_limit_is_override"] is False


@pytest.mark.parametrize("amount", [-1, float("nan"), float("inf"), "bad", True])
def test_calibrate_month_validates_amount(fresh_tracker, amount):
    with pytest.raises(ValueError, match="finite non-negative"):
        fresh_tracker.calibrate_month(amount)


@pytest.mark.parametrize("month", ["2026-1", "2026-13", "not-a-month", 202601])
def test_calibrate_month_validates_month(fresh_tracker, month):
    with pytest.raises(ValueError, match="YYYY-MM"):
        fresh_tracker.calibrate_month(1.0, month=month)


def test_calibration_endpoint_accepts_amount_and_rejects_bad_body(
    fresh_tracker, monkeypatch,
):
    from backend.app.api.routers import usage as usage_router
    from fastapi import HTTPException

    monkeypatch.setattr(usage_router, "paid_cost_tracker", fresh_tracker)
    response = asyncio.run(usage_router.calibrate_paid_cost_month({"amount_usd": 3.5}))
    assert response["monthly_spent_usd"] == pytest.approx(3.5)
    assert response["monthly_calibrated_to_usd"] == pytest.approx(3.5)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(usage_router.calibrate_paid_cost_month({}))
    assert exc_info.value.status_code == 400
