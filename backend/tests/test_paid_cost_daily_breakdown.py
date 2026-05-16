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

import importlib
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
    assert tracker.get()["total_lifetime_usd"] == pytest.approx(100.0)
    daily = tracker.get_daily(days=30)
    assert daily["days"] == []
    tracker.add(0.5, model="m", project_id="p", stage="s")
    assert tracker.get_daily(days=1)["window_total_usd"] == pytest.approx(0.5)
