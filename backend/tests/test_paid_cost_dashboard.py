"""Tests for paid_cost_dashboard.build_paid_cost_daily_dashboard.

Покрытие:
  1. Пустые источники → пустой `days[]` и нули в `totals`.
  2. paid_cost.json daily_breakdown без jsonl → aggregated_only=true.
  3. paid_cost_events.jsonl без daily_breakdown → синтез агрегатов из events.
  4. Оба источника → breakdown как source of truth для агрегатов, events для деталей.
  5. days=N ограничивает окно (старые даты вне окна не возвращаются).
  6. Битые строки jsonl не роняют endpoint.
  7. by_model / by_project / by_stage корректно агрегируются.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest


@pytest.fixture
def dashboard_module():
    """Импорт сервиса (без monkeypatching реальных файлов)."""
    _PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    if str(_PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(_PROJECT_ROOT))
    from backend.app.services.llm import paid_cost_dashboard
    return paid_cost_dashboard


# ─── 1. Empty sources ────────────────────────────────────────────────


def test_empty_sources_returns_empty_days(dashboard_module, tmp_path):
    """Когда нет paid_cost.json и нет jsonl — endpoint не падает, days=[]."""
    res = dashboard_module.build_paid_cost_daily_dashboard(
        days=7,
        paid_cost_file=tmp_path / "missing.json",
        events_file=tmp_path / "missing.jsonl",
    )
    assert res["days"] == []
    assert res["window_days"] == 7
    assert res["totals"] == {"period_total_usd": 0.0, "period_calls": 0}


def test_paid_cost_present_no_events_aggregated_only(dashboard_module, tmp_path):
    """paid_cost.json есть, jsonl нет → aggregated_only=true, events=[]."""
    today = datetime(2026, 5, 16, 14, 0, 0)
    pc_file = tmp_path / "paid_cost.json"
    pc_file.write_text(json.dumps({
        "total_lifetime_usd": 100.0,
        "display_usd": 5.0,
        "daily_breakdown": {
            "2026-05-16": {
                "total": 4.1951, "n_calls": 13,
                "by_model": {"openai/gpt-5.4": 4.1951},
                "by_project": {"M31A": 4.1951},
                "by_stage": {"block_analysis": 4.1951},
            }
        }
    }), encoding="utf-8")

    res = dashboard_module.build_paid_cost_daily_dashboard(
        days=7, paid_cost_file=pc_file,
        events_file=tmp_path / "missing.jsonl", today=today,
    )
    assert len(res["days"]) == 1
    day = res["days"][0]
    assert day["date"] == "2026-05-16"
    assert day["total_usd"] == pytest.approx(4.1951)
    assert day["n_calls"] == 13
    assert day["aggregated_only"] is True
    assert day["events"] == []
    assert day["by_model"] == {"openai/gpt-5.4": 4.1951}
    assert res["totals"] == {"period_total_usd": 4.1951, "period_calls": 13}


# ─── 2. Events-only ───────────────────────────────────────────────────


def test_events_only_synthesizes_aggregates(dashboard_module, tmp_path):
    """Только jsonl, без daily_breakdown → агрегаты собираются из events."""
    today = datetime(2026, 5, 16, 12, 0, 0)
    jsonl = tmp_path / "events.jsonl"
    events = [
        {"ts": "2026-05-16T10:00:00", "cost_usd": 0.5,
         "model": "openai/gpt-5.4", "project_id": "P1", "stage": "block_analysis"},
        {"ts": "2026-05-16T11:00:00", "cost_usd": 0.25,
         "model": "google/gemini-2.5", "project_id": "P1", "stage": "text_analysis"},
        {"ts": "2026-05-16T11:30:00", "cost_usd": 0.1,
         "model": "openai/gpt-5.4", "project_id": "P2", "stage": "block_analysis"},
    ]
    jsonl.write_text("\n".join(json.dumps(e) for e in events), encoding="utf-8")

    res = dashboard_module.build_paid_cost_daily_dashboard(
        days=7, paid_cost_file=tmp_path / "missing.json",
        events_file=jsonl, today=today,
    )
    assert len(res["days"]) == 1
    day = res["days"][0]
    assert day["date"] == "2026-05-16"
    assert day["n_calls"] == 3
    assert day["total_usd"] == pytest.approx(0.85)
    assert day["by_model"]["openai/gpt-5.4"] == pytest.approx(0.6)
    assert day["by_model"]["google/gemini-2.5"] == pytest.approx(0.25)
    assert day["by_project"]["P1"] == pytest.approx(0.75)
    assert day["by_project"]["P2"] == pytest.approx(0.1)
    assert day["by_stage"]["block_analysis"] == pytest.approx(0.6)
    assert day["by_stage"]["text_analysis"] == pytest.approx(0.25)
    assert day["aggregated_only"] is False
    assert len(day["events"]) == 3
    # Сортировка events: самое свежее первым.
    assert day["events"][0]["time"] == "11:30:00"


# ─── 3. Both sources ─────────────────────────────────────────────────


def test_both_sources_breakdown_wins_for_totals_events_for_details(dashboard_module, tmp_path):
    """Если есть и breakdown, и events за тот же день — breakdown даёт total,
    events дают детализацию (UI может показать события)."""
    today = datetime(2026, 5, 16, 18, 0, 0)
    pc_file = tmp_path / "paid_cost.json"
    pc_file.write_text(json.dumps({
        "daily_breakdown": {
            "2026-05-16": {
                "total": 4.1951, "n_calls": 13,
                "by_model": {"openai/gpt-5.4": 4.1951},
                "by_project": {"M31A": 4.1951},
                "by_stage": {"block_analysis": 4.1951},
            }
        }
    }), encoding="utf-8")
    jsonl = tmp_path / "events.jsonl"
    jsonl.write_text(json.dumps({
        "ts": "2026-05-16T14:30:16.620526",
        "cost_usd": 0.3227,
        "model": "openai/gpt-5.4",
        "project_id": "M31A",
        "stage": "block_analysis",
        "job_id": "j1",
        "input_tokens": 40517,
        "output_tokens": 17118,
    }) + "\n", encoding="utf-8")

    res = dashboard_module.build_paid_cost_daily_dashboard(
        days=7, paid_cost_file=pc_file, events_file=jsonl, today=today,
    )
    day = res["days"][0]
    # Breakdown — source of truth для total (там 4.1951, у events — только 0.3227)
    assert day["total_usd"] == pytest.approx(4.1951)
    assert day["n_calls"] == 13
    assert day["aggregated_only"] is False  # events есть
    assert len(day["events"]) == 1
    assert day["events"][0]["time"] == "14:30:16"
    assert day["events"][0]["job_id"] == "j1"
    assert day["events"][0]["input_tokens"] == 40517


# ─── 4. Window filter ────────────────────────────────────────────────


def test_days_window_filters_old_dates(dashboard_module, tmp_path):
    """days=7 не включает даты старше 7 дней назад."""
    today = datetime(2026, 5, 16, 12, 0, 0)
    pc_file = tmp_path / "paid_cost.json"
    pc_file.write_text(json.dumps({
        "daily_breakdown": {
            "2026-05-16": {"total": 1.0, "n_calls": 1,
                           "by_model": {"m": 1.0}, "by_project": {"p": 1.0}, "by_stage": {"s": 1.0}},
            "2026-05-15": {"total": 2.0, "n_calls": 2,
                           "by_model": {"m": 2.0}, "by_project": {"p": 2.0}, "by_stage": {"s": 2.0}},
            "2026-05-01": {"total": 99.0, "n_calls": 99,
                           "by_model": {"m": 99.0}, "by_project": {"p": 99.0}, "by_stage": {"s": 99.0}},
        }
    }), encoding="utf-8")

    res7 = dashboard_module.build_paid_cost_daily_dashboard(
        days=7, paid_cost_file=pc_file,
        events_file=tmp_path / "missing.jsonl", today=today,
    )
    dates7 = [d["date"] for d in res7["days"]]
    assert dates7 == ["2026-05-16", "2026-05-15"]

    res30 = dashboard_module.build_paid_cost_daily_dashboard(
        days=30, paid_cost_file=pc_file,
        events_file=tmp_path / "missing.jsonl", today=today,
    )
    dates30 = [d["date"] for d in res30["days"]]
    assert "2026-05-01" in dates30
    assert res30["totals"]["period_total_usd"] == pytest.approx(102.0)
    assert res30["totals"]["period_calls"] == 102


def test_future_dates_filtered_out(dashboard_module, tmp_path):
    """Будущие даты (если вдруг попали из-за clock skew) не возвращаются."""
    today = datetime(2026, 5, 16, 12, 0, 0)
    pc_file = tmp_path / "paid_cost.json"
    pc_file.write_text(json.dumps({
        "daily_breakdown": {
            "2026-05-16": {"total": 1.0, "n_calls": 1,
                           "by_model": {}, "by_project": {}, "by_stage": {}},
            "2026-05-20": {"total": 9.99, "n_calls": 99,
                           "by_model": {}, "by_project": {}, "by_stage": {}},
        }
    }), encoding="utf-8")
    res = dashboard_module.build_paid_cost_daily_dashboard(
        days=30, paid_cost_file=pc_file,
        events_file=tmp_path / "missing.jsonl", today=today,
    )
    dates = [d["date"] for d in res["days"]]
    assert "2026-05-20" not in dates


# ─── 5. Broken jsonl ─────────────────────────────────────────────────


def test_broken_jsonl_lines_are_skipped(dashboard_module, tmp_path):
    """Битые строки в jsonl не должны ронять endpoint."""
    today = datetime(2026, 5, 16, 12, 0, 0)
    jsonl = tmp_path / "events.jsonl"
    jsonl.write_text(
        '{"ts": "2026-05-16T10:00:00", "cost_usd": 0.5, "model": "m", '
        '"project_id": "p", "stage": "s"}\n'
        'NOT JSON AT ALL\n'
        '{"ts": "incomplete":\n'  # неполная строка
        '\n'
        '{"ts": "2026-05-16T11:00:00", "cost_usd": 0.25, "model": "m", '
        '"project_id": "p", "stage": "s"}\n',
        encoding="utf-8",
    )
    res = dashboard_module.build_paid_cost_daily_dashboard(
        days=7, paid_cost_file=tmp_path / "missing.json",
        events_file=jsonl, today=today,
    )
    day = res["days"][0]
    assert day["n_calls"] == 2  # 2 валидных события
    assert day["total_usd"] == pytest.approx(0.75)


# ─── 6. Events grouping by date ──────────────────────────────────────


def test_events_grouped_across_multiple_days(dashboard_module, tmp_path):
    """Events с разных дат корректно группируются."""
    today = datetime(2026, 5, 16, 12, 0, 0)
    jsonl = tmp_path / "events.jsonl"
    jsonl.write_text("\n".join([
        json.dumps({"ts": "2026-05-16T10:00:00", "cost_usd": 1.0,
                    "model": "m1", "project_id": "p1", "stage": "s1"}),
        json.dumps({"ts": "2026-05-15T22:00:00", "cost_usd": 0.5,
                    "model": "m2", "project_id": "p2", "stage": "s2"}),
        json.dumps({"ts": "2026-05-15T11:00:00", "cost_usd": 0.5,
                    "model": "m2", "project_id": "p2", "stage": "s2"}),
    ]), encoding="utf-8")

    res = dashboard_module.build_paid_cost_daily_dashboard(
        days=7, paid_cost_file=tmp_path / "missing.json",
        events_file=jsonl, today=today,
    )
    # Сортировка: новейший день первым.
    dates = [d["date"] for d in res["days"]]
    assert dates == ["2026-05-16", "2026-05-15"]
    assert res["days"][0]["n_calls"] == 1
    assert res["days"][1]["n_calls"] == 2
    assert res["days"][1]["total_usd"] == pytest.approx(1.0)


# ─── 7. Events truncation ────────────────────────────────────────────


def test_events_truncation_respects_max_per_day(dashboard_module, tmp_path):
    """Если событий > max_events_per_day, флаг truncated=true."""
    today = datetime(2026, 5, 16, 12, 0, 0)
    jsonl = tmp_path / "events.jsonl"
    events = [
        {"ts": f"2026-05-16T10:{i:02d}:00", "cost_usd": 0.01,
         "model": "m", "project_id": "p", "stage": "s"}
        for i in range(15)
    ]
    jsonl.write_text("\n".join(json.dumps(e) for e in events), encoding="utf-8")

    res = dashboard_module.build_paid_cost_daily_dashboard(
        days=1, paid_cost_file=tmp_path / "missing.json",
        events_file=jsonl, today=today, max_events_per_day=10,
    )
    day = res["days"][0]
    assert day["n_calls"] == 15  # n_calls — всё ещё реальное число
    assert len(day["events"]) == 10  # но events урезаны
    assert day["events_truncated"] is True
