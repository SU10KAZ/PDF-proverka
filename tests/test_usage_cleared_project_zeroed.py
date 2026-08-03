"""
test_usage_cleared_project_zeroed.py
------------------------------------
Регрессия: после старта свежего аудита карточки этапов показывали расход
ПРОШЛОГО прогона.

Механика бага:
1. manager вызывает usage_tracker.clear_project_usage(pid) — записи удалены;
2. get_all_projects_usage() пропускал проект целиком (`continue` / нет записей),
   и ключ pid исчезал из ответа /api/usage/projects-summary;
3. фронт (fetchAllProjectUsage) обновляет только пришедшие pid — отсутствующий
   проект оставался в projectUsage со старыми цифрами, и на карточке «01 Блоки»
   висели токены и стоимость предыдущего аудита.

Контракт после фикса: проект остаётся в сводке с нулями, пока не появятся
записи текущего прогона.

Run:
    python -m pytest tests/test_usage_cleared_project_zeroed.py -v
"""
from __future__ import annotations

import importlib
import sys
from datetime import datetime, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_MODULE = "backend.app.services.common.usage_service"
_PID = "TEST/cleared-proj"


def _make_record(stage: str, timestamp: str) -> dict:
    return {
        "timestamp": timestamp,
        "session_id": None,
        "project_id": _PID,
        "stage": stage,
        "model": "ensemble/gpt-codex",
        "cost_usd": 3.24,
        "cost_usd_notional": 0.0,
        "duration_ms": 1000,
        "duration_api_ms": 800,
        "num_turns": 1,
        "api_calls": 1,
        "is_retry": False,
        "input_tokens": 5_638_678,
        "output_tokens": 164_171,
        "cache_creation_tokens": 0,
        "cache_read_tokens": 0,
    }


def _make_tracker(monkeypatch, records: list[dict]):
    mod = importlib.import_module(_MODULE)
    tracker = mod.UsageTracker.__new__(mod.UsageTracker)
    tracker._records = list(records)
    tracker._session_reset_at = datetime.now().isoformat()
    tracker._cleared = {}
    monkeypatch.setattr(tracker, "_save", lambda: None)
    monkeypatch.setattr(
        type(tracker), "_get_pipeline_durations", staticmethod(lambda _pid: {})
    )
    return mod, tracker


def test_cleared_project_stays_in_summary_with_zeros(monkeypatch):
    """clear_project_usage → проект в сводке есть, но пустой."""
    old_ts = (datetime.now() - timedelta(days=10)).isoformat()
    mod, tracker = _make_tracker(monkeypatch, [_make_record("block_analysis", old_ts)])
    monkeypatch.setattr(
        type(tracker), "_get_audit_started_at", staticmethod(lambda _pid: None)
    )

    tracker.clear_project_usage(_PID)
    summary = tracker.get_all_projects_usage()

    assert _PID in summary, (
        "проект пропал из projects-summary — фронт оставит на карточках "
        "расход прошлого прогона"
    )
    entry = summary[_PID]
    assert entry["total_input_tokens"] == 0
    assert entry["total_output_tokens"] == 0
    assert entry["total_cost_usd"] == 0.0
    assert entry["stages_summary"] == {}


def test_records_of_previous_run_are_zeroed_not_skipped(monkeypatch):
    """Записи есть, но все — до старта текущего прогона: тоже нули, не пропуск."""
    old_ts = (datetime.now() - timedelta(days=10)).isoformat()
    audit_started = (datetime.now() - timedelta(minutes=5)).isoformat()
    mod, tracker = _make_tracker(monkeypatch, [_make_record("block_analysis", old_ts)])
    monkeypatch.setattr(
        type(tracker), "_get_audit_started_at", staticmethod(lambda _pid: audit_started)
    )

    summary = tracker.get_all_projects_usage()

    assert _PID in summary
    assert summary[_PID]["stages_summary"] == {}
    assert summary[_PID]["total_tokens"] == 0


def test_new_record_clears_the_zero_marker(monkeypatch):
    """Первая запись текущего прогона снимает пометку и возвращает реальные цифры."""
    old_ts = (datetime.now() - timedelta(days=10)).isoformat()
    mod, tracker = _make_tracker(monkeypatch, [_make_record("block_analysis", old_ts)])
    monkeypatch.setattr(
        type(tracker), "_get_audit_started_at", staticmethod(lambda _pid: None)
    )

    tracker.clear_project_usage(_PID)
    fresh = _make_record("block_analysis", datetime.now().isoformat())
    tracker.record_usage(mod.UsageRecord(**fresh))

    summary = tracker.get_all_projects_usage()
    assert summary[_PID]["total_input_tokens"] == 5_638_678
    assert "block_analysis" in summary[_PID]["stages_summary"]
