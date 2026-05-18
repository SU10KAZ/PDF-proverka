"""Tests for invariant paid_cost.json ↔ paid_cost_events.jsonl.

Контекст (фаза 5, после инцидента 2026-05-16):
  paid_cost.json daily_breakdown показывал n_calls=15, а paid_cost_events.jsonl
  содержал только 9 строк. Расхождение — потому что PaidCostTracker.add() и
  paid_api_events.record_paid_event() были двумя независимыми вызовами.
  Каждое новое место учёта могло забыть один из них (исторически: первые
  6 событий писались до того, как record_paid_event появился в коде).

Фикс: единый PaidCostTracker.record_paid(...) пишет в обе стороны одной
операцией с точки зрения caller'а. Существующие callsite'ы (llm_runner,
manager.stage02) мигрированы на него.

Что проверяет этот файл:
  1. N вызовов record_paid → N строк в paid_cost_events.jsonl.
  2. paid_cost.json daily_breakdown[date]["n_calls"] == N.
  3. total / by_model / by_project / by_stage суммируются корректно.
  4. cost_usd=0 или None НЕ создаёт строку события и НЕ инкрементирует.
  5. Если paid_api_events.record_paid_event падает, paid_cost.json всё ещё
     инкрементируется — но это явный warning в логе (best-effort).
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import pytest


@pytest.fixture
def isolated_tracker(tmp_path, monkeypatch):
    """Изолированный PaidCostTracker + paid_api_events с временными файлами."""
    _PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    if str(_PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(_PROJECT_ROOT))

    from backend.app.services.common import usage_service
    from backend.app.services.llm import paid_api_events as events_mod

    paid_cost_file = tmp_path / "paid_cost.json"
    paid_jsonl = tmp_path / "paid_cost_events.jsonl"
    blocked_jsonl = tmp_path / "paid_api_blocked_events.jsonl"

    monkeypatch.setattr(usage_service, "PAID_COST_FILE", paid_cost_file)
    monkeypatch.setattr(events_mod, "PAID_COST_EVENTS_FILE", paid_jsonl)
    monkeypatch.setattr(events_mod, "PAID_API_BLOCKED_EVENTS_FILE", blocked_jsonl)

    tracker = usage_service.PaidCostTracker()
    return {
        "tracker": tracker,
        "paid_cost_file": paid_cost_file,
        "paid_jsonl": paid_jsonl,
    }


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        out.append(json.loads(line))
    return out


def _read_paid_cost(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


# ─── Главный инвариант: n_calls == len(jsonl) ─────────────────────────


def test_three_paid_events_keep_invariant(isolated_tracker):
    """Каждый record_paid → 1 строка в jsonl + 1 инкремент в n_calls."""
    t = isolated_tracker["tracker"]
    t.record_paid(0.3227, model="openai/gpt-5.4", project_id="proj/A.pdf",
                  stage="block_analysis", source="manager.stage02",
                  manual_run_id="m1", job_id="j1",
                  input_tokens=40000, output_tokens=17000)
    t.record_paid(0.1500, model="openai/gpt-5.4", project_id="proj/A.pdf",
                  stage="block_analysis", source="manager.stage02",
                  manual_run_id="m1", job_id="j1",
                  input_tokens=20000, output_tokens=8000)
    t.record_paid(0.0500, model="google/gemini-2.5-flash", project_id="proj/B.pdf",
                  stage="text_analysis", source="llm_runner",
                  manual_run_id="m2", job_id="j2",
                  input_tokens=5000, output_tokens=2000)

    today = datetime.now().date().isoformat()
    events = _read_jsonl(isolated_tracker["paid_jsonl"])
    paid_cost = _read_paid_cost(isolated_tracker["paid_cost_file"])

    # 1. Кол-во строк в jsonl == 3
    assert len(events) == 3

    # 2. n_calls в daily_breakdown == 3
    day_entry = paid_cost["daily_breakdown"][today]
    assert day_entry["n_calls"] == 3

    # 3. total суммируется
    assert day_entry["total"] == pytest.approx(0.3227 + 0.1500 + 0.0500)

    # 4. by_model
    assert day_entry["by_model"]["openai/gpt-5.4"] == pytest.approx(0.4727)
    assert day_entry["by_model"]["google/gemini-2.5-flash"] == pytest.approx(0.05)

    # 5. by_project
    assert day_entry["by_project"]["proj/A.pdf"] == pytest.approx(0.4727)
    assert day_entry["by_project"]["proj/B.pdf"] == pytest.approx(0.05)

    # 6. by_stage
    assert day_entry["by_stage"]["block_analysis"] == pytest.approx(0.4727)
    assert day_entry["by_stage"]["text_analysis"] == pytest.approx(0.05)

    # 7. Каждое событие содержит обязательные поля.
    for ev in events:
        assert ev["event"] == "paid_api_cost"
        assert ev["cost_usd"] > 0
        assert ev["model"]
        assert ev["project_id"]
        assert ev["stage"]
        assert ev["source"]
        # Phase-2 forensic поля:
        assert "manual_run_id_present" in ev
        assert "manual_run_id_hash" in ev

    # 8. Sum cost_usd в jsonl == total в paid_cost.json
    sum_jsonl = sum(ev["cost_usd"] for ev in events)
    assert sum_jsonl == pytest.approx(day_entry["total"])


def test_zero_cost_records_nothing(isolated_tracker):
    """cost_usd=0 (cache hit, claude CLI subscription) не пишет ни строки в jsonl,
    ни инкремента в n_calls.

    Это критично для Stage 02 cache: на cache hit cost_usd=0, и мы НЕ должны
    создать фантомный paid event.
    """
    t = isolated_tracker["tracker"]
    t.record_paid(0.0, model="openai/gpt-5.4", project_id="proj/A.pdf",
                  stage="block_analysis", source="manager.stage02.cache_hit")
    t.record_paid(None, model="openai/gpt-5.4", project_id="proj/A.pdf",
                  stage="block_analysis", source="manager.stage02.cache_hit")
    t.record_paid(-1.0, model="openai/gpt-5.4", project_id="proj/A.pdf",
                  stage="block_analysis", source="manager.stage02.bug")

    events = _read_jsonl(isolated_tracker["paid_jsonl"])
    assert events == []

    # paid_cost.json не должен иметь сегодняшнюю запись (или должна быть пустой).
    if isolated_tracker["paid_cost_file"].exists():
        paid_cost = _read_paid_cost(isolated_tracker["paid_cost_file"])
        today = datetime.now().date().isoformat()
        assert paid_cost.get("daily_breakdown", {}).get(today, {}).get("n_calls", 0) == 0


def test_record_paid_propagates_source_and_manual_run(isolated_tracker):
    """Event content должен содержать source и manual_run_id_present для forensic.

    Это проверяет, что record_paid правильно пробрасывает поля во второй вызов
    (paid_api_events.record_paid_event) — а не только в bucket aggregation.
    """
    t = isolated_tracker["tracker"]
    t.record_paid(0.1, model="openai/gpt-5.4", project_id="proj/A.pdf",
                  stage="block_analysis", source="manager.stage02",
                  manual_run_id="real-mrid-xyz", job_id="j-real")

    events = _read_jsonl(isolated_tracker["paid_jsonl"])
    assert len(events) == 1
    ev = events[0]
    assert ev["source"] == "manager.stage02"
    assert ev["job_id"] == "j-real"
    assert ev["manual_run_id"] == "real-mrid-xyz"
    assert ev["manual_run_id_present"] is True
    assert len(ev["manual_run_id_hash"]) == 12


def test_event_failure_doesnt_block_aggregate(isolated_tracker, monkeypatch):
    """Если paid_api_events падает (disk full и т.п.), aggregate всё равно
    инкрементируется. Это best-effort политика: учёт денег важнее, чем лог.

    Это не значит, что событие может «пропасть» в production — paid_api_events
    использует свой best-effort log внутри. Но если он почему-то поднимает
    исключение, paid_cost.json должен остаться корректным.
    """
    t = isolated_tracker["tracker"]

    from backend.app.services.llm import paid_api_events as events_mod

    def _fail_event(*a, **kw):
        raise OSError("simulated disk full")

    monkeypatch.setattr(events_mod, "record_paid_event", _fail_event)

    # Не должно бросить.
    t.record_paid(0.1, model="openai/gpt-5.4", project_id="proj/A.pdf",
                  stage="block_analysis", source="manager.stage02")

    paid_cost = _read_paid_cost(isolated_tracker["paid_cost_file"])
    today = datetime.now().date().isoformat()
    # Aggregate всё равно увеличен.
    assert paid_cost["daily_breakdown"][today]["n_calls"] == 1
    assert paid_cost["daily_breakdown"][today]["total"] == pytest.approx(0.1)


# ─── Регрессионный sanity: legacy .add() остаётся доступен ────────────


def test_legacy_add_still_works_but_doesnt_write_event(isolated_tracker):
    """Регрессионный sanity: старый .add() остался работать (для совместимости
    тестов test_paid_cost_daily_breakdown), но он НЕ пишет в jsonl.

    Это намеренно: .add() — internal API для bucket aggregation. Production
    callsite'ы должны использовать record_paid (single source of truth).
    Если новый caller вызовет .add() напрямую, появится явное расхождение
    n_calls vs jsonl — и тест-инвариант его поймает.
    """
    t = isolated_tracker["tracker"]
    t.add(0.1, model="openai/gpt-5.4", project_id="proj/A.pdf",
          stage="block_analysis")

    paid_cost = _read_paid_cost(isolated_tracker["paid_cost_file"])
    today = datetime.now().date().isoformat()
    assert paid_cost["daily_breakdown"][today]["n_calls"] == 1

    # jsonl пуст — это диагностика, не bug. Если код раскачается, и кто-то
    # начнёт вызывать .add() напрямую для paid LLM учёта, инвариант будет
    # сломан — это поймает test_three_paid_events_keep_invariant выше при
    # реалистичном сценарии.
    events = _read_jsonl(isolated_tracker["paid_jsonl"])
    assert events == []
