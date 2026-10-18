"""Очередь запуска этапов со страницы «Проекты и этапы» (version_comparison_ai/stage_queue.py)."""
from __future__ import annotations

import json

import pytest

from backend.app.services.version_comparison import paths
from backend.app.services.version_comparison_ai import stage_queue as sq

OBJ = "obj-sq"


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path / "vc"))
    monkeypatch.setenv("VERSION_COMPARISON_AI_ENABLED", "1")
    cat = {"schema": "x", "object_id": OBJ, "versions": {},
           "projects": [{"project_id": "pA", "code": "А-ЭМ", "versions": []},
                        {"project_id": "pB", "code": "Б-ОВ", "versions": []}]}
    paths.catalog_path(OBJ).parent.mkdir(parents=True, exist_ok=True)
    paths.catalog_path(OBJ).write_text(json.dumps(cat, ensure_ascii=False), encoding="utf-8")
    # Состояние этапов вместо обзора: «запуск» переводит этап в running, тест сам «заканчивает» его.
    states = {(p, s): "todo" for p in ("pA", "pB") for s in sq.STAGES}
    started, paused = [], []
    monkeypatch.setattr(sq, "GRACE_S", 0)
    monkeypatch.setattr(sq, "_stages", lambda o, pid: {s: {"state": states[(pid, s)]} for s in sq.STAGES})

    def start(o, it):
        if states[(it["project_id"], it["stage"])] == "blocked":
            return "сравнение соседних не досчитано"
        started.append((it["project_id"], it["stage"]))
        states[(it["project_id"], it["stage"])] = "running"
        return None

    def pause(o, it):
        paused.append((it["project_id"], it["stage"]))
        states[(it["project_id"], it["stage"])] = "paused"
    monkeypatch.setattr(sq, "_start_stage", start)
    monkeypatch.setattr(sq, "_pause_stage", pause)
    return states, started, paused


def test_runs_one_by_one_project_after_project_stages_left_to_right(env):
    states, started, _ = env
    items = [{"project_id": "pB", "stage": "series"}, {"project_id": "pA", "stage": "report"},
             {"project_id": "pA", "stage": "luna"}, {"project_id": "pB", "stage": "ai"}]
    with pytest.raises(sq.QueueError, match="подтвердить"):
        sq.enqueue(OBJ, items, consent=False)
    q = sq.enqueue(OBJ, items, consent=True)
    assert [(it["project_id"], it["stage"]) for it in q["items"]] == [
        ("pB", "series"), ("pB", "ai"), *[("pA", s) for s in sq.STAGES]]
    assert started == [("pB", "series")], "сразу начинается только первый этап"
    sq.dispatch(OBJ)
    assert started == [("pB", "series")], "пока первый идёт, следующий не начинается"
    states[("pB", "series")] = "done"
    sq.dispatch(OBJ)
    assert started[-1] == ("pB", "ai")
    sq.enqueue(OBJ, [{"project_id": "pB", "stage": "ai"}], consent=True)       # уже в очереди — не дублируется
    assert len(sq.status(OBJ)["items"]) == 8
    states[("pB", "ai")] = "partial"
    states[("pA", "luna")] = "running"
    sq.dispatch(OBJ)
    states[("pA", "luna")] = "attention"
    for stage in sq.STAGES[1:]:
        sq.dispatch(OBJ)
        assert started[-1] == ("pA", stage)
        states[("pA", stage)] = "done"
    sq.dispatch(OBJ)
    q = sq.status(OBJ)
    assert all(it["state"] == "done" for it in q["items"])
    sq.dispatch(OBJ)
    assert sq.status(OBJ)["state"] == "done"


def test_pause_waits_and_resume_continues_same_stage_then_next(env):
    states, started, paused = env
    sq.enqueue(OBJ, [{"project_id": "pA", "stage": "series"}, {"project_id": "pB", "stage": "series"}], consent=False)
    q = sq.pause(OBJ)
    assert paused == [("pA", "series")] and q["state"] == "paused" and q["current"]["state"] == "paused"
    states[("pA", "series")] = "paused"
    sq.dispatch(OBJ)
    assert started == [("pA", "series")], "на паузе следующий этап не начинается"
    sq.resume(OBJ)
    assert started == [("pA", "series"), ("pA", "series")], "«Продолжить» — тот же этап с того же места"
    # Этап поставлен на паузу кнопкой в строке — очередь тоже останавливается.
    states[("pA", "series")] = "paused"
    sq.dispatch(OBJ)
    assert sq.status(OBJ)["state"] == "paused"
    sq.resume(OBJ)
    states[("pA", "series")] = "done"
    sq.dispatch(OBJ)
    assert started[-1] == ("pB", "series")
    assert sq.clear(OBJ)["items"] == [sq.status(OBJ)["current"]], "очистка оставляет только идущий этап"


def test_interrupted_stage_is_resumed_once_then_failed(env):
    states, started, _ = env
    sq.enqueue(OBJ, [{"project_id": "pA", "stage": "series"}], consent=False)
    states[("pA", "series")] = "interrupted"
    sq.dispatch(OBJ)                      # прерван перезапуском — отметка «досчитать»
    sq.dispatch(OBJ)                      # досчёт запущен
    assert started == [("pA", "series"), ("pA", "series")]
    states[("pA", "series")] = "interrupted"
    sq.dispatch(OBJ)
    assert sq.status(OBJ)["items"][0]["state"] == "failed"
