"""
test_schedule_completion_stamp.py
---------------------------------
Штамп «дня завершения» проекта для графика работ.

Проверяет связку save_expert_review → _stamp_schedule_completion_if_complete →
schedule_service.set_completion_once:

* полная разметка (ВСЕ замечания + ВСЕ оптимизации) → день завершения фиксируется;
* частичная разметка (оптимизация не размечена) → НЕ фиксируется;
* день заморожен: повторная правка решений в другой день НЕ двигает день завершения.

Run:
    python -m pytest tests/test_schedule_completion_stamp.py -v
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.models.expert_review import ExpertDecision


def _write_json(path: Path, payload: dict | list) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


@pytest.fixture
def kb_env(tmp_path, monkeypatch):
    """Изолированный legacy-проект + изолированные стораджи KB/графика."""
    import backend.app.services.common.project_service as project_service
    import backend.app.services.common.schedule_service as schedule_service
    import backend.app.services.knowledge_base.knowledge_base_service as kb
    from backend.app.services.storage import storage_write_facade as swf

    kb_root = tmp_path / "knowledge_base"
    monkeypatch.setattr(kb, "KNOWLEDGE_BASE_DIR", kb_root)
    monkeypatch.setattr(kb, "DECISIONS_LOG_FILE", kb_root / "decisions_log.json")
    monkeypatch.setattr(kb, "PATTERNS_FILE", kb_root / "patterns.json")
    monkeypatch.setattr(
        schedule_service, "SCHEDULE_COMPLETION_FILE", kb_root / "schedule_completion.json")
    monkeypatch.setattr(swf, "shadow_mirror_project_id_safe", lambda *a, **k: None)
    monkeypatch.setattr(project_service, "_get_projects_dir", lambda: tmp_path / "projects")
    # object_id детерминирован (без чтения глобального состояния выбора объекта).
    monkeypatch.setattr(kb, "_resolve_object_id", lambda: "")

    proj = tmp_path / "projects" / "DOC-STAMP"
    out = proj / "_output"
    out.mkdir(parents=True)
    _write_json(proj / "project_info.json", {"project_id": "DOC-STAMP", "section": "AR"})
    _write_json(out / "03_findings.json", {"findings": [{"id": "F-1"}]})
    _write_json(out / "optimization.json", {"items": [{"id": "OPT-1"}]})
    return kb, schedule_service, kb_root


def test_full_markup_stamps_completion_day(kb_env):
    kb, sched, _ = kb_env
    kb.save_expert_review("DOC-STAMP", [
        ExpertDecision(item_id="F-1", item_type="finding", decision="accepted",
                       timestamp="2026-06-18T07:00:00Z"),
        ExpertDecision(item_id="OPT-1", item_type="optimization", decision="rejected",
                       timestamp="2026-06-18T08:00:00Z"),
    ], reviewer="Узун А. И.")
    comps = sched.load_schedule_completions()
    assert comps.get(("", "DOC-STAMP")) == "2026-06-18"


def test_partial_markup_does_not_stamp(kb_env):
    kb, sched, kb_root = kb_env
    # размечено только замечание, оптимизация — нет → не завершено
    kb.save_expert_review("DOC-STAMP", [
        ExpertDecision(item_id="F-1", item_type="finding", decision="accepted",
                       timestamp="2026-06-18T07:00:00Z"),
    ], reviewer="Узун А. И.")
    assert sched.load_schedule_completions() == {}
    assert not (kb_root / "schedule_completion.json").exists()


def test_completion_day_frozen_across_later_edits(kb_env):
    kb, sched, _ = kb_env
    # день 1: полная разметка → день завершения 18-е
    kb.save_expert_review("DOC-STAMP", [
        ExpertDecision(item_id="F-1", item_type="finding", decision="accepted",
                       timestamp="2026-06-18T07:00:00Z"),
        ExpertDecision(item_id="OPT-1", item_type="optimization", decision="rejected",
                       timestamp="2026-06-18T08:00:00Z"),
    ], reviewer="Узун А. И.")
    # день 2: правка решения по F-1 (новый timestamp) — день НЕ должен меняться
    kb.save_expert_review("DOC-STAMP", [
        ExpertDecision(item_id="F-1", item_type="finding", decision="rejected",
                       timestamp="2026-06-25T09:00:00Z"),
    ], reviewer="Узун А. И.")
    comps = sched.load_schedule_completions()
    assert comps.get(("", "DOC-STAMP")) == "2026-06-18"


def test_no_optimization_items_completes_on_findings_only(kb_env):
    kb, sched, _ = kb_env
    # в проекте нет оптимизаций → достаточно разметить все замечания
    out = kb.resolve_project_dir("DOC-STAMP") / "_output"
    _write_json(out / "optimization.json", {"items": []})
    kb.save_expert_review("DOC-STAMP", [
        ExpertDecision(item_id="F-1", item_type="finding", decision="accepted",
                       timestamp="2026-06-19T07:00:00Z"),
    ], reviewer="Узун А. И.")
    comps = sched.load_schedule_completions()
    assert comps.get(("", "DOC-STAMP")) == "2026-06-19"
