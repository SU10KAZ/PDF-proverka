"""Тесты Step 2b/10: late-stage audit shadow mirror (PipelineManager) и
expert-review/KB shadow mirror (knowledge_base_service).

Проверяют, что:
* завершённый audit-этап зовёт v2 shadow-mirror через safe-обёртку фасада;
* mirror fail-soft: исключение в v2-плече не ломает основной flow;
* в legacy-режиме helper — полный no-op (worker фасада не вызывается);
* сохранение expert_review зеркалит проект в v2 и тоже fail-soft.

Все тесты env-независимы там, где это важно: вместо реальной миграции в
projects_v2 подменяется safe-обёртка `shadow_mirror_project_id_safe`.
"""
from __future__ import annotations

import types

import pytest

from backend.app.pipeline.manager import PipelineManager
from backend.app.services.storage import storage_write_facade as swf
from backend.app.services.storage.storage_write_facade import StorageWriteFacade


def _job(job_id="run-123"):
    return types.SimpleNamespace(job_id=job_id)


# ── PipelineManager._shadow_mirror_completed_audit ──────────────────────────

def test_completed_audit_triggers_shadow_mirror(monkeypatch):
    """Helper зовёт фасадную safe-обёртку с project_id и run_id из job."""
    calls = []
    monkeypatch.setattr(
        swf, "shadow_mirror_project_id_safe",
        lambda pid, *, run_id=None: calls.append((pid, run_id)),
    )
    # self не используется в теле метода → допустим dummy
    PipelineManager._shadow_mirror_completed_audit(object(), "obj/proj", _job("J1"))
    assert calls == [("obj/proj", "J1")]


def test_completed_audit_shadow_mirror_fail_soft(monkeypatch):
    """Исключение в v2-плече не пробрасывается наружу — audit не падает."""
    def boom(pid, *, run_id=None):
        raise RuntimeError("v2 mirror exploded")

    monkeypatch.setattr(swf, "shadow_mirror_project_id_safe", boom)
    # не должно бросить
    PipelineManager._shadow_mirror_completed_audit(object(), "obj/proj", _job())


def test_late_artifacts_mirror_helper_noop_in_legacy_mode(monkeypatch):
    """В legacy-режиме helper — no-op: worker фасада не вызывается."""
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "legacy")
    assert swf.v2_writes_enabled() is False

    worker_called = []
    monkeypatch.setattr(
        StorageWriteFacade, "shadow_mirror_project_by_id",
        lambda self, pid, *, run_id=None: worker_called.append(pid),
    )
    # реальная safe-обёртка должна выйти немедленно, не дойдя до worker'а
    PipelineManager._shadow_mirror_completed_audit(object(), "obj/proj", _job())
    assert worker_called == []


# ── knowledge_base_service.save_expert_review ───────────────────────────────

def _patch_kb_internals(monkeypatch, tmp_path):
    """Изолировать save_expert_review от реального проекта/decisions_log."""
    import backend.app.services.knowledge_base.knowledge_base_service as kb

    monkeypatch.setattr(kb, "_output_dir", lambda pid, must_exist=False: tmp_path)
    monkeypatch.setattr(kb, "_enrich_decisions", lambda pid, decisions, reviewer: [])
    monkeypatch.setattr(kb, "_append_to_decisions_log", lambda enriched: None)
    return kb


def test_expert_review_save_triggers_shadow_mirror(monkeypatch, tmp_path):
    """save_expert_review зеркалит проект в v2 после записи expert_review.json."""
    kb = _patch_kb_internals(monkeypatch, tmp_path)
    calls = []
    monkeypatch.setattr(
        swf, "shadow_mirror_project_id_safe",
        lambda pid, *, run_id=None: calls.append(pid),
    )
    result = kb.save_expert_review("obj/proj", [])
    assert calls == ["obj/proj"]
    assert result["saved"] == 0


def test_expert_review_shadow_mirror_fail_soft(monkeypatch, tmp_path):
    """Исключение в v2-плече не ломает save_expert_review (legacy авторитетен)."""
    kb = _patch_kb_internals(monkeypatch, tmp_path)

    def boom(pid, *, run_id=None):
        raise RuntimeError("v2 mirror exploded")

    monkeypatch.setattr(swf, "shadow_mirror_project_id_safe", boom)
    result = kb.save_expert_review("obj/proj", [])
    assert result["saved"] == 0
