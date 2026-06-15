# -*- coding: utf-8 -*-
"""Tests for Pipeline V2 controlled operator-triggered run jobs.

backend/app/services/stage_comparison/pipeline_v2_run_jobs.py

Runner всегда замокан — реальный pipeline/модели НЕ запускаются.
"""
import asyncio
import json

import pytest

from backend.app.services.stage_comparison import pipeline_v2_run_jobs as m
from backend.app.services.stage_comparison import store as store_mod
from backend.app.services.stage_comparison import paths as paths_mod
from backend.app.services.stage_comparison import (
    pipeline_v2_payload_service as payload_mod,
)

SID = "sess1"
PID = "pairA"


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Изолировать comparison root + замокать session/pair + runner."""
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path))

    def fake_get_session(sid):
        return {"id": sid} if sid == SID else None

    def fake_find_pair(sid, pid):
        if sid == SID and pid == PID:
            return {
                "id": PID, "status": "matched",
                "left": {"pdf_path": "/x/l.pdf",
                         "result_json_path": "/x/l_result.json",
                         "md_path": "/x/l.md"},
                "right": {"pdf_path": "/x/r.pdf",
                          "result_json_path": "/x/r_result.json",
                          "md_path": "/x/r.md"},
            }
        return None

    monkeypatch.setattr(store_mod, "get_session", fake_get_session)
    monkeypatch.setattr(store_mod, "_find_pair_meta", fake_find_pair)
    # реальные active-task'и между тестами не текут
    m._active_tasks.clear()
    return tmp_path


def _ok_body(**kw):
    b = {"confirm": True, "confirm_session_id": SID, "confirm_pair_id": PID,
         "mode": "dry_run"}
    b.update(kw)
    return b


def _mock_runner(monkeypatch, *, status="ok", boom=False):
    calls = {"n": 0, "llm_runner": "unset", "vision_runner": "unset"}

    def fake(left, right, out_dir, options=None, llm_runner=None,
             vision_runner=None):
        calls["n"] += 1
        calls["llm_runner"] = llm_runner
        calls["vision_runner"] = vision_runner
        if boom:
            raise RuntimeError("runner blew up")
        from pathlib import Path
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        (Path(out_dir) / payload_mod.SUMMARY_FILENAME).write_text(
            json.dumps({"status": status}), encoding="utf-8")
        return {"status": status, "warnings": []}

    monkeypatch.setattr(m, "run_pipeline_v2_dry_run", fake)
    return calls


# ─── validation / confirm gates ──────────────────────────────────────────

class TestValidation:
    def test_reject_without_confirm(self, env):
        with pytest.raises(m.PipelineV2RunConfirmError):
            m.create_run_job(SID, PID, {"confirm": False,
                                        "confirm_session_id": SID,
                                        "confirm_pair_id": PID})

    def test_reject_wrong_confirm_pair_id(self, env):
        with pytest.raises(m.PipelineV2RunConfirmError):
            m.create_run_job(SID, PID, _ok_body(confirm_pair_id="WRONG"))

    def test_reject_wrong_confirm_session_id(self, env):
        with pytest.raises(m.PipelineV2RunConfirmError):
            m.create_run_job(SID, PID, _ok_body(confirm_session_id="WRONG"))

    def test_reject_nonexistent_session(self, env):
        with pytest.raises(m.PipelineV2RunNotFound):
            m.create_run_job("NOPE", PID,
                             _ok_body(confirm_session_id="NOPE"))

    def test_reject_nonexistent_pair(self, env):
        with pytest.raises(m.PipelineV2RunNotFound):
            m.create_run_job(SID, "NOPE", _ok_body(confirm_pair_id="NOPE"))


# ─── create / artifacts gate / lock ──────────────────────────────────────

class TestCreate:
    def test_create_queued_job(self, env):
        job = m.create_run_job(SID, PID, _ok_body())
        assert job["status"] == m.STATUS_QUEUED
        assert job["pair_id"] == PID and job["type"] == m.JOB_TYPE
        assert job["models_touched"] == {"qwen": False, "gemma": False,
                                         "opus": False, "claude": False}
        # персистнут на диск
        assert m.get_job(SID, job["id"])["status"] == m.STATUS_QUEUED

    def test_409_when_artifacts_exist_without_rerun(self, env):
        art = payload_mod.pipeline_v2_artifacts_dir(SID, PID)
        art.mkdir(parents=True, exist_ok=True)
        (art / payload_mod.SUMMARY_FILENAME).write_text("{}", encoding="utf-8")
        with pytest.raises(m.PipelineV2RunConflict):
            m.create_run_job(SID, PID, _ok_body(rerun_existing=False))

    def test_rerun_existing_true_allowed_when_artifacts_exist(self, env):
        art = payload_mod.pipeline_v2_artifacts_dir(SID, PID)
        art.mkdir(parents=True, exist_ok=True)
        (art / payload_mod.SUMMARY_FILENAME).write_text("{}", encoding="utf-8")
        job = m.create_run_job(SID, PID, _ok_body(rerun_existing=True))
        assert job["status"] == m.STATUS_QUEUED and job["rerun_existing"] is True

    def test_lock_prevents_duplicate_run(self, env):
        m.create_run_job(SID, PID, _ok_body())  # job1 queued (active)
        with pytest.raises(m.PipelineV2RunConflict):
            m.create_run_job(SID, PID, _ok_body())  # job2 must conflict


# ─── background run (mocked runner) ──────────────────────────────────────

class TestRun:
    def test_completed_run_writes_manifest_and_offline(self, env, monkeypatch):
        calls = _mock_runner(monkeypatch, status="ok")
        job = m.create_run_job(SID, PID, _ok_body())
        done = asyncio.run(m.run_pipeline_v2_run_job(SID, job["id"]))
        assert done["status"] == m.STATUS_COMPLETED
        assert calls["n"] == 1
        # offline: runner вызван БЕЗ моделей
        assert calls["llm_runner"] is None and calls["vision_runner"] is None
        assert done["models_touched"] == {"qwen": False, "gemma": False,
                                          "opus": False, "claude": False}
        # manifest записан в pipeline_v2/
        art = payload_mod.pipeline_v2_artifacts_dir(SID, PID)
        manifests = list(art.glob(m.RUN_MANIFEST_PREFIX + "*.json"))
        assert len(manifests) == 1
        man = json.loads(manifests[0].read_text(encoding="utf-8"))
        assert man["runner"] == "run_pipeline_v2_dry_run"
        assert man["status"] == m.STATUS_COMPLETED
        assert man["models_touched"]["opus"] is False

    def test_failed_runner_saves_failed_status(self, env, monkeypatch):
        _mock_runner(monkeypatch, boom=True)
        job = m.create_run_job(SID, PID, _ok_body())
        done = asyncio.run(m.run_pipeline_v2_run_job(SID, job["id"]))
        assert done["status"] == m.STATUS_FAILED
        assert "runner blew up" in (done.get("error") or "")
        # статус персистнут
        assert m.get_job(SID, job["id"])["status"] == m.STATUS_FAILED

    def test_runner_status_failed_marks_failed(self, env, monkeypatch):
        _mock_runner(monkeypatch, status="failed")
        job = m.create_run_job(SID, PID, _ok_body())
        done = asyncio.run(m.run_pipeline_v2_run_job(SID, job["id"]))
        assert done["status"] == m.STATUS_FAILED

    def test_rerun_creates_backup(self, env, monkeypatch):
        # существующие артефакты → backup перед rerun
        art = payload_mod.pipeline_v2_artifacts_dir(SID, PID)
        art.mkdir(parents=True, exist_ok=True)
        (art / payload_mod.SUMMARY_FILENAME).write_text(
            json.dumps({"status": "ok", "old": True}), encoding="utf-8")
        _mock_runner(monkeypatch, status="ok")
        job = m.create_run_job(SID, PID,
                               _ok_body(rerun_existing=True, create_backup=True))
        done = asyncio.run(m.run_pipeline_v2_run_job(SID, job["id"]))
        assert done["status"] == m.STATUS_COMPLETED
        assert done["created_backup"] is True
        pair_root = paths_mod.pair_dir(SID, PID)
        backups = list(pair_root.glob("pipeline_v2_backup_before_ui_run_*"))
        assert len(backups) == 1
        # backup содержит старую версию summary
        old = json.loads((backups[0] / payload_mod.SUMMARY_FILENAME)
                         .read_text(encoding="utf-8"))
        assert old.get("old") is True


# ─── cancel / stale ──────────────────────────────────────────────────────

class TestCancelStale:
    def test_cancel_queued(self, env):
        job = m.create_run_job(SID, PID, _ok_body())
        out = m.cancel_job(SID, job["id"])
        assert out["status"] == m.STATUS_CANCELLED
        # после cancel — lock снят
        assert m.find_active_pair_job(SID, PID) is None

    def test_safety_no_model_or_llm_imports(self):
        """Модуль не импортит реальные LLM/Qwen/Opus провайдеры напрямую."""
        import ast
        from pathlib import Path
        src = (Path(__file__).resolve().parent.parent /
               "backend/app/services/stage_comparison/pipeline_v2_run_jobs.py")
        tree = ast.parse(src.read_text(encoding="utf-8"))
        imported = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                imported.append(node.module or "")
            elif isinstance(node, ast.Import):
                imported += [a.name for a in node.names]
        forbidden = ("graphic_llm", "text_llm_provider", "llm_runner",
                     "providers", "httpx", "requests", "qwen", "gemma")
        bad = [x for x in imported if any(f in x.lower() for f in forbidden)]
        assert bad == [], f"unexpected imports: {bad}"
