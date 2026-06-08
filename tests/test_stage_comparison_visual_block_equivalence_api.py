# -*- coding: utf-8 -*-
"""Router-тесты visual_block_equivalence API (Stage 3B).

Покрывает (см. задачу Stage 3B):
  1. feature flag OFF → запуск job отклоняется (403), create не вызывается;
  2. feature flag ON  → запуск вызывает job-service (create + background start);
  3. GET job status;
  4. GET jobs list фильтрует по session_id;
  5. cancel job вызывает cancel-функцию;
  6. GET pair visual-block-equivalence возвращает артефакт, если он есть;
  7. GET pair visual-block-equivalence → 404, если артефакта нет (+ 500 если битый);
  8. endpoint-ы не запускают Qwen/Opus/LLM (background start замокан, реальный
     recompute не стартует);
  9. без реального PDF (всё на tmp/mocks);
 10. tmp/monkeypatch, без живого comparison/sessions.

Монтируется ТОЛЬКО router (без main-app и portal-auth middleware) — легко и
изолированно.
"""
from __future__ import annotations

import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.services.stage_comparison import visual_block_equivalence_jobs as vbe_jobs
from backend.app.api.routers import stage_comparison as sc_router


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    # диск → tmp (живой comparison/ не трогается); registry сброшен
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path))
    # по умолчанию флаг OFF (тест ON выставляет сам)
    monkeypatch.delenv("STAGE_COMPARISON_VISUAL_BLOCK_EQUIVALENCE_JOBS_ENABLED", raising=False)
    vbe_jobs._reset_registry_for_tests()
    yield
    vbe_jobs._reset_registry_for_tests()


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(sc_router.router)
    return TestClient(app)


@pytest.fixture
def no_background(monkeypatch):
    """Замокать background start — реальный async-recompute (PDF/cv2) не стартует."""
    calls: list = []
    monkeypatch.setattr(vbe_jobs, "start_job_in_background",
                        lambda job_id, **kw: calls.append(job_id) or job_id)
    return calls


_BASE = "/api/stage-comparison"


# ─── 1. flag OFF → 403, create не вызывается ─────────────────────────────────


def test_start_rejected_when_flag_off(client, monkeypatch):
    spy = {"create": 0}
    monkeypatch.setattr(vbe_jobs, "create_visual_block_equivalence_job",
                        lambda *a, **k: spy.__setitem__("create", spy["create"] + 1) or {})
    r = client.post(f"{_BASE}/sessions/sessOFF/visual-block-equivalence/jobs",
                    json={"scope": "selected", "pair_ids": ["P1"]})
    assert r.status_code == 403
    assert spy["create"] == 0


# ─── 2. flag ON → вызывает job-service ───────────────────────────────────────


def test_start_runs_job_service_when_flag_on(client, monkeypatch, no_background):
    monkeypatch.setenv("STAGE_COMPARISON_VISUAL_BLOCK_EQUIVALENCE_JOBS_ENABLED", "true")
    r = client.post(f"{_BASE}/sessions/sessON/visual-block-equivalence/jobs",
                    json={"scope": "selected", "pair_ids": ["P1", "P2"]})
    assert r.status_code == 200
    body = r.json()
    assert body["type"] == "visual_block_equivalence"
    assert body["session_id"] == "sessON"
    assert body["scope"] == "selected"
    assert body["total_pairs"] == 2
    # background start вызван ровно для этого job; реальный recompute НЕ запущен
    assert no_background == [body["job_id"]]
    assert body["status"] == vbe_jobs.JOB_QUEUED          # ещё не обработан
    assert body["processed_pairs"] == 0
    assert body["enforced"] is False


def test_start_invalid_scope_returns_400(client, monkeypatch, no_background):
    monkeypatch.setenv("STAGE_COMPARISON_VISUAL_BLOCK_EQUIVALENCE_JOBS_ENABLED", "true")
    # scope=pair требует ровно один pair_id
    r = client.post(f"{_BASE}/sessions/s/visual-block-equivalence/jobs",
                    json={"scope": "pair", "pair_ids": ["P1", "P2"]})
    assert r.status_code == 400
    assert no_background == []


def test_start_session_scope_missing_session_404(client, monkeypatch, no_background):
    monkeypatch.setenv("STAGE_COMPARISON_VISUAL_BLOCK_EQUIVALENCE_JOBS_ENABLED", "true")
    from backend.app.services.stage_comparison import store as store_mod
    monkeypatch.setattr(store_mod, "get_session", lambda s: None)
    r = client.post(f"{_BASE}/sessions/nope/visual-block-equivalence/jobs",
                    json={"scope": "session"})
    assert r.status_code == 404


# ─── 3. GET job status ───────────────────────────────────────────────────────


def test_get_job_status(client):
    job = vbe_jobs.create_visual_block_equivalence_job(
        "sessG", scope="selected", pair_ids=["P1"], write_artifact=False)
    jid = job["job_id"]
    r = client.get(f"{_BASE}/sessions/sessG/visual-block-equivalence/jobs/{jid}")
    assert r.status_code == 200
    assert r.json()["job_id"] == jid
    assert r.json()["session_id"] == "sessG"


def test_get_job_unknown_404(client):
    r = client.get(f"{_BASE}/sessions/sessG/visual-block-equivalence/jobs/vbej_nope")
    assert r.status_code == 404


def test_get_job_wrong_session_404(client):
    job = vbe_jobs.create_visual_block_equivalence_job(
        "sessG", scope="selected", pair_ids=["P1"], write_artifact=False)
    r = client.get(f"{_BASE}/sessions/OTHER/visual-block-equivalence/jobs/{job['job_id']}")
    assert r.status_code == 404


# ─── 4. GET jobs list ────────────────────────────────────────────────────────


def test_list_jobs_filters_by_session(client):
    j1 = vbe_jobs.create_visual_block_equivalence_job(
        "sessL", scope="selected", pair_ids=["P1"], write_artifact=False)
    vbe_jobs.create_visual_block_equivalence_job(
        "sessOTHER", scope="selected", pair_ids=["P2"], write_artifact=False)
    r = client.get(f"{_BASE}/sessions/sessL/visual-block-equivalence/jobs")
    assert r.status_code == 200
    jobs = r.json()["jobs"]
    assert [x["job_id"] for x in jobs] == [j1["job_id"]]
    # другая сессия — пусто
    r2 = client.get(f"{_BASE}/sessions/EMPTY/visual-block-equivalence/jobs")
    assert r2.json()["jobs"] == []


# ─── 5. cancel ───────────────────────────────────────────────────────────────


def test_cancel_job(client):
    job = vbe_jobs.create_visual_block_equivalence_job(
        "sessC", scope="selected", pair_ids=["P1", "P2"], write_artifact=False)
    jid = job["job_id"]
    r = client.post(f"{_BASE}/sessions/sessC/visual-block-equivalence/jobs/{jid}/cancel")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == vbe_jobs.JOB_CANCELLED
    assert body["cancel_requested"] is True


def test_cancel_unknown_job_404(client):
    r = client.post(f"{_BASE}/sessions/sessC/visual-block-equivalence/jobs/vbej_x/cancel")
    assert r.status_code == 404


# ─── 6 / 7. read artifact ────────────────────────────────────────────────────


def _write_artifact(session_id, pair_id, payload):
    from backend.app.services.stage_comparison import paths as paths_mod
    p = paths_mod.visual_block_equivalence_report_path(session_id, pair_id)
    p.write_text(json.dumps(payload), encoding="utf-8")
    return p


def test_get_pair_artifact_present(client):
    _write_artifact("sessA", "pairA", {
        "schema_version": 1, "mode": "mark_only", "pairs": [],
        "summary": {"links_total": 0},
    })
    r = client.get(f"{_BASE}/sessions/sessA/pairs/pairA/visual-block-equivalence")
    assert r.status_code == 200
    assert r.json()["schema_version"] == 1
    assert r.json()["mode"] == "mark_only"


def test_get_pair_artifact_missing_404(client):
    r = client.get(f"{_BASE}/sessions/sessA/pairs/missing/visual-block-equivalence")
    assert r.status_code == 404


def test_get_pair_artifact_broken_500(client):
    from backend.app.services.stage_comparison import paths as paths_mod
    p = paths_mod.visual_block_equivalence_report_path("sessB", "pairB")
    p.write_text("{not-json", encoding="utf-8")
    r = client.get(f"{_BASE}/sessions/sessB/pairs/pairB/visual-block-equivalence")
    assert r.status_code == 500


# ─── 8 / 9. без Qwen/Opus/LLM, без реального PDF ─────────────────────────────


def test_start_does_not_trigger_real_recompute(client, monkeypatch):
    """flag ON, но НИ create НИ start не должны вызвать реальный per-pair runner
    (рендер PDF / cv2). Подменяем сам Stage 2 runner sentinel'ом — он не должен
    вызваться: фоновый запуск замокан, а create/get синхронны."""
    monkeypatch.setenv("STAGE_COMPARISON_VISUAL_BLOCK_EQUIVALENCE_JOBS_ENABLED", "true")
    called = {"runner": 0, "bg": 0}

    def _boom_runner(*a, **k):  # реальный recompute — НЕ должен вызваться
        called["runner"] += 1
        raise AssertionError("real recompute must not run in endpoint test")

    monkeypatch.setattr(vbe_jobs, "run_pair_visual_block_equivalence", _boom_runner)
    monkeypatch.setattr(vbe_jobs, "start_job_in_background",
                        lambda job_id, **k: called.__setitem__("bg", called["bg"] + 1) or job_id)

    r = client.post(f"{_BASE}/sessions/sessSafe/visual-block-equivalence/jobs",
                    json={"scope": "pair", "pair_ids": ["P1"]})
    assert r.status_code == 200
    assert called["runner"] == 0        # реальный recompute не запущен
    assert called["bg"] == 1            # только фоновый запуск (замокан)


def test_router_module_does_not_import_qwen_opus_llm():
    import ast
    from pathlib import Path
    # сам файл роутера большой; проверяем, что НАШ vbe_jobs-импорт есть и что
    # vbe_jobs (наш слой) не тянет qwen/opus/llm (через его собственный AST).
    src = Path(sc_router.__file__).read_text(encoding="utf-8")
    assert "visual_block_equivalence_jobs as vbe_jobs_mod" in src

    jobs_src = Path(vbe_jobs.__file__).read_text(encoding="utf-8")
    tree = ast.parse(jobs_src)
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
            for a in node.names:
                imported.add(f"{node.module or ''}.{a.name}")
        elif isinstance(node, ast.Import):
            for a in node.names:
                imported.add(a.name)
    blob = "\n".join(sorted(imported))
    for token in ("graphic_llm", "enriched_comparison", "unified_analysis",
                  "md_enrichment_jobs", "pipeline_queue", "qwen", "opus"):
        assert token not in blob, f"unexpected dependency in job layer: {token}"
