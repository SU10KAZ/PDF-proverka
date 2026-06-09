"""Тесты режима «Только Opus» (opus_only.prepare_opus_only + endpoint).

Режим запускает unified-analysis по готовым enriched MD без Qwen. Покрытие:
  1. Qwen не запускается (prepare не трогает enriched MD; endpoint вызывает
     create_unified_job с force_enrichment=False);
  2. пара без enriched MD → skip missing_enriched_md;
  3. пара с left/right enriched MD → eligible;
  4. existing comparison_result бэкапится перед перезапуском;
  5. clear_comparison_result удаляет результат (после backup);
  6. пара с running job не запускается (skip running_job);
  7. batch обрабатывает только переданные pair_ids;
  8. expert_review / v2_review_status НЕ трогаются;
  9. too_large → skip;
 10. endpoint: force_enrichment=False, force_compare=True, ответ ok/job_id/skipped.
"""
from __future__ import annotations

import json

import pytest

from backend.app.services.stage_comparison import opus_only as oo
from backend.app.services.stage_comparison import clear_analysis as clr
from backend.app.services.stage_comparison import store as store_mod
from backend.app.services.stage_comparison import paths as paths_mod

SID = "sess_oo"


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    # по умолчанию — нет активных job'ов
    monkeypatch.setattr(clr, "active_pair_ids", lambda _sid: set())
    return tmp_path


def _session(*pids):
    return {"id": SID, "pairs": [{"id": p} for p in pids]}


def _write_md(pid, side, text="x"):
    p = paths_mod.text_enrichment_md_path(SID, pid, side)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")


def _write_result(pid, payload):
    p = paths_mod.enriched_comparison_result_path(SID, pid)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _write_review(pid, payload):
    p = paths_mod.pair_dir(SID, pid) / "expert_review.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_skip_missing_enriched_md(env, monkeypatch):
    monkeypatch.setattr(store_mod, "get_session", lambda _s: _session("p1"))
    # MD не записан → пара пропускается.
    r = oo.prepare_opus_only(SID, ["p1"])
    assert r["eligible"] == []
    assert r["skipped"] == [{"pair_id": "p1", "reason": "missing_enriched_md"}]


def test_eligible_with_enriched_md(env, monkeypatch):
    monkeypatch.setattr(store_mod, "get_session", lambda _s: _session("p1"))
    _write_md("p1", "left"); _write_md("p1", "right")
    r = oo.prepare_opus_only(SID, ["p1"], backup_existing=False)
    assert r["eligible"] == ["p1"]
    assert r["skipped"] == []


def test_skip_running_job(env, monkeypatch):
    monkeypatch.setattr(store_mod, "get_session", lambda _s: _session("p1"))
    _write_md("p1", "left"); _write_md("p1", "right")
    monkeypatch.setattr(clr, "active_pair_ids", lambda _sid: {"p1"})
    r = oo.prepare_opus_only(SID, ["p1"])
    assert r["eligible"] == []
    assert r["skipped"] == [{"pair_id": "p1", "reason": "running_job"}]


def test_unknown_pair_skipped(env, monkeypatch):
    monkeypatch.setattr(store_mod, "get_session", lambda _s: _session("p1"))
    r = oo.prepare_opus_only(SID, ["pX"])
    assert r["skipped"] == [{"pair_id": "pX", "reason": "unknown_pair"}]


def test_backup_existing_creates_backup(env, monkeypatch):
    monkeypatch.setattr(store_mod, "get_session", lambda _s: _session("p1"))
    _write_md("p1", "left"); _write_md("p1", "right")
    _write_result("p1", {"status": "done", "changes": [{"id": "a"}]})
    r = oo.prepare_opus_only(SID, ["p1"], backup_existing=True, clear_comparison_result=False)
    assert r["eligible"] == ["p1"]
    assert "p1" in r["backups"]
    # backup содержит старый comparison_result, оригинал НЕ удалён (overwrite Opus'ом).
    from pathlib import Path
    bk = Path(r["backups"]["p1"]) / "enriched_comparison" / "comparison_result.json"
    assert bk.exists()
    assert paths_mod.enriched_comparison_result_path(SID, "p1").exists()  # не удалён


def test_clear_comparison_result_removes_with_backup(env, monkeypatch):
    monkeypatch.setattr(store_mod, "get_session", lambda _s: _session("p1"))
    _write_md("p1", "left"); _write_md("p1", "right")
    _write_result("p1", {"status": "done", "changes": [{"id": "a"}]})
    r = oo.prepare_opus_only(SID, ["p1"], clear_comparison_result=True)
    assert r["eligible"] == ["p1"]
    from pathlib import Path
    bk = Path(r["backups"]["p1"]) / "enriched_comparison" / "comparison_result.json"
    assert bk.exists()                                                    # бэкап есть
    assert not paths_mod.enriched_comparison_result_path(SID, "p1").exists()  # удалён


def test_no_backup_when_disabled_and_no_result(env, monkeypatch):
    monkeypatch.setattr(store_mod, "get_session", lambda _s: _session("p1"))
    _write_md("p1", "left"); _write_md("p1", "right")
    r = oo.prepare_opus_only(SID, ["p1"], backup_existing=False)
    assert r["backups"] == {}


def test_expert_review_not_touched(env, monkeypatch):
    monkeypatch.setattr(store_mod, "get_session", lambda _s: _session("p1"))
    _write_md("p1", "left"); _write_md("p1", "right")
    _write_result("p1", {"status": "done", "changes": []})
    _write_review("p1", {"verdicts": {"c1": "accepted"}})
    oo.prepare_opus_only(SID, ["p1"], clear_comparison_result=True)
    rev = paths_mod.pair_dir(SID, "p1") / "expert_review.json"
    assert rev.exists()                                                   # ручные отметки целы
    assert json.loads(rev.read_text())["verdicts"]["c1"] == "accepted"


def test_too_large_skipped(env, monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_ENRICHED_COMPARE_MAX_CHARS", "10")
    monkeypatch.setattr(store_mod, "get_session", lambda _s: _session("p1"))
    _write_md("p1", "left", "x" * 50); _write_md("p1", "right", "y" * 50)  # 100 > 10
    r = oo.prepare_opus_only(SID, ["p1"])
    assert r["eligible"] == []
    assert r["skipped"][0]["reason"] == "too_large"


def test_only_selected_pairs(env, monkeypatch):
    monkeypatch.setattr(store_mod, "get_session", lambda _s: _session("p1", "p2", "p3"))
    for p in ("p1", "p2", "p3"):
        _write_md(p, "left"); _write_md(p, "right")
    r = oo.prepare_opus_only(SID, ["p2"], backup_existing=False)
    assert r["eligible"] == ["p2"]   # только выбранная


# ─── endpoint ────────────────────────────────────────────────────────────────

def _client():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.app.api.routers import stage_comparison as router_mod
    app = FastAPI()
    app.include_router(router_mod.router)
    return TestClient(app), router_mod


def test_endpoint_passes_opus_only_flags(env, monkeypatch):
    monkeypatch.setattr(store_mod, "get_session", lambda _s: _session("p1"))
    _write_md("p1", "left"); _write_md("p1", "right")
    client, router_mod = _client()
    captured = {}

    def _fake_create(session_id, **kw):
        captured.update(kw)
        return {"id": "uajob_test", "status": "queued"}

    monkeypatch.setattr(router_mod.unified_jobs_mod, "create_unified_job", _fake_create)
    monkeypatch.setattr(router_mod.unified_jobs_mod, "start_job_in_background", lambda *a, **k: None)

    r = client.post(f"/api/stage-comparison/sessions/{SID}/pairs/opus-only",
                    json={"pair_ids": ["p1"], "force": True, "backup_existing": True})
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["job_id"] == "uajob_test"
    assert body["started_pairs"] == ["p1"]
    # ключевой инвариант режима: Qwen не запускается.
    assert captured["force_enrichment"] is False
    assert captured["force_compare"] is True
    assert captured["pair_ids"] == ["p1"]


def test_endpoint_skips_missing_md_without_job(env, monkeypatch):
    monkeypatch.setattr(store_mod, "get_session", lambda _s: _session("p1"))
    # MD нет → нет eligible → job не создаётся.
    client, router_mod = _client()
    called = {"n": 0}
    monkeypatch.setattr(router_mod.unified_jobs_mod, "create_unified_job",
                        lambda *a, **k: called.__setitem__("n", called["n"] + 1) or {"id": "x", "status": "queued"})
    r = client.post(f"/api/stage-comparison/sessions/{SID}/pairs/opus-only",
                    json={"pair_ids": ["p1"]})
    assert r.status_code == 200
    body = r.json()
    assert body["job_id"] is None
    assert body["started_pairs"] == []
    assert body["skipped"][0]["reason"] == "missing_enriched_md"
    assert called["n"] == 0  # job не создавался
