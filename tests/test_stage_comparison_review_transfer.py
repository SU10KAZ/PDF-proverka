"""Тесты переноса экспертных оценок из «Расхождений» в V2.

Покрывают:
  • точный перенос по raw_id (детерминированный, без Claude);
  • сохранность исходных chg_-решений (не теряются);
  • конфликт: у v2-находки уже есть решение → НЕ перезаписывается, помечается;
  • семантический перенос остатка через (фейковый) Claude + флаг «проверить»
    для неуверенных совпадений;
  • fail-soft: Claude недоступен → переносятся только точные, остаток unmatched.
"""
from __future__ import annotations

import json
from datetime import datetime

import pytest


@pytest.fixture(autouse=True)
def _tmp_comparison_root(tmp_path, monkeypatch):
    root = tmp_path / "comparison_transfer_test"
    root.mkdir()
    monkeypatch.setenv("COMPARISON_ROOT", str(root))
    yield root


def _paths():
    from backend.app.services.stage_comparison import paths as paths_mod
    return paths_mod


def _write_session(session_id: str, pair_ids: list[str]):
    paths_mod = _paths()
    session = {
        "id": session_id,
        "pair_order": list(pair_ids),
        "warnings": [],
        "created_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    paths_mod.session_json_path(session_id).write_text(
        json.dumps(session, ensure_ascii=False), encoding="utf-8")


def _write_pair(session_id: str, pair_id: str):
    paths_mod = _paths()
    pair = {
        "id": pair_id,
        "status": "matched",
        "label": pair_id,
        "left": {"filename": f"{pair_id}_L.pdf", "pdf_path": "/dev/null/L"},
        "right": {"filename": f"{pair_id}_R.pdf", "pdf_path": "/dev/null/R"},
    }
    paths_mod.pair_json_path(session_id, pair_id).write_text(
        json.dumps(pair, ensure_ascii=False), encoding="utf-8")


def _write_comparison_result(session_id: str, pair_id: str, changes: list[dict]):
    paths_mod = _paths()
    p = paths_mod.enriched_comparison_result_path(session_id, pair_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"version": 1, "status": "done", "changes": changes},
                            ensure_ascii=False), encoding="utf-8")


def _change(cid, *, title):
    return {
        "id": cid, "source": "text", "type": "changed", "category": "general",
        "severity": "medium", "title": title, "summary": f"summary {title}",
        "old_value": "было", "new_value": "стало", "construction_impact": "влияние",
        "cost_impact": "unknown", "requires_human_review": False, "confidence": 0.9,
        "evidence_left": {"quote": "L", "section": "X", "approx_location": "стр. 1"},
        "evidence_right": {"quote": "R", "section": "X", "approx_location": "стр. 1"},
    }


def _seed_source_decisions(session_id, pair_id, decisions):
    """decisions: list of (raw_id, decision, reason). Пишет в expert_review.json."""
    from backend.app.services.stage_comparison import expert_review as er
    er.apply_batch(
        session_id,
        decisions=[{"item_id": er.make_key(pair_id, raw), "decision": dec, "rejection_reason": reason}
                   for raw, dec, reason in decisions],
    )


def _built_items(session_id, pair_id):
    from backend.app.services.stage_comparison import v2_review as v2r
    return v2r.build_pair_v2_changes(session_id, pair_id)["items"]


# ─── Точный перенос ──────────────────────────────────────────────────────


def test_exact_transfer_copies_decision_and_preserves_source():
    from backend.app.services.stage_comparison import review_transfer as rt
    from backend.app.services.stage_comparison import expert_review as er

    sid, pid = "s_exact", "p1"
    _write_session(sid, [pid])
    _write_pair(sid, pid)
    _write_comparison_result(sid, pid, [
        _change("chg_a", title="A"), _change("chg_b", title="B"),
    ])
    _seed_source_decisions(sid, pid, [
        ("chg_a", "rejected", "оформительское"),
        ("chg_b", "accepted", ""),
    ])

    rep = rt.transfer_session(sid, use_claude=False)
    assert rep["totals"]["exact"] == 2
    assert rep["totals"]["applied"] == 2
    assert rep["totals"]["conflicts"] == 0

    store = er.load(sid)["decisions"]
    items = {it["raw_id"]: it for it in _built_items(sid, pid)}
    key_a = er.make_key(pid, items["chg_a"]["id"])
    key_b = er.make_key(pid, items["chg_b"]["id"])
    # Перенесённые v2-ключи
    assert store[key_a]["decision"] == "rejected"
    assert store[key_a]["rejection_reason"] == "оформительское"
    assert store[key_a]["transferred"] is True
    assert store[key_a]["transfer_method"] == "exact"
    assert store[key_b]["decision"] == "accepted"
    # Исходные chg_-решения не потеряны
    assert store[er.make_key(pid, "chg_a")]["decision"] == "rejected"
    assert store[er.make_key(pid, "chg_b")]["decision"] == "accepted"


def test_conflict_does_not_overwrite_existing_v2_decision():
    from backend.app.services.stage_comparison import review_transfer as rt
    from backend.app.services.stage_comparison import expert_review as er

    sid, pid = "s_conflict", "p1"
    _write_session(sid, [pid])
    _write_pair(sid, pid)
    _write_comparison_result(sid, pid, [_change("chg_a", title="A")])

    items = {it["raw_id"]: it for it in _built_items(sid, pid)}
    v2_key = er.make_key(pid, items["chg_a"]["id"])
    # У v2-находки уже стоит ручное решение (accepted)
    er.apply_batch(sid, decisions=[{"item_id": v2_key, "decision": "accepted",
                                    "rejection_reason": "ручное"}])
    # Источник для той же находки противоречит (rejected)
    _seed_source_decisions(sid, pid, [("chg_a", "rejected", "из расхождений")])

    rep = rt.transfer_session(sid, use_claude=False)
    assert rep["totals"]["conflicts"] == 1

    store = er.load(sid)["decisions"]
    # Ручное решение НЕ перезаписано
    assert store[v2_key]["decision"] == "accepted"
    assert store[v2_key]["rejection_reason"] == "ручное"
    assert store[v2_key]["conflict"] is True


def test_consistent_existing_is_not_a_conflict():
    from backend.app.services.stage_comparison import review_transfer as rt
    from backend.app.services.stage_comparison import expert_review as er

    sid, pid = "s_consistent", "p1"
    _write_session(sid, [pid])
    _write_pair(sid, pid)
    _write_comparison_result(sid, pid, [_change("chg_a", title="A")])
    items = {it["raw_id"]: it for it in _built_items(sid, pid)}
    v2_key = er.make_key(pid, items["chg_a"]["id"])
    er.apply_batch(sid, decisions=[{"item_id": v2_key, "decision": "rejected",
                                    "rejection_reason": "ручное"}])
    _seed_source_decisions(sid, pid, [("chg_a", "rejected", "из расхождений")])

    rep = rt.transfer_session(sid, use_claude=False)
    assert rep["totals"]["conflicts"] == 0
    assert rep["totals"]["consistent_existing"] == 1
    store = er.load(sid)["decisions"]
    assert store[v2_key].get("conflict") is not True


# ─── Семантический перенос через (фейковый) Claude ───────────────────────


class _FakeProvider:
    """Сопоставляет каждый orphan с ПЕРВОЙ текущей v2-находкой, conf из env."""
    def __init__(self, confidence=0.9):
        self._conf = confidence

    def check_availability(self):
        return True, None

    def invoke(self, *, system_prompt, user_prompt, model, timeout_sec, work_dir=None):
        from backend.app.services.stage_comparison.text_llm_provider import ProviderResult
        payload = json.loads(user_prompt)
        sources = payload["old_findings"]
        v2 = payload["current_v2_findings"]
        target = v2[0]["v2_id"] if v2 else None
        matches = [{"source_id": s["source_id"], "v2_id": target,
                    "confidence": self._conf, "reason": "same"} for s in sources]
        body = json.dumps({"matches": matches}, ensure_ascii=False)
        # claude -p --output-format json → {"result": "<inner>"}
        return ProviderResult(status="done", raw_response=json.dumps({"result": body}))


def _setup_residue_session(sid="s_sem", pid="p1"):
    """Один v2-item (chg_keep) + один orphan-источник (chg_gone, нет в V2)."""
    _write_session(sid, [pid])
    _write_pair(sid, pid)
    _write_comparison_result(sid, pid, [_change("chg_keep", title="Keep")])
    _seed_source_decisions(sid, pid, [
        ("chg_keep", "accepted", "точное"),     # exact
        ("chg_gone", "rejected", "orphan"),     # residue → semantic
    ])
    return sid, pid


def test_semantic_transfer_high_confidence(monkeypatch):
    from backend.app.services.stage_comparison import review_transfer as rt
    from backend.app.services.stage_comparison import expert_review as er
    import backend.app.services.stage_comparison.text_llm_provider as tlp

    sid, pid = _setup_residue_session("s_sem_hi")
    monkeypatch.setattr(tlp, "ClaudeCodeProvider", lambda: _FakeProvider(confidence=0.95))

    rep = rt.transfer_session(sid, use_claude=True)
    assert rep["totals"]["exact"] == 1
    assert rep["totals"]["semantic"] == 1
    # orphan мапится на тот же v2-item, что и exact (chg_keep=accepted),
    # вердикт orphan'а rejected → конфликт (разные вердикты на один ключ).
    assert rep["totals"]["conflicts"] == 1
    store = er.load(sid)["decisions"]
    items = {it["raw_id"]: it for it in _built_items(sid, pid)}
    v2_key = er.make_key(pid, items["chg_keep"]["id"])
    # exact-решение осталось accepted, помечен конфликт от семантического orphan'а
    assert store[v2_key]["decision"] == "accepted"
    assert store[v2_key]["conflict"] is True


def test_semantic_low_confidence_sets_needs_review(monkeypatch):
    from backend.app.services.stage_comparison import review_transfer as rt
    from backend.app.services.stage_comparison import expert_review as er
    import backend.app.services.stage_comparison.text_llm_provider as tlp

    sid, pid = "s_sem_lo", "p1"
    _write_session(sid, [pid])
    _write_pair(sid, pid)
    # Два v2-item'а; orphan мапится на первый, у которого НЕТ exact.
    _write_comparison_result(sid, pid, [
        _change("chg_fresh", title="Fresh"),     # без источника → пустой ключ
        _change("chg_keep", title="Keep"),
    ])
    _seed_source_decisions(sid, pid, [
        ("chg_keep", "accepted", "точное"),
        ("chg_gone", "rejected", "orphan"),       # residue
    ])
    # confidence 0.6 < порога 0.75 → перенос с needs_review
    monkeypatch.setattr(tlp, "ClaudeCodeProvider", lambda: _FakeProvider(confidence=0.6))

    rep = rt.transfer_session(sid, use_claude=True)
    assert rep["totals"]["semantic"] == 1
    assert rep["totals"]["needs_review"] == 1
    store = er.load(sid)["decisions"]
    items = {it["raw_id"]: it for it in _built_items(sid, pid)}
    # orphan ушёл на первый v2-item (chg_fresh), у которого решения не было
    fresh_key = er.make_key(pid, items["chg_fresh"]["id"])
    assert store[fresh_key]["decision"] == "rejected"
    assert store[fresh_key]["needs_review"] is True
    assert store[fresh_key]["transfer_method"] == "semantic"


def test_failsoft_when_claude_unavailable(monkeypatch):
    from backend.app.services.stage_comparison import review_transfer as rt
    import backend.app.services.stage_comparison.text_llm_provider as tlp

    sid, pid = _setup_residue_session("s_failsoft")

    class _Unavailable:
        def check_availability(self):
            return False, "claude_cli_not_found"

    monkeypatch.setattr(tlp, "ClaudeCodeProvider", lambda: _Unavailable())
    rep = rt.transfer_session(sid, use_claude=True)
    assert rep["claude_available"] is False
    assert rep["totals"]["exact"] == 1            # точный всё равно перенесён
    assert rep["totals"]["semantic"] == 0
    assert rep["totals"]["unmatched_source"] == 1  # orphan не сопоставлен


# ─── Endpoint (router-level) ─────────────────────────────────────────────


def test_transfer_endpoint_runs_exact_transfer():
    """POST .../v2-review/transfer (router-level): без Claude переносит точные
    совпадения по raw_id и возвращает отчёт. Проверяет регистрацию роута."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.app.api.routers import stage_comparison as router_mod
    from backend.app.services.stage_comparison import expert_review as er

    sid, pid = "s_endpoint", "p1"
    _write_session(sid, [pid])
    _write_pair(sid, pid)
    _write_comparison_result(sid, pid, [
        _change("chg_a", title="A"), _change("chg_b", title="B"),
    ])
    _seed_source_decisions(sid, pid, [
        ("chg_a", "rejected", "оформительское"),
        ("chg_b", "accepted", ""),
    ])

    app = FastAPI()
    app.include_router(router_mod.router)
    client = TestClient(app)
    base = f"/api/stage-comparison/sessions/{sid}/v2-review/transfer"

    r = client.post(base, json={"use_claude": False})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["totals"]["exact"] == 2
    assert body["totals"]["applied"] == 2
    assert body["totals"]["conflicts"] == 0
    # решение реально легло на v2-скоупленный ключ
    items = {it["raw_id"]: it for it in _built_items(sid, pid)}
    store = er.load(sid)["decisions"]
    assert store[er.make_key(pid, items["chg_a"]["id"])]["decision"] == "rejected"


def test_transfer_endpoint_unknown_session_404():
    """Несуществующая сессия → 404 (KeyError → HTTPException)."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.app.api.routers import stage_comparison as router_mod

    app = FastAPI()
    app.include_router(router_mod.router)
    client = TestClient(app)
    r = client.post("/api/stage-comparison/sessions/nope_missing/v2-review/transfer",
                    json={"use_claude": False})
    assert r.status_code == 404, r.text
