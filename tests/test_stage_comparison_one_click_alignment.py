# -*- coding: utf-8 -*-
"""Тесты one-click авто-сопоставления листов (page-alignment).

Покрывает:
  * classify_for_one_click — high/medium/low confidence, ambiguous, multipart,
    positional, one-sided;
  * store.auto_match_apply_pair — apply / dry_run / skip-existing / no-safe /
    replaced_existing / stale_block_links_marked / backup;
  * endpoint POST .../page-alignment/auto-match-apply (router-only, mocked);
  * связи блоков (links.json) НЕ удаляются — только stale-пометка через save;
  * endpoint не запускает Qwen/Opus/LLM.

Всё на mocks — без живого comparison/sessions, без PDF/LLM.
"""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.services.stage_comparison import stamp_auto_apply as sa
from backend.app.services.stage_comparison import store as store_mod
from backend.app.api.routers import stage_comparison as sc_router

_BASE = "/api/stage-comparison"


# ─── helpers ────────────────────────────────────────────────────────────────


def _matched(mt, lp, rp, *, score=0.99, conf=None, risk=None, reason="",
             pos_ev=None, ln="Лист L", rn="Лист R"):
    return {
        "match": True, "match_type": mt, "left_page": lp, "right_page": rp,
        "score": score, "confidence": conf if conf is not None else score,
        "risk_flags": list(risk or []), "reason": reason,
        "positive_evidence": list(pos_ev or []),
        "left_sheet_name": ln, "right_sheet_name": rn, "slot": 1,
    }


def _one_sided(side, page):
    return {
        "match": False, "match_type": f"{side}_only",
        "left_page": page if side == "left" else None,
        "right_page": page if side == "right" else None,
        "left_sheet_name": "Лист L" if side == "left" else "",
        "right_sheet_name": "Лист R" if side == "right" else "",
        "score": 0.0, "reason": "", "risk_flags": [],
    }


def _positional(lp, rp):
    return {"match": False, "match_type": "positional_alignment",
            "left_page": lp, "right_page": rp, "score": 0.0, "confidence": 0.0,
            "reason": "", "risk_flags": [], "left_sheet_name": "", "right_sheet_name": ""}


# ─── classify_for_one_click ──────────────────────────────────────────────────


def test_classify_high_confidence_exact_applied():
    r = sa.classify_for_one_click([_matched("exact_name", 5, 3, score=1.0)])
    assert len(r["applied"]) == 1 and not r["needs_review"]
    a = r["applied"][0]
    assert a["status"] == "applied" and a["slot"] == 1
    assert a["left_page"] == 5 and a["right_page"] == 3 and a["confidence"] == 1.0


def test_classify_fuzzy_low_score_needs_review():
    r = sa.classify_for_one_click([_matched("fuzzy_name", 5, 3, score=0.6)])
    assert not r["applied"] and len(r["needs_review"]) == 1
    nr = r["needs_review"][0]
    assert nr["status"] == "needs_review" and nr["review_code"] == "low_score"
    assert nr["reason"]  # человекочитаемая причина проставлена


def test_classify_fuzzy_high_score_applied():
    r = sa.classify_for_one_click([_matched("fuzzy_name", 5, 3, score=0.9)])
    assert len(r["applied"]) == 1 and not r["needs_review"]


def test_classify_ambiguous_low_margin_needs_review():
    r = sa.classify_for_one_click([_matched("exact_name", 5, 3, risk=["low_margin"])])
    assert not r["applied"]
    assert r["needs_review"][0]["review_code"] == "low_margin"


def test_classify_duplicate_name_without_strong_evidence_review():
    r = sa.classify_for_one_click([_matched("fuzzy_name", 5, 3, score=0.9,
                                            risk=["duplicate_sheet_name"])])
    assert not r["applied"]
    assert r["needs_review"][0]["review_code"] == "duplicate_sheet_name"


def test_classify_duplicate_name_with_strong_evidence_applied():
    r = sa.classify_for_one_click([_matched("fuzzy_name", 5, 3, score=0.9,
                                            risk=["duplicate_sheet_name"],
                                            pos_ev=["оборуд:вру-1"])])
    assert len(r["applied"]) == 1


def test_classify_multipart_applied():
    r = sa.classify_for_one_click([_matched("exact_multipart_group", 5, 3)])
    assert len(r["applied"]) == 1
    r2 = sa.classify_for_one_click([_matched("multipart_group", 6, 4)])
    assert len(r2["applied"]) == 1


def test_classify_equipment_canonical_applied():
    r = sa.classify_for_one_click([_matched("equipment_canonical_match", 5, 3)])
    assert len(r["applied"]) == 1


def test_classify_positional_needs_review():
    r = sa.classify_for_one_click([_positional(7, 4)])
    assert r["positional_alignment"] == 1
    assert r["needs_review"][0]["review_code"] == "positional_alignment"
    assert "unconfirmed_alignment" in r["needs_review"][0]["risk_flags"]


def test_classify_one_sided_unmatched():
    r = sa.classify_for_one_click([_one_sided("left", 9), _one_sided("right", 2)])
    assert [u["left_page"] for u in r["unmatched_old"]] == [9]
    assert [u["right_page"] for u in r["unmatched_new"]] == [2]


# ─── auto_match_apply_pair ───────────────────────────────────────────────────


@pytest.fixture
def patched_store(monkeypatch):
    """Замокать I/O store, оставив реальные classify/build."""
    calls = {"save": []}
    monkeypatch.setattr(store_mod, "_find_pair_meta", lambda s, p: {"left": {}, "right": {}})
    monkeypatch.setattr(store_mod, "_load_alignment_raw", lambda s, p: {
        "items": [{"left_page": 1, "right_page": 1, "mode": "manual"}]})  # replaced=1
    monkeypatch.setattr(store_mod, "has_manual_alignment", lambda s, p: False)
    monkeypatch.setattr(store_mod, "_backup_page_alignment", lambda s, p: "/tmp/bkp.json")

    def _save(s, p, items, *, force=False):
        calls["save"].append({"items": items, "force": force})
        return {"ok": True, "links_resync": {"updated": 5, "stale_auto": 2, "cross_page_manual": 1}}
    monkeypatch.setattr(store_mod, "save_alignment", _save)
    return calls


def _suggest(items, lc=10, rc=8):
    return {"method": "stamp", "suggested_items": items, "confidence": 0.9,
            "warnings": [], "matched_count": sum(1 for i in items if i.get("match")),
            "left_page_count": lc, "right_page_count": rc}


def test_auto_match_apply_applies_and_reports(patched_store, monkeypatch):
    items = [_matched("exact_name", 5, 3, score=1.0),
             _matched("fuzzy_name", 6, 4, score=0.6),       # → needs_review
             _one_sided("left", 9), _one_sided("right", 2)]
    monkeypatch.setattr(store_mod, "suggest_alignment_by_stamp",
                        lambda s, p, *, use_llm=False: _suggest(items))

    rep = store_mod.auto_match_apply_pair("sess", "pair")
    assert rep["status"] == "completed" and rep["applied_to_disk"] is True
    s = rep["summary"]
    assert s["auto_applied"] == 1 and s["needs_review"] == 1
    assert s["unmatched_old"] == 1 and s["unmatched_new"] == 1
    assert s["old_pages_total"] == 10 and s["new_pages_total"] == 8
    assert s["replaced_existing"] == 1
    assert s["stale_block_links_marked"] == 3   # stale_auto 2 + cross_page_manual 1
    assert rep["backup_path"] == "/tmp/bkp.json"
    assert len(patched_store["save"]) == 1 and patched_store["save"][0]["force"] is True


def test_auto_match_apply_dry_run_does_not_save(patched_store, monkeypatch):
    items = [_matched("exact_name", 5, 3, score=1.0)]
    monkeypatch.setattr(store_mod, "suggest_alignment_by_stamp",
                        lambda s, p, *, use_llm=False: _suggest(items))
    rep = store_mod.auto_match_apply_pair("sess", "pair", dry_run=True)
    assert rep["status"] == "dry_run" and rep["applied_to_disk"] is False
    assert rep["summary"]["auto_applied"] == 1          # preview показывает, что применилось бы
    assert patched_store["save"] == []                   # ничего не сохранено


def test_auto_match_apply_skips_existing_manual(patched_store, monkeypatch):
    monkeypatch.setattr(store_mod, "has_manual_alignment", lambda s, p: True)
    monkeypatch.setattr(store_mod, "suggest_alignment_by_stamp",
                        lambda s, p, *, use_llm=False: _suggest([_matched("exact_name", 5, 3)]))
    rep = store_mod.auto_match_apply_pair("sess", "pair")  # overwrite_existing=False
    assert rep["status"] == "skipped_existing_alignment"
    assert patched_store["save"] == []
    assert "manual_alignment_exists_not_overwritten" in rep["warnings"]


def test_auto_match_apply_no_safe_matches_does_not_touch_map(patched_store, monkeypatch):
    items = [_matched("fuzzy_name", 5, 3, score=0.6)]   # только review
    monkeypatch.setattr(store_mod, "suggest_alignment_by_stamp",
                        lambda s, p, *, use_llm=False: _suggest(items))
    rep = store_mod.auto_match_apply_pair("sess", "pair")
    assert rep["status"] == "needs_review"
    assert patched_store["save"] == []                   # карту не трогаем


def test_auto_match_apply_missing_pair_raises(monkeypatch):
    monkeypatch.setattr(store_mod, "_find_pair_meta", lambda s, p: None)
    with pytest.raises(KeyError):
        store_mod.auto_match_apply_pair("sess", "missing")


def test_links_not_deleted_only_stale_marked(patched_store, monkeypatch):
    # auto_match_apply_pair мутирует только через save_alignment (→ _resync помечает
    # stale, НЕ удаляет). Проверяем, что путь удаления связей не вызывается.
    deleted = {"n": 0}
    monkeypatch.setattr(store_mod, "delete_link",
                        lambda *a, **k: deleted.__setitem__("n", deleted["n"] + 1))
    if hasattr(store_mod, "_save_links"):
        monkeypatch.setattr(store_mod, "_save_links",
                            lambda *a, **k: deleted.__setitem__("n", deleted["n"] + 1))
    monkeypatch.setattr(store_mod, "suggest_alignment_by_stamp",
                        lambda s, p, *, use_llm=False: _suggest([_matched("exact_name", 5, 3)]))
    rep = store_mod.auto_match_apply_pair("sess", "pair")
    assert rep["applied_to_disk"] is True
    assert rep["summary"]["stale_block_links_marked"] == 3
    assert deleted["n"] == 0                              # связи НЕ удалялись


# ─── endpoint (router-only, mocked) ──────────────────────────────────────────


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(sc_router.router)
    return TestClient(app)


def test_endpoint_calls_service_and_returns_report(client, monkeypatch):
    seen = {}

    def _fake(session_id, pair_id, *, use_llm, overwrite_existing, dry_run):
        seen.update(dict(session_id=session_id, pair_id=pair_id, use_llm=use_llm,
                         overwrite_existing=overwrite_existing, dry_run=dry_run))
        return {"session_id": session_id, "pair_id": pair_id, "status": "completed",
                "summary": {"auto_applied": 2, "needs_review": 1}}
    monkeypatch.setattr(sc_router.store, "auto_match_apply_pair", _fake)

    r = client.post(f"{_BASE}/sessions/S1/pairs/P1/page-alignment/auto-match-apply",
                    json={"dry_run": True, "use_llm": False})
    assert r.status_code == 200
    assert r.json()["summary"]["auto_applied"] == 2
    assert seen == dict(session_id="S1", pair_id="P1", use_llm=False,
                        overwrite_existing=False, dry_run=True)


def test_endpoint_defaults_use_llm_false(client, monkeypatch):
    seen = {}
    monkeypatch.setattr(sc_router.store, "auto_match_apply_pair",
                        lambda s, p, *, use_llm, overwrite_existing, dry_run:
                        seen.update(use_llm=use_llm) or {"status": "completed", "summary": {}})
    client.post(f"{_BASE}/sessions/S/pairs/P/page-alignment/auto-match-apply", json={})
    assert seen["use_llm"] is False           # ИИ-доматчинг по умолчанию ВЫКЛ


def test_endpoint_missing_pair_404(client, monkeypatch):
    def _raise(*a, **k):
        raise KeyError("pair_not_found")
    monkeypatch.setattr(sc_router.store, "auto_match_apply_pair", _raise)
    r = client.post(f"{_BASE}/sessions/S/pairs/NOPE/page-alignment/auto-match-apply", json={})
    assert r.status_code == 404


# ─── frontend static checks (handler/endpoint/UI summary + lists) ────────────


def _repo_root():
    from pathlib import Path
    return Path(__file__).resolve().parent.parent


def test_frontend_handler_calls_correct_endpoint():
    app_js = (_repo_root() / "frontend/static/js/app.js").read_text(encoding="utf-8")
    assert "scAutoMatchApplySheets" in app_js
    assert "page-alignment/auto-match-apply" in app_js
    # handler зарегистрирован в setup-return (доступен шаблону)
    assert "scAutoMatchApplyResult, scAutoMatchApplyError, scAutoMatchApplySheets" in app_js


def test_frontend_button_present():
    html = (_repo_root() / "frontend/index.html").read_text(encoding="utf-8")
    assert "scAutoMatchApplySheets(false)" in html
    assert "🪄 Сопоставить и применить" in html


def test_frontend_shows_summary_counts():
    html = (_repo_root() / "frontend/index.html").read_text(encoding="utf-8")
    for field in ("summary.auto_applied", "summary.needs_review",
                  "summary.unmatched_old", "summary.unmatched_new",
                  "summary.stale_block_links_marked"):
        assert f"scAutoMatchApplyResult.{field}" in html, f"missing UI: {field}"


def test_frontend_shows_applied_review_unmatched_lists():
    html = (_repo_root() / "frontend/index.html").read_text(encoding="utf-8")
    assert "scAutoMatchApplyResult.applied" in html
    assert "scAutoMatchApplyResult.needs_review" in html
    assert "scAutoMatchApplyResult.unmatched_old" in html
    assert "scAutoMatchApplyResult.unmatched_new" in html


def test_endpoint_does_not_import_qwen_opus():
    import ast
    from pathlib import Path
    src = Path(store_mod.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
            for a in node.names:
                imported.add(f"{node.module or ''}.{a.name}")
        elif isinstance(node, ast.Import):
            for a in node.names:
                imported.add(a.name)
    blob = "\n".join(sorted(imported))
    for token in ("graphic_llm", "enriched_comparison", "unified_analysis", "md_image_enrichment"):
        assert token not in blob, f"unexpected import in store: {token}"
