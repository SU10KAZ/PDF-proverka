"""Тесты пакетного авто-сопоставления листов по штампам + safe auto-apply.

Покрывает:
  * stamp_auto_apply.should_auto_apply_stamp_match (политика);
  * build_auto_apply_items (display-only поля не попадают в alignment);
  * store.apply_safe_stamp_alignment_for_pair (existing-guard, overwrite,
    multipart-персистенция, идемпотентность);
  * auto_match_jobs (batch job: прогресс, fail-soft, artifact).

Сеть/Qwen/crop_url НЕ задействованы (offline-матчинг по именам листов;
use_llm=False в тестах, чтобы исключить subprocess).
"""
from __future__ import annotations

import asyncio

import pytest

from backend.app.services.stage_comparison import stamp_auto_apply as aa
from backend.app.services.stage_comparison import store as store_mod
from backend.app.services.stage_comparison import auto_match_jobs as amj


# ─── fixtures ───────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _tmp_comparison_root(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    (tmp_path / "comparison").mkdir(exist_ok=True)
    # Большой page count, чтобы alignment.validate не обрезал страницы по PDF.
    monkeypatch.setattr(store_mod, "_pdf_page_count", lambda p: 60)


def _md(pages):
    out = []
    for pn, sno, snm in pages:
        out.append(f"## СТРАНИЦА {pn}")
        if sno is not None:
            out.append(f"**Лист:** {sno}")
        if snm is not None:
            out.append(f"**Наименование листа:** {snm}")
        out.append(f"текст {pn}")
        out.append("")
    return "\n".join(out)


def _bind_pair(monkeypatch, tmp_path, pid, left_pages, right_pages):
    lp = tmp_path / f"{pid}_left.md"
    rp = tmp_path / f"{pid}_right.md"
    lp.write_text(_md(left_pages), encoding="utf-8")
    rp.write_text(_md(right_pages), encoding="utf-8")
    pair = {"id": pid,
            "left": {"md_path": str(lp), "pdf_path": str(tmp_path / "l.pdf"),
                     "result_json_path": None},
            "right": {"md_path": str(rp), "pdf_path": str(tmp_path / "r.pdf"),
                      "result_json_path": None}}
    return pair


# ═══ should_auto_apply_stamp_match ════════════════════════════════════════

def _item(mt, *, score=0.99, conf=None, risk=None, pos=None, match=True):
    return {"match": match, "match_type": mt, "score": score,
            "confidence": conf, "risk_flags": list(risk or []),
            "positive_evidence": list(pos or [])}


def test_auto_apply_exact_canonical_multipart():
    for mt in ("exact_name", "exact_canonical_name", "exact_multipart_group"):
        ok, _ = aa.should_auto_apply_stamp_match(_item(mt))
        assert ok, mt
    ok, _ = aa.should_auto_apply_stamp_match(_item("multipart_group"))
    assert ok


def test_auto_apply_low_margin_blocked():
    ok, reason = aa.should_auto_apply_stamp_match(
        _item("fuzzy_structural", score=0.95, risk=["low_margin"]))
    assert not ok and reason == "low_margin"


def test_auto_apply_text_layer_blocked_by_default(monkeypatch):
    monkeypatch.delenv("STAGE_COMPARISON_STAMP_AUTO_APPLY_TEXT_LAYER", raising=False)
    ok, reason = aa.should_auto_apply_stamp_match(_item("text_layer", score=1.0))
    assert not ok and reason == "text_layer"
    ok2, _ = aa.should_auto_apply_stamp_match(
        _item("fuzzy_name", score=0.95, risk=["text_layer_fallback"]))
    assert not ok2


def test_auto_apply_text_layer_allowed_with_flag(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_STAMP_AUTO_APPLY_TEXT_LAYER", "true")
    ok, _ = aa.should_auto_apply_stamp_match(_item("text_layer", score=1.0))
    assert ok


def test_auto_apply_fuzzy_threshold():
    ok, _ = aa.should_auto_apply_stamp_match(_item("fuzzy_structural", score=0.85))
    assert ok
    ok2, reason = aa.should_auto_apply_stamp_match(_item("fuzzy_structural", score=0.6))
    assert not ok2 and reason == "low_score"


def test_auto_apply_llm_requires_high_confidence():
    ok, _ = aa.should_auto_apply_stamp_match(
        _item("llm_semantic", score=0.9, conf=0.9, risk=["llm_semantic"]))
    assert ok
    ok2, reason = aa.should_auto_apply_stamp_match(
        _item("llm_semantic", score=0.7, conf=0.7, risk=["llm_semantic"]))
    assert not ok2 and reason == "llm_low_confidence"


def test_auto_apply_duplicate_without_strong_evidence_blocked():
    ok, reason = aa.should_auto_apply_stamp_match(
        _item("fuzzy_name", score=0.95, risk=["duplicate_sheet_name"]))
    assert not ok and reason == "duplicate_sheet_name"
    # с сильным признаком — допустимо
    ok2, _ = aa.should_auto_apply_stamp_match(
        _item("fuzzy_name", score=0.95, risk=["duplicate_sheet_name"],
              pos=["оборуд:вру-1"]))
    assert ok2


# ═══ build_auto_apply_items ═══════════════════════════════════════════════

def test_build_items_strips_display_only_fields():
    suggested = [
        {"match": True, "match_type": "exact_name", "left_page": 1, "right_page": 1,
         "score": 1.0, "confidence": 1.0, "risk_flags": [], "reason": "x",
         "positive_evidence": ["a"], "match_diag": {"z": 1}},
        {"match": False, "match_type": "right_only", "left_page": None, "right_page": 2},
    ]
    built = aa.build_auto_apply_items(suggested)
    assert built["applied"] == 1
    for it in built["items"]:
        assert set(it.keys()) == {"slot", "left_page", "right_page", "mode", "note"}


def test_build_items_splits_unsafe_into_one_sided():
    suggested = [
        {"match": True, "match_type": "fuzzy_name", "left_page": 1, "right_page": 5,
         "score": 0.4, "risk_flags": []},  # ниже порога → review
    ]
    built = aa.build_auto_apply_items(suggested)
    assert built["applied"] == 0 and built["review"] == 1
    pages = {(it["left_page"], it["right_page"]) for it in built["items"]}
    assert pages == {(1, None), (None, 5)}  # расцеплено на два слота


# ═══ store.apply_safe_stamp_alignment_for_pair ════════════════════════════

def test_apply_safe_exact_applied(monkeypatch, tmp_path):
    pair = _bind_pair(monkeypatch, tmp_path, "p1",
                      [(1, "1", "Содержание тома"), (2, "2", "Схема ГРЩ")],
                      [(1, "1", "Содержание тома"), (2, "2", "Лист X"),
                       (3, "3", "Схема ГРЩ")])
    monkeypatch.setattr(store_mod, "_find_pair_meta",
                        lambda s, p: pair if p == "p1" else None)
    res = store_mod.apply_safe_stamp_alignment_for_pair("s1", "p1", use_llm=False)
    assert res["status"] == "done"
    assert res["applied"] >= 2  # содержание + ГРЩ
    align = store_mod.get_alignment("s1", "p1")["alignment"]["items"]
    pairs = {(it["left_page"], it["right_page"]) for it in align
             if it["left_page"] and it["right_page"]}
    assert (2, 3) in pairs  # ГРЩ уехала на стр.3


def test_apply_safe_skips_existing_manual(monkeypatch, tmp_path):
    pair = _bind_pair(monkeypatch, tmp_path, "p1",
                      [(1, "1", "Схема ГРЩ")], [(1, "1", "Схема ГРЩ")])
    monkeypatch.setattr(store_mod, "_find_pair_meta",
                        lambda s, p: pair if p == "p1" else None)
    # первый прогон применяет (пишет manual)
    r1 = store_mod.apply_safe_stamp_alignment_for_pair("s1", "p1", use_llm=False)
    assert r1["status"] == "done"
    # второй — пропуск, ручное не трогаем
    r2 = store_mod.apply_safe_stamp_alignment_for_pair("s1", "p1", use_llm=False)
    assert r2["status"] == "skipped_existing_alignment"
    # overwrite=True → снова применяет
    r3 = store_mod.apply_safe_stamp_alignment_for_pair(
        "s1", "p1", use_llm=False, overwrite_existing=True)
    assert r3["status"] == "done"


def test_apply_safe_multipart_one_to_three_persisted(monkeypatch, tmp_path):
    pair = _bind_pair(monkeypatch, tmp_path, "p1",
                      [(1, "1", "Чертеж 1")],
                      [(10, "10", "Чертеж 1 (начало)"),
                       (11, "11", "Чертеж 1 (продолжение)"),
                       (12, "12", "Чертеж 1 (конец)")])
    monkeypatch.setattr(store_mod, "_find_pair_meta",
                        lambda s, p: pair if p == "p1" else None)
    res = store_mod.apply_safe_stamp_alignment_for_pair("s1", "p1", use_llm=False)
    assert res["status"] == "done"
    align = store_mod.get_alignment("s1", "p1")["alignment"]["items"]
    seq = [(it["left_page"], it["right_page"]) for it in align]
    assert (1, 10) in seq
    assert (None, 11) in seq and (None, 12) in seq
    left_used = [lp for lp, _ in seq if lp is not None]
    assert left_used.count(1) == 1  # страница 1 не дублируется


def test_apply_safe_multipart_three_to_one_persisted(monkeypatch, tmp_path):
    pair = _bind_pair(monkeypatch, tmp_path, "p1",
                      [(1, "1", "Чертеж 1 (начало)"),
                       (2, "2", "Чертеж 1 (продолжение)"),
                       (3, "3", "Чертеж 1 (конец)")],
                      [(10, "10", "Чертеж 1")])
    monkeypatch.setattr(store_mod, "_find_pair_meta",
                        lambda s, p: pair if p == "p1" else None)
    store_mod.apply_safe_stamp_alignment_for_pair("s1", "p1", use_llm=False)
    align = store_mod.get_alignment("s1", "p1")["alignment"]["items"]
    seq = [(it["left_page"], it["right_page"]) for it in align]
    assert (1, 10) in seq
    assert (2, None) in seq and (3, None) in seq


# ═══ auto_match_jobs (batch) ══════════════════════════════════════════════

def _bind_session(monkeypatch, tmp_path, pairs: dict):
    by_id = pairs
    monkeypatch.setattr(store_mod, "_find_pair_meta",
                        lambda s, p: by_id.get(p))
    session = {"id": "s1", "pairs": list(by_id.values())}
    monkeypatch.setattr(store_mod, "get_session",
                        lambda s: session if s == "s1" else None)


def test_batch_job_processes_all_pairs(monkeypatch, tmp_path):
    pairs = {
        "p1": _bind_pair(monkeypatch, tmp_path, "p1",
                         [(1, "1", "Схема ГРЩ")], [(1, "1", "Схема ГРЩ")]),
        "p2": _bind_pair(monkeypatch, tmp_path, "p2",
                         [(1, "1", "Содержание тома")], [(1, "1", "Содержание тома")]),
    }
    _bind_session(monkeypatch, tmp_path, pairs)

    job = amj.create_job("s1", use_llm=False, overwrite_existing=False)
    assert job["status"] == "queued"
    assert job["total_pairs"] == 2
    done = asyncio.run(amj.run_job("s1", job["id"]))
    assert done["status"] == "finished"
    assert done["processed_pairs"] == 2
    assert done["summary"]["applied_pairs"] >= 2
    assert done["summary"]["failed_pairs"] == 0
    # artifact записан
    last = amj.read_last_run("s1")
    assert last is not None and last["status"] == "finished"
    assert len(last["pairs"]) == 2


def test_batch_job_failsoft_on_one_pair(monkeypatch, tmp_path):
    pairs = {
        "good": _bind_pair(monkeypatch, tmp_path, "good",
                           [(1, "1", "Схема ГРЩ")], [(1, "1", "Схема ГРЩ")]),
        "bad": _bind_pair(monkeypatch, tmp_path, "bad",
                          [(1, "1", "X")], [(1, "1", "X")]),
    }
    _bind_session(monkeypatch, tmp_path, pairs)

    real_apply = store_mod.apply_safe_stamp_alignment_for_pair

    def _apply(sid, pid, **kw):
        if pid == "bad":
            raise RuntimeError("boom")
        return real_apply(sid, pid, **kw)

    monkeypatch.setattr(store_mod, "apply_safe_stamp_alignment_for_pair", _apply)

    job = amj.create_job("s1", use_llm=False)
    done = asyncio.run(amj.run_job("s1", job["id"]))
    assert done["status"] == "finished"          # не упал целиком
    assert done["processed_pairs"] == 2
    assert done["summary"]["failed_pairs"] == 1
    bad = next(it for it in done["items"] if it["pair_id"] == "bad")
    assert bad["status"] == "error" and bad["errors"]


def test_batch_job_respects_existing_alignment(monkeypatch, tmp_path):
    pairs = {
        "p1": _bind_pair(monkeypatch, tmp_path, "p1",
                         [(1, "1", "Схема ГРЩ")], [(1, "1", "Схема ГРЩ")]),
    }
    _bind_session(monkeypatch, tmp_path, pairs)
    # предварительно «ручное» выравнивание
    store_mod.save_alignment("s1", "p1",
                             [{"left_page": 1, "right_page": 1, "mode": "manual"}],
                             force=True)
    job = amj.create_job("s1", use_llm=False, overwrite_existing=False)
    done = asyncio.run(amj.run_job("s1", job["id"]))
    assert done["summary"]["skipped_existing_alignment"] == 1
    it = done["items"][0]
    assert it["status"] == "skipped_existing_alignment"


# ═══ endpoints (TestClient smoke) ═════════════════════════════════════════

def _build_app():
    from fastapi import FastAPI
    from backend.app.api.routers import stage_comparison as router_mod
    app = FastAPI()
    app.include_router(router_mod.router)
    return app


def test_endpoint_starts_job_and_progress(monkeypatch, tmp_path):
    from fastapi.testclient import TestClient
    pairs = {
        "p1": _bind_pair(monkeypatch, tmp_path, "p1",
                         [(1, "1", "Схема ГРЩ")], [(1, "1", "Схема ГРЩ")]),
    }
    _bind_session(monkeypatch, tmp_path, pairs)
    # не запускаем реальный background task в sync-TestClient (нет running loop)
    monkeypatch.setattr(amj, "start_job_in_background", lambda s, j: j)

    client = TestClient(_build_app())
    r = client.post("/api/stage-comparison/sessions/s1/page-alignment/auto-match",
                    json={"use_llm": False})
    assert r.status_code == 200
    job = r.json()
    assert job["total_pairs"] == 1
    assert job["status"] in ("queued", "running")

    r2 = client.get(
        f"/api/stage-comparison/sessions/s1/page-alignment/auto-match/{job['id']}")
    assert r2.status_code == 200
    assert r2.json()["id"] == job["id"]


def test_endpoint_session_not_found():
    from fastapi.testclient import TestClient
    client = TestClient(_build_app())
    r = client.post("/api/stage-comparison/sessions/nope/page-alignment/auto-match",
                    json={"use_llm": False})
    assert r.status_code == 404
