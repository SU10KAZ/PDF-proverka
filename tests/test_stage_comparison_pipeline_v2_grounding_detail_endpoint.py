# -*- coding: utf-8 -*-
"""Тесты read-only endpoint'а Pipeline V2 graphic vision grounding detail.

GET /api/stage-comparison/pipeline-v2/{session_id}/graphic-vision-grounding
    (+ ?pair_id=&kind=&status=&item_id=&limit=&offset=)

Покрытие spec-кейсов:
  1.  отдаёт detail report из graphic_vision_grounding_report.json;
  2.  missing report → not_found (available=false), не 404/500;
  3.  битый report → error (available=false), не 500;
  4.  filter status=grounded;
  5.  filter status=rejected;
  6.  filter kind=changes;
  7.  pagination limit/offset;
  8.  limit clamp (>500 → 500);
  9.  raw full text не отдаётся;
  10. read-only: не создаёт файлов/директорий;
  11. путь endpoint'а НЕ в auth-exempt (middleware-level 401 для анонимов).

Никаких реальных LLM/network.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.fixture()
def comparison_root(tmp_path, monkeypatch):
    root = tmp_path / "comparison_grounding_detail"
    root.mkdir()
    monkeypatch.setenv("COMPARISON_ROOT", str(root))
    yield root


@pytest.fixture()
def client(comparison_root):
    from backend.app.api.routers import stage_comparison as router_mod
    app = FastAPI()
    app.include_router(router_mod.router)
    return TestClient(app)


SID = "ba413a93c5754f6c"
PID = "pf06effb7"
EP = f"/api/stage-comparison/pipeline-v2/{SID}/graphic-vision-grounding"


def _pv2_dir(root: Path) -> Path:
    d = root / "sessions" / SID / "pairs" / PID / "pipeline_v2"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _grounding_report():
    return {
        "version": 1,
        "kind": "stage_comparison_pipeline_v2_graphic_vision_grounding",
        "status": "ok",
        "summary": {
            "items_total": 1, "entities_total": 5, "entities_grounded": 2,
            "entities_weakly_grounded": 1, "entities_ungrounded": 1,
            "changes_total": 2, "changes_grounded": 1,
            "changes_weakly_grounded": 0, "changes_rejected": 1,
            "artificial_series_rejected": 0, "designator_range_rejected": 1,
            "noop_changes_rejected": 1,
        },
        "items": [{
            "item_id": "gv_L__R", "left_block_id": "L", "right_block_id": "R",
            "graphic_type": "cabinet_scheme", "vision_status": "ok",
            "left_anchors": {"block_id": "L", "available": True, "source": "full_text",
                             "ratings": ["400a"]},
            "right_anchors": {"block_id": "R", "available": True, "source": "full_text",
                              "ratings": ["200a"]},
            "grounded_entities_old": [
                {"value": "QF5 400А", "normalized": "qf5 400a", "status": "grounded",
                 "matched_values": ["400a"], "missing_values": [], "reason": "grounded"}],
            "grounded_entities_new": [
                {"value": "QF5 200А", "normalized": "qf5 200a", "status": "grounded",
                 "matched_values": ["200a"], "missing_values": [], "reason": "grounded"},
                {"value": "QF9 777А", "normalized": "qf9 777a", "status": "weakly_grounded",
                 "matched_values": [], "missing_values": ["777a"], "reason": "partial_match"},
                {"value": "QF12 999А", "normalized": "qf12 999a", "status": "ungrounded",
                 "matched_values": [], "missing_values": ["999a"], "reason": "not_found_in_anchors"}],
            "grounded_changes": [
                {"value": "QF5: 400А → 200А", "status": "grounded",
                 "old_values": ["400a"], "new_values": ["200a"], "reason": "grounded"}],
            "rejected_entities": [
                {"value": "Автоматические выключатели (QF1...QF100)",
                 "normalized": "qf1...qf100", "status": "rejected_designator_range",
                 "reason": "artificial_designator_range"}],
            "rejected_changes": [
                {"value": "ППГнг 4x2,5 → 4x2,5 (без изменений)",
                 "status": "rejected_noop", "reason": "noop_change"}],
            "artificial_series_reasons": [],
            "warnings": [],
        }],
        "warnings": [],
    }


def _gate():
    return {"version": 1, "kind": "stage_comparison_pipeline_v2_visual_equivalence_gate",
            "status": "ok", "block_pairs": [
                {"left_block_id": "L", "right_block_id": "R",
                 "left_page_number": 52, "right_page_number": 21}]}


def _seed(root: Path, report=True, gate=True, broken=False):
    d = _pv2_dir(root)
    if report:
        txt = "{not json" if broken else json.dumps(_grounding_report(), ensure_ascii=False)
        (d / "graphic_vision_grounding_report.json").write_text(txt, encoding="utf-8")
    if gate:
        (d / "visual_equivalence_gate_report.json").write_text(
            json.dumps(_gate(), ensure_ascii=False), encoding="utf-8")
    return d


# ─── 1: detail report ────────────────────────────────────────────────────────

def test_1_returns_detail_report(client, comparison_root):
    _seed(comparison_root)
    r = client.get(EP, params={"pair_id": PID})
    assert r.status_code == 200
    d = r.json()
    assert d["status"] == "ok" and d["available"] is True
    assert d["kind"] == "stage_comparison_pipeline_v2_graphic_vision_grounding_detail"
    assert d["summary"]["entities_grounded"] == 2
    # page numbers подтянулись из gate
    ch = d["flat"]["changes"][0]
    assert ch["left_page_number"] == 52 and ch["right_page_number"] == 21
    # entity card: anchor + source + fact_level
    ents = d["flat"]["entities"]
    qf5 = next(c for c in ents if c["value"] == "QF5 400А")
    assert qf5["status"] == "grounded" and qf5["use_as_fact"] is True
    assert qf5["fact_level"] == "confirmed" and qf5["anchor_source"] == "full_text"
    assert qf5["page_number"] == 52


# ─── 2: missing → not_found ──────────────────────────────────────────────────

def test_2_missing_report_not_found(client, comparison_root):
    _pv2_dir(comparison_root)   # dir exists, no report
    r = client.get(EP, params={"pair_id": PID})
    assert r.status_code == 200
    d = r.json()
    assert d["status"] == "not_found" and d["available"] is False
    assert "not found" in d["message"].lower()


# ─── 3: broken → error, not 500 ──────────────────────────────────────────────

def test_3_broken_report_error_not_500(client, comparison_root):
    _seed(comparison_root, broken=True)
    r = client.get(EP, params={"pair_id": PID})
    assert r.status_code == 200
    d = r.json()
    assert d["status"] == "error" and d["available"] is False
    assert d["warnings"]


# ─── 4-6: filters ────────────────────────────────────────────────────────────

def test_4_filter_status_grounded(client, comparison_root):
    _seed(comparison_root)
    d = client.get(EP, params={"pair_id": PID, "status": "grounded"}).json()
    statuses = {c["status"] for c in d["flat"]["entities"] + d["flat"]["changes"]}
    assert statuses == {"grounded"}
    assert not d["flat"]["rejected"]


def test_5_filter_status_rejected(client, comparison_root):
    _seed(comparison_root)
    d = client.get(EP, params={"pair_id": PID, "status": "rejected"}).json()
    rej = d["flat"]["rejected"]
    assert rej and all(c["status"].startswith("rejected_") for c in rej)
    vals = {c["value"] for c in rej}
    assert any("QF1...QF100" in v for v in vals)
    assert any("без изменений" in v for v in vals)


def test_6_filter_kind_changes(client, comparison_root):
    _seed(comparison_root)
    d = client.get(EP, params={"pair_id": PID, "kind": "changes"}).json()
    # только change-карточки (entities пустые)
    assert not d["flat"]["entities"]
    allcards = d["flat"]["changes"] + d["flat"]["rejected"]
    assert all(c["card_type"] == "change" for c in allcards)


# ─── 7-8: pagination + clamp ─────────────────────────────────────────────────

def test_7_pagination_limit_offset(client, comparison_root):
    _seed(comparison_root)
    d1 = client.get(EP, params={"pair_id": PID, "limit": 2, "offset": 0}).json()
    assert d1["pagination"]["limit"] == 2 and d1["pagination"]["returned"] == 2
    total = d1["pagination"]["total"]
    assert total == 7   # 4 entities + 1 change + 1 rej entity + 1 rej change
    d2 = client.get(EP, params={"pair_id": PID, "limit": 2, "offset": 2}).json()
    assert d2["pagination"]["offset"] == 2
    # разные страницы — разные id
    ids1 = {c["id"] for b in d1["flat"].values() for c in b}
    ids2 = {c["id"] for b in d2["flat"].values() for c in b}
    assert ids1.isdisjoint(ids2)


def test_8_limit_clamp(client, comparison_root):
    _seed(comparison_root)
    d = client.get(EP, params={"pair_id": PID, "limit": 99999}).json()
    assert d["pagination"]["limit"] == 500
    d0 = client.get(EP, params={"pair_id": PID, "limit": 0}).json()
    assert d0["pagination"]["limit"] == 1


# ─── 9: no raw full text ─────────────────────────────────────────────────────

def test_9_no_raw_full_text(client, comparison_root):
    # вставить блок с «полным текстом» в report и убедиться, что он не утёк
    rep = _grounding_report()
    big = "QF1 100А " * 2000
    rep["items"][0]["left_anchors"]["full_text"] = big  # не должно отдаваться
    rep["items"][0]["grounded_entities_old"][0]["raw_text"] = big
    d = _pv2_dir(comparison_root)
    (d / "graphic_vision_grounding_report.json").write_text(
        json.dumps(rep, ensure_ascii=False), encoding="utf-8")
    (d / "visual_equivalence_gate_report.json").write_text(
        json.dumps(_gate(), ensure_ascii=False), encoding="utf-8")
    body = client.get(EP, params={"pair_id": PID}).text
    assert "QF1 100А QF1 100А" not in body    # длинный raw не утёк
    assert len(body) < 20000


# ─── 10: read-only ───────────────────────────────────────────────────────────

def test_10_read_only_no_writes(client, comparison_root):
    d = _seed(comparison_root)
    before = sorted(p.name for p in d.iterdir())
    client.get(EP, params={"pair_id": PID})
    client.get(EP, params={"pair_id": PID, "status": "rejected"})
    after = sorted(p.name for p in d.iterdir())
    assert before == after   # ни одного нового файла


# ─── 11: endpoint не в auth-exempt (middleware защитит анонима) ──────────────

def test_11_endpoint_not_in_auth_exempt():
    # 401 обеспечивает app-level PortalAuthMiddleware; убеждаемся, что путь
    # детали НЕ попадает в exempt-набор (в отличие от /api/info, /login)
    from backend.app.core import portal_auth
    exempt = getattr(portal_auth, "EXEMPT_PATHS", None) or getattr(
        portal_auth, "_EXEMPT_PATHS", set())
    path = f"/api/stage-comparison/pipeline-v2/{SID}/graphic-vision-grounding"
    assert path not in (exempt or set())
    # и не начинается с exempt-префикса
    assert not any(path.startswith(p) for p in (exempt or set())
                   if isinstance(p, str) and p not in ("/", ""))
