# -*- coding: utf-8 -*-
"""Тесты read-only endpoint'а Pipeline V2 Exclusion Preview v2.

GET /api/stage-comparison/pipeline-v2/{session_id}/exclusion-preview-v2
    (+ ?pair_id=&classification=&severity=&limit=&offset=)

Покрытие spec-кейсов:
  1. ready report → status=ok, mark-only инварианты на всех items;
  2. missing report → not_found (available=false), не 404/500;
  3. битый report → error (available=false), не 500;
  4. filter classification работает;
  5. filter severity работает;
  6. pagination limit/offset;
  7. limit clamp (>500 → 500);
  8. no raw text / raw model data leak;
  9. read-only: не создаёт файлов/директорий;
  10. все items имеют use_as_grounded_fact=False, auto_apply=False,
      enforce_allowed=False независимо от того, что было в файле.

Никаких реальных LLM/Qwen/Opus/network.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.fixture()
def comparison_root(tmp_path, monkeypatch):
    root = tmp_path / "comparison_xp"
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
EP = f"/api/stage-comparison/pipeline-v2/{SID}/exclusion-preview-v2"
KIND = "stage_comparison_pipeline_v2_exclusion_preview"


def _pv2_dir(root: Path) -> Path:
    d = root / "sessions" / SID / "pairs" / PID / "pipeline_v2"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _xp_item(item_id, lid, rid, ll, rl, cls, sev, action, conf=0.8,
             *, risk_flags=None, manual_vision_conflict=False):
    return {
        "item_id": item_id,
        "target_type": "block_pair",
        "left_block_id": lid,
        "right_block_id": rid,
        "left_entity_label": ll,
        "right_entity_label": rl,
        "classification": cls,
        "confidence": conf,
        "severity": sev,
        "recommended_action": action,
        "source_signals": ["link_validation"],
        "reasons": [f"lv_{cls}"],
        "risk_flags": (risk_flags or []) + (
            ["manual_vision_conflict"] if manual_vision_conflict else []),
        "evidence_refs": [],
        "manual_mapping": {},
        "link_validation": {"decision": "reject_mapping", "confidence": conf},
        # эти три будут перезаписаны endpoint'ом в False (mark-only)
        "use_as_grounded_fact": True,    # нарочно True — должно стать False
        "auto_apply": True,              # нарочно True
        "enforce_allowed": True,         # нарочно True
    }


def _report():
    return {
        "version": 1,
        "kind": KIND,
        "status": "ok",
        "session_id": SID,
        "pair_id": PID,
        "created_at": "20260612_010101",
        "summary": {
            "items_total": 5,
            "candidate_exclude": 2,
            "review_only": 1,
            "keep": 1,
            "link_validation_required": 1,
            "high_confidence_exclude": 1,
            "manual_override_present": 1,
            "manual_vision_conflict": 1,
            "repeated_reject_transitions": 1,
            "auto_enforce_enabled": False,
        },
        "items": [
            _xp_item("xp1", "L1", "R1", "ВРУ-3", "ВРУ-2",
                     "candidate_exclude", "high", "exclude_from_enrichment",
                     conf=0.95, risk_flags=["repeated_reject_mapping_transition"],
                     manual_vision_conflict=True),
            _xp_item("xp2", "L2", "R2", "ЩР-4а", "ЩР-5",
                     "candidate_exclude", "medium", "exclude_from_enrichment",
                     conf=0.75),
            _xp_item("xp3", "L3", "R3", "ЩО-1", "ЩО-1",
                     "review_only", "low", "manual_review", conf=0.5),
            _xp_item("xp4", "L4", "R4", "ВРУ-1", "ВРУ-1",
                     "keep", "low", "keep_for_enrichment", conf=0.9),
            _xp_item("xp5", "L5", "R5", "ВРУ-4", "ВРУ-4а",
                     "link_validation_required", "medium", "run_link_validation",
                     conf=0.6),
        ],
        "warnings": [],
    }


def _seed_report(root: Path, broken: bool = False, custom=None):
    d = _pv2_dir(root)
    txt = ("{not json" if broken
           else json.dumps(custom or _report(), ensure_ascii=False))
    (d / "exclusion_preview_v2_report.json").write_text(txt, encoding="utf-8")
    return d


# ─── 1: ready report → ok + mark-only ───────────────────────────────────────

def test_1_ready_report_ok_and_mark_only(client, comparison_root):
    _seed_report(comparison_root)
    r = client.get(EP, params={"pair_id": PID})
    assert r.status_code == 200
    d = r.json()
    assert d["status"] == "ok"
    assert d["available"] is True
    assert d["kind"] == KIND
    assert d["source"] == "ready_report"
    assert d["summary"]["items_total"] == 5
    assert d["summary"]["candidate_exclude"] == 2
    assert d["summary"]["repeated_reject_transitions"] == 1
    assert d["summary"]["auto_enforce_enabled"] is False
    assert len(d["items"]) == 5
    # mark-only инварианты на ВСЕХ items
    for it in d["items"]:
        assert it["use_as_grounded_fact"] is False, (
            f"item {it['item_id']}: use_as_grounded_fact must be False")
        assert it["auto_apply"] is False, (
            f"item {it['item_id']}: auto_apply must be False")
        assert it["enforce_allowed"] is False, (
            f"item {it['item_id']}: enforce_allowed must be False")
    # конкретный кейс manual_vision_conflict
    it1 = next(it for it in d["items"] if it["item_id"] == "xp1")
    assert "manual_vision_conflict" in it1["risk_flags"]
    assert it1["classification"] == "candidate_exclude"
    assert it1["severity"] == "high"


# ─── 2: missing → not_found ──────────────────────────────────────────────────

def test_2_missing_report_not_found(client, comparison_root):
    _pv2_dir(comparison_root)   # dir exists, no report
    r = client.get(EP, params={"pair_id": PID})
    assert r.status_code == 200
    d = r.json()
    assert d["status"] == "not_found"
    assert d["available"] is False
    assert d["message"]
    assert d["items"] == []


# ─── 3: broken → error, not 500 ─────────────────────────────────────────────

def test_3_broken_report_error_not_500(client, comparison_root):
    _seed_report(comparison_root, broken=True)
    r = client.get(EP, params={"pair_id": PID})
    assert r.status_code == 200
    d = r.json()
    assert d["status"] == "error"
    assert d["available"] is False
    assert d["warnings"]


# ─── 4: filter classification ────────────────────────────────────────────────

def test_4_filter_classification(client, comparison_root):
    _seed_report(comparison_root)
    # только candidate_exclude
    d = client.get(EP, params={"pair_id": PID,
                               "classification": "candidate_exclude"}).json()
    assert d["status"] == "ok"
    assert all(it["classification"] == "candidate_exclude" for it in d["items"])
    assert d["filtered_count"] == 2
    assert len(d["items"]) == 2
    # summary всё равно полное
    assert d["summary"]["items_total"] == 5

    # только keep
    dk = client.get(EP, params={"pair_id": PID, "classification": "keep"}).json()
    assert len(dk["items"]) == 1
    assert dk["items"][0]["item_id"] == "xp4"

    # неизвестный класс → status=error
    de = client.get(EP, params={"pair_id": PID, "classification": "junk"}).json()
    assert de["status"] == "error"
    assert de["available"] is False


# ─── 5: filter severity ──────────────────────────────────────────────────────

def test_5_filter_severity(client, comparison_root):
    _seed_report(comparison_root)
    dh = client.get(EP, params={"pair_id": PID, "severity": "high"}).json()
    assert all(it["severity"] == "high" for it in dh["items"])
    assert dh["filtered_count"] == 1

    dm = client.get(EP, params={"pair_id": PID, "severity": "medium"}).json()
    assert all(it["severity"] == "medium" for it in dm["items"])
    assert dm["filtered_count"] == 2

    # неизвестная severity → status=error
    de = client.get(EP, params={"pair_id": PID, "severity": "extreme"}).json()
    assert de["status"] == "error"


# ─── 6: pagination ───────────────────────────────────────────────────────────

def test_6_pagination_limit_offset(client, comparison_root):
    _seed_report(comparison_root)
    d1 = client.get(EP, params={"pair_id": PID, "limit": 3, "offset": 0}).json()
    assert d1["total_count"] == 5
    assert d1["filtered_count"] == 5
    assert len(d1["items"]) == 3

    d2 = client.get(EP, params={"pair_id": PID, "limit": 3, "offset": 3}).json()
    assert len(d2["items"]) == 2

    keys1 = {it["item_id"] for it in d1["items"]}
    keys2 = {it["item_id"] for it in d2["items"]}
    assert keys1.isdisjoint(keys2)
    assert keys1 | keys2 == {"xp1", "xp2", "xp3", "xp4", "xp5"}


# ─── 7: limit clamp ──────────────────────────────────────────────────────────

def test_7_limit_clamp(client, comparison_root):
    _seed_report(comparison_root)
    d = client.get(EP, params={"pair_id": PID, "limit": 9999}).json()
    assert d["limit"] == 500  # clamped
    d0 = client.get(EP, params={"pair_id": PID, "limit": 0}).json()
    assert d0["limit"] == 0   # 0 allowed (empty page)
    assert d0["items"] == []


# ─── 8: no raw text / model data leak ────────────────────────────────────────

def test_8_no_raw_text_leak(client, comparison_root):
    rep = _report()
    big = "ВРУ-3 ЩР-4а ЩО-1 " * 3000
    # подбросить «сырые» большие данные — они НЕ должны быть в ответе целиком
    rep["items"][0]["raw_qwen_description"] = big
    rep["items"][0]["reasoning_trace"] = big
    rep["items"][0]["debug_dump"] = big
    _pv2_dir(comparison_root)
    d = _pv2_dir(comparison_root)
    (d / "exclusion_preview_v2_report.json").write_text(
        json.dumps(rep, ensure_ascii=False), encoding="utf-8")
    resp = client.get(EP, params={"pair_id": PID})
    body = resp.text
    assert big not in body
    assert len(body) < 30000   # компактный ответ
    item0 = resp.json()["items"][0]
    assert "raw_qwen_description" not in item0
    assert "reasoning_trace" not in item0
    assert "debug_dump" not in item0


# ─── 9: read-only — не создаёт файлов ────────────────────────────────────────

def test_9_read_only_no_writes(client, comparison_root):
    d = _seed_report(comparison_root)
    before = sorted(p.name for p in d.iterdir())
    client.get(EP, params={"pair_id": PID})
    client.get(EP, params={"pair_id": PID, "classification": "candidate_exclude"})
    client.get(EP, params={"pair_id": PID, "severity": "high"})
    client.get(EP, params={"pair_id": PID, "limit": 2, "offset": 1})
    after = sorted(p.name for p in d.iterdir())
    assert before == after


# ─── 10: mark-only force override (даже если файл пишет True) ────────────────

def test_10_mark_only_forced_regardless_of_file(client, comparison_root):
    """Endpoint ВСЕГДА перезаписывает auto_apply/enforce_allowed/
    use_as_grounded_fact в False независимо от значений в файле."""
    _seed_report(comparison_root)   # файл создаётся с True — см. _xp_item
    resp = client.get(EP, params={"pair_id": PID}).json()
    assert resp["status"] == "ok"
    for it in resp["items"]:
        # именно это проверяем: даже если в JSON было True — должно стать False
        assert it.get("use_as_grounded_fact") is False
        assert it.get("auto_apply") is False
        assert it.get("enforce_allowed") is False
    # summary тоже не включает авто-применение
    assert resp["summary"]["auto_enforce_enabled"] is False
