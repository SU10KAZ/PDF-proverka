# -*- coding: utf-8 -*-
"""Тесты read-only endpoint'а Pipeline V2 link validation.

GET /api/stage-comparison/pipeline-v2/{session_id}/link-validation
    (+ ?pair_id=&decision=&agreement=&limit=&offset=)

Покрытие spec-кейсов:
  1. отдаёт готовый link_validation_report.json (status=ok, инварианты mark-only);
  2. missing report → not_found (available=false), не 404/500;
  3. битый report → error (available=false), не 500;
  4. filter decision работает;
  5. filter agreement (conflicts) работает;
  6. pagination limit/offset;
  7. limit clamp (>500 → 500);
  8. no raw prompt / raw image / огромные тексты;
  9. read-only: не создаёт файлов/директорий/job'ов.

Плюс: путь НЕ в auth-exempt.

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
    root = tmp_path / "comparison_link_validation"
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
EP = f"/api/stage-comparison/pipeline-v2/{SID}/link-validation"
KIND = "stage_comparison_pipeline_v2_link_validation"


def _pv2_dir(root: Path) -> Path:
    d = root / "sessions" / SID / "pairs" / PID / "pipeline_v2"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _item(item_id, lid, rid, ll, rl, lp, rp, manual, relation, decision, conf,
          agrees, conflicts, action, rank=1, status="done"):
    return {
        "item_id": item_id, "mapping_id": f"m_{item_id}",
        "left_block_id": lid, "right_block_id": rid,
        "left_page_number": lp, "right_page_number": rp,
        "left_entity_label": ll, "right_entity_label": rl,
        "manual_decision": manual, "candidate_kind": "manual_mapping",
        "candidate_rank": rank, "status": status,
        "validation": {
            "old_new_orientation_ok": True, "entity_relation": relation,
            "decision": decision, "confidence": conf,
            "old_entity_label": ll, "new_entity_label": rl,
            "supporting_visual_evidence": ["шины 1000А"],
            "conflicting_visual_evidence": [] if agrees else ["разный состав отходящих линий"],
            "key_devices_old": ["QF1"], "key_devices_new": ["QF1"],
            "notable_changes": [], "risks": [],
        },
        "agreement": {
            "agrees_with_manual_mapping": agrees,
            "conflicts_with_manual_mapping": conflicts,
            "reason": "vision согласуется" if agrees else "vision противоречит manual mapping",
        },
        "recommended_action": action,
        "use_as_grounded_fact": False,
        "use_for_delta_explanation": False,
    }


def _report():
    items = [
        # valid + agrees
        _item("a1", "9T7M", "DW7M", "ВРУ-3", "ВРУ-3", 28, 27,
              "confirmed_same", "same_entity", "valid_mapping", 0.92,
              True, False, "keep_mapping", rank=1),
        # ИОС1.1 реальный конфликт: manual confirmed_reorganized vs vision reject
        _item("a2", "6XDP-JLWQ-KNX", "3T6X-4PHG-D96", "ВРУ-3", "ВРУ-2", 27, 26,
              "confirmed_reorganized", "different_entity", "reject_mapping", 0.95,
              False, True, "manual_review_mapping", rank=1),
        # manual_review
        _item("a3", "EQRC", "64E3", "ЩО-1", "ЩО-1", 34, 33,
              "confirmed_same", "uncertain", "manual_review", 0.5,
              False, False, "manual_review_mapping", rank=2),
    ]
    return {
        "version": 1, "kind": KIND, "status": "ok",
        "session_id": SID, "pair_id": PID, "created_at": "20260612_010101",
        "summary": {
            "candidates_total": 3, "attempted": 3, "succeeded": 3, "failed": 0,
            "valid_mapping": 1, "manual_review": 1, "reject_mapping": 1,
            "agrees_with_manual_mapping": 1, "conflicts_with_manual_mapping": 1,
            "orientation_failed": 0,
        },
        "items": items,
        "warnings": [],
    }


def _seed_report(root: Path, broken=False):
    d = _pv2_dir(root)
    txt = "{not json" if broken else json.dumps(_report(), ensure_ascii=False)
    (d / "link_validation_report.json").write_text(txt, encoding="utf-8")
    return d


# ─── 1: ready report ─────────────────────────────────────────────────────────

def test_1_returns_ready_report(client, comparison_root):
    _seed_report(comparison_root)
    r = client.get(EP, params={"pair_id": PID})
    assert r.status_code == 200
    d = r.json()
    assert d["status"] == "ok" and d["available"] is True
    assert d["kind"] == KIND and d["source"] == "ready_report"
    assert d["summary"]["candidates_total"] == 3
    assert d["summary"]["valid_mapping"] == 1
    assert d["summary"]["reject_mapping"] == 1
    assert d["summary"]["conflicts_with_manual_mapping"] == 1
    assert len(d["items"]) == 3
    # mark-only инварианты в КАЖДОМ item
    for it in d["items"]:
        assert it["use_as_grounded_fact"] is False
        assert it["use_for_delta_explanation"] is False
        assert (it["validation"] or {}).get("do_not_use_as_fact") is True
    # конфликт ИОС1.1: ВРУ-3↔ВРУ-2 confirmed_reorganized vs different_entity/reject
    conf = next(it for it in d["items"] if it["item_id"] == "a2")
    assert conf["manual_decision"] == "confirmed_reorganized"
    assert conf["validation"]["entity_relation"] == "different_entity"
    assert conf["validation"]["decision"] == "reject_mapping"
    assert conf["agreement"]["conflicts_with_manual_mapping"] is True
    assert conf["recommended_action"] == "manual_review_mapping"
    # сортировка: конфликтный item идёт первым
    assert d["items"][0]["item_id"] == "a2"


# ─── 2: missing → not_found ──────────────────────────────────────────────────

def test_2_missing_report_not_found(client, comparison_root):
    _pv2_dir(comparison_root)   # dir exists, no report
    r = client.get(EP, params={"pair_id": PID})
    assert r.status_code == 200
    d = r.json()
    assert d["status"] == "not_found" and d["available"] is False
    assert d["message"]


# ─── 3: broken → error, not 500 ──────────────────────────────────────────────

def test_3_broken_report_error_not_500(client, comparison_root):
    _seed_report(comparison_root, broken=True)
    r = client.get(EP, params={"pair_id": PID})
    assert r.status_code == 200
    d = r.json()
    assert d["status"] == "error" and d["available"] is False
    assert d["warnings"]


# ─── 4: filter decision ──────────────────────────────────────────────────────

def test_4_filter_decision(client, comparison_root):
    _seed_report(comparison_root)
    d = client.get(EP, params={"pair_id": PID, "decision": "reject_mapping"}).json()
    assert d["filters"]["decision"] == "reject_mapping"
    assert all((it["validation"] or {}).get("decision") == "reject_mapping"
               for it in d["items"])
    assert len(d["items"]) == 1
    assert d["pagination"]["total"] == 1
    # summary всё равно полное (не зависит от фильтра)
    assert d["summary"]["candidates_total"] == 3
    # неизвестное значение деградирует к all
    d2 = client.get(EP, params={"pair_id": PID, "decision": "junk"}).json()
    assert d2["filters"]["decision"] == "all"
    assert len(d2["items"]) == 3


# ─── 5: filter agreement ─────────────────────────────────────────────────────

def test_5_filter_agreement_conflicts(client, comparison_root):
    _seed_report(comparison_root)
    d = client.get(EP, params={"pair_id": PID, "agreement": "conflicts"}).json()
    assert d["filters"]["agreement"] == "conflicts"
    assert len(d["items"]) == 1
    assert d["items"][0]["agreement"]["conflicts_with_manual_mapping"] is True
    da = client.get(EP, params={"pair_id": PID, "agreement": "agrees"}).json()
    assert len(da["items"]) == 1
    assert da["items"][0]["agreement"]["agrees_with_manual_mapping"] is True


# ─── 6: pagination ───────────────────────────────────────────────────────────

def test_6_pagination_limit_offset(client, comparison_root):
    _seed_report(comparison_root)
    d1 = client.get(EP, params={"pair_id": PID, "limit": 2, "offset": 0}).json()
    assert d1["pagination"]["limit"] == 2 and d1["pagination"]["returned"] == 2
    assert d1["pagination"]["total"] == 3
    d2 = client.get(EP, params={"pair_id": PID, "limit": 2, "offset": 2}).json()
    assert d2["pagination"]["offset"] == 2 and d2["pagination"]["returned"] == 1
    keys1 = {it["item_id"] for it in d1["items"]}
    keys2 = {it["item_id"] for it in d2["items"]}
    assert keys1.isdisjoint(keys2)


# ─── 7: limit clamp ──────────────────────────────────────────────────────────

def test_7_limit_clamp(client, comparison_root):
    _seed_report(comparison_root)
    d = client.get(EP, params={"pair_id": PID, "limit": 99999}).json()
    assert d["pagination"]["limit"] == 500
    d0 = client.get(EP, params={"pair_id": PID, "limit": 0}).json()
    assert d0["pagination"]["limit"] == 1


# ─── 8: no raw text / raw model data ─────────────────────────────────────────

def test_8_no_raw_text_leak(client, comparison_root):
    rep = _report()
    big = "ЩР-1 ЩР-2 ЩР-3 " * 3000
    # подбросить «сырьё» в item — оно НЕ должно отдаваться целиком
    rep["items"][0]["raw_qwen_text"] = big
    rep["items"][0]["validation"]["raw_full_response"] = big
    rep["items"][0]["validation"]["notable_changes"] = [big]   # длинная строка → обрезка
    rep["items"][0]["left_entity_label"] = big                 # длинная метка → обрезка
    d = _pv2_dir(comparison_root)
    (d / "link_validation_report.json").write_text(
        json.dumps(rep, ensure_ascii=False), encoding="utf-8")
    resp = client.get(EP, params={"pair_id": PID})
    body = resp.text
    assert big not in body                    # полный raw целиком не утёк
    assert len(body) < 30000                  # ответ компактный, без дампа сырья
    j = resp.json()
    it0 = next(it for it in j["items"] if it["item_id"] == "a1")
    assert "raw_qwen_text" not in it0         # неизвестные поля не пробрасываются
    assert "raw_full_response" not in (it0.get("validation") or {})
    assert len(it0["left_entity_label"]) <= 100   # метка обрезана до safe cap
    for s in (it0["validation"] or {}).get("notable_changes", []):
        assert len(s) <= 260                  # элементы списков обрезаны


# ─── 9: read-only (no writes/dirs) ───────────────────────────────────────────

def test_9_read_only_no_writes(client, comparison_root):
    d = _seed_report(comparison_root)
    before = sorted(p.name for p in d.iterdir())
    client.get(EP, params={"pair_id": PID})
    client.get(EP, params={"pair_id": PID, "decision": "reject_mapping"})
    client.get(EP, params={"pair_id": PID, "agreement": "conflicts"})
    client.get(EP, params={"pair_id": PID, "limit": 1})
    after = sorted(p.name for p in d.iterdir())
    assert before == after   # ни одного нового файла


# ─── bonus: auth — путь НЕ в exempt ──────────────────────────────────────────

def test_endpoint_not_in_auth_exempt():
    from backend.app.core import portal_auth
    exempt = getattr(portal_auth, "EXEMPT_PATHS", None) or getattr(
        portal_auth, "_EXEMPT_PATHS", set())
    path = f"/api/stage-comparison/pipeline-v2/{SID}/link-validation"
    assert path not in (exempt or set())
    assert not any(path.startswith(p) for p in (exempt or set())
                   if isinstance(p, str) and p not in ("/", ""))
