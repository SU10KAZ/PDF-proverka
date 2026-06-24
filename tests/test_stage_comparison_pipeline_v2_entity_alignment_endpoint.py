# -*- coding: utf-8 -*-
"""Тесты read-only endpoint'а Pipeline V2 entity alignment preview.

GET /api/stage-comparison/pipeline-v2/{session_id}/entity-alignment-preview
    (+ ?pair_id=&classification=&limit=&offset=)

Покрытие spec-кейсов:
  1. отдаёт готовый entity_alignment_preview_report.json;
  2. missing report → not_found (available=false), не 404/500;
  3. битый report → error (available=false), не 500;
  4. filter classification работает;
  5. pagination limit/offset;
  6. limit clamp (>500 → 500);
  7. read-only: не создаёт файлов/директорий/job'ов;
  8. no raw text / raw model data;
  9. auth: путь НЕ в exempt (middleware-level 401 для анонимов);
  10. build on-the-fly из артефактов, если готового отчёта нет.

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
    root = tmp_path / "comparison_entity_alignment"
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
EP = f"/api/stage-comparison/pipeline-v2/{SID}/entity-alignment-preview"
KIND = "stage_comparison_pipeline_v2_entity_alignment_preview"


def _pv2_dir(root: Path) -> Path:
    d = root / "sessions" / SID / "pairs" / PID / "pipeline_v2"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _report():
    def pair(lid, rid, ll, rl, fam, cls, conf, lp, rp, reasons, risk):
        return {
            "pair_key": f"{lid}__{rid}", "left_block_id": lid, "right_block_id": rid,
            "left_page_number": lp, "right_page_number": rp,
            "left_sheet_name": f"Схема {ll}", "right_sheet_name": f"Схема {rl}",
            "left_entity_label": ll, "right_entity_label": rl, "entity_family": fam,
            "classification": cls, "confidence": conf, "reasons": reasons,
            "risk_flags": risk, "recommended_action": "manual_mapping",
            "evidence": {"entity_id_match": cls == "same_entity_likely",
                         "numbered_entity_conflict": cls == "scope_reorganized"},
        }
    return {
        "version": 1, "kind": KIND, "status": "ok",
        "summary": {
            "graphic_pairs_total": 4, "same_entity_likely": 1, "possible_rename": 0,
            "scope_reorganized": 1, "mismatch_likely": 1,
            "link_validation_candidate": 1, "needs_manual_mapping": 1,
            "unpaired_left": 1, "unpaired_right": 1,
        },
        "pairs": [
            pair("9T7M", "DW7M", "ВРУ-3", "ВРУ-3", "ВРУ", "same_entity_likely",
                 0.9, 28, 27, ["entity id совпадает"], []),
            pair("EYMU", "PNNH", "ВРУ-3", "ВРУ-2", "ВРУ", "scope_reorganized",
                 0.6, 27, 26, ["numbered_entity_conflict"], ["numbered_conflict"]),
            pair("EQRC", "64E3", "ЯК-3", "ЩО-1", "ЯК", "mismatch_likely",
                 0.85, 34, 33, ["family_conflict"], []),
            pair("XX", "YY", None, None, None, "link_validation_candidate",
                 0.4, 40, 41, [], []),
        ],
        "unpaired_entities": {
            "left": [{"entity_label": "ЩО-7", "family": "ЩО", "graphic_type": "scheme",
                      "sheet_name": "Схема ЩО-7", "block_ids": ["z1"]}],
            "right": [{"entity_label": "ВРУ-А", "family": "ВРУ", "graphic_type": "scheme",
                       "sheet_name": "Схема ВРУ-А", "block_ids": ["z2"]}],
        },
        "warnings": [],
    }


def _gate():
    return {"version": 1,
            "kind": "stage_comparison_pipeline_v2_visual_equivalence_gate",
            "status": "ok", "block_pairs": [
                {"left_block_id": "L", "right_block_id": "R",
                 "left_page_number": 28, "right_page_number": 27,
                 "pair_key": "L__R"}]}


def _model():
    return {"version": 1, "pages": []}


def _seed_report(root: Path, broken=False):
    d = _pv2_dir(root)
    txt = "{not json" if broken else json.dumps(_report(), ensure_ascii=False)
    (d / "entity_alignment_preview_report.json").write_text(txt, encoding="utf-8")
    return d


# ─── 1: ready report ─────────────────────────────────────────────────────────

def test_1_returns_ready_report(client, comparison_root):
    _seed_report(comparison_root)
    r = client.get(EP, params={"pair_id": PID})
    assert r.status_code == 200
    d = r.json()
    assert d["status"] == "ok" and d["available"] is True
    assert d["kind"] == KIND and d["source"] == "ready_report"
    assert d["summary"]["graphic_pairs_total"] == 4
    assert d["summary"]["same_entity_likely"] == 1
    assert d["summary"]["scope_reorganized"] == 1
    assert d["summary"]["mismatch_likely"] == 1
    assert len(d["pairs"]) == 4
    # known-case sanity: ВРУ-3↔ВРУ-3 same, ВРУ-3↔ВРУ-2 scope, ЯК↔ЩО mismatch
    by = {(p["left_entity_label"], p["right_entity_label"]): p["classification"]
          for p in d["pairs"]}
    assert by[("ВРУ-3", "ВРУ-3")] == "same_entity_likely"
    assert by[("ВРУ-3", "ВРУ-2")] == "scope_reorganized"
    assert by[("ЯК-3", "ЩО-1")] == "mismatch_likely"
    assert len(d["unpaired_entities"]["left"]) == 1
    assert len(d["unpaired_entities"]["right"]) == 1


# ─── 2: missing → not_found ──────────────────────────────────────────────────

def test_2_missing_report_not_found(client, comparison_root):
    _pv2_dir(comparison_root)   # dir exists, no report, no gate
    r = client.get(EP, params={"pair_id": PID})
    assert r.status_code == 200
    d = r.json()
    assert d["status"] == "not_found" and d["available"] is False
    assert "not found" in d["message"].lower()


# ─── 3: broken → error, not 500 ──────────────────────────────────────────────

def test_3_broken_report_error_not_500(client, comparison_root):
    _seed_report(comparison_root, broken=True)
    r = client.get(EP, params={"pair_id": PID})
    assert r.status_code == 200
    d = r.json()
    assert d["status"] == "error" and d["available"] is False
    assert d["warnings"]


# ─── 4: filter classification ────────────────────────────────────────────────

def test_4_filter_classification(client, comparison_root):
    _seed_report(comparison_root)
    d = client.get(EP, params={"pair_id": PID,
                               "classification": "mismatch_likely"}).json()
    assert d["filters"]["classification"] == "mismatch_likely"
    assert all(p["classification"] == "mismatch_likely" for p in d["pairs"])
    assert len(d["pairs"]) == 1
    assert d["pagination"]["total"] == 1
    # summary всё равно полное (не зависит от фильтра)
    assert d["summary"]["graphic_pairs_total"] == 4
    # неизвестное значение фильтра деградирует к all
    d2 = client.get(EP, params={"pair_id": PID, "classification": "junk"}).json()
    assert d2["filters"]["classification"] == "all"
    assert len(d2["pairs"]) == 4


# ─── 5: pagination ───────────────────────────────────────────────────────────

def test_5_pagination_limit_offset(client, comparison_root):
    _seed_report(comparison_root)
    d1 = client.get(EP, params={"pair_id": PID, "limit": 2, "offset": 0}).json()
    assert d1["pagination"]["limit"] == 2 and d1["pagination"]["returned"] == 2
    assert d1["pagination"]["total"] == 4
    d2 = client.get(EP, params={"pair_id": PID, "limit": 2, "offset": 2}).json()
    assert d2["pagination"]["offset"] == 2 and d2["pagination"]["returned"] == 2
    keys1 = {p["pair_key"] for p in d1["pairs"]}
    keys2 = {p["pair_key"] for p in d2["pairs"]}
    assert keys1.isdisjoint(keys2)


# ─── 6: limit clamp ──────────────────────────────────────────────────────────

def test_6_limit_clamp(client, comparison_root):
    _seed_report(comparison_root)
    d = client.get(EP, params={"pair_id": PID, "limit": 99999}).json()
    assert d["pagination"]["limit"] == 500
    d0 = client.get(EP, params={"pair_id": PID, "limit": 0}).json()
    assert d0["pagination"]["limit"] == 1


# ─── 7: read-only (no writes/dirs) ───────────────────────────────────────────

def test_7_read_only_no_writes(client, comparison_root):
    d = _seed_report(comparison_root)
    before = sorted(p.name for p in d.iterdir())
    client.get(EP, params={"pair_id": PID})
    client.get(EP, params={"pair_id": PID, "classification": "scope_reorganized"})
    client.get(EP, params={"pair_id": PID, "limit": 1})
    after = sorted(p.name for p in d.iterdir())
    assert before == after   # ни одного нового файла


# ─── 8: no raw text / raw model data ─────────────────────────────────────────

def test_8_no_raw_text_leak(client, comparison_root):
    rep = _report()
    big = "ЩР-1 ЩР-2 ЩР-3 " * 3000
    # подбросить «сырьё» в pair — оно НЕ должно отдаваться целиком
    rep["pairs"][0]["raw_qwen_text"] = big
    rep["pairs"][0]["evidence"]["raw_full_text"] = big
    rep["pairs"][0]["left_sheet_name"] = big   # длинное имя должно обрезаться
    d = _pv2_dir(comparison_root)
    (d / "entity_alignment_preview_report.json").write_text(
        json.dumps(rep, ensure_ascii=False), encoding="utf-8")
    resp = client.get(EP, params={"pair_id": PID})
    body = resp.text
    assert big not in body                    # полный raw целиком не утёк
    assert len(body) < 20000                  # ответ компактный, без дампа сырья
    j = resp.json()
    p0 = next(p for p in j["pairs"] if p["pair_key"] == "9T7M__DW7M")
    assert "raw_qwen_text" not in p0          # неизвестные поля не пробрасываются
    assert "raw_full_text" not in (p0.get("evidence") or {})
    assert len(p0["left_sheet_name"]) <= 200  # имя обрезано до safe cap


# ─── 9: auth — путь НЕ в exempt ──────────────────────────────────────────────

def test_9_endpoint_not_in_auth_exempt():
    from backend.app.core import portal_auth
    exempt = getattr(portal_auth, "EXEMPT_PATHS", None) or getattr(
        portal_auth, "_EXEMPT_PATHS", set())
    path = f"/api/stage-comparison/pipeline-v2/{SID}/entity-alignment-preview"
    assert path not in (exempt or set())
    assert not any(path.startswith(p) for p in (exempt or set())
                   if isinstance(p, str) and p not in ("/", ""))


# ─── 10: build on-the-fly из артефактов ──────────────────────────────────────

def test_10_build_on_the_fly_from_artifacts(client, comparison_root):
    # нет готового отчёта, но есть visual gate + models → собрать on-the-fly
    d = _pv2_dir(comparison_root)
    (d / "visual_equivalence_gate_report.json").write_text(
        json.dumps(_gate(), ensure_ascii=False), encoding="utf-8")
    (d / "left_normalized_document_model.json").write_text(
        json.dumps(_model(), ensure_ascii=False), encoding="utf-8")
    (d / "right_normalized_document_model.json").write_text(
        json.dumps(_model(), ensure_ascii=False), encoding="utf-8")
    r = client.get(EP, params={"pair_id": PID})
    assert r.status_code == 200
    d2 = r.json()
    assert d2["status"] == "ok" and d2["available"] is True
    assert d2["source"] == "built_from_artifacts"
    assert d2["summary"]["graphic_pairs_total"] == 1
    # ничего не записано на диск (отчёт on-the-fly не кешируется)
    assert not (d / "entity_alignment_preview_report.json").exists()
