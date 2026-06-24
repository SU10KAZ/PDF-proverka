# -*- coding: utf-8 -*-
"""Тесты Pipeline V2 Block Link Preview (builder + endpoint + dry-run).

Покрываемые spec-кейсы задачи:
  1.  strong block match → green card;
  2.  weak block match → yellow card;
  3.  manual_review risk → orange card;
  4.  unmatched left/right попадают в unmatched;
  5.  bbox_norm сохраняется;
  6.  page links группируют block links;
  7.  visual gate данные подтягиваются;
  8.  отсутствие visual gate не ломает builder;
  9.  endpoint отдаёт готовый preview;
  10. endpoint строит preview on-the-fly из artifacts;
  11. endpoint not_found при отсутствии artifacts;
  12. endpoint read-only и не создаёт jobs/файлов;
  15. никаких vision/LLM imports/calls в builder и сервисе.

Плюс интеграция dry-run (артефакт + manifest + summary section + fail-soft
+ disable) и регрессии формата. Никаких реальных LLM/network/live backend.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.services.stage_comparison import (
    pipeline_v2_block_link_preview as blp,
)


# ─── синтетика в форме реальных артефактов ────────────────────────────────────


def _model(side: str) -> dict:
    p = side[0].upper()
    blocks = {
        f"{p}_T1": {"block_id": f"{p}_T1", "page_number": 1,
                    "block_type": "text", "semantic_type": "text",
                    "coords_norm": [0.1, 0.1, 0.9, 0.3]},
        f"{p}_M1": {"block_id": f"{p}_M1", "page_number": 1,
                    "block_type": "text", "semantic_type": "table",
                    "coords_norm": [0.1, 0.5, 0.9, 0.7]},
        f"{p}_S1": {"block_id": f"{p}_S1", "page_number": 2,
                    "block_type": "image", "semantic_type": "scheme",
                    "coords_norm": [0.05, 0.2, 0.95, 0.8]},
        f"{p}_U1": {"block_id": f"{p}_U1", "page_number": 3,
                    "block_type": "text", "semantic_type": "text",
                    "coords_norm": [0.2, 0.4, 0.8, 0.6]},
    }
    return {
        "version": 1,
        "kind": "stage_comparison_pipeline_v2_normalized_document_model",
        "source": {"pdf_path": None},
        "pages": [{"page_number": n, "blocks": []} for n in (1, 2, 3)],
        "blocks": blocks,
        "warnings": [],
    }


def _bmatch(mid, lid, rid, *, pm="pm_1_1", lp=1, rp=1, conf="strong",
            method="stamp", score=0.9, risk=None, lsem="text", rsem="text"):
    return {
        "match_id": mid, "page_match_id": pm,
        "left_block_id": lid, "right_block_id": rid,
        "left_page_number": lp, "right_page_number": rp,
        "left_semantic_type": lsem, "right_semantic_type": rsem,
        "left_block_type": "text", "right_block_type": "text",
        "method": method, "score": score, "iou": 0.9,
        "confidence": conf, "reasons": [], "risk_flags": risk or [],
    }


def _block_matching() -> dict:
    return {
        "version": 1, "kind": "stage_comparison_pipeline_v2_block_matching",
        "summary": {},
        "page_matches": [
            {"match_id": "pm_1_1", "left_page_number": 1, "right_page_number": 1,
             "left_sheet_name": "Общие данные", "right_sheet_name": "Общие данные",
             "method": "stamp_sheet", "score": 0.95, "confidence": "strong",
             "risk_flags": []},
            {"match_id": "pm_2_2", "left_page_number": 2, "right_page_number": 2,
             "left_sheet_name": "Схема", "right_sheet_name": "Схема",
             "method": "stamp_sheet", "score": 0.9, "confidence": "strong",
             "risk_flags": []},
        ],
        "block_matches": [
            _bmatch("bm_strong", "L_T1", "R_T1", conf="strong"),
            _bmatch("bm_manual", "L_M1", "R_M1", conf="medium",
                    method="table_fuzzy", score=0.5,
                    risk=["duplicate_candidate"], lsem="table", rsem="table"),
            _bmatch("bm_weak", "L_S1", "R_S1", pm="pm_2_2", lp=2, rp=2,
                    conf="weak", method="scheme_crop", score=0.4,
                    lsem="scheme", rsem="scheme"),
        ],
        "unmatched_left_pages": [], "unmatched_right_pages": [],
        "unmatched_left_blocks": [
            {"block_id": "L_U1", "page_number": 3, "semantic_type": "text",
             "block_type": "text", "risk_flags": ["one_sided_block"]},
        ],
        "unmatched_right_blocks": [
            {"block_id": "R_U1", "page_number": 3, "semantic_type": "text",
             "block_type": "text", "risk_flags": ["one_sided_block"]},
        ],
        "warnings": [],
    }


def _visual_gate() -> dict:
    return {
        "version": 1,
        "kind": "stage_comparison_pipeline_v2_visual_equivalence_gate",
        "status": "ok",
        "summary": {},
        "block_pairs": [
            {"pair_key": "L_S1__R_S1", "left_block_id": "L_S1",
             "right_block_id": "R_S1", "status": "changed_visual",
             "decision": "send_to_vision", "confidence": 0.9,
             "risk_flags": [],
             "metrics": {"mask_iou": 0.42, "normalized_correlation": 0.51,
                         "total_diff_ratio": 0.18,
                         "alignment_method": "ecc_euclidean"}},
        ],
        "warnings": [],
    }


def _build(visual=None, **kw):
    return blp.build_block_link_preview(
        _model("left"), _model("right"), _block_matching(),
        visual_gate_report=visual, **kw)


def _links_by_id(report):
    return {l["block_link_id"]: l for l in report["block_links"]}


# ─── 1-3: статусы и цвета ────────────────────────────────────────────────────


def test_1_strong_match_is_green():
    link = _links_by_id(_build())["bm_strong"]
    assert link["link_status"] == "strong"
    assert link["ui"]["color"] == "green"
    assert link["ui"]["default_visible"] is True


def test_2_weak_match_is_yellow():
    link = _links_by_id(_build())["bm_weak"]
    assert link["link_status"] == "weak"
    assert link["ui"]["color"] == "yellow"


def test_2b_medium_confidence_maps_to_weak():
    bm = _block_matching()
    bm["block_matches"] = [_bmatch("bm_med", "L_T1", "R_T1", conf="medium")]
    rep = blp.build_block_link_preview(_model("left"), _model("right"), bm)
    link = rep["block_links"][0]
    assert link["link_status"] == "weak"
    assert link["match_confidence"] == "medium"  # исходная градация сохранена


def test_3_manual_review_risk_is_orange():
    link = _links_by_id(_build())["bm_manual"]
    assert link["link_status"] == "manual_review"
    assert link["ui"]["color"] == "orange"
    assert "duplicate_candidate" in link["risk_flags"]


def test_3b_visual_manual_review_decision_forces_orange():
    visual = _visual_gate()
    visual["block_pairs"][0]["decision"] = "manual_review"
    visual["block_pairs"][0]["status"] = "identical_visual"
    visual["block_pairs"][0]["risk_flags"] = ["localized_residual_diff"]
    link = _links_by_id(_build(visual=visual))["bm_weak"]
    assert link["link_status"] == "manual_review"
    assert "localized_residual_diff" in link["risk_flags"]


def test_3c_visual_manual_review_decision_alone_forces_orange():
    """Killer-тест ревью: decision=manual_review БЕЗ risk-флагов из
    MANUAL_REVIEW_RISK_FLAGS обязан эскалировать сам по себе — даже на
    strong match."""
    visual = _visual_gate()
    visual["block_pairs"][0].update(left_block_id="L_T1",
                                    right_block_id="R_T1",
                                    decision="manual_review",
                                    status="identical_visual",
                                    risk_flags=[])
    link = _links_by_id(_build(visual=visual))["bm_strong"]
    assert link["link_status"] == "manual_review"
    assert link["ui"]["color"] == "orange"


def test_3d_visual_skipped_does_not_escalate():
    """Killer-тест ревью: gate status=skipped (cap бюджета — пара НЕ
    сравнивалась) не должен красить strong match в orange."""
    visual = _visual_gate()
    visual["block_pairs"][0].update(left_block_id="L_T1",
                                    right_block_id="R_T1",
                                    decision="manual_review",
                                    status="skipped",
                                    risk_flags=[])
    rep = _build(visual=visual)
    link = _links_by_id(rep)["bm_strong"]
    assert link["link_status"] == "strong"
    assert link["ui"]["color"] == "green"
    assert "visual_gate_skipped" in link["risk_flags"]
    assert rep["summary"]["visual_skipped"] == 1


# ─── 4: unmatched ────────────────────────────────────────────────────────────


def test_4_unmatched_blocks_present_with_gray_color():
    rep = _build()
    left = rep["unmatched"]["left_blocks"]
    right = rep["unmatched"]["right_blocks"]
    assert [b["block_id"] for b in left] == ["L_U1"]
    assert [b["block_id"] for b in right] == ["R_U1"]
    for b in left + right:
        assert b["link_status"] == "unmatched"
        assert b["ui"]["color"] == "gray"
        assert b["bbox_norm"] == [0.2, 0.4, 0.8, 0.6]
    assert left[0]["side"] == "left" and right[0]["side"] == "right"
    assert rep["summary"]["unmatched_left_blocks"] == 1
    assert rep["summary"]["unmatched_right_blocks"] == 1


# ─── 5: bbox ─────────────────────────────────────────────────────────────────


def test_5_bbox_norm_preserved_from_model():
    link = _links_by_id(_build())["bm_strong"]
    assert link["left_bbox_norm"] == [0.1, 0.1, 0.9, 0.3]
    assert link["right_bbox_norm"] == [0.1, 0.1, 0.9, 0.3]


def test_5b_missing_block_gives_null_bbox_and_risk_flag():
    bm = _block_matching()
    bm["block_matches"] = [_bmatch("bm_ghost", "NO_SUCH", "R_T1")]
    rep = blp.build_block_link_preview(_model("left"), _model("right"), bm)
    link = rep["block_links"][0]
    assert link["left_bbox_norm"] is None
    assert "left_bbox_missing" in link["risk_flags"]
    assert link["right_bbox_norm"] is not None


# ─── 6: page links группируют ────────────────────────────────────────────────


def test_6_page_links_group_block_links():
    rep = _build()
    pages = {p["page_link_id"]: p for p in rep["page_links"]}
    assert rep["summary"]["page_links_total"] == 2
    assert sorted(pages["pm_1_1"]["block_link_ids"]) == ["bm_manual", "bm_strong"]
    assert pages["pm_2_2"]["block_link_ids"] == ["bm_weak"]
    assert pages["pm_1_1"]["block_links_by_status"] == {
        "strong": 1, "manual_review": 1}
    assert pages["pm_1_1"]["left_page_number"] == 1
    assert pages["pm_1_1"]["right_page_number"] == 1
    assert pages["pm_1_1"]["left_sheet_name"] == "Общие данные"


# ─── 7-8: visual gate join ───────────────────────────────────────────────────


def test_7_visual_gate_fields_joined():
    rep = _build(visual=_visual_gate())
    link = _links_by_id(rep)["bm_weak"]
    assert link["visual_status"] == "changed_visual"
    assert link["visual_decision"] == "send_to_vision"
    assert link["visual_metrics"]["mask_iou"] == 0.42
    assert link["visual_metrics"]["normalized_correlation"] == 0.51
    assert rep["summary"]["visual_changed"] == 1
    assert rep["summary"]["visual_identical"] == 0
    assert rep["summary"]["visual_gate_available"] is True
    # связи без visual gate entry остаются с null
    assert _links_by_id(rep)["bm_strong"]["visual_status"] is None


def test_8_builder_works_without_visual_gate():
    rep = _build(visual=None)
    assert rep["status"] == "ok"
    for link in rep["block_links"]:
        assert link["visual_status"] is None
        assert link["visual_decision"] is None
    assert rep["summary"]["visual_gate_available"] is False
    assert rep["summary"]["visual_changed"] == 0


def test_8b_invalid_block_matching_raises_value_error():
    with pytest.raises(ValueError):
        blp.build_block_link_preview(_model("left"), _model("right"), None)
    with pytest.raises(ValueError):
        blp.build_block_link_preview(_model("left"), _model("right"),
                                     {"no_block_matches": True})


def test_8c_report_contract_kind_version_counts():
    rep = _build()
    assert rep["kind"] == blp.REPORT_KIND
    assert rep["version"] == blp.REPORT_VERSION
    s = rep["summary"]
    assert s["block_links_total"] == 3
    assert s["strong_links"] == 1
    assert s["weak_links"] == 1
    assert s["manual_review_links"] == 1
    assert s["graphic_links_total"] == 1  # bm_weak: scheme image-блоки
    link = _links_by_id(rep)["bm_weak"]
    assert link["is_graphic"] is True
    assert _links_by_id(rep)["bm_strong"]["is_graphic"] is False


def test_7b_visual_summary_buckets_all_statuses():
    """Killer-тест ревью: каждый visual-статус считается в СВОЙ bucket."""
    statuses = ["identical_visual", "minor_visual", "uncertain",
                "render_failed", "skipped"]
    bm = _block_matching()
    bm["block_matches"] = [
        _bmatch(f"bm_{i}", "L_T1", "R_T1") for i in range(len(statuses))]
    visual = {"kind": "stage_comparison_pipeline_v2_visual_equivalence_gate",
              "block_pairs": []}
    # каждой связи свой visual entry: ids различить нельзя (одни блоки),
    # поэтому используем разные пары блоков
    pairs = [("L_T1", "R_T1"), ("L_M1", "R_M1"), ("L_S1", "R_S1"),
             ("L_U1", "R_U1"), ("L_X1", "R_X1")]
    bm["block_matches"] = [
        _bmatch(f"bm_{i}", l, r) for i, (l, r) in enumerate(pairs)]
    visual["block_pairs"] = [
        {"left_block_id": l, "right_block_id": r, "status": s,
         "decision": "send_to_vision", "risk_flags": [], "metrics": {}}
        for (l, r), s in zip(pairs, statuses)]
    rep = blp.build_block_link_preview(_model("left"), _model("right"), bm,
                                       visual_gate_report=visual)
    s = rep["summary"]
    assert s["visual_identical"] == 1
    assert s["visual_minor"] == 1
    assert s["visual_uncertain"] == 2   # uncertain + render_failed
    assert s["visual_skipped"] == 1
    assert s["visual_changed"] == 0


def test_readiness_low_flags_real_artifact_shape():
    """Killer-тест ревью: реальный graphic descriptor хранит уровень в
    diff_readiness.readiness (не .level) — флаги должны появляться без
    visual gate."""
    left_g = {"descriptors": [
        {"block_id": "L_S1",
         "diff_readiness": {"usable_for_diff": False, "readiness": "low"}}]}
    right_g = {"descriptors": [
        {"block_id": "R_S1",
         "diff_readiness": {"usable_for_diff": False,
                            "readiness": "not_usable"}}]}
    rep = blp.build_block_link_preview(
        _model("left"), _model("right"), _block_matching(),
        left_graphic_report=left_g, right_graphic_report=right_g)
    link = _links_by_id(rep)["bm_weak"]
    assert "left_readiness_low" in link["risk_flags"]
    assert "right_readiness_not_usable" in link["risk_flags"]
    # legacy plain-string вариант тоже работает
    rep2 = blp.build_block_link_preview(
        _model("left"), _model("right"), _block_matching(),
        left_graphic_report={"descriptors": [
            {"block_id": "L_S1", "diff_readiness": "low"}]})
    assert "left_readiness_low" in _links_by_id(rep2)["bm_weak"]["risk_flags"]


def test_is_graphic_semantic_only_block():
    """Killer-тест ревью: блок графичен по semantic_type без block_type=image."""
    lm, rm = _model("left"), _model("right")
    for m, p in ((lm, "L"), (rm, "R")):
        m["blocks"][f"{p}_P1"] = {
            "block_id": f"{p}_P1", "page_number": 2, "block_type": "text",
            "semantic_type": "plan", "coords_norm": [0.1, 0.1, 0.5, 0.5]}
    bm = _block_matching()
    bm["block_matches"].append(
        _bmatch("bm_plan", "L_P1", "R_P1", pm="pm_2_2", lp=2, rp=2,
                conf="weak", lsem="plan", rsem="plan"))
    rep = blp.build_block_link_preview(lm, rm, bm)
    assert _links_by_id(rep)["bm_plan"]["is_graphic"] is True
    assert rep["summary"]["graphic_links_total"] == 2  # bm_weak + bm_plan


def test_is_graphic_falls_back_to_match_semantics_without_model_blocks():
    """Killer-тест ревью: деградированная модель без блоков не обнуляет
    графическую статистику — match-level semantic types спасают."""
    empty = {"pages": [], "blocks": {}}
    bm = _block_matching()
    bm["block_matches"] = [
        _bmatch("bm_s", "L_S1", "R_S1", conf="weak",
                lsem="scheme", rsem="scheme")]
    bm["unmatched_left_blocks"] = [
        {"block_id": "L_U1", "page_number": 3, "semantic_type": "scheme",
         "block_type": "image", "risk_flags": []}]
    bm["unmatched_right_blocks"] = []
    rep = blp.build_block_link_preview(empty, empty, bm)
    assert rep["block_links"][0]["is_graphic"] is True
    assert rep["summary"]["graphic_links_total"] == 1
    assert rep["unmatched"]["left_blocks"][0]["is_graphic"] is True


def test_page_number_fallback_is_side_correct():
    """Killer-тест ревью: fallback страницы берётся из блока СВОЕЙ стороны
    (модели намеренно асимметричны по страницам)."""
    lm, rm = _model("left"), _model("right")
    lm["blocks"]["L_T1"]["page_number"] = 2
    rm["blocks"]["R_T1"]["page_number"] = 5
    bm = _block_matching()
    match = _bmatch("bm_nopages", "L_T1", "R_T1")
    match["left_page_number"] = None
    match["right_page_number"] = None
    bm["block_matches"] = [match]
    bm["unmatched_left_blocks"] = [
        {"block_id": "L_U1", "semantic_type": "text", "block_type": "text",
         "risk_flags": []}]   # page_number отсутствует → из модели (3→7)
    lm["blocks"]["L_U1"]["page_number"] = 7
    bm["unmatched_right_blocks"] = []
    rep = blp.build_block_link_preview(lm, rm, bm)
    link = rep["block_links"][0]
    assert link["left_page_number"] == 2
    assert link["right_page_number"] == 5
    assert rep["unmatched"]["left_blocks"][0]["page_number"] == 7


def test_one_sided_pages_become_page_links():
    """Killer-тест ревью: односторонние листы попадают в page_links
    (иначе их unmatched-блоки невозможно увидеть в превью)."""
    bm = _block_matching()
    bm["unmatched_left_pages"] = [
        {"page_number": 3, "sheet_name": "Удалённый лист",
         "risk_flags": ["unmatched"]}]
    bm["unmatched_right_pages"] = [
        {"page_number": 3, "sheet_name": "Новый лист",
         "risk_flags": ["unmatched"]}]
    rep = blp.build_block_link_preview(_model("left"), _model("right"), bm)
    by_id = {p["page_link_id"]: p for p in rep["page_links"]}
    left_only = by_id["pl_left_only_3"]
    assert left_only["page_link_kind"] == "one_sided"
    assert left_only["left_page_number"] == 3
    assert left_only["right_page_number"] is None
    assert left_only["left_sheet_name"] == "Удалённый лист"
    # unmatched-блок L_U1 живёт на стр.3 → попадает в by_status
    assert left_only["block_links_by_status"] == {"unmatched": 1}
    right_only = by_id["pl_right_only_3"]
    assert right_only["left_page_number"] is None
    assert right_only["right_page_number"] == 3
    s = rep["summary"]
    assert s["matched_page_links"] == 2
    assert s["one_sided_page_links"] == 2
    assert s["page_links_total"] == 4


def test_page_link_by_status_consistent_with_grouping():
    """Killer-тест ревью: block_links_by_status считается из ТЕХ ЖЕ ids,
    что и block_link_ids (falsy match_id не даёт orphan-подсчёта)."""
    bm = _block_matching()
    bm["page_matches"].append(
        {"match_id": None, "left_page_number": 9, "right_page_number": 9,
         "method": "page_number", "score": 0.1, "confidence": "weak",
         "risk_flags": []})
    orphan = _bmatch("bm_orphan", "L_T1", "R_T1")
    orphan["page_match_id"] = None
    bm["block_matches"].append(orphan)
    rep = blp.build_block_link_preview(_model("left"), _model("right"), bm)
    broken = [p for p in rep["page_links"] if p["page_link_id"] is None]
    assert len(broken) == 1
    assert broken[0]["block_link_ids"] == []
    assert broken[0]["block_links_by_status"] == {}


# ─── endpoint (9-12) ─────────────────────────────────────────────────────────

_SID = "sessblp1234"


@pytest.fixture()
def comparison_root(tmp_path, monkeypatch):
    root = tmp_path / "comparison_blp_endpoint"
    root.mkdir()
    monkeypatch.setenv("COMPARISON_ROOT", str(root))
    yield root


@pytest.fixture()
def client(comparison_root):
    from backend.app.api.routers import stage_comparison as router_mod
    app = FastAPI()
    app.include_router(router_mod.router)
    return TestClient(app)


def _svc():
    from backend.app.services.stage_comparison import pipeline_v2_payload_service
    return pipeline_v2_payload_service


def _art_dir(root: Path, sid: str = _SID, pair_id=None) -> Path:
    base = root / "sessions" / sid
    if pair_id:
        base = base / "pairs" / pair_id
    d = base / "pipeline_v2"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _seed(root: Path, *, sid: str = _SID, pair_id=None, with_ready=False,
          with_visual=False):
    d = _art_dir(root, sid, pair_id)
    (d / "left_normalized_document_model.json").write_text(
        json.dumps(_model("left"), ensure_ascii=False), encoding="utf-8")
    (d / "right_normalized_document_model.json").write_text(
        json.dumps(_model("right"), ensure_ascii=False), encoding="utf-8")
    (d / "block_matching_report.json").write_text(
        json.dumps(_block_matching(), ensure_ascii=False), encoding="utf-8")
    if with_visual:
        (d / "visual_equivalence_gate_report.json").write_text(
            json.dumps(_visual_gate(), ensure_ascii=False), encoding="utf-8")
    if with_ready:
        rep = _build()
        rep["summary"]["block_links_total"] = 777  # маркер «готовый с диска»
        (d / "block_link_preview_report.json").write_text(
            json.dumps(rep, ensure_ascii=False), encoding="utf-8")
    return d


def _tree_snapshot(root: Path) -> set:
    return {str(p.relative_to(root)) for p in root.rglob("*")}


def _url(sid: str = _SID, pair_id=None):
    base = f"/api/stage-comparison/pipeline-v2/{sid}/block-link-preview"
    return base + (f"?pair_id={pair_id}" if pair_id else "")


def test_9_endpoint_returns_ready_report(client, comparison_root):
    _seed(comparison_root, with_ready=True)
    r = client.get(_url())
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["source"] == "ready_report"
    assert body["payload"]["kind"] == blp.REPORT_KIND
    assert body["payload"]["summary"]["block_links_total"] == 777


def test_10_endpoint_builds_on_the_fly(client, comparison_root):
    _seed(comparison_root, with_visual=True)
    r = client.get(_url())
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["source"] == "built_from_artifacts"
    s = body["payload"]["summary"]
    assert s["block_links_total"] == 3
    assert s["visual_changed"] == 1     # visual gate подтянут on-the-fly
    # pair-level вариант
    _seed(comparison_root, pair_id="p1")
    body2 = client.get(_url(pair_id="p1")).json()
    assert body2["status"] == "ok"
    assert body2["pair_id"] == "p1"


def test_11_endpoint_not_found_without_artifacts(client, comparison_root):
    (comparison_root / "sessions" / _SID).mkdir(parents=True)
    r = client.get(_url())
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "not_found"
    assert body["available"] is False
    assert "not found" in body["message"]


def test_11b_not_found_lists_available_pairs(client, comparison_root):
    _seed(comparison_root, pair_id="p1")
    body = client.get(_url()).json()
    assert body["status"] == "not_found"
    assert body["available_pairs"] == ["p1"]


def test_12_endpoint_read_only_no_new_files(client, comparison_root):
    _seed(comparison_root)
    before = _tree_snapshot(comparison_root)
    body = client.get(_url()).json()
    assert body["status"] == "ok"
    # on-the-fly отчёт НЕ кешируется на диск, дерево не изменилось
    assert _tree_snapshot(comparison_root) == before
    # незнакомая сессия тоже ничего не материализует
    before2 = _tree_snapshot(comparison_root)
    assert client.get(_url("nosuchsession")).json()["status"] == "not_found"
    assert _tree_snapshot(comparison_root) == before2


def test_12b_no_network_during_discovery(comparison_root, monkeypatch):
    import socket
    _seed(comparison_root, with_ready=True)

    def _boom(*a, **k):  # pragma: no cover
        raise AssertionError("network access attempted by readonly service")

    monkeypatch.setattr(socket, "socket", _boom)
    monkeypatch.setattr(socket, "create_connection", _boom)
    assert _svc().discover_block_link_preview(_SID)["status"] == "ok"


def test_12c_invalid_session_id_is_400(client, comparison_root):
    r = client.get("/api/stage-comparison/pipeline-v2/%2E%2E/block-link-preview")
    assert r.status_code == 400


def test_12d_broken_artifact_is_error_not_500(client, comparison_root):
    d = _art_dir(comparison_root)
    (d / "block_matching_report.json").write_text("{broken json",
                                                  encoding="utf-8")
    r = client.get(_url())
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "error"
    assert body["warnings"]


def test_12e_ready_report_with_wrong_kind_rebuilt(client, comparison_root):
    d = _seed(comparison_root)
    (d / "block_link_preview_report.json").write_text(
        json.dumps({"kind": "something_else"}), encoding="utf-8")
    body = client.get(_url()).json()
    assert body["status"] == "ok"
    assert body["source"] == "built_from_artifacts"
    assert any("unexpected kind" in w for w in body["warnings"])


def test_12f_partial_artifacts_are_error_not_not_found(client, comparison_root):
    """Killer-тест ревью: половина dry-run (block_matching есть, моделей
    нет) — это error с объяснением, а не «Pipeline V2 не запускался»."""
    d = _art_dir(comparison_root)
    (d / "block_matching_report.json").write_text(
        json.dumps(_block_matching(), ensure_ascii=False), encoding="utf-8")
    body = client.get(_url()).json()
    assert body["status"] == "error"
    assert any("inputs incomplete" in w for w in body["warnings"])
    # не-объектный required артефакт — тоже error
    d2 = _art_dir(comparison_root, sid="sessblp5678")
    (d2 / "left_normalized_document_model.json").write_text(
        json.dumps(_model("left")), encoding="utf-8")
    (d2 / "right_normalized_document_model.json").write_text(
        json.dumps(_model("right")), encoding="utf-8")
    (d2 / "block_matching_report.json").write_text("[1, 2]", encoding="utf-8")
    body2 = client.get(_url("sessblp5678")).json()
    assert body2["status"] == "error"
    assert any("expected JSON object" in w for w in body2["warnings"])


def test_12g_available_pairs_only_lists_buildable_pairs(client, comparison_root):
    """Killer-тест ревью: not_found не рекламирует пару, которая сама
    ответила бы not_found/error."""
    # пара только с block_matching — НЕ buildable
    d = _art_dir(comparison_root, pair_id="p_partial")
    (d / "block_matching_report.json").write_text(
        json.dumps(_block_matching()), encoding="utf-8")
    # полная пара — buildable
    _seed(comparison_root, pair_id="p_full")
    body = client.get(_url()).json()
    assert body["status"] == "not_found"
    assert body["available_pairs"] == ["p_full"]


def test_12h_mutated_pair_id_is_rejected_400(client, comparison_root):
    """pair_id, который _safe_id переписал бы, отклоняется — не резолвится
    молча в чужую пару."""
    _seed(comparison_root, pair_id="px")
    # 'p x' и 'p/../x' оба схлопнулись бы _safe_id → отклоняем
    r = client.get(_url(pair_id="p%20x"))
    assert r.status_code == 400
    r2 = client.get(_url(pair_id="p..x"))
    assert r2.status_code == 400


def test_12i_deeply_nested_ready_report_is_truncated_not_500(client,
                                                             comparison_root):
    d = _art_dir(comparison_root)
    deep = {"kind": blp.REPORT_KIND}
    cur = deep
    for _ in range(3000):
        cur["n"] = {}
        cur = cur["n"]
    import sys
    old = sys.getrecursionlimit()
    sys.setrecursionlimit(100000)
    try:
        (d / "block_link_preview_report.json").write_text(
            json.dumps(deep), encoding="utf-8")
    finally:
        sys.setrecursionlimit(old)
    r = client.get(_url())
    assert r.status_code == 200
    body = r.json()
    # глубина обрезана санитайзером (warning), а не 500
    assert body["status"] in ("ok", "error")
    if body["status"] == "ok":
        assert any("sanitized" in w for w in body["warnings"])


# ─── 15: офлайн-гарантии ─────────────────────────────────────────────────────


def test_15_no_vision_or_external_model_imports():
    for mod_file in (Path(blp.__file__), Path(_svc().__file__)):
        src = mod_file.read_text(encoding="utf-8").lower()
        for forbidden in ("qwen", "opus", "claude", "llm_runner",
                          "graphic_llm", "text_llm", "subprocess",
                          "httpx", "requests", "urllib"):
            assert forbidden not in src, \
                f"{mod_file.name} references {forbidden!r}"


# ─── dry-run интеграция ──────────────────────────────────────────────────────


def _result_json(tmp_path: Path, name: str) -> Path:
    """Минимальный prepared result.json для dry-run."""
    payload = {
        "pages": [
            {"page_number": 1, "width": 1000, "height": 700, "blocks": [
                {"block_id": f"{name}_b1", "block_type": "text",
                 "coords_norm": [0.1, 0.1, 0.9, 0.3],
                 "ocr_text": "ЩР-1 кабель ВВГнг 5x10"},
            ]},
        ],
    }
    p = tmp_path / f"{name}_result.json"
    p.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return p


def _run_dry(tmp_path, out_name="out", options=None):
    from backend.app.services.stage_comparison.pipeline_v2_dry_run import (
        run_pipeline_v2_dry_run,
    )
    left = _result_json(tmp_path, "left")
    right = _result_json(tmp_path, "right")
    out = tmp_path / out_name
    summary = run_pipeline_v2_dry_run(
        {"result_json_path": str(left)}, {"result_json_path": str(right)},
        out, options=options)
    return summary, out


def test_dry_run_writes_block_link_preview_artifact(tmp_path):
    summary, out = _run_dry(tmp_path)
    rep_path = out / "block_link_preview_report.json"
    assert rep_path.is_file()
    rep = json.loads(rep_path.read_text(encoding="utf-8"))
    assert rep["kind"] == blp.REPORT_KIND
    sec = summary["block_link_preview"]
    assert sec["enabled"] is True
    assert sec["status"] == "ok"
    assert sec["block_links_total"] == rep["summary"]["block_links_total"]
    # артефакт в манифесте
    manifest = json.loads((out / "pipeline_v2_manifest.json")
                          .read_text(encoding="utf-8"))
    names = {a["filename"] if isinstance(a, dict) else a
             for a in manifest.get("artifacts", [])}
    assert any("block_link_preview_report.json" in str(n) for n in names) or \
        "block_link_preview" in json.dumps(manifest)
    # MD-раздел
    md = (out / "pipeline_v2_summary.md").read_text(encoding="utf-8")
    assert "Block link preview" in md


def test_dry_run_block_link_preview_fail_soft(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import pipeline_v2_dry_run as dr

    def _boom(*a, **k):
        raise RuntimeError("blp exploded")

    monkeypatch.setattr(dr, "build_block_link_preview", _boom)
    summary, out = _run_dry(tmp_path)
    assert summary["status"] in ("ok", "completed_with_warnings")
    assert summary["block_link_preview"]["status"] == "failed"
    assert "blp exploded" in summary["block_link_preview"]["error"]
    assert any("block_link_preview" in w for w in summary["warnings"])
    assert not (out / "block_link_preview_report.json").exists()
    # downstream этапы не пострадали
    assert (out / "entity_diff_report.json").is_file()


def test_dry_run_block_link_preview_can_be_disabled(tmp_path):
    summary, out = _run_dry(
        tmp_path, options={"block_link_preview": {"enabled": False}})
    assert summary["block_link_preview"]["enabled"] is False
    assert summary["block_link_preview"]["status"] == "disabled"
    assert not (out / "block_link_preview_report.json").exists()


def test_dry_run_text_only_pair_has_no_blp_warnings(tmp_path):
    """Killer-тест ревью: пустой visual gate выход на text-only паре —
    норма; benign warning не должен деградировать статус dry-run."""
    summary, _ = _run_dry(tmp_path)
    assert not [w for w in summary["warnings"]
                if str(w).startswith("block_link_preview:")]


def test_dry_run_visual_gate_wiring_reaches_disk_artifact(tmp_path, monkeypatch):
    """Killer-тест ревью: [3c] обязан получать ve_report — иначе on-disk
    отчёт (который endpoint предпочитает) навсегда без visual-полей."""
    from backend.app.services.stage_comparison import pipeline_v2_dry_run as dr

    def _stub_gate(left_model, right_model, block_report, **kw):
        pair = (block_report.get("block_matches") or [{}])[0]
        return {"kind": "stage_comparison_pipeline_v2_visual_equivalence_gate",
                "status": "ok", "summary": {},
                "block_pairs": [{
                    "left_block_id": pair.get("left_block_id"),
                    "right_block_id": pair.get("right_block_id"),
                    "status": "changed_visual", "decision": "manual_review",
                    "risk_flags": [],
                    "metrics": {"mask_iou": 0.1,
                                "normalized_correlation": 0.2,
                                "total_diff_ratio": 0.3,
                                "alignment_method": "ecc_euclidean"}}],
                "warnings": []}

    monkeypatch.setattr(dr, "run_visual_equivalence_gate", _stub_gate)
    _, out = _run_dry(tmp_path)
    rep = json.loads((out / "block_link_preview_report.json")
                     .read_text(encoding="utf-8"))
    links = rep["block_links"]
    assert links, "synthetic pair must produce at least one block link"
    target = links[0]
    assert target["visual_status"] == "changed_visual"
    assert target["visual_decision"] == "manual_review"
    assert target["visual_metrics"]["mask_iou"] == 0.1
    assert target["link_status"] == "manual_review"
    assert rep["summary"]["visual_gate_available"] is True
    assert rep["summary"]["visual_changed"] == 1


def test_dry_run_graphic_readiness_plumbing_reaches_disk_artifact(
        tmp_path, monkeypatch):
    """Killer-тест ревью: left/right_graphic_report прокинуты в [3c] —
    low-readiness флаги доходят до on-disk отчёта."""
    from backend.app.services.stage_comparison import pipeline_v2_dry_run as dr

    real_builder = dr.build_graphic_descriptor_report

    def _stub_graphic(model, *, side, options=None):
        rep = real_builder(model, side=side, options=options)
        block_id = next(iter((model.get("blocks") or {}).keys()), None)
        rep = dict(rep if isinstance(rep, dict) else {})
        rep["descriptors"] = [{
            "block_id": block_id,
            "diff_readiness": {"usable_for_diff": False, "readiness": "low"},
        }]
        return rep

    monkeypatch.setattr(dr, "build_graphic_descriptor_report", _stub_graphic)
    _, out = _run_dry(tmp_path)
    rep = json.loads((out / "block_link_preview_report.json")
                     .read_text(encoding="utf-8"))
    assert rep["block_links"], "synthetic pair must produce a block link"
    flags = rep["block_links"][0]["risk_flags"]
    assert "left_readiness_low" in flags
    assert "right_readiness_low" in flags
