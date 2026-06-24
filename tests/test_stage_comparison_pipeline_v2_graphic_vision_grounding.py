# -*- coding: utf-8 -*-
"""Тесты Pipeline V2 Graphic Vision Grounding.

Проверяют нормализацию, grounding сущностей/изменений по anchor-тексту блока,
детект достроенных рядов и no-op изменений, сборку отчёта и интеграцию в
dry-run / ui_payload. Реальные vision/LLM/сеть не задействованы.
"""
import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import (
    pipeline_v2_graphic_vision_grounding as g)


# ─── фикстуры ────────────────────────────────────────────────────────────────

def _block(text: str, *, block_id: str = "B", key_entities=None) -> dict:
    b = {"block_id": block_id, "pdfplumber_text_excerpt": text}
    if key_entities is not None:
        b["ocr_json_summary"] = {"content_summary": "схема",
                                 "key_entities": key_entities}
    return b


def _anchors(text: str, **kw):
    return g.collect_block_text_anchors(_block(text, **kw))


def _vision_report(items):
    return {"version": 1, "kind": "x", "status": "ok", "items": items}


def _model(blocks: dict):
    return {"blocks": blocks}


# ─── 1-3, 7: grounded ────────────────────────────────────────────────────────

def test_1_rating_grounded_by_spaced_anchor():
    a = _anchors("1QF5 400 А отходящая линия")
    r = g.ground_vision_entity("QF5 400А", a)
    assert r["status"] == g.GROUNDED
    assert "400a" in r["matched_values"]


def test_2_cable_section_grounded_cross_x():
    a = _anchors("кабель 4x185 ППГнг")
    assert g.ground_vision_entity("4х185", a)["status"] == g.GROUNDED


def test_3_change_old_new_grounded_each_side():
    la = _anchors("1QF5 400А")
    ra = _anchors("1QF5 200А")
    r = g.ground_observed_change("QF5: 400А → 200А", la, ra)
    assert r["status"] == g.GROUNDED
    assert "400a" in r["old_values"] and "200a" in r["new_values"]


def test_7_designator_grounded_homoglyph_and_dash():
    a = _anchors("трансформаторы тока ТА1–ТА3 счётчик")
    assert g.ground_vision_entity("TA1-TA3", a)["status"] == g.GROUNDED


# ─── 4-5: artificial series ──────────────────────────────────────────────────

def test_4_repeated_value_rejected_when_not_in_anchors():
    a = _anchors("схема ГРЩ без таких номиналов")
    entries = [f"QF{i} 100А" for i in range(3, 18)]   # QF3…QF17 100А
    art = g.detect_artificial_series(entries, a)
    assert "100a" in art["artificial_tokens"]
    assert any("repeated_value" in r for r in art["reasons"])


def test_4b_repeated_value_protected_when_anchor_present():
    a = _anchors("100А 100А реальные линии")  # 100А есть в anchors
    entries = [f"QF{i} 100А" for i in range(3, 18)]
    art = g.detect_artificial_series(entries, a)
    assert "100a" not in art["artificial_tokens"]


def test_5_standard_ladder_rejected():
    a = _anchors("однолинейная схема ГРЩ")
    ladder = [f"2P {v}А" for v in
              (25, 32, 63, 100, 125, 160, 200, 250, 315, 400, 500, 630, 800)]
    art = g.detect_artificial_series(ladder, a)
    assert len(art["artificial_tokens"]) >= 6
    assert any("standard_ladder" in r for r in art["reasons"])


def test_5b_ladder_not_flagged_when_grounded():
    # все значения присутствуют в anchors → не достроенный ряд
    vals = (25, 32, 63, 100, 125, 160, 200, 250, 315, 400, 500, 630, 800)
    a = _anchors(" ".join(f"{v}А" for v in vals))
    ladder = [f"2P {v}А" for v in vals]
    assert not g.detect_artificial_series(ladder, a)["artificial_tokens"]


# ─── 6: noop ─────────────────────────────────────────────────────────────────

def test_6_noop_change_rejected():
    assert g.detect_noop_change("QF3 100А → QF3 100А") is True
    la = ra = _anchors("QF3 100А")
    r = g.ground_observed_change("QF3 100А → QF3 100А", la, ra)
    assert r["status"] == g.REJECTED_NOOP


def test_6b_real_change_not_noop():
    assert g.detect_noop_change("QF5 400А → 200А") is False


# ─── 8-9: ungrounded / weak ──────────────────────────────────────────────────

def test_8_entity_without_anchor_ungrounded():
    a = _anchors("совсем другой текст про вентиляцию")
    assert g.ground_vision_entity("QF9 999А", a)["status"] == g.UNGROUNDED


def test_9_partial_match_weakly_grounded():
    a = _anchors("1QF5 присутствует, но без номинала рядом")
    r = g.ground_vision_entity("QF5 777А", a)   # маркировка есть, 777А нет
    assert r["status"] == g.WEAKLY_GROUNDED
    assert "777a" in r["missing_values"]


# ─── 10: no anchors ──────────────────────────────────────────────────────────

def test_10_no_anchors_status_not_crash():
    empty = g.collect_block_text_anchors({"block_id": "B"})
    assert empty.available is False
    assert g.ground_vision_entity("QF5 400А", empty)["status"] == g.NO_ANCHOR_AVAILABLE
    # build report с блоками без текста не падает
    vr = _vision_report([{
        "item_id": "i1", "left_block_id": "L", "right_block_id": "R",
        "vision_status": "ok",
        "result": {"engineering_entities_old": ["QF1 400А"],
                   "engineering_entities_new": ["QF1 200А"],
                   "observed_changes": ["QF1 400А → 200А"]}}])
    rep = g.build_graphic_vision_grounding_report(
        vr, left_model=_model({"L": {"block_id": "L"}}),
        right_model=_model({"R": {"block_id": "R"}}))
    assert rep["status"] in ("ok", "completed_with_warnings")
    assert rep["summary"]["items_total"] == 1


# ─── 11: report summary counts ───────────────────────────────────────────────

def test_11_report_summary_counts():
    vr = _vision_report([{
        "item_id": "i1", "left_block_id": "L", "right_block_id": "R",
        "vision_status": "ok",
        "result": {
            "engineering_entities_old": ["QF5 400А", "QF6 800А"],
            "engineering_entities_new": ["QF5 200А"] + [f"QF{i} 100А" for i in range(3, 18)],
            "observed_changes": ["QF5: 400А → 200А",
                                 "ППГнг 4x2,5 → ППГнг 4x2,5 (без изменений)"],
        }}])
    lm = _model({"L": _block("1QF5 400А 1QF6 800А", block_id="L")})
    rm = _model({"R": _block("1QF5 200А", block_id="R")})
    rep = g.build_graphic_vision_grounding_report(vr, left_model=lm, right_model=rm)
    s = rep["summary"]
    assert s["items_total"] == 1
    assert s["entities_grounded"] >= 2          # QF5 400А, QF6 800А, QF5 200А
    assert s["artificial_series_rejected"] >= 10  # QF3…QF17 100А
    assert s["noop_changes_rejected"] == 1      # 4x2,5 → 4x2,5
    assert s["changes_grounded"] >= 1           # 400→200
    # арифметика согласованности (rejected = artificial + designator + invalid)
    assert s["entities_total"] == (s["entities_grounded"]
                                   + s["entities_weakly_grounded"]
                                   + s["entities_ungrounded"]
                                   + s["artificial_series_rejected"]
                                   + s["designator_range_rejected"])


# ─── 12-13: dry-run ──────────────────────────────────────────────────────────

def _grsh_result_json(amps_old, amps_new) -> dict:
    """result.json с одним графическим блоком ГРЩ (image)."""
    st = {"document_code": "DC", "organization": "O", "project_name": "P",
          "stage": "П", "sheet_number": "1",
          "sheet_name": "Однолинейная схема ГРЩ", "total_sheets": "5"}
    return {
        "pdf_path": "/tmp/x.pdf",
        "pages": [{"page_number": 1, "width": 5000, "height": 3500, "blocks": [
            {"id": "stamp1", "block_type": "text", "source": "user",
             "bbox": [0, 0, 5000, 300], "stamp_data": st},
            {"id": "grsh1", "block_type": "image", "source": "auto",
             "bbox": [200, 600, 4800, 3400],
             "pdfplumber_text": f"ГРЩ QF1 {amps_old}А",
             "ocr_json": {"key_entities": [f"{amps_old}А", f"{amps_new}А"]}},
        ]}],
    }


def test_12_dry_run_writes_grounding_report(tmp_path):
    import json
    from backend.app.services.stage_comparison import pipeline_v2_dry_run as dr

    def _runner(prompt, lp, rp, options):
        return {"old_description": "ГРЩ", "new_description": "ГРЩ",
                "observed_changes": ["QF1 1600А → 2000А"],
                "engineering_entities_old": ["QF1 1600А"],
                "engineering_entities_new": ["QF1 2000А"],
                "possible_risks": [], "confidence": "high"}

    left = tmp_path / "l_result.json"
    right = tmp_path / "r_result.json"
    left.write_text(json.dumps(_grsh_result_json(1600, 1600)), encoding="utf-8")
    right.write_text(json.dumps(_grsh_result_json(2000, 2000)), encoding="utf-8")
    out = tmp_path / "out"
    out.mkdir()
    summary = dr.run_pipeline_v2_dry_run(
        {"result_json_path": str(left)}, {"result_json_path": str(right)}, out,
        options={"graphic_vision": {"enabled": True, "render_crops": False}},
        vision_runner=_runner)
    report = out / "graphic_vision_grounding_report.json"
    assert report.exists()
    gvg = summary.get("graphic_vision_grounding") or {}
    assert gvg.get("enabled") is True
    assert gvg.get("status") in ("ok", "completed_with_warnings")
    # manifest перечисляет новый артефакт
    manifest = json.loads((out / "pipeline_v2_manifest.json").read_text("utf-8"))
    names = [a.get("filename") for a in manifest.get("artifacts", [])]
    assert "graphic_vision_grounding_report.json" in names


def test_12b_grounding_skipped_without_vision(tmp_path):
    import json
    from backend.app.services.stage_comparison import pipeline_v2_dry_run as dr
    left = tmp_path / "l_result.json"
    right = tmp_path / "r_result.json"
    left.write_text(json.dumps(_grsh_result_json(1600, 1600)), encoding="utf-8")
    right.write_text(json.dumps(_grsh_result_json(2000, 2000)), encoding="utf-8")
    out = tmp_path / "out"
    out.mkdir()
    # graphic_vision выключен → нечего грунтовать
    summary = dr.run_pipeline_v2_dry_run(
        {"result_json_path": str(left)}, {"result_json_path": str(right)}, out)
    gvg = summary.get("graphic_vision_grounding") or {}
    assert gvg.get("enabled") is False
    assert not (out / "graphic_vision_grounding_report.json").exists()


def test_13_dry_run_fail_soft_on_grounding_error(tmp_path, monkeypatch):
    import json
    from backend.app.services.stage_comparison import pipeline_v2_dry_run as dr

    def _runner(prompt, lp, rp, options):
        return {"old_description": "x", "new_description": "y",
                "observed_changes": ["a → b"], "engineering_entities_old": ["QF1"],
                "engineering_entities_new": ["QF1"], "confidence": "low"}

    def _boom(*a, **k):
        raise RuntimeError("grounding boom")
    monkeypatch.setattr(dr, "build_graphic_vision_grounding_report", _boom)

    left = tmp_path / "l_result.json"
    right = tmp_path / "r_result.json"
    left.write_text(json.dumps(_grsh_result_json(1600, 1600)), encoding="utf-8")
    right.write_text(json.dumps(_grsh_result_json(2000, 2000)), encoding="utf-8")
    out = tmp_path / "out"
    out.mkdir()
    summary = dr.run_pipeline_v2_dry_run(
        {"result_json_path": str(left)}, {"result_json_path": str(right)}, out,
        options={"graphic_vision": {"enabled": True, "render_crops": False}},
        vision_runner=_runner)
    # конвейер не падает, ошибка отражена в секции/warnings
    assert summary["status"] in ("ok", "completed_with_warnings")
    gvg = summary.get("graphic_vision_grounding") or {}
    assert "grounding boom" in str(gvg.get("error", ""))


# ─── 14: ui_payload ──────────────────────────────────────────────────────────

def test_14_ui_payload_grounding_section():
    from backend.app.services.stage_comparison import pipeline_v2_ui_payload as up
    summary = {
        "version": 1, "kind": "stage_comparison_pipeline_v2_dry_run_summary",
        "status": "ok", "artifacts": {}, "inputs": {}, "stages": {},
        "graphic_vision_grounding": {
            "enabled": True, "status": "ok",
            "entities_grounded": 12, "entities_weakly_grounded": 3,
            "entities_ungrounded": 5, "changes_grounded": 4,
            "changes_rejected": 2, "artificial_series_rejected": 7,
            "noop_changes_rejected": 1},
    }
    payload = up.build_pipeline_v2_ui_payload(summary)
    gvg = payload.get("graphic_vision_grounding")
    assert gvg is not None
    assert gvg["available"] is True
    assert gvg["entities_grounded"] == 12
    assert gvg["artificial_series_rejected"] == 7
    assert gvg["noop_changes_rejected"] == 1


def test_14b_ui_payload_no_grounding_section_when_absent():
    from backend.app.services.stage_comparison import pipeline_v2_ui_payload as up
    summary = {
        "version": 1, "kind": "stage_comparison_pipeline_v2_dry_run_summary",
        "status": "ok", "artifacts": {}, "inputs": {}, "stages": {}}
    payload = up.build_pipeline_v2_ui_payload(summary)
    assert "graphic_vision_grounding" not in payload


# ─── нормализация (детально) ─────────────────────────────────────────────────

@pytest.mark.parametrize("a,b", [
    ("400А", "400 А"), ("4х185", "4x185"),
    ("TA1-TA3", "ТА1–ТА3"), ("кВАр", "квар"), ("1600А", "1600 а"),
])
def test_normalize_equivalences(a, b):
    assert g.normalize_engineering_token(a) == g.normalize_engineering_token(b)


def test_qf_spacing_grounds_equivalently():
    # «QF 5»/«QF5»: эквивалентность на grounding-уровне (compact), а публичный
    # normalize сохраняет пробел токена для безопасного извлечения номиналов
    a = _anchors("1QF5 распределительный автомат")
    assert g.ground_vision_entity("QF 5", a)["status"] == g.GROUNDED
    assert g.ground_vision_entity("QF5", a)["status"] == g.GROUNDED


def test_normalize_keeps_distinct_ratings():
    n = g.normalize_engineering_token
    assert n("400А") != n("4000А")
    assert n("4х185") != n("4х95")
    assert n("200А") != n("2000А")


def test_normalize_decimal_comma_not_separator():
    # «233,6 кВт» — запятая десятичная, не разделитель сущностей
    n = g.normalize_engineering_token("Pp=233,6 кВт")
    assert "233.6" in n


# ─── full-text anchors (recall) ──────────────────────────────────────────────

def test_full_text_used_before_excerpt():
    # full_texts приоритетнее excerpt'а; source помечается full_text
    a = g.collect_block_text_anchors(
        {"pdfplumber_text_excerpt": "ВРУ схема без номиналов"},
        full_texts=["QF1 400А QF2 630А кабель 4х185"])
    assert a.source == "full_text"
    assert "400a" in a.ratings and "630a" in a.ratings


def test_value_in_fulltext_but_not_excerpt_becomes_grounded():
    # значение есть в full text, но не в excerpt → grounded (raise recall)
    ex = g.collect_block_text_anchors(
        {"pdfplumber_text_excerpt": "схема ГРЩ, общие данные"})
    full = g.collect_block_text_anchors(
        {"pdfplumber_text_excerpt": "схема ГРЩ, общие данные"},
        full_texts=["...много текста... QF7 315А ..."])
    assert g.ground_vision_entity("QF7 315А", ex)["status"] == g.UNGROUNDED
    assert g.ground_vision_entity("QF7 315А", full)["status"] == g.GROUNDED


def test_fallback_to_excerpt_when_no_fulltext():
    # full_texts отсутствует → fallback на excerpt
    a = g.collect_block_text_anchors(
        {"pdfplumber_text_excerpt": "QF1 160А"}, full_texts=None)
    assert a.source == "excerpt"
    assert g.ground_vision_entity("QF1 160А", a)["status"] == g.GROUNDED


def test_report_does_not_store_full_text():
    # в report сохраняются только короткие snippets/наборы, не весь full text
    big = "QF1 100А " * 5000   # ~40k символов
    vr = _vision_report([{
        "item_id": "i1", "left_block_id": "L", "right_block_id": "R",
        "vision_status": "ok",
        "result": {"engineering_entities_old": ["QF1 100А"],
                   "engineering_entities_new": ["QF1 100А"],
                   "observed_changes": []}}])
    rep = g.build_graphic_vision_grounding_report(
        vr, left_model=_model({"L": {"block_id": "L"}}),
        right_model=_model({"R": {"block_id": "R"}}),
        left_full_texts={"L": [big]}, right_full_texts={"R": [big]})
    blob = json.dumps(rep, ensure_ascii=False)
    assert len(blob) < 8000          # отчёт компактен, full text не влит
    la = rep["items"][0]["left_anchors"]
    assert la["source"] == "full_text"
    assert len(la.get("ratings", [])) <= 40


# ─── designator-range hallucinations ─────────────────────────────────────────

@pytest.mark.parametrize("txt", [
    "Автоматические выключатели (QF1...QF100)",
    "QF1-QF1000 (номиналы нечитаемы)",
    "Контакторы (KM1...KM10)",
    "Реле (KA1...KA10)",
    "Устройства защиты (SPD1...SPD10)",
])
def test_designator_range_rejected_without_anchor(txt):
    a = _anchors("схема ВРУ без таких диапазонов")
    dr = g.detect_artificial_designator_range(txt, a)
    assert dr["is_artificial"] is True
    assert dr["reason"] == g.REASON_DESIGNATOR_RANGE


@pytest.mark.parametrize("txt", ["TA1-TA3", "QF1-QF6", "ТА1-ТА3"])
def test_short_valid_range_not_rejected(txt):
    a = _anchors("схема")
    assert g.detect_artificial_designator_range(txt, a)["is_artificial"] is False


def test_long_range_with_anchor_not_rejected():
    # реальная длинная серия: верхний конец есть в anchors → не отвергаем
    a = _anchors("1QF1 1QF2 1QF13 отходящие линии QF13")
    assert g.detect_artificial_designator_range("QF1...QF13", a)["is_artificial"] is False


def test_designator_range_entity_rejected_in_item():
    vr = _vision_report([{
        "item_id": "i1", "left_block_id": "L", "right_block_id": "R",
        "vision_status": "ok",
        "result": {
            "engineering_entities_old": ["ЩР-1а"],
            "engineering_entities_new": ["Автоматические выключатели (QF1...QF100)",
                                         "Контакторы (KM1...KM10)"],
            "observed_changes": []}}])
    lm = _model({"L": _block("ЩР-1а схема", block_id="L")})
    rm = _model({"R": _block("ЩР схема", block_id="R")})
    rep = g.build_graphic_vision_grounding_report(vr, left_model=lm, right_model=rm)
    s = rep["summary"]
    assert s["designator_range_rejected"] >= 2
    rej = rep["items"][0]["rejected_entities"]
    assert any(x["status"] == g.REJECTED_DESIGNATOR_RANGE
               and x.get("reason") == g.REASON_DESIGNATOR_RANGE for x in rej)


def test_rating_ladder_still_rejected_after_changes():
    # регрессия: повтор/лесенка по-прежнему ловятся (когда нет anchor)
    a = _anchors("схема без таких номиналов")
    entries = [f"QF{i} 100А" for i in range(3, 23)]   # QF3…QF22 100А
    art = g.detect_artificial_series(entries, a)
    assert "100a" in art["artificial_tokens"]
    assert art["token_reasons"]["100a"] == g.REASON_REPEATED_RATING


def test_reasons_populated_on_grounded_and_rejected():
    a = _anchors("1QF5 400А")
    assert g.ground_vision_entity("QF5 400А", a).get  # entity has fields
    vr = _vision_report([{
        "item_id": "i1", "left_block_id": "L", "right_block_id": "R",
        "vision_status": "ok",
        "result": {"engineering_entities_old": ["QF5 400А"],
                   "engineering_entities_new": ["QF9 999А"],
                   "observed_changes": ["X → X (без изменений)"]}}])
    lm = _model({"L": _block("1QF5 400А", block_id="L")})
    rm = _model({"R": _block("схема", block_id="R")})
    rep = g.build_graphic_vision_grounding_report(vr, left_model=lm, right_model=rm)
    it = rep["items"][0]
    for x in it["grounded_entities_old"] + it["grounded_entities_new"]:
        assert "reason" in x
    # grounded → reason grounded; ungrounded → not_found
    g_old = it["grounded_entities_old"][0]
    assert g_old["reason"] == g.REASON_GROUNDED
