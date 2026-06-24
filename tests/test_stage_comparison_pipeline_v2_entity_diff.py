# -*- coding: utf-8 -*-
"""Тесты для Pipeline V2 — Deterministic Entity Diff (этап 4).

Synthetic entity_extraction_report (этап 3). Никаких реальных файлов, сети,
Qwen/Opus.

Покрываемые spec-кейсы 1..22 (см. строки тестов).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import pipeline_v2_entity_diff as ed
from backend.app.services.stage_comparison import pipeline_v2_entity_extraction as ee
from backend.app.services.stage_comparison import pipeline_v2_block_matching as bm
from backend.app.services.stage_comparison import pipeline_v2_prepared_ingest as pv1


_GROUP = {
    "stamp_field": "stamp", "norm_reference": "text", "requirement": "text",
    "document_section": "text", "equipment": "equipment", "cable": "cable",
    "power_supply": "power", "scheme_component": "scheme",
    "scheme_connection_hint": "scheme", "table_row": "table",
    "contents_item": "contents", "change_log_item": "change_log", "unknown": "unknown",
}


def _e(eid, etype, side, *, value="", name="", subject="", unit="", fields=None,
       block_id=None, page_number=1, quote="q", source="text_excerpt"):
    block_id = block_id or ("L" if side == "left" else "R")
    return {
        "entity_id": eid, "entity_type": etype, "semantic_group": _GROUP.get(etype, "unknown"),
        "side": side, "document_code": "DC", "page_number": page_number,
        "page_type": "scheme", "block_id": block_id, "block_semantic_type": "text",
        "subject": subject, "name": name, "value": value, "unit": unit,
        "fields": fields or {}, "confidence": 0.6,
        "evidence": {"quote": quote, "source": source, "block_id": block_id,
                     "page_number": page_number},
        "quality_flags": [],
    }


def _report(left_ents, right_ents, *, lblock="L", rblock="R"):
    return {
        "version": 1, "kind": "stage_comparison_pipeline_v2_entity_extraction",
        "left": {"document_code": "DC", "entities_total": len(left_ents)},
        "right": {"document_code": "DC", "entities_total": len(right_ents)},
        "summary": {},
        "left_entities": left_ents, "right_entities": right_ents,
        "matched_block_entities": [{
            "block_match_id": f"bm_{lblock}__{rblock}",
            "left_block_id": lblock, "right_block_id": rblock,
            "left_entities": [e["entity_id"] for e in left_ents],
            "right_entities": [e["entity_id"] for e in right_ents],
            "entity_type_counts": {}, "quality_flags": [],
        }],
        "unmatched_left_block_entities": [], "unmatched_right_block_entities": [],
        "warnings": [],
    }


def _diff(left_ents, right_ents):
    return ed.diff_entity_extraction_report(_report(left_ents, right_ents))


def _deltas_for(rep, left_id=None, right_id=None):
    out = []
    for d in rep["deltas"]:
        if left_id and d["left_entity_id"] != left_id:
            continue
        if right_id and d["right_entity_id"] != right_id:
            continue
        out.append(d)
    return out


# ─── tests ──────────────────────────────────────────────────────────────────


def test_1_stamp_field_changed():
    rep = _diff([_e("l1", "stamp_field", "left", subject="stage", value="П")],
                [_e("r1", "stamp_field", "right", subject="stage", value="Р")])
    ds = [d for d in rep["deltas"] if d["delta_type"] == "changed"]
    assert len(ds) == 1
    assert ds[0]["field"] == "value"
    assert "stamp_field_changed" in ds[0]["quality_flags"]
    assert ds[0]["confidence"] >= 0.75


def test_2_stamp_field_unchanged():
    rep = _diff([_e("l1", "stamp_field", "left", subject="stage", value="П")],
                [_e("r1", "stamp_field", "right", subject="stage", value=" п ")])
    assert rep["deltas"] == []
    assert rep["summary"]["matched_unchanged_total"] == 1


def test_3_added_entity():
    rep = _diff([], [_e("r1", "equipment", "right", value="видеорегистратор")])
    assert rep["summary"]["added_total"] == 1
    assert rep["deltas"][0]["delta_type"] == "added"
    assert rep["deltas"][0]["right_entity_id"] == "r1"


def test_4_removed_entity():
    rep = _diff([_e("l1", "equipment", "left", value="коммутатор")], [])
    assert rep["summary"]["removed_total"] == 1
    assert rep["deltas"][0]["delta_type"] == "removed"
    assert rep["deltas"][0]["left_entity_id"] == "l1"


def test_5_norm_unchanged_after_normalize():
    rep = _diff([_e("l1", "norm_reference", "left", value="СП 256.1325800.2016")],
                [_e("r1", "norm_reference", "right", value="сп   256.1325800.2016")])
    assert rep["deltas"] == []


def test_6_norm_changed_edition():
    rep = _diff([_e("l1", "norm_reference", "left", value="СП 256.1325800.2016")],
                [_e("r1", "norm_reference", "right", value="СП 256.1325800.2020")])
    ds = [d for d in rep["deltas"] if d["delta_type"] == "changed"]
    assert len(ds) == 1
    assert "2016" in ds[0]["old_value"] and "2020" in ds[0]["new_value"]


def test_7_equipment_poe_synonym_unchanged():
    rep = _diff([_e("l1", "equipment", "left", value="PoE-коммутатор")],
                [_e("r1", "equipment", "right", value="POE-Коммутатор")])
    assert rep["deltas"] == []


def test_8_equipment_added():
    rep = _diff([_e("l1", "equipment", "left", value="коммутатор")],
                [_e("r1", "equipment", "right", value="коммутатор"),
                 _e("r2", "equipment", "right", value="ИБП")])
    added = [d for d in rep["deltas"] if d["delta_type"] == "added"]
    assert len(added) == 1 and added[0]["right_entity_id"] == "r2"


def test_9_cable_unchanged_after_normalize():
    rep = _diff([_e("l1", "cable", "left", value="LAN U/UTP cat. 5Е")],
                [_e("r1", "cable", "right", value="UTP cat.5e")])
    assert rep["deltas"] == []


def test_10_cable_section_changed():
    rep = _diff([_e("l1", "cable", "left", value="КПСВВнг(А)-LS 1x2x0,5")],
                [_e("r1", "cable", "right", value="КПСВВнг(А)-LS 1x2x1.0")])
    ds = [d for d in rep["deltas"] if d["delta_type"] == "changed"]
    assert len(ds) == 1
    assert "numeric_change" in ds[0]["quality_flags"]


def test_11_power_unchanged_after_normalize():
    rep = _diff([_e("l1", "power_supply", "left", value="220В")],
                [_e("r1", "power_supply", "right", value="220 В")])
    assert rep["deltas"] == []


def test_12_power_changed():
    rep = _diff([_e("l1", "power_supply", "left", value="12В")],
                [_e("r1", "power_supply", "right", value="24В")])
    ds = [d for d in rep["deltas"] if d["delta_type"] == "changed"]
    assert len(ds) == 1
    assert "numeric_change" in ds[0]["quality_flags"]


def test_13_contents_page_changed():
    lf = {"document_code": "DC", "sheet_name": "Структурная схема СОВ", "page_or_note": "21"}
    rf = {"document_code": "DC", "sheet_name": "Структурная схема СОВ", "page_or_note": "32"}
    rep = _diff([_e("l1", "contents_item", "left", name="Структурная схема СОВ", fields=lf)],
                [_e("r1", "contents_item", "right", name="Структурная схема СОВ", fields=rf)])
    ds = [d for d in rep["deltas"] if d["delta_type"] == "changed"]
    assert len(ds) == 1
    assert ds[0]["field"] == "fields.page_or_note"
    assert ds[0]["old_value"] == "21" and ds[0]["new_value"] == "32"


def test_14_change_log_description_changed():
    lf = {"change_no": "1", "sheet": "5", "description": "Добавлен щит СОВ"}
    rf = {"change_no": "1", "sheet": "5", "description": "Изменена трасса кабеля"}
    rep = _diff([_e("l1", "change_log_item", "left", name="1", fields=lf)],
                [_e("r1", "change_log_item", "right", name="1", fields=rf)])
    ds = [d for d in rep["deltas"] if d["delta_type"] == "changed"]
    assert len(ds) == 1
    assert ds[0]["field"] == "fields.description"


def test_15_table_row_changed_flag():
    rep = _diff([_e("l1", "table_row", "left", value="Поз 1 | 100 шт",
                    fields={"cells": ["Поз 1", "100 шт"]})],
                [_e("r1", "table_row", "right", value="Поз 1 | 200 шт",
                    fields={"cells": ["Поз 1", "200 шт"]})])
    ds = [d for d in rep["deltas"] if d["delta_type"] == "changed"]
    assert len(ds) == 1
    assert "table_row_changed" in ds[0]["quality_flags"]


def test_16_requirement_fuzzy_numeric_change():
    rep = _diff([_e("l1", "requirement", "left",
                    value="Кабель прокладывается на высоте 2.5 м от уровня пола")],
                [_e("r1", "requirement", "right",
                    value="Кабель прокладывается на высоте 3.0 м от уровня пола")])
    ds = [d for d in rep["deltas"] if d["delta_type"] == "changed"]
    assert len(ds) == 1
    assert "numeric_change" in ds[0]["quality_flags"]
    assert "fuzzy_match" in ds[0]["quality_flags"]


def test_17_one_sided_low_info_unknown_not_high_confidence():
    rep = _diff([_e("l1", "unknown", "left", value="", quote="")], [])
    assert len(rep["deltas"]) == 1
    d = rep["deltas"][0]
    assert d["delta_type"] == "uncertain"
    assert d["confidence"] < 0.45
    assert rep["summary"]["by_confidence"]["high"] == 0


# ─── mixed report for 18/19 ─────────────────────────────────────────────────


def _mixed_report():
    left = [
        _e("l_stage", "stamp_field", "left", subject="stage", value="П"),
        _e("l_eq", "equipment", "left", value="коммутатор"),
        _e("l_cab", "cable", "left", value="КПСВВнг(А)-LS 1x2x0,5"),
    ]
    right = [
        _e("r_stage", "stamp_field", "right", subject="stage", value="Р"),   # changed
        _e("r_eq", "equipment", "right", value="коммутатор"),                # unchanged
        _e("r_cab", "cable", "right", value="КПСВВнг(А)-LS 1x2x1.0"),         # changed
        _e("r_new", "equipment", "right", value="видеорегистратор"),         # added
    ]
    return ed.diff_entity_extraction_report(_report(left, right))


def test_18_block_summaries():
    rep = _mixed_report()
    assert len(rep["block_summaries"]) == 1
    bs = rep["block_summaries"][0]
    assert bs["block_match_id"] == "bm_L__R"
    assert bs["changed_total"] == 2   # stage + cable
    assert bs["added_total"] == 1     # видеорегистратор
    assert bs["deltas_total"] == 3


def test_19_summary_counts():
    rep = _mixed_report()
    s = rep["summary"]
    assert s["changed_total"] == 2
    assert s["added_total"] == 1
    assert s["matched_unchanged_total"] == 1   # коммутатор
    assert s["matched_entities_total"] == 3    # stage, eq, cable matched
    assert "stamp_field" in s["by_entity_type"]
    assert "cable" in s["by_entity_type"]
    assert set(s["by_confidence"].keys()) == {"high", "medium", "low"}
    assert sum(s["by_delta_type"].values()) == s["deltas_total"]


def test_20_write_report_valid_json(tmp_path: Path):
    rep = _mixed_report()
    out = tmp_path / "sub" / "entity_diff_report.json"
    returned = ed.write_entity_diff_report(out, rep)
    assert returned == out and out.exists()
    reloaded = json.loads(out.read_text(encoding="utf-8"))
    assert reloaded["kind"] == ed.REPORT_KIND
    assert reloaded == rep


def test_21_no_network_and_no_llm(monkeypatch):
    import socket

    def _boom(*a, **k):  # pragma: no cover
        raise AssertionError("network access attempted in pipeline_v2 entity diff")

    monkeypatch.setattr(socket, "socket", _boom)
    monkeypatch.setattr(socket, "create_connection", _boom)

    rep = _mixed_report()
    assert rep["summary"]["deltas_total"] > 0

    src = Path(ed.__file__).read_text(encoding="utf-8")
    for forbidden in ("import requests", "import httpx", "urllib.request",
                      "import socket", "graphic_llm", "text_llm_provider",
                      "claude -p", "ClaudeCodeProvider", "qwen", "opus"):
        assert forbidden not in src, f"module references {forbidden!r}"


def test_22_integration_full_chain(tmp_path: Path):
    """result_json → normalize → match → extract → diff."""
    def _rj(stage):
        return {
            "pdf_path": "/tmp/x.pdf",
            "pages": [
                {"page_number": 1, "width": 2000, "height": 1400, "blocks": [
                    {"id": "stamp1", "block_type": "text", "source": "user",
                     "coords_px": [1400, 1200, 1990, 1390],
                     "coords_norm": [0.7, 0.85, 0.99, 0.99],
                     "ocr_text": "",
                     "ocr_json": {"document_code": "DC", "organization": "ORG",
                                  "project_name": "PN", "stage": stage,
                                  "sheet_number": "1", "sheet_name": "Схема СОВ",
                                  "total_sheets": "10"},
                     "stamp_data": {"document_code": "DC", "organization": "ORG",
                                    "project_name": "PN", "stage": stage,
                                    "sheet_number": "1", "sheet_name": "Схема СОВ",
                                    "total_sheets": "10"}},
                    {"id": "txt1", "block_type": "text", "source": "user",
                     "coords_px": [10, 10, 1000, 400],
                     "coords_norm": [0.005, 0.007, 0.5, 0.28],
                     "ocr_text": "Видеонаблюдение выполняется кабелем UTP cat.5e. "
                                 "Электропитание 220В.",
                     "stamp_data": {"document_code": "DC", "sheet_name": "Схема СОВ",
                                    "sheet_number": "1"}},
                ]},
            ],
        }

    lp = tmp_path / "old_result.json"
    rp = tmp_path / "new_result.json"
    lp.write_text(json.dumps(_rj("П"), ensure_ascii=False), encoding="utf-8")
    rp.write_text(json.dumps(_rj("Р"), ensure_ascii=False), encoding="utf-8")

    left = pv1.build_normalized_document_model(lp)
    right = pv1.build_normalized_document_model(rp)
    bmr = bm.match_normalized_documents(left, right)
    ent = ee.extract_entities_for_matched_documents(left, right, bmr)
    rep = ed.diff_entity_extraction_report(ent)

    assert rep["summary"]["deltas_total"] > 0
    # stage П→Р даёт changed stamp_field delta
    stage_deltas = [d for d in rep["deltas"]
                    if d["entity_type"] == "stamp_field" and d["subject"] == "stage"]
    assert len(stage_deltas) == 1
    assert stage_deltas[0]["delta_type"] == "changed"
    assert stage_deltas[0]["old_value"] == "П" and stage_deltas[0]["new_value"] == "Р"


# ─── доп. юнит-проверки чистых функций ───────────────────────────────────────


def test_normalizers_and_keys():
    assert ed.normalize_power_value("220 В") == ed.normalize_power_value("220В")
    assert ed.normalize_cable_value("LAN U/UTP cat. 5Е") == ed.normalize_cable_value("UTP cat.5e")
    assert ed.extract_numeric_tokens("1x2x0,5") == ["1", "2", "0.5"]
    k1 = ed.make_entity_match_key({"entity_type": "power_supply", "value": "220В"})
    k2 = ed.make_entity_match_key({"entity_type": "power_supply", "value": "220 В"})
    assert k1 == k2
    id1 = ed.make_entity_identity_key({"entity_type": "cable", "value": "КПСВВ-LS 1x2x0,5"})
    id2 = ed.make_entity_identity_key({"entity_type": "cable", "value": "КПСВВ-LS 1x2x1.0"})
    assert id1 == id2  # одно семейство кабеля


# ─── power unit cleanup (2026-06-10) ─────────────────────────────────────────


def test_23_power_unit_asymmetry_is_not_delta():
    # тот же номинал, но unit выставила только одна сторона (разные пути
    # извлечения) — это артефакт экстракции, дельты «'' → 'В'» быть не должно
    rep = _diff([_e("l1", "power_supply", "left", value="12В", unit="")],
                [_e("r1", "power_supply", "right", value="12В", unit="В")])
    assert rep["summary"]["deltas_total"] == 0
    assert rep["summary"]["matched_unchanged_total"] == 1


def test_24_no_empty_to_unit_deltas_ever():
    rep = _diff(
        [_e("l1", "power_supply", "left", value="220В", unit="В"),
         _e("l2", "power_supply", "left", value="12В", unit="")],
        [_e("r1", "power_supply", "right", value="220В", unit=""),
         _e("r2", "power_supply", "right", value="12В", unit="В")])
    for d in rep["deltas"]:
        assert not (d["field"] == "unit"
                    and (not d["old_value"] or not d["new_value"]))


def test_25_power_unit_real_change_still_delta():
    # обе стороны выставили unit и он реально различается → дельта остаётся
    rep = _diff([_e("l1", "power_supply", "left", value="0.5", unit="А")],
                [_e("r1", "power_supply", "right", value="0.5", unit="В")])
    unit_deltas = [d for d in rep["deltas"] if d["field"] == "unit"]
    assert len(unit_deltas) == 1
    assert unit_deltas[0]["old_value"] == "А" and unit_deltas[0]["new_value"] == "В"


def test_26_power_spacing_unchanged_and_real_change_kept():
    # «220 В» ↔ «220В» — не дельта; «12В» → «24В» — дельта (spec-кейсы 11/12
    # переутверждены после cleanup)
    rep = _diff([_e("l1", "power_supply", "left", value="220 В", unit="В")],
                [_e("r1", "power_supply", "right", value="220В", unit="В")])
    assert rep["summary"]["deltas_total"] == 0
    rep = _diff([_e("l1", "power_supply", "left", value="12В", unit="В")],
                [_e("r1", "power_supply", "right", value="24В", unit="В")])
    changed = [d for d in rep["deltas"] if d["delta_type"] == "changed"]
    assert len(changed) == 1
    assert changed[0]["old_value"] == "12В" and changed[0]["new_value"] == "24В"


# ─── ambiguous power tokens не дают дельт (АР2 cleanup, 2026-06-10) ──────────


def _axis_label_model(token_texts, key_entities):
    blocks = [{
        "block_id": "B1", "page_number": 2, "page_index": 1,
        "block_type": "image", "semantic_type": "plan",
        "coords_px": [100, 100, 500, 500], "coords_norm": [0.1, 0.1, 0.5, 0.5],
        "shape_type": "rectangle", "source": "user",
        "crop_url": "https://r2.example.com/b1.pdf", "image_file": None,
        "has_crop_pdf": True, "has_image_file": False,
        "has_pdfplumber_text": False, "has_ocr_json": True,
        "has_stamp_data": False,
        "text_excerpt": token_texts, "pdfplumber_text_excerpt": "",
        "stamp_data": {}, "quality_flags": [],
        "ocr_json_summary": {"content_summary": "", "detailed_description": "",
                             "key_entities": list(key_entities)},
    }]
    return {
        "version": 1, "kind": "stage_comparison_pipeline_v2_normalized_document",
        "source": {}, "document": {"document_code": "DC", "pages_total": 1},
        "summary": {},
        "pages": [{"page_number": 2, "page_index": 1, "width": 1000,
                   "height": 700, "sheet_number": "", "total_sheets": "",
                   "sheet_name": "План", "document_code": "DC",
                   "page_type": "scheme", "blocks": ["B1"]}],
        "blocks": {b["block_id"]: b for b in blocks}, "warnings": [],
    }


def test_27_no_power_delta_from_axis_label_4v():
    # «4 в» (осевая метка) больше не извлекается как power_supply →
    # one-sided power-дельты по ней не существует; полный chain extract→diff.
    # Right ГЕНУИННО без «4 в» (и в тексте, и в key_entities) — на коде до
    # фикса этот вход давал one-sided removed power-дельту.
    left = _axis_label_model("Фасад 4 в осях 1-5", ["4 в"])
    right = _axis_label_model("Фасад в осях 1-5", [])
    bmr = bm.match_normalized_documents(left, right)
    ent = ee.extract_entities_for_matched_documents(left, right, bmr)
    rep = ed.diff_entity_extraction_report(ent)
    assert [d for d in rep["deltas"] if d["entity_type"] == "power_supply"] == []


def test_27b_axis_label_spacing_jitter_no_scheme_deltas():
    # тот же физический ярлык «4 в» / «4в» (OCR spacing-джиттер между
    # сторонами) — fallback компактизирует чистый номинал-токен, пары
    # removed+added по scheme_component не плодятся
    left = _axis_label_model("Фасад корпуса", ["4 в"])
    right = _axis_label_model("Фасад корпуса", ["4в"])
    bmr = bm.match_normalized_documents(left, right)
    ent = ee.extract_entities_for_matched_documents(left, right, bmr)
    rep = ed.diff_entity_extraction_report(ent)
    assert rep["summary"]["deltas_total"] == 0


def test_28_power_spacing_unchanged_and_change_kept_post_cleanup():
    # «220В» ↔ «220 В» — не дельта; «12В» → «24В» — дельта (переутверждение
    # spec-кейсов 10/11 после ambiguous-фильтра)
    rep = _diff([_e("l1", "power_supply", "left", value="220В", unit="В")],
                [_e("r1", "power_supply", "right", value="220 В", unit="В")])
    assert rep["summary"]["deltas_total"] == 0
    rep = _diff([_e("l1", "power_supply", "left", value="12В", unit="В")],
                [_e("r1", "power_supply", "right", value="24В", unit="В")])
    assert rep["summary"]["changed_total"] == 1


def test_29_latin_v_equals_cyrillic():
    assert ed.normalize_power_value("12V") == ed.normalize_power_value("12В")
    rep = _diff([_e("l1", "power_supply", "left", value="12V", unit="В")],
                [_e("r1", "power_supply", "right", value="12В", unit="В")])
    assert rep["summary"]["deltas_total"] == 0
