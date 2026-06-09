# -*- coding: utf-8 -*-
"""Тесты для Pipeline V2 — Entity Extraction (этап 3).

Synthetic normalized models + synthetic block_matching_report. Никаких реальных
файлов, сети, Qwen/Opus.

Покрываемые spec-кейсы:
  1.  stamp_data → stamp_field;
  2.  change_log table/text → change_log_item;
  3.  contents table/text → contents_item;
  4.  text-блок → requirement;
  5.  text-блок → norm_reference;
  6.  text-блок → equipment;
  7.  text-блок → cable;
  8.  text-блок → power_supply;
  9.  scheme-блок с key_entities → scheme_component/equipment/cable/power;
 10.  scheme-блок без key_entities → quality flag;
 11.  дедуп не плодит одинаковые cable/power в одном блоке;
 12.  matched block entities связаны с block_match_id;
 13.  unmatched block entities → unmatched_left/right;
 14.  summary считает by_entity_type/by_semantic_group/by_source;
 15.  write_entity_extraction_report пишет валидный JSON;
 16.  модуль без сети и без Qwen/Opus/provider-импортов;
 17.  интеграция: result_json → normalize → match → extract.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import pipeline_v2_entity_extraction as ee
from backend.app.services.stage_comparison import pipeline_v2_block_matching as bm
from backend.app.services.stage_comparison import pipeline_v2_prepared_ingest as pv1


# ─── builders ───────────────────────────────────────────────────────────────


def _blk(block_id, *, page_number, block_type, semantic_type,
         coords_norm=(0.1, 0.1, 0.5, 0.5), text_excerpt="",
         pdfplumber_text_excerpt="", crop_url=None, stamp_data=None,
         ocr_json_summary=None):
    cn = list(coords_norm) if coords_norm is not None else []
    return {
        "block_id": block_id,
        "page_number": page_number,
        "page_index": page_number - 1,
        "block_type": block_type,
        "semantic_type": semantic_type,
        "coords_px": [c * 1000 for c in cn] if cn else [],
        "coords_norm": cn,
        "shape_type": "rectangle",
        "source": "user",
        "crop_url": crop_url,
        "image_file": None,
        "has_crop_pdf": bool(crop_url),
        "has_image_file": False,
        "has_pdfplumber_text": bool(pdfplumber_text_excerpt),
        "has_ocr_json": bool(ocr_json_summary),
        "has_stamp_data": bool(stamp_data),
        "text_excerpt": text_excerpt,
        "pdfplumber_text_excerpt": pdfplumber_text_excerpt,
        "stamp_data": stamp_data or {},
        "ocr_json_summary": ocr_json_summary,
        "quality_flags": [],
    }


def _page(page_number, *, page_type, sheet_name="", document_code="DC", blocks=()):
    return {
        "page_number": page_number,
        "page_index": page_number - 1,
        "width": 1000, "height": 700,
        "sheet_number": "", "total_sheets": "",
        "sheet_name": sheet_name,
        "document_code": document_code,
        "page_type": page_type,
        "blocks": [b["block_id"] for b in blocks],
        "_blocks": list(blocks),
    }


def _model(document_code, pages):
    registry, clean = {}, []
    for p in pages:
        for b in p.get("_blocks", []):
            registry[b["block_id"]] = b
        clean.append({k: v for k, v in p.items() if k != "_blocks"})
    return {
        "version": 1, "kind": "stage_comparison_pipeline_v2_normalized_document",
        "source": {}, "document": {"document_code": document_code, "pages_total": len(clean)},
        "summary": {}, "pages": clean, "blocks": registry, "warnings": [],
    }


_STAMP = {"document_code": "АА/БЭ-ДС3-ИОС4", "project_name": "ЖК Тест",
          "sheet_name": "Структурная схема СОВ и СКУД. Корпус 4", "stage": "Р",
          "sheet_number": "5", "total_sheets": "40", "organization": "ТЕСТ-ПРОЕКТ",
          "signatures": [{"role": "ГИП", "surname": "Иванов", "date": "2026-01-10"}]}

_SCHEME_NAME = "Структурная схема СОВ и СКУД. Корпус 4"
_CONTENTS_TABLE = (
    "| Обозначение | Наименование | Стр. |\n"
    "| АА-ИОС4 | Содержание тома | 4 |\n"
    "| АА-ИОС4.1 | Структурная схема СОВ | 21 |")
_CHANGE_LOG_TABLE = (
    "| Изм. | Лист | Содержание изменений | Код | Примечание |\n"
    "| 1 | 5 | Добавлен щит СОВ | АА-1 | по замечанию |\n"
    "| 2 | 8 | Изменена трасса кабеля | АА-2 | |")


# ─── unit tests 1..11 ────────────────────────────────────────────────────────


def test_1_stamp_fields():
    blk = _blk("s1", page_number=1, block_type="text", semantic_type="stamp",
               stamp_data=_STAMP)
    ents = ee.extract_stamp_entities(blk)
    types = {e["subject"] for e in ents}
    assert "document_code" in types and "sheet_name" in types and "stage" in types
    dc = next(e for e in ents if e["subject"] == "document_code")
    assert dc["value"] == _STAMP["document_code"]
    assert all(e["entity_type"] == "stamp_field" for e in ents)
    # подпись извлечена
    assert any(e.get("subject") == "signature" for e in ents)


def test_2_change_log_items():
    blk = _blk("cl", page_number=2, block_type="text", semantic_type="text",
               text_excerpt=_CHANGE_LOG_TABLE)
    ents = ee.extract_entities_for_block(blk, page={"page_type": "change_log",
                                                    "document_code": "DC"})
    items = [e for e in ents if e["entity_type"] == "change_log_item"]
    assert len(items) == 2
    first = items[0]
    assert first["fields"].get("change_no") == "1"
    assert "Добавлен щит СОВ" in first["value"]


def test_3_contents_items():
    blk = _blk("c1", page_number=1, block_type="text", semantic_type="text",
               text_excerpt=_CONTENTS_TABLE)
    ents = ee.extract_entities_for_block(blk, page={"page_type": "contents",
                                                    "document_code": "DC"})
    items = [e for e in ents if e["entity_type"] == "contents_item"]
    assert len(items) == 2
    assert any(i["fields"].get("sheet_name") == "Содержание тома" for i in items)


def _text_block(text):
    return _blk("t", page_number=1, block_type="text", semantic_type="text",
                text_excerpt=text)


def _text_entities(text):
    return ee.extract_entities_for_block(
        _text_block(text), page={"page_type": "text", "document_code": "DC"})


def test_4_requirement():
    ents = _text_entities("Система видеонаблюдения предусматривается на входах. "
                          "Шкаф устанавливается в техпомещении.")
    reqs = [e for e in ents if e["entity_type"] == "requirement"]
    assert len(reqs) >= 2


def test_5_norm_reference():
    ents = _text_entities("Монтаж по СП 134.13330.2012 и ГОСТ Р 53246-2008. "
                          "Соответствует ПУЭ.")
    refs = {e["value"] for e in ents if e["entity_type"] == "norm_reference"}
    assert any(r.startswith("СП") for r in refs)
    assert any(r.lower().startswith("гост") for r in refs)
    assert any("ПУЭ" in r for r in refs)


def test_6_equipment():
    ents = _text_entities("Коммутатор и видеорегистратор размещаются в шкафу связи.")
    eq = {e["value"] for e in ents if e["entity_type"] == "equipment"}
    assert "коммутатор" in eq and "видеорегистратор" in eq and "шкаф" in eq


def test_7_cable():
    ents = _text_entities("Прокладывается кабель UTP cat.5e нг(А)-LSLTx до камеры.")
    cables = {e["value"].lower() for e in ents if e["entity_type"] == "cable"}
    assert any("utp" in c for c in cables)
    assert any("cat.5e" in c or "cat5e" in c for c in cables)


def test_8_power_supply():
    ents = _text_entities("Электропитание 220В, резервное 12В от ИБП I категории.")
    pw = {e["value"] for e in ents if e["entity_type"] == "power_supply"}
    assert "220В" in pw and "12В" in pw and "ИБП" in pw
    assert any("категори" in p.lower() for p in pw)


def _scheme_block():
    return _blk("sch", page_number=2, block_type="image", semantic_type="scheme",
                crop_url="https://r2.example.com/sch.pdf",
                coords_norm=(0.05, 0.05, 0.9, 0.85),
                ocr_json_summary={
                    "content_summary": "Структурная схема СОВ. Коммутатор подключается "
                                       "к видеорегистратору по Ethernet.",
                    "detailed_description": "",
                    "key_entities": ["Коммутатор Cisco", "UTP cat.5e", "220В",
                                     "Видеорегистратор", "ИБП", "Щит СОВ"]})


def test_9_scheme_components():
    ents = ee.extract_entities_for_block(_scheme_block(),
                                         page={"page_type": "scheme", "document_code": "DC"})
    types = {e["entity_type"] for e in ents}
    assert "scheme_component" in types   # Щит СОВ
    assert "equipment" in types          # Коммутатор/Видеорегистратор
    assert "cable" in types              # UTP cat.5e
    assert "power_supply" in types       # 220В/ИБП
    assert "scheme_connection_hint" in types  # подключается/ethernet
    # key_entities → source ocr_json
    assert any(e["evidence"]["source"] == "ocr_json" for e in ents)


def test_10_scheme_without_key_entities_flag():
    blk = _blk("schn", page_number=1, block_type="image", semantic_type="scheme",
               crop_url=None, ocr_json_summary=None,
               text_excerpt="Схема без ключевых сущностей")
    left = _model("DC", [_page(1, page_type="scheme", sheet_name="Только слева", blocks=[blk])])
    right = _model("DC", [])
    rep = ee.extract_entities_for_matched_documents(left, right, {"block_matches": []})
    entry = next(e for e in rep["unmatched_left_block_entities"] if e["block_id"] == "schn")
    assert "scheme_without_key_entities" in entry["quality_flags"]
    assert "scheme_without_crop" in entry["quality_flags"]
    assert any(w.startswith("scheme_blocks_without_key_entities") for w in rep["warnings"])


def test_11_dedup_within_block():
    blk = _text_block("Кабель UTP cat.5e. Снова UTP cat.5e. Питание 220В и опять 220В.")
    ents = ee.extract_entities_for_block(blk, page={"page_type": "text", "document_code": "DC"})
    cable_vals = [e["value"].lower() for e in ents if e["entity_type"] == "cable"]
    power_vals = [e["value"] for e in ents if e["entity_type"] == "power_supply"]
    assert cable_vals.count("utp") == 1
    assert power_vals.count("220В") == 1


# ─── matched-docs fixture (12,13,14,15) ─────────────────────────────────────


def _build_matched_models():
    left = _model("АА/БЭ-ДС3-ИОС4", [
        _page(1, page_type="contents", sheet_name="Содержание тома", blocks=[
            _blk("Lc", page_number=1, block_type="text", semantic_type="text",
                 coords_norm=(0.1, 0.1, 0.9, 0.6), text_excerpt=_CONTENTS_TABLE)]),
        _page(2, page_type="scheme", sheet_name=_SCHEME_NAME, blocks=[
            _blk("Lsch", page_number=2, block_type="image", semantic_type="scheme",
                 coords_norm=(0.05, 0.05, 0.9, 0.85),
                 crop_url="https://r2.example.com/Lsch.pdf",
                 ocr_json_summary={"content_summary": "Схема СОВ",
                                   "detailed_description": "",
                                   "key_entities": ["Коммутатор", "UTP cat.5e", "220В"]}),
            _blk("Lstamp", page_number=2, block_type="text", semantic_type="stamp",
                 coords_norm=(0.7, 0.85, 0.99, 0.99), stamp_data=_STAMP)]),
    ])
    right = _model("АА/БЭ-ДС3-ИОС4", [
        _page(1, page_type="contents", sheet_name="Содержание тома", blocks=[
            _blk("Rc", page_number=1, block_type="text", semantic_type="text",
                 coords_norm=(0.1, 0.1, 0.9, 0.6), text_excerpt=_CONTENTS_TABLE)]),
        _page(2, page_type="scheme", sheet_name=_SCHEME_NAME, blocks=[
            _blk("Rsch", page_number=2, block_type="image", semantic_type="scheme",
                 coords_norm=(0.06, 0.05, 0.9, 0.86),
                 crop_url="https://r2.example.com/Rsch.pdf",
                 ocr_json_summary={"content_summary": "Схема СОВ",
                                   "detailed_description": "",
                                   "key_entities": ["Коммутатор", "UTP cat.5e", "220В",
                                                    "Видеорегистратор"]}),
            _blk("Rstamp", page_number=2, block_type="text", semantic_type="stamp",
                 coords_norm=(0.7, 0.85, 0.99, 0.99), stamp_data=_STAMP)]),
        # односторонняя страница в NEW без key_entities
        _page(9, page_type="scheme", sheet_name="План трасс СКС (только справа)", blocks=[
            _blk("Rsch_nokeys", page_number=9, block_type="image", semantic_type="scheme",
                 coords_norm=(0.1, 0.1, 0.8, 0.8))]),
    ])
    return left, right


@pytest.fixture()
def matched_report():
    left, right = _build_matched_models()
    bmr = bm.match_normalized_documents(left, right)
    return left, right, bmr, ee.extract_entities_for_matched_documents(left, right, bmr)


def test_12_matched_block_entities_linked(matched_report):
    left, right, bmr, rep = matched_report
    # stamp-пара связана и несёт stamp_field
    stamp_entry = next(e for e in rep["matched_block_entities"]
                       if e["left_block_id"] == "Lstamp")
    assert stamp_entry["block_match_id"].startswith("bm_")
    assert stamp_entry["left_entities"] and stamp_entry["right_entities"]
    assert stamp_entry["entity_type_counts"].get("stamp_field", 0) > 0
    # все block_match_id существуют в отчёте этапа 2
    bm_ids = {m["match_id"] for m in bmr["block_matches"]}
    assert all(e["block_match_id"] in bm_ids for e in rep["matched_block_entities"])


def test_13_unmatched_block_entities(matched_report):
    left, right, bmr, rep = matched_report
    ids = {e["block_id"] for e in rep["unmatched_right_block_entities"]}
    assert "Rsch_nokeys" in ids


def test_14_summary_counts(matched_report):
    left, right, bmr, rep = matched_report
    s = rep["summary"]
    assert s["entities_total"] == s["left_entities_total"] + s["right_entities_total"]
    assert s["by_entity_type"].get("stamp_field", 0) > 0
    assert "stamp" in s["by_semantic_group"]
    assert "stamp_data" in s["by_source"]
    assert "ocr_json" in s["by_source"]
    assert s["blocks_processed"] == len(left["blocks"]) + len(right["blocks"])
    assert s["warnings_count"] == len(rep["warnings"])


def test_15_write_report_valid_json(matched_report, tmp_path: Path):
    left, right, bmr, rep = matched_report
    out = tmp_path / "sub" / "entity_extraction_report.json"
    returned = ee.write_entity_extraction_report(out, rep)
    assert returned == out and out.exists()
    reloaded = json.loads(out.read_text(encoding="utf-8"))
    assert reloaded["kind"] == ee.REPORT_KIND
    assert reloaded == rep


def test_16_no_network_and_no_llm(monkeypatch):
    import socket

    def _boom(*a, **k):  # pragma: no cover
        raise AssertionError("network access attempted in pipeline_v2 entity extraction")

    monkeypatch.setattr(socket, "socket", _boom)
    monkeypatch.setattr(socket, "create_connection", _boom)

    left, right = _build_matched_models()
    bmr = bm.match_normalized_documents(left, right)
    rep = ee.extract_entities_for_matched_documents(left, right, bmr)
    assert rep["summary"]["entities_total"] > 0

    src = Path(ee.__file__).read_text(encoding="utf-8")
    for forbidden in ("import requests", "import httpx", "urllib.request",
                      "import socket", "graphic_llm", "text_llm_provider",
                      "claude -p", "ClaudeCodeProvider", "qwen", "opus"):
        assert forbidden not in src, f"module references {forbidden!r}"


def test_17_integration_full_chain(tmp_path: Path):
    """result_json → normalize → match → extract."""
    def _rj():
        return {
            "pdf_path": "/tmp/x.pdf",
            "pages": [
                {"page_number": 1, "width": 2000, "height": 1400, "blocks": [
                    {"id": "t1", "block_type": "text", "source": "user",
                     "coords_px": [10, 10, 1800, 600],
                     "coords_norm": [0.005, 0.007, 0.9, 0.43],
                     "ocr_text": _CONTENTS_TABLE,
                     "stamp_data": {"document_code": "DC", "sheet_name": "Содержание тома",
                                    "sheet_number": "1"}}]},
                {"page_number": 2, "width": 5000, "height": 3500, "blocks": [
                    {"id": "img1", "block_type": "image", "source": "user",
                     "coords_px": [250, 175, 4500, 3000],
                     "coords_norm": [0.05, 0.05, 0.9, 0.85],
                     "crop_url": "https://r2.example.com/img1.pdf",
                     "ocr_text": "Коммутатор подключается к видеорегистратору. 220В.",
                     "ocr_json": {"content_summary": "Структурная схема СОВ",
                                  "clean_ocr_text": "Коммутатор UTP cat.5e 220В",
                                  "detailed_description": "",
                                  "key_entities": ["Коммутатор", "UTP cat.5e", "220В",
                                                   "Щит СОВ"],
                                  "location": ""},
                     "stamp_data": {"document_code": "DC", "sheet_name": _SCHEME_NAME,
                                    "sheet_number": "2"}}]},
            ],
        }

    lp = tmp_path / "old_result.json"
    rp = tmp_path / "new_result.json"
    lp.write_text(json.dumps(_rj(), ensure_ascii=False), encoding="utf-8")
    rp.write_text(json.dumps(_rj(), ensure_ascii=False), encoding="utf-8")

    left = pv1.build_normalized_document_model(lp)
    right = pv1.build_normalized_document_model(rp)
    # ingest сохранил ocr_json_summary (backward-compatible расширение этапа 1)
    img_block = next(b for b in left["blocks"].values() if b["block_type"] == "image")
    assert img_block.get("ocr_json_summary") and img_block["ocr_json_summary"]["key_entities"]

    bmr = bm.match_normalized_documents(left, right)
    rep = ee.extract_entities_for_matched_documents(left, right, bmr)

    assert rep["summary"]["entities_total"] > 0
    # на схемной паре извлеклись scheme/equipment/cable/power сущности
    matched_types: set = set()
    for e in rep["matched_block_entities"]:
        matched_types |= set(e["entity_type_counts"].keys())
    assert "scheme_component" in matched_types or "equipment" in matched_types


# ─── доп. юнит-проверки ──────────────────────────────────────────────────────


def test_normalize_and_id():
    assert ee.normalize_entity_text("  Коммутатор   ЦОД ") == "коммутатор цод"
    assert ee.make_entity_id("left", "blk 1", 3) == "ent_l_blk_1_03"
    assert ee.make_entity_id("right", "b2", 0) == "ent_r_b2_00"
