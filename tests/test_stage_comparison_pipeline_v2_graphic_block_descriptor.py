# -*- coding: utf-8 -*-
"""Тесты для Pipeline V2 — Graphic Block Descriptor.

Synthetic normalized models + synthetic block_matching_report. Никаких реальных
файлов, сети, Qwen/Opus.

Покрываемые spec-кейсы 1..19 (см. строки тестов).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import pipeline_v2_graphic_block_descriptor as gd
from backend.app.services.stage_comparison import pipeline_v2_block_matching as bm
from backend.app.services.stage_comparison import pipeline_v2_prepared_ingest as pv1


# ─── builders ───────────────────────────────────────────────────────────────


def _blk(bid, *, page_number=1, block_type="image", semantic_type="scheme",
         coords_norm=(0.05, 0.05, 0.9, 0.85), crop_url=None, image_file=None,
         pdfplumber_text_excerpt="", content_summary="", detailed_description="",
         key_entities=None, text_excerpt="", stamp_data=None, shape_type="rectangle"):
    ke = list(key_entities) if key_entities else []
    summ = None
    if content_summary or detailed_description or ke:
        summ = {"content_summary": content_summary,
                "detailed_description": detailed_description, "key_entities": ke}
    cn = list(coords_norm) if coords_norm else []
    return {
        "block_id": bid, "page_number": page_number, "page_index": page_number - 1,
        "block_type": block_type, "semantic_type": semantic_type,
        "coords_px": [c * 1000 for c in cn] if cn else [], "coords_norm": cn,
        "shape_type": shape_type, "source": "user",
        "crop_url": crop_url, "image_file": image_file,
        "has_crop_pdf": bool(crop_url), "has_image_file": bool(image_file),
        "has_pdfplumber_text": bool(pdfplumber_text_excerpt), "has_ocr_json": bool(summ),
        "has_stamp_data": bool(stamp_data),
        "text_excerpt": text_excerpt, "pdfplumber_text_excerpt": pdfplumber_text_excerpt,
        "ocr_json_summary": summ, "stamp_data": stamp_data or {}, "quality_flags": [],
    }


def _page(page_number, *, sheet_name="", document_code="DC", page_type="scheme", blocks=()):
    return {
        "page_number": page_number, "page_index": page_number - 1,
        "width": 1000, "height": 700, "sheet_number": "", "total_sheets": "",
        "sheet_name": sheet_name, "document_code": document_code,
        "page_type": page_type, "blocks": [b["block_id"] for b in blocks],
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


_PAGE = {"page_type": "scheme", "sheet_name": "", "document_code": "DC"}


def _desc(block, page=None):
    return gd.describe_graphic_block(block, page or _PAGE, {"side": "left"})


# ─── tests ──────────────────────────────────────────────────────────────────


def test_1_image_scheme_gets_descriptor():
    d = _desc(_blk("b1", content_summary="Схема", key_entities=["Коммутатор"]))
    assert d["descriptor_id"].startswith("gdesc_")
    assert d["block_id"] == "b1"
    assert "geometry" in d and "diff_readiness" in d and "tokens" in d


def test_2_structural_scheme():
    d = _desc(_blk("b1", content_summary="Структурная схема СОВ и СКУД. Корпус 4",
                   key_entities=["ШК.СВН4", "Коммутатор"], crop_url="u"))
    assert d["graphic_type"] == "structural_scheme"


def test_3_single_line_scheme():
    d = _desc(_blk("b1", content_summary="Однолинейная схема ГРЩ-1, ВРУ-2, QF3, АВР",
                   key_entities=["ГРЩ-1", "QF3"]))
    assert d["graphic_type"] == "single_line_scheme"


def test_4_cabinet_scheme():
    d = _desc(_blk("b1", content_summary="Шкаф связи СС: патч-панель, кросс, ИБП, RJ45, 24 порта"))
    assert d["graphic_type"] == "cabinet_scheme"


def test_5_discipline_and_systems():
    d = _desc(_blk("b1", content_summary="Структурная схема. СКУД: считыватель. "
                   "СОВ: домофон. СОТ: видеонаблюдение, камера."))
    assert {"СКУД", "СОВ", "СОТ"} <= set(d["systems"])
    assert d["discipline"] in ("SS", "SKUD", "SOV", "SOT")


def test_6_equipment_tokens_dedup():
    d = _desc(_blk("b1", content_summary="Коммутатор и ИБП в шкафу",
                   key_entities=["Коммутатор", "коммутатор", "ИБП"]))
    eq = d["tokens"]["equipment"]
    norm = [gd._norm(x) for x in eq]
    assert norm.count("коммутатор") == 1
    assert len(norm) == len(set(norm))  # без дублей


def test_7_cable_tokens_dedup():
    d = _desc(_blk("b1", content_summary="Кабель UTP cat.5e до камеры",
                   key_entities=["UTP cat.5e", "UTP cat.5e"]))
    cables = d["tokens"]["cables"]
    norm = [gd._norm(c) for c in cables]
    assert len(norm) == len(set(norm))  # без дублей
    assert any("utp" in c for c in norm)


def test_8_power_tokens_normalized():
    d = _desc(_blk("b1", content_summary="Электропитание 220 В, 220В и 12В от ИБП"))
    # каноничная форма из scan — lower, без пробелов («220 В» и «220В» → один «220в»)
    norm_power = [gd._norm(p) for p in d["tokens"]["power"]]
    assert "220в" in norm_power
    assert "12в" in norm_power
    assert norm_power.count("220в") == 1  # дедуп


def test_9_location_and_floor_tokens():
    d = _desc(_blk("b1", content_summary="Размещение: Корпус 4, Секция 1, помещение СС, "
                   "УЭРМ. Этажи: -2 этаж, 16 этаж."))
    locs = " ".join(gd._norm(x) for x in d["tokens"]["locations"])
    floors = [gd._norm(x) for x in d["tokens"]["floors"]]
    assert "корпус 4" in locs and "уэрм" in locs
    assert "-2 этаж" in floors and "16 этаж" in floors


def test_10_connection_hints():
    d = _desc(_blk("b1", content_summary="Коммутатор подключается к ШК.СВН3 по Ethernet. "
                   "Ввод ~220В."))
    hints = " ".join(gd._norm(x) for x in d["tokens"]["connection_hints"])
    assert "подключается" in hints
    assert "ethernet" in hints
    assert "к шк.свн3" in hints or "к шк" in hints


def test_11_geometry_metrics():
    g = gd.compute_graphic_geometry_metrics(
        _blk("b1", coords_norm=(0.05, 0.05, 0.9, 0.85)), _PAGE)
    assert 0.6 < g["area_ratio"] < 0.75
    assert g["is_large_block"] is True
    assert g["aspect_ratio"] > 0


def test_12_low_readiness_block_flags():
    d = _desc(_blk("b1", semantic_type="unknown", coords_norm=(0.1, 0.1, 0.2, 0.2),
                   crop_url=None, image_file=None))
    r = d["diff_readiness"]
    assert r["readiness"] == "not_usable"
    assert r["usable_for_diff"] is False
    assert "graphic_without_crop" in d["quality_flags"]
    assert "graphic_without_text_layer" in d["quality_flags"]
    assert "graphic_without_key_entities" in d["quality_flags"]


def test_13_high_or_medium_readiness_block():
    d = _desc(_blk("b1", crop_url="https://r2/x.pdf", image_file="/tmp/x.png",
                   content_summary="Структурная схема СКУД",
                   key_entities=["Коммутатор", "UTP cat.5e", "220В"]))
    r = d["diff_readiness"]
    assert r["readiness"] in ("high", "medium")
    assert r["usable_for_diff"] is True


# ─── matched (14/15) ─────────────────────────────────────────────────────────


def _matched_models(left_block, right_block):
    left = _model("DC", [_page(1, sheet_name="Схема", blocks=[left_block])])
    right = _model("DC", [_page(1, sheet_name="Схема", blocks=[right_block])])
    bmr = {"block_matches": [{"match_id": "bm_1", "left_block_id": left_block["block_id"],
                              "right_block_id": right_block["block_id"],
                              "confidence": "strong"}]}
    return left, right, bmr


def test_14_matched_token_overlap():
    lb = _blk("L1", content_summary="Структурная схема СКУД",
              key_entities=["Коммутатор", "UTP cat.5e", "220В"], crop_url="u")
    rb = _blk("R1", content_summary="Структурная схема СКУД",
              key_entities=["Коммутатор", "UTP cat.5e", "12В"], crop_url="u")
    left, right, bmr = _matched_models(lb, rb)
    matched = gd.describe_matched_graphic_blocks(left, right, bmr)
    assert len(matched) == 1
    m = matched[0]
    assert m["block_match_id"] == "bm_1"
    assert m["graphic_type_match"] is True
    assert m["discipline_match"] is True
    assert m["token_overlap"]["equipment"] == 1.0   # Коммутатор обе стороны
    assert 0.0 <= m["token_overlap"]["power"] < 1.0  # 220В vs 12В


def test_15_mismatch_risk_flags():
    lb = _blk("L1", content_summary="Структурная схема СКУД. Считыватель.",
              key_entities=["Коммутатор"], crop_url="u")
    rb = _blk("R1", content_summary="Однолинейная схема ГРЩ-1 ВРУ-2 QF3 АВР",
              key_entities=["ГРЩ-1"], crop_url="u")
    left, right, bmr = _matched_models(lb, rb)
    m = gd.describe_matched_graphic_blocks(left, right, bmr)[0]
    assert m["graphic_type_match"] is False
    assert "graphic_type_mismatch" in m["risk_flags"]
    assert "discipline_mismatch" in m["risk_flags"]


# ─── report (16/17) ──────────────────────────────────────────────────────────


def _report_model():
    return _model("DC", [
        _page(1, sheet_name="Структурная схема СКУД", blocks=[
            _blk("g1", content_summary="Структурная схема СКУД",
                 key_entities=["Коммутатор", "UTP cat.5e", "220В"], crop_url="u")]),
        _page(2, sheet_name="Однолинейная схема", blocks=[
            _blk("g2", content_summary="Однолинейная схема ГРЩ ВРУ QF АВР",
                 key_entities=["ГРЩ-1"], crop_url="u")]),
        _page(3, sheet_name="Пустой чертёж", blocks=[
            _blk("g3", semantic_type="unknown", crop_url=None, image_file=None)]),
        _page(4, sheet_name="Текст", page_type="text", blocks=[
            _blk("t1", block_type="text", semantic_type="text",
                 text_excerpt="обычный текст без схемы")]),
    ])


def test_16_summary_counts():
    rep = gd.build_graphic_descriptor_report(_report_model(), side="left")
    s = rep["summary"]
    # t1 (обычный текст) не графический → 3 дескриптора
    assert s["graphic_blocks_total"] == 3
    assert "single_line_scheme" in s["by_graphic_type"]
    assert "structural_scheme" in s["by_graphic_type"]
    assert sum(s["by_graphic_type"].values()) == 3
    assert sum(s["by_readiness"].values()) == 3
    assert s["by_discipline"]  # непустой


def test_17_write_report_valid_json(tmp_path: Path):
    rep = gd.build_graphic_descriptor_report(_report_model(), side="left")
    out = tmp_path / "sub" / "graphic_descriptor_report.json"
    returned = gd.write_graphic_descriptor_report(out, rep)
    assert returned == out and out.exists()
    reloaded = json.loads(out.read_text(encoding="utf-8"))
    assert reloaded["kind"] == gd.REPORT_KIND
    assert reloaded == rep


def test_18_no_network_and_no_llm(monkeypatch):
    import socket

    def _boom(*a, **k):  # pragma: no cover
        raise AssertionError("network access attempted in pipeline_v2 graphic descriptor")

    monkeypatch.setattr(socket, "socket", _boom)
    monkeypatch.setattr(socket, "create_connection", _boom)

    rep = gd.build_graphic_descriptor_report(_report_model(), side="left")
    assert rep["summary"]["graphic_blocks_total"] == 3

    src = Path(gd.__file__).read_text(encoding="utf-8")
    for forbidden in ("import requests", "import httpx", "urllib.request",
                      "import socket", "graphic_llm", "text_llm_provider",
                      "claude -p", "ClaudeCodeProvider", "qwen", "opus"):
        assert forbidden not in src, f"module references {forbidden!r}"


def test_19_integration_full_chain(tmp_path: Path):
    """result_json → normalize → match → build_graphic_descriptor_report."""
    def _rj():
        return {
            "pdf_path": "/tmp/x.pdf",
            "pages": [
                {"page_number": 1, "width": 5000, "height": 3500, "blocks": [
                    {"id": "img1", "block_type": "image", "source": "user",
                     "coords_px": [250, 175, 4500, 3000],
                     "coords_norm": [0.05, 0.05, 0.9, 0.85],
                     "crop_url": "https://r2.example.com/img1.pdf",
                     "ocr_text": "Структурная схема СКУД. Коммутатор. 220В.",
                     "ocr_json": {"content_summary": "Структурная схема СКУД",
                                  "clean_ocr_text": "Коммутатор UTP cat.5e 220В",
                                  "detailed_description": "",
                                  "key_entities": ["Коммутатор", "UTP cat.5e", "220В"],
                                  "location": ""},
                     "stamp_data": {"document_code": "DC",
                                    "sheet_name": "Структурная схема СКУД",
                                    "sheet_number": "1"}},
                ]},
            ],
        }

    lp = tmp_path / "old_result.json"
    rp = tmp_path / "new_result.json"
    lp.write_text(json.dumps(_rj(), ensure_ascii=False), encoding="utf-8")
    rp.write_text(json.dumps(_rj(), ensure_ascii=False), encoding="utf-8")

    left = pv1.build_normalized_document_model(lp)
    right = pv1.build_normalized_document_model(rp)
    bmr = bm.match_normalized_documents(left, right)

    rep = gd.build_graphic_descriptor_report(
        left, bmr, side="left", options={"counterpart_model": right})
    assert rep["summary"]["graphic_blocks_total"] >= 1
    d = rep["descriptors"][0]
    assert d["graphic_type"] == "structural_scheme"
    assert "Коммутатор" in d["tokens"]["equipment"] or "коммутатор" in \
        [gd._norm(x) for x in d["tokens"]["equipment"]]
    # matched секция заполнена (есть counterpart_model)
    assert len(rep["matched_graphic_blocks"]) >= 1


# ─── доп. юнит-проверки ──────────────────────────────────────────────────────


def test_classify_graphic_token():
    assert gd.classify_graphic_token("UTP cat.5e") == "cable"
    assert gd.classify_graphic_token("220В") == "power"
    assert gd.classify_graphic_token("ИБП") == "power"
    assert gd.classify_graphic_token("-2 этаж") == "floor"
    assert gd.classify_graphic_token("Корпус 4") == "location"
    assert gd.classify_graphic_token("Коммутатор") == "equipment"
    assert gd.classify_graphic_token("ШК.СВН4") == "equipment"
    assert gd.classify_graphic_token("подключается к ШК") == "connection_hint"


# ─── ambiguous power tokens (АР2 cleanup, 2026-06-10) ────────────────────────


def test_power_tokens_axis_labels_suppressed_on_plan():
    # «4 в»/«1в» на АР-плане — осевые/блочные метки, не номиналы питания
    blk = _blk("ar1", semantic_type="plan", content_summary="Фасад 4 в осях 1-5",
               key_entities=["4 в", "1в"], crop_url="u")
    tokens = gd.extract_graphic_tokens(blk)
    assert tokens["power"] == []
    assert gd.classify_graphic_token("4 в") != "power"
    assert gd.classify_graphic_token("1в") != "power"


def test_power_tokens_valid_voltage_kept_on_scheme():
    blk = _blk("s1", semantic_type="scheme",
               content_summary="Питание шкафа 220В от щита, резерв ИБП",
               key_entities=["220В", "ИБП"], crop_url="u")
    tokens = gd.extract_graphic_tokens(blk)
    assert "220В" in tokens["power"]
    assert "ИБП" in tokens["power"]
    assert gd.classify_graphic_token("220В") == "power"
