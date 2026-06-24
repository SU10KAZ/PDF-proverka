# -*- coding: utf-8 -*-
"""Тесты для Pipeline V2 — Block Matching OLD↔NEW (этап 2).

Работают на synthetic ``normalized_document_model`` (этап 1), не на реальных
файлах. Никаких сетевых вызовов, Qwen/Opus/OCR/PDF-render не задействованы.

Покрываемые spec-кейсы:
  1.  страницы с одинаковым sheet_name и разными физ. номерами → strong;
  2.  contents ↔ contents;
  3.  change_log ↔ change_log;
  4.  схемная страница с image-блоком матчится по sheet_name;
  5.  image/scheme-блоки матчатся по coords_norm IoU;
  6.  text-блоки матчатся по fuzzy text;
  7.  stamp-блоки матчатся stamp↔stamp;
  8.  несовместимые типы не матчатся как strong;
  9.  односторонние блоки попадают в unmatched;
 10.  отсутствующие координаты дают warning/risk_flag;
 11.  дубли-кандидаты помечаются risk_flag;
 12.  write_block_matching_report пишет валидный JSON;
 13.  модуль без сети и без Qwen/Opus/provider-импортов;
 14.  совместимость с моделью этапа 1 (result_json → normalize → match).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import pipeline_v2_block_matching as bm
from backend.app.services.stage_comparison import pipeline_v2_prepared_ingest as pv1


# ─── synthetic model builders ───────────────────────────────────────────────


def _blk(block_id, *, page_number, block_type="image", semantic_type="scheme",
         coords_norm=(0.1, 0.1, 0.5, 0.5), text_excerpt="", crop_url=None,
         stamp_data=None, pdfplumber_text_excerpt=""):
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
        "has_ocr_json": False,
        "has_stamp_data": bool(stamp_data),
        "text_excerpt": text_excerpt,
        "pdfplumber_text_excerpt": pdfplumber_text_excerpt,
        "stamp_data": stamp_data or {},
        "quality_flags": [],
    }


def _page(page_number, *, page_type, sheet_name="", sheet_number="",
          document_code="DOC", blocks=()):
    return {
        "page_number": page_number,
        "page_index": page_number - 1,
        "width": 1000,
        "height": 700,
        "sheet_number": sheet_number,
        "total_sheets": "",
        "sheet_name": sheet_name,
        "document_code": document_code,
        "page_type": page_type,
        "blocks": [b["block_id"] for b in blocks],
        "_blocks": list(blocks),  # вспомогательное, в реальной модели нет
    }


def _model(document_code, pages):
    registry = {}
    clean_pages = []
    for p in pages:
        for b in p.get("_blocks", []):
            registry[b["block_id"]] = b
        cp = {k: v for k, v in p.items() if k != "_blocks"}
        clean_pages.append(cp)
    return {
        "version": 1,
        "kind": "stage_comparison_pipeline_v2_normalized_document",
        "source": {},
        "document": {"document_code": document_code, "pages_total": len(clean_pages)},
        "summary": {},
        "pages": clean_pages,
        "blocks": registry,
        "warnings": [],
    }


_SCHEME_NAME = "Структурная схема СОВ и СКУД. Корпус 4"


def _build_main_models():
    """OLD/NEW с раздвинутыми номерами листов, чтобы проверить матчинг по имени."""
    # contents text-блоки (для text_fuzzy)
    contents_text = ("Содержание тома. Раздел 1 Пояснительная записка. "
                     "Раздел 2 Графическая часть.")
    left = _model("АА/БЭ-ДС3-ИОС4", [
        _page(4, page_type="contents", sheet_name="Содержание тома", blocks=[
            _blk("L_c1", page_number=4, block_type="text", semantic_type="text",
                 coords_norm=(0.1, 0.1, 0.9, 0.6), text_excerpt=contents_text),
        ]),
        _page(5, page_type="change_log", sheet_name="Справка о внесённых изменениях",
              blocks=[
            # координаты ОТСУТСТВУЮТ → warning/risk_flag
            _blk("L_cl1", page_number=5, block_type="text", semantic_type="text",
                 coords_norm=None,
                 text_excerpt="Справка о внесённых изменениях в рабочую документацию"),
        ]),
        _page(52, page_type="scheme", sheet_name=_SCHEME_NAME, sheet_number="52", blocks=[
            _blk("L_sch", page_number=52, block_type="image", semantic_type="scheme",
                 coords_norm=(0.05, 0.05, 0.90, 0.85),
                 crop_url="https://r2.example.com/L_sch.pdf"),
            _blk("L_stamp", page_number=52, block_type="text", semantic_type="stamp",
                 coords_norm=(0.70, 0.85, 0.99, 0.99),
                 stamp_data={"document_code": "АА/БЭ-ДС3-ИОС4",
                             "sheet_name": _SCHEME_NAME, "sheet_number": "52"}),
        ]),
        # односторонняя страница (только в OLD)
        _page(7, page_type="text", sheet_name="Только в старой стадии", blocks=[
            _blk("L_only", page_number=7, block_type="text", semantic_type="text",
                 coords_norm=(0.1, 0.1, 0.4, 0.4), text_excerpt="уникальный текст старой"),
        ]),
    ])
    right = _model("АА/БЭ-ДС3-ИОС4", [
        _page(4, page_type="contents", sheet_name="Содержание тома", blocks=[
            _blk("R_c1", page_number=4, block_type="text", semantic_type="text",
                 coords_norm=(0.1, 0.1, 0.9, 0.6), text_excerpt=contents_text),
        ]),
        _page(6, page_type="change_log", sheet_name="Справка о внесённых изменениях",
              blocks=[
            _blk("R_cl1", page_number=6, block_type="text", semantic_type="text",
                 coords_norm=(0.1, 0.1, 0.9, 0.5),
                 text_excerpt="Справка о внесённых изменениях в рабочую документацию"),
        ]),
        _page(21, page_type="scheme", sheet_name=_SCHEME_NAME, sheet_number="21", blocks=[
            _blk("R_sch", page_number=21, block_type="image", semantic_type="scheme",
                 coords_norm=(0.06, 0.05, 0.90, 0.86),
                 crop_url="https://r2.example.com/R_sch.pdf"),
            _blk("R_stamp", page_number=21, block_type="text", semantic_type="stamp",
                 coords_norm=(0.70, 0.85, 0.99, 0.99),
                 stamp_data={"document_code": "АА/БЭ-ДС3-ИОС4",
                             "sheet_name": _SCHEME_NAME, "sheet_number": "21"}),
        ]),
    ])
    return left, right


@pytest.fixture()
def report():
    left, right = _build_main_models()
    return bm.match_normalized_documents(left, right)


def _pm(report, ln, rn):
    return next((m for m in report["page_matches"]
                 if m["left_page_number"] == ln and m["right_page_number"] == rn), None)


# ─── tests ──────────────────────────────────────────────────────────────────


def test_1_same_sheet_name_different_page_numbers_strong(report):
    m = _pm(report, 52, 21)
    assert m is not None, "scheme лист 52↔21 должен сматчиться по имени"
    assert m["confidence"] == "strong"
    assert m["method"] == "exact_sheet"


def test_2_contents_matches_contents(report):
    m = _pm(report, 4, 4)
    assert m is not None
    assert m["left_page_type"] == "contents" and m["right_page_type"] == "contents"
    assert m["confidence"] == "strong"


def test_3_change_log_matches_change_log(report):
    m = _pm(report, 5, 6)
    assert m is not None
    assert m["left_page_type"] == "change_log" and m["right_page_type"] == "change_log"
    assert m["confidence"] in ("strong", "medium")


def test_4_scheme_page_matched_by_sheet_name(report):
    m = _pm(report, 52, 21)
    assert m is not None
    assert m["left_page_type"] == "scheme" and m["right_page_type"] == "scheme"
    assert "sheet_name_exact" in m["reasons"]


def test_5_scheme_blocks_match_by_iou(report):
    bmatch = next((x for x in report["block_matches"]
                   if x["left_block_id"] == "L_sch" and x["right_block_id"] == "R_sch"), None)
    assert bmatch is not None
    assert bmatch["method"] in ("semantic_type_iou", "scheme_crop")
    assert bmatch["iou"] > 0.8
    assert bmatch["confidence"] == "strong"


def test_6_text_blocks_match_by_fuzzy(report):
    bmatch = next((x for x in report["block_matches"]
                   if x["left_block_id"] == "L_c1" and x["right_block_id"] == "R_c1"), None)
    assert bmatch is not None
    assert bmatch["method"] == "text_fuzzy"
    assert bmatch["confidence"] == "strong"


def test_7_stamp_blocks_match_stamp_to_stamp(report):
    bmatch = next((x for x in report["block_matches"]
                   if x["left_block_id"] == "L_stamp" and x["right_block_id"] == "R_stamp"), None)
    assert bmatch is not None
    assert bmatch["method"] == "stamp"
    assert bmatch["left_semantic_type"] == "stamp" and bmatch["right_semantic_type"] == "stamp"


def test_8_incompatible_types_not_strong():
    # left page: только stamp; right page: только scheme, на одних координатах
    left = _model("D", [_page(1, page_type="scheme", sheet_name="Схема X", blocks=[
        _blk("Ls", page_number=1, block_type="text", semantic_type="stamp",
             coords_norm=(0.1, 0.1, 0.9, 0.9))])])
    right = _model("D", [_page(1, page_type="scheme", sheet_name="Схема X", blocks=[
        _blk("Rs", page_number=1, block_type="image", semantic_type="scheme",
             coords_norm=(0.1, 0.1, 0.9, 0.9))])])
    rep = bm.match_normalized_documents(left, right)
    # stamp↔scheme несовместимы → не должно быть матча между ними
    pair = [x for x in rep["block_matches"]
            if {x["left_semantic_type"], x["right_semantic_type"]} == {"stamp", "scheme"}]
    assert pair == []
    # оба блока остаются непарными
    assert any(b["block_id"] == "Ls" for b in rep["unmatched_left_blocks"])
    assert any(b["block_id"] == "Rs" for b in rep["unmatched_right_blocks"])


def test_9_one_sided_blocks_in_unmatched(report):
    ids = {b["block_id"] for b in report["unmatched_left_blocks"]}
    assert "L_only" in ids
    only = next(b for b in report["unmatched_left_blocks"] if b["block_id"] == "L_only")
    assert "one_sided_block" in only["risk_flags"]
    # односторонняя страница тоже отмечена
    pg = next(p for p in report["unmatched_left_pages"] if p["page_number"] == 7)
    assert "one_sided_page" in pg["risk_flags"]


def test_10_missing_coords_warning_and_flag(report):
    assert any(w.startswith("blocks_missing_coords") for w in report["warnings"])
    # матч change_log text-блоков несёт missing_coords (у L_cl1 нет coords)
    cl = next((x for x in report["block_matches"]
               if x["left_block_id"] == "L_cl1"), None)
    assert cl is not None
    assert "missing_coords" in cl["risk_flags"]


def test_11_duplicate_candidates_flagged():
    left = _model("D", [_page(1, page_type="scheme", sheet_name="Схема Y", blocks=[
        _blk("L1", page_number=1, semantic_type="scheme",
             coords_norm=(0.1, 0.1, 0.5, 0.5))])])
    right = _model("D", [_page(1, page_type="scheme", sheet_name="Схема Y", blocks=[
        _blk("RA", page_number=1, semantic_type="scheme",
             coords_norm=(0.1, 0.1, 0.5, 0.5)),
        _blk("RB", page_number=1, semantic_type="scheme",
             coords_norm=(0.12, 0.11, 0.52, 0.51))])])
    rep = bm.match_normalized_documents(left, right)
    m = next(x for x in rep["block_matches"] if x["left_block_id"] == "L1")
    assert "duplicate_candidate" in m["risk_flags"]


def test_12_write_report_creates_valid_json(report, tmp_path: Path):
    out = tmp_path / "sub" / "block_matching_report.json"
    returned = bm.write_block_matching_report(out, report)
    assert returned == out and out.exists()
    reloaded = json.loads(out.read_text(encoding="utf-8"))
    assert reloaded["kind"] == bm.REPORT_KIND
    assert reloaded == report
    # summary самосогласован
    s = reloaded["summary"]
    assert s["page_matches_total"] == len(reloaded["page_matches"])
    assert s["block_matches_total"] == len(reloaded["block_matches"])
    assert s["warnings_count"] == len(reloaded["warnings"])


def test_13_no_network_and_no_llm(monkeypatch):
    import socket

    def _boom(*a, **k):  # pragma: no cover
        raise AssertionError("network access attempted in pipeline_v2 block matching")

    monkeypatch.setattr(socket, "socket", _boom)
    monkeypatch.setattr(socket, "create_connection", _boom)

    left, right = _build_main_models()
    rep = bm.match_normalized_documents(left, right)
    assert rep["summary"]["page_matches_total"] >= 3

    src = Path(bm.__file__).read_text(encoding="utf-8")
    for forbidden in ("import requests", "import httpx", "urllib.request",
                      "import socket", "graphic_llm", "text_llm_provider",
                      "claude -p", "ClaudeCodeProvider", "qwen", "opus"):
        assert forbidden not in src, f"module references {forbidden!r}"


def test_14_compatibility_with_stage1_model(tmp_path: Path):
    """result_json → normalize_result_json/build_model (этап 1) → match (этап 2)."""
    def _rj(sheet_a, sheet_b):
        return {
            "pdf_path": "/tmp/x.pdf",
            "pages": [
                {"page_number": 1, "width": 2000, "height": 1400, "blocks": [
                    {"id": "t1", "block_type": "text", "source": "user",
                     "coords_px": [10, 10, 800, 400], "coords_norm": [0.005, 0.007, 0.4, 0.28],
                     "ocr_text": "Содержание тома. Раздел 1.",
                     "stamp_data": {"document_code": "DC", "sheet_name": sheet_a,
                                    "sheet_number": "1"}},
                ]},
                {"page_number": 2, "width": 5000, "height": 3500, "blocks": [
                    {"id": "img1", "block_type": "image", "source": "user",
                     "coords_px": [250, 175, 4500, 3000],
                     "coords_norm": [0.05, 0.05, 0.9, 0.85],
                     "crop_url": "https://r2.example.com/img1.pdf",
                     "ocr_text": "однолинейная схема",
                     "stamp_data": {"document_code": "DC", "sheet_name": sheet_b,
                                    "sheet_number": "2"}},
                ]},
            ],
        }

    lp = tmp_path / "old_result.json"
    rp = tmp_path / "new_result.json"
    lp.write_text(json.dumps(_rj("Содержание тома", _SCHEME_NAME), ensure_ascii=False),
                  encoding="utf-8")
    rp.write_text(json.dumps(_rj("Содержание тома", _SCHEME_NAME), ensure_ascii=False),
                  encoding="utf-8")

    left_model = pv1.build_normalized_document_model(lp)
    right_model = pv1.build_normalized_document_model(rp)
    rep = bm.match_normalized_documents(left_model, right_model)

    assert rep["summary"]["page_matches_total"] == 2
    # contents и scheme страницы сматчились
    types = {(m["left_page_type"], m["right_page_type"]) for m in rep["page_matches"]}
    assert ("contents", "contents") in types
    assert any(lt == "scheme" and rt == "scheme" for lt, rt in types)


# ─── доп. юнит-проверки чистых функций ───────────────────────────────────────


def test_compute_bbox_iou_norm_basic():
    assert bm.compute_bbox_iou_norm([0, 0, 1, 1], [0, 0, 1, 1]) == 1.0
    assert bm.compute_bbox_iou_norm([0, 0, 0.5, 0.5], [0.6, 0.6, 1, 1]) == 0.0
    assert bm.compute_bbox_iou_norm([], [0, 0, 1, 1]) == 0.0
    half = bm.compute_bbox_iou_norm([0, 0, 1, 1], [0, 0, 0.5, 1])
    assert 0.49 < half < 0.51


def test_normalize_match_text_and_identity_keys():
    assert bm.normalize_match_text("Лист 5  Схема ГРЩ-0,4кВ (из 22)") == "схема грщ 0 4кв"
    assert bm.normalize_match_text("ВРУ №1") == "вру no1"  # NFKC: «№» → «no», детерминировано/симметрично
    assert bm.normalize_match_text("  Корпус   4 ") == "корпус 4"
    pk = bm.make_page_identity_key({"sheet_name": "Содержание тома", "page_type": "contents"})
    assert pk == "contents|name:содержание тома"
    bk = bm.make_block_identity_key({"semantic_type": "stamp", "block_id": "b9"})
    assert bk == "stamp|b9"
