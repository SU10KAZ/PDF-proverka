# -*- coding: utf-8 -*-
"""Тесты для Pipeline V2 — Prepared Package Ingest (этап 1).

Работают на маленьком synthetic fixture, имитирующем реальный ``result.json``
(структура: pages[].blocks[] со stamp_data/ocr_json/coords/crop_url). Никаких
реальных PDF, никаких сетевых вызовов, Qwen/Opus не задействованы.

Покрываемые spec-кейсы:
  1.  нормализуется документ с несколькими страницами и блоками;
  2.  страница «Справка о внесённых изменениях» → change_log;
  3.  страница «Содержание тома» → contents;
  4.  страница «Структурная схема …» + image-блок → scheme;
  5.  image-блок с crop_url → has_crop_pdf=True;
  6.  штампный блок (ocr_json-штамп) → semantic_type=stamp;
  7.  text-блок → semantic_type=text;
  8.  table-блок → semantic_type=table;
  9.  summary считает blocks/pages/by_type;
 10.  warnings/quality_flags при image-блоке без crop_url/image_file;
 11.  write_normalized_document_model создаёт валидный JSON-файл;
 12.  модуль не делает сетевых вызовов и не требует Qwen/Opus.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import pipeline_v2_prepared_ingest as pv2


# ─── synthetic fixture ──────────────────────────────────────────────────────

_STAMP = {
    "document_code": "АА/БЭ-03-ДС3-ЭОМ1",
    "project_name": "ЖК «Тест», г. Москва",
    "stage": "П",
    "organization": "ТЕСТ-ПРОЕКТ",
}


def _stamp(sheet_name: str, sheet_number: str = "", total: str = "40") -> dict:
    sd = dict(_STAMP)
    sd["sheet_name"] = sheet_name
    if sheet_number:
        sd["sheet_number"] = sheet_number
        sd["total_sheets"] = total
    return sd


def _block(bid, block_type, *, coords_px=None, coords_norm=None, shape="rectangle",
           ocr_text="", ocr_json=None, pdfplumber_text="", crop_url=None,
           image_file=None, stamp_data=None, category_code=None, ocr_html=None):
    b = {
        "id": bid,
        "block_type": block_type,
        "coords_px": coords_px if coords_px is not None else [10, 10, 100, 100],
        "coords_norm": coords_norm if coords_norm is not None else [0.01, 0.01, 0.1, 0.1],
        "shape_type": shape,
        "source": "user",
        "ocr_text": ocr_text,
    }
    if ocr_json is not None:
        b["ocr_json"] = ocr_json
    if pdfplumber_text:
        b["pdfplumber_text"] = pdfplumber_text
    if crop_url is not None:
        b["crop_url"] = crop_url
    if image_file is not None:
        b["image_file"] = image_file
    if stamp_data is not None:
        b["stamp_data"] = stamp_data
    if category_code is not None:
        b["category_code"] = category_code
    if ocr_html is not None:
        b["ocr_html"] = ocr_html
    return b


def _build_result_json() -> dict:
    """4-страничный документ, покрывающий все классы."""
    # Штамп-словарь (как реальный ocr_json штампного блока).
    stamp_ocr_json = {
        "document_code": _STAMP["document_code"],
        "organization": _STAMP["organization"],
        "project_name": _STAMP["project_name"],
        "stage": _STAMP["stage"],
        "sheet_number": "5",
        "sheet_name": "Электроснабжение. Силовое оборудование",
        "total_sheets": "40",
        "revisions": [],
        "signatures": [],
    }
    return {
        "pdf_path": "/tmp/some/path/doc.pdf",
        "pages": [
            # 1 — contents
            {
                "page_number": 1, "width": 2384, "height": 1684,
                "blocks": [
                    _block("p1_t1", "text",
                           ocr_text="Содержание тома\n1. Пояснительная записка\n2. Графическая часть",
                           stamp_data=_stamp("Содержание тома", "1")),
                ],
            },
            # 2 — change_log
            {
                "page_number": 2, "width": 2384, "height": 1684,
                "blocks": [
                    _block("p2_t1", "text",
                           ocr_text="Справка о внесённых изменениях в проектную документацию",
                           stamp_data=_stamp("Справка о внесённых изменениях", "2")),
                ],
            },
            # 3 — scheme (image + crop_url)
            {
                "page_number": 3, "width": 5000, "height": 3500,
                "blocks": [
                    _block("p3_img", "image",
                           coords_norm=[0.05, 0.05, 0.9, 0.85],
                           crop_url="https://r2.example.com/crops/p3_img.pdf?sig=secret",
                           image_file="/tmp/crops/p3_img.png",
                           ocr_text="Однолинейная схема",
                           pdfplumber_text="ВРУ-1 ЩР-1а QF3 1000А",
                           ocr_json={"content_summary": "однолинейная схема ВРУ",
                                     "clean_ocr_text": "ВРУ-1 ЩР-1а"},
                           stamp_data=_stamp("Структурная схема электроснабжения", "3")),
                ],
            },
            # 4 — mixed: stamp + text + table + image(crop) + image(no crop)
            {
                "page_number": 4, "width": 5000, "height": 3500,
                "blocks": [
                    # stamp block: ocr_json IS a stamp dict
                    _block("p4_stamp", "text",
                           coords_norm=[0.7, 0.85, 0.99, 0.99],
                           ocr_text="",
                           ocr_json=stamp_ocr_json,
                           stamp_data=_stamp("Электроснабжение. Силовое оборудование", "5")),
                    # plain text
                    _block("p4_text", "text",
                           ocr_text="Электроснабжение жилого дома выполнено кабелями "
                                    "с медными жилами, проложенными в лотках.",
                           stamp_data=_stamp("Электроснабжение. Силовое оборудование", "5")),
                    # table
                    _block("p4_table", "table",
                           ocr_text="Ведомость кабелей и проводов",
                           stamp_data=_stamp("Электроснабжение. Силовое оборудование", "5")),
                    # image with crop_url
                    _block("p4_img_crop", "image",
                           coords_norm=[0.05, 0.05, 0.4, 0.4],
                           crop_url="https://r2.example.com/crops/p4_img.pdf",
                           image_file="/tmp/crops/p4_img.png",
                           ocr_text="фрагмент",
                           stamp_data=_stamp("Электроснабжение. Силовое оборудование", "5")),
                    # image WITHOUT crop_url and WITHOUT image_file (quality flag)
                    _block("p4_img_nocrop", "image",
                           coords_norm=[0.45, 0.05, 0.65, 0.25],
                           ocr_text="",
                           stamp_data=_stamp("Электроснабжение. Силовое оборудование", "5")),
                ],
            },
        ],
    }


@pytest.fixture()
def result_json_path(tmp_path: Path) -> Path:
    p = tmp_path / "doc_result.json"
    p.write_text(json.dumps(_build_result_json(), ensure_ascii=False), encoding="utf-8")
    return p


@pytest.fixture()
def model(result_json_path: Path) -> dict:
    return pv2.build_normalized_document_model(result_json_path)


# ─── tests ──────────────────────────────────────────────────────────────────


def test_1_normalizes_multiple_pages_and_blocks(model):
    assert model["version"] == pv2.MODEL_VERSION
    assert model["kind"] == pv2.MODEL_KIND
    assert model["summary"]["pages_total"] == 4
    assert len(model["pages"]) == 4
    assert model["summary"]["blocks_total"] == 8
    assert len(model["blocks"]) == 8
    # document stamp summary заполнен
    assert model["document"]["document_code"] == _STAMP["document_code"]
    assert model["document"]["stage"] == "П"
    assert model["document"]["organization"] == _STAMP["organization"]
    assert model["document"]["pages_total"] == 4


def _page(model, page_number):
    return next(p for p in model["pages"] if p["page_number"] == page_number)


def test_2_change_log_page(model):
    assert _page(model, 2)["page_type"] == "change_log"


def test_3_contents_page(model):
    assert _page(model, 1)["page_type"] == "contents"


def test_4_scheme_page(model):
    assert _page(model, 3)["page_type"] == "scheme"


def test_5_image_block_with_crop_url_has_crop_pdf(model):
    blk = model["blocks"]["p3_img"]
    assert blk["has_crop_pdf"] is True
    assert blk["crop_url"]
    # cloud crop отмечается, но НЕ скачивается
    assert "has_cloud_crop_url" in blk["quality_flags"]


def test_6_stamp_block_semantic_type(model):
    assert model["blocks"]["p4_stamp"]["semantic_type"] == "stamp"


def test_7_text_block_semantic_type(model):
    assert model["blocks"]["p4_text"]["semantic_type"] == "text"


def test_8_table_block_semantic_type(model):
    assert model["blocks"]["p4_table"]["semantic_type"] == "table"


def test_9_summary_counts(model):
    s = model["summary"]
    assert s["blocks_total"] == 8
    assert s["pages_total"] == 4
    # by_block_type: 4 text, 3 image, 1 table
    assert s["by_block_type"].get("text") == 4
    assert s["by_block_type"].get("image") == 3
    assert s["by_block_type"].get("table") == 1
    # image counters
    assert s["image_blocks_total"] == 3
    assert s["image_blocks_with_crop_url"] == 2
    assert s["image_blocks_with_image_file"] == 2
    # semantic counters
    assert s["stamp_blocks_total"] == 1
    assert s["table_blocks_total"] == 1
    assert s["scheme_blocks_total"] == 1  # large_scheme на стр.3
    assert s["text_blocks_total"] == 4
    # by_page_type
    assert s["by_page_type"].get("contents") == 1
    assert s["by_page_type"].get("change_log") == 1
    assert s["by_page_type"].get("scheme") == 1
    assert sum(s["by_page_type"].values()) == 4
    # by_semantic_type суммируется до общего числа блоков
    assert sum(s["by_semantic_type"].values()) == 8


def test_10_image_block_without_crop_or_image_file_flagged(model):
    blk = model["blocks"]["p4_img_nocrop"]
    assert "image_block_without_crop_or_image_file" in blk["quality_flags"]
    assert blk["has_crop_pdf"] is False
    assert blk["has_image_file"] is False
    # такой блок без crop/image_file не должен помечаться как cloud crop
    assert "has_cloud_crop_url" not in blk["quality_flags"]


def test_11_write_model_creates_valid_json(model, tmp_path: Path):
    out = tmp_path / "sub" / "normalized_document_model.json"
    returned = pv2.write_normalized_document_model(out, model)
    assert returned == out
    assert out.exists()
    reloaded = json.loads(out.read_text(encoding="utf-8"))
    assert reloaded["kind"] == pv2.MODEL_KIND
    assert reloaded["summary"]["blocks_total"] == 8
    assert reloaded == model


def test_12_no_network_and_no_llm(result_json_path: Path, monkeypatch):
    """Сборка модели не делает сетевых вызовов и не зовёт Qwen/Opus."""
    import socket

    def _boom(*a, **k):  # pragma: no cover - вызывается только при нарушении
        raise AssertionError("network access attempted in pipeline_v2 ingest")

    monkeypatch.setattr(socket, "socket", _boom)
    monkeypatch.setattr(socket, "create_connection", _boom)

    m = pv2.build_normalized_document_model(result_json_path)
    assert m["summary"]["blocks_total"] == 8

    # модуль не импортирует сетевые/LLM клиенты
    src = Path(pv2.__file__).read_text(encoding="utf-8")
    for forbidden in ("import requests", "import httpx", "urllib.request",
                      "import socket", "graphic_llm", "text_llm_provider",
                      "claude -p", "ClaudeCodeProvider"):
        assert forbidden not in src, f"module references {forbidden!r}"


# ─── дополнительные юнит-проверки чистых функций ─────────────────────────────


def test_classify_block_semantic_type_legend_and_plan():
    legend = {"block_type": "image", "ocr_text": "Условные обозначения",
              "stamp_data": {"sheet_name": "Условные обозначения"}}
    assert pv2.classify_block_semantic_type(legend) == "legend"

    plan = {"block_type": "image", "ocr_text": "",
            "stamp_data": {"sheet_name": "План 1 этажа"}}
    assert pv2.classify_block_semantic_type(plan) == "plan"


def test_normalize_result_json_flat_format(tmp_path: Path):
    """Flat-формат B (data['blocks']) тоже нормализуется."""
    data = {
        "blocks": [
            {"id": "b1", "block_type": "text", "page_number": 1,
             "page_width": 1000, "page_height": 700, "ocr_text": "hi",
             "coords_px": [0, 0, 10, 10]},
            {"id": "b2", "block_type": "image", "page_number": 1,
             "coords_norm": [0, 0, 1, 1], "crop_url": "https://x/y.pdf"},
        ],
    }
    p = tmp_path / "flat_result.json"
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    norm = pv2.normalize_result_json(p)
    assert norm["ok"] is True
    assert len(norm["blocks"]) == 2
    assert len(norm["pages"]) == 1
    assert norm["pages"][0]["block_ids"] == ["b1", "b2"]


def test_normalize_result_json_missing_file(tmp_path: Path):
    norm = pv2.normalize_result_json(tmp_path / "nope.json")
    assert norm["ok"] is False
    assert norm["blocks"] == []
    assert norm["pages"] == []


def test_build_model_warns_on_missing_md(result_json_path: Path):
    m = pv2.build_normalized_document_model(
        result_json_path, document_md_path="/no/such/file.md")
    assert "document_md_path_unreadable" in m["warnings"]
    assert m["summary"]["warnings_count"] == len(m["warnings"])


def test_md_sheet_name_fallback(result_json_path: Path, tmp_path: Path):
    """Имя листа из MD подставляется, если в stamp_data его нет."""
    # result.json без sheet_name на стр.1 → MD-fallback
    data = _build_result_json()
    # уберём sheet_name из всех блоков стр.1
    for b in data["pages"][0]["blocks"]:
        b["stamp_data"].pop("sheet_name", None)
    rj = tmp_path / "r2.json"
    rj.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    md = tmp_path / "doc.md"
    md.write_text(
        "## СТРАНИЦА 1\n**Наименование листа:** Содержание тома\n\nтекст\n",
        encoding="utf-8")
    m = pv2.build_normalized_document_model(rj, document_md_path=md)
    p1 = _page(m, 1)
    assert p1["sheet_name"] == "Содержание тома"
    assert p1["page_type"] == "contents"
