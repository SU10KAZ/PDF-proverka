"""Классификатор семантики блока — покрытие после выноса из Pipeline V2.

Функция ``classify_block_semantic_type`` кормит ``semantic_type`` каждого блока
в ``prepared_document`` — это основной путь новой детерминированной цепочки
сравнения. Раньше её единственные проверки жили в тестах удалённого модуля
``pipeline_v2_prepared_ingest`` и ушли вместе с ним; здесь они восстановлены и
расширены, чтобы правка порядка веток или списка ключевых слов не прошла молча.
"""
from __future__ import annotations

import pytest

from backend.app.services.stage_comparison.block_semantic_type import (
    classify_block_semantic_type,
)


# ─── ассерции, восстановленные из удалённого теста Pipeline V2 ───────────────

def test_legend_by_sheet_name():
    legend = {"block_type": "image", "ocr_text": "Условные обозначения",
              "stamp_data": {"sheet_name": "Условные обозначения"}}
    assert classify_block_semantic_type(legend) == "legend"


def test_plan_by_sheet_name():
    plan = {"block_type": "image", "ocr_text": "",
            "stamp_data": {"sheet_name": "План 1 этажа"}}
    assert classify_block_semantic_type(plan) == "plan"


# ─── порядок веток: он и есть контракт ──────────────────────────────────────

def test_stamp_wins_over_everything():
    """Штамп проверяется первым: лист с легендой в имени всё равно штамп."""
    block = {"block_type": "image", "ocr_json_is_stamp": True,
             "stamp_data": {"sheet_name": "Условные обозначения"}}
    assert classify_block_semantic_type(block) == "stamp"


def test_legend_wins_over_table():
    """Экспликация — легенда, а не таблица (ветка легенды стоит выше)."""
    block = {"block_type": "table", "ocr_text": "Экспликация помещений"}
    assert classify_block_semantic_type(block) == "legend"


def test_text_block_never_becomes_scheme():
    """Текст, упоминающий схему, остаётся текстом: schema — графический тип."""
    block = {"block_type": "text", "ocr_text": "Принципиальная схема — см. лист 5"}
    assert classify_block_semantic_type(block) == "text"


def test_text_block_with_table_keyword():
    block = {"block_type": "text", "ocr_text": "Ведомость чертежей"}
    assert classify_block_semantic_type(block) == "table"


def test_text_block_title_page():
    block = {"block_type": "text", "ocr_text": "Титульный лист"}
    assert classify_block_semantic_type(block) == "title"


# ─── схемы и порог площади ──────────────────────────────────────────────────

def test_small_scheme_stays_scheme():
    block = {"block_type": "image", "ocr_text": "Однолинейная схема",
             "coords_norm": [0.0, 0.0, 0.3, 0.3]}   # площадь 0.09 < 0.45
    assert classify_block_semantic_type(block) == "scheme"


def test_large_scheme_by_area():
    block = {"block_type": "image", "ocr_text": "Однолинейная схема",
             "coords_norm": [0.0, 0.0, 0.9, 0.9]}   # площадь 0.81 >= 0.45
    assert classify_block_semantic_type(block) == "large_scheme"


def test_structural_scheme_is_large_regardless_of_area():
    block = {"block_type": "image", "ocr_text": "Структурная схема",
             "coords_norm": [0.0, 0.0, 0.1, 0.1]}
    assert classify_block_semantic_type(block) == "large_scheme"


def test_area_from_pixels_when_no_norm():
    block = {"block_type": "image", "ocr_text": "Схема электроснабжения",
             "coords_px": [0, 0, 900, 700], "page_width": 1000, "page_height": 1000}
    assert classify_block_semantic_type(block) == "large_scheme"


# ─── нормализация строк ─────────────────────────────────────────────────────

@pytest.mark.parametrize("text", ["Перечень условных обозначений",
                                  "перечень условных обозначении"])
def test_legend_keyword_is_case_insensitive(text):
    assert classify_block_semantic_type({"block_type": "image", "ocr_text": text}) == "legend"


def test_yo_is_normalised_to_ye():
    """OCR чередует ё/е — ключевые слова хранятся в форме с «е»."""
    block = {"block_type": "image", "ocr_text": "Перечёнь условных обозначений"}
    assert classify_block_semantic_type(block) == "legend"


# ─── устойчивость к неполным данным ─────────────────────────────────────────

def test_empty_block_is_unknown_not_crash():
    assert classify_block_semantic_type({}) in {"unknown", "text"}


def test_stamp_data_not_a_dict_is_tolerated():
    block = {"block_type": "image", "ocr_text": "", "stamp_data": "не словарь"}
    assert isinstance(classify_block_semantic_type(block), str)


def test_none_fields_are_tolerated():
    block = {"block_type": None, "category_code": None, "ocr_text": None,
             "stamp_data": None, "coords_norm": None, "coords_px": None}
    assert isinstance(classify_block_semantic_type(block), str)


# ─── связь с живым потребителем ─────────────────────────────────────────────

def test_prepared_document_imports_this_module():
    """prepared_document обязан брать классификатор отсюда, а не из Pipeline V2."""
    from pathlib import Path
    import backend.app.services.stage_comparison.prepared_document as pd
    src = Path(pd.__file__).read_text(encoding="utf-8")
    assert "from .block_semantic_type import classify_block_semantic_type" in src
    assert "pipeline_v2" not in src
