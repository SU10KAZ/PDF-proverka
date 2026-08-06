"""
test_project_id_form_dedup.py
-----------------------------
Один проект — одна форма имени в журнале решений и в графике.

Инцидент 06.08.2026: у Репникова в графике работ на один день встали четыре
плитки вместо двух — `PS/ПД-00542664-ПС-1_V1` и `ПД-00542664-ПС-1_V1` (то же
для ПС-2). Проект приехал из папки заказчика `.../PS/…`, поэтому его
`project_id` несёт префикс `PS`, а дисциплина документа определена как `SS`.

Две формы попадают в лог разными путями: загрузка решений из Excel берёт
project_id из скрытой колонки файла (с префиксом), сохранение из интерфейса —
из адреса страницы (без префикса). Дедуп в логе ключуется на
`(source_project, item_id)`, поэтому разные написания заводили ДВЕ группы
записей: 6409 лишних из 20433 (31%) на 117 проектах.

Склейка в графике сверялась с `section` и на паре PS/SS не срабатывала.

Run: python -m pytest tests/test_project_id_form_dedup.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.services.common.schedule_service import canonical_project  # noqa: E402
from backend.app.services.knowledge_base.knowledge_base_service import (  # noqa: E402
    canonical_source_project,
)


# ─── Запись в журнал: одна форма на все пути ───

@pytest.mark.parametrize("pid,expected", [
    ("PS/ПД-00542664-ПС-1_V1", "ПД-00542664-ПС-1_V1"),   # из Excel-колонки
    ("ПД-00542664-ПС-1_V1", "ПД-00542664-ПС-1_V1"),      # из адреса страницы
    ("EOM/13АВ-РД-ЭО-К3 V1", "13АВ-РД-ЭО-К3 V1"),
    ("AR/СТ26_01-14-АР0-2-РД", "СТ26_01-14-АР0-2-РД"),
])
def test_both_write_paths_produce_one_name(pid, expected):
    assert canonical_source_project(pid) == expected


def test_excel_and_ui_forms_collapse_to_same_key():
    """Именно это включает дедуп по (source_project, item_id) в логе."""
    assert canonical_source_project("PS/ПД-00542664-ПС-1_V1") == canonical_source_project(
        "ПД-00542664-ПС-1_V1"
    )


@pytest.mark.parametrize("pid", [
    "",
    "ПД-00542664-ПС-1_V1",
    "13АВ-РД-ОВ2-К1 V1",
])
def test_names_without_prefix_are_untouched(pid):
    assert canonical_source_project(pid) == pid


@pytest.mark.parametrize("pid", [
    "Объект 214/13АВ-РД-ОВ2-К1",      # кириллица — не код дисциплины
    "проекты/АР5-2-РД",               # длинный кириллический сегмент
    "very-long-section/АР5-2-РД",     # длиннее шести знаков
    "214. Alia (ASTERUS)/АР5-2-РД",   # пробелы и точки
])
def test_slash_inside_name_is_not_a_prefix(pid):
    """Срезать можно только короткий латинский код, иначе потеряем часть имени."""
    assert canonical_source_project(pid) == pid


# ─── График: склейка плиток ───

def test_schedule_merges_forms_when_prefix_differs_from_section():
    """Тот самый случай: префикс PS, дисциплина SS — раньше не склеивалось."""
    a = canonical_project("PS/ПД-00542664-ПС-1_V1", "SS")
    b = canonical_project("ПД-00542664-ПС-1_V1", "SS")
    assert a == b == "ПД-00542664-ПС-1_V1"


def test_schedule_merges_forms_when_prefix_matches_section():
    """Совпадающий префикс работал и раньше — не сломать."""
    a = canonical_project("OV/13АВ-РД-ОВ2-К1 V1", "OV")
    b = canonical_project("13АВ-РД-ОВ2-К1 V1", "OV")
    assert a == b == "13АВ-РД-ОВ2-К1 V1"


def test_schedule_merges_without_section_known():
    """Раздел пустой — префикс всё равно срезается, иначе плитки двоятся."""
    assert canonical_project("EOM/ПД-00542664-ЭО1-3_V1", "") == "ПД-00542664-ЭО1-3_V1"


def test_schedule_keeps_cyrillic_slash_name():
    assert canonical_project("Объект 214/13АВ-РД-ОВ2-К1", "OV") == "Объект 214/13АВ-РД-ОВ2-К1"


def test_same_bare_name_in_different_disciplines_stays_separate():
    """Ключ группы в графике включает object_id и section, поэтому одинаковые
    «голые» имена из разных дисциплин не сливаются — проверяем, что функция не
    выбрасывает информацию, на которой держится это различие."""
    assert canonical_project("AR/К1-РД", "AR") == canonical_project("KJ/К1-РД", "KJ")
    # сами по себе имена равны — различие обеспечивает section в ключе группы
