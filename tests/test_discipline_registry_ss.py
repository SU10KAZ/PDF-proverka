"""Реестр дисциплин: слаботочка (АК/АПЗ/АПС/КК/СОВ/СОТ/СОУЭ) определяется как SS.

Использует РЕАЛЬНЫЙ prompts/disciplines/_registry.json (без мока), чтобы поймать
регрессию реестра. Раньше «13АВ-РД-АК-…» не имел паттерна → падал в текстовый
детект и ложно определялся как OV (вентиляция/дымоудаление в АК-документе).

Run: python -m pytest tests/test_discipline_registry_ss.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.services.common import discipline_service as ds  # noqa: E402


@pytest.fixture(autouse=True)
def fresh_registry():
    ds._registry_cache = None  # читаем реальный файл
    yield
    ds._registry_cache = None


@pytest.mark.parametrize("name", [
    "13АВ-РД-АК-К6 (Книга 1) V2",
    "13АВ-РД-АПЗ.АПС-К4",
    "13АВ-РД-КК-ПА",
    "13АВ-РД-СОВ-К3-6",
    "13АВ-РД-СОТ-К3-6",
    "13АВ-РД-СОУЭ-ПА",
])
def test_slabotochka_detected_as_ss_by_name(name):
    d = ds.detect_discipline_detailed(name, name, "")
    assert d["code"] == "SS", d
    assert d["source"] == "folder_name", d


@pytest.mark.parametrize("name,code", [
    ("133-23-ГК-АР1", "AR"),
    ("13АВ-РД-ЭМ-К1", "EOM"),
    ("13АВ-РД-КЖ5.1", "KJ"),
    ("13АВ-РД-ОВ1.1-К2", "OV"),
    ("13АВ-РД-ВК1-К1", "VK"),
])
def test_other_disciplines_unaffected(name, code):
    assert ds.detect_discipline_detailed(name, name, "")["code"] == code
