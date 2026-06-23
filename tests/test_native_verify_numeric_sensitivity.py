"""reserc.md #35 — числовая чувствительность верификации цитат норм.

Word-level Jaccard почти не реагирует на одно изменённое число среди многих слов.
_salient_numbers + numeric_recall ловят случай «текст похож, но номинал другой»,
не добавляя ложных подтверждений (numeric-check только ужесточает).
"""
from __future__ import annotations

from backend.app.pipeline.stages.norms import _native_verify as nv


def test_salient_numbers_extracts_values():
    nums = nv._salient_numbers("кабель ВВГнг 5х10, ток 160 А, ширина 0,5 с, 1000")
    # сечение 5x10, ток 160а, 0.5с, 1000
    assert "5x10" in nums
    assert any(n.startswith("160") for n in nums)
    assert "1000" in nums


def test_salient_numbers_empty():
    assert nv._salient_numbers("текст без числовых значений") == set()
    assert nv._salient_numbers("") == set()


def test_numeric_recall():
    assert nv._numeric_recall(set(), {"5x10"}) is None          # в цитате чисел нет
    assert nv._numeric_recall({"5x10"}, {"5x10"}) == 1.0
    assert nv._numeric_recall({"5x10", "160а"}, {"5x10"}) == 0.5
    assert nv._numeric_recall({"25а"}, {"16а"}) == 0.0


def test_jaccard_alone_misses_number_swap():
    # Демонстрация проблемы: тексты отличаются ТОЛЬКО числом → высокий Jaccard.
    a = "Номинальный ток аппарата защиты должен быть не менее 16 А для линии"
    b = "Номинальный ток аппарата защиты должен быть не менее 25 А для линии"
    assert nv._jaccard(a, b) >= nv.SIMILARITY_THRESHOLD   # Jaccard бы подтвердил
    # numeric_recall ловит расхождение
    nc = nv._salient_numbers(a)
    na = nv._salient_numbers(b)
    assert nv._numeric_recall(nc, na) < nv.NUMERIC_MIN_RECALL


def test_numeric_match_keeps_verification():
    # Правильная цитата (числа совпадают) — numeric_recall высокий, не мешает.
    a = "ток не менее 16 А, сечение 5х10"
    b = "Для линии: ток не менее 16 А при сечении 5х10 мм²"
    assert nv._numeric_recall(nv._salient_numbers(a), nv._salient_numbers(b)) == 1.0
