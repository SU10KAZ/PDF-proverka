"""reserc.md #98 — единый модуль нормализации text_norm.

Проверяем эквивалентность миграции (delegating-обёртки дают тот же результат,
что прежние локальные нормализаторы) + ключевой инвариант: strip_html в
normalize_block_content (иначе слои сравнения дают противоположные verdicts).
"""
from __future__ import annotations

from backend.app.services.stage_comparison import text_norm as tn


def test_norm_for_grounding_basics():
    assert tn.norm_for_grounding("  Объём   работ\n\nПо\tЛёгкому ") == "объем работ по легкому"
    assert tn.norm_for_grounding(None) == ""
    assert tn.norm_for_grounding("ЁЖИК ёжик") == "ежик ежик"


def test_strip_html_removes_tags_keeps_content():
    assert tn.strip_html('<div data-bbox="1,2,3,4">5х10</div>').strip() == "5х10"
    assert tn.strip_html(None) == ""


def test_normalize_block_content_strips_html_and_prefix():
    # HTML + debug-префикс BLOCK: + ё + регистр + пробелы
    got = tn.normalize_block_content("BLOCK: b_07\n<div data-bbox='0,0'>Кабель ВВГнг 5Х10 Ё</div>")
    assert "block" not in got
    assert "<" not in got and ">" not in got
    # норм блок-контента НЕ транслитерирует х→x (это делает salient_numbers)
    assert got == "кабель ввгнг 5х10 е"


def test_salient_numbers_min_len():
    assert tn.salient_numbers("5х10 0,5 160а 1000", min_len=1) == ["5x10", "0.5", "160", "1000"]
    # min_len отсекает короткие (номера пунктов/единичные)
    assert "5" not in tn.salient_numbers("раздел 5 кабель 4х185", min_len=3)


def test_migration_equivalence_block_text():
    """normalize_block_text (text_block_equivalence) теперь = text_norm."""
    from backend.app.services.stage_comparison import text_block_equivalence as tbe
    raw = "BLOCK: x\n<b>Объём  5Х10  Ё</b>"
    assert tbe.normalize_block_text(raw) == tn.normalize_block_content(raw)


def test_migration_equivalence_grounding():
    """evidence_first_fallback._norm_text и comparison_merge._norm_text = base."""
    from backend.app.services.stage_comparison import evidence_first_fallback as eff
    from backend.app.services.stage_comparison import comparison_merge as cm
    s = "  Кабель\tВВГнг   Ёлка "
    assert eff._norm_text(s) == tn.norm_for_grounding(s)
    assert cm._norm_text(s) == tn.norm_for_grounding(s)
    # comparison_merge ещё извлекает dict-evidence
    assert cm._norm_text({"quote": s}) == tn.norm_for_grounding(s)
