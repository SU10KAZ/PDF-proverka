"""reserc.md #20/#95 — EN/RU drift guard для text_analysis_task.md.

EN-шаблон имел приоритет в task_builder, но не содержал секцию критериев
«ПРОВЕРИТЬ ПО СМЕЖНЫМ» (anti-false-positive guidance) → она молча не доходила
до LLM. Эти тесты ловят будущий дрейф: обе языковые версии обязаны нести и
заголовок секции критериев, и обе ветки СТАВИТЬ / НЕ СТАВИТЬ.

Только чтение файлов: ни LLM, ни pipeline.
"""
from __future__ import annotations

from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_TEMPLATES = {
    "ru": _ROOT / "prompts" / "pipeline" / "ru" / "text_analysis_task.md",
    "en": _ROOT / "prompts" / "pipeline" / "en" / "text_analysis_task.md",
}


@pytest.mark.parametrize("lang", sorted(_TEMPLATES))
def test_template_exists(lang):
    assert _TEMPLATES[lang].is_file(), f"missing: {_TEMPLATES[lang]}"


@pytest.mark.parametrize("lang", sorted(_TEMPLATES))
def test_template_has_cross_discipline_criteria_section(lang):
    text = _TEMPLATES[lang].read_text(encoding="utf-8")
    # Заголовок секции критериев (RU или EN вариант).
    assert (
        "Критерии для «ПРОВЕРИТЬ ПО СМЕЖНЫМ»" in text
        or "Criteria for the «ПРОВЕРИТЬ ПО СМЕЖНЫМ»" in text
    ), f"{lang}/text_analysis_task.md потерял секцию критериев «ПРОВЕРИТЬ ПО СМЕЖНЫМ»"
    # Обе ветки: когда СТАВИТЬ и когда НЕ СТАВИТЬ.
    assert ("ONLY when" in text) or ("только когда" in text), (
        f"{lang}: нет ветки «ставить только когда»"
    )
    assert ("Do NOT assign" in text) or ("НЕ СТАВИТЬ" in text), (
        f"{lang}: нет ветки «не ставить когда»"
    )
