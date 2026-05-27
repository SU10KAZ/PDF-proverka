"""Тесты для backend/app/services/stage_comparison/text_location.py.

Проверяют:
  - md_page_marker метод: ищет цитату → берёт ближайший `## СТРАНИЦА N` сверху
  - heading_match fallback: если quote не найден, ищет по evidence.section
  - approx_location fallback: парсит «стр. 2» / «стр. 1–3» из LLM-подсказки
  - alignment_slot: маппится через page_alignment.items
  - not_found: ничего не нашлось — confidence=0.0, method='not_found'
  - двусторонний матч (left+right) даёт confidence=1.0 при наличии slot
"""
from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def sample_md(tmp_path: Path) -> tuple[Path, Path]:
    """Минимальные MD-файлы с маркерами `## СТРАНИЦА N`."""
    left = tmp_path / "left.md"
    left.write_text(
        "# Title\n\n## СТРАНИЦА 1\nКорпус 1. Фасад в осях 3.К-1.А M1_200\n"
        "\n## СТРАНИЦА 2\n**Лист:** 2\nСодержание тома\n"
        "\n## СТРАНИЦА 3\nКласс пожарной безопасности С0\n",
        encoding="utf-8",
    )
    right = tmp_path / "right.md"
    right.write_text(
        "# Title\n\n## СТРАНИЦА 1\nКорпус 1. Фасад в осях 3.К-1.А M1_200\n"
        "\n## СТРАНИЦА 2\n**Лист:** 2\nСодержание тома исправлено\n"
        "\n## СТРАНИЦА 3\nКласс пожарной безопасности С1\n",
        encoding="utf-8",
    )
    return left, right


def _pair(left: Path, right: Path) -> dict:
    return {
        "id": "p1",
        "left":  {"filename": "left.pdf",  "md_path": str(left)},
        "right": {"filename": "right.pdf", "md_path": str(right)},
    }


def test_md_page_marker_resolves_both_sides(sample_md):
    """Quote найдена в обеих MD → method=md_page_marker, обе страницы."""
    from backend.app.services.stage_comparison import text_location as tl

    left, right = sample_md
    pair = _pair(left, right)
    change = {
        "evidence_left":  {"quote": "Класс пожарной безопасности С0"},
        "evidence_right": {"quote": "Класс пожарной безопасности С1"},
    }
    alignment = [
        {"slot": 1, "left_page": 1, "right_page": 1},
        {"slot": 2, "left_page": 2, "right_page": 2},
        {"slot": 3, "left_page": 3, "right_page": 3},
    ]
    loc = tl.resolve_text_change_location(pair, change, alignment_items=alignment)
    assert loc["method"] == "md_page_marker"
    assert loc["left_page"] == 3
    assert loc["right_page"] == 3
    assert loc["alignment_slot"] == 3
    assert loc["confidence"] == 1.0


def test_md_page_marker_one_sided(sample_md):
    """Только evidence_right имеет совпадение — left остаётся None."""
    from backend.app.services.stage_comparison import text_location as tl

    left, right = sample_md
    pair = _pair(left, right)
    change = {
        "evidence_right": {"quote": "Содержание тома исправлено"},
    }
    alignment = [
        {"slot": 1, "left_page": 1, "right_page": 1},
        {"slot": 2, "left_page": 2, "right_page": 2},
    ]
    loc = tl.resolve_text_change_location(pair, change, alignment_items=alignment)
    assert loc["method"] == "md_page_marker"
    assert loc["left_page"] is None
    assert loc["right_page"] == 2
    assert loc["alignment_slot"] == 2
    assert loc["confidence"] == 0.7


def test_heading_match_when_quote_missing(tmp_path):
    """Quote не найдена, но evidence.section совпадает с heading — fallback."""
    from backend.app.services.stage_comparison import text_location as tl

    md = tmp_path / "left.md"
    md.write_text(
        "# Title\n\n## СТРАНИЦА 1\n### Архитектурные решения\nТело раздела\n"
        "\n## СТРАНИЦА 2\nДругой текст\n",
        encoding="utf-8",
    )
    pair = {
        "id": "p1",
        "left":  {"md_path": str(md)},
        "right": {"md_path": str(md)},
    }
    change = {
        "evidence_left": {"quote": "Цитата которой нет в MD",
                          "section": "Архитектурные решения"},
    }
    loc = tl.resolve_text_change_location(pair, change, alignment_items=[])
    assert loc["method"] == "heading_match"
    assert loc["left_page"] == 1
    assert loc["confidence"] == 0.3


def test_not_found_when_no_evidence(tmp_path):
    """Ни quote, ни section — method='not_found', page=None."""
    from backend.app.services.stage_comparison import text_location as tl

    md = tmp_path / "left.md"
    md.write_text("## СТРАНИЦА 1\nA\n## СТРАНИЦА 2\nB\n", encoding="utf-8")
    pair = {
        "id": "p1",
        "left":  {"md_path": str(md)},
        "right": {"md_path": str(md)},
    }
    change = {}
    loc = tl.resolve_text_change_location(pair, change, alignment_items=[])
    assert loc["method"] == "not_found"
    assert loc["left_page"] is None
    assert loc["right_page"] is None
    assert loc["alignment_slot"] is None
    assert loc["confidence"] == 0.0


def test_no_alignment_still_returns_pages(sample_md):
    """alignment_items=None → page_no возвращается, slot=None, confidence=0.5."""
    from backend.app.services.stage_comparison import text_location as tl

    left, right = sample_md
    pair = _pair(left, right)
    change = {"evidence_left": {"quote": "Класс пожарной безопасности С0"}}
    loc = tl.resolve_text_change_location(pair, change, alignment_items=None)
    assert loc["method"] == "md_page_marker"
    assert loc["left_page"] == 3
    assert loc["alignment_slot"] is None
    assert loc["confidence"] == 0.5


def test_approx_location_single_page(tmp_path):
    """Quote/section не нашлись, но approx_location='стр. 2' → page=2, conf=0.2."""
    from backend.app.services.stage_comparison import text_location as tl

    md = tmp_path / "left.md"
    md.write_text("## СТРАНИЦА 1\nA\n## СТРАНИЦА 2\nB\n", encoding="utf-8")
    pair = {
        "id": "p1",
        "left":  {"md_path": str(md)},
        "right": {"md_path": str(md)},
    }
    change = {
        "evidence_left": {
            "quote": "цитата которой нет в MD",
            "section": "Титульный лист",
            "approx_location": "стр. 2",
        },
        "evidence_right": {
            "quote": "тоже нет",
            "section": "Титульный лист",
            "approx_location": "стр. 1–3",
        },
    }
    loc = tl.resolve_text_change_location(pair, change, alignment_items=[])
    assert loc["method"] == "approx_location"
    assert loc["left_page"] == 2
    assert loc["right_page"] == 1  # начало диапазона «стр. 1–3»
    assert loc["confidence"] == 0.2


def test_approx_location_only_one_side(tmp_path):
    """Только evidence_right.approx_location валиден — left остаётся None."""
    from backend.app.services.stage_comparison import text_location as tl

    md = tmp_path / "left.md"
    md.write_text("## СТРАНИЦА 1\nA\n", encoding="utf-8")
    pair = {
        "id": "p1",
        "left":  {"md_path": str(md)},
        "right": {"md_path": str(md)},
    }
    change = {
        "evidence_left": {"quote": "нет в MD", "section": "Титульный лист"},
        "evidence_right": {
            "quote": "тоже нет",
            "section": "Титульный лист",
            "approx_location": "Лист 5",
        },
    }
    loc = tl.resolve_text_change_location(pair, change, alignment_items=[])
    assert loc["method"] == "approx_location"
    assert loc["left_page"] is None
    assert loc["right_page"] == 5
    assert loc["confidence"] == 0.2


def test_approx_location_unparseable_falls_through_to_not_found(tmp_path):
    """approx_location без цифры → method='not_found'."""
    from backend.app.services.stage_comparison import text_location as tl

    md = tmp_path / "left.md"
    md.write_text("## СТРАНИЦА 1\nA\n", encoding="utf-8")
    pair = {
        "id": "p1",
        "left":  {"md_path": str(md)},
        "right": {"md_path": str(md)},
    }
    change = {
        "evidence_left": {"quote": "нет", "approx_location": "штамп"},
    }
    loc = tl.resolve_text_change_location(pair, change, alignment_items=[])
    assert loc["method"] == "not_found"
    assert loc["left_page"] is None
    assert loc["confidence"] == 0.0


def test_md_page_marker_wins_over_approx_location(tmp_path):
    """Если md_page_marker сработал — approx_location не перетирает результат."""
    from backend.app.services.stage_comparison import text_location as tl

    md = tmp_path / "left.md"
    md.write_text(
        "## СТРАНИЦА 1\nуникальная цитата здесь\n## СТРАНИЦА 2\nB\n",
        encoding="utf-8",
    )
    pair = {
        "id": "p1",
        "left":  {"md_path": str(md)},
        "right": {"md_path": str(md)},
    }
    change = {
        "evidence_left": {
            "quote": "уникальная цитата здесь",
            "approx_location": "стр. 99",  # должен быть проигнорирован
        },
    }
    loc = tl.resolve_text_change_location(pair, change, alignment_items=[])
    assert loc["method"] == "md_page_marker"
    assert loc["left_page"] == 1


def test_missing_md_returns_not_found(tmp_path):
    """MD-файл отсутствует на диске — method='not_found', confidence=0.0."""
    from backend.app.services.stage_comparison import text_location as tl

    pair = {
        "id": "p1",
        "left":  {"md_path": str(tmp_path / "nonexistent_left.md")},
        "right": {"md_path": str(tmp_path / "nonexistent_right.md")},
    }
    change = {"evidence_left": {"quote": "anything"}}
    loc = tl.resolve_text_change_location(pair, change, alignment_items=[])
    assert loc["method"] == "not_found"
    assert loc["confidence"] == 0.0
