"""Резерв нативного текстового слоя PDF: происхождение, права и полнота.

Лист однолинейной схемы приходит из CAD одной картинкой, поэтому Markdown не
даёт по нему ни одной текстовой единицы. Пока сравнение видело такую сторону
пустой, оно честно говорило «я не прочитал ничего» — и на этом останавливалось,
хотя в PDF текст есть.

Резерв дочитывает такую страницу прямо из вектор-слоя. Права у прочитанного
намеренно урезаны: оно может ПОДТВЕРДИТЬ дословное совпадение и опровергнуть
чужое утверждение, но не может ничего утверждать само. Причина в том, что у
строки чертежа нет объекта-владельца: «500А» принадлежит конкретному аппарату,
но сама об этом не знает, и сближать её с чужой строкой по похожести значит
выдумывать изменения.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from backend.app.services.stage_comparison import (
    recognition_coverage,
    text_comparison,
    text_differences,
    production_text_flow,
)


ROOT = Path(__file__).resolve().parents[1]
STORE = ROOT / "projects_v2/objects/272_Sadovnicheskaya_76_Balchug_Esteyt/comparison"
DRAWING_ONLY_PDF = (
    STORE
    / "stage_1/documents/Страница_52_из_АА_БЭ-03-ДС3-ИОС1.1"
    / "versions/v001/02_work/document.pdf"
)


def _fragment(fragment_id: str, text: str, source: str, page: int = 1) -> dict:
    return {
        "id": fragment_id,
        "stage": "stage_1",
        "pdf_page": page,
        "source": source,
        "text": text,
        "canonical_text": text.lower(),
        "source_kind": "paragraph",
        "order": 0,
        "bboxes": [{"x": 0.1, "y": 0.1, "width": 0.2, "height": 0.01}],
    }


# ── Провенанс ─────────────────────────────────────────────────────────────

def test_markdown_fragments_are_labelled():
    fragments = text_comparison.parse_markdown_fragments(
        "## Page 1\n\n### BLOCK #1 [TEXT]: blk_a\n\nПримечания:\n",
        "stage_1",
    )
    assert fragments
    assert {item["source"] for item in fragments} == {
        text_comparison.SOURCE_MARKDOWN
    }


def test_native_fragments_are_labelled_and_located():
    fitz = pytest.importorskip("fitz")
    if not DRAWING_ONLY_PDF.is_file():
        pytest.skip("корпус ГРЩ не установлен")
    fragments = text_comparison.native_page_fragments(
        DRAWING_ONLY_PDF, [1], "left", fitz
    )
    assert fragments
    assert {item["source"] for item in fragments} == {
        text_comparison.SOURCE_NATIVE_PDF
    }
    assert all(item["bboxes"] for item in fragments)
    assert all(item["pdf_page"] == 1 for item in fragments)


# ── Права нативной единицы в сравнении ────────────────────────────────────

def test_native_fragment_confirms_an_exact_match():
    """Дословное совпадение подтверждать можно — это отмена ложного «добавлено»."""
    result = text_differences.compare_group(
        [_fragment("l1", "Садовническая ул., вл 76/71", text_comparison.SOURCE_NATIVE_PDF)],
        [_fragment("r1", "Садовническая ул., вл 76/71", text_comparison.SOURCE_MARKDOWN)],
    )
    assert len(result["same"]) == 1
    assert not result["added"]
    assert not result["removed"]


def test_native_fragment_never_becomes_removed():
    """Двести строк чертежа не должны превращаться в двести «удалено»."""
    result = text_differences.compare_group(
        [
            _fragment("l1", "Шкаф упр. насосов", text_comparison.SOURCE_NATIVE_PDF),
            _fragment("l2", "1QF4", text_comparison.SOURCE_NATIVE_PDF),
        ],
        [_fragment("r1", "Примечания:", text_comparison.SOURCE_MARKDOWN)],
    )
    assert not result["removed"]
    assert [item["after"] for item in result["added"]] == ["Примечания:"]


def test_native_fragment_never_invents_a_changed_value():
    """«500А» и «3200А» — подписи разных аппаратов, а не изменение значения.

    Похожие короткие строки чертежа сблизились бы по счёту кандидатов, и
    сравнение объявило бы изменение, которого никто не доказывал.
    """
    result = text_differences.compare_group(
        [_fragment("l1", "1QF4 500А ППГнг(А)-НF", text_comparison.SOURCE_NATIVE_PDF)],
        [_fragment("r1", "1QF4 3200А ППГнг(А)-HF", text_comparison.SOURCE_MARKDOWN)],
    )
    assert not result["changed"]
    assert not result["removed"]


def test_markdown_fragments_keep_all_their_rights():
    result = text_differences.compare_group(
        [_fragment("l1", "Площадь 44,10 м2", text_comparison.SOURCE_MARKDOWN)],
        [_fragment("r1", "Площадь 44,14 м2", text_comparison.SOURCE_MARKDOWN)],
    )
    assert len(result["changed"]) == 1


def test_fragment_without_a_label_is_treated_as_markdown():
    """Записи, сделанные до появления провенанса, в правах не поражаются."""
    left = _fragment("l1", "Площадь 44,10 м2", text_comparison.SOURCE_MARKDOWN)
    right = _fragment("r1", "Площадь 44,14 м2", text_comparison.SOURCE_MARKDOWN)
    del left["source"], right["source"]
    assert len(text_differences.compare_group([left], [right])["changed"]) == 1


# ── Полнота распознавания ─────────────────────────────────────────────────

def test_native_page_is_never_fully_recognized():
    """Источник и проверяющий совпали — «достаточно» здесь невозможно."""
    verdict = recognition_coverage.page_coverage(
        [_fragment("l1", "Шинопровод 2000А", text_comparison.SOURCE_NATIVE_PDF)],
        {"has_text_layer": True, "char_count": 4606},
    )
    assert verdict["status"] == recognition_coverage.PARTIAL
    assert recognition_coverage.REASON_NATIVE_FALLBACK in verdict["reason_codes"]


def test_page_with_no_fragments_at_all_stays_insufficient():
    verdict = recognition_coverage.page_coverage(
        [], {"has_text_layer": True, "char_count": 4606}
    )
    assert verdict["status"] == recognition_coverage.INSUFFICIENT
    assert recognition_coverage.REASON_NO_FRAGMENTS in verdict["reason_codes"]


def test_markdown_page_can_still_be_sufficient():
    """Резерв не отбирает у нормально прочитанной страницы её вердикт."""
    fragment = _fragment("l1", "Помещение 315.1", text_comparison.SOURCE_MARKDOWN)
    fragment["pdf_canonical_text"] = "помещение 315.1"
    verdict = recognition_coverage.page_coverage(
        [fragment], {"has_text_layer": True, "char_count": 4606}
    )
    assert verdict["status"] == recognition_coverage.SUFFICIENT


# ── Включение резерва ─────────────────────────────────────────────────────

def test_fallback_only_fires_on_a_page_markdown_left_empty():
    fitz = pytest.importorskip("fitz")
    if not DRAWING_ONLY_PDF.is_file():
        pytest.skip("корпус ГРЩ не установлен")
    already_read = [_fragment("m1", "Примечания:", text_comparison.SOURCE_MARKDOWN)]
    report = production_text_flow._append_native_fallback(
        already_read, pdf_path=DRAWING_ONLY_PDF, pages={1}, side="left", fitz=fitz
    )
    assert report["applied"] is False
    assert len(already_read) == 1


def test_fallback_fires_when_markdown_read_nothing():
    fitz = pytest.importorskip("fitz")
    if not DRAWING_ONLY_PDF.is_file():
        pytest.skip("корпус ГРЩ не установлен")
    fragments: list[dict] = []
    report = production_text_flow._append_native_fallback(
        fragments, pdf_path=DRAWING_ONLY_PDF, pages={1}, side="left", fitz=fitz
    )
    assert report["applied"] is True
    assert report["pages"] == [1]
    assert report["fragments"] == len(fragments) > 0
    assert {item["source"] for item in fragments} == {
        text_comparison.SOURCE_NATIVE_PDF
    }
