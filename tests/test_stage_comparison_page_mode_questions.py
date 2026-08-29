"""Ручная пара страниц: анализируется она, а не весь документ.

Инженер выбрал «страница 29 слева ↔ страница 8 справа». Сопоставитель листов
при этом всё равно прогонялся по ВСЕМ страницам обоих документов, и каждое его
неуверенное предположение о посторонних листах становилось вопросом инженеру.
На ручной паре так набиралось одиннадцать вопросов о листах, которых человек
не выбирал, — и анализ выбранной пары ждал ответа на них.

Сопоставитель здесь совещательный. Его предложения остаются, но приходят
отдельной необязательной строкой.
"""
from __future__ import annotations

from backend.app.services.stage_comparison import production_orchestrator as production
from backend.app.services.stage_comparison.review_queue import build_review_queue


def _relations() -> dict:
    """Сопоставитель нашёл пары среди СТОРОННИХ листов обоих документов."""
    return {
        "kind": "stage_comparison_sheet_relations",
        "input_signature": "sheet-input",
        "sheet_labels": {
            "LEFT": {str(page): f"Лист Л{page}" for page in range(1, 40)},
            "RIGHT": {str(page): f"Лист П{page}" for page in range(1, 40)},
        },
        "relations": [
            {
                "relation_id": f"srel_{index}",
                "left_pages": [index],
                "right_pages": [index + 1],
                "relation_type": "MATCHED",
                "status": "POSSIBLE",
                "confidence": 0.4,
            }
            for index in range(1, 12)
        ],
    }


def _synthesis() -> dict:
    return {"changes": [], "review_items": []}


def test_a_document_run_still_asks_about_the_sheets_it_matched():
    queue = build_review_queue(
        _relations(), {}, _synthesis(), generated_at="fixed",
    )

    assert queue["counts"]["SHEET"] == 11


def test_a_manual_page_pair_asks_nothing_about_other_sheets():
    queue = build_review_queue(
        _relations(), {}, _synthesis(),
        include_sheet_questions=False,
        generated_at="fixed",
    )

    assert queue["counts"].get("SHEET", 0) == 0
    assert queue["questions"] == []


def test_the_question_scope_is_part_of_the_queue_identity():
    # Очередь, собранная для ручной пары, не должна выглядеть годной для
    # документного прогона: иначе ответы одной перепутаются с другой.
    document = build_review_queue(
        _relations(), {}, _synthesis(), generated_at="fixed",
    )
    page = build_review_queue(
        _relations(), {}, _synthesis(),
        include_sheet_questions=False, generated_at="fixed",
    )

    assert document["input_signature"] != page["input_signature"]


def test_the_page_mode_picks_the_scope_by_itself():
    page = production._build_review_questions(
        sheet_relations=_relations(),
        sheet_suggestions=None,
        entity_relations={},
        synthesis=_synthesis(),
        answers=None,
        input_mode="PAGE",
    )
    document = production._build_review_questions(
        sheet_relations=_relations(),
        sheet_suggestions=None,
        entity_relations={},
        synthesis=_synthesis(),
        answers=None,
        input_mode="DOCUMENT",
    )

    assert page["counts"].get("SHEET", 0) == 0
    assert document["counts"]["SHEET"] == 11


# ── Рекомендации остаются, но не блокируют ────────────────────────────────

def _suggestions() -> dict:
    return {
        "input_signature": "suggestions-input",
        "suggestions": [{
            "suggestion_id": "sugg_1",
            "relation_id": "srel_1",
            "selected_left_pages": [29],
            "selected_right_pages": [8],
            "suggested_left_pages": [29],
            "suggested_right_pages": [9],
            "actions": ["COMPARE_ADDITIONALLY", "IGNORE"],
        }],
    }


def test_a_matcher_recommendation_is_offered_but_never_blocks():
    questions = production._build_review_questions(
        sheet_relations=_relations(),
        sheet_suggestions=_suggestions(),
        entity_relations={},
        synthesis=_synthesis(),
        answers=None,
        input_mode="PAGE",
    )

    kinds = {item["question_type"] for item in questions["questions"]}
    assert kinds == {"PAGE_SUGGESTION_ACTION"}
    assert all(item["advisory"] is True for item in questions["questions"])
    assert all(item["blocking"] is False for item in questions["questions"])
    assert questions["counts"]["advisory"] == 1
    assert questions["counts"]["blocking"] == 0


def test_the_pipeline_does_not_wait_for_a_recommendation():
    questions = production._build_review_questions(
        sheet_relations=_relations(),
        sheet_suggestions=_suggestions(),
        entity_relations={},
        synthesis=_synthesis(),
        answers=None,
        input_mode="PAGE",
    )

    stage = production._review_question_stage(questions)

    assert stage["blocking"] == 0
    assert stage["advisory"] == 1
    assert stage["status"] == "COMPLETED"


def test_a_real_question_still_makes_the_pipeline_wait():
    questions = production._build_review_questions(
        sheet_relations=_relations(),
        sheet_suggestions=_suggestions(),
        entity_relations={},
        synthesis=_synthesis(),
        answers=None,
        input_mode="DOCUMENT",
    )

    stage = production._review_question_stage(questions)

    assert stage["blocking"] == 11
    assert stage["status"] == "NEEDS_REVIEW"
