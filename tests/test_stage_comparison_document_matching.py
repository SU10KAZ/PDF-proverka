from __future__ import annotations

from backend.app.services.stage_comparison import document_matching


def _document(name: str, side: str) -> dict:
    return {"filename": name, "pdf_path": f"/{side}/{name}"}


def test_approximate_revision_and_separator_variants_score_high():
    similarity = document_matching.document_name_similarity

    assert similarity("13АВ-РД-АР0.1-ПА_V2.pdf", "13АВ-РД-АР0.1-ПА_V3.pdf") >= 0.95
    assert similarity("АА_БЭ-03-ДС3-АР1.pdf", "АА_БЭ-03-АР1-КОРР.pdf") >= 0.95
    assert similarity("АА_БЭ-03-ДС3-ИОС-4.1.pdf", "ААБЭ-03-ДС3-ИОС4.1.pdf") >= 0.95


def test_different_document_codes_stay_below_automatic_threshold():
    similarity = document_matching.document_name_similarity(
        "АА_БЭ-03-ДЗ-КР6.7.pdf",
        "АА_БЭ-03-КР2-4.2.pdf",
    )

    assert similarity < document_matching.MIN_DOCUMENT_SIMILARITY


def test_suggestion_is_one_to_one_and_leaves_uncertain_documents_in_separate_bottom_rows():
    left = [
        _document("13АВ-РД-АР0.1-ПА_V2.pdf", "left"),
        _document("АА_БЭ-03-ДС3-АР1.pdf", "left"),
        _document("АА_БЭ-03-ДЗ-КР6.7.pdf", "left"),
    ]
    right = [
        _document("АА_БЭ-03-АР1-КОРР.pdf", "right"),
        _document("13АВ-РД-АР0.1-ПА_V3.pdf", "right"),
        _document("АА_БЭ-03-6.2-Корр_ТХ_ВТ.pdf", "right"),
    ]

    result = document_matching.suggest_document_pairing(left, right)

    assert result["matched_count"] == 2
    assert result["unmatched_left_count"] == 1
    assert result["unmatched_right_count"] == 1
    assert result["left_order"][:2] == [left[0]["pdf_path"], left[1]["pdf_path"]]
    assert result["right_order"][:2] == [right[1]["pdf_path"], right[0]["pdf_path"]]
    assert result["left_order"][-2:] == [left[2]["pdf_path"], None]
    assert result["right_order"][-2:] == [None, right[2]["pdf_path"]]
    assert len({pair["right_pdf"] for pair in result["confirmed_pairs"]}) == 2


def test_empty_stage_returns_empty_orders():
    assert document_matching.suggest_document_pairing([], []) == {
        "left_order": [],
        "right_order": [],
        "confirmed_pairs": [],
        "matches": [],
        "matched_count": 0,
        "unmatched_left_count": 0,
        "unmatched_right_count": 0,
    }
